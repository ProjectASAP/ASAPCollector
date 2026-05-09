from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

BENCH_ROOT = Path(__file__).resolve().parent
REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent

if str(BENCH_ROOT) not in sys.path:
    sys.path.insert(0, str(BENCH_ROOT))

from scrape import append_metrics_snapshot
from groupings import parse_grouping_list
from compare_crosskey import (
    append_report_section,
    evaluate_grouping,
    load_ground_truth_per_symbol,
    load_sketch_per_symbol,
    write_crosskey_csv,
)

PATCH_CMD = REPO_ROOT / "opentelemetry-collector-contrib-patch" / "cmd"

_ASAP_OTEL_BIN = PATCH_CMD / "asap-otel" / "asap-otel"
DEFAULT_COLLECTOR_PATHS = {
    # All sketch types now ship in the unified asap-otel distribution; only
    # `kll/KLL` is still produced as a standalone binary by `cmd/kll/`.
    "ddsketch": _ASAP_OTEL_BIN,
    "kll": PATCH_CMD / "kll" / "KLL",
    "hll": _ASAP_OTEL_BIN,
    "countsketch": _ASAP_OTEL_BIN,
    "countminsketch": _ASAP_OTEL_BIN,
    "nop": _ASAP_OTEL_BIN,
}

PROMETHEUS_METRICS_URL = os.environ.get("PROMETHEUS_METRICS_URL", "http://localhost:8889/metrics")
DEFAULT_DAYS = ("08-11-21", "09-11-21", "10-11-21", "11-11-21", "12-11-21")


@dataclass(frozen=True)
class QueryCfg:
    """Per-query benchmark config. Add/remove queries by editing QUERY_CONFIG only."""

    aggregations: tuple[str, ...]
    time_window: str
    dataset: str
    group_by: tuple[str, ...]
    sketch_family: str  # "quantile" | "frequency" | "cardinality" | "nop"


# Populated incrementally by each debs_qN branch.
QUERY_CONFIG: dict[str, QueryCfg] = {}

NOP_QUERIES = frozenset(q for q, c in QUERY_CONFIG.items() if c.sketch_family == "nop")
DEFAULT_QUERIES = tuple(QUERY_CONFIG)

# Controller `select_window_strategy`: window mode if latency_sla is unset or
# latency_sla >= time_window; otherwise batch. Sketch processors only forward to
# Prometheus each batch in batch mode, so benchmark plans must set an SLA
# strictly below every query time_window (5m default, 15m for Q8).
BENCH_LATENCY_SLA_FOR_BATCH_MODE = "30s"


def _env_path(key: str, default: Path) -> Path:
    v = os.environ.get(key)
    return Path(v).expanduser() if v else default


def resolve_collector_bin(query: str, sketch: str, collector_override: str | None) -> Path:
    if collector_override:
        return Path(collector_override).expanduser()
    cfg = QUERY_CONFIG[query]
    family = cfg.sketch_family
    if family == "nop":
        return _env_path("COLLECTOR_NOP", DEFAULT_COLLECTOR_PATHS["nop"])
    if family == "cardinality":
        return _env_path("COLLECTOR_HLL", DEFAULT_COLLECTOR_PATHS["hll"])
    if family == "frequency":
        if sketch == "countminsketch":
            return _env_path("COLLECTOR_COUNTMINSKETCH", DEFAULT_COLLECTOR_PATHS["countminsketch"])
        return _env_path("COLLECTOR_COUNTSKETCH", DEFAULT_COLLECTOR_PATHS["countsketch"])
    if family == "quantile":
        if sketch == "kll":
            return _env_path("COLLECTOR_KLL", DEFAULT_COLLECTOR_PATHS["kll"])
        return _env_path("COLLECTOR_DDSKETCH", DEFAULT_COLLECTOR_PATHS["ddsketch"])
    return _env_path("COLLECTOR_DDSKETCH", DEFAULT_COLLECTOR_PATHS["ddsketch"])


def plan_aggregations(query: str) -> list[str]:
    return list(QUERY_CONFIG[query].aggregations)


def time_window_for_query(query: str) -> str:
    return QUERY_CONFIG[query].time_window


def dataset_for_query(query: str) -> str:
    return QUERY_CONFIG[query].dataset


def replay_technical_mode(mode: str) -> str:
    if mode == "throughput":
        return "max"
    if mode in ("latency", "sketch-finance"):
        return "paced"
    return "paced"


def group_by_labels_for_plan(query: str) -> list[str]:
    return list(QUERY_CONFIG[query].group_by)


def sketch_for_matrix_cell(query: str, sketch_quantile: str, sketch_freq: str, sketch_card: str) -> str:
    family = QUERY_CONFIG[query].sketch_family
    if family == "frequency":
        return sketch_freq
    if family == "cardinality":
        return sketch_card
    if family == "nop":
        return "nop"
    return sketch_quantile


def sketch_type_for_plan(query: str, sketch: str | None) -> str | None:
    s = (sketch or "").strip().lower()
    family = QUERY_CONFIG[query].sketch_family
    if family == "frequency":
        return "countminsketch" if s == "countminsketch" else "countsketch"
    if family == "cardinality":
        return "hll"
    if family == "quantile":
        return "kll" if s == "kll" else "ddsketch"
    return None


def build_plan_body(metric: str, query: str, sketch: str | None) -> dict[str, Any]:
    body: dict[str, Any] = {
        "metric_name": metric,
        "aggregations": plan_aggregations(query),
        "time_window": time_window_for_query(query),
        "latency_sla": BENCH_LATENCY_SLA_FOR_BATCH_MODE,
        "group_by_labels": group_by_labels_for_plan(query),
        "accuracy_sla": 0.01,
        "workload": {
            "series_count": 6000,
            "samples_per_sec_per_series": 100,
            "bytes_per_raw_sample": 100,
            "data_distribution": "zipf",
        },
    }
    st = sketch_type_for_plan(query, sketch)
    if st is not None:
        body["sketch_type"] = st
    return body


def controller_ready(controller: str) -> bool:
    try:
        r = requests.get(f"{controller.rstrip('/')}/api/v1/agents", timeout=2)
        return r.status_code == 200
    except requests.RequestException:
        return False


def parse_controller_api_port(controller: str) -> int:
    parsed = urlparse(controller)
    if parsed.port is not None:
        return parsed.port
    if parsed.scheme == "https":
        return 443
    return 8080


def start_controller_if_needed(
    controller: str,
    controller_bin: Path,
    auto_start: bool,
    results_dir: Path,
) -> int | None:
    if controller_ready(controller):
        return None
    if not auto_start:
        print("Controller not reachable; set AUTO_START_CONTROLLER=1 or start it manually.", file=sys.stderr)
        sys.exit(1)
    parsed = urlparse(controller)
    host = (parsed.hostname or "localhost").lower()
    if host not in ("localhost", "127.0.0.1", "::1"):
        print("Controller auto-start only supports localhost.", file=sys.stderr)
        sys.exit(1)
    if not controller_bin.is_file() or not os.access(controller_bin, os.X_OK):
        print(f"Controller binary missing or not executable: {controller_bin}", file=sys.stderr)
        sys.exit(1)
    api_port = parse_controller_api_port(controller)
    opamp_port = int(os.environ.get("CONTROLLER_OPAMP_PORT", "4320"))
    results_dir.mkdir(parents=True, exist_ok=True)
    log_path = results_dir / "controller.log"
    env = os.environ.copy()
    env["CONTROLLER_ADDR"] = f"0.0.0.0:{api_port}"
    env["CONTROLLER_OPAMP_ADDR"] = f"0.0.0.0:{opamp_port}"
    env["CONTROLLER_OPAMP_ENDPOINT"] = f"ws://127.0.0.1:{opamp_port}/v1/opamp"
    with open(log_path, "ab", buffering=0) as logf:
        proc = subprocess.Popen(
            [str(controller_bin)],
            cwd=str(REPO_ROOT),
            env=env,
            stdout=logf,
            stderr=subprocess.STDOUT,
        )
    for _ in range(320):
        if controller_ready(controller):
            print(f"Controller ready (pid {proc.pid})", file=sys.stderr)
            return proc.pid
        time.sleep(0.25)
        if proc.poll() is not None:
            print(f"Controller exited during startup. See {log_path}", file=sys.stderr)
            sys.exit(1)
    print(f"Controller did not become ready. See {log_path}", file=sys.stderr)
    proc.send_signal(signal.SIGTERM)
    proc.wait(timeout=5)
    sys.exit(1)


def stop_controller(pid: int | None) -> None:
    if pid is None:
        return
    try:
        os.kill(pid, signal.SIGTERM)
        time.sleep(0.2)
    except ProcessLookupError:
        return
    try:
        os.waitpid(pid, 0)
    except ChildProcessError:
        pass


def post_plan(controller: str, body: dict[str, Any]) -> None:
    url = f"{controller.rstrip('/')}/api/v1/plan"
    r = requests.post(url, json=body, timeout=60)
    r.raise_for_status()
    print(r.text[:2000], file=sys.stderr)


def nop_config_path(collector_bin: Path) -> Path:
    parent = collector_bin.parent
    cand = parent.parent / "config-bench.yaml"
    if cand.is_file():
        return cand
    return parent / "config-bench.yaml"


def wait_for_prometheus(url: str, timeout_s: float = 30.0) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=2)
            if r.status_code == 200:
                return True
        except requests.RequestException:
            pass
        time.sleep(0.25)
    return False


def kill_process_on_tcp_port(port: int) -> None:
    try:
        out = subprocess.check_output(
            ["lsof", "-ti", f"tcp:{port}"],
            stderr=subprocess.DEVNULL,
            text=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return
    for pid in out.strip().split():
        try:
            subprocess.run(["kill", pid], check=False)
        except OSError:
            pass
    time.sleep(1)


def maybe_clear_aggregate_csvs(results_dir: Path, clear: bool) -> None:
    if not clear:
        return
    for name in ("throughput.csv", "latency.csv"):
        p = results_dir / name
        if p.is_file():
            p.unlink()


def _run_one_query_day(
    bench_root: Path,
    results_dir: Path,
    query: str,
    day: str,
    days: str,
    mode: str,
    speed: float,
    batch_size: int,
    sketch: str,
    collector_override: str | None,
    accuracy_minutes: int = 0,
) -> int:
    """Run one (query, day) cell: start collector, scrape, replay, compare, analyze.

    Returns 0 on success, non-zero on failure. Does not manage the controller
    lifecycle — callers are responsible for that.
    """
    controller = os.environ.get("CONTROLLER", "http://localhost:8080")
    metric = os.environ.get("METRIC", "financial.last_trade_price")
    collector_bin = resolve_collector_bin(query, sketch, collector_override)
    if not collector_bin.is_file() or not os.access(collector_bin, os.X_OK):
        print(f"Collector binary missing or not executable: {collector_bin}", file=sys.stderr)
        return 1

    metrics_port = int(urlparse(PROMETHEUS_METRICS_URL).port or 8889)
    kill_process_on_tcp_port(metrics_port)

    is_nop = query in NOP_QUERIES
    py = sys.executable
    cpid: subprocess.Popen | None = None
    try:
        if not is_nop:
            post_plan(controller, build_plan_body(metric, query, sketch if sketch and sketch != "nop" else None))
            cpid = subprocess.Popen(
                [str(collector_bin), f"--config={controller.rstrip('/')}/api/v1/config/{metric}"],
                stdout=open(results_dir / "collector.log", "wb"),
                stderr=subprocess.STDOUT,
                cwd=str(bench_root),
            )
        else:
            nop_cfg = nop_config_path(collector_bin)
            if not nop_cfg.is_file():
                print(f"NOP config not found: {nop_cfg}", file=sys.stderr)
                return 1
            print(f"NOP query {query}: using {nop_cfg}", file=sys.stderr)
            cpid = subprocess.Popen(
                [str(collector_bin), f"--config={nop_cfg}"],
                stdout=open(results_dir / "collector.log", "wb"),
                stderr=subprocess.STDOUT,
                cwd=str(bench_root),
            )

        if not is_nop:
            if not wait_for_prometheus(PROMETHEUS_METRICS_URL, timeout_s=30.0):
                print(
                    f"Warning: {PROMETHEUS_METRICS_URL} not reachable within 30s; "
                    "sketch_output may be empty.",
                    file=sys.stderr,
                )
        else:
            time.sleep(3)

        scrape_argv = [
            py,
            str(bench_root / "scrape.py"),
            "--query", query,
            "--day", day,
            "--out-dir", str(results_dir / "sketch_output"),
            "--url", PROMETHEUS_METRICS_URL,
            "--duration", "7200",
        ]
        if is_nop:
            scrape_argv.append("--no-probe")
        scrape_proc = subprocess.Popen(scrape_argv, cwd=str(bench_root))

        replay_argv = [
            py,
            str(bench_root / "replay.py"),
            "--dataset", dataset_for_query(query),
            "--days", days,
            "--mode", replay_technical_mode(mode),
            "--speed-factor", str(speed),
            "--batch-size", str(batch_size),
            "--results-dir", str(results_dir),
            "--query", query,
            "--day", day,
        ]
        if accuracy_minutes > 0:
            replay_argv += ["--max-event-minutes", str(accuracy_minutes)]
        subprocess.run(replay_argv, check=True, cwd=str(bench_root))

        scrape_proc.send_signal(signal.SIGTERM)
        try:
            scrape_proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            scrape_proc.kill()

        if not is_nop:
            day_tag = day.replace(".csv", "").replace("debs2022-gc-trading-day-", "")
            out_csv = results_dir / "sketch_output" / query / f"{day_tag}.csv"
            try:
                n = append_metrics_snapshot(PROMETHEUS_METRICS_URL, out_csv)
                print(f"Final Prometheus snapshot: {n} metric lines -> {out_csv}", file=sys.stderr)
            except Exception as exc:
                print(f"Final snapshot failed: {exc}", file=sys.stderr)

        time.sleep(2)
        if cpid is not None:
            cpid.send_signal(signal.SIGTERM)
            try:
                cpid.wait(timeout=15)
            except subprocess.TimeoutExpired:
                cpid.kill()
            cpid = None

        subprocess.run(
            [
                py, str(bench_root / "compare.py"),
                "--query", query,
                "--day", day,
                "--gt-dir", str(results_dir / "ground_truth"),
                "--sketch-dir", str(results_dir / "sketch_output"),
                "--out-dir", str(results_dir / "comparison"),
            ],
            check=True,
            cwd=str(bench_root),
        )
        subprocess.run(
            [
                py, str(bench_root / "analyze.py"),
                "--results-dir", str(results_dir),
                "--query", query,
                "--day", day,
                "--replay-mode", mode,
            ],
            check=True,
            cwd=str(bench_root),
        )
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {e}", file=sys.stderr)
        return e.returncode or 1
    finally:
        if cpid is not None and cpid.poll() is None:
            cpid.kill()

    return 0


def run_benchmark_test(args: argparse.Namespace) -> int:
    bench_root = BENCH_ROOT
    results_dir: Path = args.results_dir
    results_dir.mkdir(parents=True, exist_ok=True)
    maybe_clear_aggregate_csvs(results_dir, args.clear_results)

    controller = os.environ.get("CONTROLLER", "http://localhost:8080")
    controller_bin = _env_path(
        "CONTROLLER_BIN",
        REPO_ROOT / "controller" / "target" / "release" / "controller",
    )
    auto_start = os.environ.get("AUTO_START_CONTROLLER", "1") == "1"
    ctrl_pid = start_controller_if_needed(controller, controller_bin, auto_start, results_dir)

    query = args.query
    day = args.day
    days_raw = (getattr(args, "days", None) or "").strip()
    days = days_raw if days_raw else day
    sketch = args.sketch or ""

    print(f"Using collector: (QUERY={query} SKETCH={sketch or '(default)'})", file=sys.stderr)

    if args.skip_gt != "1":
        if query in ("Q9", "Q10", "Q11", "Q12"):
            print(f"No offline ground truth for {query}", file=sys.stderr)
        else:
            try:
                subprocess.run(
                    [
                        sys.executable,
                        str(bench_root / "ground_truth" / "run_gt.py"),
                        "--query", query,
                        "--day", day,
                        "--out-dir", str(results_dir / "ground_truth"),
                    ],
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                stop_controller(ctrl_pid)
                return e.returncode or 1
    else:
        print(f"skip_gt=1: skipping ground truth for {query}/{day}", file=sys.stderr)

    try:
        rc = _run_one_query_day(
            bench_root=bench_root,
            results_dir=results_dir,
            query=query,
            day=day,
            days=days,
            mode=args.mode,
            speed=args.speed,
            batch_size=args.batch_size,
            sketch=sketch,
            collector_override=args.collector,
            accuracy_minutes=getattr(args, "accuracy_minutes", 0),
        )
    finally:
        stop_controller(ctrl_pid)

    return rc


def run_matrix(args: argparse.Namespace) -> int:
    bench_root = BENCH_ROOT
    results_dir: Path = args.results_dir
    results_dir.mkdir(parents=True, exist_ok=True)
    maybe_clear_aggregate_csvs(results_dir, args.clear_results)

    if args.skip_gt != "1":
        print("Pre-computing ground truth...", file=sys.stderr)
        subprocess.run(
            [
                sys.executable,
                str(bench_root / "ground_truth" / "run_gt.py"),
                "--query", "all",
                "--day", "all",
                "--out-dir", str(results_dir / "ground_truth"),
            ],
            check=True,
            cwd=str(bench_root),
        )

    controller = os.environ.get("CONTROLLER", "http://localhost:8080")
    api_port = parse_controller_api_port(controller)
    kill_process_on_tcp_port(api_port)

    controller_bin = _env_path(
        "CONTROLLER_BIN",
        REPO_ROOT / "controller" / "target" / "release" / "controller",
    )
    auto_start = os.environ.get("AUTO_START_CONTROLLER", "1") == "1"
    ctrl_pid = start_controller_if_needed(controller, controller_bin, auto_start, results_dir)

    queries = args.queries
    days = args.days
    total = 0
    n_pass = 0
    n_fail = 0
    failed: list[str] = []

    try:
        for query in queries:
            for day in days:
                total += 1
                sk = sketch_for_matrix_cell(
                    query,
                    args.sketch_quantile,
                    args.sketch_freq,
                    args.sketch_card,
                )
                print(f"[{total}] QUERY={query} DAY={day} SKETCH={sk}", file=sys.stderr)
                rc = _run_one_query_day(
                    bench_root=bench_root,
                    results_dir=results_dir,
                    query=query,
                    day=day,
                    days=day,
                    mode=args.mode,
                    speed=args.speed,
                    batch_size=args.batch_size,
                    sketch=sk,
                    collector_override=args.collector,
                )
                if rc == 0:
                    n_pass += 1
                    print(f"DONE [{query}/{day}]", file=sys.stderr)
                else:
                    n_fail += 1
                    failed.append(f"{query}/{day}(exit={rc})")
                    print(f"FAILED [{query}/{day}] exit={rc}", file=sys.stderr)
                print("", file=sys.stderr)
    finally:
        stop_controller(ctrl_pid)

    print(f"total: {total}  pass: {n_pass}  fail: {n_fail}", file=sys.stderr)
    if failed:
        print("Failed runs:", file=sys.stderr)
        for f in failed:
            print(f"  {f}", file=sys.stderr)
        return 1
    return 0


def run_crosskey(args: argparse.Namespace) -> int:
    """Cross-key roll-up accuracy.

    Reads the per-symbol sketch output and ground-truth CSVs already produced
    by a prior ``run.py test`` run, rolls them up by each requested grouping,
    and emits one row per grouping into ``results/crosskey/<Q>_<day>.csv``,
    plus a 'Cross-key merging accuracy' section in ``results/report.md``.
    """
    bench_root = BENCH_ROOT
    results_dir: Path = args.results_dir
    results_dir.mkdir(parents=True, exist_ok=True)

    query = args.query
    day = args.day
    day_tag = day.replace(".csv", "").replace("debs2022-gc-trading-day-", "")

    sketch_csv = results_dir / "sketch_output" / query / f"{day_tag}.csv"
    gt_csv = results_dir / "ground_truth" / query / f"{day_tag}.csv"

    if not sketch_csv.is_file():
        print(
            f"crosskey: sketch CSV missing: {sketch_csv}\n"
            f"  Run `run.py test --query {query} --day {day}` first, "
            f"or pass --synthesize-smoke to generate a 10-row stub.",
            file=sys.stderr,
        )
        if args.synthesize_smoke:
            sketch_csv.parent.mkdir(parents=True, exist_ok=True)
            _write_smoke_sketch_csv(sketch_csv)
            print(f"crosskey: wrote synthetic smoke CSV -> {sketch_csv}", file=sys.stderr)
        else:
            return 1

    if not gt_csv.is_file() and args.synthesize_smoke:
        gt_csv.parent.mkdir(parents=True, exist_ok=True)
        _write_smoke_gt_csv(gt_csv)
        print(f"crosskey: wrote synthetic smoke GT -> {gt_csv}", file=sys.stderr)

    sketch_df = load_sketch_per_symbol(sketch_csv, query)
    gt_df = load_ground_truth_per_symbol(gt_csv, query)

    if sketch_df.empty:
        print(f"crosskey: no usable sketch rows in {sketch_csv}", file=sys.stderr)
        return 2
    if gt_df.empty:
        print(
            f"crosskey: no ground truth at {gt_csv}; using sketch values as self-reference. "
            "Errors will be 0; this is only useful for smoke testing the script flow.",
            file=sys.stderr,
        )
        gt_df = sketch_df.copy()

    groupings = parse_grouping_list(args.groupings)
    rows = []
    for label, fn in groupings:
        rows.append(
            evaluate_grouping(
                query=query,
                day=day_tag,
                mode=args.mode,
                grouping_label=label,
                group_fn=fn,
                sketch_df=sketch_df,
                gt_df=gt_df,
            )
        )

    out_csv = results_dir / "crosskey" / f"{query}_{day_tag}.csv"
    write_crosskey_csv(rows, out_csv)
    print(f"crosskey: wrote {len(rows)} row(s) -> {out_csv}", file=sys.stderr)

    if args.update_report:
        report_md = results_dir / "report.md"
        append_report_section(rows, report_md)
        print(f"crosskey: appended section to {report_md}", file=sys.stderr)

    for r in rows:
        print(
            f"  {r.grouping:<14} n_groups={r.n_groups:<5} fan_in_avg={r.fan_in_avg:.2f} "
            f"rel_err_p50={r.rel_err_p50:.4g} rel_err_p99={r.rel_err_p99:.4g} "
            f"mode={r.mode}{' note=' + r.note if r.note else ''}",
            file=sys.stderr,
        )

    return 0


def _write_smoke_sketch_csv(path: Path) -> None:
    """Write a 10-row synthetic Prometheus-style sketch CSV for smoke testing."""
    import csv as _csv
    fieldnames = ["scrape_wall_ns", "metric", "labels", "value"]
    rows = [
        ("RDSA",  101.5, 12),
        ("RDSB",  100.9, 11),
        ("BP",     53.2, 14),
        ("ASML",  605.0,  9),
        ("SAP",   123.4,  8),
        ("INGA",   12.1, 20),
        ("ABN",    11.8, 18),
        ("AD",     27.5,  7),
        ("AIR",    99.0,  6),
        ("ZZZZ",   42.0,  3),  # no sector mapping -> OTHER
    ]
    wall = "1731000000000000000"
    with open(path, "w", newline="", encoding="utf-8") as fp:
        w = _csv.DictWriter(fp, fieldnames=fieldnames)
        w.writeheader()
        for sym, v, n in rows:
            labels = json.dumps({"symbol": sym, "ddsketch.quantile": "0.5", "count": str(n)},
                                sort_keys=True)
            w.writerow({"scrape_wall_ns": wall,
                        "metric": "financial_last_trade_price_ddsketch",
                        "labels": labels,
                        "value": f"{v}"})


def _write_smoke_gt_csv(path: Path) -> None:
    import csv as _csv
    fieldnames = ["window_start_ms", "symbol", "median", "count"]
    rows = [
        ("RDSA",  101.4, 12),
        ("RDSB",  100.7, 11),
        ("BP",     53.0, 14),
        ("ASML",  604.5,  9),
        ("SAP",   123.0,  8),
        ("INGA",   12.0, 20),
        ("ABN",    11.7, 18),
        ("AD",     27.4,  7),
        ("AIR",    98.8,  6),
        ("ZZZZ",   41.5,  3),
    ]
    with open(path, "w", newline="", encoding="utf-8") as fp:
        w = _csv.DictWriter(fp, fieldnames=fieldnames)
        w.writeheader()
        for sym, v, n in rows:
            w.writerow({"window_start_ms": "0", "symbol": sym, "median": v, "count": n})


def main() -> None:
    parser = argparse.ArgumentParser(description="DEBS benchmark orchestration.")
    sub = parser.add_subparsers(dest="command", required=True)

    p_test = sub.add_parser("test", help="Single benchmark run.")
    p_test.add_argument("--query", default=os.environ.get("QUERY", "Q1"))
    p_test.add_argument("--day", default=os.environ.get("DAY", "08-11-21"))
    p_test.add_argument("--days", default=os.environ.get("DAYS", ""), help="Comma-separated days; defaults to --day.")
    p_test.add_argument("--mode", default=os.environ.get("MODE", "sketch-finance"))
    p_test.add_argument("--speed", type=float, default=float(os.environ.get("SPEED", "100")))
    p_test.add_argument("--batch-size", type=int, default=int(os.environ.get("BATCH_SIZE", "5000")))
    p_test.add_argument("--accuracy-minutes", type=int, default=0,
                        help="Minutes of event time to replay (0 = full day).")
    p_test.add_argument("--skip-gt", default=os.environ.get("SKIP_GT", "0"))
    p_test.add_argument("--sketch", default=os.environ.get("SKETCH", ""))
    p_test.add_argument("--collector", default=os.environ.get("COLLECTOR", "") or None)
    p_test.add_argument("--results-dir", type=Path, default=BENCH_ROOT / "results")
    p_test.add_argument("--clear-results", action="store_true",
                        help="Remove throughput.csv and latency.csv before this run.")

    p_matrix = sub.add_parser("matrix", help="Full matrix run.")
    p_matrix.add_argument("--queries", nargs="+", default=list(DEFAULT_QUERIES))
    p_matrix.add_argument("--days", nargs="+", default=list(DEFAULT_DAYS))
    p_matrix.add_argument("--mode", default=os.environ.get("MODE", "sketch-finance"))
    p_matrix.add_argument("--speed", type=float, default=float(os.environ.get("SPEED", "100")))
    p_matrix.add_argument("--batch-size", type=int, default=int(os.environ.get("BATCH_SIZE", "5000")))
    p_matrix.add_argument("--skip-gt", default=os.environ.get("SKIP_GT", "1"))
    p_matrix.add_argument("--sketch-quantile", default=os.environ.get("SKETCH_QUANTILE", "ddsketch"))
    p_matrix.add_argument("--sketch-freq", default=os.environ.get("SKETCH_FREQ", "countsketch"))
    p_matrix.add_argument("--sketch-card", default=os.environ.get("SKETCH_CARD", "hll"))
    p_matrix.add_argument("--collector", default=os.environ.get("COLLECTOR", "") or None)
    p_matrix.add_argument("--results-dir", type=Path, default=BENCH_ROOT / "results")
    p_matrix.add_argument("--clear-results", action="store_true")

    p_crosskey = sub.add_parser(
        "crosskey",
        help="Cross-key roll-up accuracy (per-symbol -> sector -> all).",
    )
    p_crosskey.add_argument("--query", default=os.environ.get("QUERY", "Q5"))
    p_crosskey.add_argument("--day", default=os.environ.get("DAY", "08-11-21"))
    p_crosskey.add_argument(
        "--groupings",
        default="per_symbol,per_sector,all,random_8,random_32",
        help="Comma-separated groupings: per_symbol, per_sector, all, random_<N>.",
    )
    p_crosskey.add_argument(
        "--mode",
        choices=("sketch_merge", "point_rollup"),
        default="point_rollup",
        help="Roll-up mode. sketch_merge requires serialized sketch payloads in "
             "Prometheus labels; otherwise it falls back to point_rollup.",
    )
    p_crosskey.add_argument("--results-dir", type=Path, default=BENCH_ROOT / "results")
    p_crosskey.add_argument(
        "--update-report",
        action="store_true",
        default=True,
        help="Append a 'Cross-key merging accuracy' section to results/report.md.",
    )
    p_crosskey.add_argument(
        "--no-update-report",
        action="store_false",
        dest="update_report",
        help="Skip writing to report.md.",
    )
    p_crosskey.add_argument(
        "--synthesize-smoke",
        action="store_true",
        help="If sketch/GT CSVs are missing, write a 10-row synthetic stub for testing.",
    )

    args = parser.parse_args()
    if args.command == "test":
        sys.exit(run_benchmark_test(args))
    if args.command == "matrix":
        sys.exit(run_matrix(args))
    if args.command == "crosskey":
        sys.exit(run_crosskey(args))


if __name__ == "__main__":
    main()
