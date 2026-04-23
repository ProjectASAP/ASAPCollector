from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import requests

BENCH_ROOT = Path(__file__).resolve().parent
REPO_ROOT   = Path(__file__).resolve().parent.parent.parent.parent

if str(BENCH_ROOT) not in sys.path:
    sys.path.insert(0, str(BENCH_ROOT))

from common import (
    DEFAULT_QUERIES,
    DEFAULT_SLICES,
    NOP_QUERIES,
    PROMETHEUS_METRICS_URL,
    QUERY_CONFIG,
    build_plan_body,
    nop_config_path,
    parse_controller_api_port,
    resolve_collector_bin,
    sketch_for_matrix_cell,
    sketch_type_for_plan,
    time_window_for_query,
)
from scrape import append_snapshot


def _env_path(key: str, default: Path) -> Path:
    v = os.environ.get(key)
    return Path(v).expanduser() if v else default


def controller_ready(url: str) -> bool:
    try:
        r = requests.get(f"{url.rstrip('/')}/api/v1/agents", timeout=2)
        return r.status_code == 200
    except requests.RequestException:
        return False


def start_controller_if_needed(
    controller: str,
    controller_bin: Path,
    auto_start: bool,
    results_dir: Path,
) -> int | None:
    if controller_ready(controller):
        return None
    if not auto_start:
        print("Controller not reachable; set AUTO_START_CONTROLLER=1 or start manually.",
              file=sys.stderr)
        sys.exit(1)
    parsed = urlparse(controller)
    host = (parsed.hostname or "localhost").lower()
    if host not in ("localhost", "127.0.0.1", "::1"):
        print("Controller auto-start only supports localhost.", file=sys.stderr)
        sys.exit(1)
    if not controller_bin.is_file() or not os.access(controller_bin, os.X_OK):
        print(f"Controller binary missing or not executable: {controller_bin}", file=sys.stderr)
        sys.exit(1)
    api_port  = parse_controller_api_port(controller)
    opamp_port = int(os.environ.get("CONTROLLER_OPAMP_PORT", "4320"))
    results_dir.mkdir(parents=True, exist_ok=True)
    log_path = results_dir / "controller.log"
    env = os.environ.copy()
    env["CONTROLLER_ADDR"]           = f"0.0.0.0:{api_port}"
    env["CONTROLLER_OPAMP_ADDR"]     = f"0.0.0.0:{opamp_port}"
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


def post_plan(controller: str, body: dict) -> None:
    url = f"{controller.rstrip('/')}/api/v1/plan"
    r = requests.post(url, json=body, timeout=60)
    r.raise_for_status()
    print(r.text[:2000], file=sys.stderr)


def wait_for_prometheus(url: str, timeout_s: float = 30.0) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            if requests.get(url, timeout=2).status_code == 200:
                return True
        except requests.RequestException:
            pass
        time.sleep(0.25)
    return False


def kill_on_port(port: int) -> None:
    try:
        out = subprocess.check_output(["lsof", "-ti", f"tcp:{port}"],
                                      stderr=subprocess.DEVNULL, text=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        return
    for pid in out.strip().split():
        try:
            subprocess.run(["kill", pid], check=False)
        except OSError:
            pass
    time.sleep(1)


def replay_mode_arg(mode: str) -> str:
    return "max" if mode == "throughput" else "paced"


def _run_one(
    bench_root: Path,
    results_dir: Path,
    query: str,
    slice_tag: str,
    mode: str,
    speed: float,
    batch_size: int,
    sketch: str,
    collector_override: str | None,
) -> int:
    controller = os.environ.get("CONTROLLER", "http://localhost:8080")
    metric     = os.environ.get("METRIC", QUERY_CONFIG[query].metric_name)

    collector_bin = resolve_collector_bin(query, sketch, collector_override)
    if not collector_bin.is_file() or not os.access(collector_bin, os.X_OK):
        print(f"Collector binary missing: {collector_bin}", file=sys.stderr)
        return 1

    metrics_port = int(urlparse(PROMETHEUS_METRICS_URL).port or 8889)
    kill_on_port(metrics_port)

    is_nop = query in NOP_QUERIES
    py = sys.executable
    cpid: subprocess.Popen | None = None
    try:
        if not is_nop:
            post_plan(controller, build_plan_body(metric, query,
                                                  sketch if sketch and sketch != "nop" else None))
            cpid = subprocess.Popen(
                [str(collector_bin),
                 f"--config={controller.rstrip('/')}/api/v1/config/{metric}"],
                stdout=open(results_dir / "collector.log", "wb"),
                stderr=subprocess.STDOUT,
                cwd=str(bench_root),
            )
        else:
            nop_cfg = nop_config_path(collector_bin)
            if not nop_cfg.is_file():
                print(f"NOP config not found: {nop_cfg}", file=sys.stderr)
                return 1
            cpid = subprocess.Popen(
                [str(collector_bin), f"--config={nop_cfg}"],
                stdout=open(results_dir / "collector.log", "wb"),
                stderr=subprocess.STDOUT,
                cwd=str(bench_root),
            )

        if not is_nop:
            if not wait_for_prometheus(PROMETHEUS_METRICS_URL, 30.0):
                print(f"Warning: {PROMETHEUS_METRICS_URL} not reachable within 30s.",
                      file=sys.stderr)
        else:
            time.sleep(3)

        scrape_proc = subprocess.Popen([
            py, str(bench_root / "scrape.py"),
            "--query", query,
            "--slice", slice_tag,
            "--out-dir", str(results_dir / "sketch_output"),
            "--url", PROMETHEUS_METRICS_URL,
            "--duration", "7200",
        ] + (["--no-probe"] if is_nop else []),
            cwd=str(bench_root),
        )

        replay_argv = [
            py, str(bench_root / "replay.py"),
            "--query", query,
            "--slice", slice_tag,
            "--mode", replay_mode_arg(mode),
            "--speed-factor", str(speed),
            "--batch-size", str(batch_size),
            "--results-dir", str(results_dir),
        ]
        subprocess.run(replay_argv, check=True, cwd=str(bench_root))

        scrape_proc.send_signal(signal.SIGTERM)
        try:
            scrape_proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            scrape_proc.kill()

        if not is_nop:
            out_csv = results_dir / "sketch_output" / query / f"{slice_tag}.csv"
            try:
                n = append_snapshot(PROMETHEUS_METRICS_URL, out_csv)
                print(f"Final Prometheus snapshot: {n} lines -> {out_csv}", file=sys.stderr)
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

        subprocess.run([
            py, str(bench_root / "compare.py"),
            "--query", query,
            "--slice", slice_tag,
            "--gt-dir",     str(results_dir / "ground_truth"),
            "--sketch-dir", str(results_dir / "sketch_output"),
            "--out-dir",    str(results_dir / "comparison"),
        ], check=True, cwd=str(bench_root))

        subprocess.run([
            py, str(bench_root / "analyze.py"),
            "--results-dir", str(results_dir),
            "--query", query,
            "--slice", slice_tag,
            "--replay-mode", mode,
        ], check=True, cwd=str(bench_root))

    except subprocess.CalledProcessError as e:
        print(f"Command failed: {e}", file=sys.stderr)
        return e.returncode or 1
    finally:
        if cpid is not None and cpid.poll() is None:
            cpid.kill()

    return 0


def _maybe_clear_csvs(results_dir: Path, clear: bool) -> None:
    if not clear:
        return
    for name in ("throughput.csv", "latency.csv"):
        p = results_dir / name
        if p.is_file():
            p.unlink()


def run_test(args: argparse.Namespace) -> int:
    results_dir: Path = args.results_dir
    results_dir.mkdir(parents=True, exist_ok=True)
    _maybe_clear_csvs(results_dir, args.clear_results)

    controller    = os.environ.get("CONTROLLER", "http://localhost:8080")
    controller_bin = _env_path(
        "CONTROLLER_BIN",
        REPO_ROOT / "controller" / "target" / "release" / "controller",
    )
    auto_start = os.environ.get("AUTO_START_CONTROLLER", "1") == "1"
    ctrl_pid   = start_controller_if_needed(controller, controller_bin, auto_start, results_dir)

    query     = args.query
    slice_tag = args.slice
    sketch    = args.sketch or ""

    if args.skip_gt != "1":
        try:
            subprocess.run([
                sys.executable,
                str(BENCH_ROOT / "ground_truth" / "run_gt.py"),
                "--query",  query,
                "--slice",  slice_tag,
                "--out-dir", str(results_dir / "ground_truth"),
            ], check=True)
        except subprocess.CalledProcessError as e:
            stop_controller(ctrl_pid)
            return e.returncode or 1
    else:
        print(f"skip_gt=1: skipping ground truth for {query}/{slice_tag}", file=sys.stderr)

    try:
        rc = _run_one(
            bench_root=BENCH_ROOT,
            results_dir=results_dir,
            query=query,
            slice_tag=slice_tag,
            mode=args.mode,
            speed=args.speed,
            batch_size=args.batch_size,
            sketch=sketch,
            collector_override=args.collector,
        )
    finally:
        stop_controller(ctrl_pid)

    return rc


def run_matrix(args: argparse.Namespace) -> int:
    results_dir: Path = args.results_dir
    results_dir.mkdir(parents=True, exist_ok=True)
    _maybe_clear_csvs(results_dir, args.clear_results)

    if args.skip_gt != "1":
        print("Pre-computing ground truth...", file=sys.stderr)
        subprocess.run([
            sys.executable,
            str(BENCH_ROOT / "ground_truth" / "run_gt.py"),
            "--query", "all",
            "--slice", "all",
            "--out-dir", str(results_dir / "ground_truth"),
        ], check=True, cwd=str(BENCH_ROOT))

    controller = os.environ.get("CONTROLLER", "http://localhost:8080")
    kill_on_port(parse_controller_api_port(controller))

    controller_bin = _env_path(
        "CONTROLLER_BIN",
        REPO_ROOT / "controller" / "target" / "release" / "controller",
    )
    auto_start = os.environ.get("AUTO_START_CONTROLLER", "1") == "1"
    ctrl_pid   = start_controller_if_needed(controller, controller_bin, auto_start, results_dir)

    queries = args.queries
    slices  = args.slices
    total = n_pass = n_fail = 0
    failed: list[str] = []

    try:
        for query in queries:
            for sl in slices:
                total += 1
                sk = sketch_for_matrix_cell(
                    query, args.sketch_quantile, args.sketch_freq, args.sketch_card,
                )
                print(f"[{total}] QUERY={query} SLICE={sl} SKETCH={sk}", file=sys.stderr)
                rc = _run_one(
                    bench_root=BENCH_ROOT,
                    results_dir=results_dir,
                    query=query,
                    slice_tag=sl,
                    mode=args.mode,
                    speed=args.speed,
                    batch_size=args.batch_size,
                    sketch=sk,
                    collector_override=args.collector,
                )
                if rc == 0:
                    n_pass += 1
                    print(f"DONE [{query}/{sl}]", file=sys.stderr)
                else:
                    n_fail += 1
                    failed.append(f"{query}/{sl}(exit={rc})")
                    print(f"FAILED [{query}/{sl}] exit={rc}", file=sys.stderr)
                print("", file=sys.stderr)
    finally:
        stop_controller(ctrl_pid)

    print(f"total: {total}  pass: {n_pass}  fail: {n_fail}", file=sys.stderr)
    if failed:
        for f in failed:
            print(f"  {f}", file=sys.stderr)
        return 1
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Snowset benchmark orchestration.")
    sub = parser.add_subparsers(dest="command", required=True)

    p_test = sub.add_parser("test", help="Single benchmark run.")
    p_test.add_argument("--query",  default=os.environ.get("QUERY", "Q1"),
                        choices=list(DEFAULT_QUERIES))
    p_test.add_argument("--slice",  default=os.environ.get("SLICE", "full"),
                        help="Dataset slice tag (default: full).")
    p_test.add_argument("--mode",   default=os.environ.get("MODE", "sketch-snowset"))
    p_test.add_argument("--speed",  type=float,
                        default=float(os.environ.get("SPEED", "1000")))
    p_test.add_argument("--batch-size", type=int,
                        default=int(os.environ.get("BATCH_SIZE", "5000")))
    p_test.add_argument("--skip-gt", default=os.environ.get("SKIP_GT", "0"))
    p_test.add_argument("--sketch", default=os.environ.get("SKETCH", ""))
    p_test.add_argument("--collector", default=os.environ.get("COLLECTOR", "") or None)
    p_test.add_argument("--results-dir", type=Path, default=BENCH_ROOT / "results")
    p_test.add_argument("--clear-results", action="store_true")

    p_matrix = sub.add_parser("matrix", help="Full matrix run.")
    p_matrix.add_argument("--queries", nargs="+", default=list(DEFAULT_QUERIES))
    p_matrix.add_argument("--slices",  nargs="+", default=list(DEFAULT_SLICES))
    p_matrix.add_argument("--mode",    default=os.environ.get("MODE", "sketch-snowset"))
    p_matrix.add_argument("--speed",   type=float,
                          default=float(os.environ.get("SPEED", "1000")))
    p_matrix.add_argument("--batch-size", type=int,
                          default=int(os.environ.get("BATCH_SIZE", "5000")))
    p_matrix.add_argument("--skip-gt", default=os.environ.get("SKIP_GT", "1"))
    p_matrix.add_argument("--sketch-quantile",
                          default=os.environ.get("SKETCH_QUANTILE", "ddsketch"))
    p_matrix.add_argument("--sketch-freq",
                          default=os.environ.get("SKETCH_FREQ", "countsketch"))
    p_matrix.add_argument("--sketch-card",
                          default=os.environ.get("SKETCH_CARD", "hll"))
    p_matrix.add_argument("--collector", default=os.environ.get("COLLECTOR", "") or None)
    p_matrix.add_argument("--results-dir", type=Path, default=BENCH_ROOT / "results")
    p_matrix.add_argument("--clear-results", action="store_true")

    args = parser.parse_args()
    if args.command == "test":
        sys.exit(run_test(args))
    if args.command == "matrix":
        sys.exit(run_matrix(args))


if __name__ == "__main__":
    main()
