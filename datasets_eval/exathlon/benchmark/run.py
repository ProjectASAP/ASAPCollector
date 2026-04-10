from __future__ import annotations

"""Main orchestration for the Exathlon benchmark.

Subcommands:
  test    — single (query, file) run
  matrix  — full matrix: all queries × all files
"""

import argparse
import os
import signal
import shutil
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

from scrape import append_metrics_snapshot, fetch_prometheus_metrics_text
from common import DEFAULT_FILES, file_tag_safe, METRIC_NAME

PATCH_CMD = REPO_ROOT / "opentelemetry-collector-contrib-patch" / "cmd"
CONTROLLER_SKETCH_DEFAULTS = REPO_ROOT / "controller" / "sketch_params_default.yml"

DEFAULT_COLLECTOR_PATHS = {
    "ddsketch": PATCH_CMD / "ddsketchcol" / "ddsketchcol",
    "kll": PATCH_CMD / "kll" / "KLL",
    # HLL's builder config writes the binary at the contrib-patch repo root.
    "hll": REPO_ROOT / "opentelemetry-collector-contrib-patch" / "HLL",
    "countsketch": PATCH_CMD / "countsketchcol" / "dist" / "countsketchcol",
    "countminsketch": PATCH_CMD / "countminsketchcol" / "dist" / "countminsketchcol",
    "nop": PATCH_CMD / "nopcol" / "dist" / "nopcol",
}

PROMETHEUS_METRICS_URL = os.environ.get("PROMETHEUS_METRICS_URL", "http://localhost:8889/metrics")

# ---------------------------------------------------------------------------
# Query configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class QueryCfg:
    """Per-query benchmark configuration."""
    aggregations: tuple[str, ...]
    time_window: str
    group_by: tuple[str, ...]
    sketch_family: str  # "quantile" | "frequency" | "cardinality" | "nop"


QUERY_CONFIG: dict[str, QueryCfg] = {
    "Q1": QueryCfg(
        aggregations=("p50", "p95", "p99"),
        time_window="5m",
        group_by=("entity", "metric_base"),
        sketch_family="quantile",
    ),
    "Q2": QueryCfg(
        aggregations=(),
        time_window="5m",
        group_by=("entity", "metric_base"),
        sketch_family="nop",
    ),
    "Q3": QueryCfg(
        aggregations=("count",),
        time_window="5m",
        group_by=("entity", "metric_base", "aggregation"),
        sketch_family="frequency",
    ),
    "Q4": QueryCfg(
        aggregations=("p0", "p100"),
        time_window="5m",
        group_by=("entity", "metric_base"),
        sketch_family="quantile",
    ),
    "Q5": QueryCfg(
        aggregations=("p25", "p50", "p75"),
        time_window="5m",
        group_by=("entity", "metric_base"),
        sketch_family="quantile",
    ),
    "Q6": QueryCfg(
        aggregations=("cardinality",),
        time_window="5m",
        group_by=("entity",),
        sketch_family="cardinality",
    ),
    "Q7": QueryCfg(
        aggregations=("count",),
        time_window="5m",
        group_by=("entity", "window_start_s"),
        sketch_family="frequency",
    ),
    "Q8": QueryCfg(
        aggregations=("p95",),
        time_window="5m",
        group_by=("entity", "metric_base"),
        sketch_family="quantile",
    ),
    "Q9": QueryCfg(
        aggregations=("cardinality",),
        time_window="5m",
        group_by=("entity",),
        sketch_family="cardinality",
    ),
    "Q10": QueryCfg(
        aggregations=(),
        time_window="5m",
        group_by=("entity", "metric_base"),
        sketch_family="nop",
    ),
    "Q11": QueryCfg(
        aggregations=(),
        time_window="15m",
        group_by=("entity", "metric_base"),
        sketch_family="nop",
    ),
    "Q12": QueryCfg(
        aggregations=(),
        time_window="5m",
        group_by=("entity",),
        sketch_family="nop",
    ),
}

NOP_QUERIES = frozenset(q for q, c in QUERY_CONFIG.items() if c.sketch_family == "nop")
DEFAULT_QUERIES = tuple(QUERY_CONFIG)

# Controller latency SLA forcing batch-mode output (strictly below shortest window = 5m).
BENCH_LATENCY_SLA_FOR_BATCH_MODE = "30s"

# Queries that have no offline ground truth (throughput / latency only).
NO_GT_QUERIES = frozenset({"Q2", "Q10", "Q11", "Q12"})


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

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
    if not s:
        # Let controller planner choose defaults unless user explicitly pins one.
        return None
    family = QUERY_CONFIG[query].sketch_family
    if family == "frequency":
        return "countminsketch" if s == "countminsketch" else "countsketch"
    if family == "cardinality":
        return "hll"
    if family == "quantile":
        return "kll" if s == "kll" else "ddsketch"
    return None


def replay_mode_for_run(mode: str) -> str:
    """Translate benchmark-level mode names into replay.py mode values."""
    m = (mode or "").strip().lower()
    if m == "sketch-telemetry":
        return "paced"
    if m == "throughput":
        return "max"
    if m in ("max", "paced", "scaled"):
        return m
    raise ValueError(
        f"unsupported mode {mode!r}; use sketch-telemetry/throughput or max/paced/scaled"
    )


def build_plan_body(
    metric: str,
    query: str,
    sketch: str | None,
    run_mode: str,
) -> dict[str, Any]:
    cfg = QUERY_CONFIG[query]
    # Controller API accepts aggregation families, not percentile labels.
    if cfg.sketch_family == "quantile":
        aggregations = ["quantile"]
    elif cfg.sketch_family == "frequency":
        aggregations = ["frequency"]
    elif cfg.sketch_family == "cardinality":
        aggregations = ["cardinality"]
    else:
        aggregations = list(cfg.aggregations)
    body: dict[str, Any] = {
        "metric_name": metric,
        "aggregations": aggregations,
        "time_window": cfg.time_window,
        "group_by_labels": list(cfg.group_by),
        "accuracy_sla": 0.01,
        "workload": {
            "series_count": 4000,
            "samples_per_sec_per_series": 1,
            "bytes_per_raw_sample": 64,
            "data_distribution": "zipf",
        },
    }
    # Explicitly request the quantile values required by each query so that
    # the controller forwards them to the collector config.  Without this the
    # DDSketch collector falls back to its hardcoded default grid, which omits
    # p95 and causes frac_q95_lt_1pct to be NaN in compare.py.
    if cfg.sketch_family == "quantile":
        quantiles_needed = sorted(
            {
                float(p.lstrip("p")) / 100
                for p in cfg.aggregations
                if p.startswith("p") and p[1:].isdigit()
            }
        )
        if quantiles_needed:
            body["quantile_grid"] = quantiles_needed
    # Force batch-mode only for throughput-oriented runs.
    if replay_mode_for_run(run_mode) == "max":
        body["latency_sla"] = BENCH_LATENCY_SLA_FOR_BATCH_MODE
    st = sketch_type_for_plan(query, sketch)
    if st is not None:
        body["sketch_type"] = st
    return body


# ---------------------------------------------------------------------------
# Controller lifecycle
# ---------------------------------------------------------------------------

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
    return 443 if parsed.scheme == "https" else 8080


def start_controller_if_needed(
    controller: str,
    controller_bin: Path,
    auto_start: bool,
    results_dir: Path,
) -> int | None:
    if controller_ready(controller):
        return None
    if not auto_start:
        print("Controller not reachable; set AUTO_START_CONTROLLER=1 or start manually.", file=sys.stderr)
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
    env["CONTROLLER_SKETCH_DEFAULTS"] = str(CONTROLLER_SKETCH_DEFAULTS)
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
    if not r.ok:
        print(f"POST {url} failed: {r.status_code}", file=sys.stderr)
        print(r.text[:4000], file=sys.stderr)
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


def _parse_time_window_seconds(time_window: str) -> int:
    """Parse a time window string such as '5m', '15m', '1h', '300s' to seconds."""
    tw = time_window.strip().lower()
    if tw.endswith("m"):
        return int(tw[:-1]) * 60
    if tw.endswith("h"):
        return int(tw[:-1]) * 3600
    if tw.endswith("s"):
        return int(tw[:-1])
    return int(tw)


def _wait_for_frequency_flush(
    prometheus_url: str,
    timeout_s: float,
    poll_interval_s: float = 2.0,
) -> bool:
    """Poll the Prometheus endpoint until a CMS window-flush row appears.

    A flush row is identified by the presence of a ``sample_count`` label,
    which the CMS processor always attaches to its output data points.
    Returns True when a flush is detected, False when the timeout expires.

    Background: for Q7 the paced replay sends all anomaly events in a single
    batch at the *end* of the pacing sleep (when the last event's wall-clock
    deadline is reached).  The CMS window boundary may therefore not occur
    until up to ``window_duration`` seconds after the batch is sent, which is
    *after* the replay subprocess exits.  Keeping the scraper alive until this
    function returns ensures the flush is captured in the sketch-output CSV.
    """
    import json as _json
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            rows = fetch_prometheus_metrics_text(prometheus_url)
            for row in rows:
                try:
                    labels = _json.loads(row.get("labels", "{}"))
                except Exception:
                    continue
                if "sample_count" in labels:
                    return True
        except Exception:
            pass
        remaining = deadline - time.time()
        if remaining <= 0:
            break
        time.sleep(min(poll_interval_s, remaining))
    return False


def kill_process_on_tcp_port(port: int) -> None:
    try:
        out = subprocess.check_output(
            ["lsof", "-ti", f"tcp:{port}"], stderr=subprocess.DEVNULL, text=True
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return
    for pid in out.strip().split():
        try:
            subprocess.run(["kill", pid], check=False)
        except OSError:
            pass
    deadline = time.time() + 3
    while time.time() < deadline:
        try:
            remaining = subprocess.check_output(
                ["lsof", "-ti", f"tcp:{port}"], stderr=subprocess.DEVNULL, text=True
            ).strip().split()
        except (subprocess.CalledProcessError, FileNotFoundError):
            return
        if not remaining:
            return
        time.sleep(0.25)

    for pid in remaining:
        try:
            subprocess.run(["kill", "-9", pid], check=False)
        except OSError:
            pass
    time.sleep(0.5)


def maybe_clear_aggregate_csvs(results_dir: Path, clear: bool) -> None:
    if not clear:
        return
    for name in ("throughput.csv", "latency.csv"):
        p = results_dir / name
        if p.is_file():
            p.unlink()


def per_run_send_times_path(results_dir: Path, query: str, file_tag: str) -> Path:
    tag = file_tag_safe(file_tag)
    out_dir = results_dir / "send_times"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"{query}_{tag}.csv"


# ---------------------------------------------------------------------------
# Core per-(query, file) runner
# ---------------------------------------------------------------------------

def _run_one_query_file(
    bench_root: Path,
    results_dir: Path,
    query: str,
    file_tag: str,
    files: str,
    mode: str,
    speed: float,
    batch_size: int,
    sketch: str,
    collector_override: str | None,
    accuracy_minutes: int = 0,
) -> int:
    """Run one (query, file_tag) cell.  Returns 0 on success, non-zero on failure."""
    controller = os.environ.get("CONTROLLER", "http://localhost:8080")
    metric = os.environ.get("METRIC", METRIC_NAME)
    collector_bin = resolve_collector_bin(query, sketch, collector_override)
    if not collector_bin.is_file() or not os.access(collector_bin, os.X_OK):
        print(f"Collector binary missing or not executable: {collector_bin}", file=sys.stderr)
        return 1

    metrics_port = int(urlparse(PROMETHEUS_METRICS_URL).port or 8889)
    kill_process_on_tcp_port(metrics_port)

    is_nop = query in NOP_QUERIES
    py = sys.executable
    cpid: subprocess.Popen | None = None
    tag = file_tag_safe(file_tag)
    replay_mode = replay_mode_for_run(mode)
    send_times_run_path = per_run_send_times_path(results_dir, query, file_tag)
    if replay_mode == "paced" and abs(float(speed) - 1.0) > 1e-12:
        print(
            "warning: replay mode 'paced' ignores --speed; use --mode scaled to apply speed factor",
            file=sys.stderr,
        )
    if accuracy_minutes > 0:
        print(
            f"warning: --accuracy-minutes={accuracy_minutes} replays only a prefix of event time; "
            "accuracy may not be representative of full-file results",
            file=sys.stderr,
        )

    try:
        if not is_nop:
            post_plan(
                controller,
                build_plan_body(
                    metric,
                    query,
                    sketch if sketch and sketch != "nop" else None,
                    mode,
                ),
            )
            with open(results_dir / "collector.log", "wb") as logf:
                cpid = subprocess.Popen(
                    [
                        str(collector_bin),
                        f"--config={controller.rstrip('/')}/api/v1/config/{metric}",
                    ],
                    stdout=logf,
                    stderr=subprocess.STDOUT,
                    cwd=str(bench_root),
                )
        else:
            nop_cfg = nop_config_path(collector_bin)
            if not nop_cfg.is_file():
                print(f"NOP config not found: {nop_cfg}", file=sys.stderr)
                return 1
            print(f"NOP query {query}: using {nop_cfg}", file=sys.stderr)
            with open(results_dir / "collector.log", "wb") as logf:
                cpid = subprocess.Popen(
                    [str(collector_bin), f"--config={nop_cfg}"],
                    stdout=logf,
                    stderr=subprocess.STDOUT,
                    cwd=str(bench_root),
                )

        if not is_nop:
            if not wait_for_prometheus(PROMETHEUS_METRICS_URL, timeout_s=30.0):
                print(
                    f"Warning: {PROMETHEUS_METRICS_URL} not reachable within 30s.",
                    file=sys.stderr,
                )
        else:
            time.sleep(3)

        scrape_argv = [
            py, str(bench_root / "scrape.py"),
            "--query", query,
            "--file", file_tag,
            "--out-dir", str(results_dir / "sketch_output"),
            "--url", PROMETHEUS_METRICS_URL,
            "--duration", "7200",
        ]
        if is_nop:
            scrape_argv.append("--no-probe")
        scrape_proc = subprocess.Popen(scrape_argv, cwd=str(bench_root))

        # Q7 pre-computes per-window IQR fences and tags each anomaly event
        # with its event-time window via the window_start_s attribute.  Pacing
        # the replay to 1× wall-clock time causes the CMS to flush before later
        # batches are sent, leaving event-time windows beyond the first flush
        # uncaptured.  Since the CMS groups by (entity, window_start_s) rather
        # than by arrival time, pacing adds no semantic value: send all events
        # as fast as possible so the first 5-minute wall-clock window captures
        # every event-time window in one flush.
        effective_replay_mode = (
            "max" if query == "Q7" else replay_mode
        )
        replay_argv = [
            py, str(bench_root / "replay.py"),
            "--files", files,
            "--mode", effective_replay_mode,
            "--speed-factor", str(speed),
            "--batch-size", str(batch_size),
            "--results-dir", str(results_dir),
            "--query", query,
            "--file", file_tag,
        ]
        if accuracy_minutes > 0:
            replay_argv += ["--max-event-minutes", str(accuracy_minutes)]
        subprocess.run(replay_argv, check=True, cwd=str(bench_root))

        send_times_latest = results_dir / "send_times.csv"
        if send_times_latest.is_file():
            shutil.copy2(send_times_latest, send_times_run_path)

        # For frequency-family queries (Q3, Q7) the CMS window tick fires up to
        # ``window_duration`` seconds after the collector starts.  Q7 uses max
        # replay mode so all events arrive before the first tick; Q3 uses paced
        # mode where the batch may arrive just before a tick boundary.  In both
        # cases keep the collector and scraper alive until the flush is detected
        # (via a ``sample_count`` label on the Prometheus endpoint) or the
        # timeout expires.  Allow 2× the window duration to cover the worst
        # case where events arrive just after a tick and must wait a full extra
        # window before the next flush.
        # NOTE: this must happen *before* killing either process so the
        # Prometheus HTTP server stays available for both polling and the final
        # snapshot call below.
        if not is_nop and QUERY_CONFIG[query].sketch_family == "frequency":
            window_s = _parse_time_window_seconds(QUERY_CONFIG[query].time_window)
            flush_timeout = 2 * window_s + 30
            print(
                f"Frequency query {query}: waiting up to {flush_timeout}s "
                "for CMS window flush...",
                file=sys.stderr,
            )
            flushed = _wait_for_frequency_flush(
                PROMETHEUS_METRICS_URL,
                timeout_s=flush_timeout,
            )
            if flushed:
                print("CMS window flush detected.", file=sys.stderr)
            else:
                print(
                    "CMS window flush not detected within timeout; proceeding.",
                    file=sys.stderr,
                )

        if not is_nop:
            out_csv = results_dir / "sketch_output" / query / f"{tag}.csv"
            try:
                n = append_metrics_snapshot(PROMETHEUS_METRICS_URL, out_csv)
                print(f"Final Prometheus snapshot: {n} lines -> {out_csv}", file=sys.stderr)
            except Exception as exc:
                print(f"Final snapshot failed: {exc}", file=sys.stderr)

        scrape_proc.send_signal(signal.SIGTERM)
        try:
            scrape_proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            scrape_proc.kill()

        time.sleep(2)
        if cpid is not None:
            cpid.send_signal(signal.SIGTERM)
            try:
                cpid.wait(timeout=15)
            except subprocess.TimeoutExpired:
                cpid.kill()
            cpid = None

        compare_argv = [
            py, str(bench_root / "compare.py"),
            "--query", query,
            "--file", file_tag,
            "--gt-dir", str(results_dir / "ground_truth"),
            "--sketch-dir", str(results_dir / "sketch_output"),
            "--out-dir", str(results_dir / "comparison"),
            "--send-times", str(send_times_run_path),
        ]
        if accuracy_minutes > 0:
            compare_argv += ["--accuracy-minutes", str(accuracy_minutes)]
        subprocess.run(compare_argv, check=True, cwd=str(bench_root))
        subprocess.run(
            [
                py, str(bench_root / "analyze.py"),
                "--results-dir", str(results_dir),
                "--query", query,
                "--file", file_tag,
                "--replay-mode", mode,
                "--send-times", str(send_times_run_path),
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


# ---------------------------------------------------------------------------
# test subcommand
# ---------------------------------------------------------------------------

def run_benchmark_test(args: argparse.Namespace) -> int:
    bench_root = BENCH_ROOT
    results_dir: Path = args.results_dir
    results_dir.mkdir(parents=True, exist_ok=True)
    maybe_clear_aggregate_csvs(results_dir, args.clear_results)

    controller = os.environ.get("CONTROLLER", "http://localhost:8080")
    api_port = parse_controller_api_port(controller)
    opamp_port = int(os.environ.get("CONTROLLER_OPAMP_PORT", "4320"))
    if not controller_ready(controller) and os.environ.get("AUTO_START_CONTROLLER", "1") == "1":
        kill_process_on_tcp_port(api_port)
        kill_process_on_tcp_port(opamp_port)

    controller_bin = _env_path(
        "CONTROLLER_BIN",
        REPO_ROOT / "controller" / "target" / "release" / "controller",
    )
    auto_start = os.environ.get("AUTO_START_CONTROLLER", "1") == "1"
    ctrl_pid = start_controller_if_needed(controller, controller_bin, auto_start, results_dir)

    query = args.query
    file_tag = args.file
    files_raw = (getattr(args, "files", None) or "").strip()
    files = files_raw if files_raw else file_tag
    sketch = args.sketch or ""

    print(f"Collector: QUERY={query} SKETCH={sketch or '(default)'}", file=sys.stderr)

    if args.skip_gt != "1":
        if query in NO_GT_QUERIES:
            print(f"No offline ground truth for {query}", file=sys.stderr)
        else:
            try:
                subprocess.run(
                    [
                        sys.executable,
                        str(bench_root / "ground_truth" / "run_gt.py"),
                        "--query", query,
                        "--file", file_tag,
                        "--out-dir", str(results_dir / "ground_truth"),
                    ],
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                stop_controller(ctrl_pid)
                return e.returncode or 1
    else:
        print(f"skip_gt=1: skipping ground truth for {query}/{file_tag}", file=sys.stderr)

    try:
        rc = _run_one_query_file(
            bench_root=bench_root,
            results_dir=results_dir,
            query=query,
            file_tag=file_tag,
            files=files,
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


# ---------------------------------------------------------------------------
# matrix subcommand
# ---------------------------------------------------------------------------

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
                "--file", "all",
                "--out-dir", str(results_dir / "ground_truth"),
            ],
            check=True,
            cwd=str(bench_root),
        )

    controller = os.environ.get("CONTROLLER", "http://localhost:8080")
    api_port = parse_controller_api_port(controller)
    opamp_port = int(os.environ.get("CONTROLLER_OPAMP_PORT", "4320"))
    kill_process_on_tcp_port(api_port)
    kill_process_on_tcp_port(opamp_port)

    controller_bin = _env_path(
        "CONTROLLER_BIN",
        REPO_ROOT / "controller" / "target" / "release" / "controller",
    )
    auto_start = os.environ.get("AUTO_START_CONTROLLER", "1") == "1"
    ctrl_pid = start_controller_if_needed(controller, controller_bin, auto_start, results_dir)

    queries = args.queries
    file_tags = args.files
    total = 0
    n_pass = 0
    n_fail = 0
    failed: list[str] = []

    try:
        for query in queries:
            for file_tag in file_tags:
                total += 1
                sk = sketch_for_matrix_cell(
                    query,
                    args.sketch_quantile,
                    args.sketch_freq,
                    args.sketch_card,
                )
                print(f"[{total}] QUERY={query} FILE={file_tag} SKETCH={sk}", file=sys.stderr)
                rc = _run_one_query_file(
                    bench_root=bench_root,
                    results_dir=results_dir,
                    query=query,
                    file_tag=file_tag,
                    files=file_tag,
                    mode=args.mode,
                    speed=args.speed,
                    batch_size=args.batch_size,
                    sketch=sk,
                    collector_override=args.collector,
                )
                if rc == 0:
                    n_pass += 1
                    print(f"DONE [{query}/{file_tag}]", file=sys.stderr)
                else:
                    n_fail += 1
                    failed.append(f"{query}/{file_tag}(exit={rc})")
                    print(f"FAILED [{query}/{file_tag}] exit={rc}", file=sys.stderr)
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


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Exathlon benchmark orchestration.")
    sub = parser.add_subparsers(dest="command", required=True)

    # ── test ──
    p_test = sub.add_parser("test", help="Single (query, file) benchmark run.")
    p_test.add_argument("--query", default=os.environ.get("QUERY", "Q1"))
    p_test.add_argument(
        "--file",
        default=os.environ.get("FILE", "app1/1_0_10000_17"),
        help="File tag to replay (e.g. app1/1_0_10000_17).",
    )
    p_test.add_argument(
        "--files",
        default=os.environ.get("FILES", ""),
        help="Comma-separated file tags for replay; defaults to --file.",
    )
    p_test.add_argument("--mode", default=os.environ.get("MODE", "sketch-telemetry"))
    p_test.add_argument("--speed", type=float, default=float(os.environ.get("SPEED", "100")))
    p_test.add_argument(
        "--batch-size",
        type=int,
        default=int(os.environ.get("BATCH_SIZE", "50000")),
    )
    p_test.add_argument("--accuracy-minutes", type=int, default=0,
                        help="Minutes of event time to replay (0 = full file).")
    p_test.add_argument("--skip-gt", default=os.environ.get("SKIP_GT", "0"))
    p_test.add_argument("--sketch", default=os.environ.get("SKETCH", ""))
    p_test.add_argument("--collector", default=os.environ.get("COLLECTOR", "") or None)
    p_test.add_argument("--results-dir", type=Path, default=BENCH_ROOT / "results")
    p_test.add_argument("--clear-results", action="store_true",
                        help="Remove throughput.csv and latency.csv before this run.")

    # ── matrix ──
    p_matrix = sub.add_parser("matrix", help="Full matrix run (all queries × all files).")
    p_matrix.add_argument("--queries", nargs="+", default=list(DEFAULT_QUERIES))
    p_matrix.add_argument("--files", nargs="+", default=list(DEFAULT_FILES))
    p_matrix.add_argument("--mode", default=os.environ.get("MODE", "sketch-telemetry"))
    p_matrix.add_argument("--speed", type=float, default=float(os.environ.get("SPEED", "100")))
    p_matrix.add_argument(
        "--batch-size",
        type=int,
        default=int(os.environ.get("BATCH_SIZE", "50000")),
    )
    p_matrix.add_argument("--skip-gt", default=os.environ.get("SKIP_GT", "1"))
    p_matrix.add_argument("--sketch-quantile", default=os.environ.get("SKETCH_QUANTILE", "ddsketch"))
    p_matrix.add_argument("--sketch-freq", default=os.environ.get("SKETCH_FREQ", "countsketch"))
    p_matrix.add_argument("--sketch-card", default=os.environ.get("SKETCH_CARD", "hll"))
    p_matrix.add_argument("--collector", default=os.environ.get("COLLECTOR", "") or None)
    p_matrix.add_argument("--results-dir", type=Path, default=BENCH_ROOT / "results")
    p_matrix.add_argument("--clear-results", action="store_true")

    args = parser.parse_args()
    if args.command == "test":
        sys.exit(run_benchmark_test(args))
    if args.command == "matrix":
        sys.exit(run_matrix(args))


if __name__ == "__main__":
    main()
