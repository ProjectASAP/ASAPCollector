#!/usr/bin/env python3
"""measure_freshness.py — freshness measurement driver.

Two operating modes share this file. The mode is selected by which
flags the caller passes; mixing them is rejected with a clear error.

## v6 mode (preferred — see deploy/configs/mvp-freshness-probes.yaml)

Polls a Prometheus-style /api/v1/query endpoint for one of the three
freshness probe metrics emitted by the fake-exporter (see
deploy/fake-exporter/probes.go) and computes per-sample freshness::

    delta_ms = poll_response_ts_ms - observed_value

`observed_value` is the cumulative counter value returned by
`last_over_time(<probe>[10s])`. The fake-exporter encodes the
unix_ts_ms of the most recent emission directly in the counter's
cumulative value (probes.go), so the subtraction yields the
wall-clock latency between emission and visibility through the
chosen query layer.

Three probes route via metric name to three different storage paths::

    http_freshness_probe_raw      → Prometheus B0
    http_freshness_probe_warm     → sketch warm tier (agent processor)
    http_freshness_probe_archive  → Gorilla-archive (gorillas3processor)

Run once per path against the appropriate query endpoint; see
deploy/scripts/run_freshness_phase.sh for the canonical three-path
driver.

Usage::

    measure_freshness.py \\
        --query-endpoint http://prometheus-b0:9090 \\
        --probe http_freshness_probe_raw \\
        --path-label raw \\
        --duration 60 \\
        --poll-interval-ms 100 \\
        --output /tmp/freshness/raw.csv

Output CSV (always written, even with zero samples)::

    path,sample_ts_ms,observed_ts_ms,delta_ms

A summary line (count, p50, p99) goes to stderr at exit.

## v4 mode (legacy — preserved for older callers that still pass the v4 flag set)

Pushes two synthetic gauges into an OTLP/HTTP receiver every 1s, then
polls a PromQL backend every 100ms until the first non-NaN
observation lands. Drives BOTH the producer and the consumer side.

Usage::

    measure_freshness.py \\
        --baseline asap-single-sketch \\
        --otlp-http http://localhost:14328 \\
        --query http://localhost:19091 \\
        --duration 60 \\
        --out freshness.csv

Output CSV columns (legacy)::

    baseline,path,sample_ts_ms,observation_ts_ms,delta_ms

The legacy mode is preserved verbatim from v4. The v6 mode is the
direction Phase D + onwards goes.
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Iterable, Optional


# Canonical v6 probe metric names. Keeping them as an allow-list at
# the CLI boundary catches typos before the run starts and produces
# an empty CSV with no warning.
ALLOWED_PROBES = (
    "http_freshness_probe_raw",
    "http_freshness_probe_warm",
    "http_freshness_probe_archive",
)


# -- Prometheus query helper (shared between v4 and v6 modes) ----------


def query_once_v6(
    base_url: str, promql: str, timeout_s: float
) -> tuple[int, Optional[float]]:
    """Execute one /api/v1/query and return (response_ts_ms, observed_value).

    `observed_value` is None on empty result / HTTP error / decode
    error. The response_ts_ms is captured the moment urllib finishes
    reading the body — that's the latest defensible "the data was
    visible to a query at this wall-clock" timestamp.
    """
    qs = urllib.parse.urlencode({"query": promql})
    url = f"{base_url.rstrip('/')}/api/v1/query?{qs}"
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            body = resp.read()
    except (urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
        print(f"[warn] query error: {exc}", file=sys.stderr)
        return int(time.time() * 1000), None

    response_ts_ms = int(time.time() * 1000)
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        print(f"[warn] json decode error: {exc}", file=sys.stderr)
        return response_ts_ms, None

    if payload.get("status") != "success":
        # Probe metric likely unknown on this endpoint — keep polling.
        err = payload.get("error", "<no error field>")
        print(f"[warn] query non-success: {err}", file=sys.stderr)
        return response_ts_ms, None

    result = payload.get("data", {}).get("result", []) or []
    if not result:
        return response_ts_ms, None

    val_pair = result[0].get("value")
    if not val_pair or len(val_pair) < 2:
        return response_ts_ms, None
    try:
        observed = float(val_pair[1])
    except (TypeError, ValueError):
        return response_ts_ms, None
    # v7: GorillaQueryEngine returns `NaN` for empty windows (no
    # chunks land yet, or the postings filter pruned everything
    # away). Treat NaN identically to "no data" so downstream
    # `int(observed)` doesn't blow up. The replay client will keep
    # polling; the next tick's chunks may carry a real sample.
    import math
    if math.isnan(observed) or math.isinf(observed):
        return response_ts_ms, None
    return response_ts_ms, observed


def write_v6_summary(
    deltas: list[float], path_label: str, count_attempted: int
) -> None:
    """One-line `freshness:` summary on stderr — count, p50, p99.

    Designed to be both human-readable and grep-friendly; the
    prefix is the agreed marker for log scrapers.
    """
    if not deltas:
        print(
            f"freshness: path={path_label} attempted={count_attempted} "
            f"got=0 p50=NA p99=NA",
            file=sys.stderr,
        )
        return
    deltas_sorted = sorted(deltas)
    p50 = statistics.median(deltas_sorted)
    if len(deltas_sorted) >= 2:
        p99 = statistics.quantiles(deltas_sorted, n=100, method="inclusive")[98]
    else:
        # quantiles requires ≥ 2 points; for a single sample, the
        # value IS the p99.
        p99 = deltas_sorted[0]
    print(
        f"freshness: path={path_label} attempted={count_attempted} "
        f"got={len(deltas_sorted)} p50={p50:.1f}ms p99={p99:.1f}ms",
        file=sys.stderr,
    )


# -- v6 main --------------------------------------------------------


def run_v6(args: argparse.Namespace) -> int:
    if args.probe not in ALLOWED_PROBES:
        sys.exit(
            f"--probe must be one of {ALLOWED_PROBES}; got {args.probe!r}"
        )
    promql = f"last_over_time({args.probe}[{args.query_window}])"
    deadline = time.time() + args.duration
    poll_period_s = max(0.001, args.poll_interval_ms / 1000.0)
    deltas: list[float] = []
    count_attempted = 0

    print(
        f"# v6 freshness probe — endpoint={args.query_endpoint} "
        f"probe={args.probe} path={args.path_label} duration={args.duration}s "
        f"poll={args.poll_interval_ms}ms",
        file=sys.stderr,
    )

    with open(args.output, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["path", "sample_ts_ms", "observed_ts_ms", "delta_ms"])
        # Stream rows so a long run leaves usable partial output if
        # the script is killed.
        next_tick = time.time()
        while time.time() < deadline:
            count_attempted += 1
            response_ts_ms, observed = query_once_v6(
                args.query_endpoint, promql, args.http_timeout_s
            )
            if observed is not None:
                # `observed` is unix_ts_ms encoded in the counter
                # value. Casting through int avoids floating-point
                # drift in the CSV column for downstream joins.
                observed_ts_ms = int(observed)
                delta_ms = response_ts_ms - observed_ts_ms
                writer.writerow(
                    [args.path_label, response_ts_ms, observed_ts_ms, delta_ms]
                )
                deltas.append(delta_ms)
            # Self-correcting cadence: if a query took longer than
            # the poll interval, skip ahead instead of accumulating
            # drift.
            next_tick += poll_period_s
            sleep_for = next_tick - time.time()
            if sleep_for > 0:
                time.sleep(sleep_for)
            else:
                next_tick = time.time()

    write_v6_summary(deltas, args.path_label, count_attempted)
    return 0


# == v4 (legacy) =====================================================
#
# Pre-existing legacy implementation, kept verbatim except for the
# rename of the helper functions to avoid namespace clashes with the
# v6 helpers above. The MVP driver (run_mvp_demo.sh) no longer calls
# the legacy v4 form; the legacy mode is preserved here so older
# callers / scripts that still pass the v4 flag set continue to work.


def _otlp_metric_envelope(metric_name: str, value_ms: float, ts_ns: int) -> dict:
    """Tiny OTLP/HTTP gauge payload. JSON form because that's what
    the OTLP/HTTP receiver accepts on `/v1/metrics` without a
    proto compiler dependency."""
    return {
        "resourceMetrics": [
            {
                "resource": {
                    "attributes": [
                        {
                            "key": "service.name",
                            "value": {"stringValue": "freshness-probe"},
                        }
                    ]
                },
                "scopeMetrics": [
                    {
                        "scope": {"name": "freshness-probe"},
                        "metrics": [
                            {
                                "name": metric_name,
                                "gauge": {
                                    "dataPoints": [
                                        {
                                            "timeUnixNano": str(ts_ns),
                                            "asDouble": value_ms,
                                        }
                                    ]
                                },
                            }
                        ],
                    }
                ],
            }
        ]
    }


def push_loop(
    otlp_http_url: str,
    metrics: list[str],
    stop: threading.Event,
    period_s: float = 1.0,
) -> None:
    """Push one gauge per metric per `period_s` until `stop` is set.
    Each gauge's value encodes the emission unix_ts_ms.

    OTLP/HTTP `/v1/metrics` accepts JSON when the
    `Content-Type: application/json` header is set."""
    url = otlp_http_url.rstrip("/") + "/v1/metrics"
    while not stop.is_set():
        ts_ms = time.time() * 1000.0
        ts_ns = int(ts_ms * 1_000_000)
        for m in metrics:
            payload = _otlp_metric_envelope(m, ts_ms, ts_ns)
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                url,
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    resp.read()
            except (urllib.error.URLError, urllib.error.HTTPError, OSError) as e:
                print(
                    f"# push failed ({m}): {e}",
                    file=sys.stderr,
                )
        if stop.wait(period_s):
            return


def prom_query_v4(
    query_url: str, q: str, timeout_s: float = 2.0
) -> dict | None:
    """Single instant query. Returns the parsed JSON body or None
    on transport failure."""
    params = urllib.parse.urlencode({"query": q})
    url = query_url.rstrip("/") + f"/api/v1/query?{params}"
    try:
        with urllib.request.urlopen(url, timeout=timeout_s) as resp:
            return json.load(resp)
    except (urllib.error.URLError, urllib.error.HTTPError, OSError):
        return None


def parse_value_ms(body: dict | None) -> float | None:
    """`{"data": {"result": [{"value": [<ts>, "<v>"]}]}}` → float v.
    None if the response was empty / errored."""
    if not body or body.get("status") != "success":
        return None
    result = (body.get("data") or {}).get("result") or []
    if not result:
        return None
    val = result[0].get("value")
    if not val or len(val) < 2:
        return None
    try:
        return float(val[1])
    except (TypeError, ValueError):
        return None


def poll_loop_v4(
    query_url: str,
    metric: str,
    path_label: str,
    baseline: str,
    stop: threading.Event,
    out_rows: list,
    period_s: float = 0.1,
    range_window: str = "10s",
) -> None:
    """Poll PromQL every `period_s` for `last_over_time(<metric>[<range_window>])`."""
    q = f"last_over_time({metric}[{range_window}])"
    seen_sample_ts: float | None = None
    while not stop.is_set():
        body = prom_query_v4(query_url, q)
        sample_v = parse_value_ms(body)
        obs_ts_ms = time.time() * 1000.0
        if sample_v is not None and sample_v == sample_v:  # NaN guard
            if sample_v != seen_sample_ts:
                delta_ms = obs_ts_ms - sample_v
                out_rows.append(
                    (
                        baseline,
                        path_label,
                        f"{sample_v:.3f}",
                        f"{obs_ts_ms:.3f}",
                        f"{delta_ms:.3f}",
                    )
                )
                seen_sample_ts = sample_v
        if stop.wait(period_s):
            return


def _percentile(xs: list[float], p: float) -> float:
    if not xs:
        return float("nan")
    s = sorted(xs)
    idx = max(0, min(len(s) - 1, int(round(p * (len(s) - 1)))))
    return s[idx]


def run_v4(args: argparse.Namespace) -> int:
    paths = [x.strip() for x in args.paths.split(",") if x.strip()]
    valid = {"warm", "archive"}
    bad = [x for x in paths if x not in valid]
    if bad:
        sys.exit(f"--paths must be subset of {valid}; got bad entries {bad}")
    metric_for_path = {
        "warm": "http_freshness_probe_warm",
        "archive": "http_freshness_probe_archive",
    }

    stop = threading.Event()
    push_metrics = [metric_for_path[x] for x in paths]
    rows: list[tuple] = []

    push_thread = threading.Thread(
        target=push_loop,
        args=(args.otlp_http, push_metrics, stop, args.push_period),
        daemon=True,
    )
    poll_threads = []
    for path in paths:
        t = threading.Thread(
            target=poll_loop_v4,
            args=(
                args.query,
                metric_for_path[path],
                path,
                args.baseline,
                stop,
                rows,
                args.poll_period,
                args.range_window,
            ),
            daemon=True,
        )
        poll_threads.append(t)

    print(
        f"# freshness probe (legacy v4) — baseline={args.baseline} "
        f"duration={args.duration}s paths={paths} "
        f"otlp={args.otlp_http} query={args.query}",
        file=sys.stderr,
    )
    push_thread.start()
    for t in poll_threads:
        t.start()

    try:
        stop.wait(args.duration)
    except KeyboardInterrupt:
        pass
    stop.set()
    push_thread.join(timeout=5)
    for t in poll_threads:
        t.join(timeout=5)

    header = ("baseline", "path", "sample_ts_ms", "observation_ts_ms", "delta_ms")
    with open(args.out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)

    by_path: dict[str, list[float]] = {}
    for r in rows:
        try:
            by_path.setdefault(r[1], []).append(float(r[4]))
        except ValueError:
            continue
    print(f"# wrote {args.out} ({len(rows)} rows)")
    for path in sorted(by_path):
        deltas = by_path[path]
        print(
            f"# baseline={args.baseline} path={path} count={len(deltas)} "
            f"p50={_percentile(deltas, 0.50):.1f}ms "
            f"p99={_percentile(deltas, 0.99):.1f}ms"
        )
    return 0


# == argparse + dispatch ============================================


def build_parser() -> argparse.ArgumentParser:
    """Build a parser that accepts both v4 and v6 flag sets.

    Mode is detected at runtime — see `select_mode`. Mixing flag sets
    is rejected there.
    """
    p = argparse.ArgumentParser(
        description="ASAP freshness measurement driver (v4 + v6)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # v6 mode flags (preferred)
    g6 = p.add_argument_group("v6 mode (poll-only, fake-exporter does the push)")
    g6.add_argument("--query-endpoint", help="Prometheus /api/v1/query base URL")
    g6.add_argument(
        "--probe",
        choices=ALLOWED_PROBES,
        help="probe metric name (must match fake-exporter probes.go)",
    )
    g6.add_argument(
        "--path-label",
        help='output column "path" tag (e.g. "raw" / "warm" / "archive")',
    )
    g6.add_argument(
        "--poll-interval-ms",
        type=int,
        default=100,
        help="poll cadence in ms (default 100 = 10 Hz)",
    )
    g6.add_argument("--output", help="output CSV path (v6 schema)")
    g6.add_argument(
        "--query-window",
        default="10s",
        help='last_over_time window (default "10s")',
    )
    g6.add_argument(
        "--http-timeout-s",
        type=float,
        default=2.0,
        help="HTTP timeout per query in seconds (default 2.0)",
    )

    # v4 mode flags (legacy)
    g4 = p.add_argument_group("v4 mode (legacy — script does the push too)")
    g4.add_argument("--baseline", help="baseline label (legacy)")
    g4.add_argument("--otlp-http", help="OTLP/HTTP push endpoint (legacy)")
    g4.add_argument("--query", help="PromQL HTTP endpoint (legacy)")
    g4.add_argument(
        "--paths",
        default="warm,archive",
        help="legacy paths subset of {warm,archive}",
    )
    g4.add_argument(
        "--push-period",
        type=float,
        default=1.0,
        help="legacy: seconds between pushes (default 1.0)",
    )
    g4.add_argument(
        "--poll-period",
        type=float,
        default=0.1,
        help="legacy: seconds between polls (default 0.1)",
    )
    g4.add_argument(
        "--range-window",
        default="10s",
        help='legacy: last_over_time window (default "10s")',
    )
    g4.add_argument("--out", help="legacy output CSV path")

    # Shared
    p.add_argument(
        "--duration",
        type=float,
        default=60.0,
        help="measurement window in seconds (both modes; default 60)",
    )
    return p


def select_mode(args: argparse.Namespace) -> str:
    """Decide v4 vs v6 from which flags the caller passed.

    v6 is selected when ANY of the v6-only flags are present;
    v4 is selected when the v4 trio (--baseline + --otlp-http + --query)
    is fully present. Mixing the two is rejected with a clear error.
    """
    v6_present = any(
        getattr(args, k) is not None
        for k in ("query_endpoint", "probe", "path_label", "output")
    )
    v4_present = any(
        getattr(args, k) is not None
        for k in ("baseline", "otlp_http", "query", "out")
    )
    if v6_present and v4_present:
        sys.exit(
            "Mixing v4 (--baseline / --otlp-http / --query / --out) and "
            "v6 (--query-endpoint / --probe / --path-label / --output) "
            "flag sets is not supported."
        )
    if v6_present:
        # v6 requires the full quad.
        missing = [
            n
            for n, v in (
                ("--query-endpoint", args.query_endpoint),
                ("--probe", args.probe),
                ("--path-label", args.path_label),
                ("--output", args.output),
            )
            if v is None
        ]
        if missing:
            sys.exit(f"v6 mode requires {missing}")
        return "v6"
    if v4_present:
        missing = [
            n
            for n, v in (
                ("--baseline", args.baseline),
                ("--otlp-http", args.otlp_http),
                ("--query", args.query),
                ("--out", args.out),
            )
            if v is None
        ]
        if missing:
            sys.exit(f"v4 mode requires {missing}")
        return "v4"
    sys.exit(
        "Specify either v6 flags (--query-endpoint / --probe / --path-label "
        "/ --output) or v4 flags (--baseline / --otlp-http / --query / --out)."
    )


def main(argv: Iterable[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    mode = select_mode(args)
    if mode == "v6":
        return run_v6(args)
    return run_v4(args)


if __name__ == "__main__":
    sys.exit(main())
