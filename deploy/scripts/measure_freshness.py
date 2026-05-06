#!/usr/bin/env python3
"""measure_freshness.py — issue #46 v4 criterion ⑥ (sample-emit → first-query).

Pushes two synthetic gauges into an OTLP HTTP receiver every 1s, then
polls a PromQL backend every 100ms until the first non-NaN
observation lands. The Δ between sample emission timestamp and
observation timestamp is criterion ⑥'s "freshness" measurement.

Two probe metrics so the warm-tier and cold-archive paths can be
measured independently:

  * `http_freshness_probe_warm`     — routed via
        `backend-storage-routing.yaml` → `sketch_warm_tier`
        (warm-tier sketch on the B6 ASAP single-sketch baseline).

  * `http_freshness_probe_archive`  — routed → `gorilla_s3_archive`
        (Gorilla cold archive on the same baseline).

For B0 (raw → Prometheus) the same probe metrics are pushed and
queried against Prometheus's `/api/v1/query`; only the warm path
is meaningful (Prometheus has no cold archive), so the archive
column for B0 is left blank.

For B1 (SERF → Prometheus) the probes go through the SERF agent's
PRW exporter to Prometheus; same warm-only column shape as B0.

Sample value encoding:
    For each tick, the gauge value is the unix_ts_ms of emission
    (i.e. value = ts_emit_ms). The Δ is observation_ts - value.
    Probing with `last_over_time(<metric>[10s])` returns the most
    recent value; subtracting from the wall-clock at observation
    yields a freshness number that's robust to scrape-interval
    quantization (the 10s window covers the worst-case 15s scrape
    interval Prometheus is configured with — adjusted from the
    queryable side to under-report freshness rather than over-).

Output schema (CSV):

    baseline,path,sample_ts_ms,observation_ts_ms,delta_ms

Where `path` ∈ {warm, archive}.

Stdlib only. Independent of `run_mvp_demo.sh` — exits cleanly
after the configurable measurement window (default 60s).

Usage:

  python3 measure_freshness.py \\
      --baseline asap-single-sketch \\
      --otlp-http http://localhost:14328 \\
      --query     http://localhost:19091 \\
      --duration  60 \\
      --out       freshness.csv

For B0 / B1 baselines:

  python3 measure_freshness.py \\
      --baseline b0-prometheus \\
      --otlp-http http://localhost:14318 \\
      --query     http://localhost:9090 \\
      --paths     warm \\
      --duration  60 \\
      --out       freshness.csv
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Iterable


# ── OTLP HTTP push ─────────────────────────────────────────────────


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
        # Sleep until the next period — wake on stop early.
        if stop.wait(period_s):
            return


# ── PromQL polling ─────────────────────────────────────────────────


def prom_query(query_url: str, q: str, timeout_s: float = 2.0) -> dict | None:
    """Single instant query. Returns the parsed JSON body or None
    on transport failure. The caller is responsible for digging
    into `.data.result[0].value[1]`."""
    params = urllib.parse.urlencode({"query": q})
    url = query_url.rstrip("/") + f"/api/v1/query?{params}"
    try:
        with urllib.request.urlopen(url, timeout=timeout_s) as resp:
            return json.load(resp)
    except (urllib.error.URLError, urllib.error.HTTPError, OSError) as e:
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


def poll_loop(
    query_url: str,
    metric: str,
    path_label: str,
    baseline: str,
    stop: threading.Event,
    out_rows: list,
    period_s: float = 0.1,
    range_window: str = "10s",
) -> None:
    """Poll PromQL every `period_s` for `last_over_time(<metric>[<range_window>])`.
    On every successful non-NaN observation, append a CSV row to
    `out_rows`. Each row is `(baseline, path_label, sample_ts_ms,
    observation_ts_ms, delta_ms)`.

    `last_over_time` returns the most recent sample's value. Since
    each sample's value encodes its own `unix_ts_ms`, the Δ between
    observation wall-clock and value gives the freshness signal."""
    q = f"last_over_time({metric}[{range_window}])"
    seen_sample_ts: float | None = None
    while not stop.is_set():
        body = prom_query(query_url, q)
        sample_v = parse_value_ms(body)
        obs_ts_ms = time.time() * 1000.0
        if sample_v is not None and sample_v == sample_v:  # NaN guard
            # Only record one row per distinct sample_ts so the
            # output is one Δ per sample, not one Δ per poll.
            if sample_v != seen_sample_ts:
                delta_ms = obs_ts_ms - sample_v
                out_rows.append(
                    (baseline, path_label, f"{sample_v:.3f}", f"{obs_ts_ms:.3f}", f"{delta_ms:.3f}")
                )
                seen_sample_ts = sample_v
        if stop.wait(period_s):
            return


# ── stat helpers ───────────────────────────────────────────────────


def _percentile(xs: list[float], p: float) -> float:
    if not xs:
        return float("nan")
    s = sorted(xs)
    idx = max(0, min(len(s) - 1, int(round(p * (len(s) - 1)))))
    return s[idx]


# ── main ───────────────────────────────────────────────────────────


def main(argv: Iterable[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--baseline",
        required=True,
        help="Label for the baseline this run measures "
        "(e.g. b0-prometheus, b1-serf, asap-single-sketch).",
    )
    p.add_argument(
        "--otlp-http",
        required=True,
        help="OTLP/HTTP endpoint for the synthetic-gauge push "
        "(e.g. http://localhost:14318 for the gateway, "
        "http://localhost:14328 for agent-1 in the ASAP overlay).",
    )
    p.add_argument(
        "--query",
        required=True,
        help="PromQL HTTP endpoint for polling "
        "(e.g. http://localhost:19091 for the precompute backend, "
        "http://localhost:9090 for raw Prometheus).",
    )
    p.add_argument(
        "--paths",
        default="warm,archive",
        help="Comma-separated subset of {warm,archive}. B0 / B1 "
        "have no cold archive — pass `warm` only.",
    )
    p.add_argument(
        "--duration",
        type=float,
        default=60.0,
        help="Measurement window in seconds (default 60s).",
    )
    p.add_argument(
        "--push-period",
        type=float,
        default=1.0,
        help="Seconds between gauge pushes (default 1.0).",
    )
    p.add_argument(
        "--poll-period",
        type=float,
        default=0.1,
        help="Seconds between PromQL polls (default 0.1).",
    )
    p.add_argument(
        "--range-window",
        default="10s",
        help="Window for `last_over_time(<metric>[<window>])` "
        "(default 10s; covers the 15s scrape interval).",
    )
    p.add_argument(
        "--out",
        required=True,
        help="Output CSV path. Columns: "
        "baseline,path,sample_ts_ms,observation_ts_ms,delta_ms.",
    )
    args = p.parse_args(argv)

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
            target=poll_loop,
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
        f"# freshness probe — baseline={args.baseline} duration={args.duration}s "
        f"paths={paths} otlp={args.otlp_http} query={args.query}",
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

    # CSV write.
    header = ("baseline", "path", "sample_ts_ms", "observation_ts_ms", "delta_ms")
    with open(args.out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)

    # Per-(baseline, path) summary to stdout for the run-only agent.
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


if __name__ == "__main__":
    sys.exit(main())
