#!/usr/bin/env python3
"""PromQL replay client for the e2e harness (P5).

Fires PromQL queries at the backend's HTTP query surface
(`:19091/api/v1/query`) at a fixed QPS, captures wall-clock
latency + result vector per attempt, and tags each line of the
output JSONL log with the active plan id (polled from the
controller's /metrics every second).

Output schema (JSONL, one line per query attempt):

    {
        "ts": "2026-04-30T13:45:01.123Z",
        "query": "histogram_quantile(0.99, http_requests_total_latency_ms)",
        "kind": "quantile",                # quantile | topk | count_unique | sum
        "duration_ms": 12.4,
        "status": "success",               # success | http_error | timeout | json_error
        "http_code": 200,
        "result_type": "vector",
        "result": [...],                   # raw PromQL result; verbatim
        "plan_id": "p_dd99_60s_keepall",   # latest seen from controller /metrics
        "fallback_used": "cold" | "prom" | null,  # parsed from response if backend exposes
    }

The reducer (P8) joins this against the raw-tee JSONL on
`(query, ts_ms_window)` to compute accuracy.

Usage:

    python3 promql_replay.py \\
        --target http://localhost:19091 \\
        --controller http://localhost:18080 \\
        --queries queries.json \\
        --qps 10 \\
        --duration 60 \\
        --out replay.jsonl
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import threading
import time
import urllib.parse
import urllib.request
from typing import Any


# --- query-suite primitives ----------------------------------------

# A query in the input list looks like:
#
#   {"kind": "quantile", "promql": "histogram_quantile(0.99, ...)"}
#
# `kind` is the sketch family the query exercises so the reducer can
# pick the right ground-truth function:
#   - quantile      → DDSketch / KLL          → P-th quantile
#   - topk          → CountSketch (heavy-hit) → top-K by frequency
#   - count_unique  → HLL                     → distinct cardinality
#   - sum           → Sum                     → exact, identity check
QUERY_KINDS = {"quantile", "topk", "count_unique", "sum"}


def load_queries(path: str) -> list[dict[str, str]]:
    with open(path, "r") as f:
        loaded = json.load(f)
    if not isinstance(loaded, list):
        sys.exit(f"queries file must be a JSON list, got {type(loaded)}")
    out = []
    for i, q in enumerate(loaded):
        if "promql" not in q or "kind" not in q:
            sys.exit(f"queries[{i}] missing required 'promql' or 'kind' field")
        if q["kind"] not in QUERY_KINDS:
            sys.exit(f"queries[{i}].kind must be one of {QUERY_KINDS}, got {q['kind']!r}")
        out.append({"kind": q["kind"], "promql": q["promql"]})
    return out


# --- controller plan-id poller -------------------------------------


class PlanIdTracker:
    """Polls controller /metrics for `asap_active_plan_id` (or
    `asap_plan_id` — whichever the controller exposes today) and
    updates a thread-shared latest value. The replay loop reads
    `latest()` per query without blocking on the poll."""

    def __init__(self, controller_url: str, interval_s: float = 1.0):
        self.url = f"{controller_url.rstrip('/')}/metrics"
        self.interval_s = interval_s
        self._lock = threading.Lock()
        self._latest: str | None = None
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=2.0)

    def latest(self) -> str | None:
        with self._lock:
            return self._latest

    def _loop(self) -> None:
        while not self._stop.is_set():
            self._poll_once()
            self._stop.wait(self.interval_s)

    def _poll_once(self) -> None:
        try:
            with urllib.request.urlopen(self.url, timeout=2.0) as resp:
                body = resp.read().decode("utf-8", errors="replace")
        except Exception:
            return
        # Look for either `asap_active_plan_id` or `asap_plan_id` in
        # the prom-text exposition. A common pattern is:
        #   asap_active_plan_id{plan_id="p_dd99_60s_keepall"} 1
        for line in body.splitlines():
            line = line.strip()
            if line.startswith("#") or not line:
                continue
            for needle in ("asap_active_plan_id", "asap_plan_id"):
                if line.startswith(needle):
                    pid = self._extract_plan_id(line)
                    if pid is not None:
                        with self._lock:
                            self._latest = pid
                        return

    @staticmethod
    def _extract_plan_id(line: str) -> str | None:
        # Cheap parse: pull the value of plan_id="..." if present;
        # else the whole label set; else None.
        i = line.find('plan_id="')
        if i < 0:
            return None
        i += len('plan_id="')
        j = line.find('"', i)
        if j < 0:
            return None
        return line[i:j]


# --- query runner --------------------------------------------------


def run_query(
    target: str,
    promql: str,
    timeout_s: float = 10.0,
) -> tuple[float, dict[str, Any]]:
    """Returns (duration_ms, result_dict). On any error the
    result_dict has a `status` key explaining what happened."""
    qs = urllib.parse.urlencode({"query": promql})
    url = f"{target.rstrip('/')}/api/v1/query?{qs}"
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(url, timeout=timeout_s) as resp:
            code = resp.getcode()
            body = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        return (
            (time.perf_counter() - started) * 1000.0,
            {
                "status": "http_error",
                "http_code": e.code,
                "result": None,
                "result_type": None,
                "fallback_used": None,
                "error": str(e),
            },
        )
    except Exception as e:
        return (
            (time.perf_counter() - started) * 1000.0,
            {
                "status": "timeout",
                "http_code": None,
                "result": None,
                "result_type": None,
                "fallback_used": None,
                "error": str(e),
            },
        )
    duration_ms = (time.perf_counter() - started) * 1000.0

    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as e:
        return duration_ms, {
            "status": "json_error",
            "http_code": code,
            "result": None,
            "result_type": None,
            "fallback_used": None,
            "error": str(e),
        }

    data = parsed.get("data") or {}
    return duration_ms, {
        "status": parsed.get("status", "unknown"),
        "http_code": code,
        "result": data.get("result"),
        "result_type": data.get("resultType"),
        "fallback_used": parsed.get("fallback_used"),  # populated by ASAP backend if present
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="PromQL replay client (P5)")
    ap.add_argument("--target", default="http://localhost:19091",
                    help="backend PromQL HTTP base URL")
    ap.add_argument("--controller", default="http://localhost:18080",
                    help="controller base URL (for /metrics plan-id polling)")
    ap.add_argument("--queries", required=True,
                    help="path to JSON list of {kind, promql} entries")
    ap.add_argument("--qps", type=float, default=10.0,
                    help="aggregate query rate (rows-per-second across all queries)")
    ap.add_argument("--duration", type=float, default=60.0,
                    help="run for this many seconds total")
    ap.add_argument("--out", required=True, help="JSONL output path")
    ap.add_argument("--timeout", type=float, default=10.0,
                    help="per-query HTTP timeout (s)")
    ap.add_argument("--no-plan-poll", action="store_true",
                    help="disable controller /metrics polling (run without plan tagging)")
    args = ap.parse_args()

    queries = load_queries(args.queries)
    if not queries:
        sys.exit("no queries in input")

    tracker = None
    if not args.no_plan_poll:
        tracker = PlanIdTracker(args.controller)
        tracker.start()

    period_s = 1.0 / args.qps if args.qps > 0 else 0.0
    end_at = time.monotonic() + args.duration
    n = 0

    print(
        f"replay: target={args.target} qps={args.qps} duration={args.duration}s "
        f"queries={len(queries)} out={args.out}"
    )

    try:
        with open(args.out, "w") as out:
            while time.monotonic() < end_at:
                q = queries[n % len(queries)]
                n += 1
                t_start = time.perf_counter()
                dur_ms, res = run_query(args.target, q["promql"], args.timeout)
                rec = {
                    "ts": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
                    "query": q["promql"],
                    "kind": q["kind"],
                    "duration_ms": dur_ms,
                    "plan_id": tracker.latest() if tracker else None,
                    **res,
                }
                out.write(json.dumps(rec) + "\n")
                out.flush()

                if period_s > 0:
                    elapsed = time.perf_counter() - t_start
                    if elapsed < period_s:
                        time.sleep(period_s - elapsed)
    finally:
        if tracker is not None:
            tracker.stop()

    print(f"replay: completed {n} queries → {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
