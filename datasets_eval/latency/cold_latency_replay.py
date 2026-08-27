#!/usr/bin/env python3
"""Cold-arm query-latency replay (Fig 7 cold-fallback arm).

Mirrors deploy/mvp-multinode/scripts/metricsql_replay.py (warm arm) but
pins the PromQL *evaluation timestamp* (`time=<at_time>`) to the instant
the cold workload was anchored at, so every instant query deterministically
intersects the cold-archived window in MinIO/Thanos. (The warm arm queried
at live `now` because its in-memory warm window sat at `now`; the cold
window sits at a fixed past instant — the ship takes ~one window to land —
so we pin the eval time to it. The pin changes only WHICH timestamp the
backend evaluates at; the per-query server-side latency it measures is
identical in kind to the warm arm.)

Fires the query mix at a fixed QPS against the data-plane query surface
(:9091/api/v1/query), captures per-query wall-clock latency + the served
`data_source` (asap_query=warm vs thanos_query=cold) + result vector, and
writes a JSONL identical in schema to the warm replay so
compute_latency.py reduces both arms the same way.

Usage:
  cold_latency_replay.py --target http://127.0.0.1:9091 \
    --queries queries-latency-cold.json --at-time <unix_seconds> \
    --qps 15 --duration 40 --out replay-cold.jsonl
"""
from __future__ import annotations
import argparse, datetime as dt, json, sys, threading, time
import urllib.parse, urllib.request, urllib.error


def run_query(target: str, metricsql: str, at_time: float | None,
              timeout_s: float = 10.0):
    params = {"query": metricsql}
    if at_time is not None:
        params["time"] = f"{at_time:.3f}"
    url = f"{target.rstrip('/')}/api/v1/query?" + urllib.parse.urlencode(params)
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(url, timeout=timeout_s) as resp:
            code = resp.getcode()
            body = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        return (time.perf_counter() - started) * 1000.0, {
            "status": "http_error", "http_code": e.code, "result": None,
            "result_type": None, "data_source": None, "error": str(e)}
    except Exception as e:
        return (time.perf_counter() - started) * 1000.0, {
            "status": "timeout", "http_code": None, "result": None,
            "result_type": None, "data_source": None, "error": str(e)}
    duration_ms = (time.perf_counter() - started) * 1000.0
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as e:
        return duration_ms, {"status": "json_error", "http_code": code,
                             "result": None, "result_type": None,
                             "data_source": None, "error": str(e)}
    data = parsed.get("data") or {}
    data_source = None
    for info in (parsed.get("infos") or []):
        if isinstance(info, str) and info.startswith("data_source:"):
            data_source = info.split(":", 1)[1].strip()
    return duration_ms, {
        "status": parsed.get("status", "unknown"), "http_code": code,
        "result": data.get("result"), "result_type": data.get("resultType"),
        "data_source": data_source}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", default="http://127.0.0.1:9091")
    ap.add_argument("--queries", required=True)
    ap.add_argument("--at-time", type=float, default=None,
                    help="Unix seconds to pin the PromQL eval timestamp to. "
                         "Omit to query live now (warm-style).")
    ap.add_argument("--qps", type=float, default=15.0)
    ap.add_argument("--duration", type=float, default=40.0)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    with open(args.queries) as f:
        queries = json.load(f)
    if not queries:
        sys.exit("no queries")

    interval = 1.0 / args.qps
    deadline = time.time() + args.duration
    lock = threading.Lock()
    rows: list[dict] = []
    qi = 0
    n_sent = 0
    t0 = time.time()
    while time.time() < deadline:
        q = queries[qi % len(queries)]
        qi += 1
        dur_ms, res = run_query(args.target, q["metricsql"], args.at_time)
        rec = {
            "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
            "query": q["metricsql"], "kind": q.get("kind", "other"),
            "duration_ms": round(dur_ms, 4), "status": res["status"],
            "http_code": res.get("http_code"),
            "result_type": res.get("result_type"),
            "result": res.get("result"),
            "data_source": res.get("data_source"),
            "n_result_series": len(res.get("result") or []),
        }
        rows.append(rec)
        n_sent += 1
        # fixed-rate pacing
        next_at = t0 + n_sent * interval
        sleep = next_at - time.time()
        if sleep > 0:
            time.sleep(sleep)

    with open(args.out, "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    # quick summary
    from collections import Counter
    ds = Counter(r["data_source"] for r in rows)
    st = Counter(r["status"] for r in rows)
    empt = sum(1 for r in rows if not r["result"])
    print(f"replay: {len(rows)} queries -> {args.out}")
    print(f"  data_source: {dict(ds)}")
    print(f"  status: {dict(st)}  empty_results: {empt}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
