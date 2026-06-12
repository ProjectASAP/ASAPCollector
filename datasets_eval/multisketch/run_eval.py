#!/usr/bin/env python3
"""Multi-sketch-family eval driver.

Replays an aliased google_cluster JSONL into the collapsed single-host
asap stack, forces a window flush, queries each family via the data-plane
query engine, compares against exact offline ground truth (gt_eval), and
records edge CPU / RSS / wire bytes.

The stack must already be up (datasets_eval/multisketch/stack.sh up ...).
This driver does NOT bring it up — the caller chooses the workload/agent
config per arm so it can measure a fresh stack.

Window strategy: the agent runs a wide (600s) window so all replayed rows
land in ONE flush; the warm aggregate then equals the exact GT over all
rows. After replay we trigger a flush by waiting (or rely on shutdown
flush) and SWEEP the query `time=` across recent wall-clock windows to
find the populated window (instant queries must hit the sealed window).
"""
from __future__ import annotations
import argparse, json, math, subprocess, sys, time, urllib.parse, urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
GCT = HERE.parent / "google_cluster"
sys.path.insert(0, str(GCT / "e2e"))
import gt_eval  # noqa: E402


def q(base, promql, t=None, timeout=30.0):
    params = {"query": promql}
    if t is not None:
        params["time"] = str(t)
    url = base.rstrip("/") + "/api/v1/query?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(urllib.request.Request(url), timeout=timeout) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read().decode())
        except Exception:
            return {"status": "error", "error": str(e)}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def extract(body, group_label=None):
    """Return scalar float, or {key:val} dict keyed by group_label."""
    data = body.get("data") or {}
    if not isinstance(data, dict):
        return None
    rtype = data.get("resultType", "")
    src = "unknown"
    for info in body.get("infos") or []:
        if isinstance(info, str) and info.startswith("data_source:"):
            src = info.split(":", 1)[1].strip()
    res = data.get("result")
    if rtype in ("vector", "matrix") and res:
        if group_label:
            out = {}
            for s in res:
                lbl = s.get("metric", {}).get(group_label, "")
                v = s.get("value") or (s.get("values") or [[None, None]])[-1]
                if v and v[1] is not None:
                    out[lbl] = float(v[1])
            return out, src
        s = res[0]
        v = s.get("value") or (s.get("values") or [[None, None]])[-1]
        return (float(v[1]) if v and v[1] is not None else None), src
    if rtype == "scalar" and res:
        return float(res[1]), src
    return None, src


def sweep_query(base, promql, group_label=None, span_s=900, step_s=30):
    """Sweep time= backwards to find the populated sealed window."""
    now = int(time.time())
    best = None
    for off in range(0, span_s + 1, step_s):
        body = q(base, promql, t=now - off)
        val, src = extract(body, group_label)
        if val not in (None, {}, 0.0) and not (isinstance(val, float) and math.isnan(val)):
            return val, src, now - off
        if best is None and val is not None:
            best = (val, src, now - off)
    return (best if best else (None, "empty", None))


def relerr(got, true):
    if true == 0:
        return abs(got) if got else 0.0
    return abs(got - true) / abs(true)


def recall_at_k(got_keys, true_keys):
    g, t = set(got_keys), set(true_keys)
    return len(g & t) / len(t) if t else 0.0


def agent_wire_bytes(metrics_url="http://127.0.0.1:8890/metrics"):
    """Total bytes the agent's otlp/backend exporter shipped (warm-sketch wire)."""
    try:
        with urllib.request.urlopen(metrics_url, timeout=10) as r:
            for line in r.read().decode().splitlines():
                if line.startswith("otelcol_exporter_queue_batch_send_size_bytes_sum") \
                        and 'otlp/backend' in line:
                    return float(line.rsplit(" ", 1)[1])
    except Exception:
        return None
    return None


def agent_sent_points(metrics_url="http://127.0.0.1:8890/metrics"):
    try:
        with urllib.request.urlopen(metrics_url, timeout=10) as r:
            for line in r.read().decode().splitlines():
                if line.startswith("otelcol_exporter_sent_metric_points_total") \
                        and 'otlp/backend' in line:
                    return float(line.rsplit(" ", 1)[1])
    except Exception:
        return None
    return None


def docker_stats():
    out = subprocess.run(
        ["docker", "stats", "--no-stream", "--format",
         "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"],
        capture_output=True, text=True, timeout=30).stdout
    d = {}
    for line in out.strip().splitlines():
        parts = line.split("\t")
        if len(parts) == 3:
            d[parts[0]] = {"cpu": parts[1], "mem": parts[2]}
    return d


def container_net_rx(name):
    """RX bytes for a container (proc net stat via docker exec)."""
    try:
        out = subprocess.run(["docker", "exec", name, "cat", "/proc/net/dev"],
                             capture_output=True, text=True, timeout=10).stdout
        total = 0
        for line in out.splitlines():
            if ":" in line and "lo:" not in line:
                f = line.split(":", 1)[1].split()
                total += int(f[0])  # rx bytes
        return total
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--jsonl", required=True)
    ap.add_argument("--queries", required=True)
    ap.add_argument("--backend", default="http://127.0.0.1:9091")
    ap.add_argument("--agent-endpoint", default="127.0.0.1:4317")
    ap.add_argument("--out", required=True)
    ap.add_argument("--flush-wait", type=float, default=60.0,
                    help="seconds to wait after replay for window seal+ship")
    ap.add_argument("--arm", default="arm")
    ap.add_argument("--align-window", action="store_true", default=True)
    ap.add_argument("--no-align-window", dest="align_window", action="store_false")
    args = ap.parse_args()

    queries = json.loads(Path(args.queries).read_text())
    rows = gt_eval.load_rows(Path(args.jsonl))
    gt = gt_eval.evaluate_queries(queries, rows)

    # baseline net + stats
    rx0 = {n: container_net_rx(n) for n in ("asap-data-plane",)}
    stats_before = docker_stats()

    # Align replay to just after a fresh 60s wall-clock boundary so the full
    # ~40s replay lands inside ONE agent window (agent windows on 60s
    # boundaries). Wait until we're within the first ~3s of a 60s window.
    if args.align_window:
        phase = time.time() % 60.0
        if phase > 3.0:
            wait = 60.0 - phase + 0.5
            print(f"[{args.arm}] aligning replay to next 60s boundary (sleep {wait:.1f}s)",
                  file=sys.stderr)
            time.sleep(wait)

    # replay
    print(f"[{args.arm}] replaying {args.jsonl} -> {args.agent_endpoint}", file=sys.stderr)
    rc = subprocess.call([sys.executable, str(GCT / "run.py"), "replay",
                          "--jsonl", args.jsonl, "--endpoint", args.agent_endpoint,
                          "--pace-factor", "0", "--wall-clock-anchor"])
    if rc != 0:
        print(f"[{args.arm}] replay rc={rc}", file=sys.stderr)

    # Wait for the window to seal NATURALLY (do NOT restart the agent — a
    # restart cancels the export queue mid-flush and drops sketch envelopes).
    # Poll the sum query (cheapest, lands first) until it equals the GT total,
    # which signals the full window sealed + shipped + ingested.
    sum_q = next((x for x in queries if x["id"] == "sum-cpu"), None)
    sum_gt = gt.get("sum-cpu") if sum_q else None
    print(f"[{args.arm}] waiting up to {args.flush_wait}s for natural window seal+ship",
          file=sys.stderr)
    deadline = time.time() + args.flush_wait
    while time.time() < deadline:
        if sum_q and sum_gt:
            v, _, _ = sweep_query(args.backend, sum_q["metricsql"], None, span_s=240, step_s=30)
            if v is not None and abs(v - sum_gt) / abs(sum_gt) < 1e-6:
                print(f"[{args.arm}] full window landed (sum={v})", file=sys.stderr)
                break
        time.sleep(10)

    stats_after = docker_stats()
    rx1 = {n: container_net_rx(n) for n in ("asap-data-plane",)}
    wire_bytes = agent_wire_bytes()
    sent_points = agent_sent_points()

    results = []
    for qd in queries:
        spec = qd.get("gt", {})
        op = spec.get("op", "")
        group = None
        if op in ("topk_sum", "topk_count"):
            group = spec.get("key_label")
        elif op in ("quantile", "sum") and spec.get("by"):
            group = spec["by"][0]
        val, src, at_t = sweep_query(args.backend, qd["metricsql"], group)
        true = gt.get(qd["id"])
        rec = {"id": qd["id"], "kind": qd.get("kind"), "metricsql": qd["metricsql"],
               "true": true, "warm": val, "data_source": src, "at_time": at_t}
        # accuracy metric per family
        if op in ("topk_sum", "topk_count") and isinstance(val, dict) and isinstance(true, dict):
            rec["recall_at_k"] = recall_at_k(val.keys(), true.keys())
        elif op == "sum" and isinstance(true, dict) and isinstance(val, dict):
            errs = {k: relerr(val.get(k, 0.0), tv) for k, tv in true.items()}
            rec["rel_err_by_group"] = errs
            rec["max_rel_err"] = max(errs.values()) if errs else None
        elif isinstance(val, (int, float)) and isinstance(true, (int, float)):
            rec["rel_err"] = relerr(val, true)
        results.append(rec)
        print(f"  {qd['id']:26s} true={true} warm={val} src={src} t={at_t}", file=sys.stderr)

    summary = {
        "arm": args.arm,
        "results": results,
        "edge_stats_after": stats_after.get("asap-agent-a"),
        "data_plane_stats_after": stats_after.get("asap-data-plane"),
        "all_stats_after": stats_after,
        "agent_wire_bytes_to_backend": wire_bytes,
        "agent_sent_metric_points": sent_points,
        "data_plane_rx_delta_bytes": (
            (rx1["asap-data-plane"] - rx0["asap-data-plane"])
            if rx0["asap-data-plane"] and rx1["asap-data-plane"] else None),
    }
    Path(args.out).write_text(json.dumps(summary, indent=2, default=str) + "\n")
    print(f"[{args.arm}] wrote {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
