#!/usr/bin/env python3
"""Headline accuracy-vs-cost Pareto sweep (Fig 1 of docs/evaluation-plan-figures.md).

Runs ONE consistent sweep on the real ASAP stack against the 2019 Google
cluster trace, producing operating points (cost, accuracy) where:

  y = accuracy = 1 - p99_rel_err of quantile_over_time(0.99, cpu_rate[30s])
                 vs the dataset's TRUE pooled p99.
  x = total cost = equal-weight mean of (edge CPU cores, wire egress KB/s),
                   each normalized so the raw-forwarding anchor = 1.0.

Topology per arm (fresh backend + producer per arm — delta-query
queryability requires a clean per-series-base backend):

    otel-app (SDK pre-aggregation: raw-buffer | dd-full | dd-delta,
              producer-side warm-sample-p)
        --OTLP/gRPC(:4317, gzip)-->  data_plane backend (sketch store)
        --PromQL(:9091)-->  query p99  vs  exact offline pooled GT (gt_eval)

Cost is measured per arm:
  * wire egress  = iptables byte counter on tcp dport 4317 (loopback OTLP),
                   divided by the replay wall-clock -> KB/s.
  * edge CPU     = producer process utime+stime delta (clock ticks -> seconds),
                   divided by replay wall-clock -> cores.

The two knobs the sweep exercises:
  * sampling p   -> -warm-sample-p (drops raw points before the SDK
                    aggregation; admitted ingest ~ p, smaller wire, ~constant
                    pooled accuracy until small-N degrades).
  * delta/ε_cdm  -> -agg dd-delta (SDK ships delta-transmitted sketch state;
                    cuts steady-state wire vs dd-full). The trace is LOOPED so
                    multiple SDK windows elapse and the per-window delta benefit
                    emerges at steady state (the first-window-full-state is
                    amortized).

NB: this minimal topology runs the SDK pre-aggregation as the "edge" (the
otel-app SDK View IS the edge sketch). A separate asap-otel collector hop is
NOT inserted: it would only forward the same SDK sketch envelope to the same
backend, adding a constant per-arm cost that cancels in the raw-normalized
ratio. We state this explicitly so the cost axis is unambiguous.
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import gt_eval  # noqa: E402

REPO = Path("/mydata/ASAPCollector")
BACKEND_BIN = Path("/mydata/ASAPQuery-backend/target/release/data_plane")
OTEL_APP = REPO / "otel-app" / "otel-app"
METRIC = "google_cluster_2019_cpu_rate"
# Wide range so the query MERGES all tumbling sub-windows of the single
# replay pass into one pooled p99 (matches the global pooled GT). The
# headline query is quantile_over_time(0.99, cpu_rate[30s]); we widen the
# range only so the multi-window single pass is queryable as one pool.
QUERY = f"quantile_over_time(0.99, {METRIC}[3600s])"
GRPC_PORT = 4317
HTTP_PORT_BASE = 4318
QUERY_PORT = 9091
IPT_CHAIN = "PARETOBW"
CLK_TCK = os.sysconf("SC_CLK_TCK")


# ---------------------------------------------------------------------------
# wire metering via iptables (loopback byte counter on OTLP ingest port)
# ---------------------------------------------------------------------------
def ipt(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["sudo", "-n", "iptables", *args],
                          capture_output=True, text=True)


def ipt_setup() -> None:
    ipt("-N", IPT_CHAIN)
    ipt("-F", IPT_CHAIN)
    ipt("-A", IPT_CHAIN, "-p", "tcp", "--dport", str(GRPC_PORT))
    if ipt("-C", "INPUT", "-j", IPT_CHAIN).returncode != 0:
        ipt("-I", "INPUT", "-j", IPT_CHAIN)
    ipt("-Z", IPT_CHAIN)


def ipt_teardown() -> None:
    ipt("-D", "INPUT", "-j", IPT_CHAIN)
    ipt("-F", IPT_CHAIN)
    ipt("-X", IPT_CHAIN)


def ipt_zero() -> None:
    ipt("-Z", IPT_CHAIN)


def ipt_bytes() -> int:
    out = ipt("-L", IPT_CHAIN, "-v", "-n", "-x").stdout
    for line in out.splitlines():
        if f"dpt:{GRPC_PORT}" in line:
            return int(line.split()[1])
    return 0


# ---------------------------------------------------------------------------
# process CPU sampling via /proc/<pid>/stat (utime+stime, fields 14,15)
# ---------------------------------------------------------------------------
def proc_cpu_ticks(pid: int) -> int:
    try:
        parts = Path(f"/proc/{pid}/stat").read_text().split()
        # field indices 13,14 (0-based) = utime,stime
        return int(parts[13]) + int(parts[14])
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# backend lifecycle
# ---------------------------------------------------------------------------
def port_free(port: int) -> bool:
    out = subprocess.run(["ss", "-ltn"], capture_output=True, text=True).stdout
    return f":{port} " not in out and f":{port}\n" not in out and \
        not any(f":{port}" in ln.split()[3] for ln in out.splitlines()[1:]
                if len(ln.split()) > 3)


def wait_ports_free(ports: list[int], timeout_s: float = 20.0) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if all(port_free(p) for p in ports):
            return True
        time.sleep(0.5)
    return False


def start_backend(streaming_cfg: Path, logdir: Path) -> subprocess.Popen:
    log = open(logdir / "data_plane.log", "w")
    env = dict(os.environ, RUST_LOG="info")
    p = subprocess.Popen(
        [str(BACKEND_BIN),
         "--streaming-config", str(streaming_cfg),
         "--enable-otel-ingest",
         "--otel-grpc-port", str(GRPC_PORT),
         "--otel-http-port", str(HTTP_PORT_BASE),
         "--query-port", str(QUERY_PORT),
         "--prometheus-scrape-interval", "60",
         "--output-dir", str(logdir),
         "--log-level", "warn"],
        stdout=log, stderr=subprocess.STDOUT, env=env)
    return p


def await_query_ready(timeout_s: float = 30.0) -> bool:
    url = f"http://127.0.0.1:{QUERY_PORT}/api/v1/query?" + urllib.parse.urlencode(
        {"query": "vector(1)"})
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=3) as r:
                if r.status == 200:
                    return True
        except Exception:
            time.sleep(0.5)
    return False


def query_p99() -> float | None:
    url = f"http://127.0.0.1:{QUERY_PORT}/api/v1/query?" + urllib.parse.urlencode(
        {"query": QUERY})
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            body = json.loads(r.read().decode())
    except Exception as e:
        print(f"  query error: {e}", file=sys.stderr)
        return None
    data = body.get("data") or {}
    res = data.get("result") or []
    if not res:
        return None
    # pooled single-series -> one result; if multiple, take max (global p99
    # is dominated by the heaviest series in the single-series-pool design,
    # but the pool design yields exactly one series).
    vals = []
    for s in res:
        v = s.get("value")
        if v and v[1] is not None:
            vals.append(float(v[1]))
    return max(vals) if vals else None


# ---------------------------------------------------------------------------
# one arm
# ---------------------------------------------------------------------------
def run_arm(name: str, agg: str, p: float, scale: float, duration_s: float,
            loop: bool, sdk_window: str, streaming_cfg: Path,
            trace_csv: Path, logroot: Path) -> dict:
    logdir = logroot / name
    logdir.mkdir(parents=True, exist_ok=True)

    # fresh backend: ensure prior backend's ports are released first
    if not wait_ports_free([GRPC_PORT, QUERY_PORT, HTTP_PORT_BASE]):
        raise RuntimeError(f"{name}: ports still bound before backend start")
    bk = start_backend(streaming_cfg, logdir)
    if not await_query_ready() or bk.poll() is not None:
        try:
            bk.send_signal(signal.SIGTERM)
        except Exception:
            pass
        raise RuntimeError(f"{name}: backend not ready (pid alive="
                           f"{bk.poll() is None})")

    ipt_zero()
    plog = open(logdir / "producer.log", "w")
    cmd = [str(OTEL_APP),
           "-target", f"127.0.0.1:{GRPC_PORT}",
           "-trace-file", str(trace_csv),
           "-trace-metric-name", METRIC,
           "-trace-scale", str(scale),
           f"-trace-loop={'true' if loop else 'false'}",
           "-agg", agg,
           "-sdk-window", sdk_window,
           "-warm-sample-p", str(p),
           "-five-sketch=false", "-freshness-probes=false",
           "-seed", "12345"]
    if duration_s > 0:
        cmd += ["-duration", f"{int(duration_s)}s"]
    prod = subprocess.Popen(cmd, stdout=plog, stderr=subprocess.STDOUT)

    t0 = time.time()
    cpu0 = proc_cpu_ticks(prod.pid)
    # poll cpu while running so we get the cumulative ticks before exit
    last_cpu = cpu0
    while prod.poll() is None:
        c = proc_cpu_ticks(prod.pid)
        if c:
            last_cpu = c
        time.sleep(0.25)
        # generous guard: single-pass at the sweep scale is ~20-25s; cap at
        # max(duration, 120) + 30 so a slow pass is never truncated early.
        if time.time() - t0 > max(duration_s, 120) + 30:
            prod.send_signal(signal.SIGTERM)
            break
    t1 = time.time()
    wall = t1 - t0
    cpu_ticks = max(last_cpu - cpu0, 0)
    cpu_cores = (cpu_ticks / CLK_TCK) / wall if wall > 0 else 0.0

    wire_bytes = ipt_bytes()
    wire_kbps = (wire_bytes / 1024.0) / wall if wall > 0 else 0.0

    # parse REPLAY_STATS (last line) for admitted/candidate
    admitted = candidate = 0
    for line in (logdir / "producer.log").read_text().splitlines():
        if "REPLAY_STATS" in line:
            for tok in line.split():
                if tok.startswith("candidate="):
                    candidate = int(tok.split("=")[1])
                elif tok.startswith("admitted="):
                    admitted = int(tok.split("=")[1])
    adm_per_s = admitted / wall if wall > 0 else 0.0

    # let the open window seal + scrape lookback catch it
    time.sleep(12)
    p99 = query_p99()
    if p99 is None:
        time.sleep(6)
        p99 = query_p99()

    bk.send_signal(signal.SIGTERM)
    try:
        bk.wait(timeout=10)
    except subprocess.TimeoutExpired:
        bk.kill()
    time.sleep(1.0)

    return {
        "name": name, "agg": agg, "p": p, "scale": scale, "loop": loop,
        "wall_s": round(wall, 2),
        "wire_bytes": wire_bytes, "wire_kbps": round(wire_kbps, 3),
        "cpu_ticks": cpu_ticks, "cpu_cores": round(cpu_cores, 4),
        "candidate": candidate, "admitted": admitted,
        "admitted_per_s": round(adm_per_s, 1),
        "p99": p99,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trace-csv", type=Path, default=Path("/tmp/gct-cpu-pooled.csv"))
    ap.add_argument("--jsonl", type=Path, default=Path("/tmp/gct-otlp.jsonl"))
    ap.add_argument("--streaming-config", type=Path,
                    default=Path("/tmp/pareto/streaming-config.yaml"))
    ap.add_argument("--out-dir", type=Path, default=Path("/tmp/pareto/results"))
    ap.add_argument("--duration", type=float, default=0.0,
                    help="per-arm producer duration cap; 0 = single full pass")
    args = ap.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    logroot = args.out_dir / "logs"
    logroot.mkdir(parents=True, exist_ok=True)

    # exact pooled ground truth p99 over the whole trace
    rows = gt_eval.load_rows(args.jsonl)
    gt_p99 = gt_eval.gt_quantile(rows, {"metric": METRIC, "q": 0.99, "by": []})
    print(f"GT pooled p99 = {gt_p99:.6f}  ({len(rows)} jsonl rows)", file=sys.stderr)

    # SINGLE-PASS grid (loop=False): each arm replays the 100k-point trace
    # exactly once so sampling p genuinely thins the realized sample set
    # (looping would re-cover the full value distribution and hide ε_s).
    # SCALE compresses the ~31d trace timestamps into ~20s wall; SDK_WIN=5s
    # yields ~4 tumbling sub-windows the wide-range query merges into one
    # pooled p99 (matching the global pooled GT).
    SCALE = 134_000
    SDK_WIN = "5s"
    arms = [
        # raw anchor (no sketch — raw-buffer ships every admitted sample).
        # accuracy is 1.0 BY DEFINITION (raw forwarding is exact); this arm
        # only sets the cost normalizer (its wire+CPU).
        dict(name="raw",            agg="raw-buffer", p=1.0, anchor=True),
        # DDSketch full envelope (ε_cdm=0)
        dict(name="dd_full_p100",   agg="dd-full",    p=1.0),
        dict(name="dd_full_p050",   agg="dd-full",    p=0.5),
        dict(name="dd_full_p025",   agg="dd-full",    p=0.25),
        dict(name="dd_full_p010",   agg="dd-full",    p=0.1),
        # DDSketch delta (ε_cdm>0 sub-window emission)
        dict(name="dd_delta_p100",  agg="dd-delta",   p=1.0),
        dict(name="dd_delta_p050",  agg="dd-delta",   p=0.5),
        dict(name="dd_delta_p025",  agg="dd-delta",   p=0.25),
        dict(name="dd_delta_p010",  agg="dd-delta",   p=0.1),
    ]

    ipt_setup()
    results = []
    try:
        for a in arms:
            anchor = a.get("anchor", False)
            print(f"\n=== arm {a['name']} (agg={a['agg']} p={a['p']}"
                  f"{' ANCHOR' if anchor else ''}) ===", file=sys.stderr)
            r = run_arm(a["name"], a["agg"], a["p"], SCALE, args.duration,
                        loop=False, sdk_window=SDK_WIN,
                        streaming_cfg=args.streaming_config,
                        trace_csv=args.trace_csv, logroot=logroot)
            r["anchor"] = anchor
            if anchor:
                # raw forwarding is exact by definition
                r["rel_err"] = 0.0
                r["accuracy"] = 1.0
                r["p99_measured"] = r["p99"]
            elif r["p99"] is not None and gt_p99 > 0:
                r["rel_err"] = abs(r["p99"] - gt_p99) / gt_p99
                r["accuracy"] = 1.0 - r["rel_err"]
            else:
                r["rel_err"] = None
                r["accuracy"] = None
            print(f"  -> p99={r['p99']} rel_err={r['rel_err']} "
                  f"cpu_cores={r['cpu_cores']} wire_kbps={r['wire_kbps']} "
                  f"adm/s={r['admitted_per_s']}", file=sys.stderr)
            results.append(r)
    finally:
        subprocess.run(["pkill", "-f", "data_plane --streaming-config"],
                       capture_output=True)
        ipt_teardown()

    # normalize to raw anchor
    raw = next(r for r in results if r["name"] == "raw")
    raw_cpu = raw["cpu_cores"] or 1e-9
    raw_wire = raw["wire_kbps"] or 1e-9
    for r in results:
        r["cpu_norm"] = round(r["cpu_cores"] / raw_cpu, 4)
        r["wire_norm"] = round(r["wire_kbps"] / raw_wire, 4)
        r["cost_total"] = round(0.5 * (r["cpu_norm"] + r["wire_norm"]), 4)

    payload = {"gt_p99": gt_p99, "raw_anchor": raw["name"],
               "cost_def": ("total cost = 0.5*(cpu_norm + wire_norm), each "
                            "normalized to raw=1.0; cpu = producer/edge "
                            "utime+stime cores, wire = OTLP gzipped bytes on "
                            "tcp dport 4317 (iptables) / wall. Backend CPU is "
                            "OUT of total (same backend binary per arm). Both "
                            "sub-axes reported so either can be dropped."),
               "query": QUERY,
               "results": results}
    (args.out_dir / "sweep-results.json").write_text(
        json.dumps(payload, indent=2) + "\n")
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
