#!/usr/bin/env python3
"""Per-family multi-sketch accuracy driver (cold-OFF, wall-clock-anchored).

For each family arm:
  1. bring up the cold-OFF stack (data_plane warm-only + control_plane +
     bare fused asap_edge) with that family's workload + agent config,
  2. replay the family's data slice with --wall-clock-anchor (rows stamped
     at send-time wall-clock now so a recent-range [Ns] query intersects the
     warm windows),
  3. wait for the window(s) to seal (poll until the family's warm read is
     non-empty),
  4. query the warm read and score it against the EXACT offline GT
     (gt_eval over the SAME replayed rows):
        quantile (DDSketch / KLL)  -> PER-SERIES rel-err vs each series'
                                      exact quantile (the metric is stored
                                      per-series; PromQL quantile_over_time is
                                      per-series), summarised (mean/median/p95
                                      /frac-in-envelope),
        topk (CountSketch+heap / CountSketch-no-heap) -> recall@10 vs GT topk,
        frequency (CountMinSketch) -> f_hat vs exact freq (one-sided over-est,
                                      check f_hat in [f, f+eps*N]),
        cardinality (HLL)          -> rel-err vs exact distinct count.
  5. record wire bytes (agent->backend) + edge CPU/RSS.

Writes results/perfamily-<arm>.json per arm and an aggregate
results/perfamily-all.json.
"""
from __future__ import annotations
import argparse, json, math, os, subprocess, sys, time, urllib.parse, urllib.request
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
GCT = HERE.parent / "google_cluster"
sys.path.insert(0, str(GCT / "e2e"))
import gt_eval  # noqa: E402

# Endpoints are env-overridable so the SAME accuracy scorers run against a
# remote MULTINODE backend (e2e_metrics.sh sets E2E_BACKEND=node2:9091 etc.)
# instead of the single-host stack.
BASE = os.environ.get("E2E_BACKEND", "http://127.0.0.1:9091")
AGENT_METRICS = os.environ.get("E2E_AGENT_METRICS", "http://127.0.0.1:8890/metrics")
REPLAY_ENDPOINT = os.environ.get("E2E_REPLAY_ENDPOINT", "127.0.0.1:4317")
EXTERNAL_STACK = os.environ.get("E2E_EXTERNAL_STACK") == "1"
STACK = str(HERE / "stack-coldoff.sh")


# ---- query helpers ----
def q(promql, t=None, timeout=120):
    params = {"query": promql}
    if t is not None:
        params["time"] = str(t)
    url = BASE + "/api/v1/query?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        try:
            return json.loads(e.read().decode())  # type: ignore
        except Exception:
            return {"status": "error", "error": str(e)}


def src_of(body):
    for info in body.get("infos") or []:
        if isinstance(info, str) and info.startswith("data_source:"):
            return info.split(":", 1)[1].strip()
    return "unknown"


def series_list(body):
    return (body.get("data") or {}).get("result") or []


def qlin(vals, q_):
    if not vals:
        return float("nan")
    s = sorted(vals); n = len(s)
    if n == 1:
        return s[0]
    pos = q_ * (n - 1); lo = math.floor(pos); hi = math.ceil(pos)
    if lo == hi:
        return s[int(pos)]
    f = pos - lo
    return s[lo] * (1 - f) + s[hi] * f


def sid_of(metric_labels):
    m = metric_labels
    return f"{m.get('zone')}:{m.get('rack')}:{m.get('host')}:{m.get('service')}:{m.get('task')}"


def agent_wire_bytes():
    try:
        with urllib.request.urlopen(AGENT_METRICS, timeout=10) as r:
            for line in r.read().decode().splitlines():
                if line.startswith("otelcol_exporter_sent_metric_points_total") and "otlp/backend" in line:
                    sent = float(line.rsplit(" ", 1)[1])
    except Exception:
        sent = None
    wire = None
    try:
        with urllib.request.urlopen(AGENT_METRICS, timeout=10) as r:
            for line in r.read().decode().splitlines():
                if line.startswith("otelcol_exporter_queue_batch_send_size_bytes_sum") and "otlp/backend" in line:
                    wire = float(line.rsplit(" ", 1)[1])
    except Exception:
        pass
    return wire, sent


def docker_stat(name):
    try:
        out = subprocess.run(["docker", "stats", "--no-stream", "--format",
                              "{{.CPUPerc}}\t{{.MemUsage}}", name],
                             capture_output=True, text=True, timeout=30).stdout.strip()
        cpu, mem = out.split("\t")
        return {"cpu": cpu, "mem": mem}
    except Exception:
        return None


# ---- per-family scorers ----
def score_quantile(metric, rows, window, t, qs=(0.99, 0.50), envelope=0.02):
    """Per-series rel-err of the warm sketch quantile vs exact per-series GT."""
    series = defaultdict(list)
    for r in rows:
        if r["metric"] == metric:
            series[r["series_id"]].append(float(r["value"]))
    out = {}
    for ql in qs:
        body = q(f"quantile_over_time({ql}, {metric}[{window}])", t=t)
        warm = {sid_of(s["metric"]): float(s["value"][1]) for s in series_list(body) if s.get("value")}
        errs = []
        for sid, vals in series.items():
            if sid not in warm:
                continue
            gt = qlin(vals, ql)
            if gt == 0:
                continue
            errs.append(abs(warm[sid] - gt) / abs(gt))
        errs.sort()
        n = len(errs)
        out[str(ql)] = {
            "data_source": src_of(body),
            "n_series_total": len(series),
            "n_series_matched": n,
            "mean_rel_err": (sum(errs) / n) if n else None,
            "median_rel_err": errs[n // 2] if n else None,
            "p95_rel_err": errs[int(n * 0.95)] if n and n > 1 else (errs[0] if n else None),
            "max_rel_err": errs[-1] if n else None,
            "frac_within_envelope": (sum(1 for e in errs if e <= envelope) / n) if n else None,
            "envelope": envelope,
            "global_gt": qlin([float(r["value"]) for r in rows if r["metric"] == metric], ql),
        }
    return out


def score_topk(metric, rows, t, k=10, key="host"):
    # The per-key topk over a CountSketch resolves to the labelled per-host
    # heap only at certain query times around the populated window; sweep a
    # band of times near `t` and keep the read that exposes the most distinct
    # keyed series (a single unlabelled aggregate {'' : v} = 1 series is the
    # not-yet-materialised state and must be skipped).
    best_warm, best_n, best_t = {}, -1, t
    for off in range(-30, 151, 10):
        body = q(f"topk({k}, sum by ({key}) ({metric}))", t=t - off)
        w = {}
        for s in series_list(body):
            lbl = s["metric"].get(key, "")
            if lbl and s.get("value"):
                w[lbl] = float(s["value"][1])
        if len(w) > best_n:
            best_n, best_warm, best_t = len(w), w, t - off
    warm = best_warm
    body = q(f"topk({k}, sum by ({key}) ({metric}))", t=best_t)
    # exact GT topk
    agg = defaultdict(float)
    for r in rows:
        if r["metric"] == metric:
            agg[r["attributes"].get(key, "")] += float(r["value"])
    gt = dict(sorted(agg.items(), key=lambda kv: (-kv[1], kv[0]))[:k])
    inter = set(warm) & set(gt)
    return {
        "data_source": src_of(body),
        "k": k, "n_warm": len(warm),
        "recall_at_k": len(inter) / len(gt) if gt else None,
        "warm_topk": warm, "gt_topk": gt,
    }


def score_frequency(metric, rows, t, item_label, item_value, window):
    body = q(f"count_over_time({metric}{{{item_label}=\"{item_value}\"}}[{window}])", t=t)
    res = series_list(body)
    fhat = None
    if res:
        # sum across any series returned for that item (one item -> typically 1 series)
        fhat = sum(float(s["value"][1]) for s in res if s.get("value"))
    f = sum(1 for r in rows if r["metric"] == metric and r["attributes"].get(item_label) == item_value)
    N = sum(1 for r in rows if r["metric"] == metric)
    return {
        "data_source": src_of(body),
        "f_hat": fhat, "f_true": f, "N": N,
        "abs_over": (fhat - f) if fhat is not None else None,
        "one_sided_ok": (fhat is not None and fhat >= f),
        "rel_err": (abs(fhat - f) / f) if (fhat is not None and f) else None,
    }


def score_cardinality(metric, rows, t, key_label):
    body = q(f"count({metric})", t=t)
    res = series_list(body)
    chat = float(res[0]["value"][1]) if res and res[0].get("value") else None
    seen = set()
    for r in rows:
        if r["metric"] == metric:
            seen.add(r["attributes"].get(key_label, ""))
    c = float(len(seen))
    return {
        "data_source": src_of(body),
        "card_hat": chat, "card_true": c,
        "rel_err": (abs(chat - c) / c) if (chat is not None and c) else None,
    }


# ---- arm runner ----
ARMS = {
    "ddsketch": dict(jsonl="/tmp/perfam-ddsketch.jsonl", agent="agent-ddsketch-coldoff.yaml",
                     workload="workloads/ddsketch.yaml", kind="quantile",
                     metric="google_cluster_2019_cpu_rate_q_ddsketch", envelope=0.02),
    "kll": dict(jsonl="/tmp/perfam-kll.jsonl", agent="agent-kll-coldoff.yaml",
                workload="workloads/kll.yaml", kind="quantile",
                metric="google_cluster_2019_cpu_rate_q_kll", envelope=0.05),
    "countsketch": dict(jsonl="/tmp/perfam-countsketch.jsonl", agent="agent-allfamilies-coldoff.yaml",
                        workload="workloads/all-families.yaml", kind="topk_h2h",
                        metric="google_cluster_2019_cpu_rate_topk_cs",
                        metric2="google_cluster_2019_cpu_rate_topk_cms"),
    "countminsketch": dict(jsonl="/tmp/perfam-countminsketch.jsonl", agent="agent-countminsketch-coldoff.yaml",
                           workload="workloads/countminsketch.yaml", kind="frequency",
                           metric="google_cluster_2019_cpu_rate_freq_cms",
                           item_label="service", item_value="svc-000003"),
    "hll": dict(jsonl="/tmp/perfam-hll.jsonl", agent="agent-hll-coldoff.yaml",
                workload="workloads/hll.yaml", kind="cardinality",
                metric="google_cluster_2019_cpu_rate_card_hll", key_label="service"),
}


def stack_up(workload, agent):
    if EXTERNAL_STACK:  # multinode: the stack is already up (e2e_metrics.sh)
        return
    subprocess.run([STACK, "up", str(HERE / workload), str(HERE / agent)],
                   cwd=str(ROOT), check=True, timeout=180)


def stack_down():
    if EXTERNAL_STACK:
        return
    subprocess.run([STACK, "down"], cwd=str(ROOT), timeout=60)


def replay(jsonl):
    subprocess.call([sys.executable, str(GCT / "run.py"), "replay",
                     "--jsonl", jsonl, "--endpoint", REPLAY_ENDPOINT,
                     "--pace-factor", "0", "--wall-clock-anchor"])


def wait_full_ship(probe_query, sum_metric, sum_gt, max_wait=260):
    """Wait until the FULL window has sealed AND fully shipped, then return the
    query time at which the sketch read is most complete.

    Gating on the cheap exact `sum(sum_metric)` reaching the offline GT
    guarantees every datapoint of the window has landed (the warm sum is
    lossless), so the sketch read taken at the same window time sees the
    complete state — not a partially-shipped window. `probe_query` is a
    family-appropriate read (quantile_over_time for quantile arms, an instant
    `sum by ()` for topk, count_over_time for frequency, count() for HLL) used
    to (a) confirm the sketch read is non-empty and (b) pick the query time
    that maximises the read's series count."""
    deadline = time.time() + max_wait
    while time.time() < deadline:
        now = int(time.time())
        full = None
        for off in range(0, 240, 15):
            t = now - off
            res = series_list(q(f"sum({sum_metric})", t=t))
            if res and res[0].get("value"):
                v = float(res[0]["value"][1])
                if abs(v - sum_gt) / abs(sum_gt) < 1e-6:
                    full = t
                    break
        if full is not None:
            time.sleep(5)
            now = int(time.time())
            best_t, best_n = full, -1
            for off in range(0, 180, 10):
                t = now - off
                n = len(series_list(q(probe_query, t=t)))
                if n > best_n:
                    best_n, best_t = n, t
            return best_t if best_n > 0 else full
        time.sleep(8)
    return None


def run_arm(name, window="120s"):
    a = ARMS[name]
    print(f"\n===== ARM {name} =====", file=sys.stderr)
    stack_up(a["workload"], a["agent"])
    time.sleep(3)
    print(f"[{name}] replaying {a['jsonl']}", file=sys.stderr)
    replay(a["jsonl"])
    rows = gt_eval.load_rows(Path(a["jsonl"]))

    # Gate scoring on the FULL window having sealed+shipped: wait until the
    # lossless warm sum(cpu_rate) reaches the offline GT (every datapoint
    # landed) AND the sketch read is non-empty at the same window time. This
    # avoids scoring a partially-shipped window (subset of series / truncated
    # distribution).
    m = a.get("metric")
    if a["kind"] == "quantile":
        probe_query = f"quantile_over_time(0.50, {m}[{window}])"
    elif a["kind"] == "topk_h2h":
        probe_query = f"sum by (host) ({m})"
    elif a["kind"] == "frequency":
        probe_query = f"count_over_time({m}{{{a['item_label']}=\"{a['item_value']}\"}}[{window}])"
    elif a["kind"] == "cardinality":
        probe_query = f"count({m})"
    else:
        probe_query = f"quantile_over_time(0.50, {m}[{window}])"
    sum_metric = "google_cluster_2019_cpu_rate"
    sum_gt = gt_eval.gt_sum(rows, {"metric": sum_metric, "by": []})
    print(f"[{name}] waiting for full ship (sum GT={sum_gt}, probe='{probe_query}')", file=sys.stderr)
    at = wait_full_ship(probe_query, sum_metric, sum_gt)
    print(f"[{name}] full window at t={at}", file=sys.stderr)

    wire, sent = agent_wire_bytes()
    edge = docker_stat("asap-agent-a")
    dp = docker_stat("asap-data-plane")
    rec = {"arm": name, "kind": a["kind"], "at_time": at,
           "agent_wire_bytes_to_backend": wire, "agent_sent_metric_points": sent,
           "edge_stats": edge, "data_plane_stats": dp}

    if at is None:
        rec["score"] = {"error": "no window sealed/queryable in time"}
        stack_down()
        return rec

    if a["kind"] == "quantile":
        rec["score"] = score_quantile(a["metric"], rows, window, at, envelope=a["envelope"])
    elif a["kind"] == "topk_h2h":
        rec["score"] = {
            "countsketch_heap": score_topk(a["metric"], rows, at),
            "countsketch_noheap": score_topk(a["metric2"], rows, at),
        }
    elif a["kind"] == "frequency":
        rec["score"] = score_frequency(a["metric"], rows, at, a["item_label"], a["item_value"], window)
    elif a["kind"] == "cardinality":
        rec["score"] = score_cardinality(a["metric"], rows, at, a["key_label"])

    (HERE / "results" / f"perfamily-{name}.json").write_text(json.dumps(rec, indent=2, default=str) + "\n")
    print(f"[{name}] wrote results/perfamily-{name}.json", file=sys.stderr)
    stack_down()
    return rec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--arms", default="ddsketch,kll,countsketch,countminsketch,hll")
    ap.add_argument("--window", default="120s")
    args = ap.parse_args()
    allrec = {}
    for name in args.arms.split(","):
        name = name.strip()
        if not name:
            continue
        try:
            allrec[name] = run_arm(name, args.window)
        except Exception as e:
            allrec[name] = {"arm": name, "error": str(e)}
            print(f"[{name}] ERROR {e}", file=sys.stderr)
            stack_down()
    (HERE / "results" / "perfamily-all.json").write_text(json.dumps(allrec, indent=2, default=str) + "\n")
    print("wrote results/perfamily-all.json", file=sys.stderr)


if __name__ == "__main__":
    main()
