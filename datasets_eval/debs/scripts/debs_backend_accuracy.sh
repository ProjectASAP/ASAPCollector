#!/usr/bin/env bash
# Real-dataset QUERY ACCURACY through the REAL ASAPQuery-backend, on DEBS 2022.
# Maps DEBS trading events → OTLP for the four sketch families, replays them
# through a fused asap_edge agent into data_plane, then QUERIES the warm backend
# and scores each answer against the EXACT ground truth computed offline from the
# same DEBS rows:
#   quantile (DDSketch+KLL) · topk (CountSketch) · distinct (HLL) · freq (CountMin)
#
# Uses the multisketch single-host stack.sh (agent + data_plane + control_plane),
# the same real backend the cluster runs. Usage: debs_backend_accuracy.sh [n_events]
set -uo pipefail
ROOT=/mydata/ASAPCollector
MS="${ROOT}/datasets_eval/multisketch"
DEBS_CSV="${DEBS_CSV:-${ROOT}/datasets_eval/debs/data/debs2022-gc-trading-day-08-11-21.csv}"
# N events. 500k DEBS events ≈ 1.55M OTLP points, which SEND in ~23s — well
# within one boundary-aligned 60s window (measured). The wall-clock-anchor
# collapses every row into ONE window; boundary alignment (below) gives the send
# a full 60s of headroom so nothing seals mid-replay. A smaller slice (e.g. 60k)
# is the flat intro of the trace — no heavy hitters, half the prices zero — so
# keep the rich 500k slice; it fits.
N="${1:-500000}"
JSONL=/tmp/debs_otlp.jsonl; GT=/tmp/debs_gt.json
BASE="http://127.0.0.1:9091"
trap '[ -n "${KEEP_STACK:-}" ] || "${MS}/stack.sh" down >/dev/null 2>&1 || true' EXIT

echo "== 1. map DEBS ($N events) → OTLP + exact GT =="
python3 "${ROOT}/datasets_eval/debs/scripts/debs_otlp_map.py" "$DEBS_CSV" "$N" "$JSONL" "$GT"

echo "== 2. bring up the real all-families stack (agent + data_plane + control_plane) =="
AGENT_CFG="${AGENT_CFG:-${MS}/agent-allfamilies-coldoff.yaml}"
echo "   agent config: ${AGENT_CFG}"
"${MS}/stack.sh" up "${MS}/workloads/all-families.yaml" "${AGENT_CFG}" >/tmp/debs_stack.log 2>&1
sleep 8

echo "== 3. replay DEBS OTLP → agent :4317 (wall-clock-anchored) =="
# Align to a fresh 60s wall-clock boundary so the collapsed instant sits at the
# START of a window — the whole send then has a full 60s of headroom before the
# window seals (otherwise a mid-window anchor can straddle the seal and drop the
# tail of the replay).
python3 -c 'import time; t=time.time(); s=60-(t%60); s=s if s>5 else s+60; print(f"   aligning to next 60s boundary in {s:.1f}s"); time.sleep(s)'
# --anchor-span-s 50: spread the points across 50s (distinct ns each) inside the
# one window, so the count-type sketches (topk_cs/freq_cms carry value==1.0 per
# event) keep per-key multiplicity instead of deduping every event of a key onto
# one identical (series, ts, value) sample.
python3 "${ROOT}/datasets_eval/google_cluster/run.py" replay \
  --jsonl "$JSONL" --endpoint 127.0.0.1:4317 --pace-factor 0 --wall-clock-anchor \
  --anchor-span-s 45 \
  >/tmp/debs_replay.log 2>&1
echo "   waiting for the warm window to seal…"; sleep 75

echo "== 4. query the backend + score vs exact GT =="
python3 - "$BASE" "$GT" <<'PY'
import sys, json, time, urllib.request, urllib.parse
BASE, GT = sys.argv[1], sys.argv[2]
gt = json.load(open(GT))
def q(promql, t=None):
    p = {"query": promql}
    if t is not None: p["time"] = t
    u = f"{BASE}/api/v1/query?" + urllib.parse.urlencode(p)
    try:
        body = json.loads(urllib.request.urlopen(u, timeout=60).read().decode())
        if isinstance(body, dict):
            return body.get("data", {}).get("result", []) or []
    except Exception:
        pass
    return []
def val(res):
    try:
        return float(res[0]["value"][1]) if res else None
    except Exception:
        return None
# Instant sketch reads only resolve against the window whose seal is 'current' —
# after it rolls the same query goes empty. Sweep time= backward to the offset
# that returns the MOST series, i.e. the sealed replay window.
def best_time(probe):
    now = int(time.time()); best_t, best_n = now, -1
    for off in range(0, 200, 8):
        n = len(q(probe, t=now-off))
        if n > best_n: best_n, best_t = n, now-off
    return best_t, best_n
# Probe with the instant topk aggregate (the bare metric never returns series).
bt, bn = best_time('topk(50, google_cluster_2019_cpu_rate_topk_cs)')
print(f"# sealed-window probe: t=now-{int(time.time())-bt}s has {bn} topk_cs series")
# landed-event sanity: total inserts the CountSketch actually saw this window.
tot = val(q('sum(count_over_time(google_cluster_2019_cpu_rate_topk_cs[300s]))', t=bt))
print(f"# CountSketch landed events this window: {tot}  (GT events sent: {gt['events']})")

print(f"\n{'query':<26} {'backend':>14} {'exact GT':>14} {'accuracy':>22}")
print("-"*80)

# distinct (HLL) — cardinality of distinct symbols
r = q('count(google_cluster_2019_cpu_rate_card_hll)', t=bt)
est = val(r); tru = gt["distinct"]
acc = f"rel_err {abs(est-tru)/tru:.2%}" if est else f"({r})"
print(f"{'distinct (HLL)':<26} {str(est):>14} {tru:>14} {acc:>22}")

# frequency of one key — from the CountSketch heap (point-frequency sketch).
sym = gt["freq_query"]["symbol"]; tru = gt["freq_query"]["true_count"]
est = val(q(f'google_cluster_2019_cpu_rate_topk_cs{{item="{sym}"}}', t=bt))
acc = f"rel_err {abs(est-tru)/tru:.2%}" if est else f"(not in heap)"
print(f"{'freq '+sym[:16]+' (CS)':<26} {str(est):>14} {tru:>14} {acc:>22}")

# topk (CountSketch) — recall@10 + dump the actual returned items/counts.
r = q('topk(10, google_cluster_2019_cpu_rate_topk_cs)', t=bt)
got_items = [(s["metric"].get("item"), s["value"][1]) for s in r]
got = {i for i, _ in got_items if i}
truk = {d["symbol"] for d in gt["topk10"]}
rec = len(got & truk) / len(truk) if truk else 0
print(f"{'topk@10 (CountSketch)':<26} {str(len(got & truk))+'/'+str(len(truk)):>14} {str(len(truk)):>14} {'recall '+f'{rec:.0%}':>22}")
print(f"   backend top: {got_items[:5]}")
print(f"   GT      top: {[(d['symbol'], d['count']) for d in gt['topk10'][:5]]}")

# quantile (DDSketch, KLL) — dump p50/p90/p99 to see if it's a scale/tail issue.
for fam, m in [("DDSketch", "q_ddsketch"), ("KLL", "q_kll")]:
    for ql, key in [(0.50, "p50"), (0.90, "p90"), (0.99, "p99")]:
        est = val(q(f'quantile_over_time({ql}, google_cluster_2019_cpu_rate_{m}[300s])', t=bt))
        tru = gt["quantile"][key]
        acc = f"rel_err {abs(est-tru)/tru:.2%}" if est and tru else f"(empty)"
        print(f"{fam+' '+key:<26} {str(round(est,2) if est else est):>14} {str(tru):>14} {acc:>22}")
PY
echo "== done =="
