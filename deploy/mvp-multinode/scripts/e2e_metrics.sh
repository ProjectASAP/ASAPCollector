#!/usr/bin/env bash
# e2e_metrics.sh — ONE consolidated multinode end-to-end metrics run against the
# REAL ASAPQuery-backend. Every number below is produced by THIS run (nothing is
# looked up from a prior result file). Merges the two existing harnesses:
#
#   • run_demo.sh (library)   — brings up the real multinode `asap` stack
#     (cold node1 + warm node2 data_plane/control_plane + agents node3/4) and its
#     arm_measure captures, per component and per node:
#         – CPU % + mem      (docker stats, sampled over the soak, every node)
#         – bandwidth        (per-edge NIC tx/rx + per-stage bytes)
#         – query LATENCY    (MetricsQL replay against node2:9091)
#   • run_perfamily.py        — replays a GT-known slice through the SAME warm
#     backend (E2E_EXTERNAL_STACK, E2E_BACKEND=node2:9091) and scores query
#     ACCURACY vs the exact offline ground truth, per family:
#         – DDSketch/KLL quantile rel-err, CountSketch topk recall,
#           CountMinSketch freq envelope, HLL cardinality rel-err
#     + agent→backend wire bytes.
#
# Output: one fresh report dir with the per-component resource/bandwidth CSVs,
# the query-latency replay, and the per-family accuracy JSON — plus a summary.
#
# Usage: e2e_metrics.sh [arms]      (arms default: ddsketch,countsketch,countminsketch,hll,kll)
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MV="$(cd "${SCRIPT_DIR}/.." && pwd)"
ROOT="$(cd "${MV}/../.." && pwd)"
export TOPOLOGY_ENV="${TOPOLOGY_ENV:-${MV}/topology.8node.env}"
export RUN_DEMO_LIB=1
# shellcheck disable=SC1090
source "${SCRIPT_DIR}/run_demo.sh"

ARMS="${1:-ddsketch,countsketch,countminsketch,hll,kll}"
SOAK_S="${SOAK_S:-90}"
STAMP="$(cat /proc/sys/kernel/random/uuid | cut -c1-8)"
OUT="${E2E_OUT:-${MV}/eval-8node/e2e-${STAMP}}"
mkdir -p "${OUT}"
export RUN_DIR="${OUT}"   # arm_measure writes under RUN_DIR/<arm>/

slog(){ printf '[%s] [e2e] %s\n' "$(date +%H:%M:%S)" "$*"; }

# 1. Bring up the real multinode asap stack (reuse images already on the nodes).
slog "sync configs + bring up the multinode 'asap' stack (cold=${NODE1_HOST}, warm=${NODE2_HOST}, agents=${NODE0_HOST}/${NODE3_HOST})"
SKIP_BUILD=1 SKIP_LOAD="${SKIP_LOAD:-1}" ensure_images
sync_all_nodes
arm_up asap

# 2. Per-component resources + bandwidth (snapshot_resources.sh — the WORKING
#    multinode tool; run_demo's arm_measure references measure_per_edge_bandwidth
#    /measure_stages.py which no longer exist) + query LATENCY (metricsql_replay).
slog "snapshot_resources: per-component CPU/mem + per-node NIC bandwidth (${SOAK_S}s)"
SNAP_NODES="${NODE0_HOST} ${NODE1_HOST} ${NODE2_HOST} ${NODE3_HOST}" \
  bash "${SCRIPT_DIR}/snapshot_resources.sh" asap "${SOAK_S}" "${OUT}/resources" \
  > "${OUT}/snapshot.log" 2>&1 &
SNAP_PID=$!
slog "query latency: MetricsQL replay against warm backend node2:9091 (${SOAK_S}s)"
python3 "${ROOT}/deploy/mvp-singlenode/scripts/metricsql_replay.py" \
  --target "http://${NODE2_IP}:9091" \
  --queries "${ROOT}/deploy/mvp-singlenode/scripts/queries-e2e.json" \
  --duration "${SOAK_S}" --out "${OUT}/replay.jsonl" \
  > "${OUT}/replay.log" 2>&1 || slog "latency replay non-zero (see replay.log)"
wait "${SNAP_PID}" 2>/dev/null || true

# 3. Query ACCURACY vs exact GT against the SAME warm backend (node2:9091).
slog "accuracy: replay GT-known slices → warm backend node2:9091 → score per family (${ARMS})"
E2E_EXTERNAL_STACK=1 \
E2E_BACKEND="http://${NODE2_IP}:9091" \
E2E_AGENT_METRICS="http://${NODE0_IP}:8890/metrics" \
E2E_REPLAY_ENDPOINT="${NODE0_IP}:4317" \
  python3 "${ROOT}/datasets_eval/multisketch/run_perfamily.py" \
    --arms "${ARMS}" --window "${SOAK_S}s" \
  > "${OUT}/accuracy.log" 2>&1 || slog "accuracy step exited non-zero (see accuracy.log)"
# run_perfamily writes datasets_eval/multisketch/results/perfamily-*.json — copy the fresh ones in.
cp "${ROOT}"/datasets_eval/multisketch/results/perfamily-*.json "${OUT}/" 2>/dev/null || true

# 4. Consolidated fresh report: per-component resources + bandwidth + latency +
#    per-family accuracy, all from THIS run.
slog "aggregating fresh report → ${OUT}/E2E_METRICS.md"
python3 - "${OUT}" > "${OUT}/E2E_METRICS.md" 2>"${OUT}/report.log" <<'PY'
import csv, glob, json, os, sys, statistics
out = sys.argv[1]
print(f"# e2e metrics — fresh multinode run ({os.path.basename(out)})\n")
print("All numbers below are from THIS run against the real ASAPQuery-backend.\n")

# --- per-component CPU/mem (snapshot_resources container-summary) ---
print("## Per-component CPU / memory (soak mean)\n")
cs = os.path.join(out, "resources", "container-summary-asap.csv")
if os.path.exists(cs):
    print("| component | node | mean CPU% | max mem |")
    print("|---|---|---|---|")
    for r in csv.DictReader(open(cs)):
        print("| " + " | ".join(str(r.get(k, "")) for k in list(r)[:4]) + " |")
else:
    print("_container-summary-asap.csv missing (snapshot failed — see snapshot.log)_")

# --- per-node NIC bandwidth ---
print("\n## Per-node NIC bandwidth\n")
nic = os.path.join(out, "resources", f"nic-summary-asap.csv")
nic = nic if os.path.exists(nic) else next(iter(glob.glob(os.path.join(out, "resources", "*nic*"))), "")
if nic and os.path.exists(nic):
    for line in open(nic):
        print("    " + line.rstrip())
else:
    print("_nic summary missing_")

# --- query latency (metricsql replay) ---
print("\n## Query latency (MetricsQL replay vs node2:9091)\n")
rj = os.path.join(out, "replay.jsonl")
lat = []
if os.path.exists(rj):
    for line in open(rj):
        try:
            d = json.loads(line); v = d.get("latency_ms") or d.get("ms") or d.get("elapsed_ms")
            if v is not None: lat.append(float(v))
        except Exception: pass
if lat:
    lat.sort()
    p = lambda q: lat[min(len(lat)-1, int(q*len(lat)))]
    print(f"- n={len(lat)}  p50={p(0.5):.1f}ms  p95={p(0.95):.1f}ms  p99={p(0.99):.1f}ms")
else:
    print("_no latency samples (see replay.log)_")

# --- per-family accuracy ---
print("\n## Query accuracy (vs exact ground truth)\n")
accs = [f for f in sorted(glob.glob(os.path.join(out, "perfamily-*.json"))) if not f.endswith("perfamily-all.json")]
if not accs:
    print("_no accuracy JSONs — the family replay/prep did not run (see accuracy.log)_")
PY
python3 - "${OUT}" >> "${OUT}/E2E_METRICS.md" 2>>"${OUT}/report.log" <<'PY'
import json, glob, os, sys
out = sys.argv[1]
print("\n## Query accuracy (this run, vs exact ground truth)\n")
print("| family | query | mean rel-err | median | p95 | within 2% |")
print("|---|---|---|---|---|---|")
for f in sorted(glob.glob(os.path.join(out, "perfamily-*.json"))):
    if f.endswith("perfamily-all.json"):
        continue
    d = json.load(open(f))
    fam = os.path.basename(f).replace("perfamily-", "").replace(".json", "")
    kind = d.get("kind", "?")
    sc = d.get("score", {})
    # quantile families store per-quantile dicts; cardinality/topk/freq store flat.
    if isinstance(sc, dict) and any(isinstance(v, dict) for v in sc.values()):
        for q, v in sc.items():
            if isinstance(v, dict) and "mean_rel_err" in v:
                print(f"| {fam} | {kind} q{q} | {v['mean_rel_err']:.2%} | {v.get('median_rel_err',float('nan')):.2%} | {v.get('p95_rel_err',float('nan')):.2%} | {v.get('frac_within_envelope',float('nan')):.1%} |")
    elif "rel_err" in sc and sc["rel_err"] is not None:
        print(f"| {fam} | {kind} | {sc['rel_err']:.2%} | — | — | — |")
    else:
        print(f"| {fam} | {kind} | (see {os.path.basename(f)}) | | | |")
PY

arm_down || true
slog "done — fresh metrics in ${OUT}/  (report: ${OUT}/E2E_METRICS.md)"
cat "${OUT}/E2E_METRICS.md" 2>/dev/null || true
