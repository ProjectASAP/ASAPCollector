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

# 2. Per-component resources + bandwidth + query latency (existing multinode measure).
slog "arm_measure: per-component CPU/mem + per-node bandwidth + query latency (${SOAK_S}s soak)"
SOAK_S="${SOAK_S}" arm_measure asap

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

# 4. Consolidated fresh report: resources/bandwidth/latency (aggregate_report.py)
#    + a fresh per-family accuracy table appended from this run's JSONs.
slog "aggregating fresh report → ${OUT}/E2E_METRICS.md"
python3 "${SCRIPT_DIR}/aggregate_report.py" --run-dir "${OUT}" --out "${OUT}/E2E_METRICS.md" \
  2>"${OUT}/report.log" || slog "resource report failed (see report.log)"
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
