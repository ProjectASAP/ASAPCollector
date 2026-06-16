#!/usr/bin/env bash
# fig9_coordinated.sh — Fig 9: coordinated sampling on a skewed fleet.
#
# Brings up the backend with the CDM monitor coordinator enabled and a
# monitored http_requests_total (mvp-workload-fig9.yaml), then launches one
# coordinated producer per source node at DIFFERENT rates (skewed fleet). Each
# producer reports its per-window rate to the coordinator (data-plane :4319) and
# applies the granted warm-sample-p. We read each edge's granted p and show the
# differentiation: hot edge -> low p, quiet edge -> high p (p_i ∝ √(f_i/rate_i)).
#
# Usage: TOPOLOGY_ENV=.../topology.8node.env SKIP_BUILD=1 SKIP_LOAD=1 fig9_coordinated.sh
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export RUN_DEMO_LIB=1
source "${SCRIPT_DIR}/run_demo.sh"
CFG_SRC="$(cd "${SCRIPT_DIR}/../configs" && pwd)"

# skewed fleet: edge -> (node, Hz). 100/20/5 Hz ~ 20x rate skew.
EDGES=( "edge-hot:node3:100" "edge-med:node4:20" "edge-quiet:node5:5" )
RUN_ID="${RUN_ID:-fig9-$(date +%H%M%S)}"
OUT="${RUN_BASE}/${RUN_ID}"; mkdir -p "${OUT}"
CSV="${OUT}/fig9.csv"; echo "edge,node,freq_hz,granted_p" > "${CSV}"
slog(){ printf '[%s] [fig9] %s\n' "$(date +%H:%M:%S)" "$*" | tee -a "${OUT}/fig9.log"; }
COORD="${NODE2_IP}:4319"

# teardown across all involved nodes
f9_down(){ for e in "${EDGES[@]}"; do stop_node "${e#*:}" >/dev/null 2>&1 || true; done
           stop_node "${NODE1_HOST}" >/dev/null 2>&1; stop_node "${NODE2_HOST}" >/dev/null 2>&1; backend_down >/dev/null 2>&1 || true; }

slog "RUN_ID=${RUN_ID} coordinator=${COORD} edges='${EDGES[*]}'"
f9_down; sleep 3

# 1. sync configs, then swap the MONITORED workload onto node2's mount path
sync_all_nodes >/dev/null 2>&1
rsync -a "${CFG_SRC}/asap/mvp-workload-fig9.yaml" \
      "${NODE2_HOST}:/mydata/mvp-multinode/configs/asap/mvp-workload.yaml"
slog "swapped monitored workload onto ${NODE2_HOST}"

# 2. backend up WITH the coordinator enabled
DP_MONITOR_FLAGS="--enable-monitor-coordinator --monitor-grpc-port 4319" \
  backend_up asap >>"${OUT}/fig9.log" 2>&1
slog "backend up (coordinator on :4319)"; sleep 8

# 3. wait for the controller to emit the monitor; read its agg_id
AGG=""
for i in $(seq 1 30); do
  AGG=$(curl -s "http://${NODE2_IP}:9091/api/v1/streaming-config" 2>/dev/null \
        | python3 -c "import json,sys
try:
  m=json.load(sys.stdin).get('streaming_config',{}).get('monitors',[])
  print(m[0].get('agg_id','') if m else '')
except: print('')" 2>/dev/null)
  [ -n "${AGG}" ] && break; sleep 4
done
[ -z "${AGG}" ] && { slog "FATAL: no monitor agg_id emitted (monitors[] stayed empty)"; f9_down; exit 1; }
slog "monitor agg_id=${AGG}"

# 4. launch one coordinated producer per edge at its skewed rate
for spec in "${EDGES[@]}"; do
  IFS=: read -r eid node hz <<< "${spec}"
  slog "${eid} on ${node} @ ${hz}Hz (coordinated)"
  docker_run_on "${node}" --cpus=2 --memory=6g --memory-swap=6g \
    --name "asap-prod-${eid}" \
    asap/otel-app:dev \
    -target="${NODE2_IP}:4317" -producer-id="${eid}" \
    -cardinality=200 -freq-hz="${hz}" -sdk-window=1s -agg=raw-buffer \
    -max-buffer-per-series=512 -freshness-probes=false \
    -warm-sample-p=1.0 -coordinator-url="${COORD}" \
    -monitor-agg-id="${AGG}" -edge-id="${eid}"
done

# 5. let several 30s monitor epochs pass so grants converge
slog "waiting 150s for coordinated grants to converge..."
sleep 150

# 6. read each edge's latest granted p
for spec in "${EDGES[@]}"; do
  IFS=: read -r eid node hz <<< "${spec}"
  p=$(ssh -o BatchMode=yes "${node}" "docker logs asap-prod-${eid} 2>&1 | grep -oE 'applied granted warm-sample-p [0-9.]+' | tail -1 | grep -oE '[0-9.]+$'")
  [ -z "${p}" ] && p="(none — bootstrap 1.0)"
  echo "${eid},${node},${hz},${p}" >> "${CSV}"
  slog "${eid} @ ${hz}Hz -> granted_p=${p}"
done

slog "done -> ${CSV}"; cat "${CSV}"
f9_down
