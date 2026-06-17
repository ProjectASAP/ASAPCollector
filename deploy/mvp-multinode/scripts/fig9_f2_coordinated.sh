#!/usr/bin/env bash
# Fig 9 (whole-sketch / F2 variant): coordinated sampling driven by each edge's
# WHOLE-SKETCH L2 mass F2_i = Σ_x f_i(x)², not a single point. This is the right
# model when the queried point is unknown a priori — keeping the whole sketch
# within ε makes ANY future point query accurate (ASAPQuery-backend#380).
#
# Workload: every edge has the SAME rate but a different distribution SKEW
# (concentration c). Per timestamp each edge emits E events over E/c distinct
# series, each c times ⇒ rate equal, F2/rate = c. So p_i ∝ √(F2_i/rate_i) = √c:
#   edge-skew (c=16) : edge-mid (c=4) : edge-flat (c=1)  →  p ≈ 4 : 2 : 1.
set -uo pipefail
SD="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MV="$(cd "${SD}/.." && pwd)"   # deploy/mvp-multinode
export TOPOLOGY_ENV="${TOPOLOGY_ENV:-${MV}/topology.8node.env}"
export RUN_DEMO_LIB=1; source "${SD}/run_demo.sh"
CFG="${MV}/configs"
AGG_PORT=4319
declare -A NODEMAP=( [edge-skew]=node3 [edge-mid]=node4 [edge-flat]=node5 )
declare -A CONC=(    [edge-skew]=16    [edge-mid]=4      [edge-flat]=1 )
EVENTS_PER_TS=16   # same for all edges ⇒ equal rate; skew is purely in concentration
slog(){ printf '[%s] [fig9-f2] %s\n' "$(date +%H:%M:%S)" "$*"; }

down(){ for e in "${!NODEMAP[@]}"; do ssh -o BatchMode=yes "${NODEMAP[$e]}" "docker rm -f asap-$e >/dev/null 2>&1"; done
        stop_node "${COLD_HOST}" >/dev/null 2>&1; stop_node "${WARM_HOST}" >/dev/null 2>&1; backend_down >/dev/null 2>&1 || true; }
# NOTE: no EXIT trap — leave the stack up after the run so grants can be inspected.
down; sleep 3

# 1. swap the F2 monitored workload onto WARM, backend up WITH coordinator
sync_all_nodes >/dev/null 2>&1
rsync -a "${CFG}/asap/mvp-workload-fig9-f2.yaml" "${WARM_HOST}:/mydata/mvp-multinode/configs/asap/mvp-workload.yaml"
DP_MONITOR_FLAGS="--enable-monitor-coordinator --monitor-grpc-port ${AGG_PORT}" backend_up asap >/dev/null 2>&1
slog "backend up (coordinator :${AGG_PORT}, functional=f2 whole-sketch)"; sleep 10

# 2. read the emitted monitor agg_id
AGG=""
for i in $(seq 1 30); do
  AGG=$(curl -s "http://${WARM_IP}:9091/api/v1/streaming-config" 2>/dev/null | python3 -c "import json,sys
try: m=json.load(sys.stdin).get('streaming_config',{}).get('monitors',[]); print(m[0]['agg_id'] if m else '')
except: print('')" 2>/dev/null)
  [ -n "${AGG}" ] && break; sleep 4
done
[ -z "${AGG}" ] && { slog "FATAL: no monitor agg_id"; exit 1; }
slog "monitor agg_id=${AGG}"

# 2b. wait for the coordinator to HOT-RELOAD the controller-pushed f2 monitor
slog "waiting for coordinator to hot-reload the monitor (no seed, no restart)..."
for i in $(seq 1 30); do
  ssh -o BatchMode=yes "${WARM_HOST}" "docker logs asap-data-plane 2>&1 | grep -q 'hot-reloaded monitors'" && break
  sleep 3
done
ssh -o BatchMode=yes "${WARM_HOST}" "docker logs asap-data-plane 2>&1 | grep -iE 'hot-reloaded monitors|no .monitors. yet' | tail -3"
fn=$(curl -s "http://${WARM_IP}:9091/api/v1/streaming-config" 2>/dev/null | python3 -c "import json,sys
try: m=json.load(sys.stdin).get('streaming_config',{}).get('monitors',[]); print(m[0].get('functional','') if m else '')
except: print('')" 2>/dev/null)
slog "coordinator hot-reloaded; served monitor functional=${fn:-?}"

# 3. generate equal-rate, concentration-skewed CSVs + ship.
#    Per timestamp: E events over E/c distinct series, each c times ⇒ rate equal,
#    per-series count ∝ c, so F2/rate = c.
gen(){ python3 -c "
import sys; c=int(sys.argv[1]); E=int(sys.argv[2]); n=max(1,E//c)
print('timestamp_ms,series_id,value')
for t in range(0,2000,100):
    for j in range(n):
        for _ in range(c): print(f'{t},s{j},1.0')
" "$1" "$2"; }
for e in "${!NODEMAP[@]}"; do gen "${CONC[$e]}" "${EVENTS_PER_TS}" > /tmp/$e.csv; scp -q -o BatchMode=yes /tmp/$e.csv "${NODEMAP[$e]}:/tmp/edge.csv"; done

# 4. launch coordinated trace-replay edges in F2 mode (no key; functional=f2)
for e in edge-skew edge-mid edge-flat; do
  slog "${e} on ${NODEMAP[$e]} (conc=${CONC[$e]}, E=${EVENTS_PER_TS}/ts, equal rate)"
  ssh -o BatchMode=yes "${NODEMAP[$e]}" "docker rm -f asap-$e >/dev/null 2>&1; docker run -d --network host --name asap-$e \
    -v /tmp/edge.csv:/tmp/edge.csv:ro asap/otel-app:dev \
    -producer-id=$e -target=${WARM_IP}:4317 -trace-file=/tmp/edge.csv -trace-loop \
    -coordinator-url=${WARM_IP}:${AGG_PORT} -monitor-agg-id=${AGG} -monitor-functional=f2 \
    -monitor-config-url=http://${WARM_IP}:9091/api/v1/streaming-config \
    -edge-id=$e -warm-sample-p=1.0" >/dev/null
done

# 5. converge, then read granted p
slog "converging 150s..."; sleep 150
echo "edge,node,concentration,learned,granted_p"
OUT="${FIG9_OUT:-${MV}/eval-8node/fig9_f2.csv}"; echo "edge,node,concentration,learned_monitors,granted_p" > "$OUT"
for e in edge-skew edge-mid edge-flat; do
  log=$(ssh -o BatchMode=yes "${NODEMAP[$e]}" "docker logs asap-$e 2>&1")
  learned=$(echo "$log" | grep -oE 'learned [0-9]+ monitor' | tail -1 | grep -oE '[0-9]+' | head -1)
  p=$(echo "$log" | grep -oE 'applied warm-sample-p [0-9.]+' | tail -1 | grep -oE '[0-9.]+$')
  echo "$e ${NODEMAP[$e]} conc=${CONC[$e]} learned=${learned:-0} p=${p:-1.0}"
  echo "$e,${NODEMAP[$e]},${CONC[$e]},${learned:-0},${p:-1.0}" >> "$OUT"
done
slog "coordinator unconfigured count:"
ssh -o BatchMode=yes "${WARM_HOST}" "docker logs asap-data-plane 2>&1 | grep -c unconfigured" 2>/dev/null || true
