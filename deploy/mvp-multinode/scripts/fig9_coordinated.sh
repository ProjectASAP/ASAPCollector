#!/usr/bin/env bash
# Fig 9 rerun: cms_point coordinated sampling on a skewed fleet.
# Monitored key s0 has CONSTANT frequency across edges; total rate is skewed.
# Expect p_hot < p_med < p_quiet (p_i ∝ √(f_i/rate_i), f_i const ⇒ p ∝ 1/√rate).
set -uo pipefail
SD="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MV="$(cd "${SD}/.." && pwd)"   # deploy/mvp-multinode
export TOPOLOGY_ENV="${TOPOLOGY_ENV:-${MV}/harness/topology/8node.env}"
export RUN_DEMO_LIB=1; source "${SD}/run_demo.sh"
CFG="${MV}/configs"
AGG_PORT=4319
declare -A NODEMAP=( [edge-hot]=node3 [edge-med]=node4 [edge-quiet]=node5 )
declare -A SER=(    [edge-hot]=400    [edge-med]=80     [edge-quiet]=16 )
slog(){ printf '[%s] [fig9] %s\n' "$(date +%H:%M:%S)" "$*"; }

down(){ for e in "${!NODEMAP[@]}"; do ssh -o BatchMode=yes "${NODEMAP[$e]}" "docker rm -f asap-$e >/dev/null 2>&1"; done
        stop_node "${COLD_HOST}" >/dev/null 2>&1; stop_node "${WARM_HOST}" >/dev/null 2>&1; backend_down >/dev/null 2>&1 || true; }
# NOTE: no EXIT trap — leave the stack up after the run so grants can be inspected.
down; sleep 3

# 1. swap monitored workload onto node2, backend up WITH coordinator
sync_all_nodes >/dev/null 2>&1
rsync -a "${CFG}/asap/mvp-workload-fig9.yaml" "${WARM_HOST}:/mydata/mvp-multinode/configs/asap/mvp-workload.yaml"
DP_MONITOR_FLAGS="--enable-monitor-coordinator --monitor-grpc-port ${AGG_PORT}" backend_up asap >/dev/null 2>&1
slog "backend up (coordinator :${AGG_PORT}, cms_point key=s0)"; sleep 10

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

# 2b. Wait for the coordinator to HOT-RELOAD the controller-pushed monitor.
#     The data-plane coordinator picks up `monitors:` live from the control plane's
#     streaming-config push (ASAPQuery-backend#379, MonitorCoordinator::reconfigure)
#     — no boot-config seed, no restart. The workload yaml carries the real
#     coordination params (tau 7000 / window_secs 15) so the published monitor is
#     directly usable. Earlier this step seeded the boot config + restarted the
#     data-plane to work around the coordinator reading monitors() only at boot;
#     #379 makes that unnecessary.
slog "waiting for coordinator to hot-reload the monitor (no seed, no restart)..."
for i in $(seq 1 30); do
  ssh -o BatchMode=yes "${WARM_HOST}" "docker logs asap-data-plane 2>&1 | grep -q 'hot-reloaded monitors'" && break
  sleep 3
done
ssh -o BatchMode=yes "${WARM_HOST}" "docker logs asap-data-plane 2>&1 | grep -iE 'hot-reloaded monitors|no .monitors. yet' | tail -3"
k=$(curl -s "http://${WARM_IP}:9091/api/v1/streaming-config" 2>/dev/null | python3 -c "import json,sys
try: m=json.load(sys.stdin).get('streaming_config',{}).get('monitors',[]); print(m[0]['key'] if m else '')
except: print('')" 2>/dev/null)
slog "coordinator hot-reloaded; served monitor key=${k:-?}"

# 3. generate constant-key CSVs (s0 once/timestamp = const freq; N-1 others) + ship
gen(){ python3 -c "
import sys; N=int(sys.argv[1])
print('timestamp_ms,series_id,value')
for t in range(0,2000,10):
    print(f'{t},s0,1.0')
    for s in range(1,N): print(f'{t},x{s},1.0')
" "$1"; }
for e in "${!SER[@]}"; do gen "${SER[$e]}" > /tmp/$e.csv; scp -q -o BatchMode=yes /tmp/$e.csv "${NODEMAP[$e]}:/tmp/edge.csv"; done

# 4. launch coordinated trace-replay edges with -monitor-key=s0
for e in edge-hot edge-med edge-quiet; do
  slog "${e} on ${NODEMAP[$e]} (N=${SER[$e]} series, s0 const-freq)"
  ssh -o BatchMode=yes "${NODEMAP[$e]}" "docker rm -f asap-$e >/dev/null 2>&1; docker run -d --network host --name asap-$e \
    -v /tmp/edge.csv:/tmp/edge.csv:ro asap/otel-app:dev \
    -producer-id=$e -target=${WARM_IP}:4317 -trace-file=/tmp/edge.csv -trace-loop \
    -coordinator-url=${WARM_IP}:${AGG_PORT} -monitor-agg-id=${AGG} -monitor-key=s0 \
    -monitor-config-url=http://${WARM_IP}:9091/api/v1/streaming-config \
    -edge-id=$e -warm-sample-p=1.0" >/dev/null
done

# 5. converge, then read granted p
slog "converging 150s..."; sleep 150
echo "edge,node,total_series,learned,granted_p"
OUT="${FIG9_OUT:-${MV}/eval-8node/fig9_cmspoint.csv}"; echo "edge,node,total_series,learned_monitors,granted_p" > "$OUT"
for e in edge-hot edge-med edge-quiet; do
  log=$(ssh -o BatchMode=yes "${NODEMAP[$e]}" "docker logs asap-$e 2>&1")
  learned=$(echo "$log" | grep -oE 'learned [0-9]+ monitor' | tail -1 | grep -oE '[0-9]+' | head -1)
  # new sample_controller log: "applied warm-sample-p 0.1234 (was ...) = max over N monitors ..."
  p=$(echo "$log" | grep -oE 'applied warm-sample-p [0-9.]+' | tail -1 | grep -oE '[0-9.]+$')
  echo "$e ${NODEMAP[$e]} ${SER[$e]} learned=${learned:-0} p=${p:-1.0}"
  echo "$e,${NODEMAP[$e]},${SER[$e]},${learned:-0},${p:-1.0}" >> "$OUT"
done
slog "coordinator-side p (data-plane log):"
ssh -o BatchMode=yes "${WARM_HOST}" "docker logs asap-data-plane 2>&1 | grep -iE 'sample_p|grant|alloc' | tail -6" 2>/dev/null || true
