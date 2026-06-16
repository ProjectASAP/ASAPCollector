#!/usr/bin/env bash
# Fig 9 rerun: cms_point coordinated sampling on a skewed fleet.
# Monitored key s0 has CONSTANT frequency across edges; total rate is skewed.
# Expect p_hot < p_med < p_quiet (p_i ∝ √(f_i/rate_i), f_i const ⇒ p ∝ 1/√rate).
set -uo pipefail
SD="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MV="$(cd "${SD}/.." && pwd)"   # deploy/mvp-multinode
export TOPOLOGY_ENV="${TOPOLOGY_ENV:-${MV}/topology.8node.env}"
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

# 2b. Seed the monitor into the data-plane's BOOT streaming-config and restart it.
#     The coordinator reads streaming_config.monitors() ONCE at boot
#     (main.rs MonitorCoordinator::new) and is NOT re-seeded by the controller's
#     post-boot hot-reload POST — so a monitor that only arrives via hot-reload
#     leaves the coordinator empty and it rejects every edge report
#     ("register for unconfigured monitor — ignored"). sync_all_nodes already
#     overwrote this file with the clean repo copy, so a single append is idempotent
#     across reruns. agg_id is taken live (content-addressed, stable run-to-run).
BOOT_CFG=/mydata/mvp-multinode/configs/asap/backend-streaming.yaml
ssh -o BatchMode=yes "${WARM_HOST}" "cat >> ${BOOT_CFG}" <<EOF

monitors:
  - agg_id: ${AGG}
    key: s0
    tau: 7000.0
    epsilon: 0.2
    window_ms: 15000
EOF
ssh -o BatchMode=yes "${WARM_HOST}" "docker restart asap-data-plane >/dev/null 2>&1"
slog "seeded monitor (agg_id=${AGG}, key=s0) into boot config + restarted data-plane"
# wait until the coordinator boots WITH the monitor (served at :9091 from boot)
for i in $(seq 1 30); do
  k=$(curl -s "http://${WARM_IP}:9091/api/v1/streaming-config" 2>/dev/null | python3 -c "import json,sys
try: m=json.load(sys.stdin).get('streaming_config',{}).get('monitors',[]); print(m[0]['key'] if m else '')
except: print('')" 2>/dev/null)
  [ "${k}" = "s0" ] && break; sleep 3
done
ssh -o BatchMode=yes "${WARM_HOST}" "docker logs asap-data-plane 2>&1 | grep -iE 'monitor coordinator|no .monitors' | tail -2"
slog "coordinator ready with monitor key=${k:-?}"

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
