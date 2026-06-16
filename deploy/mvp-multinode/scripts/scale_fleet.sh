#!/usr/bin/env bash
# scale_fleet.sh — Fig 10 driver: sweep fleet size N (agents) and measure that
# per-agent wire bandwidth and CPU stay flat as the fleet grows.
#
# One supervised asap agent per source host (host-network :4317), N producers
# each, all feeding the single warm backend (node2). For each N we soak, read
# the backend sink NIC RX and the mean per-agent CPU, and write a row to
# scale.csv. Physical N is capped at the number of SRC_NODES (5 here: node3-7);
# extend to N=100 with cost_model/simulator.py, anchored on these points.
#
# Usage:
#   TOPOLOGY_ENV=.../topology.8node.env SKIP_BUILD=1 SKIP_LOAD=1 \
#     scale_fleet.sh [N_LIST] [SOAK_S]
# Reuses run_demo.sh as a library (RUN_DEMO_LIB=1).
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
N_LIST="${1:-1 2 3 4 5}"
SOAK="${2:-${SOAK_S:-45}}"
WARMUP="${WARMUP_S:-60}"

# pull in topology + bring-up primitives (backend_up, docker_run_on, ADD_HOSTS, …)
export RUN_DEMO_LIB=1
source "${SCRIPT_DIR}/run_demo.sh"

read -ra SRC <<< "${SRC_HOSTS:-${SRC_NODES:-node3 node4 node5 node6 node7}}"
RUN_ID="${RUN_ID:-scale-$(date +%Y%m%d-%H%M%S)}"
OUT="${RUN_BASE}/${RUN_ID}"; mkdir -p "${OUT}"
CSV="${OUT}/scale.csv"
echo "n_agents,arm,sink_rx_MB_s,per_agent_rx_MB_s,agent_cpu_mean_perc,agent_mem_mean_mib" > "${CSV}"
slog() { printf '[%s] [scale] %s\n' "$(date +%H:%M:%S)" "$*" | tee -a "${OUT}/scale.log"; }

IFACE_PROBE='iface=$(ip -4 -o addr show | awk '"'"'$4 ~ /^10\.10\.1\./ {print $2; exit}'"'"'); iface=${iface:-eth0}'
nic_rx() { ssh -o BatchMode=yes "$1" "${IFACE_PROBE}"'; cat /sys/class/net/$iface/statistics/rx_bytes'; }

# launch one supervised asap agent + its producers on a source host
launch_agent() {
    local node=$1 idx=$2
    slog "agent-${idx} on ${node} (supervised) + ${N_PRODUCERS_PER_NODE} producers"
    docker_run_on "${node}" --cpus=4 --memory=12g --memory-swap=12g \
        --name "asap-agent-${idx}" --hostname "agent-${idx}" \
        -e "X_AGENT_ID=agent-${idx}" -e "AGENT_ID=agent-${idx}" \
        -v /mydata/mvp-multinode/configs/asap/supervisor.yaml:/etc/otel/supervisor.yaml:ro \
        asap/asap-otel-supervised:dev --config /etc/otel/supervisor.yaml
    sleep 5
    local i
    for i in $(seq 1 "${N_PRODUCERS_PER_NODE}"); do
        docker_run_on "${node}" --cpus=1 --memory=4g --memory-swap=4g \
            --name "asap-producer-${idx}-${i}" \
            asap/otel-app:dev \
            -target=127.0.0.1:4317 -producer-id="p-${idx}-${i}" \
            -cardinality="${PER_AGENT_CARDINALITY}" -freq-hz="${OTELAPP_FREQ_HZ}" \
            -sdk-window="${OTELAPP_SDK_WINDOW}" -agg="${OTELAPP_SDK_AGG}" \
            -max-buffer-per-series="${OTELAPP_MAX_BUFFER_PER_SERIES}" \
            -freshness-probes=false -seed="$((42 + idx))"
    done
}

# mean CPU% / mem(MiB) of asap-agent-* across the active source hosts.
# Averages SAMPLES snapshots (a single docker-stats snapshot is far too noisy —
# it caught 174% then 2.8% on adjacent N in v1), spaced over the soak.
agent_stats() {
    local nodes=("$@") n s
    local SAMPLES="${STAT_SAMPLES:-6}"
    : > "${OUT}/.stats_tmp"
    for s in $(seq 1 "${SAMPLES}"); do
        for n in "${nodes[@]}"; do
            ssh -o BatchMode=yes "$n" \
              "docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}' 2>/dev/null | grep '^asap-agent-'" \
              >> "${OUT}/.stats_tmp" 2>/dev/null || true
        done
        [ "$s" -lt "${SAMPLES}" ] && sleep 4
    done
    python3 - "${OUT}/.stats_tmp" <<'PY'
import sys, re
cpu=[]; mem=[]
for ln in open(sys.argv[1]):
    p=ln.split()
    if len(p)<2: continue
    try: cpu.append(float(p[1].rstrip('%')))
    except: pass
    m=re.search(r'([\d.]+)([KMG]i?B)', ln)
    if m:
        v=float(m.group(1)); u=m.group(2)
        f={'KiB':1/1024,'MiB':1,'GiB':1024,'KB':1/1024,'MB':1,'GB':1024}.get(u,1)
        mem.append(v*f)
print(f"{(sum(cpu)/len(cpu) if cpu else 0):.1f},{(sum(mem)/len(mem) if mem else 0):.1f}")
PY
}

# Full teardown across ALL source nodes (node3-7) + both backends. run_demo.sh's
# arm_down only reaps NODE0/NODE3 (the 4-node harness's two source slots), so on
# the 8-node fleet agents on node5-7 would survive into the next N and collide.
scale_down() {
    local n
    for n in "${SRC[@]}"; do stop_node "$n" >/dev/null 2>&1 || true; done
    stop_node "${COLD_HOST}" >/dev/null 2>&1 || true
    stop_node "${WARM_HOST}" >/dev/null 2>&1 || true
    backend_down >/dev/null 2>&1 || true
}

ARM=asap
slog "RUN_ID=${RUN_ID} N_LIST='${N_LIST}' SOAK=${SOAK}s SRC='${SRC[*]}' card=${PER_AGENT_CARDINALITY} freq=${OTELAPP_FREQ_HZ} prod/agent=${N_PRODUCERS_PER_NODE}"

for N in ${N_LIST}; do
    [ "${N}" -gt "${#SRC[@]}" ] && { slog "N=${N} exceeds ${#SRC[@]} source nodes — skipping (use simulator)"; continue; }
    slog "=== N=${N} ==="
    scale_down; sleep 3
    backend_up "${ARM}" >>"${OUT}/scale.log" 2>&1; sleep 6
    active=(); for k in $(seq 0 $((N-1))); do launch_agent "${SRC[$k]}" "$((k+1))"; active+=("${SRC[$k]}"); done
    slog "warmup ${WARMUP}s"; sleep "${WARMUP}"

    rx0=$(nic_rx "${WARM_HOST}"); t0=$(date +%s.%N)
    sleep "${SOAK}"
    rx1=$(nic_rx "${WARM_HOST}"); t1=$(date +%s.%N)
    stats=$(agent_stats "${active[@]}")
    rxps=$(awk -v a="$rx0" -v b="$rx1" -v t0="$t0" -v t1="$t1" 'BEGIN{printf "%.3f",(b-a)/((t1-t0)*1e6)}')
    peragent=$(awk -v r="$rxps" -v n="$N" 'BEGIN{printf "%.3f", r/n}')
    echo "${N},${ARM},${rxps},${peragent},${stats}" >> "${CSV}"
    slog "N=${N}: sink_rx=${rxps} MB/s  per_agent=${peragent} MB/s  agent(cpu%,mem MiB)=${stats}"
    scale_down
done

slog "done -> ${CSV}"; cat "${CSV}"
