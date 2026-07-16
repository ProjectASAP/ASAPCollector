#!/usr/bin/env bash
# epsilon_cluster_sweep.sh — Phase-2 integrated cluster ε-sweep.
#
# For each coordinated-sampling admission p (= the ε-floor p=1/(1+ε²·rate) the
# autonomous coordinator sets for a target ε), bring up the full warm+cold ASAP
# stack and capture the integrated metric set the single-node run could NOT:
#   · real data FRESHNESS  (gen→queryable Δ per tier: warm sketch, archive gorilla)
#   · per-component RESOURCES (edge / data-plane / control-plane / thanos / minio)
#   · query LATENCY (PromQL replay p50/p99 on warm :9091)
# accuracy (warm + cold-tier fallthrough) is layered on via run_e2e separately.
#
# Wall-clock paced (trace-scale 1.0) so freshness is a true wall-clock latency.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PKG_DIR="$(dirname "${SCRIPT_DIR}")"
export TOPOLOGY_ENV="${TOPOLOGY_ENV:-${PKG_DIR}/topology.8node.env}"
source "${TOPOLOGY_ENV}"

OUT_BASE="${OUT_BASE:-/mydata/eval/phase2/sweep}"
SOAK="${SOAK_S:-90}"
N_FRESH="${N_FRESH:-60}"
# ε-floor admission grid (p); implied ε computed post-hoc from measured rate.
P_GRID="${P_GRID:-1.0 0.5 0.25 0.1 0.05}"
# Light, stable workload so producers don't OOM (the default ~250MB/s starves
# the agent → producer backpressure → OOM-137). A modest, steady rate is what
# we want for clean gen->queryable / resource / accuracy measurement anyway.
export PER_AGENT_CARDINALITY="${PER_AGENT_CARDINALITY:-300}"
export OTELAPP_FREQ_HZ="${OTELAPP_FREQ_HZ:-20}"
export N_PRODUCERS_PER_NODE="${N_PRODUCERS_PER_NODE:-2}"
export SKIP_BUILD=1 SKIP_LOAD=1
mkdir -p "${OUT_BASE}"

echo "[eps-sweep] OUT=${OUT_BASE} SOAK=${SOAK}s P_GRID=${P_GRID}"

for p in ${P_GRID}; do
    arm="p${p}"
    out="${OUT_BASE}/${arm}"
    mkdir -p "${out}"
    echo "[eps-sweep] === p=${p} ==="

    SKIP_BUILD=1 SKIP_LOAD=1 bash "${SCRIPT_DIR}/run_demo.sh" down >"${out}/down.log" 2>&1 || true
    sleep 3
    SKIP_BUILD=1 SKIP_LOAD=1 OTELAPP_WARM_SAMPLE_P="${p}" \
        bash "${SCRIPT_DIR}/run_demo.sh" up asap >"${out}/up.log" 2>&1
    echo "[eps-sweep] p=${p} up; soak ${SOAK}s"
    sleep 5

    # resources (backgrounded over the soak window) + query-latency replay
    SNAP_NODES="${COLD_HOST} ${WARM_HOST} ${SRC_HOSTS}" \
        bash "${SCRIPT_DIR}/snapshot_resources.sh" "${arm}" "${SOAK}" "${out}/resources" \
        >"${out}/snap.log" 2>&1 &
    snap_pid=$!

    # freshness (real gen→queryable per tier) during the same soak
    ARM=asap OUT="${out}/freshness" NODE2_IP="${WARM_IP}" \
        N_SAMPLES="${N_FRESH}" POLL_MS=100 \
        bash "${SCRIPT_DIR}/measure_freshness.sh" >"${out}/freshness.log" 2>&1 || true

    # query latency: time N warm-tier queries (probe = always-served, name-routed)
    python3 - "${WARM_IP}" "${out}/latency.csv" <<'PY' || true
import sys,time,urllib.parse,urllib.request
ip,out=sys.argv[1],sys.argv[2]
q="last_over_time(http_freshness_probe_warm[10s])"
url=f"http://{ip}:9091/api/v1/query?"+urllib.parse.urlencode({"query":q})
lat=[]
for _ in range(40):
    t=time.perf_counter()
    try: urllib.request.urlopen(url,timeout=5).read()
    except Exception: continue
    lat.append((time.perf_counter()-t)*1000.0)
lat.sort()
if lat:
    p50=lat[len(lat)//2]; p99=lat[min(len(lat)-1,int(.99*len(lat)))]
    open(out,"w").write(f"p50_ms,p99_ms,n\n{p50:.2f},{p99:.2f},{len(lat)}\n")
    print(f"[latency] p50={p50:.2f}ms p99={p99:.2f}ms n={len(lat)}")
PY

    wait "${snap_pid}" 2>/dev/null || true
    echo "[eps-sweep] p=${p} done → ${out}"
done

SKIP_BUILD=1 SKIP_LOAD=1 bash "${SCRIPT_DIR}/run_demo.sh" down >/dev/null 2>&1 || true
echo "[eps-sweep] complete. results under ${OUT_BASE}"
