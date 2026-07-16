#!/usr/bin/env bash
# epsilon_accuracy_sweep.sh — Phase-2 cluster ACCURACY column, google_cluster trace.
#
# Per coordinated-sampling admission p, bring up the asap stack with otel-app
# producers in TRACE-REPLAY mode (the real google-cluster-2019 cpu_rate trace,
# same dataset as Phase-1), landing the trace under the warm DDSketch metric, and
# query the warm sketch quantiles. accuracy = 1 - |sketch - GT|/GT against the
# exact offline GT over the full trace. -warm-sample-p admission sampling applies
# to the replayed gauge, so this is accuracy-under-coordinated-sampling.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PKG_DIR="$(dirname "${SCRIPT_DIR}")"
export TOPOLOGY_ENV="${TOPOLOGY_ENV:-${PKG_DIR}/topology.8node.env}"
source "${TOPOLOGY_ENV}"

OUT="${OUT:-/mydata/eval/phase2/accuracy}"
P_GRID="${P_GRID:-1.0 0.5 0.25 0.1 0.05}"
TRACE_CSV="${TRACE_CSV:-/tmp/gct-cpu-pooled.csv}"
METRIC="${METRIC:-http_requests_total_latency_ms}"
SEAL_S="${SEAL_S:-55}"
export PER_AGENT_CARDINALITY=300 OTELAPP_FREQ_HZ=20 N_PRODUCERS_PER_NODE=1
export SKIP_BUILD=1 SKIP_LOAD=1
export OTELAPP_TRACE_MOUNT="-v ${TRACE_CSV}:/trace.csv:ro"
export OTELAPP_TRACE_ARGS="-trace-file=/trace.csv -trace-loop -trace-scale=30000 -trace-metric-name=${METRIC}"
mkdir -p "${OUT}"
: > "${OUT}/accuracy-raw.csv"
echo "p,quantile,sketch_mean,n_series" >> "${OUT}/accuracy-raw.csv"

query_q() {  # $1=quantile ; echoes "mean nseries"
    local qp=$1
    ssh -o BatchMode=yes "${WARM_HOST}" \
        "curl -s 'http://localhost:9091/api/v1/query' --data-urlencode 'query=quantile_over_time(${qp}, ${METRIC}[5m])'" 2>/dev/null \
    | python3 -c 'import sys,json
try:
    d=json.load(sys.stdin); r=d["data"]["result"]
    v=[float(s["value"][1]) for s in r]
    print(f"{sum(v)/len(v):.8f} {len(v)}" if v else "nan 0")
except Exception: print("nan 0")'
}

for p in ${P_GRID}; do
    echo "[acc-sweep] === p=${p} ==="
    SKIP_BUILD=1 SKIP_LOAD=1 bash "${SCRIPT_DIR}/run_demo.sh" down >/dev/null 2>&1 || true
    sleep 3
    OTELAPP_WARM_SAMPLE_P="${p}" bash "${SCRIPT_DIR}/run_demo.sh" up asap \
        >"${OUT}/up-p${p}.log" 2>&1
    echo "[acc-sweep] p=${p} up; trace replay + seal ${SEAL_S}s"
    sleep "${SEAL_S}"
    for qp in 0.99 0.90 0.50; do
        read -r mean nser <<< "$(query_q ${qp})"
        echo "${p},${qp},${mean},${nser}" >> "${OUT}/accuracy-raw.csv"
        echo "[acc-sweep]   p=${p} q=${qp} sketch=${mean} (n=${nser})"
    done
done
SKIP_BUILD=1 SKIP_LOAD=1 bash "${SCRIPT_DIR}/run_demo.sh" down >/dev/null 2>&1 || true
echo "[acc-sweep] complete → ${OUT}/accuracy-raw.csv"
