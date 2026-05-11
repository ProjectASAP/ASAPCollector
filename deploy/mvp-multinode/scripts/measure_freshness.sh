#!/usr/bin/env bash
# measure_freshness.sh — sample-generation-to-backend-write delay.
#
# Mechanism: fake-exporter emits `http_freshness_probe_{raw,warm,archive}`
# counters whose cumulative value is the Unix-epoch-ms at emission. We poll
# the relevant backend for `<probe>` (instant query — gives the latest sample's
# value as a number == its emission ts_ms). Δ = poll_response_ts_ms − value.
#
# Per #46 runbook §"Freshness probe protocol":
#   - For B0/B1 the only landing is Prometheus (no warm/archive tiers in use).
#     We use http_freshness_probe_raw (lands in Prometheus via PRW).
#   - For ASAP, both warm (asap-query-backend) and archive (Thanos→MinIO via
#     gorillas3processor) probes exist; we measure each independently against
#     the right query endpoint. raw still lands in Prometheus.
#
# Inputs (env):
#   ARM      b0 | b1 | asap
#   OUT      output dir (writes freshness-${ARM}.csv + summary lines)
#   N_SAMPLES  default 60 polls
#   POLL_MS    default 100ms between polls
set -euo pipefail
ARM=${ARM:?arm}
OUT=${OUT:?out}
N_SAMPLES=${N_SAMPLES:-60}
POLL_MS=${POLL_MS:-100}
NODE2_IP=${NODE2_IP:-10.10.1.3}

mkdir -p "${OUT}"
CSV="${OUT}/freshness-${ARM}.csv"
echo "arm,probe,tier,poll_idx,poll_ts_ms,observed_value_ms,delta_ms" > "${CSV}"

# Per-tier endpoint + probe metric mapping
case "${ARM}" in
    b0|b1)
        PROBES=("raw|http_freshness_probe_raw_milliseconds_total|http://${NODE2_IP}:9090")
        ;;
    asap)
        # warm tier → asap-query-backend; archive tier → also goes through
        # asap-query-backend's EngineRouter (it dispatches to thanos_archive).
        # raw probe (if path enabled) → Prometheus.
        PROBES=(
            "raw|http_freshness_probe_raw_milliseconds_total|http://${NODE2_IP}:9090"
            "warm|http_freshness_probe_warm_milliseconds_total|http://${NODE2_IP}:9091"
            "archive|http_freshness_probe_archive_milliseconds_total|http://${NODE2_IP}:9091"
        )
        ;;
    *) echo "unknown arm ${ARM}" >&2; exit 1 ;;
esac

for spec in "${PROBES[@]}"; do
    IFS='|' read -r TIER PROBE Q <<< "${spec}"
    echo "[freshness ${ARM}/${TIER}] polling ${Q} every ${POLL_MS}ms × ${N_SAMPLES}"
    deltas=()
    for i in $(seq 1 ${N_SAMPLES}); do
        # Use last_over_time for a 10s window so a slightly delayed write still
        # registers. The result `value[1]` is the sample's value (the encoded
        # emission ts_ms).
        body=$(curl -s --max-time 2 \
            --data-urlencode "query=last_over_time(${PROBE}[10s])" \
            "${Q}/api/v1/query" 2>/dev/null || echo '')
        poll_ts_ms=$(($(date +%s%N)/1000000))
        v=$(echo "${body}" | python3 -c "
import sys,json
try:
    d=json.load(sys.stdin)
    r=d['data']['result']
    if r:
        # Take the most recent series's value
        print(int(float(r[0]['value'][1])))
    else:
        print('')
except: print('')
" 2>/dev/null)
        if [ -n "${v}" ]; then
            delta=$((poll_ts_ms - v))
            echo "${ARM},${PROBE},${TIER},${i},${poll_ts_ms},${v},${delta}" >> "${CSV}"
            deltas+=(${delta})
        else
            echo "${ARM},${PROBE},${TIER},${i},${poll_ts_ms},," >> "${CSV}"
        fi
        # sleep POLL_MS
        python3 -c "import time; time.sleep(${POLL_MS}/1000)" 2>/dev/null || true
    done

    # Per-tier summary stats (p50, p99)
    if [ ${#deltas[@]} -gt 0 ]; then
        printf "%s\n" "${deltas[@]}" | python3 -c "
import sys
v=sorted(int(x) for x in sys.stdin if x.strip())
n=len(v)
def pct(p): return v[max(0,min(n-1,int(p*n)))]
print(f'[freshness ${ARM}/${TIER}] n={n} p50={pct(0.50)}ms p90={pct(0.90)}ms p99={pct(0.99)}ms min={v[0]} max={v[-1]}')"
    else
        echo "[freshness ${ARM}/${TIER}] no successful polls"
    fi
done

echo "[freshness ${ARM}] done; csv=${CSV}"
