#!/usr/bin/env bash
# measure_freshness.sh — sample-generation-to-backend-write delay.
#
# Mechanism: otel-app emits `http_freshness_probe_{raw,warm,archive}`
# counters whose cumulative value is the Unix-epoch-ms at emission. We poll
# the relevant backend for `<probe>` (instant query — gives the latest sample's
# value as a number == its emission ts_ms). Δ = poll_response_ts_ms − value.
#
# Per #46 runbook §"Freshness probe protocol":
#   - For B0/B1 the only landing is Prometheus (no warm/archive tiers in use).
#     We use http_freshness_probe_raw (lands in Prometheus via PRW).
#   - For ASAP, measure the warm ASAPQuery-backend result. Archive fallback is
#     explicitly outside the MVP acceptance scope.
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
NODE1_IP=${NODE1_IP:-10.10.1.2}

mkdir -p "${OUT}"
CSV="${OUT}/freshness-${ARM}.csv"
echo "arm,probe,tier,poll_idx,poll_ts_ms,observed_value_ms,delta_ms" > "${CSV}"

# Per-tier endpoint + probe metric mapping
# NOTE: the probe is a Float64Counter WithUnit("ms"); depending on the OTLP→VM
# add_metric_suffixes setting the queryable name is either `http_freshness_probe_<tier>`
# or `http_freshness_probe_<tier>_milliseconds_total`. Match by __name__ regex so the
# probe is found regardless of the suffix the backend applied.

# Extra query args, set per arm. VictoriaMetrics defaults -search.latencyOffset
# to 30s (it evaluates instant queries 30s in the past to avoid partial head
# data), which made the raw-baseline freshness read ~31s instead of the true
# ~1s. Override it per-query with the minimum VM accepts (latency_offset=1ms;
# 0 is rejected as out-of-range) so freshness reflects when the sample is
# actually queryable, not VM's safety lag. The asap backend has no such offset,
# so this is only applied on the VM path.
VM_QARGS=()
case "${ARM}" in
    b0|b1|b2|b3)
        # Raw baseline lands in VictoriaMetrics, which serves PromQL on :8428
        # (NOT :9090 — there is no Prometheus in this topology; :9090 is
        # unreachable and was the cause of the prior "no successful polls").
        PROBES=('raw|{__name__=~"http_freshness_probe_raw.*"}|http://'"${NODE1_IP}"':8428')
        VM_QARGS=(--data-urlencode "latency_offset=1ms")
        ;;
    asap|asap-gzip)
        # The asap backend's EngineRouter keys on the EXACT raw metric name
        # (backend-storage-routing.yaml routes http_freshness_probe_{warm,archive}
        # → gorilla_s3_archive and serves last_over_time from the archive). A
        # __name__ regex would defeat that name-keyed routing, so use bare names.
        # (No raw-tier probe in asap: it isn't routed/queryable at the backend.)
        PROBES=(
            'warm|http_freshness_probe_warm|http://'"${NODE2_IP}"':9091'
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
            --data-urlencode "query=last_over_time(${PROBE}[15s])" \
            "${VM_QARGS[@]}" \
            "${Q}/api/v1/query" 2>/dev/null || echo '')
        poll_ts_ms=$(($(date +%s%N)/1000000))
        v=$(echo "${body}" | python3 -c "
import sys,json
try:
    d=json.load(sys.stdin)
    r=d['data']['result']
    # Multiple producers each emit the probe → one series each; take the most
    # recent (max) encoded emission ts_ms across all matching series.
    vals=[float(s['value'][1]) for s in r if s.get('value')]
    print(int(max(vals)) if vals else '')
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
