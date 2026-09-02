#!/usr/bin/env bash
# measure_freshness.sh — sample-generation-to-backend-write delay.
#
# Mechanism: the producer emits dedicated counters whose numeric value equals
# the Unix epoch milliseconds of the latest emission. Poll
# `last_over_time(probe[10s])` and subtract that returned value from the poll
# time. This measures data age; it does not trust a timestamp assigned by the
# backend while reconstructing a result.
#
# ASAP is measured separately for all three query classes and both full and
# delta modes used by the checked-in plan. Archive fallback is outside scope.
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
echo "arm,query_class,transmission_mode,tier,poll_idx,poll_ts_ms,observed_timestamp_ms,delta_ms" > "${CSV}"

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
        PROBES=('all|raw|warm|last_over_time({__name__=~"http_freshness_probe_raw(_milliseconds_total)?"}[10s])|http://'"${NODE1_IP}"':8428')
        VM_QARGS=(--data-urlencode "latency_offset=1ms")
        ;;
    asap|asap-gzip)
        # The asap backend's EngineRouter keys on the EXACT raw metric name
        # (backend-storage-routing.yaml routes http_freshness_probe_{warm,archive}
        # → gorilla_s3_archive and serves last_over_time from the archive). A
        # __name__ regex would defeat that name-keyed routing, so use bare names.
        # (No raw-tier probe in asap: it isn't routed/queryable at the backend.)
        PROBES=(
            'window-per-series|delta|warm|last_over_time(http_freshness_probe_warm[10s])|http://'"${NODE2_IP}"':9091'
            'label-at-timestamp|full|warm|last_over_time(http_freshness_probe_warm[10s])|http://'"${NODE2_IP}"':9091'
            'window-and-label|full|warm|last_over_time(http_freshness_probe_warm[10s])|http://'"${NODE2_IP}"':9091'
        )
        ;;
    *) echo "unknown arm ${ARM}" >&2; exit 1 ;;
esac

for spec in "${PROBES[@]}"; do
    IFS='|' read -r QUERY_CLASS MODE TIER PROBE Q <<< "${spec}"
    echo "[freshness ${ARM}/${QUERY_CLASS}/${MODE}] polling ${Q} every ${POLL_MS}ms × ${N_SAMPLES}"
    deltas=()
    for i in $(seq 1 ${N_SAMPLES}); do
        # Each spec is a complete instant query. Freshness uses the probe's
        # timestamp-encoded VALUE, never the returned sample timestamp.
        body=$(curl -s --max-time 2 \
            --data-urlencode "query=${PROBE}" \
            "${VM_QARGS[@]}" \
            "${Q}/api/v1/query" 2>/dev/null || echo '')
        poll_ts_ms=$(($(date +%s%N)/1000000))
        v=$(echo "${body}" | python3 -c "
import sys,json
try:
    d=json.load(sys.stdin)
    r=d['data']['result']
    vals=[float(s['value'][1]) for s in r if s.get('value')]
    print(int(max(vals)) if vals else '')
except: print('')
" 2>/dev/null)
        if [ -n "${v}" ]; then
            delta=$((poll_ts_ms - v))
            echo "${ARM},${QUERY_CLASS},${MODE},${TIER},${i},${poll_ts_ms},${v},${delta}" >> "${CSV}"
            deltas+=(${delta})
        else
            echo "${ARM},${QUERY_CLASS},${MODE},${TIER},${i},${poll_ts_ms},," >> "${CSV}"
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
