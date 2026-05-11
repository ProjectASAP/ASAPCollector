#!/usr/bin/env bash
# validate_arm.sh — query the arm's raw backend and compare what got
# ingested vs what should have been generated.
#
# For B0/B1: query Prometheus on node2:9090
# For ASAP: query asap-query-backend on node2:9091 (warm tier),
#           and `mc ls` MinIO bucket for archive object count
#
# Inputs (env):
#   ARM         b0 | b1 | asap
#   N_PRODUCERS_PER_NODE × 2 → expected distinct producer_ids
#   PER_AGENT_CARDINALITY    → expected series-per-producer
#   EXPORTER_FREQ_HZ         → expected events/series/sec
#   OUT                      → output directory (writes validate-<arm>.json + .md)
#
# Output: a JSON snapshot + a markdown table that says PASS/PARTIAL/FAIL
set -euo pipefail
ARM=${ARM:?arm}
OUT=${OUT:?out}
NODE2_IP=${NODE2_IP:-10.10.1.3}
N_PRODUCERS_PER_NODE=${N_PRODUCERS_PER_NODE:-5}
PER_AGENT_CARDINALITY=${PER_AGENT_CARDINALITY:-1000}
EXPORTER_FREQ_HZ=${EXPORTER_FREQ_HZ:-100}

EXPECTED_PRODUCERS=$((N_PRODUCERS_PER_NODE * 2))
EXPECTED_SERIES=$((EXPECTED_PRODUCERS * PER_AGENT_CARDINALITY))
EXPECTED_RATE_PER_SERIES=${EXPORTER_FREQ_HZ}
EXPECTED_AGG_RATE=$((EXPECTED_SERIES * EXPECTED_RATE_PER_SERIES))

mkdir -p "${OUT}"

# Pick query endpoint per arm
if [ "${ARM}" = "asap" ]; then
    Q="http://${NODE2_IP}:9091"
    METRIC="http_requests_total"
else
    Q="http://${NODE2_IP}:9090"
    METRIC="http_requests_total"
fi

q() {
    curl -s --max-time 5 --data-urlencode "query=$1" "${Q}/api/v1/query"
}

# 1. Distinct series count
S_BODY=$(q "count(${METRIC})")
SERIES_COUNT=$(echo "${S_BODY}" | python3 -c "import sys,json; r=json.load(sys.stdin)['data']['result']; print(r[0]['value'][1] if r else 0)" 2>/dev/null || echo 0)

# 2. Distinct producer_id values
P_BODY=$(q "count(count(${METRIC}) by (producer_id))")
PRODUCER_COUNT=$(echo "${P_BODY}" | python3 -c "import sys,json; r=json.load(sys.stdin)['data']['result']; print(r[0]['value'][1] if r else 0)" 2>/dev/null || echo 0)

# 3. List of producer_ids
PIDS_BODY=$(q "group(${METRIC}) by (producer_id)")
PIDS=$(echo "${PIDS_BODY}" | python3 -c "
import sys,json
d=json.load(sys.stdin)['data']['result']
ids=sorted(set(s['metric'].get('producer_id','MISSING') for s in d))
print(','.join(ids))" 2>/dev/null || echo "")

# 4. Per-series rate (avg counter increment over 1m)
R_BODY=$(q "avg(rate(${METRIC}[1m]))")
PER_SERIES_RATE=$(echo "${R_BODY}" | python3 -c "import sys,json; r=json.load(sys.stdin)['data']['result']; print(f\"{float(r[0]['value'][1]):.2f}\" if r else 0)" 2>/dev/null || echo 0)

# 5. Aggregate event rate
AGG_BODY=$(q "sum(rate(${METRIC}[1m]))")
AGG_RATE=$(echo "${AGG_BODY}" | python3 -c "import sys,json; r=json.load(sys.stdin)['data']['result']; print(f\"{float(r[0]['value'][1]):.0f}\" if r else 0)" 2>/dev/null || echo 0)

# 6. count_over_time over a 10s window — per-series sample count.
# Expected ≈ EXPORTER_FREQ_HZ × 10 (e.g. 100 Hz × 10 s = 1000 samples).
SAMPLE_BODY=$(q "count_over_time(${METRIC}[10s])")
EXPECTED_SAMPLES_10S=$((EXPORTER_FREQ_HZ * 10))
PER_SERIES_SAMPLES_MIN=$(echo "${SAMPLE_BODY}" | python3 -c "
import sys,json
r=json.load(sys.stdin)['data']['result']
counts=[int(s['value'][1]) for s in r]
print(min(counts) if counts else 0)" 2>/dev/null || echo 0)
PER_SERIES_SAMPLES_MEAN=$(echo "${SAMPLE_BODY}" | python3 -c "
import sys,json
r=json.load(sys.stdin)['data']['result']
counts=[int(s['value'][1]) for s in r]
print(f'{sum(counts)/len(counts):.1f}' if counts else 0)" 2>/dev/null || echo 0)
PER_SERIES_SAMPLES_MAX=$(echo "${SAMPLE_BODY}" | python3 -c "
import sys,json
r=json.load(sys.stdin)['data']['result']
counts=[int(s['value'][1]) for s in r]
print(max(counts) if counts else 0)" 2>/dev/null || echo 0)
# Also compute a verdict: PASS if mean ≥ 80% of expected
SAMPLES_OK=$(python3 -c "
e=${EXPECTED_SAMPLES_10S}; got=${PER_SERIES_SAMPLES_MEAN:-0}
ratio = got/e if e else 0
print('PASS' if ratio >= 0.8 else ('PARTIAL' if ratio >= 0.5 else 'FAIL'))" 2>/dev/null || echo FAIL)

# Verdict logic
SERIES_OK="FAIL"; [ "${SERIES_COUNT}" -ge $((EXPECTED_SERIES * 90 / 100)) ] && SERIES_OK="PASS" || true
PROD_OK="FAIL"; [ "${PRODUCER_COUNT}" -eq "${EXPECTED_PRODUCERS}" ] && PROD_OK="PASS" || true
# Per-series rate within ±20% of expected
RATE_OK=$(python3 -c "
e=${EXPORTER_FREQ_HZ}; got=${PER_SERIES_RATE:-0}
print('PASS' if abs(got-e)/e <= 0.2 else ('PARTIAL' if got>=e*0.5 else 'FAIL'))" 2>/dev/null || echo FAIL)

# Write JSON
cat > "${OUT}/validate-${ARM}.json" <<EOF
{
  "arm": "${ARM}",
  "expected": {
    "series": ${EXPECTED_SERIES},
    "producers": ${EXPECTED_PRODUCERS},
    "per_series_rate_events_per_s": ${EXPECTED_RATE_PER_SERIES},
    "aggregate_rate_events_per_s": ${EXPECTED_AGG_RATE}
  },
  "observed": {
    "series_count": ${SERIES_COUNT},
    "producer_count": ${PRODUCER_COUNT},
    "producer_ids": "${PIDS}",
    "per_series_rate_events_per_s": ${PER_SERIES_RATE},
    "aggregate_rate_events_per_s": ${AGG_RATE},
    "per_series_samples_in_1m_min": ${PER_SERIES_SAMPLES_MIN},
    "per_series_samples_in_1m_mean": ${PER_SERIES_SAMPLES_MEAN},
    "per_series_samples_in_1m_max": ${PER_SERIES_SAMPLES_MAX}
  },
  "verdict": {
    "series_count": "${SERIES_OK}",
    "producer_count": "${PROD_OK}",
    "per_series_rate": "${RATE_OK}",
    "per_series_samples_in_10s": "${SAMPLES_OK}"
  }
}
EOF

# Markdown summary
cat > "${OUT}/validate-${ARM}.md" <<EOF
## Arm \`${ARM}\` ingest validation

| metric | expected | observed | verdict |
|---|---|---|---|
| distinct series | ${EXPECTED_SERIES} | ${SERIES_COUNT} | ${SERIES_OK} |
| distinct producers | ${EXPECTED_PRODUCERS} | ${PRODUCER_COUNT} | ${PROD_OK} |
| per-series rate (events/s) | ${EXPECTED_RATE_PER_SERIES} | ${PER_SERIES_RATE} | ${RATE_OK} |
| aggregate rate (events/s) | ${EXPECTED_AGG_RATE} | ${AGG_RATE} | — |
| samples per series in last 10s | ~${EXPORTER_FREQ_HZ}×10 = ${EXPECTED_SAMPLES_10S} | min=${PER_SERIES_SAMPLES_MIN}, mean=${PER_SERIES_SAMPLES_MEAN}, max=${PER_SERIES_SAMPLES_MAX} | ${SAMPLES_OK} |

producer_ids seen: \`${PIDS}\`
EOF

echo "[validate ${ARM}] wrote ${OUT}/validate-${ARM}.{json,md}"
cat "${OUT}/validate-${ARM}.md"
