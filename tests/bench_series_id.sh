#!/usr/bin/env bash
# bench_series_id.sh — Bandwidth benchmark: series_id enabled vs disabled
#
# Runs e2esdkbench twice against the controller+sketchcollector:
#   1. With enable_series_id: true  (UID registry active)
#   2. With enable_series_id: false (stateless OTLP, full attrs every sample)
#
# Compares total bytes sent and per-sample overhead.
#
# Usage:
#   ./tests/bench_series_id.sh [--skip-build] [--series 100] [--duration 20s]

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONTROLLER_DIR="${ROOT}/controller"
SKETCHCOL="${ROOT}/opentelemetry-collector-contrib-patch/cmd/sketchcollector/sketchcollector"
E2EBENCH_DIR="${ROOT}/opentelemetry-app"

SERIES=100
BENCH_DURATION="20s"
SAMPLES_PER_SEC=10
SKIP_BUILD=false

for arg in "$@"; do
  case "$arg" in
    --series=*)     SERIES="${arg#*=}" ;;
    --duration=*)   BENCH_DURATION="${arg#*=}" ;;
    --rate=*)       SAMPLES_PER_SEC="${arg#*=}" ;;
    --skip-build)   SKIP_BUILD=true ;;
    *) echo "Unknown arg: $arg" >&2; exit 1 ;;
  esac
done

CONTROLLER_API="http://localhost:8080"
OUTPUT_DIR="/tmp/bench_series_id_$(date +%s)"
mkdir -p "$OUTPUT_DIR"

CONTROLLER_PID=""
COLLECTOR_PID=""

cleanup() {
  [[ -n "$CONTROLLER_PID" ]] && kill "$CONTROLLER_PID" 2>/dev/null || true
  [[ -n "$COLLECTOR_PID"  ]] && kill "$COLLECTOR_PID"  2>/dev/null || true
}
trap cleanup EXIT

CONTROLLER_BIN="${CONTROLLER_DIR}/target/release/controller"
[[ ! -x "$CONTROLLER_BIN" ]] && { echo "ERROR: controller not built" >&2; exit 1; }
[[ ! -x "$SKETCHCOL" ]]      && { echo "ERROR: sketchcollector not built" >&2; exit 1; }

run_bench() {
  local label="$1"
  local enable_series_id="$2"
  local metric_name="bench_${label}"
  local out_dir="${OUTPUT_DIR}/${label}"
  mkdir -p "$out_dir"

  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  Benchmark: ${label} (enable_series_id=${enable_series_id})"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # Kill any leftover processes
  for port in 8080 4320 4317 8889; do
    fuser -k "$port/tcp" 2>/dev/null || true
  done
  sleep 0.5

  # Start controller
  CONTROLLER_ADDR="0.0.0.0:8080" \
  CONTROLLER_OPAMP_ADDR="0.0.0.0:4320" \
  CONTROLLER_OPAMP_ENDPOINT="ws://localhost:4320/v1/opamp" \
  "$CONTROLLER_BIN" > "${out_dir}/controller.log" 2>&1 &
  CONTROLLER_PID=$!

  for i in $(seq 1 20); do
    curl -sf "${CONTROLLER_API}/api/v1/agents" > /dev/null 2>&1 && break
    sleep 0.5
  done

  # Submit plan
  curl -sf -X POST "${CONTROLLER_API}/api/v1/plan" \
    -H "Content-Type: application/json" \
    -d "{
      \"metric_name\": \"${metric_name}\",
      \"aggregations\": [\"quantile\"],
      \"time_window\": \"5m\",
      \"accuracy_sla\": 0.01,
      \"sketch_type\": \"ddsketch\",
      \"workload\": {
        \"series_count\": ${SERIES},
        \"samples_per_sec_per_series\": ${SAMPLES_PER_SEC},
        \"bytes_per_raw_sample\": 100,
        \"data_distribution\": \"zipf\"
      }
    }" > /dev/null 2>&1

  # If we need to disable series_id, patch the generated config
  CONFIG_YAML=$(curl -sf "${CONTROLLER_API}/api/v1/config/${metric_name}")
  if [[ "$enable_series_id" == "false" ]]; then
    CONFIG_YAML=$(echo "$CONFIG_YAML" | sed 's/enable_series_id: true/enable_series_id: false/')
  fi
  echo "$CONFIG_YAML" > "${out_dir}/collector-config.yaml"

  # Start collector with the (possibly patched) config file
  "$SKETCHCOL" --config="${out_dir}/collector-config.yaml" > "${out_dir}/collector.log" 2>&1 &
  COLLECTOR_PID=$!

  for i in $(seq 1 30); do
    curl -sf "http://localhost:8889/metrics" > /dev/null 2>&1 && break
    sleep 1
    if [[ $i -eq 30 ]]; then
      echo "  ERROR: collector timeout" >&2
      tail -5 "${out_dir}/collector.log" >&2
      return 1
    fi
  done

  # Run benchmark
  pushd "$E2EBENCH_DIR" > /dev/null
  go run ./cmd/e2esdkbench \
    --sketch-type="ddsketch" \
    --endpoint="localhost:4317" \
    --series="$SERIES" \
    --samples-per-sec-per-series="$SAMPLES_PER_SEC" \
    --duration="$BENCH_DURATION" \
    --output-dir="$out_dir" 2>&1 | grep -E "^(===|Sketch|Series|Rate|Total|Avg|Peak)" | sed 's/^/  /'
  popd > /dev/null

  # Cleanup processes for next run
  kill "$COLLECTOR_PID" 2>/dev/null; wait "$COLLECTOR_PID" 2>/dev/null || true
  kill "$CONTROLLER_PID" 2>/dev/null; wait "$CONTROLLER_PID" 2>/dev/null || true
  COLLECTOR_PID=""
  CONTROLLER_PID=""
}

# ── Run both benchmarks ──────────────────────────────────────────────────────
echo "Series ID Bandwidth Benchmark"
echo "Series: ${SERIES}, Rate: ${SAMPLES_PER_SEC} sps, Duration: ${BENCH_DURATION}"

run_bench "with_series_id" "true"
run_bench "without_series_id" "false"

# ── Compare results ──────────────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Comparison"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

WITH_JSON=$(ls "${OUTPUT_DIR}/with_series_id"/ddsketch_*_summary.json 2>/dev/null | head -1)
WITHOUT_JSON=$(ls "${OUTPUT_DIR}/without_series_id"/ddsketch_*_summary.json 2>/dev/null | head -1)

if [[ -n "$WITH_JSON" && -n "$WITHOUT_JSON" ]]; then
  python3 -c "
import json, sys

with open('${WITH_JSON}') as f: w = json.load(f)
with open('${WITHOUT_JSON}') as f: wo = json.load(f)

dur_w = w['duration_sec']
dur_wo = wo['duration_sec']
series = ${SERIES}
rate = ${SAMPLES_PER_SEC}

samples_w = series * rate * dur_w
samples_wo = series * rate * dur_wo

bps_w = w['total_bytes_sent'] / samples_w if samples_w > 0 else 0
bps_wo = wo['total_bytes_sent'] / samples_wo if samples_wo > 0 else 0

print(f'  With series_id:    {w[\"total_bytes_sent\"]:>10} bytes  ({bps_w:.1f} B/sample)  avg {w[\"avg_bandwidth_bps\"]:.0f} B/s')
print(f'  Without series_id: {wo[\"total_bytes_sent\"]:>10} bytes  ({bps_wo:.1f} B/sample)  avg {wo[\"avg_bandwidth_bps\"]:.0f} B/s')
if bps_wo > 0:
    savings = (1 - bps_w / bps_wo) * 100
    print(f'  Savings:           {savings:.1f}%')
else:
    print('  Savings:           N/A')
"
else
  echo "  Could not find summary files for comparison."
fi

echo ""
echo "Results saved to: ${OUTPUT_DIR}/"
