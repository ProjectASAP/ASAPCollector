#!/usr/bin/env bash
# bench_series_id.sh — Bandwidth benchmark: series_id enabled vs disabled
#
# Runs e2esdkbench with series_id on/off and compares wire bytes.
# Supports both sketch (ddsketch) and raw (baseline/Gauge) modes to show
# that savings are most visible with raw samples at high cardinality.
#
# Usage:
#   ./tests/bench_series_id.sh [--skip-build] [--duration 15s]
#   ./tests/bench_series_id.sh --sketch-type=baseline --series=1000
#   ./tests/bench_series_id.sh --all   # run all combinations

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONTROLLER_DIR="${ROOT}/controller"
SKETCHCOL="${ROOT}/opentelemetry-collector-contrib-patch/cmd/sketchcollector/sketchcollector"
E2EBENCH_DIR="${ROOT}/opentelemetry-app"

SERIES=500
BENCH_DURATION="15s"
SAMPLES_PER_SEC=10
SKETCH_TYPE="baseline"
SKIP_BUILD=false
RUN_ALL=false

for arg in "$@"; do
  case "$arg" in
    --series=*)       SERIES="${arg#*=}" ;;
    --duration=*)     BENCH_DURATION="${arg#*=}" ;;
    --rate=*)         SAMPLES_PER_SEC="${arg#*=}" ;;
    --sketch-type=*)  SKETCH_TYPE="${arg#*=}" ;;
    --skip-build)     SKIP_BUILD=true ;;
    --all)            RUN_ALL=true ;;
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

# ── run_single: one benchmark run ────────────────────────────────────────────
# Args: label enable_series_id sketch_type series
run_single() {
  local label="$1"
  local enable_sid="$2"
  local stype="$3"
  local nseries="$4"
  local metric_name="bench_${label}"
  local out_dir="${OUTPUT_DIR}/${label}"
  mkdir -p "$out_dir"

  # Kill leftover processes
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

  # Submit plan (use ddsketch as processor type — baseline samples pass through)
  curl -sf -X POST "${CONTROLLER_API}/api/v1/plan" \
    -H "Content-Type: application/json" \
    -d "{
      \"metric_name\": \"${metric_name}\",
      \"aggregations\": [\"quantile\"],
      \"time_window\": \"5m\",
      \"accuracy_sla\": 0.01,
      \"sketch_type\": \"ddsketch\",
      \"workload\": {
        \"series_count\": ${nseries},
        \"samples_per_sec_per_series\": ${SAMPLES_PER_SEC},
        \"bytes_per_raw_sample\": 100,
        \"data_distribution\": \"zipf\"
      }
    }" > /dev/null 2>&1

  # Fetch config and optionally disable series_id
  CONFIG_YAML=$(curl -sf "${CONTROLLER_API}/api/v1/config/${metric_name}")
  if [[ "$enable_sid" == "false" ]]; then
    CONFIG_YAML=$(echo "$CONFIG_YAML" | sed 's/enable_series_id: true/enable_series_id: false/')
  fi
  echo "$CONFIG_YAML" > "${out_dir}/collector-config.yaml"

  # Start collector
  "$SKETCHCOL" --config="${out_dir}/collector-config.yaml" > "${out_dir}/collector.log" 2>&1 &
  COLLECTOR_PID=$!

  for i in $(seq 1 30); do
    curl -sf "http://localhost:8889/metrics" > /dev/null 2>&1 && break
    sleep 1
    [[ $i -eq 30 ]] && { echo "  ERROR: collector timeout" >&2; return 1; }
  done

  # Run benchmark
  pushd "$E2EBENCH_DIR" > /dev/null
  go run ./cmd/e2esdkbench \
    --sketch-type="$stype" \
    --endpoint="localhost:4317" \
    --series="$nseries" \
    --samples-per-sec-per-series="$SAMPLES_PER_SEC" \
    --duration="$BENCH_DURATION" \
    --output-dir="$out_dir" > "${out_dir}/bench.log" 2>&1
  popd > /dev/null

  # Cleanup for next run
  kill "$COLLECTOR_PID" 2>/dev/null; wait "$COLLECTOR_PID" 2>/dev/null || true
  kill "$CONTROLLER_PID" 2>/dev/null; wait "$CONTROLLER_PID" 2>/dev/null || true
  COLLECTOR_PID=""
  CONTROLLER_PID=""
}

# ── compare: extract and compare two runs ────────────────────────────────────
compare() {
  local label_on="$1"
  local label_off="$2"
  local stype="$3"
  local nseries="$4"

  local on_json=$(ls "${OUTPUT_DIR}/${label_on}"/*_summary.json 2>/dev/null | head -1)
  local off_json=$(ls "${OUTPUT_DIR}/${label_off}"/*_summary.json 2>/dev/null | head -1)

  if [[ -z "$on_json" || -z "$off_json" ]]; then
    echo "  [WARN] Missing summary files, skipping comparison"
    return
  fi

  python3 -c "
import json
with open('${on_json}') as f: on = json.load(f)
with open('${off_json}') as f: off = json.load(f)

dur_on = on['duration_sec']
dur_off = off['duration_sec']
rate = ${SAMPLES_PER_SEC}
series = ${nseries}

samples_on = series * rate * dur_on
samples_off = series * rate * dur_off

bps_on = on['total_bytes_sent'] / samples_on if samples_on > 0 else 0
bps_off = off['total_bytes_sent'] / samples_off if samples_off > 0 else 0

savings = (1 - bps_on / bps_off) * 100 if bps_off > 0 else 0

print(f'  {\"${stype}\":>12s} @ {series:>5} series:  '
      f'with_sid={bps_on:6.1f} B/sample  '
      f'without={bps_off:6.1f} B/sample  '
      f'savings={savings:5.1f}%  '
      f'({on[\"total_bytes_sent\"]} vs {off[\"total_bytes_sent\"]} bytes)')
"
}

# ── Main ─────────────────────────────────────────────────────────────────────
echo "Series ID Bandwidth Benchmark"
echo "Duration: ${BENCH_DURATION}, Rate: ${SAMPLES_PER_SEC} sps"
echo "Output: ${OUTPUT_DIR}"
echo ""

if [[ "$RUN_ALL" == true ]]; then
  # Run multiple combinations: baseline (raw Gauge) at various cardinalities
  # + ddsketch for comparison
  COMBOS=(
    "baseline:100"
    "baseline:500"
    "baseline:1000"
    "ddsketch:100"
    "ddsketch:500"
  )
else
  COMBOS=("${SKETCH_TYPE}:${SERIES}")
fi

RESULTS=()
for combo in "${COMBOS[@]}"; do
  IFS=':' read -r stype nseries <<< "$combo"
  label_on="${stype}_${nseries}_sid_on"
  label_off="${stype}_${nseries}_sid_off"

  echo "── ${stype} @ ${nseries} series ──"
  echo -n "  Running with series_id=true... "
  run_single "$label_on" "true" "$stype" "$nseries"
  echo "done"

  echo -n "  Running with series_id=false... "
  run_single "$label_off" "false" "$stype" "$nseries"
  echo "done"

  RESULTS+=("${label_on}:${label_off}:${stype}:${nseries}")
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Results"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

for r in "${RESULTS[@]}"; do
  IFS=':' read -r label_on label_off stype nseries <<< "$r"
  compare "$label_on" "$label_off" "$stype" "$nseries"
done

echo ""
echo "Results saved to: ${OUTPUT_DIR}/"
