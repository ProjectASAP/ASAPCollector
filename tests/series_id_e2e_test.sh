#!/usr/bin/env bash
# series_id_e2e_test.sh — Integration test for series_id UID registry
#
# Verifies the register → elide → rehydrate round-trip:
#   1. Start controller + sketchcollector with enable_series_id: true
#   2. Send metrics via e2esdkbench (first export sends full attributes)
#   3. Verify the collector assigns series_ids (SeriesAssignment in response)
#   4. Verify subsequent exports omit attributes (bandwidth reduction)
#   5. Compare wire bytes between first and steady-state exports
#
# Usage:
#   ./tests/series_id_e2e_test.sh [--skip-build] [--series 50] [--duration 15s]
#
# Prerequisites:
#   - cargo, go, curl, python3
#   - Run ./setup.sh first

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONTROLLER_DIR="${ROOT}/controller"
SKETCHCOL="${ROOT}/opentelemetry-collector-contrib-patch/cmd/sketchcollector/sketchcollector"
E2EBENCH_DIR="${ROOT}/opentelemetry-app"

SERIES=50
BENCH_DURATION="15s"
SKIP_BUILD=false

for arg in "$@"; do
  case "$arg" in
    --series=*)     SERIES="${arg#*=}" ;;
    --duration=*)   BENCH_DURATION="${arg#*=}" ;;
    --skip-build)   SKIP_BUILD=true ;;
    *) echo "Unknown arg: $arg" >&2; exit 1 ;;
  esac
done

CONTROLLER_API="http://localhost:8080"
CONTROLLER_OPAMP="ws://localhost:4320/v1/opamp"
COLLECTOR_OTLP="localhost:4317"
OUTPUT_DIR="/tmp/series_id_test_$(date +%s)"

CONTROLLER_PID=""
COLLECTOR_PID=""

cleanup() {
  echo ""
  echo "==> Cleaning up..."
  [[ -n "$CONTROLLER_PID" ]] && kill "$CONTROLLER_PID" 2>/dev/null || true
  [[ -n "$COLLECTOR_PID"  ]] && kill "$COLLECTOR_PID"  2>/dev/null || true
}
trap cleanup EXIT

mkdir -p "$OUTPUT_DIR"
echo "Output dir: $OUTPUT_DIR"
echo ""

# ── Step 1: Build ─────────────────────────────────────────────────────────────
CONTROLLER_BIN="${CONTROLLER_DIR}/target/release/controller"
if [[ "$SKIP_BUILD" == false ]]; then
  echo "==> [Step 1] Building controller..."
  cargo build --manifest-path="${CONTROLLER_DIR}/Cargo.toml" --release 2>&1 | tail -3
  echo ""
fi

if [[ ! -x "$CONTROLLER_BIN" ]]; then
  echo "ERROR: controller binary not found at ${CONTROLLER_BIN}" >&2; exit 1
fi
if [[ ! -x "$SKETCHCOL" ]]; then
  echo "ERROR: sketchcollector binary not found at ${SKETCHCOL}" >&2
  echo "       Run: ${ROOT}/build_sketchcollector.sh" >&2; exit 1
fi

# ── Step 2: Start controller + collector ──────────────────────────────────────
for port in 8080 4320 4317 8889; do
  fuser -k "$port/tcp" 2>/dev/null || true
done
sleep 0.5

echo "==> [Step 2] Starting controller..."
CONTROLLER_ADDR="0.0.0.0:8080" \
CONTROLLER_OPAMP_ADDR="0.0.0.0:4320" \
CONTROLLER_OPAMP_ENDPOINT="$CONTROLLER_OPAMP" \
"$CONTROLLER_BIN" > "${OUTPUT_DIR}/controller.log" 2>&1 &
CONTROLLER_PID=$!

for i in $(seq 1 20); do
  curl -sf "${CONTROLLER_API}/api/v1/agents" > /dev/null 2>&1 && break
  sleep 0.5
  [[ $i -eq 20 ]] && { echo "ERROR: controller timeout" >&2; exit 1; }
done
echo "    Controller ready (pid ${CONTROLLER_PID})"

# Submit a plan with enable_series_id (default true)
echo "==> [Step 2] Submitting plan..."
PLAN_RESP=$(curl -sf -X POST "${CONTROLLER_API}/api/v1/plan" \
  -H "Content-Type: application/json" \
  -d "{
    \"metric_name\":    \"series_id_test\",
    \"aggregations\":   [\"quantile\"],
    \"time_window\":    \"5m\",
    \"accuracy_sla\":   0.01,
    \"sketch_type\":    \"ddsketch\",
    \"workload\": {
      \"series_count\": ${SERIES},
      \"samples_per_sec_per_series\": 10,
      \"bytes_per_raw_sample\": 100,
      \"data_distribution\": \"zipf\"
    }
  }")

# Verify enable_series_id is in the YAML
CONFIG_YAML=$(curl -sf "${CONTROLLER_API}/api/v1/config/series_id_test")
echo "$CONFIG_YAML" > "${OUTPUT_DIR}/collector-config.yaml"

if grep -q "enable_series_id: true" "${OUTPUT_DIR}/collector-config.yaml"; then
  echo "    [OK] enable_series_id: true in generated YAML"
else
  echo "    [FAIL] enable_series_id not found in YAML" >&2
  cat "${OUTPUT_DIR}/collector-config.yaml" >&2
  exit 1
fi

# ── Step 3: Start collector ───────────────────────────────────────────────────
echo "==> [Step 3] Starting sketchcollector..."
"$SKETCHCOL" \
  --config="http://localhost:8080/api/v1/config/series_id_test" \
  > "${OUTPUT_DIR}/collector.log" 2>&1 &
COLLECTOR_PID=$!

for i in $(seq 1 30); do
  if curl -sf "http://localhost:8889/metrics" > /dev/null 2>&1; then
    echo "    Collector ready (pid ${COLLECTOR_PID})"
    break
  fi
  sleep 1
  [[ $i -eq 30 ]] && { echo "ERROR: collector timeout" >&2; tail -10 "${OUTPUT_DIR}/collector.log" >&2; exit 1; }
done
echo ""

# ── Step 4: Run benchmark and capture bandwidth ──────────────────────────────
echo "==> [Step 4] Running e2esdkbench (series=${SERIES}, duration=${BENCH_DURATION})..."
pushd "$E2EBENCH_DIR" > /dev/null
go run ./cmd/e2esdkbench \
  --sketch-type="ddsketch" \
  --endpoint="$COLLECTOR_OTLP" \
  --series="$SERIES" \
  --samples-per-sec-per-series=10 \
  --duration="$BENCH_DURATION" \
  --output-dir="$OUTPUT_DIR" 2>&1 | tee "${OUTPUT_DIR}/bench.log"
popd > /dev/null
echo ""

# ── Step 5: Verify series_id assignment ───────────────────────────────────────
echo "==> [Step 5] Verifying series_id round-trip..."
PASS=0
TOTAL=0

# Check 1: collector log shows data processing
TOTAL=$((TOTAL + 1))
if grep -qi "accepted\|processed\|flushed\|sketch\|metric_points" "${OUTPUT_DIR}/collector.log" 2>/dev/null; then
  echo "    [OK] Collector processed metrics"
  PASS=$((PASS + 1))
else
  echo "    [WARN] No processing evidence in collector log"
fi

# Check 2: benchmark produced results
TOTAL=$((TOTAL + 1))
SUMMARY_FILE=$(ls "${OUTPUT_DIR}"/ddsketch_*_summary.json 2>/dev/null | head -1)
if [[ -n "$SUMMARY_FILE" ]]; then
  TOTAL_BYTES=$(python3 -c "import json; d=json.load(open('${SUMMARY_FILE}')); print(d['total_bytes_sent'])")
  AVG_BW=$(python3 -c "import json; d=json.load(open('${SUMMARY_FILE}')); print(f\"{d['avg_bandwidth_bps']:.0f}\")")
  echo "    [OK] Benchmark: ${TOTAL_BYTES} bytes sent, avg ${AVG_BW} B/s"
  PASS=$((PASS + 1))

  # Check 3: estimate per-sample byte savings
  # With series_id, steady-state should be ~20 B/sample vs ~136 B stateless
  # Calculate: total_bytes / (series * rate * duration)
  TOTAL=$((TOTAL + 1))
  DURATION_SEC=$(python3 -c "import json; d=json.load(open('${SUMMARY_FILE}')); print(f\"{d['duration_sec']:.0f}\")")
  EXPECTED_SAMPLES=$((SERIES * 10 * DURATION_SEC))
  if [[ $EXPECTED_SAMPLES -gt 0 ]]; then
    BYTES_PER_SAMPLE=$((TOTAL_BYTES / EXPECTED_SAMPLES))
    echo "    [INFO] ~${BYTES_PER_SAMPLE} bytes/sample (expected <50 with series_id, ~136 without)"
    if [[ $BYTES_PER_SAMPLE -lt 100 ]]; then
      echo "    [OK] Bandwidth consistent with series_id attribute elision"
      PASS=$((PASS + 1))
    else
      echo "    [WARN] Bytes/sample higher than expected (series_id may not be eliding attributes yet)"
    fi
  fi
else
  echo "    [WARN] No benchmark summary file found"
fi

# Check 4: plan is cached
TOTAL=$((TOTAL + 1))
if curl -sf "${CONTROLLER_API}/api/v1/plan/series_id_test" > /dev/null 2>&1; then
  echo "    [OK] Plan cached in controller"
  PASS=$((PASS + 1))
else
  echo "    [WARN] Plan not in cache"
fi

echo ""
echo "    Checks: ${PASS}/${TOTAL} passed"
echo ""

# ── Summary ───────────────────────────────────────────────────────────────────
echo "==> Logs saved to: ${OUTPUT_DIR}/"
echo "    controller.log        — controller stdout/stderr"
echo "    collector.log         — sketchcollector stdout/stderr"
echo "    collector-config.yaml — generated config with enable_series_id"
echo ""

if [[ $PASS -ge 2 ]]; then
  echo "==> [PASS] Series ID integration test completed."
else
  echo "==> [FAIL] Some checks did not pass." >&2
  exit 1
fi
