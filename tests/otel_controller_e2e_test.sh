#!/usr/bin/env bash
# e2e_test.sh — End-to-end integration test: controller + ddsketchcol + e2esdkbench
#
# Usage:
#   ./tests/otel_controller_e2e_test.sh
#       [--sketch ddsketch|kll|hll|countsketch|countminsketch]
#       [--series 500]
#       [--rate 50]          # samples-per-sec-per-series sent by e2esdkbench
#       [--duration 30s]
#       [--distribution zipf|uniform|bursty]
#       [--memory-budget-mb N]   # optional agent memory budget in MiB
#       [--skip-build]
#       [--all-sketches]     # plan+YAML check for all 5 sketch types (no collector/bench)
#
# The test submits a WorkloadCharacteristics alongside the plan request so
# the controller can make a delta transmission decision.  It verifies that:
#   • The plan response contains delta_decision and transmission_costs.
#   • The generated collector YAML contains delta_transmission when delta is used.
#   • The YAML processor key matches the planned sketch type.
#   • The pipeline processor list references the same key as the processor block.
#   • e2esdkbench successfully delivers metrics through the collector (single-sketch mode).
#
# Prerequisites (must already be on PATH or built):
#   - cargo          (Rust toolchain)
#   - go             (Go toolchain)
#   - ddsketchcol    (built via ../../build_ddsketchcol.sh)
#   - curl, jq

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONTROLLER_DIR="${ROOT}/controller"
DDSKETCHCOL="${ROOT}/opentelemetry-collector-contrib-patch/cmd/ddsketchcol/ddsketchcol"
E2EBENCH_DIR="${ROOT}/opentelemetry-app"

# ── Defaults ──────────────────────────────────────────────────────────────────
SKETCH="ddsketch"
SERIES=500
SAMPLES_PER_SEC=50
BENCH_DURATION="30s"
DISTRIBUTION="zipf"
MEMORY_BUDGET_MB=""
SKIP_BUILD=false
ALL_SKETCHES=false

for arg in "$@"; do
  case "$arg" in
    --sketch=*)            SKETCH="${arg#*=}" ;;
    --series=*)            SERIES="${arg#*=}" ;;
    --rate=*)              SAMPLES_PER_SEC="${arg#*=}" ;;
    --duration=*)          BENCH_DURATION="${arg#*=}" ;;
    --distribution=*)      DISTRIBUTION="${arg#*=}" ;;
    --memory-budget-mb=*)  MEMORY_BUDGET_MB="${arg#*=}" ;;
    --skip-build)          SKIP_BUILD=true ;;
    --all-sketches)        ALL_SKETCHES=true ;;
    *) echo "Unknown arg: $arg" >&2; exit 1 ;;
  esac
done

# ── Sketch-type helpers ────────────────────────────────────────────────────────

# Maps a sketch type to the aggregation type used in the plan request.
sketch_to_aggregation() {
  case "$1" in
    ddsketch|kll)                    echo "quantile" ;;
    hll)                             echo "cardinality" ;;
    countsketch|countminsketch)      echo "frequency" ;;
    *) echo "quantile" ;;
  esac
}

# Maps a sketch type to the expected processor key in the generated YAML.
sketch_to_processor_key() {
  echo "$1"   # processor key == sketch type string for all current types
}

# Returns the sketch_type JSON field value to include in the plan request.
# Only needed when the default planner selection for the aggregation type
# differs from the desired sketch (e.g. kll overrides the default ddsketch
# for quantile queries).
sketch_to_override_json() {
  case "$1" in
    kll|countminsketch)  echo "\"sketch_type\": \"$1\"," ;;
    *)                   echo "" ;;  # rely on planner default
  esac
}

# Build the optional memory_budget field for the JSON body.
if [[ -n "$MEMORY_BUDGET_MB" ]]; then
  MEMORY_BUDGET_JSON="$(( MEMORY_BUDGET_MB * 1024 * 1024 ))"
else
  MEMORY_BUDGET_JSON="null"
fi

CONTROLLER_API="http://localhost:8080"
CONTROLLER_OPAMP="ws://localhost:4320/v1/opamp"
COLLECTOR_OTLP="localhost:4317"
COLLECTOR_PROM="http://localhost:8889/metrics"
METRIC_NAME="latency"
OUTPUT_DIR="/tmp/e2e_test_$(date +%s)"

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
if [[ "$SKIP_BUILD" == false ]]; then
  echo "==> [Step 1] Building controller..."
  cargo build --manifest-path="${CONTROLLER_DIR}/Cargo.toml" --release 2>&1 | tail -3
  echo ""

  if [[ ! -x "$DDSKETCHCOL" ]]; then
    echo "==> [Step 1] Building ddsketchcol..."
    bash "${ROOT}/build_ddsketchcol.sh" --skip-patches
    echo ""
  fi
else
  echo "==> [Step 1] Skipping build (--skip-build)"
fi

CONTROLLER_BIN="${CONTROLLER_DIR}/target/release/controller"
if [[ ! -x "$CONTROLLER_BIN" ]]; then
  echo "ERROR: controller binary not found at ${CONTROLLER_BIN}" >&2
  echo "       Run without --skip-build, or run: cargo build --release" >&2
  exit 1
fi
if [[ ! -x "$DDSKETCHCOL" ]]; then
  echo "ERROR: ddsketchcol binary not found at ${DDSKETCHCOL}" >&2
  echo "       Run: ${ROOT}/build_ddsketchcol.sh" >&2
  exit 1
fi

# ── Step 2: Start the controller ──────────────────────────────────────────────
# Guard against stale processes from a previous run.
for port in 8080 4320 4317 8889; do
  if fuser "$port/tcp" > /dev/null 2>&1; then
    echo "    Killing stale process on port $port..."
    fuser -k "$port/tcp" 2>/dev/null || true
    sleep 0.5
  fi
done

echo "==> [Step 2] Starting controller (API :8080, OpAMP :4320)..."
CONTROLLER_ADDR="0.0.0.0:8080" \
CONTROLLER_OPAMP_ADDR="0.0.0.0:4320" \
CONTROLLER_OPAMP_ENDPOINT="$CONTROLLER_OPAMP" \
"$CONTROLLER_BIN" > "${OUTPUT_DIR}/controller.log" 2>&1 &
CONTROLLER_PID=$!

# Wait for the controller to be ready.
for i in $(seq 1 20); do
  if curl -sf "${CONTROLLER_API}/api/v1/agents" > /dev/null 2>&1; then
    echo "    Controller ready (pid ${CONTROLLER_PID})"
    break
  fi
  sleep 0.5
  if [[ $i -eq 20 ]]; then
    echo "ERROR: controller did not start in time. Log:" >&2
    cat "${OUTPUT_DIR}/controller.log" >&2
    exit 1
  fi
done
echo ""

# ── --all-sketches mode: plan + YAML check for every sketch type ──────────────
# Runs steps 3/4/4a/4b for all five sketch types without starting a collector
# or running e2esdkbench.  Use this to verify the controller assigns the right
# processor key for each aggregation type without needing all collector binaries.
if [[ "$ALL_SKETCHES" == true ]]; then
  ALL_PASS=true
  for ST in ddsketch kll hll countsketch countminsketch; do
    AGG=$(sketch_to_aggregation "$ST")
    OVR=$(sketch_to_override_json "$ST")
    PKEY=$(sketch_to_processor_key "$ST")
    MN="${ST}_metric"
    echo "==> [all-sketches] sketch=${ST}  agg=${AGG}  expected_key=${PKEY}"

    RESP=$(curl -sf -X POST "${CONTROLLER_API}/api/v1/plan" \
      -H "Content-Type: application/json" \
      -d "{
        \"metric_name\":  \"${MN}\",
        \"aggregations\": [\"${AGG}\"],
        \"time_window\":  \"5m\",
        \"accuracy_sla\": 0.01,
        \"repeat_every\": \"1m\",
        \"latency_sla\":  \"10m\",
        ${OVR}
        \"workload\": {
          \"series_count\": 100, \"samples_per_sec_per_series\": 20,
          \"bytes_per_raw_sample\": 100, \"data_distribution\": \"zipf\",
          \"memory_budget_bytes\": null
        }
      }")

    CHOSEN=$(echo "$RESP" | jq -r '.sketch_type // empty' 2>/dev/null || true)
    if [[ "$CHOSEN" != "$ST" ]]; then
      echo "    [FAIL] planner returned sketch_type='${CHOSEN}', expected '${ST}'"
      ALL_PASS=false; continue
    fi

    YAML=$(curl -sf "${CONTROLLER_API}/api/v1/config/${MN}")
    YAML_FILE="${OUTPUT_DIR}/collector-config-${ST}.yaml"
    echo "$YAML" > "$YAML_FILE"

    if ! grep -q "${PKEY}:" "$YAML_FILE"; then
      echo "    [FAIL] YAML missing processor key '${PKEY}:'"
      ALL_PASS=false; continue
    fi
    if ! grep -q "- ${PKEY}" "$YAML_FILE"; then
      echo "    [FAIL] pipeline processor list missing '- ${PKEY}'"
      ALL_PASS=false; continue
    fi
    echo "    [OK]   processor key '${PKEY}:' present and pipeline references it"
  done

  echo ""
  if [[ "$ALL_PASS" == true ]]; then
    echo "==> [PASS] all-sketches plan+YAML check passed for all 5 sketch types."
  else
    echo "==> [FAIL] one or more sketch types failed the plan+YAML check." >&2
    exit 1
  fi
  exit 0
fi

# ── Step 3: Submit a plan with WorkloadCharacteristics ───────────────────────
AGGREGATION=$(sketch_to_aggregation "$SKETCH")
SKETCH_OVERRIDE_JSON=$(sketch_to_override_json "$SKETCH")
echo "==> [Step 3] Submitting plan (sketch=${SKETCH}, agg=${AGGREGATION}, series=${SERIES}, rate=${SAMPLES_PER_SEC}Hz, dist=${DISTRIBUTION})..."
PLAN_RESP=$(curl -sf -X POST "${CONTROLLER_API}/api/v1/plan" \
  -H "Content-Type: application/json" \
  -d "{
    \"metric_name\":    \"${METRIC_NAME}\",
    \"aggregations\":   [\"${AGGREGATION}\"],
    \"time_window\":    \"5m\",
    \"accuracy_sla\":   0.01,
    \"repeat_every\":   \"1m\",
    \"latency_sla\":    \"10m\",
    ${SKETCH_OVERRIDE_JSON}
    \"workload\": {
      \"series_count\":               ${SERIES},
      \"samples_per_sec_per_series\": ${SAMPLES_PER_SEC},
      \"bytes_per_raw_sample\":       100,
      \"data_distribution\":          \"${DISTRIBUTION}\",
      \"memory_budget_bytes\":        ${MEMORY_BUDGET_JSON}
    }
  }")
echo "    Plan response:"
echo "$PLAN_RESP" | jq . 2>/dev/null || echo "$PLAN_RESP"

CHOSEN_SKETCH=$(echo "$PLAN_RESP" | jq -r '.sketch_type // empty' 2>/dev/null \
  || echo "$PLAN_RESP" | grep -o '"sketch_type":"[^"]*"' | cut -d'"' -f4)
echo "    Chosen sketch: ${CHOSEN_SKETCH}"
if [[ "$CHOSEN_SKETCH" != "$SKETCH" ]]; then
  echo "ERROR: expected planner to choose '${SKETCH}' but got '${CHOSEN_SKETCH}'." >&2
  exit 1
fi

# ── Step 3a: Verify delta_decision and transmission_costs are present ─────────
echo ""
echo "==> [Step 3a] Verifying delta_decision in plan response..."
DELTA_MODE=$(echo "$PLAN_RESP" | jq -r '.delta_decision.mode // empty' 2>/dev/null || true)
FILL_RATE=$(echo "$PLAN_RESP"  | jq -r '.transmission_costs.estimated_fill_rate // empty' 2>/dev/null || true)
FULL_BW=$(echo "$PLAN_RESP"    | jq -r '.transmission_costs.sketch_full_bytes_per_sec // empty' 2>/dev/null || true)
DELTA_BW=$(echo "$PLAN_RESP"   | jq -r '.transmission_costs.sketch_delta_bytes_per_sec // empty' 2>/dev/null || true)
RAW_BW=$(echo "$PLAN_RESP"     | jq -r '.transmission_costs.raw_bytes_per_sec // empty' 2>/dev/null || true)

if [[ -z "$DELTA_MODE" ]]; then
  echo "ERROR: plan response missing delta_decision field." >&2
  echo "       Expected fields: delta_decision, transmission_costs" >&2
  exit 1
fi

echo "    delta_decision.mode         = ${DELTA_MODE}"
echo "    estimated_fill_rate         = ${FILL_RATE}"
echo "    raw_bytes_per_sec           = ${RAW_BW}"
echo "    sketch_full_bytes_per_sec   = ${FULL_BW}"
echo "    sketch_delta_bytes_per_sec  = ${DELTA_BW}"

# Save delta mode for YAML check below.
EXPECTS_DELTA=false
if [[ "$DELTA_MODE" == "use_delta" ]]; then
  EXPECTS_DELTA=true
  echo "    [OK] Controller decided: use_delta"
else
  echo "    [OK] Controller decided: ${DELTA_MODE} (full sketch or raw)"
fi
echo ""

# ── Step 4: Fetch and verify the generated collector YAML ─────────────────────
echo "==> [Step 4] Fetching collector YAML from controller..."
CONFIG_URL="${CONTROLLER_API}/api/v1/config/${METRIC_NAME}"
CONFIG_YAML=$(curl -sf "$CONFIG_URL")
echo "$CONFIG_YAML" > "${OUTPUT_DIR}/collector-config.yaml"
echo "    Config written to ${OUTPUT_DIR}/collector-config.yaml"

# Sanity-check the YAML has the essential sections.
for section in "receivers:" "processors:" "exporters:" "service:"; do
  if ! grep -q "$section" "${OUTPUT_DIR}/collector-config.yaml"; then
    echo "ERROR: generated YAML missing section: $section" >&2
    cat "${OUTPUT_DIR}/collector-config.yaml" >&2
    exit 1
  fi
done
echo "    YAML looks valid (has receivers, processors, exporters, service)"

# ── Step 4a: Verify delta_transmission in YAML matches the plan decision ──────
echo ""
echo "==> [Step 4a] Checking delta_transmission field in generated YAML..."
YAML_HAS_DELTA=false
if grep -q "delta_transmission: true" "${OUTPUT_DIR}/collector-config.yaml"; then
  YAML_HAS_DELTA=true
fi

if [[ "$EXPECTS_DELTA" == true && "$YAML_HAS_DELTA" == true ]]; then
  YAML_THRESHOLD=$(grep "delta_threshold" "${OUTPUT_DIR}/collector-config.yaml" | awk '{print $2}' || echo "?")
  echo "    [OK] delta_transmission: true  (threshold: ${YAML_THRESHOLD})"
elif [[ "$EXPECTS_DELTA" == false && "$YAML_HAS_DELTA" == false ]]; then
  echo "    [OK] delta_transmission absent (full-sketch or raw mode)"
elif [[ "$EXPECTS_DELTA" == true && "$YAML_HAS_DELTA" == false ]]; then
  echo "ERROR: plan decided use_delta but YAML does not contain delta_transmission: true" >&2
  cat "${OUTPUT_DIR}/collector-config.yaml" >&2
  exit 1
else
  echo "    [WARN] YAML has delta_transmission: true but plan decided ${DELTA_MODE}"
fi
echo ""

# ── Step 4b: Verify YAML processor key and pipeline reference ────────────────
EXPECTED_PROC_KEY=$(sketch_to_processor_key "$CHOSEN_SKETCH")
echo "==> [Step 4b] Checking processor key '${EXPECTED_PROC_KEY}:' and pipeline reference in YAML..."
if ! grep -q "${EXPECTED_PROC_KEY}:" "${OUTPUT_DIR}/collector-config.yaml"; then
  echo "ERROR: YAML missing processor key '${EXPECTED_PROC_KEY}:'" >&2
  cat "${OUTPUT_DIR}/collector-config.yaml" >&2
  exit 1
fi
echo "    [OK] Processor block '${EXPECTED_PROC_KEY}:' present"

if ! grep -q "- ${EXPECTED_PROC_KEY}" "${OUTPUT_DIR}/collector-config.yaml"; then
  echo "ERROR: pipeline processor list missing '- ${EXPECTED_PROC_KEY}'" >&2
  cat "${OUTPUT_DIR}/collector-config.yaml" >&2
  exit 1
fi
echo "    [OK] Pipeline processor list references '- ${EXPECTED_PROC_KEY}'"
echo ""

# ── Step 5: Start the collector with the HTTP config provider ─────────────────
echo "==> [Step 5] Starting ddsketchcol (OTLP :4317, Prom :8889)..."
"$DDSKETCHCOL" \
  --config="http://localhost:8080/api/v1/config/${METRIC_NAME}" \
  > "${OUTPUT_DIR}/collector.log" 2>&1 &
COLLECTOR_PID=$!

# Wait for the OTLP gRPC port to be open.
for i in $(seq 1 30); do
  if curl -sf "http://localhost:8889/metrics" > /dev/null 2>&1; then
    echo "    Collector ready (pid ${COLLECTOR_PID})"
    break
  fi
  sleep 1
  if [[ $i -eq 30 ]]; then
    echo "ERROR: collector did not start in time. Log:" >&2
    tail -30 "${OUTPUT_DIR}/collector.log" >&2
    exit 1
  fi
done
echo ""

# ── Step 6: Run e2esdkbench ───────────────────────────────────────────────────
echo "==> [Step 6] Running e2esdkbench (sketch=${SKETCH}, series=${SERIES}, rate=${SAMPLES_PER_SEC}Hz, duration=${BENCH_DURATION})..."
pushd "$E2EBENCH_DIR" > /dev/null
go run ./cmd/e2esdkbench \
  --sketch-type="$SKETCH" \
  --endpoint="$COLLECTOR_OTLP" \
  --series="$SERIES" \
  --samples-per-sec-per-series="$SAMPLES_PER_SEC" \
  --duration="$BENCH_DURATION" \
  --output-dir="$OUTPUT_DIR"
popd > /dev/null
echo ""

# ── Step 7: Verify metrics in Prometheus output ───────────────────────────────
# Give the collector a moment to flush the last window/batch from the benchmark.
sleep 3

echo "==> [Step 7] Verifying metrics..."
# Internal telemetry on :8888 — accepted data points counter.
INTERNAL_OUT=$(curl -sf "http://localhost:8888/metrics" 2>/dev/null || true)
PIPELINE_OUT=$(curl -sf "$COLLECTOR_PROM"              2>/dev/null || true)

# Check collector log for evidence of data processing.
COLLECTOR_LOG="${OUTPUT_DIR}/collector.log"
if grep -q "metric_points" "$COLLECTOR_LOG" 2>/dev/null || \
   grep -qi "accepted\|processed\|flushed\|sketch" "$COLLECTOR_LOG" 2>/dev/null; then
  echo "    [OK] Collector log shows data processing activity"
else
  echo "    [WARN] No processing evidence in collector log"
fi

# Internal telemetry on :8888 — only present if service.telemetry is configured.
if echo "$INTERNAL_OUT" | grep -q "otelcol_processor_accepted_metric_points"; then
  ACCEPTED=$(echo "$INTERNAL_OUT" \
    | grep "otelcol_processor_accepted_metric_points" \
    | grep -v "^#" | awk '{print $2}' | head -1)
  echo "    [OK] otelcol_processor_accepted_metric_points = ${ACCEPTED}"
fi

# Pipeline output on :8889 — present only when transmit_sketch=false.
# When transmit_sketch=true (default), output is binary sketch payloads.
if [[ -n "$PIPELINE_OUT" ]]; then
  echo "    [OK] Prometheus endpoint :8889 is reachable"
else
  echo "    [WARN] Prometheus endpoint :8889 not reachable"
fi

echo ""
echo "==> Logs saved to: ${OUTPUT_DIR}/"
echo "    controller.log        — controller stdout/stderr"
echo "    collector.log         — ddsketchcol stdout/stderr"
echo "    collector-config.yaml — config fetched from controller"
echo "    ${SKETCH}_*_summary.json — e2esdkbench bandwidth / CPU / memory summary"
echo ""
echo "==> Delta decision summary:"
echo "    mode            = ${DELTA_MODE}"
echo "    fill_rate       = ${FILL_RATE}"
echo "    raw_bw          = ${RAW_BW} B/s"
echo "    sketch_full_bw  = ${FULL_BW} B/s"
echo "    sketch_delta_bw = ${DELTA_BW} B/s"
echo ""
echo "==> [PASS] End-to-end test completed."
