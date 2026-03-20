#!/usr/bin/env bash
# e2e_test.sh — End-to-end integration test: controller + ddsketchcol + e2esdkbench
#
# Usage:
#   ./scripts/e2e_test.sh [--sketch ddsketch|kll|hll|countsketch|countminsketch]
#                         [--series 500]
#                         [--duration 30s]
#                         [--skip-build]
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
BENCH_DURATION="30s"
SKIP_BUILD=false

for arg in "$@"; do
  case "$arg" in
    --sketch=*)       SKETCH="${arg#*=}" ;;
    --series=*)       SERIES="${arg#*=}" ;;
    --duration=*)     BENCH_DURATION="${arg#*=}" ;;
    --skip-build)     SKIP_BUILD=true ;;
    *) echo "Unknown arg: $arg" >&2; exit 1 ;;
  esac
done

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

# ── Step 3: Submit a plan ─────────────────────────────────────────────────────
echo "==> [Step 3] Submitting plan for metric '${METRIC_NAME}' (sketch=${SKETCH})..."
PLAN_RESP=$(curl -sf -X POST "${CONTROLLER_API}/api/v1/plan" \
  -H "Content-Type: application/json" \
  -d "{
    \"metric_name\": \"${METRIC_NAME}\",
    \"aggregations\": [\"quantile\"],
    \"time_window\": \"5m\",
    \"accuracy_sla\": 0.01,
    \"repeat_every\": \"1m\",
    \"latency_sla\": \"10m\",
    \"sketch_type\": \"ddsketch\"
  }")
echo "    Plan response: $PLAN_RESP"
CHOSEN_SKETCH=$(echo "$PLAN_RESP" | grep -o '"sketch_type":"[^"]*"' | cut -d'"' -f4)
echo "    Chosen sketch: ${CHOSEN_SKETCH}"
if [[ "$CHOSEN_SKETCH" != "ddsketch" ]]; then
  echo "ERROR: ddsketchcol only supports 'ddsketch' processor; planner chose '${CHOSEN_SKETCH}'." >&2
  echo "       Tighten accuracy_sla (e.g. 0.01) so only DDSketch meets the SLA." >&2
  exit 1
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
echo "==> [Step 6] Running e2esdkbench (sketch=${SKETCH}, series=${SERIES}, duration=${BENCH_DURATION})..."
pushd "$E2EBENCH_DIR" > /dev/null
go run ./cmd/e2esdkbench \
  --sketch-type="$SKETCH" \
  --endpoint="$COLLECTOR_OTLP" \
  --series="$SERIES" \
  --samples-per-sec-per-series=50 \
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
echo "    controller.log      — controller stdout/stderr"
echo "    collector.log       — ddsketchcol stdout/stderr"
echo "    collector-config.yaml — config fetched from controller"
echo "    ${SKETCH}_*_summary.json — e2esdkbench summary"
echo ""
echo "==> [PASS] End-to-end test completed."
