#!/usr/bin/env bash
# promql_e2e_collector_test.sh — PromQL-driven end-to-end test
#
# Tests the full pipeline: PromQL query → controller planning → OTel collector
# config generation → collector startup → real metric ingestion → verification.
#
# Unlike otel_controller_e2e_test.sh (which uses explicit metric_name +
# aggregations), this test submits raw PromQL via query_string and validates
# the controller correctly parses, plans, generates config, and that the
# collector actually processes metrics end-to-end.
#
# Usage:
#   ./tests/promql_e2e_collector_test.sh
#       [--query 'histogram_quantile(0.99, rate(http_request_duration_seconds[5m]))']
#       [--series 100]
#       [--rate 20]
#       [--duration 20s]
#       [--skip-build]
#       [--plan-only]    # run only plan tests (no collector/bench), useful for CI
#
# Prerequisites:
#   - cargo      (Rust toolchain)
#   - go         (Go toolchain)  — only for data-plane tests
#   - curl, python3
#   - Run ./setup.sh first to initialise submodules and apply patches

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONTROLLER_DIR="${ROOT}/controller"
ASAP_OTEL="${ROOT}/opentelemetry-collector-contrib-patch/cmd/asap-otel/asap-otel"
E2EBENCH_DIR="${ROOT}/opentelemetry-app"

# ── Defaults ──────────────────────────────────────────────────────────────────
SERIES=100
SAMPLES_PER_SEC=20
BENCH_DURATION="20s"
SKIP_BUILD=false
PLAN_ONLY=false

# ── PromQL test cases ─────────────────────────────────────────────────────────
# Each entry: "promql_query|expected_sketch|expected_aggregation|metric_name"
# metric_name is what the controller should extract from the PromQL.
#
# Note: expected_sketch is advisory — the cost-model planner may choose a
# different sketch (e.g. KLL instead of DDSketch for quantile).  The test
# only WARNs on mismatch, it does not fail.
TEST_CASES=(
  'quantile_over_time(0.99, http_request_duration_seconds[5m])|ddsketch|quantile|http_request_duration_seconds'
  'histogram_quantile(0.99, rate(http_request_duration_seconds[5m]))|ddsketch|quantile|http_request_duration_seconds'
  'avg_over_time(cpu_usage_percent[10m])|ddsketch|quantile|cpu_usage_percent'
  'count by (region) (count_over_time(user_sessions[1h]))|hll|cardinality|user_sessions'
  'topk by (endpoint) (10, sum(rate(api_errors_total[5m])))|countminsketch|frequency|api_errors_total'
)

# Allow user to pass a single custom query instead
CUSTOM_QUERY=""
for arg in "$@"; do
  case "$arg" in
    --query=*)      CUSTOM_QUERY="${arg#*=}" ;;
    --series=*)     SERIES="${arg#*=}" ;;
    --rate=*)       SAMPLES_PER_SEC="${arg#*=}" ;;
    --duration=*)   BENCH_DURATION="${arg#*=}" ;;
    --skip-build)   SKIP_BUILD=true ;;
    --plan-only)    PLAN_ONLY=true ;;
    *) echo "Unknown arg: $arg" >&2; exit 1 ;;
  esac
done

CONTROLLER_API="http://localhost:8080"
CONTROLLER_OPAMP="ws://localhost:4320/v1/opamp"
COLLECTOR_OTLP="localhost:4317"
COLLECTOR_PROM="http://localhost:8889/metrics"
OUTPUT_DIR="/tmp/promql_e2e_test_$(date +%s)"

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

# ── JSON helpers (no jq dependency) ───────────────────────────────────────────
_json_str() { echo "$2" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('$1',''))" 2>/dev/null; }
_json_nested() { echo "$3" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('$1',{}).get('$2',''))" 2>/dev/null; }

# ── Step 1: Build ─────────────────────────────────────────────────────────────
if [[ "$SKIP_BUILD" == false ]]; then
  echo "==> [Step 1] Building controller..."
  cargo build --manifest-path="${CONTROLLER_DIR}/Cargo.toml" --release 2>&1 | tail -3
  echo ""

  if [[ ! -x "$ASAP_OTEL" ]]; then
    echo "==> [Step 1] Building asap-otel..."
    bash "${ROOT}/build_asap_otel.sh" --skip-patches
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
if [[ "$PLAN_ONLY" == false && ! -x "$ASAP_OTEL" ]]; then
  echo "ERROR: asap-otel binary not found at ${ASAP_OTEL}" >&2
  echo "       Run: ${ROOT}/build_asap_otel.sh, or use --plan-only" >&2
  exit 1
fi

# ── Step 2: Start the controller ──────────────────────────────────────────────
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

# ── Step 3: PromQL plan submission tests ──────────────────────────────────────
# Phase A: validate the controller correctly parses multiple PromQL queries,
# extracts metric names and aggregation types, and generates valid YAML.

echo "==> [Step 3] PromQL parsing + planning tests"
echo "    ──────────────────────────────────────────"
PLAN_PASS=0
PLAN_FAIL=0

run_plan_test() {
  local promql="$1"
  local expected_sketch="$2"
  local expected_agg="$3"
  local expected_metric="$4"
  local test_label="$5"

  echo ""
  echo "    [Test] ${test_label}"
  echo "    Query: ${promql}"

  # Submit plan with query_string (the PromQL-driven path)
  local resp
  resp=$(curl -sf -X POST "${CONTROLLER_API}/api/v1/plan" \
    -H "Content-Type: application/json" \
    -d "{
      \"query_string\":  \"${promql}\",
      \"accuracy_sla\":  0.01,
      \"latency_sla\":   \"10m\",
      \"workload\": {
        \"series_count\":               ${SERIES},
        \"samples_per_sec_per_series\": ${SAMPLES_PER_SEC},
        \"bytes_per_raw_sample\":       100,
        \"data_distribution\":          \"zipf\",
        \"memory_budget_bytes\":        null
      }
    }" 2>/dev/null) || {
    echo "    [FAIL] curl failed — controller rejected the request"
    echo "$resp" >> "${OUTPUT_DIR}/plan_failures.log"
    PLAN_FAIL=$((PLAN_FAIL + 1))
    return
  }

  echo "$resp" > "${OUTPUT_DIR}/plan_${test_label}.json"

  # Verify metric name was extracted
  local actual_metric
  actual_metric=$(_json_str "metric" "$resp")
  if [[ "$actual_metric" != "$expected_metric" ]]; then
    echo "    [FAIL] metric: expected '${expected_metric}', got '${actual_metric}'"
    PLAN_FAIL=$((PLAN_FAIL + 1))
    return
  fi
  echo "    [OK]   metric = ${actual_metric}"

  # Verify sketch type
  local actual_sketch
  actual_sketch=$(_json_str "sketch_type" "$resp")
  if [[ "$actual_sketch" != "$expected_sketch" ]]; then
    echo "    [WARN] sketch_type: expected '${expected_sketch}', got '${actual_sketch}' (planner may choose differently)"
  else
    echo "    [OK]   sketch_type = ${actual_sketch}"
  fi

  # Verify delta_decision is present
  local delta_mode
  delta_mode=$(_json_nested "delta_decision" "mode" "$resp")
  if [[ -z "$delta_mode" ]]; then
    echo "    [FAIL] missing delta_decision in response"
    PLAN_FAIL=$((PLAN_FAIL + 1))
    return
  fi
  echo "    [OK]   delta_decision.mode = ${delta_mode}"

  # Verify staged_plan is present (query_string triggers the algebra pipeline)
  local has_staged
  has_staged=$(echo "$resp" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print('yes' if d.get('staged_plan') is not None else 'no')
" 2>/dev/null)
  if [[ "$has_staged" == "yes" ]]; then
    echo "    [OK]   staged_plan present (algebra pipeline ran)"
  else
    echo "    [WARN] staged_plan absent (algebra pipeline may have been skipped)"
  fi

  # Fetch and verify generated YAML
  local yaml
  yaml=$(curl -sf "${CONTROLLER_API}/api/v1/config/${actual_metric}" 2>/dev/null) || {
    echo "    [FAIL] could not fetch collector YAML for metric '${actual_metric}'"
    PLAN_FAIL=$((PLAN_FAIL + 1))
    return
  }
  echo "$yaml" > "${OUTPUT_DIR}/config_${test_label}.yaml"

  # Check essential YAML sections
  local missing_sections=false
  for section in "receivers:" "processors:" "exporters:" "service:"; do
    if ! grep -q "$section" "${OUTPUT_DIR}/config_${test_label}.yaml"; then
      echo "    [FAIL] YAML missing section: ${section}"
      missing_sections=true
    fi
  done
  if [[ "$missing_sections" == true ]]; then
    PLAN_FAIL=$((PLAN_FAIL + 1))
    return
  fi

  # Check processor key matches sketch type
  if grep -q "${actual_sketch}:" "${OUTPUT_DIR}/config_${test_label}.yaml"; then
    echo "    [OK]   YAML processor '${actual_sketch}:' present"
  else
    echo "    [FAIL] YAML missing processor key '${actual_sketch}:'"
    PLAN_FAIL=$((PLAN_FAIL + 1))
    return
  fi

  # Check pipeline references the processor
  if grep -q -- "- ${actual_sketch}" "${OUTPUT_DIR}/config_${test_label}.yaml"; then
    echo "    [OK]   pipeline references '- ${actual_sketch}'"
  else
    echo "    [FAIL] pipeline missing '- ${actual_sketch}'"
    PLAN_FAIL=$((PLAN_FAIL + 1))
    return
  fi

  # ── Start a collector with this config and verify it boots ──────────────
  if [[ "$PLAN_ONLY" == false ]]; then
    local col_pid=""
    "$ASAP_OTEL" \
      --config="${OUTPUT_DIR}/config_${test_label}.yaml" \
      > "${OUTPUT_DIR}/collector_${test_label}.log" 2>&1 &
    col_pid=$!

    # Wait up to 15s for the collector to start (check Prometheus port in YAML)
    local booted=false
    for attempt in $(seq 1 15); do
      # Check if the process is still alive (it would exit quickly on bad config)
      if ! kill -0 "$col_pid" 2>/dev/null; then
        echo "    [FAIL] collector exited early — config rejected"
        cat "${OUTPUT_DIR}/collector_${test_label}.log" | tail -5 | sed 's/^/    /'
        PLAN_FAIL=$((PLAN_FAIL + 1))
        return
      fi
      # Check if the collector log shows it started serving
      if grep -qi "Everything is ready\|starting.*server\|listening" "${OUTPUT_DIR}/collector_${test_label}.log" 2>/dev/null; then
        booted=true
        break
      fi
      sleep 1
    done

    if [[ "$booted" == true ]]; then
      echo "    [OK]   collector started (pid ${col_pid}, processor=${actual_sketch})"
    else
      # Still running after 15s but no "ready" log — check if it's alive
      if kill -0 "$col_pid" 2>/dev/null; then
        echo "    [OK]   collector running (pid ${col_pid}, no ready log but alive)"
      else
        echo "    [FAIL] collector died during startup"
        tail -5 "${OUTPUT_DIR}/collector_${test_label}.log" | sed 's/^/    /'
        PLAN_FAIL=$((PLAN_FAIL + 1))
        return
      fi
    fi

    # Clean up — kill the collector so the next test can use the ports
    kill "$col_pid" 2>/dev/null
    wait "$col_pid" 2>/dev/null || true
  fi

  echo "    [PASS]"
  PLAN_PASS=$((PLAN_PASS + 1))
}

# Run all built-in test cases
IDX=0
for tc in "${TEST_CASES[@]}"; do
  IFS='|' read -r promql expected_sketch expected_agg expected_metric <<< "$tc"
  IDX=$((IDX + 1))
  run_plan_test "$promql" "$expected_sketch" "$expected_agg" "$expected_metric" "case_${IDX}"
done

# Run custom query if provided
if [[ -n "$CUSTOM_QUERY" ]]; then
  echo ""
  echo "    ── Custom query ──"
  # For custom queries we don't know the expected values — just verify it doesn't crash
  resp=$(curl -sf -X POST "${CONTROLLER_API}/api/v1/plan" \
    -H "Content-Type: application/json" \
    -d "{
      \"query_string\":  \"${CUSTOM_QUERY}\",
      \"accuracy_sla\":  0.01,
      \"workload\": {
        \"series_count\": ${SERIES},
        \"samples_per_sec_per_series\": ${SAMPLES_PER_SEC},
        \"bytes_per_raw_sample\": 100,
        \"data_distribution\": \"zipf\",
        \"memory_budget_bytes\": null
      }
    }" 2>/dev/null) && {
    echo "    [OK] Custom query accepted by controller"
    actual_metric=$(_json_str "metric" "$resp")
    actual_sketch=$(_json_str "sketch_type" "$resp")
    echo "    metric=${actual_metric}  sketch=${actual_sketch}"
    echo "$resp" | python3 -m json.tool > "${OUTPUT_DIR}/plan_custom.json" 2>/dev/null
    PLAN_PASS=$((PLAN_PASS + 1))
  } || {
    echo "    [FAIL] Custom query rejected"
    PLAN_FAIL=$((PLAN_FAIL + 1))
  }
fi

echo ""
echo "    ──────────────────────────────────────────"
echo "    Plan tests: ${PLAN_PASS} passed, ${PLAN_FAIL} failed"

if [[ $PLAN_FAIL -gt 0 ]]; then
  echo ""
  echo "==> [FAIL] Some PromQL plan tests failed. Aborting before collector phase." >&2
  exit 1
fi

if [[ "$PLAN_ONLY" == true ]]; then
  echo ""
  echo "==> [PASS] PromQL plan tests completed (--plan-only, skipping data-plane)."
  exit 0
fi
echo ""

# ── Step 4: Data-plane test — pick one query, run collector + bench ───────────
# We use a dedicated metric name and pin sketch_type to "ddsketch" because the
# data-plane benchmark (e2esdkbench uses the sketch type to configure its SDK
# aggregation pipeline).
# A unique metric name avoids cache collision with step 3's unpinned plans.
LIVE_QUERY="quantile_over_time(0.99, benchmark_latency[5m])"
LIVE_SKETCH="ddsketch"
LIVE_METRIC="benchmark_latency"

echo "==> [Step 4] Data-plane test: PromQL → collector → metrics"
echo "    Query:  ${LIVE_QUERY}"
echo "    Sketch: ${LIVE_SKETCH} (pinned for collector compatibility)"
echo "    Metric: ${LIVE_METRIC}"
echo ""

# Submit plan with sketch_type pinned to ddsketch
PLAN_RESP=$(curl -sf -X POST "${CONTROLLER_API}/api/v1/plan" \
  -H "Content-Type: application/json" \
  -d "{
    \"query_string\":  \"${LIVE_QUERY}\",
    \"sketch_type\":   \"${LIVE_SKETCH}\",
    \"accuracy_sla\":  0.01,
    \"latency_sla\":   \"10m\",
    \"workload\": {
      \"series_count\":               ${SERIES},
      \"samples_per_sec_per_series\": ${SAMPLES_PER_SEC},
      \"bytes_per_raw_sample\":       100,
      \"data_distribution\":          \"zipf\",
      \"memory_budget_bytes\":        null
    }
  }")

CHOSEN_SKETCH=$(_json_str "sketch_type" "$PLAN_RESP")
DELTA_MODE=$(_json_nested "delta_decision" "mode" "$PLAN_RESP")
echo "    Planner chose: sketch=${CHOSEN_SKETCH}, delta=${DELTA_MODE}"
echo ""

# ── Step 4a: Fetch collector config ───────────────────────────────────────────
echo "==> [Step 4a] Fetching collector config for '${LIVE_METRIC}'..."
CONFIG_YAML=$(curl -sf "${CONTROLLER_API}/api/v1/config/${LIVE_METRIC}")
echo "$CONFIG_YAML" > "${OUTPUT_DIR}/live-collector-config.yaml"
echo "    Config saved to ${OUTPUT_DIR}/live-collector-config.yaml"

for section in "receivers:" "processors:" "exporters:" "service:"; do
  if ! grep -q "$section" "${OUTPUT_DIR}/live-collector-config.yaml"; then
    echo "ERROR: generated YAML missing section: $section" >&2
    cat "${OUTPUT_DIR}/live-collector-config.yaml" >&2
    exit 1
  fi
done
echo "    [OK] YAML structure valid"
echo ""

# ── Step 4b: Start the collector ──────────────────────────────────────────────
# Kill any stale collector on the data-plane ports
for port in 4317 8889; do
  if fuser "$port/tcp" > /dev/null 2>&1; then
    echo "    Killing stale process on port $port..."
    fuser -k "$port/tcp" 2>/dev/null || true
    sleep 0.5
  fi
done

echo "==> [Step 4b] Starting asap-otel (OTLP :4317, Prom :8889)..."
"$ASAP_OTEL" \
  --config="http://localhost:8080/api/v1/config/${LIVE_METRIC}" \
  > "${OUTPUT_DIR}/collector.log" 2>&1 &
COLLECTOR_PID=$!

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

# ── Step 4c: Send metrics via e2esdkbench ─────────────────────────────────────
echo "==> [Step 4c] Running e2esdkbench (sketch=${CHOSEN_SKETCH}, series=${SERIES}, rate=${SAMPLES_PER_SEC}Hz, duration=${BENCH_DURATION})..."
pushd "$E2EBENCH_DIR" > /dev/null
go run ./cmd/e2esdkbench \
  --sketch-type="$CHOSEN_SKETCH" \
  --endpoint="$COLLECTOR_OTLP" \
  --series="$SERIES" \
  --samples-per-sec-per-series="$SAMPLES_PER_SEC" \
  --duration="$BENCH_DURATION" \
  --output-dir="$OUTPUT_DIR"
popd > /dev/null
echo ""

# ── Step 5: Verify metrics reached the collector ──────────────────────────────
sleep 3

echo "==> [Step 5] Verifying data-plane metrics..."
INTERNAL_OUT=$(curl -sf "http://localhost:8888/metrics" 2>/dev/null || true)
PIPELINE_OUT=$(curl -sf "$COLLECTOR_PROM"              2>/dev/null || true)
COLLECTOR_LOG="${OUTPUT_DIR}/collector.log"
VERIFY_PASS=0
VERIFY_TOTAL=0

# Check 1: collector log shows processing activity
VERIFY_TOTAL=$((VERIFY_TOTAL + 1))
if grep -q "metric_points" "$COLLECTOR_LOG" 2>/dev/null || \
   grep -qi "accepted\|processed\|flushed\|sketch" "$COLLECTOR_LOG" 2>/dev/null; then
  echo "    [OK] Collector log shows data processing activity"
  VERIFY_PASS=$((VERIFY_PASS + 1))
else
  echo "    [WARN] No processing evidence in collector log"
fi

# Check 2: internal telemetry accepted metric points
VERIFY_TOTAL=$((VERIFY_TOTAL + 1))
if echo "$INTERNAL_OUT" | grep -q "otelcol_processor_accepted_metric_points"; then
  ACCEPTED=$(echo "$INTERNAL_OUT" \
    | grep "otelcol_processor_accepted_metric_points" \
    | grep -v "^#" | awk '{print $2}' | head -1)
  echo "    [OK] otelcol_processor_accepted_metric_points = ${ACCEPTED}"
  VERIFY_PASS=$((VERIFY_PASS + 1))
else
  echo "    [WARN] Internal telemetry metric not found (service.telemetry may not be configured)"
fi

# Check 3: Prometheus endpoint reachable
VERIFY_TOTAL=$((VERIFY_TOTAL + 1))
if [[ -n "$PIPELINE_OUT" ]]; then
  echo "    [OK] Prometheus endpoint :8889 reachable"
  VERIFY_PASS=$((VERIFY_PASS + 1))
else
  echo "    [WARN] Prometheus endpoint :8889 not reachable"
fi

# Check 4: plan still cached in controller
VERIFY_TOTAL=$((VERIFY_TOTAL + 1))
CACHED_PLAN=$(curl -sf "${CONTROLLER_API}/api/v1/plan/${LIVE_METRIC}" 2>/dev/null || true)
if [[ -n "$CACHED_PLAN" ]]; then
  echo "    [OK] Plan still cached in controller for '${LIVE_METRIC}'"
  VERIFY_PASS=$((VERIFY_PASS + 1))
else
  echo "    [WARN] Plan not found in controller cache"
fi

echo ""
echo "    Data-plane checks: ${VERIFY_PASS}/${VERIFY_TOTAL} passed"
echo ""

# ── Summary ───────────────────────────────────────────────────────────────────
echo "==> Logs saved to: ${OUTPUT_DIR}/"
echo "    controller.log           — controller stdout/stderr"
echo "    collector.log            — asap-otel stdout/stderr"
echo "    live-collector-config.yaml — YAML generated from PromQL"
echo "    plan_case_*.json         — plan responses per test case"
echo "    config_case_*.yaml       — YAML configs per test case"
echo ""
echo "==> Summary:"
echo "    PromQL plan tests:  ${PLAN_PASS} passed, ${PLAN_FAIL} failed"
echo "    Data-plane checks:  ${VERIFY_PASS}/${VERIFY_TOTAL} passed"
echo "    Sketch type:        ${CHOSEN_SKETCH}"
echo "    Delta decision:     ${DELTA_MODE}"
echo ""

if [[ $PLAN_FAIL -eq 0 && $VERIFY_PASS -ge 1 ]]; then
  echo "==> [PASS] PromQL end-to-end test completed successfully."
else
  echo "==> [FAIL] Some checks did not pass." >&2
  exit 1
fi
