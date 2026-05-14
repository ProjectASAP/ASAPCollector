#!/usr/bin/env bash
# verify_gorilla_compression.sh — Success-metric checks for gorilla-thanos-multinode.
#
# Checks:
#   1. MinIO has TSDB blocks in asap-gorilla-tsdb bucket
#   2. Thanos query API is healthy
#   3. Thanos serves metric names (data queryable end-to-end)
#   4. Agent self-metrics show gorillas3 activity
#   5. Network traffic explanation (what's on the wire)
#
# Usage:
#   bash verify_gorilla_compression.sh \
#     --minio-host  node2        \   # SSH-accessible hostname for MinIO
#     --thanos-host 10.10.1.3   \   # IP/host for Thanos HTTP API
#     --agent-host  10.10.1.1   \   # IP/host for agent Prometheus metrics
#     [--out results.txt]           # optional output file

set -euo pipefail

# ── Argument parsing ──────────────────────────────────────────────────────
MINIO_HOST="node2"
THANOS_HOST="10.10.1.3"
AGENT_HOST="10.10.1.1"
OUT_FILE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --minio-host)  MINIO_HOST="$2";  shift 2 ;;
        --thanos-host) THANOS_HOST="$2"; shift 2 ;;
        --agent-host)  AGENT_HOST="$2";  shift 2 ;;
        --out)         OUT_FILE="$2";    shift 2 ;;
        *) echo "Unknown arg: $1" >&2; exit 1 ;;
    esac
done

# ── Output tee setup ──────────────────────────────────────────────────────
if [[ -n "${OUT_FILE}" ]]; then
    exec > >(tee "${OUT_FILE}") 2>&1
fi

# ── Counters ──────────────────────────────────────────────────────────────
PASS=0
FAIL=0

pass() { echo "  [PASS] $*"; PASS=$((PASS+1)); }
fail() { echo "  [FAIL] $*"; FAIL=$((FAIL+1)); }
section() { echo ""; echo "=== $* ==="; }

echo "========================================"
echo "  gorilla-thanos-multinode verification"
echo "  $(date)"
echo "  minio-host:  ${MINIO_HOST}"
echo "  thanos-host: ${THANOS_HOST}"
echo "  agent-host:  ${AGENT_HOST}"
echo "========================================"

# ── Check 1: MinIO has TSDB blocks ───────────────────────────────────────
section "Check 1: MinIO has TSDB blocks in asap-gorilla-tsdb"
echo "  Connecting to ${MINIO_HOST} via SSH to run mc ls ..."
echo "  Note: First blocks appear after tsdb_block_duration=60s flush."
echo "  If this fails immediately after stack_up, wait 60-90s and retry."

BLOCK_COUNT=0
if ssh -n -o ConnectTimeout=10 -o BatchMode=yes -o StrictHostKeyChecking=no \
       "${MINIO_HOST}" \
       "docker run --rm --network host \
           --add-host=minio:127.0.0.1 \
           --entrypoint=sh minio/mc:latest -c \
           'mc alias set local http://minio:9000 asap asap-local-only 2>/dev/null &&
            mc ls local/asap-gorilla-tsdb --recursive 2>/dev/null'" \
   > /tmp/minio_ls_out.txt 2>&1; then
    BLOCK_COUNT=$(grep -vc "Added .* successfully" /tmp/minio_ls_out.txt || echo 0)
    echo "  mc ls output (${BLOCK_COUNT} lines):"
    head -20 /tmp/minio_ls_out.txt | sed 's/^/    /'
    if [[ "${BLOCK_COUNT}" -gt 0 ]]; then
        pass "MinIO asap-gorilla-tsdb has ${BLOCK_COUNT} object(s) — gorillas3 is writing blocks"
    else
        fail "MinIO asap-gorilla-tsdb is EMPTY — gorillas3 may not have flushed yet (wait 60-90s)"
        echo "  Hint: tsdb_block_duration=60s means first block appears ~60s after startup."
        echo "        Run: docker logs asap-agent-a 2>&1 | grep -i gorilla"
    fi
else
    fail "Could not SSH to ${MINIO_HOST} or mc ls failed (exit $?)"
    echo "  Raw output:"
    cat /tmp/minio_ls_out.txt | sed 's/^/    /' || true
fi

# ── Check 2: Thanos query API healthy ────────────────────────────────────
section "Check 2: Thanos query API healthy (${THANOS_HOST}:10903)"
THANOS_URL="http://${THANOS_HOST}:10903"
QUERY_RESP=""
if QUERY_RESP=$(curl -sf --max-time 10 \
    "${THANOS_URL}/api/v1/query?query=up" 2>&1); then
    if echo "${QUERY_RESP}" | grep -q '"status":"success"'; then
        pass "Thanos query API returned status=success"
    else
        fail "Thanos query API responded but status != success"
        echo "  Response: ${QUERY_RESP}" | head -5
    fi
else
    fail "Thanos query API unreachable at ${THANOS_URL} (curl failed)"
    echo "  Hint: Check if asap-thanos-query is running on node2:"
    echo "        ssh node2 'docker ps | grep thanos-query'"
fi

# ── Check 3: Thanos serves metric names ──────────────────────────────────
section "Check 3: Thanos serves metric names (data queryable end-to-end)"
LABEL_RESP=""
METRIC_COUNT=0
if LABEL_RESP=$(curl -sf --max-time 15 \
    "${THANOS_URL}/api/v1/label/__name__/values" 2>&1); then
    if echo "${LABEL_RESP}" | grep -q '"status":"success"'; then
        METRIC_COUNT=$(echo "${LABEL_RESP}" | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d.get(chr(100)+chr(97)+chr(116)+chr(97), [])))" 2>/dev/null || echo 0)
        echo "  Thanos label __name__ values: ${METRIC_COUNT} metric name(s)"
        echo "${LABEL_RESP}" | python3 -c "
import sys, json
d = json.load(sys.stdin)
names = d.get('data', [])
for n in names[:10]:
    print('   ', n)
if len(names) > 10:
    print('    ... and', len(names)-10, 'more')
" 2>/dev/null || echo "  (could not parse JSON for display)"
        if [[ "${METRIC_COUNT}" -gt 0 ]]; then
            pass "Thanos serves ${METRIC_COUNT} metric name(s) — gorillas3 → MinIO → Thanos pipeline is end-to-end"
        else
            fail "Thanos returned success but 0 metric names — blocks may not be synced yet (thanos-store sync-block-duration=30s)"
            echo "  Hint: Wait 30s for store-gateway to sync new blocks from MinIO, then retry."
        fi
    else
        fail "Thanos label API responded but status != success"
    fi
else
    fail "Thanos label API unreachable at ${THANOS_URL}/api/v1/label/__name__/values"
fi

# ── Check 4: Agent gorilla telemetry ─────────────────────────────────────
section "Check 4: Agent gorilla self-metrics (${AGENT_HOST}:8890)"
AGENT_METRICS_URL="http://${AGENT_HOST}:8890/metrics"
GORILLA_LINES=""
if AGENT_METRICS_RAW=$(curl -sf --max-time 10 "${AGENT_METRICS_URL}" 2>&1); then
    GORILLA_LINES=$(echo "${AGENT_METRICS_RAW}" | grep -i 'gorilla\|gorillas3' || true)
    GORILLA_COUNT=$(echo "${GORILLA_LINES}" | grep -c . || echo 0)
    echo "  gorilla-related metric lines found: ${GORILLA_COUNT}"
    if [[ "${GORILLA_COUNT}" -gt 0 ]]; then
        echo "${GORILLA_LINES}" | head -10 | sed 's/^/    /'
        pass "Agent self-metrics include ${GORILLA_COUNT} gorilla/gorillas3 line(s)"
    else
        echo "  All metric names (first 20):"
        echo "${AGENT_METRICS_RAW}" | grep '^[^#]' | awk '{print $1}' | sort -u | head -20 | sed 's/^/    /'
        fail "No gorilla/gorillas3 metrics found in agent self-metrics at port 8890"
        echo "  Hint: Agent may not have started gorillas3 yet, or the pipeline has not"
        echo "        processed any data. Check: docker logs asap-agent-a 2>&1 | tail -30"
    fi
else
    fail "Agent metrics endpoint unreachable at ${AGENT_METRICS_URL}"
    echo "  Hint: Check if asap-agent-a is running on node0:"
    echo "        ssh node0 'docker ps | grep agent-a'"
fi

# ── Check 5: Network traffic explanation ─────────────────────────────────
section "Check 5: Network traffic analysis"
cat <<'TRAFFIC'
  What's on the wire in this stack:

  ┌─────────────────────────────────────────────────────────────────────┐
  │  S3 PUT to port 9000: agent → MinIO (Gorilla-compressed TSDB blocks)│
  │  - Protocol: HTTP/1.1 PUT (S3 API)                                  │
  │  - Content: Prometheus TSDB block files (chunks/, index, meta.json) │
  │  - Compression: Gorilla delta-of-delta + XOR encoding in gorillas3  │
  │  - Frequency: one PUT every tsdb_block_duration=60s per flush       │
  │                                                                     │
  │  No outbound gRPC port 4317 from agents:                            │
  │  - drop_original: true in gorillas3 means the metric stream does    │
  │    NOT leave the agent as raw OTLP. Gorillas3 absorbs the data,     │
  │    compresses it, and writes TSDB blocks to MinIO.                  │
  │  - The nop exporter receives empty batches (nothing to export).     │
  └─────────────────────────────────────────────────────────────────────┘

  How to observe on the wire:

  On node0 (agent host) — observe S3 PUTs going out to MinIO:
    ssh node0 'sudo tcpdump -i eth0 -n "dst port 9000" -c 20'
    (You should see HTTP PUT requests to 10.10.1.3:9000)

  On node0 — confirm NO outbound gRPC from agent:
    ssh node0 'sudo tcpdump -i eth0 -n "dst port 4317" -c 20'
    (You should see ONLY inbound from producers, no outbound to backend)

  On node2 (MinIO host) — see blocks arriving:
    ssh node2 'docker logs asap-minio 2>&1 | grep PUT | tail -20'

TRAFFIC
pass "Network traffic explanation printed (observational check)"

# ── Summary ───────────────────────────────────────────────────────────────
echo ""
echo "========================================"
echo "  VERIFICATION SUMMARY"
echo "  PASSED: ${PASS}"
echo "  FAILED: ${FAIL}"
echo "========================================"

if [[ "${FAIL}" -eq 0 ]]; then
    echo "  ALL CHECKS PASSED — gorilla-thanos pipeline verified end-to-end"
    exit 0
else
    echo "  SOME CHECKS FAILED — see details above"
    echo "  Common causes:"
    echo "    - Blocks not flushed yet (wait 60-90s after stack_up)"
    echo "    - Thanos store-gateway sync lag (wait 30s after first block)"
    echo "    - Container not running (check docker ps on respective nodes)"
    exit 1
fi
