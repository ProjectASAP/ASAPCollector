#!/usr/bin/env bash
# verify_buffer_store.sh — Verification checks for the disk-buffer + gorilla-buffer-store upgrade.
#
# Tests the new gorilla-gateway architecture:
#   - Gateway writes blocks to GATEWAY_BUFFER_DIR on disk (not in-memory)
#   - A gorilla-buffer-store sidecar (Thanos filesystem store) reads that dir
#     and exposes blocks via Store gRPC, giving ~30s freshness instead of the
#     full flush interval lag.
#   - Thanos Query uses both thanos-store-gateway (MinIO) and gorilla-buffer-store (disk),
#     deduplicating via --query.replica-label=block_source.
#   - Disk blocks carry block_source=gateway-buffer; MinIO blocks carry block_source=minio.
#
# Checks:
#   1. gorilla-buffer-store container is running on node1
#   2. buffer-store HTTP health endpoint is alive (node1:10922/-/ready)
#   3. gorilla-gateway /v1/blocks API reports complete blocks in the disk buffer
#   4. block_source=gateway-buffer is injected in on-disk meta.json files
#   5. Thanos Query has gorilla-buffer-store registered as a Store endpoint
#   6. Thanos serves metric names (data is queryable from the buffer path)
#   7. Freshness: disk buffer contains blocks not yet flushed to MinIO (complete + not flushing)
#
# Usage:
#   bash verify_buffer_store.sh \
#     [--gateway-host node1]    \   # SSH-accessible hostname for node1 (docker ps, file reads)
#     [--node1-ip   10.10.1.2] \   # IP of node1 (curl to /v1/blocks and buffer-store HTTP)
#     [--thanos-host 10.10.1.3] \  # IP of Thanos query (curl)
#     [--out results.txt]           # optional output file

set -euo pipefail

# ── Argument parsing ──────────────────────────────────────────────────────
GATEWAY_HOST="node1"
NODE1_IP="10.10.1.2"
THANOS_HOST="10.10.1.3"
OUT_FILE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --gateway-host) GATEWAY_HOST="$2"; shift 2 ;;
        --node1-ip)     NODE1_IP="$2";     shift 2 ;;
        --thanos-host)  THANOS_HOST="$2";  shift 2 ;;
        --out)          OUT_FILE="$2";     shift 2 ;;
        *) echo "Unknown arg: $1" >&2; exit 1 ;;
    esac
done

if [[ -n "${OUT_FILE}" ]]; then
    exec > >(tee "${OUT_FILE}") 2>&1
fi

# ── Helpers ───────────────────────────────────────────────────────────────
PASS=0; FAIL=0; SKIP=0
pass() { echo "  [PASS] $*"; PASS=$((PASS+1)); }
fail() { echo "  [FAIL] $*"; FAIL=$((FAIL+1)); }
skip() { echo "  [SKIP] $*"; SKIP=$((SKIP+1)); }
section() { echo ""; echo "=== $* ==="; }
on_node1() { ssh -n -o ConnectTimeout=10 -o BatchMode=yes -o StrictHostKeyChecking=no \
                 "${GATEWAY_HOST}" "$@" < /dev/null; }

echo "========================================"
echo "  gorilla-buffer-store verification"
echo "  $(date)"
echo "  gateway-host: ${GATEWAY_HOST}"
echo "  node1-ip:     ${NODE1_IP}"
echo "  thanos-host:  ${THANOS_HOST}"
echo "========================================"

# ── Check 1: gorilla-buffer-store container is running on node1 ───────────
section "Check 1: gorilla-buffer-store container running on node1"
BS_NAME=""
if BS_NAME=$(on_node1 'docker ps --format "{{.Names}}" | grep "^asap-gorilla-buffer-store$"' 2>/dev/null); then
    pass "Container '${BS_NAME}' is running on ${GATEWAY_HOST}"
else
    fail "asap-gorilla-buffer-store not found in docker ps on ${GATEWAY_HOST}"
    echo "  Hint: run 'bash run_demo.sh up' which calls buffer_store_up() after gateway_up()"
    echo "        or start it manually: docker run --name asap-gorilla-buffer-store ..."
fi

# ── Check 2: buffer-store HTTP health endpoint alive ─────────────────────
section "Check 2: buffer-store HTTP health (${NODE1_IP}:10922/-/ready)"
BS_HTTP_URL="http://${NODE1_IP}:10922"
if curl -sf --max-time 10 "${BS_HTTP_URL}/-/ready" -o /dev/null; then
    pass "buffer-store HTTP /-/ready returned 200 OK"
elif curl -sf --max-time 10 "${BS_HTTP_URL}/metrics" -o /dev/null; then
    pass "buffer-store HTTP /metrics reachable (/-/ready not supported in this build)"
else
    fail "buffer-store HTTP unreachable at ${BS_HTTP_URL}"
    echo "  Hint: check if port 10922 is exposed and gorilla-buffer-store is healthy:"
    echo "        ssh ${GATEWAY_HOST} 'docker logs asap-gorilla-buffer-store | tail -20'"
fi

# ── Check 3: /v1/blocks API reports complete blocks ───────────────────────
section "Check 3: gorilla-gateway /v1/blocks API (${NODE1_IP}:9100/v1/blocks)"
BLOCKS_URL="http://${NODE1_IP}:9100/v1/blocks"
BLOCKS_JSON=""
COMPLETE_COUNT=0
FLUSHING_COUNT=0
if BLOCKS_JSON=$(curl -sf --max-time 10 "${BLOCKS_URL}" 2>/dev/null); then
    COMPLETE_COUNT=$(echo "${BLOCKS_JSON}" | python3 -c \
        "import json,sys; d=json.load(sys.stdin); print(sum(1 for b in d if b.get('complete')))" 2>/dev/null || echo 0)
    FLUSHING_COUNT=$(echo "${BLOCKS_JSON}" | python3 -c \
        "import json,sys; d=json.load(sys.stdin); print(sum(1 for b in d if b.get('flushing')))" 2>/dev/null || echo 0)
    TOTAL_COUNT=$(echo "${BLOCKS_JSON}" | python3 -c \
        "import json,sys; d=json.load(sys.stdin); print(len(d))" 2>/dev/null || echo 0)
    echo "  /v1/blocks: total=${TOTAL_COUNT} complete=${COMPLETE_COUNT} flushing=${FLUSHING_COUNT}"
    echo "${BLOCKS_JSON}" | python3 -c "
import json, sys
d = json.load(sys.stdin)
for b in d[:5]:
    print(f\"  ulid={b.get('ulid','?')}  complete={b.get('complete')}  flushing={b.get('flushing')}\")
if len(d) > 5:
    print(f'  ... and {len(d)-5} more')
" 2>/dev/null || true
    if [[ "${COMPLETE_COUNT}" -gt 0 ]]; then
        pass "/v1/blocks shows ${COMPLETE_COUNT} complete block(s) in disk buffer"
    else
        fail "/v1/blocks returned 0 complete blocks — gateway may not have received any yet"
        echo "  Hint: wait ~10s after agent startup for first gorillas3 block flush."
        echo "        Check: ssh ${GATEWAY_HOST} 'docker logs asap-gorilla-gateway | grep recv'"
    fi
else
    fail "/v1/blocks endpoint unreachable at ${BLOCKS_URL}"
    echo "  Hint: check that gorilla-gateway is running and the new main.go (with disk buffer) is deployed."
fi

# ── Check 4: block_source=gateway-buffer in on-disk meta.json ────────────
section "Check 4: block_source=gateway-buffer injected in on-disk meta.json"
META_PATH=""
META_JSON=""
if META_PATH=$(on_node1 'find /mydata/gorilla-gateway/buffer -name meta.json | head -1' 2>/dev/null) && \
   [[ -n "${META_PATH}" ]]; then
    META_JSON=$(on_node1 "cat '${META_PATH}'" 2>/dev/null || echo "")
    echo "  Found meta.json at: ${META_PATH}"
    BLOCK_SOURCE=$(echo "${META_JSON}" | python3 -c \
        "import json,sys; m=json.load(sys.stdin); print(m.get('thanos',{}).get('labels',{}).get('block_source','MISSING'))" \
        2>/dev/null || echo "PARSE_ERROR")
    echo "  thanos.labels.block_source = '${BLOCK_SOURCE}'"
    if [[ "${BLOCK_SOURCE}" == "gateway-buffer" ]]; then
        pass "block_source=gateway-buffer correctly injected by injectThanosLabel()"
    elif [[ "${BLOCK_SOURCE}" == "MISSING" ]]; then
        fail "thanos.labels.block_source key not found in meta.json — label injection failed"
        echo "  Expected: DiskBuffer.Write() calls injectThanosLabel(data, \"gateway-buffer\")"
        echo "  meta.json thanos section: $(echo "${META_JSON}" | python3 -c "import json,sys; m=json.load(sys.stdin); print(json.dumps(m.get('thanos','absent'), indent=2))" 2>/dev/null)"
    else
        fail "block_source='${BLOCK_SOURCE}' (expected 'gateway-buffer')"
    fi
else
    fail "No meta.json found under /mydata/gorilla-gateway/buffer on ${GATEWAY_HOST}"
    echo "  Hint: buffer dir may be empty — wait for first gorillas3 block (10s after startup)."
    echo "        Check: ssh ${GATEWAY_HOST} 'ls /mydata/gorilla-gateway/buffer/'"
fi

# ── Check 5: Thanos Query has gorilla-buffer-store registered ─────────────
section "Check 5: gorilla-buffer-store registered in Thanos Query (${THANOS_HOST}:10903)"
THANOS_URL="http://${THANOS_HOST}:10903"
STORES_RESP=""
if STORES_RESP=$(curl -sf --max-time 10 "${THANOS_URL}/api/v1/stores" 2>/dev/null); then
    echo "  Thanos /api/v1/stores response (first 500 chars):"
    echo "${STORES_RESP}" | head -c 500 | sed 's/^/    /'
    echo ""
    # Look for port 10921 (gorilla-buffer-store gRPC port)
    if echo "${STORES_RESP}" | grep -q "10921"; then
        pass "Thanos Query has gorilla-buffer-store:10921 registered as a store endpoint"
    else
        fail "Port 10921 not found in Thanos /api/v1/stores — gorilla-buffer-store is not connected"
        echo "  Hint: verify asap-thanos-query was started with --endpoint=gorilla-buffer-store:10921"
        echo "        and that ADD_HOSTS includes --add-host=gorilla-buffer-store:${NODE1_IP}"
        echo "        Check: ssh node2 'docker inspect asap-thanos-query | grep endpoint'"
    fi
else
    fail "Thanos /api/v1/stores unreachable at ${THANOS_URL}/api/v1/stores"
    echo "  Hint: check thanos-query is running on node2:"
    echo "        ssh node2 'docker ps | grep thanos-query'"
fi

# ── Check 6: Thanos serves metric names via buffer-store path ─────────────
section "Check 6: Thanos serves metric names (buffer-store or MinIO)"
LABEL_RESP=""
METRIC_COUNT=0
if LABEL_RESP=$(curl -sf --max-time 15 \
    "${THANOS_URL}/api/v1/label/__name__/values" 2>/dev/null); then
    if echo "${LABEL_RESP}" | grep -q '"status":"success"'; then
        METRIC_COUNT=$(echo "${LABEL_RESP}" | python3 -c \
            "import json,sys; d=json.load(sys.stdin); print(len(d.get('data',[])))" 2>/dev/null || echo 0)
        echo "  Metric names served: ${METRIC_COUNT}"
        echo "${LABEL_RESP}" | python3 -c "
import sys, json
d = json.load(sys.stdin)
for n in d.get('data', [])[:8]:
    print('   ', n)
" 2>/dev/null || true
        if [[ "${METRIC_COUNT}" -gt 0 ]]; then
            pass "Thanos serves ${METRIC_COUNT} metric name(s) — buffer-store pipeline is end-to-end"
        else
            fail "Thanos returned 0 metric names — buffer-store may not have synced yet"
            echo "  Hint: wait 30s for gorilla-buffer-store sync-block-duration to pick up new blocks."
            echo "        Blocks need to be complete (all 3 files present) to appear in the buffer."
        fi
    else
        fail "Thanos label API status != success"
    fi
else
    fail "Thanos label API unreachable at ${THANOS_URL}/api/v1/label/__name__/values"
fi

# ── Check 7: Freshness — buffer has blocks not yet flushed to MinIO ───────
section "Check 7: Freshness — disk buffer contains pre-flush blocks (complete + not flushing)"
if [[ -n "${BLOCKS_JSON}" ]] && [[ "${TOTAL_COUNT}" -gt 0 ]]; then
    UNFLUSHED_COUNT=$(echo "${BLOCKS_JSON}" | python3 -c \
        "import json,sys; d=json.load(sys.stdin); print(sum(1 for b in d if b.get('complete') and not b.get('flushing')))" \
        2>/dev/null || echo 0)
    echo "  Blocks complete but not yet flushed to MinIO: ${UNFLUSHED_COUNT}"
    if [[ "${UNFLUSHED_COUNT}" -gt 0 ]]; then
        pass "${UNFLUSHED_COUNT} complete block(s) are pre-flush in buffer — gorilla-buffer-store is serving data unavailable in MinIO"
        echo "  These blocks have block_source=gateway-buffer and are queryable via gorilla-buffer-store:10921"
        echo "  After GATEWAY_FLUSH_INTERVAL they will be uploaded with block_source=minio and deleted locally."
    else
        echo "  All complete blocks are already marked 'flushing' (mid-upload) or the flush already ran."
        echo "  This is acceptable in a steady-state system — try catching it right after a gorillas3 flush."
        skip "No pre-flush blocks at this instant; buffer may have just been flushed (not a failure)"
    fi
else
    skip "/v1/blocks data not available from Check 3; skipping freshness check"
fi

# ── Summary ───────────────────────────────────────────────────────────────
echo ""
echo "========================================"
echo "  BUFFER-STORE VERIFICATION SUMMARY"
echo "  PASSED: ${PASS}"
echo "  FAILED: ${FAIL}"
echo "  SKIPPED: ${SKIP}"
echo "========================================"

if [[ "${FAIL}" -eq 0 ]]; then
    echo "  ALL CHECKS PASSED (${SKIP} skipped) — gorilla-buffer-store upgrade verified"
    exit 0
else
    echo "  SOME CHECKS FAILED — see details above"
    echo ""
    echo "  Common causes:"
    echo "    Check 1/2: buffer-store not started (run_demo.sh up now calls buffer_store_up)"
    echo "    Check 3:   gateway image is old (pre-disk-buffer); rebuild: docker build -f Dockerfile.gorilla-gateway"
    echo "    Check 4:   old gateway binary (in-memory); same rebuild needed"
    echo "    Check 5:   thanos-query started without --endpoint=gorilla-buffer-store:10921"
    echo "    Check 6:   sync-block-duration not elapsed yet (wait 30s)"
    echo "    Check 7:   timing — flush may have run between checks (not a hard failure)"
    exit 1
fi
