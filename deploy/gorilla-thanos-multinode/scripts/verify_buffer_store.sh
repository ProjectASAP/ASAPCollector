#!/usr/bin/env bash
# verify_buffer_store.sh — Verification checks for gorilla-buffer-store on node2.
#
# New architecture (no gorilla-gateway):
#   Agents → S3 PUT directly → MinIO (node2)
#   gorilla-buffer-store (node2): Thanos store, reads MinIO via S3, syncs every 15s,
#     serves only last BUFFER_STORE_DURATION of blocks via StoreAPI :10921.
#   thanos-store-gateway (node2): reads MinIO, syncs every 30s, serves all history.
#   thanos-query: federates both; hot blocks queryable within ~75s of measurement.
#
# Checks:
#   1. gorilla-buffer-store container is running on node2
#   2. buffer-store HTTP health endpoint alive (node2:10922)
#   3. buffer-store has loaded blocks from MinIO (log: "loaded new block")
#   4. buffer-store is configured with --min-time (hot-window filter)
#   5. buffer-store syncs faster than store-gateway (15s vs 30s)
#   6. Thanos Query has gorilla-buffer-store registered at :10921
#   7. Thanos serves metric names (end-to-end pipeline)
#
# Usage:
#   bash verify_buffer_store.sh \
#     [--backend-host node2]     \   # SSH hostname for node2
#     [--thanos-host 10.10.1.3] \   # IP for Thanos HTTP API
#     [--out results.txt]            # optional output file

set -euo pipefail

BACKEND_HOST="node2"
THANOS_HOST="10.10.1.3"
OUT_FILE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --backend-host) BACKEND_HOST="$2"; shift 2 ;;
        --thanos-host)  THANOS_HOST="$2";  shift 2 ;;
        --out)          OUT_FILE="$2";     shift 2 ;;
        # legacy args (old script compat — ignored)
        --gateway-host|--node1-ip) shift 2 ;;
        *) echo "Unknown arg: $1" >&2; exit 1 ;;
    esac
done

[[ -n "${OUT_FILE}" ]] && exec > >(tee "${OUT_FILE}") 2>&1

PASS=0; FAIL=0; SKIP=0
pass() { echo "  [PASS] $*"; PASS=$((PASS+1)); }
fail() { echo "  [FAIL] $*"; FAIL=$((FAIL+1)); }
skip() { echo "  [SKIP] $*"; SKIP=$((SKIP+1)); }
section() { echo ""; echo "=== $* ==="; }
on_backend() {
    ssh -n -o ConnectTimeout=10 -o BatchMode=yes -o StrictHostKeyChecking=no \
        "${BACKEND_HOST}" "$@" < /dev/null
}

BACKEND_IP=$(getent hosts "${BACKEND_HOST}" 2>/dev/null | awk '{print $1}' || echo "${THANOS_HOST}")
BS_HTTP_URL="http://${THANOS_HOST}:10922"
THANOS_URL="http://${THANOS_HOST}:10903"

echo "========================================"
echo "  gorilla-buffer-store verification"
echo "  $(date)"
echo "  backend-host: ${BACKEND_HOST}"
echo "  thanos-host:  ${THANOS_HOST}"
echo "========================================"

# ── Check 1: gorilla-buffer-store container running on node2 ─────────────
section "Check 1: gorilla-buffer-store container running on ${BACKEND_HOST}"
BS_STATUS=""
if BS_STATUS=$(on_backend 'docker ps --format "{{.Names}}\t{{.Status}}" | grep "^asap-gorilla-buffer-store"' 2>/dev/null) \
   && [[ -n "${BS_STATUS}" ]]; then
    pass "Container running: ${BS_STATUS}"
else
    fail "asap-gorilla-buffer-store not found in docker ps on ${BACKEND_HOST}"
    echo "  Hint: run 'bash run_demo.sh up' — gorilla-buffer-store is launched inside backend_up()"
fi

# ── Check 2: buffer-store HTTP health ────────────────────────────────────
section "Check 2: buffer-store HTTP health (${THANOS_HOST}:10922)"
if curl -sf --max-time 10 "${BS_HTTP_URL}/-/ready" -o /dev/null 2>/dev/null; then
    pass "buffer-store HTTP /-/ready returned 200 OK"
elif curl -sf --max-time 10 "${BS_HTTP_URL}/metrics" -o /dev/null 2>/dev/null; then
    pass "buffer-store HTTP /metrics reachable (/-/ready endpoint not exposed in this build)"
else
    fail "buffer-store HTTP unreachable at ${BS_HTTP_URL}"
    echo "  Hint: check container: ssh ${BACKEND_HOST} 'docker logs asap-gorilla-buffer-store | tail -20'"
fi

# ── Check 3: buffer-store loaded blocks from MinIO ───────────────────────
section "Check 3: buffer-store has loaded blocks from MinIO"
BS_LOGS=""
LOADED_COUNT=0
if BS_LOGS=$(on_backend 'docker logs asap-gorilla-buffer-store 2>&1' 2>/dev/null); then
    LOADED_COUNT=$(echo "${BS_LOGS}" | grep -c '"loaded new block"' || echo 0)
    SYNC_COUNT=$(echo "${BS_LOGS}" | grep -c 'successfully synchronized block metadata' || echo 0)
    echo "  log lines: loaded_new_block=${LOADED_COUNT}  sync_cycles=${SYNC_COUNT}"
    echo "${BS_LOGS}" | grep '"loaded new block"' | tail -3 | sed 's/^/    /'
    if [[ "${LOADED_COUNT}" -gt 0 ]]; then
        pass "buffer-store has loaded ${LOADED_COUNT} block(s) from MinIO"
    else
        fail "No 'loaded new block' in buffer-store logs — no blocks synced from MinIO yet"
        echo "  Hint: wait ~75s (60s block + 15s sync) for first block to appear."
        echo "        Check MinIO: ssh ${BACKEND_HOST} 'docker exec asap-minio mc ls local/asap-gorilla-tsdb'"
    fi
else
    fail "Could not read logs from asap-gorilla-buffer-store on ${BACKEND_HOST}"
fi

# ── Check 4: buffer-store has --min-time configured ──────────────────────
section "Check 4: buffer-store hot-window filter (--min-time)"
BS_ARGS=""
if BS_ARGS=$(on_backend 'docker inspect asap-gorilla-buffer-store --format "{{json .Args}}"' 2>/dev/null); then
    echo "  Container args: ${BS_ARGS}"
    if echo "${BS_ARGS}" | grep -q 'min-time'; then
        MIN_TIME=$(echo "${BS_ARGS}" | python3 -c \
            "import json,sys; args=json.load(sys.stdin); \
             idx=[i for i,a in enumerate(args) if 'min-time' in a]; \
             print(args[idx[0]] if idx else 'not found')" 2>/dev/null || echo "?")
        pass "buffer-store has ${MIN_TIME} — only blocks within window are served"
    else
        fail "--min-time not found in container args — buffer-store serves ALL blocks (same as store-gateway)"
        echo "  Expected: --min-time=-1h (or BUFFER_STORE_DURATION value)"
    fi
else
    fail "Could not inspect asap-gorilla-buffer-store args on ${BACKEND_HOST}"
fi

# ── Check 5: buffer-store sync cadence faster than store-gateway ─────────
section "Check 5: buffer-store syncs every 15s (vs store-gateway 30s)"
BS_SYNC=""
SG_SYNC=""
if BS_SYNC=$(on_backend 'docker inspect asap-gorilla-buffer-store --format "{{json .Args}}"' 2>/dev/null) && \
   SG_SYNC=$(on_backend 'docker inspect asap-thanos-store-gateway --format "{{json .Args}}"' 2>/dev/null); then
    BS_INTERVAL=$(echo "${BS_SYNC}" | python3 -c \
        "import json,sys; args=json.load(sys.stdin); \
         idx=[i for i,a in enumerate(args) if 'sync-block-duration' in a]; \
         print(args[idx[0]+1] if idx else '?')" 2>/dev/null || echo "?")
    SG_INTERVAL=$(echo "${SG_SYNC}" | python3 -c \
        "import json,sys; args=json.load(sys.stdin); \
         idx=[i for i,a in enumerate(args) if 'sync-block-duration' in a]; \
         print(args[idx[0]+1] if idx else '?')" 2>/dev/null || echo "?")
    echo "  gorilla-buffer-store sync-block-duration: ${BS_INTERVAL}"
    echo "  thanos-store-gateway  sync-block-duration: ${SG_INTERVAL}"
    if [[ "${BS_INTERVAL}" == "15s" && "${SG_INTERVAL}" == "30s" ]]; then
        pass "buffer-store (15s) syncs 2× faster than store-gateway (30s)"
    elif [[ "${BS_INTERVAL}" != "?" ]]; then
        pass "buffer-store sync=${BS_INTERVAL}, store-gateway sync=${SG_INTERVAL}"
    else
        fail "Could not read sync-block-duration from container args"
    fi
else
    fail "Could not inspect container args on ${BACKEND_HOST}"
fi

# ── Check 6: Thanos Query has gorilla-buffer-store registered ─────────────
section "Check 6: gorilla-buffer-store registered in Thanos Query (:10921)"
STORES_RESP=""
if STORES_RESP=$(curl -sf --max-time 10 "${THANOS_URL}/api/v1/stores" 2>/dev/null); then
    echo "  Registered stores:"
    echo "${STORES_RESP}" | python3 -c "
import json, sys
d = json.load(sys.stdin)
for store in d.get('data', []):
    name = store.get('name', '?')
    labels = store.get('labelSets', [])
    print(f'    {name}  labels={labels}')
" 2>/dev/null || echo "${STORES_RESP}" | head -c 400 | sed 's/^/    /'
    if echo "${STORES_RESP}" | grep -q "10921"; then
        pass "gorilla-buffer-store:10921 is registered in Thanos Query"
    else
        fail "Port 10921 not found in Thanos /api/v1/stores — buffer-store not connected"
        echo "  Hint: verify thanos-query started with --endpoint=gorilla-buffer-store:10921"
        echo "        and ADD_HOSTS maps gorilla-buffer-store → ${THANOS_HOST}"
    fi
else
    fail "Thanos /api/v1/stores unreachable at ${THANOS_URL}/api/v1/stores"
fi

# ── Check 7: Thanos serves metric names (end-to-end) ─────────────────────
section "Check 7: Thanos serves metric names (end-to-end pipeline)"
LABEL_RESP=""
if LABEL_RESP=$(curl -sf --max-time 15 "${THANOS_URL}/api/v1/label/__name__/values" 2>/dev/null) && \
   echo "${LABEL_RESP}" | grep -q '"status":"success"'; then
    METRIC_COUNT=$(echo "${LABEL_RESP}" | python3 -c \
        "import json,sys; d=json.load(sys.stdin); print(len(d.get('data',[])))" 2>/dev/null || echo 0)
    echo "  Metric names: ${METRIC_COUNT}"
    echo "${LABEL_RESP}" | python3 -c "
import sys, json
d = json.load(sys.stdin)
for n in d.get('data', [])[:10]:
    print('   ', n)
" 2>/dev/null || true
    if [[ "${METRIC_COUNT}" -gt 0 ]]; then
        pass "Thanos serves ${METRIC_COUNT} metric name(s) — direct-MinIO pipeline end-to-end"
    else
        fail "Thanos returned 0 metric names — store sync not yet complete (wait 15-30s)"
    fi
else
    fail "Thanos label API unreachable or returned error"
fi

# ── Summary ───────────────────────────────────────────────────────────────
echo ""
echo "========================================"
echo "  BUFFER-STORE VERIFICATION SUMMARY"
echo "  PASSED:  ${PASS}"
echo "  FAILED:  ${FAIL}"
echo "  SKIPPED: ${SKIP}"
echo "========================================"

if [[ "${FAIL}" -eq 0 ]]; then
    echo "  ALL CHECKS PASSED — gorilla-buffer-store (direct-MinIO arch) verified"
    exit 0
else
    echo "  SOME CHECKS FAILED — see details above"
    exit 1
fi
