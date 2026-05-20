#!/usr/bin/env bash
# run_demo.sh — gorilla-thanos-multinode demo orchestrator.
#
# Drives the gorilla-thanos stack across 4 nodes on 10.10.1.0/24:
#
#   node0 (10.10.1.1)  producers + agent-a   (data source)
#   node1 (10.10.1.2)  idle (no services)
#   node2 (10.10.1.3)  backend: MinIO + Thanos (store-gateway=archive-store, query, compact)
#                                + gorilla-buffer-store (hot-store; recent merged windows, BUFFER_STORE_DURATION)
#   node3 (10.10.1.4)  producers + agent-b   (data source)
#
# Pipeline: agents build 60s Gorilla TSDB blocks and POST them to the head-merger
#   (no direct S3 write, no gateway hop).
#   gorilla-head-merger on node2: receives per-emit blocks over HTTP (:9099/ingest),
#   durably stages them in /tmp/gorilla-buffer/served/ (block-level WAL + hot-store
#   source), and on TUMBLING window close (MERGE_WINDOW, configurable, default 1h)
#   concatenates each window's per-series chunks into one block, uploads that single
#   block to MinIO, then prunes the per-emit blocks. Windows older than retention dropped.
#   gorilla-buffer-store (hot-store) on node2: thanos store (FILESYSTEM objstore on the
#   served dir) serves the current window's per-emit blocks (fresh) + recent cut blocks via :10921.
#   thanos-store-gateway (archive-store): syncs every 30s, serves cut blocks from MinIO.
#   thanos-query federates both endpoints (fan-out + chunk merge).
#
# Image requirement: asap/asap-otel:dev must be built after 2026-05-17
#   (gorillas3 Phase 3 — `bucket:` field removed). Rebuild if needed:
#   cd /mydata/ASAPCollector && bash restore_otel_collector_contrib_patches.sh && bash build_asap_otel.sh
#
# Commands:
#   sync     rsync configs + topology.env to nodes 0, 2, 3
#   up       bring up backend+buffer-store (node2) + agents/producers (node0, node3)
#   down     stop all asap-* containers on node0, node2, node3
#   verify   run verify_gorilla_compression.sh success-metric checks
#   all      sync + down + up + soak + verify + down

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PKG_DIR="$(dirname "${SCRIPT_DIR}")"
source "${PKG_DIR}/topology.env"

RUN_ID="${RUN_ID:-gorilla-thanos-$(date +%Y%m%d-%H%M%S)}"
mkdir -p "${LOG_BASE}"

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" | tee -a "${LOG_BASE}/${RUN_ID}.log" >&2; }
die() { log "FATAL: $*"; exit 1; }

# ── ssh wrapper — runs commands on a remote node ──────────────────────────
on() {
    local node=$1; shift
    timeout --kill-after=5 30 \
        ssh -n -o ConnectTimeout=5 -o StrictHostKeyChecking=no -o BatchMode=yes \
            -o ServerAliveInterval=5 -o ServerAliveCountMax=2 \
            "${node}" "$@" < /dev/null
}

# ── stop + remove all asap-* containers on a node ────────────────────────
stop_node() {
    local node=$1
    on "${node}" 'docker ps -a --format "{{.Names}}" | grep "^asap-" | xargs -r docker rm -f' || true
}

# ── stage configs to a remote node under /mydata ─────────────────────────
# Syncs configs/ and topology.env. Does NOT sync scripts/.
sync_to() {
    local node=$1
    log "rsync configs → ${node}"
    rsync -a --delete \
        "${CONFIG_SRC}/" \
        "${node}:/mydata/gorilla-thanos-multinode/configs/"
    rsync -a "${PKG_DIR}/topology.env" \
        "${node}:/mydata/gorilla-thanos-multinode/topology.env"
}

# Sync to active nodes only (node1 is idle).
sync_all_nodes() {
    for n in "${NODE0_HOST}" "${NODE2_HOST}" "${NODE3_HOST}"; do
        on "${n}" 'mkdir -p /mydata/gorilla-thanos-multinode/{configs,logs,results}'
    done
    sync_to "${NODE0_HOST}"
    sync_to "${NODE2_HOST}"
    sync_to "${NODE3_HOST}"
}

# ── docker_run wrapper — always uses host network + DNS aliases ───────────
docker_run_on() {
    local node=$1; shift
    on "${node}" "docker run -d --restart unless-stopped --network host \
        ${ADD_HOSTS[*]} \
        $*"
}

# ─── BACKEND STACK on node2 ───────────────────────────────────────────────
# Services: MinIO + Thanos (store-gateway, query, compact)
#           + gorilla-head-merger + gorilla-buffer-store.
#
# gorilla-head-merger: receives per-emit Gorilla blocks over HTTP (:9099/ingest),
#   durably stages them in /tmp/gorilla-buffer/served/ (block-level WAL + hot-store
#   source), and on tumbling-window close (MERGE_WINDOW, configurable, default 1h)
#   concatenates each window's per-series chunks into one block, uploads that single
#   block to MinIO, then prunes the per-emit blocks. Drops windows older than retention.
#
# gorilla-buffer-store (hot-store): thanos store (FILESYSTEM objstore on served dir) :10921.
#   Serves the current window's per-emit blocks (fresh) + recent cut blocks.
#
# thanos-store-gateway (archive-store) :10901 — cut blocks from MinIO (30s sync, history).
# Thanos Query federates both endpoints (fan-out + chunk-level merge).
backend_up() {
    log "node2 backend up (MinIO + Thanos + gorilla-head-merger/buffer-store, window=${MERGE_WINDOW:-1h})"

    # MinIO
    docker_run_on "${NODE2_HOST}" \
        --name asap-minio \
        -e MINIO_ROOT_USER=asap \
        -e MINIO_ROOT_PASSWORD=asap-local-only \
        minio/minio:latest server /data --console-address :9001

    sleep 5

    # Create asap-gorilla-tsdb bucket (sole write destination; asap-gorilla is never written).
    on "${NODE2_HOST}" "docker run --rm --network host \
        ${ADD_HOSTS[*]} \
        --entrypoint=sh minio/mc:latest -c '
            mc alias set local http://minio:9000 asap asap-local-only &&
            mc mb --ignore-existing local/asap-gorilla-tsdb &&
            mc anonymous set download local/asap-gorilla-tsdb'" \
        || log "minio bucket setup non-fatal warn — bucket may already exist"

    # Thanos store-gateway — serves ALL blocks from MinIO via StoreAPI :10901
    docker_run_on "${NODE2_HOST}" \
        --name asap-thanos-store-gateway \
        --user 0 \
        -v /mydata/gorilla-thanos-multinode/configs/thanos-objstore.yaml:/etc/thanos/objstore.yaml:ro \
        quay.io/thanos/thanos:v0.41.0 \
        store \
        --objstore.config-file=/etc/thanos/objstore.yaml \
        --http-address=0.0.0.0:10902 \
        --grpc-address=0.0.0.0:10901 \
        --data-dir=/tmp/thanos-store \
        --sync-block-duration=30s \
        --block-sync-concurrency=20

    # gorilla-head-merger — local head block + block-level WAL.
    # Receives per-emit Gorilla blocks from agents over HTTP (:9099/ingest),
    # durably stages them in the served dir (WAL + hot-store source), and on
    # window close (now >= end + grace) concatenates each window's per-series
    # chunks into ONE block, uploads that single block to MinIO, then prunes the
    # per-emit blocks. Windows older than -retention=BUFFER_STORE_DURATION drop.
    on "${NODE2_HOST}" "mkdir -p /tmp/gorilla-buffer/served"
    docker_run_on "${NODE2_HOST}" \
        --name asap-gorilla-head-merger \
        --user 0 \
        -v /tmp/gorilla-buffer:/var/gorilla-buffer \
        asap/gorilla-head-merger:dev \
        -bucket=asap-gorilla-tsdb \
        -endpoint=minio:9000 \
        -access-key=asap \
        -secret-key=asap-local-only \
        -window="${MERGE_WINDOW:-1h}" \
        -retention="${BUFFER_STORE_DURATION}" \
        -cut-interval=15s \
        -ingest-addr=":${MERGER_INGEST_PORT:-9099}" \
        -served-dir=/var/gorilla-buffer/served

    # gorilla-buffer-store (hot-store) — serves the head-merger's served dir via StoreAPI :10921.
    # Reads from FILESYSTEM objstore (/tmp/gorilla-buffer/served/): the current
    # window's per-emit blocks (fresh) + the recent cut window blocks.
    # No --min-time filter: the merger handles expiry.
    docker_run_on "${NODE2_HOST}" \
        --name asap-gorilla-buffer-store \
        --user 0 \
        -v /mydata/gorilla-thanos-multinode/configs/buffer-fs-objstore.yaml:/etc/thanos/objstore.yaml:ro \
        -v /tmp/gorilla-buffer/served:/var/gorilla-buffer/served:ro \
        quay.io/thanos/thanos:v0.41.0 \
        store \
        --objstore.config-file=/etc/thanos/objstore.yaml \
        --http-address=0.0.0.0:10922 \
        --grpc-address=0.0.0.0:10921 \
        --data-dir=/tmp/thanos-buffer-store \
        --sync-block-duration=20s \
        --block-sync-concurrency=4

    # Thanos query — federated PromQL over store-gateway (all) + buffer-store (hot window)
    docker_run_on "${NODE2_HOST}" \
        --name asap-thanos-query \
        quay.io/thanos/thanos:v0.41.0 \
        query \
        --grpc-address=0.0.0.0:10911 \
        --http-address=0.0.0.0:10903 \
        --endpoint=thanos-store-gateway:10901 \
        --endpoint=gorilla-buffer-store:10921

    # Thanos compact — background block compaction and downsampling
    docker_run_on "${NODE2_HOST}" \
        --name asap-thanos-compact \
        --user 0 \
        -v /mydata/gorilla-thanos-multinode/configs/thanos-objstore.yaml:/etc/thanos/objstore.yaml:ro \
        quay.io/thanos/thanos:v0.41.0 \
        compact \
        --objstore.config-file=/etc/thanos/objstore.yaml \
        --data-dir=/tmp/thanos-compact \
        --retention.resolution-raw=30d \
        --retention.resolution-5m=180d \
        --retention.resolution-1h=1y \
        --compact.concurrency=1 \
        --wait \
        --http-address=0.0.0.0:10904
}

backend_down() {
    log "node2 backend down"
    stop_node "${NODE2_HOST}"
}

# ─── AGENTS + PRODUCERS on node0 and node3 ───────────────────────────────
# Agent config: gorillas3-only, drop_original: true.
# gorillas3 builds 60s Gorilla TSDB blocks and POSTs them to the head-merger
# (gorilla-head-merger:${MERGER_INGEST_PORT}/ingest), NOT to MinIO.
agents_up() {
    log "node0 agent-a up (gorilla-only, ship → gorilla-head-merger:${MERGER_INGEST_PORT:-9099})"
    docker_run_on "${NODE0_HOST}" \
        --name asap-agent-a \
        --hostname agent-a \
        -e AGENT_ID=agent-a \
        -v /mydata/gorilla-thanos-multinode/configs/agent-gorilla-only.yaml:/etc/otel/config.yaml:ro \
        asap/asap-otel:dev \
        --config=/etc/otel/config.yaml

    log "node3 agent-b up (gorilla-only, ship → gorilla-head-merger:${MERGER_INGEST_PORT:-9099})"
    docker_run_on "${NODE3_HOST}" \
        --name asap-agent-b \
        --hostname agent-b \
        -e AGENT_ID=agent-b \
        -v /mydata/gorilla-thanos-multinode/configs/agent-gorilla-only.yaml:/etc/otel/config.yaml:ro \
        asap/asap-otel:dev \
        --config=/etc/otel/config.yaml

    sleep 5

    for i in $(seq 1 ${N_PRODUCERS_PER_NODE}); do
        log "node0 producer-a-${i} up"
        docker_run_on "${NODE0_HOST}" \
            --name asap-producer-a-${i} \
            -e EXPORTER_TARGET=agent-a:4317 \
            -e EXPORTER_PRODUCER_ID=p-a-${i} \
            -e EXPORTER_RATE=${EXPORTER_RATE} \
            -e EXPORTER_CARDINALITY=${PER_AGENT_CARDINALITY} \
            -e EXPORTER_FREQ_HZ=${EXPORTER_FREQ_HZ} \
            -e EXPORTER_SDK_WINDOW=${EXPORTER_SDK_WINDOW} \
            -e EXPORTER_SDK_AGG=${EXPORTER_SDK_AGG} \
            -e EXPORTER_MAX_BUFFER_PER_SERIES=${EXPORTER_MAX_BUFFER_PER_SERIES} \
            -e EXPORTER_FRESHNESS_PROBES=${EXPORTER_FRESHNESS_PROBES} \
            -e EXPORTER_FRESHNESS_PROBE_HZ=${EXPORTER_FRESHNESS_PROBE_HZ} \
            -e EXPORTER_FIVE_SKETCH=${EXPORTER_FIVE_SKETCH} \
            -e EXPORTER_FIXED_LATENCY=${EXPORTER_FIXED_LATENCY} \
            asap/fake-exporter:dev
    done

    for i in $(seq 1 ${N_PRODUCERS_PER_NODE}); do
        log "node3 producer-b-${i} up"
        docker_run_on "${NODE3_HOST}" \
            --name asap-producer-b-${i} \
            -e EXPORTER_TARGET=agent-b:4317 \
            -e EXPORTER_PRODUCER_ID=p-b-${i} \
            -e EXPORTER_RATE=${EXPORTER_RATE} \
            -e EXPORTER_CARDINALITY=${PER_AGENT_CARDINALITY} \
            -e EXPORTER_FREQ_HZ=${EXPORTER_FREQ_HZ} \
            -e EXPORTER_SDK_WINDOW=${EXPORTER_SDK_WINDOW} \
            -e EXPORTER_SDK_AGG=${EXPORTER_SDK_AGG} \
            -e EXPORTER_MAX_BUFFER_PER_SERIES=${EXPORTER_MAX_BUFFER_PER_SERIES} \
            -e EXPORTER_FRESHNESS_PROBES=${EXPORTER_FRESHNESS_PROBES} \
            -e EXPORTER_FRESHNESS_PROBE_HZ=${EXPORTER_FRESHNESS_PROBE_HZ} \
            -e EXPORTER_FIVE_SKETCH=${EXPORTER_FIVE_SKETCH} \
            -e EXPORTER_FIXED_LATENCY=${EXPORTER_FIXED_LATENCY} \
            asap/fake-exporter:dev
    done
}

agents_down() {
    log "node0 + node3 agents/producers down"
    stop_node "${NODE0_HOST}"
    stop_node "${NODE3_HOST}"
}

# ─── Stack lifecycle ──────────────────────────────────────────────────────
stack_up() {
    backend_up
    sleep 5
    agents_up
    log "=== stack up; waiting WARMUP_S=${WARMUP_S}s for gorillas3 to flush first blocks ==="
    sleep "${WARMUP_S}"
}

stack_down() {
    agents_down
    backend_down
}

# ─── Verification ─────────────────────────────────────────────────────────
run_verify() {
    log "=== running verify_gorilla_compression.sh ==="
    bash "${SCRIPT_DIR}/verify_gorilla_compression.sh" \
        --minio-host "${NODE2_HOST}" \
        --thanos-host "${NODE2_IP}" \
        --agent-host "${NODE0_IP}" \
        "$@"
}

# ─── Command dispatch ─────────────────────────────────────────────────────
cmd="${1:-help}"
case "${cmd}" in
    sync)
        sync_all_nodes
        ;;
    up)
        stack_up
        ;;
    down)
        stack_down
        ;;
    verify)
        run_verify "${@:2}"
        ;;
    all)
        log "=== gorilla-thanos-multinode: full run ==="
        sync_all_nodes
        stack_down || true
        stack_up
        log "=== soaking for SOAK_S=${SOAK_S}s ==="
        sleep "${SOAK_S}"
        run_verify
        stack_down || true
        log "=== run complete; logs in ${LOG_BASE}/ ==="
        ;;
    help|*)
        cat <<EOF
usage: $0 <cmd>
  sync     rsync configs + topology.env to nodes 0, 2, 3
  up       bring up backend+buffer-store (node2) + agents/producers (node0, node3)
  down     stop all asap-* containers on node0, node2, node3
  verify   run verify_gorilla_compression.sh success-metric checks
  all      sync + down + up + soak (${SOAK_S}s) + verify + down

Topology:
  node0 (10.10.1.1)  producers + agent-a (gorilla-only, ship → head-merger)
  node1 (10.10.1.2)  idle
  node2 (10.10.1.3)  MinIO + Thanos (store-gateway, query, compact) + gorilla-head-merger + gorilla-buffer-store
  node3 (10.10.1.4)  producers + agent-b (gorilla-only, ship → head-merger)

Network traffic:
  producers → OTLP gRPC :4317 → agent → HTTP POST :${MERGER_INGEST_PORT:-9099} → gorilla-head-merger (per-emit Gorilla blocks)
  merger → S3 PUT :9000 → MinIO (one cut block per window, on window close)
  NO raw OTLP crosses the network (drop_original: true)

Buffer window: BUFFER_STORE_DURATION=${BUFFER_STORE_DURATION}, MERGE_WINDOW=${MERGE_WINDOW:-1h} (set in topology.env)
  gorilla-head-merger   :9099 — receives per-emit blocks, WALs them, cuts each window into one block, flushes to S3
  gorilla-buffer-store  :10921 — hot-store: serves current-window per-emit blocks (fresh) + recent cut blocks via FILESYSTEM objstore
  thanos-store-gateway  :10901 — archive-store: syncs MinIO every 30s, serves cut blocks (history)
  thanos-query          :10903 — federates both (fan-out + chunk merge)

Image note: asap/asap-otel:dev must be built with the gorillas3 ship_endpoint sink (issue #408).
  Rebuild: cd /mydata/ASAPCollector && bash restore_otel_collector_contrib_patches.sh && bash build_asap_otel.sh
  Merger: docker build -f deploy/docker/Dockerfile.gorilla-head-merger -t asap/gorilla-head-merger:dev .
EOF
        ;;
esac
