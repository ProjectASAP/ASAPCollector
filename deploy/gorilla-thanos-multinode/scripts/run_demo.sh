#!/usr/bin/env bash
# run_demo.sh — gorilla-thanos-multinode demo orchestrator.
#
# Drives the gorilla-thanos stack across 4 nodes on 10.10.1.0/24:
#
#   node0 (10.10.1.1)  producers + agent-a   (data source)
#   node1 (10.10.1.2)  gorilla-gateway        (S3 proxy, 20s flush to MinIO)
#   node2 (10.10.1.3)  backend: MinIO + Thanos (query, store-gateway, compact)
#   node3 (10.10.1.4)  producers + agent-b   (data source)
#
# Key differences vs. mvp-multinode/scripts/run_demo.sh:
#   - gorillas3 only — no sketch processors (ddsketch/KLL/HLL/countsketch/countmin)
#   - No routing connector, no OpAMP controller
#   - gorilla-gateway on node1 (agent → 10s blocks → gateway → 20s flush → MinIO)
#   - No asap-query-backend Rust binary
#   - Thanos query engine instead of asap_query_engine
#   - drop_original: true — NO raw OTLP crosses the network to any backend
#     (only S3 PUTs to MinIO port 9000)
#
# Commands:
#   sync     rsync configs to nodes 0, 1, 2, 3
#   up       bring up backend (node2) + agents/producers (node0, node3)
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
# Wrapped with timeout to prevent stuck SSH channels from blocking the
# orchestrator (common race after `docker run -d` under load).
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
# Note: syncs only configs/ (not scripts/ or mvp paths).
# Destination: /mydata/gorilla-thanos-multinode/configs/
sync_to() {
    local node=$1
    log "rsync configs → ${node}"
    rsync -a --delete \
        "${CONFIG_SRC}/" \
        "${node}:/mydata/gorilla-thanos-multinode/configs/"
    rsync -a "${PKG_DIR}/topology.env" \
        "${node}:/mydata/gorilla-thanos-multinode/topology.env"
}

# Sync to nodes 0, 2, 3 only. Node1 is idle (no gateway).
sync_all_nodes() {
    for n in "${NODE0_HOST}" "${NODE1_HOST}" "${NODE2_HOST}" "${NODE3_HOST}"; do
        on "${n}" 'mkdir -p /mydata/gorilla-thanos-multinode/{configs,logs,results}'
    done
    sync_to "${NODE0_HOST}"
    sync_to "${NODE1_HOST}"
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
# Services: MinIO + Thanos (store-gateway, query, compact).
# No controller, no asap-query-backend, no prometheus.
backend_up() {
    log "node2 backend up (MinIO + Thanos)"

    # MinIO — S3-compatible object store for gorillas3 TSDB blocks
    docker_run_on "${NODE2_HOST}" \
        --name asap-minio \
        -e MINIO_ROOT_USER=asap \
        -e MINIO_ROOT_PASSWORD=asap-local-only \
        minio/minio:latest server /data --console-address :9001

    # Wait for MinIO to be ready before creating buckets
    sleep 5

    # Create required buckets using mc (minio client).
    # --entrypoint=sh is required: mc image's default entrypoint is `mc`,
    # not `sh` — without this override `sh -c '...'` is silently rejected
    # and buckets are never created.
    # Alias name: 'local' (NOT 'asap') to avoid collision with the MinIO
    # username 'asap' in mc's namespace.
    on "${NODE2_HOST}" "docker run --rm --network host \
        ${ADD_HOSTS[*]} \
        --entrypoint=sh minio/mc:latest -c '
            mc alias set local http://minio:9000 asap asap-local-only &&
            mc mb --ignore-existing local/asap-gorilla &&
            mc mb --ignore-existing local/asap-gorilla-tsdb &&
            mc anonymous set download local/asap-gorilla &&
            mc anonymous set download local/asap-gorilla-tsdb'" \
        || log "minio bucket setup non-fatal warn — buckets may already exist"

    # Thanos store-gateway — serves TSDB blocks from MinIO via StoreAPI gRPC
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

    # Thanos query — federated PromQL query over store-gateway StoreAPI
    docker_run_on "${NODE2_HOST}" \
        --name asap-thanos-query \
        quay.io/thanos/thanos:v0.41.0 \
        query \
        --grpc-address=0.0.0.0:10911 \
        --http-address=0.0.0.0:10903 \
        --endpoint=thanos-store-gateway:10901 \
        --query.replica-label=replica

    # Thanos compact — background compaction + downsampling of TSDB blocks
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


# ─── GORILLA-GATEWAY on node1 ─────────────────────────────────────────────
# Buffering S3 proxy: receives 10s TSDB blocks from agents, flushes to MinIO
# every 20s. Agents write to gateway:9100; gateway writes to minio:9000.
gateway_up() {
    log node1 gorilla-gateway up
    docker_run_on "${NODE1_HOST}" \
        --name asap-gorilla-gateway \
        -e GATEWAY_LISTEN=0.0.0.0:9100 \
        -e GATEWAY_UPSTREAM_ENDPOINT=minio:9000 \
        -e GATEWAY_ACCESS_KEY=asap \
        -e GATEWAY_SECRET_KEY=asap-local-only \
        -e GATEWAY_FLUSH_INTERVAL=20s \
        asap/gorilla-gateway:dev
}

gateway_down() {
    log node1 gorilla-gateway down
    stop_node "${NODE1_HOST}"
}

# ─── AGENTS + PRODUCERS on node0 and node3 ───────────────────────────────
# Agent config: gorillas3-only, no OpAMP extensions, no controller env vars.
# drop_original: true → agents emit S3 PUTs to MinIO, NO outbound gRPC.
agents_up() {
    log "node0 agent-a up (gorilla-only)"
    docker_run_on "${NODE0_HOST}" \
        --name asap-agent-a \
        --hostname agent-a \
        -e AGENT_ID=agent-a \
        -v /mydata/gorilla-thanos-multinode/configs/agent-gorilla-only.yaml:/etc/otel/config.yaml:ro \
        asap/asap-otel:dev \
        --config=/etc/otel/config.yaml

    log "node3 agent-b up (gorilla-only)"
    docker_run_on "${NODE3_HOST}" \
        --name asap-agent-b \
        --hostname agent-b \
        -e AGENT_ID=agent-b \
        -v /mydata/gorilla-thanos-multinode/configs/agent-gorilla-only.yaml:/etc/otel/config.yaml:ro \
        asap/asap-otel:dev \
        --config=/etc/otel/config.yaml

    # Wait for agent OTLP receiver to come up before starting producers
    sleep 5

    # Producers on node0 → agent-a:4317 (localhost:4317 on node0)
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

    # Producers on node3 → agent-b:4317 (localhost:4317 on node3)
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
    gateway_up
    sleep 2
    agents_up
    log "=== stack up; waiting WARMUP_S=${WARMUP_S}s for gorillas3 to flush first blocks ==="
    sleep "${WARMUP_S}"
}

stack_down() {
    agents_down
    gateway_down
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
  sync     rsync /mydata/gorilla-thanos-multinode/configs to nodes 0, 2, 3
  up       bring up backend (node2) + agents/producers (node0, node3)
  down     stop all asap-* containers on node0, node2, node3
  verify   run verify_gorilla_compression.sh success-metric checks
  all      sync + down + up + soak (${SOAK_S}s) + verify + down

Topology:
  node0 (10.10.1.1)  producers + agent-a (gorilla-only, drop_original: true)
  node1 (10.10.1.2)  gorilla-gateway (S3 proxy: agents → 10s → gateway → 20s → MinIO)
  node2 (10.10.1.3)  MinIO + Thanos (store-gateway, query, compact)
  node3 (10.10.1.4)  producers + agent-b (gorilla-only, drop_original: true)

Network traffic:
  producers → OTLP gRPC :4317 → agent → S3 PUT :9100 → gorilla-gateway → S3 PUT :9000 → MinIO
  NO raw OTLP crosses the network to any backend (drop_original: true)
EOF
        ;;
esac
