#!/usr/bin/env bash
# run_demo.sh — multi-node MVP demo orchestrator (issue #46).
#
# Drives the same six criteria as deploy/mvp-singlenode/scripts/run_mvp_demo.sh
# (bandwidth, query latency, e2e resource, accuracy, cold-fallback,
# freshness) but distributed across 4 hosts on 10.10.1.0/24:
#
#   node0 (10.10.1.1)  producers + agent-a   (data source)
#   node1 (10.10.1.2)  gateway               (ASAP arm only)
#   node2 (10.10.1.3)  backend stack         (controller, asap-query-backend,
#                                             prometheus, minio,
#                                             thanos-{query,store-gateway,compact})
#   node3 (10.10.1.4)  producers + agent-b   (data source)
#
# Three arms run back-to-back over the same workload:
#   b0     OTel agent → Prometheus (PRW)              [no gateway, no backend]
#   b1     OTel agent + serfprocessor → Prometheus    [no gateway, no backend]
#   asap   OTel agent → gateway → asap-query-backend  [+ controller,
#          + Thanos/MinIO archive, sketches per controller plan]
#
# Scope notes vs. the canonical single-host `run_mvp_demo.sh`:
# - Uses `docker run --network host --add-host` (no docker-compose, no
#   overlay network). The DNS aliases injected via --add-host preserve
#   every existing service-name reference inside the YAML configs
#   (gateway:4317, backend:9091, minio:9000, controller:4320, ...).
# - Per-arm bring-up uses subsets of the existing /mydata/ASAPCollector/deploy
#   configs. No config rewriting; only host placement changes.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PKG_DIR="$(dirname "${SCRIPT_DIR}")"
source "${PKG_DIR}/topology.env"

RUN_ID="${RUN_ID:-mvp-multinode-$(date +%Y%m%d-%H%M%S)}"
RUN_DIR="${RUN_BASE}/${RUN_ID}"
mkdir -p "${RUN_DIR}" "${LOG_BASE}"

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" | tee -a "${LOG_BASE}/${RUN_ID}.log" >&2; }
die() { log "FATAL: $*"; exit 1; }

# ── ssh wrapper that runs commands on a remote node with our env ──
on() {
    local node=$1; shift
    # Wrap with timeout so a stuck ssh channel can't pause the orchestrator
    # indefinitely. SSH itself sometimes wedges its stdout pipe after
    # `docker run -d` returns the container id (race during channel close
    # under load). 30s is generous for any docker run on a 10Gbps LAN.
    timeout --kill-after=5 30 \
        ssh -n -o ConnectTimeout=5 -o StrictHostKeyChecking=no -o BatchMode=yes \
            -o ServerAliveInterval=5 -o ServerAliveCountMax=2 \
            "${node}" "$@" < /dev/null
}

# ── stop + remove all asap-* containers on a node ──
stop_node() {
    local node=$1
    on "${node}" 'docker ps -a --format "{{.Names}}" | grep "^asap-" | xargs -r docker rm -f' || true
}

# ── stage configs/scripts to a remote node under /mydata ──
sync_to() {
    local node=$1
    log "rsync configs → ${node}"
    rsync -a --delete \
        "${CONFIG_SRC}/" \
        "${node}:/mydata/mvp-multinode/configs/"
    rsync -a "${ROOT}/deploy/mvp-singlenode/scripts/" "${node}:/mydata/mvp-multinode/scripts/"
    rsync -a "${PKG_DIR}/topology.env" "${node}:/mydata/mvp-multinode/topology.env"
}

sync_all_nodes() {
    for n in "${NODE0_HOST}" "${NODE1_HOST}" "${NODE2_HOST}" "${NODE3_HOST}"; do
        on "${n}" 'mkdir -p /mydata/mvp-multinode/{configs,scripts,logs,results}'
    done
    sync_to "${NODE0_HOST}"
    sync_to "${NODE1_HOST}"
    sync_to "${NODE2_HOST}"
    sync_to "${NODE3_HOST}"
}

# ── docker_run wrapper that always uses host network + DNS aliases ──
docker_run_on() {
    local node=$1; shift
    on "${node}" "docker run -d --restart unless-stopped --network host \
        ${ADD_HOSTS[*]} \
        $*"
}

# ─── BACKEND STACK on node2 ────────────────────────────────────────
#
# Common services across all arms: prometheus, minio (b0/b1 don't use
# minio but it's harmless idle), and the controller. ASAP arm adds
# backend + thanos-{query,store-gateway,compact}.

backend_up() {
    local arm=$1
    log "node2 backend up (${arm})"

    # B0/B1: VictoriaMetrics on node2:8428 — accepts Prometheus
    # remote_write at /api/v1/write and serves PromQL on the same
    # port. No mounted config; CLI flags only.
    # ASAP arm: still uses Prometheus for self-telemetry scraping
    # (gateway/backend self-metrics).
    if [ "${arm}" != "asap" ]; then
        docker_run_on "${NODE2_HOST}" \
            --name asap-victoriametrics \
            victoriametrics/victoria-metrics:v1.110.0 \
            --httpListenAddr=:8428 \
            --retentionPeriod=24h
    else
        local prom_args=(
            "--config.file=/etc/prometheus/prometheus.yml"
            "--storage.tsdb.retention.time=24h"
            "--web.enable-lifecycle"
        )
        docker_run_on "${NODE2_HOST}" \
            --name asap-prometheus \
            -v /mydata/mvp-multinode/configs/shared/prometheus-with-remote-write.yml:/etc/prometheus/prometheus.yml:ro \
            prom/prometheus:v2.55.0 "${prom_args[@]}"
    fi

    if [ "${arm}" = "asap" ]; then
        # MinIO + bucket setup
        docker_run_on "${NODE2_HOST}" \
            --name asap-minio \
            -e MINIO_ROOT_USER=asap \
            -e MINIO_ROOT_PASSWORD=asap-local-only \
            minio/minio:latest server /data --console-address :9001

        # Wait for minio to be ready then create buckets
        sleep 5
        # mc image's default entrypoint is `mc`, not `sh` — must use
        # --entrypoint=sh, otherwise the bucket-create step is silently
        # rejected with "sh is not a recognized command" and gorillas3
        # writes vanish into a non-existent bucket.
        on "${NODE2_HOST}" "docker run --rm --network host \
            ${ADD_HOSTS[*]} \
            --entrypoint=sh minio/mc:latest -c '
                mc alias set asap http://minio:9000 asap asap-local-only &&
                mc mb --ignore-existing asap/asap-gorilla &&
                mc mb --ignore-existing asap/asap-gorilla-tsdb &&
                mc mb --ignore-existing asap/raw &&
                mc anonymous set download asap/asap-gorilla &&
                mc anonymous set download asap/asap-gorilla-tsdb &&
                mc anonymous set download asap/raw'" || log "minio bucket setup non-fatal warn"

        # Thanos store-gateway
        docker_run_on "${NODE2_HOST}" \
            --name asap-thanos-store-gateway \
            --user 0 \
            -v /mydata/mvp-multinode/configs/shared/thanos-objstore.yaml:/etc/thanos/objstore.yaml:ro \
            quay.io/thanos/thanos:v0.41.0 \
            store \
            --objstore.config-file=/etc/thanos/objstore.yaml \
            --http-address=0.0.0.0:10902 \
            --grpc-address=0.0.0.0:10901 \
            --data-dir=/tmp/thanos-store \
            --sync-block-duration=30s \
            --block-sync-concurrency=20

        # Thanos query
        docker_run_on "${NODE2_HOST}" \
            --name asap-thanos-query \
            quay.io/thanos/thanos:v0.41.0 \
            query \
            --http-address=0.0.0.0:10903 \
            --endpoint=thanos-store-gateway:10901 \
            --query.replica-label=replica

        # Thanos compact
        docker_run_on "${NODE2_HOST}" \
            --name asap-thanos-compact \
            --user 0 \
            -v /mydata/mvp-multinode/configs/shared/thanos-objstore.yaml:/etc/thanos/objstore.yaml:ro \
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

        # Phase-9 single-binary refactor (2026-05): the controller and
        # backend ship from the same `asap/query-backend:dev` image but
        # run as TWO separate processes (entrypoints
        # `/usr/local/bin/controller` vs `/usr/local/bin/asap-query-backend`).
        # The single-node `deploy/mvp-singlenode/docker-compose/base.yml`
        # models them as two compose services; mirror that here.
        #
        # Without the controller container the backend never receives
        # the controller's POST /api/v1/streaming-config and falls back
        # to the static `configs/asap/backend-streaming.yaml`
        # (DDSketch-only, no Sum/Count/Topk roles). The post-#290 wave
        # — sum-by-zone (#291), rate-over-Sum and topk-over-rate (#292)
        # — only fires when the controller-driven plan installs the Sum
        # aggregation, so this container is load-bearing for ASAP-arm
        # validation.
        #
        # The `controller` DNS alias resolves to NODE2_IP via ADD_HOSTS;
        # both the agents and the controller-self-reference (the OpAMP
        # endpoint URL the controller bakes into emitted agent yaml) use
        # that name.

        # asap-query-backend (ASAP only) — data plane process.
        sleep 3
        docker_run_on "${NODE2_HOST}" \
            --name asap-backend \
            -e RUST_LOG=info \
            -e ASAP_SKETCH_FAMILY=ddsketch \
            -e ASAP_GORILLA_S3_ENDPOINT=http://minio:9000 \
            -e ASAP_GORILLA_S3_BUCKET=asap-gorilla \
            -e ASAP_GORILLA_S3_REGION=us-east-1 \
            -e ASAP_GORILLA_S3_ACCESS_KEY_ID=asap \
            -e ASAP_GORILLA_S3_SECRET_ACCESS_KEY=asap-local-only \
            -e ASAP_GORILLA_S3_TENANT=default \
            -e ASAP_GORILLA_S3_USE_SSL=false \
            -e 'ASAP_GORILLA_S3_PREFIX_TEMPLATE={tenant}/{metric}/{YYYY}/{MM}/{DD}/{HH}/' \
            -e ASAP_BACKEND_STORAGE_ROUTING=/etc/asap/backend-storage-routing.yaml \
            -e ASAP_THANOS_QUERY_URL=http://thanos-query:10903 \
            -v /mydata/mvp-multinode/configs/asap/backend-streaming.yaml:/etc/asap/streaming.yaml:ro \
            -v /mydata/mvp-multinode/configs/asap/backend-storage-routing.yaml:/etc/asap/backend-storage-routing.yaml:ro \
            asap/query-backend:dev \
            --streaming-config=/etc/asap/streaming.yaml \
            --query-port=9091 \
            --enable-otel-ingest \
            --otel-grpc-port=4317 \
            --otel-http-port=4318

        # asap-controller (ASAP only) — control plane process. Brought
        # up AFTER the backend so the controller's startup pre-pop
        # replan tick has a live backend to POST the streaming-config
        # plan to (CONTROLLER_BACKEND_ENDPOINT). Without that POST the
        # backend stays on the static DDSketch-only fallback and the
        # wave's Sum/Topk queries silently return empty.
        sleep 3
        docker_run_on "${NODE2_HOST}" \
            --name asap-controller \
            -e RUST_LOG="info,controller=debug,control_plane=debug" \
            -e USE_TYPED_STAGE_SPLIT=1 \
            -e CONTROLLER_ADDR=0.0.0.0:8080 \
            -e CONTROLLER_OPAMP_ADDR=0.0.0.0:4320 \
            -e CONTROLLER_GRPC_ADDR=0.0.0.0:4321 \
            -e CONTROLLER_OPAMP_ENDPOINT=ws://controller:4320/v1/opamp \
            -e CONTROLLER_BACKEND_ENDPOINT=http://backend:9091/api/v1/streaming-config \
            -e CONTROLLER_WORKLOADS=/etc/asap/mvp-workload.yaml \
            -v /mydata/mvp-multinode/configs/asap/mvp-workload.yaml:/etc/asap/mvp-workload.yaml:ro \
            --entrypoint /usr/local/bin/controller \
            asap/query-backend:dev
    fi
}

backend_down() {
    log "node2 backend down"
    stop_node "${NODE2_HOST}"
}

# ─── GATEWAY on node1 (ASAP arm only) ───────────────────────────────
gateway_up() {
    local arm=$1
    if [ "${arm}" != "asap" ]; then
        log "node1 gateway skipped (arm=${arm})"
        return 0
    fi
    log "node1 gateway up"
    docker_run_on "${NODE1_HOST}" \
        --name asap-gateway \
        -v /mydata/mvp-multinode/configs/asap/asap-otel-gateway-mvp-placeholder.yaml:/etc/otel/config.yaml:ro \
        asap/asap-otel:dev \
        --config=/etc/otel/config.yaml
}

gateway_down() {
    log "node1 gateway down"
    stop_node "${NODE1_HOST}"
}

# ─── AGENTS + PRODUCERS on node0 and node3 ──────────────────────────
agents_up() {
    local arm=$1
    local agent_cfg
    case "${arm}" in
        b0)   agent_cfg=b0/asap-otel-agent-b0-prometheus.yaml ;;
        b1)   agent_cfg=b1/asap-otel-agent-b1-serf-prometheus.yaml ;;
        asap) agent_cfg=asap/asap-otel-agent-b6-asap-single-sketch.yaml ;;
        *) die "unknown arm ${arm}" ;;
    esac

    # node0 → agent-a (binds 0.0.0.0:4317 on node0). Producers on node0
    # send to localhost:4317 == agent-a:4317.
    log "node0 agent-a up (${arm})"
    docker_run_on "${NODE0_HOST}" \
        --name asap-agent-a \
        --hostname agent-a \
        -e AGENT_ID=agent-a \
        -e CONTROLLER_OPAMP_URL=ws://controller:4320/v1/opamp \
        -e ASAP_SKETCH_FAMILY=ddsketch \
        -v /mydata/mvp-multinode/configs/${agent_cfg}:/etc/otel/config.yaml:ro \
        asap/asap-otel:dev \
        --config=/etc/otel/config.yaml

    # Same on node3 → agent-b
    log "node3 agent-b up (${arm})"
    docker_run_on "${NODE3_HOST}" \
        --name asap-agent-b \
        --hostname agent-b \
        -e AGENT_ID=agent-b \
        -e CONTROLLER_OPAMP_URL=ws://controller:4320/v1/opamp \
        -e ASAP_SKETCH_FAMILY=ddsketch \
        -v /mydata/mvp-multinode/configs/${agent_cfg}:/etc/otel/config.yaml:ro \
        asap/asap-otel:dev \
        --config=/etc/otel/config.yaml

    # Wait for agent OTLP receiver to come up
    sleep 5

    # Producers on node0 → agent-a:4317 (== 10.10.1.1:4317 == localhost:4317)
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
            asap/fake-exporter:dev
    done
}

agents_down() {
    log "node0 + node3 agents/producers down"
    stop_node "${NODE0_HOST}"
    stop_node "${NODE3_HOST}"
}

# ─── ARM lifecycle ──────────────────────────────────────────────────
arm_up() {
    local arm=$1
    log "=== ARM UP: ${arm} ==="
    backend_up "${arm}"
    gateway_up "${arm}"
    sleep 5
    agents_up "${arm}"
    log "=== arm ${arm} all containers started; waiting WARMUP_S=${WARMUP_S} ==="
    sleep "${WARMUP_S}"
}

arm_down() {
    log "=== ARM DOWN ==="
    agents_down
    gateway_down
    backend_down
}

# ─── Measurements (per-edge bandwidth, per-stage stats, replay) ─────
arm_measure() {
    local arm=$1
    local out="${RUN_DIR}/${arm}"
    mkdir -p "${out}"

    log "[measure ${arm}] starting per-node docker stats sampling for ${SOAK_S}s"
    # On each node, sample container stats and capture to local file.
    for n in "${NODE0_HOST}" "${NODE1_HOST}" "${NODE2_HOST}" "${NODE3_HOST}"; do
        on "${n}" "python3 /mydata/mvp-multinode/scripts/measure_per_edge_bandwidth.py \
            --duration ${SOAK_S} --period 1.0 \
            --out /mydata/mvp-multinode/results/edge-${n}.csv \
            > /mydata/mvp-multinode/results/edge-${n}.log 2>&1 &"
        on "${n}" "python3 /mydata/mvp-multinode/scripts/measure_stages.py \
            --baseline ${arm} --duration ${SOAK_S} \
            --out /mydata/mvp-multinode/results/stages-${n}.csv \
            > /mydata/mvp-multinode/results/stages-${n}.log 2>&1 &"
    done

    # PromQL replay: for b0/b1 target prometheus on node2:9090; for asap target backend on node2:9091
    local query_endpoint
    if [ "${arm}" = "asap" ]; then
        query_endpoint="http://${NODE2_IP}:9091"
    else
        query_endpoint="http://${NODE2_IP}:9090"
    fi

    log "[measure ${arm}] MetricsQL replay against ${query_endpoint} for ${SOAK_S}s"
    python3 "${ROOT}/deploy/mvp-singlenode/scripts/metricsql_replay.py" \
        --target "${query_endpoint}" \
        --queries "${ROOT}/deploy/mvp-singlenode/scripts/queries-e2e.json" \
        --duration "${SOAK_S}" \
        --out "${out}/replay.jsonl" \
        > "${out}/replay.log" 2>&1 &
    local REPLAY_PID=$!

    # Wait for soak to complete
    wait ${REPLAY_PID} 2>/dev/null || true
    sleep 3   # let measurement scripts on remote nodes finish

    # Pull CSVs back from each node
    for n in "${NODE0_HOST}" "${NODE1_HOST}" "${NODE2_HOST}" "${NODE3_HOST}"; do
        scp "${n}:/mydata/mvp-multinode/results/edge-${n}.csv"     "${out}/edge-${n}.csv"     2>/dev/null || true
        scp "${n}:/mydata/mvp-multinode/results/stages-${n}.csv"   "${out}/stages-${n}.csv"   2>/dev/null || true
    done

    log "[measure ${arm}] done; outputs in ${out}/"
}

# ─── high-level orchestration ───────────────────────────────────────
run_arm() {
    local arm=$1
    arm_down || true
    sleep 2
    arm_up "${arm}"
    arm_measure "${arm}"
    arm_down || true
}

cmd=${1:-help}
case "${cmd}" in
    sync)            sync_all_nodes ;;
    up)              arm_up "${2:?need arm name}" ;;
    down)            arm_down ;;
    measure)         arm_measure "${2:?need arm name}" ;;
    arm)             run_arm "${2:?need arm name}" ;;
    all)
        sync_all_nodes
        for arm in b0 b1 asap; do
            run_arm "${arm}"
        done
        log "=== generating MVP_REPORT.md ==="
        python3 "${SCRIPT_DIR}/aggregate_report.py" --run-dir "${RUN_DIR}" --out "${RUN_DIR}/MVP_REPORT.md" \
            || log "report aggregation failed (non-fatal); inspect ${RUN_DIR}/"
        log "=== run complete: ${RUN_DIR} ==="
        ;;
    help|*)
        cat <<EOF
usage: $0 <cmd> [arm]
  sync                rsync /mydata/ASAPCollector configs+scripts to all 4 nodes
  up <arm>            bring up containers for an arm (b0|b1|asap)
  down                stop and remove all asap-* containers cluster-wide
  arm <arm>           full single-arm lifecycle: up → measure → down
  all                 sync + run all 3 arms back-to-back + generate report
EOF
        ;;
esac
