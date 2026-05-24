#!/usr/bin/env bash
# run_demo.sh — multi-node MVP demo orchestrator (issue #46).
#
# Drives the same six criteria as deploy/mvp-singlenode/scripts/run_mvp_demo.sh
# (bandwidth, query latency, e2e resource, accuracy, cold-fallback,
# freshness) but distributed across 4 hosts on 10.10.1.0/24:
#
#   node0 (10.10.1.1)  producers + agent-a   (data source)
#   node1 (10.10.1.2)  (unused since #400 — the asap-gateway double-hop
#                       was removed; agents push OTLP straight to backend)
#   node2 (10.10.1.3)  backend stack         (controller, asap-query-backend,
#                                             prometheus, minio,
#                                             thanos-{query,store-gateway,compact})
#   node3 (10.10.1.4)  producers + agent-b   (data source)
#
# Compression-matched bandwidth sweep — SIX arms run back-to-back over
# the same workload. The whole point is that the compression codec
# MATCHES within each aggregation comparison: PRW is Snappy-only, OTLP
# supports none/gzip/zstd (not Snappy), so the matched comparisons use
# OTLP+{none,gzip} on BOTH the raw baseline and the asap arm. PRW/Snappy
# (b2) and serf (b3) are kept as real-world reference points.
#
#   b0         raw   OTLP→VM,  compression none   matched-none baseline
#   b1         raw   OTLP→VM,  compression gzip   matched-gzip baseline (primary)
#   b2         raw   PRW →VM   (Snappy, native)   Prometheus reference
#   b3         raw   serf wire codec → gw → VM    serf-compressed wire ref
#   asap       agg   OTLP→backend, none           matched-none asap
#   asap-gzip  agg   OTLP→backend, gzip           matched-gzip asap (primary)
#
# b3 is serf as a REAL wire codec: the agent serf-XOR-COMPRESSES and
# ships compressed SERF1 blocks to a serf-gateway (node1) that
# DECOMPRESSES and inserts raw into VM (node2). No PRW. The
# serf-compressed wire is the agent→gateway hop (node1 RX). This is
# serf's analog of b1's gzip / b2's Snappy compressed wire.
#
# Two clean apples-to-apples aggregation comparisons (same codec, only
# aggregation differs):
#   none:  b0 vs asap          gzip:  b1 vs asap-gzip   (primary)
# Reading down a codec column shows compression gains; comparing within
# a codec row shows aggregation gains. Backend RX (node2 enp130s0f0) is
# the bandwidth metric.
#
# Scope notes vs. the canonical single-host `run_mvp_demo.sh`:
# - Uses `docker run --network host --add-host` (no docker-compose, no
#   overlay network). The DNS aliases injected via --add-host preserve
#   every existing service-name reference inside the YAML configs
#   (backend:9091, minio:9000, controller:4320, ...).
# - Per-arm bring-up uses subsets of the existing /mydata/ASAPCollector/deploy
#   configs. No config rewriting; only host placement changes.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PKG_DIR="$(dirname "${SCRIPT_DIR}")"
source "${PKG_DIR}/topology.env"

# Derive ROOT/CONFIG_SRC deterministically from this script's location so the
# rsync source in sync_to() is always the configs that ship alongside this
# run_demo.sh — not whatever absolute ROOT topology.env happened to hard-code
# (which is how a stale agent config got synced over the intended one).
# deploy/mvp-multinode → two levels up is the ASAPCollector repo root.
ROOT="$(cd "${PKG_DIR}/../.." && pwd)"
CONFIG_SRC="${PKG_DIR}/configs"

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
        # `data/gorilla-merger` is the merger's tsdb volume mount on node2.
        on "${n}" 'mkdir -p /mydata/mvp-multinode/{configs,scripts,logs,results,data/gorilla-merger}'
        # gorilla-merger runs distroless nonroot (UID 65532); mkdir leaves the
        # dir owned by the ssh user, so 65532 can't write /data/lock → crash-loop.
        # 0777 lets the nonroot UID write without sudo (harmless on the other nodes).
        on "${n}" 'chmod 0777 /mydata/mvp-multinode/data/gorilla-merger'
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

# is_asap_arm — true for the aggregation arms (full backend stack), false
# for the raw baselines (b0/b1/b2/b3, VictoriaMetrics sink). The two asap
# arms (`asap`, `asap-gzip`) are identical at the backend; they differ
# only in the agent's otlp/backend wire codec (none vs gzip), which the
# backend's `.accept_compressed(Gzip)` handles transparently.
is_asap_arm() {
    case "$1" in
        asap|asap-gzip) return 0 ;;
        *)              return 1 ;;
    esac
}

backend_up() {
    local arm=$1
    log "node2 backend up (${arm})"

    # Raw baselines (b0/b1/b2/b3): VictoriaMetrics on node2:8428.
    #   - b0/b1 push OTLP HTTP to /opentelemetry/v1/metrics (compression
    #     none / gzip respectively).
    #   - b2 pushes Prometheus remote_write (Snappy) to /api/v1/write.
    #   - b3 (serf wire codec) ships serf-compressed blocks to the
    #     serf-gateway (node1), which DECOMPRESSES and inserts raw into
    #     VM via OTLP HTTP /opentelemetry/v1/metrics. No PRW on b3.
    # VM serves PromQL on the same port for all four.
    #
    # `-opentelemetry.usePrometheusNaming` is LEFT OFF so VM stores the
    # OTLP metric names verbatim (`http_requests_total`,
    # `http_requests_total_latency_ms`) — matching the replay query suite
    # (queries-e2e.json). With the flag ON, VM would sanitize names and
    # append the gauge's `ms` unit suffix
    # (→ `http_requests_total_latency_ms_milliseconds`), breaking the
    # quantile queries. The PRW arms (b2/b3) already pin
    # `add_metric_suffixes: false` agent-side for the same reason; the
    # OTLP arms (b0/b1) rely on VM's default (no Prometheus naming) to get
    # the verbatim names. If a future VM bump changes the default OTLP
    # naming, pin `-opentelemetry.usePrometheusNaming=false` here.
    #
    # ASAP arms: still use Prometheus for self-telemetry scraping
    # (backend/agent self-metrics).
    if ! is_asap_arm "${arm}"; then
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

    if is_asap_arm "${arm}"; then
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

        # gorilla-merger — Thanos-Receive-style pending-window component.
        #
        # Brought up BEFORE thanos-query so its StoreAPI is listening when
        # query starts fanning out. On node2's --network host, the merger
        # binds its two default ports directly:
        #   HTTP  :10908  /ingest/gorilla (edge fragment cold-ship, issue #24)
        #                 + /metrics + /-/healthy + /-/ready
        #   gRPC  :10907  Thanos StoreAPI (the <2h pending query surface)
        # These don't collide with the three thanos containers (10901–10905).
        #
        # The shipper uploads completed 2h blocks to the SAME bucket the
        # store-gateway watches (configs/shared/thanos-objstore.yaml →
        # asap-gorilla-tsdb), so query unions: merger StoreAPI (recent <2h)
        # + store-gateway (>=2h S3) with no double-count (the merger drops
        # its local copy once shipped). `cluster=asap-mvp` is the merger's
        # distinguishing external label (applied to every series + block).
        docker_run_on "${NODE2_HOST}" \
            --name asap-gorilla-merger \
            -v /mydata/mvp-multinode/configs/shared/thanos-objstore.yaml:/etc/thanos/objstore.yaml:ro \
            -v /mydata/mvp-multinode/data/gorilla-merger:/data \
            asap/gorilla-merger:dev \
            --http-address=0.0.0.0:10908 \
            --grpc-address=0.0.0.0:10907 \
            --tsdb.path=/data \
            --objstore.config-file=/etc/thanos/objstore.yaml \
            --external-labels=cluster=asap-mvp,merger=m1

        # Thanos query
        #
        # All containers on node2 run with --network host, so the default
        # thanos gRPC port (10901) collides with thanos-store-gateway, and
        # 10902 collides with the store-gateway HTTP port. Pin query's gRPC
        # listener to :10905 so all three thanos containers coexist on the
        # same host. The data_plane (asap-backend) only talks to thanos-query
        # over HTTP :10903; the gRPC port is just thanos-query's own control
        # surface and isn't exposed.
        #
        # Issue #32: register the gorilla-merger StoreAPI (10907) as a second
        # --endpoint so query fans out to BOTH the merger (recent <2h pending
        # window) and the store-gateway (>=2h S3 blocks) and unions the
        # results. Without this, query only sees shipped S3 blocks and the
        # most-recent <2h of merger-ingested data is invisible.
        docker_run_on "${NODE2_HOST}" \
            --name asap-thanos-query \
            quay.io/thanos/thanos:v0.41.0 \
            query \
            --http-address=0.0.0.0:10903 \
            --grpc-address=0.0.0.0:10905 \
            --endpoint=thanos-store-gateway:10901 \
            --endpoint=gorilla-merger:10907 \
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

# ─── SERF-GATEWAY on node1 (b3 arm only) ────────────────────────────
#
# The decompression half of the serf-as-real-wire-codec arm. The b3
# agents serf-XOR-COMPRESS the metric stream and POST SERF1 blocks to
# `http://serf-gw:9000/serf` (serf-gw → node1 via ADD_HOSTS). This
# gateway runs the asap-otel binary with a serfreceiver pipeline that
# DECODES the blocks back to raw points and inserts them into VM (node2)
# over OTLP HTTP. The only compressed hop is agent(node0/3) → gw(node1);
# the gw → VM hop carries raw decompressed OTLP. So the serf-compressed
# wire == node1 NIC RX == the serfexporter's bytes_sent counter.
#
# Brought up BEFORE the b3 agents so the serfreceiver is already
# listening when the agents' first window flushes. node1 is otherwise
# idle (the asap-gateway double-hop was retired in #400), and arm_down's
# `stop_node node1` reaps this container at teardown.
serf_gateway_up() {
    log "node1 serf-gateway up (b3 serf wire codec)"
    docker_run_on "${NODE1_HOST}" \
        --name asap-serf-gateway \
        --hostname serf-gw \
        -v /mydata/mvp-multinode/configs/b3/serf-gateway.yaml:/etc/otel/config.yaml:ro \
        asap/asap-otel:dev \
        --config=/etc/otel/config.yaml
}

# ─── AGENTS + PRODUCERS on node0 and node3 ──────────────────────────
agents_up() {
    local arm=$1
    local agent_cfg
    # Compression-matched bandwidth sweep — six arms. The codec MUST
    # match within each aggregation comparison (none: b0/asap; gzip:
    # b1/asap-gzip). PRW/Snappy (b2) and serf (b3) are real-world
    # reference points, NOT matched pairs.
    case "${arm}" in
        b0)        agent_cfg=b0/asap-otel-agent-b0-otlp-none.yaml ;;        # raw OTLP→VM, compression none  (matched-none baseline)
        b1)        agent_cfg=b1/asap-otel-agent-b1-otlp-gzip.yaml ;;        # raw OTLP→VM, compression gzip  (matched-gzip baseline)
        b2)        agent_cfg=b2/asap-otel-agent-b2-prw-snappy.yaml ;;       # raw PRW→VM   (Snappy, native)  (Prometheus ref)
        b3)        agent_cfg=b3/asap-otel-agent-b3-serf.yaml ;;             # serf wire codec → gw → VM      (serf-compressed wire ref)
        asap)      agent_cfg=asap/asap-otel-agent-b6-asap-single-sketch.yaml ;;  # edge-agg, OTLP→backend none  (matched-none asap)
        asap-gzip) agent_cfg=asap-gzip/asap-otel-agent-asap-gzip.yaml ;;    # edge-agg, OTLP→backend gzip   (matched-gzip asap)
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
            -e EXPORTER_SEED=${EXPORTER_SEED:-42} \
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
            -e EXPORTER_SEED=${EXPORTER_SEED:-42} \
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
    sleep 5
    # b3 serf wire codec: bring up the serf-gateway (node1) AFTER the VM
    # backend (node2) is up but BEFORE the agents, so the serfreceiver is
    # listening when the agents' first compressed window flushes.
    if [[ "${arm}" == "b3" ]]; then
        serf_gateway_up
        sleep 3
    fi
    agents_up "${arm}"
    log "=== arm ${arm} all containers started; waiting WARMUP_S=${WARMUP_S} ==="
    sleep "${WARMUP_S}"
}

# Issue #400 (post-removal): the asap-gateway node1 hop is gone, but
# `stop_node node1` is kept in arm_down() so any leftover container
# from a previous deploy (e.g. an interactive `docker run` an operator
# left behind, or a container still alive from before this fix
# landed) is reaped on every arm cycle. The wave's bandwidth budget
# depends on node1 being idle — leaving stray gateway containers
# alive on node1 would silently re-establish the double-hop.
arm_down() {
    log "=== ARM DOWN ==="
    agents_down
    stop_node "${NODE1_HOST}"
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

    # PromQL replay endpoint:
    #   asap arm  → asap-query-backend on node2:9091
    #   b0 / b1   → VictoriaMetrics on node2:8428 (serves PromQL on the
    #               same port as its /api/v1/write PRW receive). Prior to
    #               2026-05 this pointed at :9090 (Prometheus), but the
    #               b0/b1 backend_up() path brings up `asap-victoriametrics`
    #               not Prometheus, so :9090 was unreachable → 100% timeout.
    local query_endpoint
    if is_asap_arm "${arm}"; then
        query_endpoint="http://${NODE2_IP}:9091"
    else
        query_endpoint="http://${NODE2_IP}:8428"
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
        # Compression-matched bandwidth sweep — six arms. The two clean
        # apples-to-apples aggregation comparisons (same codec, only
        # aggregation differs):
        #   none: b0 vs asap        gzip: b1 vs asap-gzip  (primary)
        # b2 (PRW/Snappy) + b3 (serf) are real-world reference points.
        for arm in b0 b1 b2 b3 asap asap-gzip; do
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
  up <arm>            bring up containers for an arm
                      arms: b0 b1 b2 b3 asap asap-gzip
                        b0        raw OTLP→VM,  compression none  (matched-none baseline)
                        b1        raw OTLP→VM,  compression gzip  (matched-gzip baseline)
                        b2        raw PRW→VM    (Snappy, native)  (Prometheus ref)
                        b3        serf wire codec → gw → VM       (serf-compressed wire ref)
                        asap      edge-agg, OTLP→backend none     (matched-none asap)
                        asap-gzip edge-agg, OTLP→backend gzip     (matched-gzip asap)
  down                stop and remove all asap-* containers cluster-wide
  arm <arm>           full single-arm lifecycle: up → measure → down
  all                 sync + run all 6 arms back-to-back + generate report
EOF
        ;;
esac
