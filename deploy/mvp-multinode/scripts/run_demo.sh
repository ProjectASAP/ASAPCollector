#!/usr/bin/env bash
# run_demo.sh — multi-node MVP demo orchestrator (issue #46).
#
# Drives the six issue-#46 MVP acceptance criteria.
# (bandwidth, query latency, e2e resource, accuracy, cold-fallback,
# freshness) but distributed across 4 hosts on 10.10.1.0/24:
#
#   node0 (10.10.1.1)  producers + agent-a   (data source)
#   node1 (10.10.1.2)  (unused since #400 — the asap-gateway double-hop
#                       was removed; agents push OTLP straight to backend)
#   node2 (10.10.1.3)  backend stack         (control_plane, data_plane,
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
#   (data-plane:9091, minio:9000, control-plane:4320, ...).
# - Per-arm bring-up uses subsets of the existing /mydata/ASAPCollector/deploy
#   configs. No config rewriting; only host placement changes.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PKG_DIR="$(dirname "${SCRIPT_DIR}")"
# TOPOLOGY_ENV lets a caller point at an alternate topology file (e.g. the
# 8-node scaling cluster) without editing the committed 4-node default.
TOPOLOGY_ENV="${TOPOLOGY_ENV:-${PKG_DIR}/harness/topology/4node.env}"
source "${TOPOLOGY_ENV}"

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

write_run_manifest() {
    # Immutable paired-run provenance. Delayed until `all` so help/library
    # invocations do not create misleading run directories or require the
    # sibling backend checkout.
    ROOT="${ROOT}" BACKEND="${BACKEND}" RUN_ID="${RUN_ID}" RUN_DIR="${RUN_DIR}" \
    PER_AGENT_CARDINALITY="${PER_AGENT_CARDINALITY}" OTELAPP_FREQ_HZ="${OTELAPP_FREQ_HZ}" \
    SOAK_S="${SOAK_S}" OTELAPP_SEED="${OTELAPP_SEED:-42}" \
    python3 -c 'import datetime,json,os,subprocess; root=os.environ["ROOT"]; backend=os.environ["BACKEND"]; rid=os.environ["RUN_ID"]; seed=int(os.environ["OTELAPP_SEED"]); commit=lambda p: subprocess.check_output(["git","-C",p,"rev-parse","HEAD"],text=True).strip(); json.dump({"schema_version":1,"run_id":rid,"started_at":datetime.datetime.now(datetime.timezone.utc).isoformat(),"collector_commit":commit(root),"backend_commit":commit(backend),"time_alignment":{"mode":"relative_logical_sequence","contract":"same seed, query id, per-query sequence and bounded elapsed-time skew"},"workload":{"cardinality":int(os.environ["PER_AGENT_CARDINALITY"]),"frequency_hz":float(os.environ["OTELAPP_FREQ_HZ"]),"soak_s":float(os.environ["SOAK_S"])},"arms":{"b1":{"seed":seed},"asap-gzip":{"seed":seed}}},open(os.path.join(os.environ["RUN_DIR"],"run-manifest.json"),"w"),indent=2)'
    cp "${PKG_DIR}/harness/acceptance.json" "${RUN_DIR}/acceptance.json"
    cp "${PKG_DIR}/harness/queries/e2e.json" "${RUN_DIR}/queries-e2e.json"
}

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

# ── build all locally-built images from CURRENT source, then ship them ──
#
# Source changes constantly during development, so every deploy rebuilds the
# images from source and re-loads them onto the nodes — no more stale-image
# version skew. External images (minio/thanos/prometheus/victoria-metrics) are
# NOT built here; each node pulls those itself. Set SKIP_BUILD=1 / SKIP_LOAD=1
# to reuse what's already present (e.g. iterating on config only).
SKETCHLIB_GO="${SKETCHLIB_GO:-/mydata/sketchlib-go}"

build_images() {
    log "build_images: rebuilding all local images from current source"
    # shellcheck disable=SC1091
    source "${HOME}/.cargo/env" 2>/dev/null || true
    export PATH="${PATH}:/usr/local/go/bin:${HOME}/go/bin"

    # Build from the repos AS THEY ARE ON DISK — this compiles your local edits
    # and NEVER pulls or checks out for you (so it can't clobber uncommitted
    # work). Print exactly what's being built, and fail loudly on a stale /
    # incomplete checkout instead of silently shipping the wrong code (e.g. an
    # ASAPQuery-backend tree that predates control_plane/Dockerfile would
    # otherwise build a data-plane WITHOUT the latest fixes).
    log "  ROOT (ASAPCollector)     = ${ROOT} @ $(git -C "${ROOT}" log -1 --format='%h %s' 2>/dev/null || echo 'non-git')"
    log "  BACKEND (ASAPQuery-back) = ${BACKEND} @ $(git -C "${BACKEND}" log -1 --format='%h %s' 2>/dev/null || echo 'non-git')"
    local missing=0 req
    for req in "${BACKEND}/data_plane/Dockerfile" "${BACKEND}/control_plane/Dockerfile" "${BACKEND}/gorilla-merger" \
               "${ROOT}/build_asap_otel.sh" "${ROOT}/build_opamp_supervisor.sh" \
               "${ROOT}/deploy/docker/Dockerfile.asap-otel" "${ROOT}/deploy/docker/Dockerfile.asap-otel-supervised" \
               "${ROOT}/deploy/docker/Dockerfile.otel-app" \
               "${ROOT}/asap-precompute-rs" "${ROOT}/asap-gorilla-rust" "${ROOT}/asap-gorilla-go" \
               "${SKETCHLIB}" "${SKETCHLIB_GO}"; do
        [ -e "${req}" ] || { log "  MISSING build input: ${req}"; missing=1; }
    done
    [ "${missing}" = 1 ] && die "build_images: required build inputs missing — is ${BACKEND} at the intended commit? (stale/incomplete checkout; pull or point BACKEND= at a current tree)"

    # ── ASAPQuery-backend: split WARM engine (data_plane + control_plane) ──
    log "  → asap/data-plane:dev"
    DOCKER_BUILDKIT=1 docker build -f "${BACKEND}/data_plane/Dockerfile" \
        --build-context asap-precompute-rs="${ROOT}/asap-precompute-rs" \
        --build-context asap-sketchlib="${SKETCHLIB}" \
        --build-context asap-gorilla-rust="${ROOT}/asap-gorilla-rust" \
        -t asap/data-plane:dev "${BACKEND}"
    log "  → asap/control-plane:dev"
    DOCKER_BUILDKIT=1 docker build -f "${BACKEND}/control_plane/Dockerfile" \
        --build-context asap-precompute-rs="${ROOT}/asap-precompute-rs" \
        --build-context asap-sketchlib="${SKETCHLIB}" \
        --build-context asap-gorilla-rust="${ROOT}/asap-gorilla-rust" \
        -t asap/control-plane:dev "${BACKEND}"

    # ── ASAPCollector: agent collector + its opamp-supervisor wrapper ──
    # build_asap_otel.sh injects the local sketchlib-go / asap-precompute-go /
    # asap-gorilla-go replace directives and produces the OCB binary, so #432
    # (and any asap-gorilla-go edit) is picked up WITHOUT a published tag.
    log "  → asap-otel binary + asap/asap-otel:dev"
    GONOSUMCHECK="github.com/ProjectASAP/*" GOPRIVATE="github.com/ProjectASAP/*" \
        GONOSUMDB="github.com/ProjectASAP/*" bash "${ROOT}/build_asap_otel.sh"
    docker build -f "${ROOT}/deploy/docker/Dockerfile.asap-otel" -t asap/asap-otel:dev "${ROOT}"
    log "  → opamp-supervisor + asap/asap-otel-supervised:dev"
    bash "${ROOT}/build_opamp_supervisor.sh"
    docker build -f "${ROOT}/deploy/docker/Dockerfile.asap-otel-supervised" \
        -t asap/asap-otel-supervised:dev "${ROOT}"

    # ── otel-app (producers) ──
    log "  → asap/otel-app:dev"
    DOCKER_BUILDKIT=1 docker build -f "${ROOT}/deploy/docker/Dockerfile.otel-app" \
        --build-context sketchlib-go="${SKETCHLIB_GO}" \
        --build-context asap-precompute-go="${ROOT}/asap-precompute-go" \
        -t asap/otel-app:dev "${ROOT}"

    # ── gorilla-merger (cold sink) ──
    # The merger imports asap-gorilla-go AND its intchunk subpackage (the cold
    # value-chunk codec behind the decode-on-read helper). intchunk is NOT in
    # any published asap-gorilla-go tag, so — exactly like data-plane's sibling
    # path-deps and build_asap_otel.sh's asap-gorilla-go replace — we hand the
    # in-repo monorepo checkout to the build as the `asap-gorilla-go`
    # build-context; the Dockerfile rewrites the go.mod replace to point at it.
    # A gh_token secret is still mounted so any OTHER private fetch keeps working
    # (the Dockerfile uses it only when present; asap-gorilla-go itself is now
    # local source and needs no token).
    log "  → asap/gorilla-merger:dev"
    local gh_token_file="${GH_TOKEN_FILE:-}" cleanup_token=0
    if [ -z "${gh_token_file}" ]; then
        gh_token_file="$(mktemp)"; cleanup_token=1
        python3 -c "import yaml; d=yaml.safe_load(open('${HOME}/.config/gh/hosts.yml')); print(d['github.com'].get('oauth_token') or d['github.com'].get('token'), end='')" > "${gh_token_file}"
    fi
    DOCKER_BUILDKIT=1 docker build --secret id=gh_token,src="${gh_token_file}" \
        --build-context asap-gorilla-go="${ROOT}/asap-gorilla-go" \
        -t asap/gorilla-merger:dev "${BACKEND}/gorilla-merger"
    [ "${cleanup_token}" = 1 ] && rm -f "${gh_token_file}"

    log "build_images: done"
    docker images | grep -E "^asap/" | sort | sed 's/^/  /' | while read -r l; do log "${l}"; done
}

# ── distribute the locally-built images to the node(s) that RUN them.
# Mapping follows the cold/warm split: WARM engine on node2, agents+producers
# on node0/node3, COLD/thanos sink on node1. ──
load_images() {
    log "load_images: distributing local images to their nodes (cold/warm split)"
    _ship() { # _ship IMAGE NODE...
        local img=$1; shift
        for n in "$@"; do
            log "  ${img} → ${n}"
            docker save "${img}" \
                | timeout 300 ssh -o ConnectTimeout=8 -o StrictHostKeyChecking=no -o BatchMode=yes "${n}" 'docker load' >/dev/null \
                || die "load ${img} → ${n} failed"
        done
    }
    _ship asap/data-plane:dev            "${NODE2_HOST}"
    _ship asap/control-plane:dev         "${NODE2_HOST}"
    _ship asap/asap-otel:dev             "${NODE0_HOST}" "${NODE3_HOST}"
    _ship asap/asap-otel-supervised:dev  "${NODE0_HOST}" "${NODE3_HOST}"
    _ship asap/otel-app:dev         "${NODE0_HOST}" "${NODE3_HOST}"
    _ship asap/gorilla-merger:dev        "${NODE1_HOST}"
    log "load_images: done"
}

# build + load, once per invocation, gated by SKIP_BUILD / SKIP_LOAD.
ensure_images() {
    if [ "${SKIP_BUILD:-0}" = 1 ]; then log "SKIP_BUILD=1 — reusing existing local images"; else build_images; fi
    if [ "${SKIP_LOAD:-0}" = 1 ]; then log "SKIP_LOAD=1 — not redistributing images"; else load_images; fi
}

# ── stage configs/scripts to a remote node under /mydata ──
sync_to() {
    local node=$1
    log "rsync configs → ${node}"
    rsync -a --delete \
        "${CONFIG_SRC}/" \
        "${node}:/mydata/mvp-multinode/configs/"
    rsync -a --exclude '__pycache__' --exclude 'tests/' \
        "${SCRIPT_DIR}/" "${node}:/mydata/mvp-multinode/scripts/"
    rsync -a "${TOPOLOGY_ENV}" "${node}:/mydata/mvp-multinode/topology.env"
}

sync_all_nodes() {
    for n in "${NODE0_HOST}" "${NODE1_HOST}" "${NODE2_HOST}" "${NODE3_HOST}"; do
        # `data/gorilla-merger` is the merger's tsdb volume mount on node2.
        on "${n}" 'mkdir -p /mydata/mvp-multinode/{configs,scripts,logs,results,data/gorilla-merger,data/sketch-persistence,data/victoriametrics,data/minio}'
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
#
# Restart policy defaults to `no` so a host reboot does NOT silently bring
# the benchmark stack back up under the operator (post-reboot containers
# race with the next `run_demo.sh` invocation, and the new flags / configs
# in that run never take effect on the already-up containers). Override with
# DOCKER_RESTART_POLICY=unless-stopped for long-lived deployments that
# should survive a daemon restart.
docker_run_on() {
    local node=$1; shift
    on "${node}" "docker run -d --restart ${DOCKER_RESTART_POLICY:-no} --network host \
        ${ADD_HOSTS[*]} \
        $*"
}

# ─── BACKEND STACK on node2 ────────────────────────────────────────
#
# Common services across all arms: prometheus, minio (b0/b1 don't use
# minio but it's harmless idle), and the control plane. ASAP arm adds
# the data plane + thanos-{query,store-gateway,compact}.

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
    # COLD/WARM split (2026-05): the cold backend (VM / Thanos / MinIO /
    # gorilla-merger / Prometheus) runs on node1; only the WARM asap engine
    # (data_plane + control_plane) runs on node2. Per-container --memory caps
    # make any runaway a recoverable container OOM-kill, not a host freeze.
    log "backend up (${arm}) — cold=node1, warm=node2"

    # Raw baselines (b0/b1/b2/b3): VictoriaMetrics on node1:8428.
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
        docker_run_on "${NODE1_HOST}" --cpus=4 --memory=16g --memory-swap=16g \
            --name asap-victoriametrics \
            -v /mydata/mvp-multinode/data/victoriametrics:/victoria-metrics-data \
            victoriametrics/victoria-metrics:v1.110.0 \
            --httpListenAddr=:8428 \
            --retentionPeriod=24h
    else
        local prom_args=(
            "--config.file=/etc/prometheus/prometheus.yml"
            "--storage.tsdb.retention.time=24h"
            "--web.enable-lifecycle"
        )
        docker_run_on "${NODE1_HOST}" --cpus=2 --memory=8g --memory-swap=8g \
            --name asap-prometheus \
            -v /mydata/mvp-multinode/configs/shared/prometheus-with-remote-write.yml:/etc/prometheus/prometheus.yml:ro \
            prom/prometheus:v2.55.0 "${prom_args[@]}"
    fi

    if is_asap_arm "${arm}"; then
        # MinIO + bucket setup
        docker_run_on "${NODE1_HOST}" --cpus=2 --memory=16g --memory-swap=16g \
            --name asap-minio \
            -v /mydata/mvp-multinode/data/minio:/data \
            -e MINIO_ROOT_USER=asap \
            -e MINIO_ROOT_PASSWORD=asap-local-only \
            minio/minio:latest server /data --console-address :9001

        # Wait for minio to be ready then create buckets
        sleep 5
        # mc image's default entrypoint is `mc`, not `sh` — must use
        # --entrypoint=sh, otherwise the bucket-create step is silently
        # rejected with "sh is not a recognized command" and gorillas3
        # writes vanish into a non-existent bucket.
        on "${NODE1_HOST}" "docker run --rm --network host \
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
        docker_run_on "${NODE1_HOST}" --cpus=2 --memory=16g --memory-swap=16g \
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
        docker_run_on "${NODE1_HOST}" --cpus=4 --memory=32g --memory-swap=32g \
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
        # same host. The data_plane (asap-data-plane) only talks to thanos-query
        # over HTTP :10903; the gRPC port is just thanos-query's own control
        # surface and isn't exposed.
        #
        # Issue #32: register the gorilla-merger StoreAPI (10907) as a second
        # --endpoint so query fans out to BOTH the merger (recent <2h pending
        # window) and the store-gateway (>=2h S3 blocks) and unions the
        # results. Without this, query only sees shipped S3 blocks and the
        # most-recent <2h of merger-ingested data is invisible.
        docker_run_on "${NODE1_HOST}" --cpus=2 --memory=8g --memory-swap=8g \
            --name asap-thanos-query \
            quay.io/thanos/thanos:v0.41.0 \
            query \
            --http-address=0.0.0.0:10903 \
            --grpc-address=0.0.0.0:10905 \
            --endpoint=thanos-store-gateway:10901 \
            --endpoint=gorilla-merger:10907 \
            --query.replica-label=replica

        # Thanos compact
        docker_run_on "${NODE1_HOST}" --cpus=2 --memory=24g --memory-swap=24g \
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

        # data_plane reorg (2026-05): the control plane and data plane now
        # ship as TWO separate images — `asap/control-plane:dev` (entrypoint
        # `/usr/local/bin/control_plane`) and `asap/data-plane:dev`
        # (entrypoint `/usr/local/bin/data_plane`) — built from
        # ASAPQuery-backend's per-crate Dockerfiles. They run as two separate
        # processes/containers.
        #
        # Without the control-plane container the data plane never receives
        # the control plane's POST /api/v1/streaming-config and falls back
        # to the static `configs/asap/backend-streaming.yaml`
        # (DDSketch-only, no Sum/Count/Topk roles). The post-#290 wave
        # — sum-by-zone (#291), rate-over-Sum and topk-over-rate (#292)
        # — only fires when the control-plane-driven plan installs the Sum
        # aggregation, so this container is load-bearing for ASAP-arm
        # validation.
        #
        # The `control-plane` DNS alias resolves to NODE2_IP via ADD_HOSTS;
        # both the agents and the control-plane-self-reference (the OpAMP
        # endpoint URL the control plane bakes into emitted agent yaml) use
        # that name.

        # asap-data-plane (ASAP only) — data plane process.
        sleep 3
        # Durable disk-backed sketch tier (ASAPQuery-backend #329): the warm
        # SketchStore seals aged epochs and flushes them to on-disk parts under
        # --persistence-dir, so memory is bounded by flush-then-evict (not just
        # #327's drop) and warm sketch state survives a restart. Enabled by
        # default; PERSISTENCE_ENABLED=0 reverts to the in-memory-only path.
        # Tuning knobs are env-overridable — prod defaults here; for fast-flush
        # validation set e.g. PERSIST_SEAL_WINDOWS=4 PERSIST_HOT_WINDOW_SECS=120.
        # Values are flag-shaped with no spaces, so the unquoted expansion below
        # word-splits cleanly (empty when disabled).
        local persist_flags=""
        if [ "${PERSISTENCE_ENABLED:-1}" = 1 ]; then
            persist_flags="--persistence-enabled --persistence-dir=/data/sketch-persistence --persistence-memory-limit-mb=${PERSIST_MEM_LIMIT_MB:-2048} --persistence-hot-window-secs=${PERSIST_HOT_WINDOW_SECS:-3600} --persistence-delete-older-than-secs=${PERSIST_DELETE_OLDER_SECS:-604800} --persistence-flush-interval-ms=${PERSIST_FLUSH_INTERVAL_MS:-1000} --persistence-part-cache-mb=${PERSIST_PART_CACHE_MB:-256} --persistence-seal-window-count=${PERSIST_SEAL_WINDOWS:-20}"
        fi
        docker_run_on "${NODE2_HOST}" --cpus=8 --memory=64g --memory-swap=64g \
            --name asap-data-plane \
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
            -v /mydata/mvp-multinode/data/sketch-persistence:/data/sketch-persistence \
            asap/data-plane:dev \
            --streaming-config=/etc/asap/streaming.yaml \
            --query-port=9091 \
            --enable-otel-ingest \
            --otel-grpc-port=${DP_OTLP_GRPC_PORT:-4317} \
            --otel-http-port=${DP_OTLP_HTTP_PORT:-4318} \
            ${persist_flags} ${DP_MONITOR_FLAGS:-}

        # asap-control-plane (ASAP only) — control plane process. Brought
        # up AFTER the data plane so the control plane's startup pre-pop
        # replan tick has a live data plane to POST the streaming-config
        # plan to (CONTROLLER_BACKEND_ENDPOINT). Without that POST the
        # data plane stays on the static DDSketch-only fallback and the
        # wave's Sum/Topk queries silently return empty.
        #
        # ASAP_EDGE_FUSED=1 gates the control plane to emit the FUSED
        # asap_edge agent config (single fused pipeline) instead of the
        # old 5-sketch routing shape. The `asap/control-plane:dev` image's
        # default entrypoint IS `/usr/local/bin/control_plane`, so no
        # `--entrypoint` override is needed.
        sleep 3
        docker_run_on "${NODE2_HOST}" --cpus=2 --memory=4g --memory-swap=4g \
            --name asap-control-plane \
            -e RUST_LOG="info,controller=debug,control_plane=debug" \
            -e USE_TYPED_STAGE_SPLIT=1 \
            -e ASAP_EDGE_FUSED=1 \
            -e ASAP_EDGE_BACKEND_OTLP_PORT=${ASAP_EDGE_BACKEND_OTLP_PORT:-4317} \
            -e CONTROLLER_ADDR=0.0.0.0:8080 \
            -e CONTROLLER_OPAMP_ADDR=0.0.0.0:4320 \
            -e CONTROLLER_GRPC_ADDR=0.0.0.0:4321 \
            -e CONTROLLER_OPAMP_ENDPOINT=ws://control-plane:4320/v1/opamp \
            -e CONTROLLER_BACKEND_ENDPOINT=http://data-plane:9091/api/v1/streaming-config \
            -e CONTROLLER_WORKLOADS=/etc/asap/mvp-workload.yaml \
            -v /mydata/mvp-multinode/configs/asap/mvp-workload.yaml:/etc/asap/mvp-workload.yaml:ro \
            asap/control-plane:dev
    fi
}

# Wipe the persistent backend STATE dirs so each arm starts from empty — the
# same clean slate the raw baselines get (MinIO/Thanos/VM run with
# container-local storage that's removed with the container). The two ASAP
# state dirs are HOST bind-mounts that survive container removal:
#   - data/gorilla-merger   : the merger's pending/shipped TSDB blocks + WAL
#   - data/sketch-persistence: the data_plane's flushed sketch index
# Without this they accumulate across EVERY up/down cycle — the merger grew to
# multiple GB (and inflated cold-query memory) across a day of runs because
# its per-window blocks were never cleared between runs. Set
# KEEP_BACKEND_DATA=1 to preserve them (e.g. to inspect blocks after a run).
clean_backend_data() {
    if [ "${KEEP_BACKEND_DATA:-0}" = 1 ]; then
        log "KEEP_BACKEND_DATA=1 — preserving backend state dirs"
        return
    fi
    log "wiping persistent backend state"
    # The merger writes subdirs as uid 65532; the parent dirs are 0777 so the
    # ssh user can unlink them, but fall back to sudo if a stricter umask blocks.
    for n in "${NODE1_HOST}" "${NODE2_HOST}"; do
        on "${n}" 'd=/mydata/mvp-multinode/data; rm -rf "$d"/gorilla-merger/* "$d"/sketch-persistence/* "$d"/victoriametrics/* "$d"/minio/* 2>/dev/null || sudo rm -rf "$d"/gorilla-merger/* "$d"/sketch-persistence/* "$d"/victoriametrics/* "$d"/minio/* 2>/dev/null || true' || true
    done
}

backend_down() {
    log "backend down — warm=node2, cold=node1"
    stop_node "${NODE2_HOST}"
    # Cold/thanos stack now lives on node1 (cold/warm split).
    stop_node "${NODE1_HOST}"
    # Containers are gone — now safe to clear their persistent state dirs.
    clean_backend_data
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
    docker_run_on "${NODE1_HOST}" --cpus=4 --memory=8g --memory-swap=8g \
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
        asap)      agent_cfg=asap/asap-otel-agent-asapedge.yaml ;;             # fused asap_edge edge-agg, OTLP→backend none (matched-none asap)
        asap-gzip) agent_cfg=asap-gzip/asap-otel-agent-asap-gzip.yaml ;;    # edge-agg, OTLP→backend gzip   (matched-gzip asap)
        *) die "unknown arm ${arm}" ;;
    esac

    # ── agent launch: supervised (asap arms) vs static (baselines) ──────
    #
    # The asap/asap-gzip arms talk to the controller, so they run the
    # OpenTelemetry opamp-supervisor (asap-otel-supervised image) instead of
    # the bare collector. The supervisor connects to the controller's OpAMP
    # server, receives the pushed remote config, merges it with its own
    # bootstrap pieces, writes it to disk, and (re)starts the asap-otel
    # collector against it — so the agent APPLIES the controller's plan
    # instead of running the static mounted ${agent_cfg} forever.
    #
    # The supervisor config (configs/asap/supervisor.yaml) is identity-templated
    # via the X_AGENT_ID env var (the controller keys agents by the X-Agent-ID
    # OpAMP header). It points server.endpoint at control-plane:4320 itself, so
    # the controller's OpAMP URL no longer comes from the mounted collector
    # config. The supervisor INJECTS its own `opamp` extension (→ its local
    # OpAMP server) into the collector's merged config; the controller must
    # therefore emit a supervisor-compatible fused config that does NOT carry
    # its own `opamp` extension / `service.extensions: [opamp]` (otherwise the
    # remote config's opamp block, merged last, clobbers the supervisor's and
    # the collector phones the controller directly instead of the supervisor).
    #
    # The b0/b1/b2/b3 baselines have no controller OpAMP server, so they keep
    # running the bare asap-otel collector with their static mounted config.
    if is_asap_arm "${arm}"; then
        # node0 → agent-a (binds 0.0.0.0:4317 on node0). Producers on node0
        # send to localhost:4317 == agent-a:4317.
        log "node0 agent-a up (${arm}, supervised)"
        docker_run_on "${NODE0_HOST}" --cpus=4 --memory=12g --memory-swap=12g \
            --name asap-agent-a \
            --hostname agent-a \
            -e X_AGENT_ID=agent-a \
            -e AGENT_ID=agent-a \
            -v /mydata/mvp-multinode/configs/asap/supervisor.yaml:/etc/otel/supervisor.yaml:ro \
            asap/asap-otel-supervised:dev \
            --config /etc/otel/supervisor.yaml

        # Same on node3 → agent-b (skipped in SINGLE_NODE mode: agent-b would
        # collide with agent-a on host :4317 under --network host).
        if [ "${SINGLE_NODE:-0}" != 1 ]; then
        log "node3 agent-b up (${arm}, supervised)"
        docker_run_on "${NODE3_HOST}" --cpus=4 --memory=12g --memory-swap=12g \
            --name asap-agent-b \
            --hostname agent-b \
            -e X_AGENT_ID=agent-b \
            -e AGENT_ID=agent-b \
            -v /mydata/mvp-multinode/configs/asap/supervisor.yaml:/etc/otel/supervisor.yaml:ro \
            asap/asap-otel-supervised:dev \
            --config /etc/otel/supervisor.yaml
        fi
    else
        # Raw baselines: bare asap-otel collector, static mounted config, no
        # controller/OpAMP.
        log "node0 agent-a up (${arm})"
        docker_run_on "${NODE0_HOST}" --cpus=4 --memory=12g --memory-swap=12g \
            --name asap-agent-a \
            --hostname agent-a \
            -e AGENT_ID=agent-a \
            -e ASAP_SKETCH_FAMILY=ddsketch \
            -v /mydata/mvp-multinode/configs/${agent_cfg}:/etc/otel/config.yaml:ro \
            asap/asap-otel:dev \
            --config=/etc/otel/config.yaml

        if [ "${SINGLE_NODE:-0}" != 1 ]; then
        log "node3 agent-b up (${arm})"
        docker_run_on "${NODE3_HOST}" --cpus=4 --memory=12g --memory-swap=12g \
            --name asap-agent-b \
            --hostname agent-b \
            -e AGENT_ID=agent-b \
            -e ASAP_SKETCH_FAMILY=ddsketch \
            -v /mydata/mvp-multinode/configs/${agent_cfg}:/etc/otel/config.yaml:ro \
            asap/asap-otel:dev \
            --config=/etc/otel/config.yaml
        fi
    fi

    # Wait for agent OTLP receiver to come up
    sleep 5

    # Producers on node0 → agent-a:4317 (== 10.10.1.1:4317 == localhost:4317)
    for i in $(seq 1 ${N_PRODUCERS_PER_NODE}); do
        log "node0 producer-a-${i} up"
        docker_run_on "${NODE0_HOST}" --cpus=1 --memory=4g --memory-swap=4g \
            --name asap-producer-a-${i} \
            asap/otel-app:dev \
            -target=agent-a:4317 \
            -producer-id=p-a-${i} \
            -cardinality=${PER_AGENT_CARDINALITY} \
            -freq-hz=${OTELAPP_FREQ_HZ} \
            -sdk-window=${OTELAPP_SDK_WINDOW} \
            -agg=${OTELAPP_SDK_AGG} \
            -max-buffer-per-series=${OTELAPP_MAX_BUFFER_PER_SERIES} \
            -freshness-probes=${OTELAPP_FRESHNESS_PROBES} \
            -freshness-probe-hz=${OTELAPP_FRESHNESS_PROBE_HZ} \
            -seed=${OTELAPP_SEED:-42}
    done
    for i in $(seq 1 ${N_PRODUCERS_PER_NODE}); do
        [ "${SINGLE_NODE:-0}" = 1 ] && break
        log "node3 producer-b-${i} up"
        docker_run_on "${NODE3_HOST}" --cpus=1 --memory=4g --memory-swap=4g \
            --name asap-producer-b-${i} \
            asap/otel-app:dev \
            -target=agent-b:4317 \
            -producer-id=p-b-${i} \
            -cardinality=${PER_AGENT_CARDINALITY} \
            -freq-hz=${OTELAPP_FREQ_HZ} \
            -sdk-window=${OTELAPP_SDK_WINDOW} \
            -agg=${OTELAPP_SDK_AGG} \
            -max-buffer-per-series=${OTELAPP_MAX_BUFFER_PER_SERIES} \
            -freshness-probes=${OTELAPP_FRESHNESS_PROBES} \
            -freshness-probe-hz=${OTELAPP_FRESHNESS_PROBE_HZ} \
            -seed=${OTELAPP_SEED:-42}
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

    log "[measure ${arm}] starting per-node host NIC and process sampling for ${SOAK_S}s"
    # On each node, sample container stats and capture to local file.
    for n in "${NODE0_HOST}" "${NODE1_HOST}" "${NODE2_HOST}" "${NODE3_HOST}"; do
        on "${n}" "bash /mydata/mvp-multinode/scripts/measure_nic_bw.sh \
            ${SOAK_S} /mydata/mvp-multinode/results/nic-${n}.csv \
            > /mydata/mvp-multinode/results/nic-${n}.log 2>&1 &"
        on "${n}" "python3 /mydata/mvp-multinode/scripts/measure_stages.py \
            --baseline ${arm} --duration ${SOAK_S} \
            --out /mydata/mvp-multinode/results/stages-${n}.csv \
            > /mydata/mvp-multinode/results/stages-${n}.log 2>&1 &"
    done

    # PromQL replay endpoint:
    #   asap arm  → asap-data-plane on node2:9091
    #   b0 / b1   → VictoriaMetrics on node2:8428 (serves PromQL on the
    #               same port as its /api/v1/write PRW receive). Prior to
    #               2026-05 this pointed at :9090 (Prometheus), but the
    #               b0/b1 backend_up() path brings up `asap-victoriametrics`
    #               not Prometheus, so :9090 was unreachable → 100% timeout.
    local query_endpoint
    if is_asap_arm "${arm}"; then
        query_endpoint="http://${NODE2_IP}:9091"
    else
        query_endpoint="http://${NODE1_IP}:8428"
    fi

    log "[measure ${arm}] MetricsQL replay against ${query_endpoint} for ${SOAK_S}s"
    python3 "${SCRIPT_DIR}/metricsql_replay.py" \
        --target "${query_endpoint}" \
        --controller "http://${NODE2_IP}:8080" \
        --queries "${PKG_DIR}/harness/queries/e2e.json" \
        --duration "${SOAK_S}" \
        --out "${out}/replay.jsonl" \
        > "${out}/replay.log" 2>&1 &
    local REPLAY_PID=$!

    # Wait for soak to complete
    wait ${REPLAY_PID} 2>/dev/null || true

    if is_asap_arm "${arm}"; then
        # Controller-side acknowledgement is the proof that the supervisor
        # applied the pushed plan; seeing only a plan in the controller is not
        # sufficient for functional correctness.
        on "${NODE2_HOST}" "curl -fsS http://127.0.0.1:8080/api/v1/agents" \
            > "${out}/controller-agents.json"
        on "${NODE2_HOST}" "curl -fsS -H 'X-Agent-ID: agent-a' http://127.0.0.1:8080/api/v1/collector-config/agent" \
            > "${out}/controller-config.yaml"
        on "${NODE2_HOST}" "docker logs asap-control-plane 2>&1" > "${out}/controller.log"
    fi

    # Freshness is part of the acceptance gate, not an optional report extra.
    # It runs after replay while the arm is still live.
    ARM="${arm}" OUT="${out}" NODE1_IP="${NODE1_IP}" NODE2_IP="${NODE2_IP}" \
        N_SAMPLES="${FRESHNESS_SAMPLES:-20}" POLL_MS="${FRESHNESS_POLL_MS:-100}" \
        bash "${SCRIPT_DIR}/measure_freshness.sh" > "${out}/freshness.log" 2>&1
    sleep 3   # let measurement scripts on remote nodes finish

    on "${NODE1_HOST}" "bash /mydata/mvp-multinode/scripts/measure_storage.sh ${arm} node1 /mydata/mvp-multinode/results/storage-node1.csv"
    on "${NODE2_HOST}" "bash /mydata/mvp-multinode/scripts/measure_storage.sh ${arm} node2 /mydata/mvp-multinode/results/storage-node2.csv"

    # Pull CSVs back from each node
    for n in "${NODE0_HOST}" "${NODE1_HOST}" "${NODE2_HOST}" "${NODE3_HOST}"; do
        scp "${n}:/mydata/mvp-multinode/results/nic-${n}.csv"      "${out}/nic-${n}.csv"      2>/dev/null || true
        scp "${n}:/mydata/mvp-multinode/results/stages-${n}.csv"   "${out}/stages-${n}.csv"   2>/dev/null || true
    done
    scp "${NODE1_HOST}:/mydata/mvp-multinode/results/storage-node1.csv" "${out}/storage-node1.csv"
    scp "${NODE2_HOST}:/mydata/mvp-multinode/results/storage-node2.csv" "${out}/storage-node2.csv"

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

# Allow sourcing as a library so other drivers (e.g. scale_fleet.sh) can compose
# backend_up + docker_run_on + topology for custom fleet sizes without invoking
# the CLI dispatch below.
if [ -n "${RUN_DEMO_LIB:-}" ]; then return 0 2>/dev/null || true; fi

cmd=${1:-help}
case "${cmd}" in
    sync)            sync_all_nodes ;;
    build)           build_images ;;
    load)            load_images ;;
    up)              ensure_images; sync_all_nodes; arm_up "${2:?need arm name}" ;;
    down)            arm_down ;;
    measure)         arm_measure "${2:?need arm name}" ;;
    arm)             ensure_images; sync_all_nodes; run_arm "${2:?need arm name}" ;;
    all)
        write_run_manifest
        ensure_images
        sync_all_nodes
        # Compression-matched bandwidth sweep — six arms. The two clean
        # apples-to-apples aggregation comparisons (same codec, only
        # aggregation differs):
        #   none: b0 vs asap        gzip: b1 vs asap-gzip  (primary)
        # b2 (PRW/Snappy) + b3 (serf) are real-world reference points.
        for arm in b0 b1 b2 b3 asap asap-gzip; do
            run_arm "${arm}"
        done
        log "=== evaluating issue #46 acceptance contract ==="
        python3 "${SCRIPT_DIR}/mvp_evaluate.py" \
            --run-dir "${RUN_DIR}" \
            --config "${PKG_DIR}/harness/acceptance.json"
        log "=== run complete: ${RUN_DIR} ==="
        ;;
    help|*)
        cat <<EOF
usage: $0 <cmd> [arm]
  build               rebuild ALL local images from current source (data-plane,
                      control-plane, asap-otel, asap-otel-supervised,
                      otel-app, gorilla-merger)
  load                ship the local images to the node(s) that run them
                      (cold/warm split: warm→node2, agents→node0/3, cold→node1)
  sync                rsync /mydata/ASAPCollector configs+scripts to all 4 nodes
  up <arm>            build + load + sync, then bring up containers for an arm
                      arms: b0 b1 b2 b3 asap asap-gzip
                        b0        raw OTLP→VM,  compression none  (matched-none baseline)
                        b1        raw OTLP→VM,  compression gzip  (matched-gzip baseline)
                        b2        raw PRW→VM    (Snappy, native)  (Prometheus ref)
                        b3        serf wire codec → gw → VM       (serf-compressed wire ref)
                        asap      edge-agg, OTLP→backend none     (matched-none asap)
                        asap-gzip edge-agg, OTLP→backend gzip     (matched-gzip asap)
  down                stop and remove all asap-* containers cluster-wide
  arm <arm>           build + load + sync, then full single-arm lifecycle: up → measure → down
  all                 build + load + sync + run all 6 arms back-to-back + report

  Source is rebuilt and re-shipped on every up/arm/all so a deploy never runs a
  stale image. Escape hatches when iterating on config only:
    SKIP_BUILD=1   reuse existing local images (don't rebuild)
    SKIP_LOAD=1    don't re-ship images to nodes
  Scale knob: PER_AGENT_CARDINALITY=<n> (default ${PER_AGENT_CARDINALITY}) — lower for light validation.
EOF
        ;;
esac
