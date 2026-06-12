#!/usr/bin/env bash
# stack.sh — collapsed single-host (node0) ASAP stack for the multi-sketch
# family evaluation. Brings up the SAME fused asap_edge + data_plane +
# control_plane + gorilla cold tier the mvp-multinode `asap` arm runs, but
# all containers on THIS host (node0) so one machine can run a fresh stack
# per eval arm. All host DNS aliases (data-plane, control-plane, minio,
# gorilla-merger, thanos-*) resolve to 127.0.0.1 via --add-host.
#
# Port map (all --network host on node0):
#   agent OTLP receiver (replay target) : 4317 / 4318
#   data-plane OTLP ingest              : 14317 / 14318   (distinct from agent)
#   data-plane query                    : 9091
#   control-plane HTTP/OpAMP/gRPC       : 8080 / 4320 / 4321
#   minio                               : 9000 / 9001
#   gorilla-merger HTTP/gRPC            : 10908 / 10907
#   thanos store/query/compact          : 10901/10903/10905/10904/10902
#
# Usage:
#   stack.sh up   <workload.yaml> <agent-config.yaml>
#   stack.sh down
#   stack.sh ps
set -uo pipefail

ROOT=/mydata/ASAPCollector
CFG=${ROOT}/deploy/mvp-multinode/configs
WORKDIR=/mydata/mvp-multinode          # reuse the same host bind-mount dirs
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

H=127.0.0.1
ADD_HOSTS=(
  --add-host=control-plane:${H} --add-host=data-plane:${H}
  --add-host=minio:${H} --add-host=prometheus:${H} --add-host=victoriametrics:${H}
  --add-host=thanos-query:${H} --add-host=thanos-store-gateway:${H}
  --add-host=thanos-compact:${H} --add-host=gorilla-merger:${H}
  --add-host=agent-a:${H} --add-host=serf-gw:${H}
)

DR() { docker run -d --restart no --network host "${ADD_HOSTS[@]}" "$@"; }

log(){ printf '[stack %s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }

down() {
  docker ps -a --format '{{.Names}}' | grep '^asap-' | xargs -r docker rm -f >/dev/null 2>&1
  log "all asap-* containers removed"
}

wipe_state() {
  rm -rf "${WORKDIR}"/data/gorilla-merger/* "${WORKDIR}"/data/sketch-persistence/* 2>/dev/null \
    || sudo rm -rf "${WORKDIR}"/data/gorilla-merger/* "${WORKDIR}"/data/sketch-persistence/* 2>/dev/null || true
  mkdir -p "${WORKDIR}"/data/gorilla-merger "${WORKDIR}"/data/sketch-persistence "${WORKDIR}"/configs
}

up() {
  local workload=$1 agentcfg=$2
  down; wipe_state

  # stage the agent config + workload + supervisor + shared configs into the
  # host bind dir the containers mount.
  mkdir -p "${WORKDIR}"/configs/asap "${WORKDIR}"/configs/shared
  cp "${CFG}/shared/thanos-objstore.yaml" "${WORKDIR}/configs/shared/"
  cp "${CFG}/asap/backend-streaming.yaml" "${WORKDIR}/configs/asap/"
  cp "${CFG}/asap/backend-storage-routing.yaml" "${WORKDIR}/configs/asap/"
  cp "${CFG}/asap/supervisor.yaml" "${WORKDIR}/configs/asap/"
  cp "${workload}" "${WORKDIR}/configs/asap/eval-workload.yaml"
  cp "${agentcfg}" "${WORKDIR}/configs/asap/eval-agent.yaml"

  log "minio up"
  DR --name asap-minio -e MINIO_ROOT_USER=asap -e MINIO_ROOT_PASSWORD=asap-local-only \
     minio/minio:latest server /data --console-address :9001 >/dev/null
  sleep 4
  docker run --rm --network host "${ADD_HOSTS[@]}" --entrypoint=sh minio/mc:latest -c '
      mc alias set asap http://minio:9000 asap asap-local-only &&
      mc mb --ignore-existing asap/asap-gorilla &&
      mc mb --ignore-existing asap/asap-gorilla-tsdb &&
      mc mb --ignore-existing asap/raw' >/dev/null 2>&1 || log "minio bucket warn"

  log "thanos store-gateway up"
  DR --name asap-thanos-store-gateway --user 0 \
     -v "${WORKDIR}/configs/shared/thanos-objstore.yaml:/etc/thanos/objstore.yaml:ro" \
     quay.io/thanos/thanos:v0.41.0 store \
     --objstore.config-file=/etc/thanos/objstore.yaml \
     --http-address=0.0.0.0:10902 --grpc-address=0.0.0.0:10901 \
     --data-dir=/tmp/thanos-store --sync-block-duration=30s >/dev/null

  log "gorilla-merger up"
  DR --name asap-gorilla-merger \
     -v "${WORKDIR}/configs/shared/thanos-objstore.yaml:/etc/thanos/objstore.yaml:ro" \
     -v "${WORKDIR}/data/gorilla-merger:/data" \
     asap/gorilla-merger:dev \
     --http-address=0.0.0.0:10908 --grpc-address=0.0.0.0:10907 \
     --tsdb.path=/data --objstore.config-file=/etc/thanos/objstore.yaml \
     --external-labels=cluster=asap-mvp,merger=m1 >/dev/null

  log "thanos query up"
  DR --name asap-thanos-query quay.io/thanos/thanos:v0.41.0 query \
     --http-address=0.0.0.0:10903 --grpc-address=0.0.0.0:10905 \
     --endpoint=thanos-store-gateway:10901 --endpoint=gorilla-merger:10907 \
     --query.replica-label=replica >/dev/null

  log "data-plane up (OTLP ingest :14317, query :9091)"
  DR --name asap-data-plane --cpus=8 \
     -e RUST_LOG="${DP_RUST_LOG:-info}" -e ASAP_SKETCH_FAMILY=ddsketch \
     -e ASAP_GORILLA_S3_ENDPOINT=http://minio:9000 -e ASAP_GORILLA_S3_BUCKET=asap-gorilla \
     -e ASAP_GORILLA_S3_REGION=us-east-1 -e ASAP_GORILLA_S3_ACCESS_KEY_ID=asap \
     -e ASAP_GORILLA_S3_SECRET_ACCESS_KEY=asap-local-only -e ASAP_GORILLA_S3_TENANT=default \
     -e ASAP_GORILLA_S3_USE_SSL=false \
     -e 'ASAP_GORILLA_S3_PREFIX_TEMPLATE={tenant}/{metric}/{YYYY}/{MM}/{DD}/{HH}/' \
     -e ASAP_BACKEND_STORAGE_ROUTING=/etc/asap/backend-storage-routing.yaml \
     -e ASAP_THANOS_QUERY_URL=http://thanos-query:10903 \
     -v "${WORKDIR}/configs/asap/backend-streaming.yaml:/etc/asap/streaming.yaml:ro" \
     -v "${WORKDIR}/configs/asap/backend-storage-routing.yaml:/etc/asap/backend-storage-routing.yaml:ro" \
     -v "${WORKDIR}/data/sketch-persistence:/data/sketch-persistence" \
     asap/data-plane:dev \
     --streaming-config=/etc/asap/streaming.yaml --query-port=9091 \
     --enable-otel-ingest --otel-grpc-port=14317 --otel-http-port=14318 >/dev/null
     # persistence DISABLED for the eval: the wide (600s) eval window exceeds
     # the default hot-window, so the disk-flusher would seal+evict the warm
     # sketches out of the query path before we query them. In-memory-only
     # keeps every family's sketch state resident + queryable.
  sleep 4

  log "control-plane up (workload=$(basename "${workload}"), backend OTLP port 14317)"
  DR --name asap-control-plane --cpus=2 \
     -e RUST_LOG="info,controller=debug,control_plane=debug" \
     -e USE_TYPED_STAGE_SPLIT=1 -e ASAP_EDGE_FUSED=1 \
     -e ASAP_EDGE_BACKEND_OTLP_PORT=14317 \
     -e CONTROLLER_ADDR=0.0.0.0:8080 -e CONTROLLER_OPAMP_ADDR=0.0.0.0:4320 \
     -e CONTROLLER_GRPC_ADDR=0.0.0.0:4321 \
     -e CONTROLLER_OPAMP_ENDPOINT=ws://control-plane:4320/v1/opamp \
     -e CONTROLLER_BACKEND_ENDPOINT=http://data-plane:9091/api/v1/streaming-config \
     -e CONTROLLER_WORKLOADS=/etc/asap/eval-workload.yaml \
     -v "${WORKDIR}/configs/asap/eval-workload.yaml:/etc/asap/eval-workload.yaml:ro" \
     asap/control-plane:dev >/dev/null
  sleep 5

  log "agent (bare, static fused asap_edge) up — OTLP receiver :4317 for replay"
  # Run the BARE collector against the static fused config (not the
  # supervised image): the prebuilt control-plane emits a config the
  # prebuilt collector rejects (enable_series_id key skew), so we drive
  # the SAME asap_edge processor from a static config that exports to
  # data-plane:14317. The control-plane still POSTed the matching backend
  # streaming-config, which is what the data-plane query path needs.
  DR --name asap-agent-a --cpus=8 --hostname agent-a \
     -e AGENT_ID=agent-a \
     -v "${WORKDIR}/configs/asap/eval-agent.yaml:/etc/otel/config.yaml:ro" \
     asap/asap-otel:dev --config=/etc/otel/config.yaml >/dev/null
  sleep 6
  log "stack up. containers:"; docker ps --format '  {{.Names}}\t{{.Status}}' | grep asap- >&2
}

case "${1:-}" in
  up)   up "${2:?need workload}" "${3:-${CFG}/asap/asap-otel-agent-asapedge.yaml}" ;;
  down) down ;;
  ps)   docker ps --format '{{.Names}}\t{{.Status}}' | grep asap- ;;
  *) echo "usage: stack.sh up <workload> [agentcfg] | down | ps" >&2; exit 2 ;;
esac
