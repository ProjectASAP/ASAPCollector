#!/usr/bin/env bash
# stack-coldon.sh — cold-ON single-host ASAP stack for the Fig 7
# COLD-FALLBACK latency arm. Derived from
# datasets_eval/multisketch/stack.sh (the full cold stack), with two
# eval-specific changes:
#
#   1. ALL docker commands run via `sudo docker` (the direct docker
#      socket is permission-denied on this host; sudo docker works).
#   2. The data-plane mounts a COLD storage-routing table
#      (backend-storage-routing-coldon.yaml) that routes
#      `google_cluster_2019_cpu_rate` to `gorilla_object_store`, so its
#      PromQL queries are answered by the ThanosQueryEngine
#      (data_source=thanos_query) over the gorilla cold tier — that is
#      what makes the COLD arm observable end-to-end.
#
# Components (all --network host on node0):
#   minio                       : 9000 / 9001
#   thanos store-gateway        : 10901/10902
#   gorilla-merger HTTP/gRPC    : 10908 / 10907
#   thanos query                : 10903 / 10905
#   data-plane OTLP ingest      : 14317/14318, query :9091  (cold ON)
#   control-plane               : 8080 / 4320 / 4321
#   agent (bare fused asap_edge, cold ON) OTLP receiver :4317/4318
#
# Usage:
#   stack-coldon.sh up   <workload.yaml> <agent-config.yaml> <routing.yaml>
#   stack-coldon.sh down
#   stack-coldon.sh ps
set -uo pipefail

ROOT=/mydata/ASAPCollector
CFG=${ROOT}/deploy/mvp-multinode/configs
WORKDIR=/mydata/mvp-multinode
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

H=127.0.0.1
ADD_HOSTS=(
  --add-host=control-plane:${H} --add-host=data-plane:${H}
  --add-host=minio:${H} --add-host=prometheus:${H} --add-host=victoriametrics:${H}
  --add-host=thanos-query:${H} --add-host=thanos-store-gateway:${H}
  --add-host=thanos-compact:${H} --add-host=gorilla-merger:${H}
  --add-host=agent-a:${H} --add-host=serf-gw:${H}
)

DR() { sudo docker run -d --restart no --network host "${ADD_HOSTS[@]}" "$@"; }
log(){ printf '[stack-coldon %s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }

down() {
  sudo docker ps -a --format '{{.Names}}' | grep '^asap-' | xargs -r sudo docker rm -f >/dev/null 2>&1
  log "all asap-* containers removed"
}

wipe_state() {
  sudo rm -rf "${WORKDIR}"/data/gorilla-merger/* "${WORKDIR}"/data/sketch-persistence/* 2>/dev/null || true
  mkdir -p "${WORKDIR}"/data/gorilla-merger "${WORKDIR}"/data/sketch-persistence "${WORKDIR}"/configs
}

up() {
  local workload=$1 agentcfg=$2 routing=$3
  down; wipe_state

  mkdir -p "${WORKDIR}"/configs/asap "${WORKDIR}"/configs/shared
  cp "${CFG}/shared/thanos-objstore.yaml" "${WORKDIR}/configs/shared/"
  cp "${CFG}/asap/backend-streaming.yaml" "${WORKDIR}/configs/asap/"
  cp "${routing}" "${WORKDIR}/configs/asap/backend-storage-routing.yaml"
  cp "${CFG}/asap/supervisor.yaml" "${WORKDIR}/configs/asap/" 2>/dev/null || true
  cp "${workload}" "${WORKDIR}/configs/asap/eval-workload.yaml"
  cp "${agentcfg}" "${WORKDIR}/configs/asap/eval-agent.yaml"

  log "minio up"
  DR --name asap-minio -e MINIO_ROOT_USER=asap -e MINIO_ROOT_PASSWORD=asap-local-only \
     minio/minio:latest server /data --console-address :9001 >/dev/null
  sleep 4
  sudo docker run --rm --network host "${ADD_HOSTS[@]}" --entrypoint=sh minio/mc:latest -c '
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
  # --user 0: the merger image is distroless `nonroot` (uid 65532) but the
  # host bind-mount /data is owned by the invoking user; run as root so the
  # merger can mkdir /data/pending (else: "mkdir /data/pending: permission
  # denied"). Same workaround the thanos store-gateway uses above.
  DR --name asap-gorilla-merger --user 0 \
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

  log "data-plane up (cold ON — gorilla_object_store routing, OTLP :14317, query :9091)"
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

  log "agent (bare, static fused asap_edge, cold ON) up — OTLP receiver :4317 for replay"
  DR --name asap-agent-a --cpus=8 --hostname agent-a \
     -e AGENT_ID=agent-a \
     -v "${WORKDIR}/configs/asap/eval-agent.yaml:/etc/otel/config.yaml:ro" \
     asap/asap-otel:dev --config=/etc/otel/config.yaml >/dev/null
  sleep 6
  log "stack up. containers:"; sudo docker ps --format '  {{.Names}}\t{{.Status}}' | grep asap- >&2
}

case "${1:-}" in
  up)   up "${2:?need workload}" "${3:?need agentcfg}" "${4:?need routing}" ;;
  down) down ;;
  ps)   sudo docker ps --format '{{.Names}}\t{{.Status}}' | grep asap- ;;
  *) echo "usage: stack-coldon.sh up <workload> <agentcfg> <routing> | down | ps" >&2; exit 2 ;;
esac
