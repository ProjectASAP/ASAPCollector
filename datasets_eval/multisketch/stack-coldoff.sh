#!/usr/bin/env bash
# stack-coldoff.sh — MINIMAL cold/archive-OFF single-host ASAP stack for the
# multi-sketch family ACCURACY eval (feat/multisketch-accuracy).
#
# Difference vs stack.sh: NO MinIO / Thanos / gorilla-merger cold tier, and
# the data-plane is started WITHOUT `ASAP_THANOS_QUERY_URL` (so the binary
# registers the NoDataArchiveEngine stub — there is no real archive engine).
# The agent runs with `cold: {enabled: false}` (no gorillas3 ship). This is
# the warm-only path the Pareto/gct DDSketch accuracy runs used: recent
# range-selector queries (quantile_over_time(...[Ns]), count_over_time(...))
# stay on the warm SketchStore instead of being forwarded to the (empty)
# archive.
#
# Components (all --network host on node0):
#   data-plane OTLP ingest :14317/14318, query :9091  (cold OFF, no thanos)
#   control-plane HTTP/OpAMP/gRPC :8080/4320/4321 (POSTs backend streaming-config)
#   agent (bare fused asap_edge) OTLP receiver :4317/4318 (replay target),
#     metrics :8890
#
# Usage:
#   stack-coldoff.sh up   <workload.yaml> <agent-config.yaml>
#   stack-coldoff.sh down
#   stack-coldoff.sh ps
set -uo pipefail

ROOT=/mydata/ASAPCollector
CFG=${ROOT}/deploy/mvp-multinode/configs
WORKDIR=/mydata/mvp-multinode
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

H=127.0.0.1
ADD_HOSTS=(
  --add-host=control-plane:${H} --add-host=data-plane:${H}
  --add-host=agent-a:${H}
)

DR() { docker run -d --restart no --network host "${ADD_HOSTS[@]}" "$@"; }
log(){ printf '[stack-coldoff %s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }

down() {
  docker ps -a --format '{{.Names}}' | grep '^asap-' | xargs -r docker rm -f >/dev/null 2>&1
  log "all asap-* containers removed"
}

wipe_state() {
  rm -rf "${WORKDIR}"/data/sketch-persistence/* 2>/dev/null \
    || sudo rm -rf "${WORKDIR}"/data/sketch-persistence/* 2>/dev/null || true
  mkdir -p "${WORKDIR}"/data/sketch-persistence "${WORKDIR}"/configs
}

up() {
  local workload=$1 agentcfg=$2
  down; wipe_state

  mkdir -p "${WORKDIR}"/configs/asap "${WORKDIR}"/configs/shared
  cp "${CFG}/asap/backend-streaming.yaml" "${WORKDIR}/configs/asap/"
  cp "${CFG}/asap/backend-storage-routing.yaml" "${WORKDIR}/configs/asap/"
  cp "${workload}" "${WORKDIR}/configs/asap/eval-workload.yaml"
  cp "${agentcfg}" "${WORKDIR}/configs/asap/eval-agent.yaml"

  log "data-plane up (cold OFF — NO ASAP_THANOS_QUERY_URL; OTLP :14317, query :9091)"
  # NOTE: no ASAP_THANOS_QUERY_URL, no ASAP_GORILLA_S3_* → the binary
  # registers the NoDataArchiveEngine stub; warm SketchStore is the only
  # live engine. Persistence disabled (in-memory warm sketches stay resident
  # and queryable for the whole eval window).
  DR --name asap-data-plane --cpus=8 \
     -e RUST_LOG="${DP_RUST_LOG:-info}" -e ASAP_SKETCH_FAMILY=ddsketch \
     -e ASAP_BACKEND_STORAGE_ROUTING=/etc/asap/backend-storage-routing.yaml \
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

  log "agent (bare, static fused asap_edge, cold OFF) up — OTLP receiver :4317 for replay"
  DR --name asap-agent-a --cpus=8 --hostname agent-a \
     -e AGENT_ID=agent-a \
     -v "${WORKDIR}/configs/asap/eval-agent.yaml:/etc/otel/config.yaml:ro" \
     asap/asap-otel:dev --config=/etc/otel/config.yaml >/dev/null
  sleep 6
  log "stack up. containers:"; docker ps --format '  {{.Names}}\t{{.Status}}' | grep asap- >&2
}

case "${1:-}" in
  up)   up "${2:?need workload}" "${3:?need agentcfg}" ;;
  down) down ;;
  ps)   docker ps --format '{{.Names}}\t{{.Status}}' | grep asap- ;;
  *) echo "usage: stack-coldoff.sh up <workload> <agentcfg> | down | ps" >&2; exit 2 ;;
esac
