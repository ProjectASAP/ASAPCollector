#!/usr/bin/env bash
# run_parity.sh — Phase 5 step E orchestrator. Drives the canonical
# golden input (golden_input/inputs.json) through each ASAP-flavored
# agent — asap-otel, asap-otap, optionally asap-telegraf — captures
# the SketchEnvelope.Payload bytes each one emits, and runs the Go
# test in MODE=binary to assert byte-equality across pairs.
#
# Then, if the backend image is available, replays the canonical
# PromQL query set (deploy/scripts/queries-e2e.json) against each
# agent's emitted envelope stream and asserts response equality.
#
# # Modes
#
# This script delegates the actual byte/PromQL comparisons to the Go
# test in this directory (parity_test.go). Two modes:
#
#   --mode=fixture (default)
#       Quickly verify the cross-language gate (#243) golden fixtures
#       are present and non-empty. No Docker needed.
#
#   --mode=binary
#       Bring up each agent via deploy/docker-compose/cross-host-parity.yml,
#       drive the golden input, capture envelopes + PromQL responses,
#       then run parity_test.go in binary mode to compare.
#
# # Prerequisites for binary mode
#
#   - docker + docker compose v2 on PATH.
#   - Pre-built images:
#       asap/asap-otel:dev      (bash build_asap_otel.sh +
#                                docker build -f deploy/docker/Dockerfile.asap-otel)
#       asap/asap-otap:dev     (bash build_asap_otap.sh +
#                                docker build -f deploy/docker/Dockerfile.asap-otap)
#       asap/asap-telegraf:dev (only if --include-telegraf;
#                                bash build_asap_telegraf.sh +
#                                Dockerfile.asap-telegraf — see follow-up #1
#                                in PROGRESS.md, currently a follow-up).
#       asap/query-backend:dev  (deploy/docker/Dockerfile.backend)
#
# Missing images cause an explicit failure with the build command in
# the message — never a silent skip in binary mode.
#
# # Usage
#
#   bash integration/parity/agent-binary/run_parity.sh
#   bash integration/parity/agent-binary/run_parity.sh --mode=binary
#   bash integration/parity/agent-binary/run_parity.sh --mode=binary --include-telegraf
#
# # Output
#
#   ./captures/<agent>/<sketch>_envelope.bin   — captured envelope payloads
#   ./captures/promql/<agent>_<query>.json     — captured PromQL responses
#   ./captures/run.log                         — orchestrator log
#
# # Exit codes
#
#   0 — all per-pair byte-equality + PromQL-equality assertions pass.
#   1 — at least one assertion failed (Phase 5E exit criterion missed).
#   2 — prerequisite missing (Docker, image, fixture).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
COMPOSE_FILE="${REPO_ROOT}/deploy/docker-compose/cross-host-parity.yml"
CAPTURE_DIR="${SCRIPT_DIR}/captures"
GOLDEN_PARITY_DIR="${REPO_ROOT}/integration/parity/runtime-impl/golden"

MODE="fixture"
INCLUDE_TELEGRAF=0
KEEP_RUNNING=0

usage() {
  sed -n 's/^# \{0,1\}//p' "$0" | sed -n '/^run_parity.sh/,/^# Exit codes/p'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode=*) MODE="${1#--mode=}"; shift ;;
    --mode) MODE="$2"; shift 2 ;;
    --include-telegraf) INCLUDE_TELEGRAF=1; shift ;;
    --keep-running) KEEP_RUNNING=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown arg: $1" >&2; usage >&2; exit 1 ;;
  esac
done

case "${MODE}" in
  fixture|binary) ;;
  *) echo "--mode must be fixture or binary, got: ${MODE}" >&2; exit 1 ;;
esac

mkdir -p "${CAPTURE_DIR}"
LOG_FILE="${CAPTURE_DIR}/run.log"
: > "${LOG_FILE}"

log() { echo "[$(date -u +%FT%TZ)] $*" | tee -a "${LOG_FILE}"; }

log "Phase 5 step E run_parity.sh: mode=${MODE} include_telegraf=${INCLUDE_TELEGRAF}"

# -------- fixture mode --------------------------------------------------
# Drives the Go test in fixture mode. The test reproduces the
# cross-language gate's canonical envelope bytes inline via canonical.go
# (sketchlib-go SerializePortable* helpers) and asserts byte-equality
# across pairs — no Docker, no agent binaries. If
# integration/parity/runtime-impl/golden/*.bin is also present, the test cross-checks
# the inline regen against the on-disk fixture and surfaces any drift.
if [[ "${MODE}" == "fixture" ]]; then
  if [[ -d "${GOLDEN_PARITY_DIR}" ]]; then
    log "On-disk gate fixtures present at ${GOLDEN_PARITY_DIR}; the Go test"
    log "  will cross-check inline regen against them."
    for f in ddsketch_envelope.bin kll_envelope.bin hll_envelope.bin \
             countsketch_envelope.bin cms_envelope.bin; do
      if [[ -s "${GOLDEN_PARITY_DIR}/${f}" ]]; then
        log "    ok: ${f} ($(stat -c%s "${GOLDEN_PARITY_DIR}/${f}") bytes)"
      else
        log "    note: ${f} missing — inline regen will be the only source"
      fi
    done
  else
    log "On-disk gate fixtures absent under ${GOLDEN_PARITY_DIR}."
    log "  Inline regen drives the assertion. To enable the cross-check,"
    log "  regenerate fixtures with:"
    log "    cd ${REPO_ROOT}/integration/parity/runtime-impl && \\"
    log "      GOLDEN_REGEN=1 go test -run TestGenerateGoldenFixtures ./..."
  fi
  log "Running Go test in fixture mode..."
  pushd "${SCRIPT_DIR}" >/dev/null
  if (( INCLUDE_TELEGRAF == 1 )); then
    CROSS_HOST_PARITY_MODE=fixture CROSS_HOST_PARITY_INCLUDE_TELEGRAF=1 \
      go test -v ./... 2>&1 | tee -a "${LOG_FILE}"
  else
    CROSS_HOST_PARITY_MODE=fixture \
      go test -v ./... 2>&1 | tee -a "${LOG_FILE}"
  fi
  status=${PIPESTATUS[0]}
  popd >/dev/null
  if (( status != 0 )); then
    log "FAIL: fixture-mode Go test exited ${status}"
    exit 1
  fi
  log "OK: fixture-mode Phase 5E parity check passed."
  exit 0
fi

# -------- binary mode ---------------------------------------------------
# Bring up the agents via Docker compose, drive the canonical input,
# capture envelopes + PromQL responses, then run the Go test in binary
# mode against the captures.
log "Binary-mode prerequisites check..."
if ! command -v docker >/dev/null 2>&1; then
  log "ERROR: docker not on PATH. Install Docker Engine + compose v2."
  exit 2
fi
if ! docker compose version >/dev/null 2>&1; then
  log "ERROR: 'docker compose' subcommand unavailable. Need compose v2."
  exit 2
fi
if [[ ! -f "${COMPOSE_FILE}" ]]; then
  log "ERROR: ${COMPOSE_FILE} missing — bug in PR layout."
  exit 2
fi

# Required images
required_images=("asap/asap-otel:dev" "asap/asap-otap:dev" "asap/query-backend:dev")
if (( INCLUDE_TELEGRAF == 1 )); then
  required_images+=("asap/asap-telegraf:dev")
fi
missing_images=0
for img in "${required_images[@]}"; do
  if ! docker image inspect "${img}" >/dev/null 2>&1; then
    log "ERROR: image ${img} not present locally."
    missing_images=$((missing_images + 1))
  else
    log "  ok: image ${img} present"
  fi
done
if (( missing_images > 0 )); then
  log "Build the missing image(s) per Dockerfile.<name> headers:"
  log "  bash build_asap_otel.sh && docker build -f deploy/docker/Dockerfile.asap-otel  -t asap/asap-otel:dev  ."
  log "  bash build_asap_otap.sh       && docker build -f deploy/docker/Dockerfile.asap-otap -t asap/asap-otap:dev ."
  if (( INCLUDE_TELEGRAF == 1 )); then
    log "  bash build_asap_telegraf.sh   && docker build -f deploy/docker/Dockerfile.asap-telegraf -t asap/asap-telegraf:dev ."
  fi
  log "  docker build -f deploy/docker/Dockerfile.backend -t asap/query-backend:dev ."
  exit 2
fi

# Compose profile selection: telegraf is gated by a profile so the
# default invocation only spins up asap-otel + asap-otap.
profile_args=()
if (( INCLUDE_TELEGRAF == 1 )); then
  profile_args+=("--profile" "telegraf")
fi

cleanup() {
  if (( KEEP_RUNNING == 1 )); then
    log "--keep-running set; leaving containers up. Tear down later with:"
    log "  docker compose -f ${COMPOSE_FILE} down -v"
    return
  fi
  log "Tearing down compose stack..."
  docker compose -f "${COMPOSE_FILE}" "${profile_args[@]}" down -v >>"${LOG_FILE}" 2>&1 || true
}
trap cleanup EXIT

log "Bringing up compose stack: ${COMPOSE_FILE}"
docker compose -f "${COMPOSE_FILE}" "${profile_args[@]}" up -d >>"${LOG_FILE}" 2>&1

# Wait for each agent + backend to be healthy. The compose file pins
# `healthcheck:` blocks so this loop just polls the runtime status.
log "Waiting for services to become healthy..."
deadline=$(( $(date +%s) + 90 ))
services=("asap-otel" "asap-otap" "envelope-tap" "query-backend")
if (( INCLUDE_TELEGRAF == 1 )); then services+=("asap-telegraf"); fi
while true; do
  ready=1
  for svc in "${services[@]}"; do
    state=$(docker compose -f "${COMPOSE_FILE}" "${profile_args[@]}" ps --format json "${svc}" 2>/dev/null \
      | head -1 \
      | sed -n 's/.*"State":"\([^"]*\)".*/\1/p')
    if [[ "${state}" != "running" ]]; then
      ready=0
      break
    fi
  done
  if (( ready == 1 )); then break; fi
  if (( $(date +%s) > deadline )); then
    log "ERROR: services did not reach 'running' within 90s"
    docker compose -f "${COMPOSE_FILE}" "${profile_args[@]}" ps >>"${LOG_FILE}" 2>&1
    exit 1
  fi
  sleep 2
done
log "All services running."

# Drive the canonical input. The driver lives in the envelope-tap
# container; it loads golden_input/inputs.json (mounted) and emits
# OTLP metrics to each agent's gRPC port. Telegraf consumes a
# line-protocol fixture written from the same JSON.
log "Driving golden input through agents (envelope-tap driver)..."
docker compose -f "${COMPOSE_FILE}" "${profile_args[@]}" \
  exec -T envelope-tap /usr/local/bin/drive-parity \
    --input /golden_input/inputs.json \
    --capture /captures \
    --agents "${services[*]}" \
    >>"${LOG_FILE}" 2>&1 || {
      log "ERROR: drive-parity failed; see ${LOG_FILE}"
      exit 1
    }

# Replay queries-e2e.json against each agent's backend instance and
# write canonical-JSON responses under captures/promql/.
log "Replaying queries-e2e.json against each agent's backend..."
mkdir -p "${CAPTURE_DIR}/promql"
docker compose -f "${COMPOSE_FILE}" "${profile_args[@]}" \
  exec -T envelope-tap /usr/local/bin/replay-promql \
    --queries /queries-e2e.json \
    --capture /captures/promql \
    --agents "${services[*]}" \
    >>"${LOG_FILE}" 2>&1 || {
      log "ERROR: replay-promql failed; see ${LOG_FILE}"
      exit 1
    }

# Hand off to the Go test for the actual byte-equality + PromQL-equality
# assertions. This is where the per-pair / per-sketch / per-query
# verdict matrix is written.
log "Running Go test in binary mode against captures/..."
pushd "${SCRIPT_DIR}" >/dev/null
if (( INCLUDE_TELEGRAF == 1 )); then
  CROSS_HOST_PARITY_MODE=binary \
    CROSS_HOST_PARITY_CAPTURE_DIR="${CAPTURE_DIR}" \
    CROSS_HOST_PARITY_INCLUDE_TELEGRAF=1 \
    go test -v ./... 2>&1 | tee -a "${LOG_FILE}"
else
  CROSS_HOST_PARITY_MODE=binary \
    CROSS_HOST_PARITY_CAPTURE_DIR="${CAPTURE_DIR}" \
    go test -v ./... 2>&1 | tee -a "${LOG_FILE}"
fi
status=${PIPESTATUS[0]}
popd >/dev/null

if (( status != 0 )); then
  log "FAIL: binary-mode Go test exited ${status}"
  exit 1
fi
log "OK: binary-mode Phase 5E parity check passed."
exit 0
