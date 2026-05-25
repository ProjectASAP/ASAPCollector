#!/usr/bin/env bash
# build_opamp_supervisor.sh — Build the OpenTelemetry opamp-supervisor binary.
#
# The supervisor (github.com/open-telemetry/opentelemetry-collector-contrib/
# cmd/opampsupervisor) is the component that MANAGES the asap-otel collector
# process and APPLIES the remote config the controller pushes over OpAMP. The
# opampextension that asap-otel embeds is report-only (it reports health +
# effective config but never reloads the collector); the supervisor is what
# actually rewrites the collector's on-disk config and restarts it when the
# controller pushes a new AgentRemoteConfig.
#
# ## Version pin
#
# The supervisor is built straight from the `opentelemetry-collector-contrib`
# submodule, which is pinned (see .gitmodules → release/v0.141.0) at the
# `cmd/opampsupervisor/v0.141.0` git tag. That is the SAME release train as the
# asap-otel collector components (core v1.47.0 / contrib v0.141.0, per
# opentelemetry-collector-contrib-patch/cmd/asap-otel/builder-config.yaml), so
# the OpAMP wire types + config-merge semantics match the collector the
# supervisor manages. Building from the submodule (rather than `go install
# ...@version`) guarantees the pin can never drift from the collector.
#
# Usage:
#   ./build_opamp_supervisor.sh
#
# The resulting binary is written to:
#   deploy/docker/opampsupervisor
# (the build context the supervised image's COPY expects).
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SUPERVISOR_SRC="${ROOT_DIR}/opentelemetry-collector-contrib/cmd/opampsupervisor"
OUT="${ROOT_DIR}/deploy/docker/opampsupervisor"

if [[ ! -d "${SUPERVISOR_SRC}" ]]; then
  echo "ERROR: ${SUPERVISOR_SRC} not found." >&2
  echo "Initialize the contrib submodule first:" >&2
  echo "  git submodule update --init opentelemetry-collector-contrib" >&2
  exit 1
fi

echo "==> Supervisor pin: $(cd "${ROOT_DIR}/opentelemetry-collector-contrib" && git describe --tags --match 'cmd/opampsupervisor/*' HEAD 2>/dev/null || echo 'unknown')"
echo "==> Building opamp-supervisor (CGO disabled, static) ..."
cd "${SUPERVISOR_SRC}"
# CGO_ENABLED=0 → static binary that runs in the debian-slim runtime stage
# without libc surprises. -mod=mod lets go resolve the supervisor's own
# go.sum-pinned deps from the proxy (no private modules here).
CGO_ENABLED=0 GOFLAGS=-mod=mod go build -o "${OUT}" .

echo ""
echo "Build successful: ${OUT}"
