#!/usr/bin/env bash
# Wrapper — apply ALL OTel-side patch overlays (collector + contrib +
# proto + opentelemetry-go). Does NOT call restore_telegraf_patches.sh;
# Telegraf is a separate runtime with its own build script.
#
# Used by build_asap_otel.sh's Step 1 ("Apply patches to submodules").
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"${ROOT_DIR}/restore_otel_collector_contrib_patches.sh" "$@"
"${ROOT_DIR}/restore_otel_collector_patches.sh" "$@"
"${ROOT_DIR}/restore_otel_proto_patches.sh" "$@"
"${ROOT_DIR}/restore_opentelemetry_go_patches.sh" "$@"
