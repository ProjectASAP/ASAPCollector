#!/usr/bin/env bash
# Wrapper — capture submodule edits across ALL OTel-side patch trees
# (collector + contrib + proto + opentelemetry-go). Mirror of
# restore_otel_patches.sh.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"${ROOT_DIR}/backup_otel_collector_contrib_patches.sh" "$@"
"${ROOT_DIR}/backup_otel_collector_patches.sh" "$@"
"${ROOT_DIR}/backup_otel_proto_patches.sh" "$@"
"${ROOT_DIR}/backup_opentelemetry_go_patches.sh" "$@"
