#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"${ROOT_DIR}/restore_otel_collector_contrib_patches.sh" "$@"
"${ROOT_DIR}/restore_otel_collector_patches.sh" "$@"
"${ROOT_DIR}/restore_otel_client_patches.sh" "$@"
"${ROOT_DIR}/restore_otel_proto_patches.sh" "$@"
"${ROOT_DIR}/restore_opentelemetry_go_patches.sh" "$@"
