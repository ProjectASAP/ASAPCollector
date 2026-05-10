#!/usr/bin/env bash
# Legacy alias — restore_otel_client_patches.sh historically applied
# the same overlay as restore_opentelemetry_go_patches.sh
# (opentelemetry-go-patch/ → opentelemetry-go/). Kept for symmetry
# with restore_all.sh / restore_otel_patches.sh references in older
# READMEs and CI scripts.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${ROOT_DIR}/restore_opentelemetry_go_patches.sh" "$@"
