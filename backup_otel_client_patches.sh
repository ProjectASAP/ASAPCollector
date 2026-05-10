#!/usr/bin/env bash
# Legacy alias — same behavior as backup_opentelemetry_go_patches.sh
# (opentelemetry-go/ → opentelemetry-go-patch/). Kept for symmetry
# with backup_all.sh / backup_otel_patches.sh references in older
# READMEs and CI scripts.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${ROOT_DIR}/backup_opentelemetry_go_patches.sh" "$@"
