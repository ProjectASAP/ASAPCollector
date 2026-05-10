#!/usr/bin/env bash
# Wrapper — apply EVERY patch overlay (OTel side + Telegraf).
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"${ROOT_DIR}/restore_otel_patches.sh" "$@"
"${ROOT_DIR}/restore_telegraf_patches.sh" "$@"
