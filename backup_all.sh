#!/usr/bin/env bash
# Wrapper — capture submodule edits across EVERY patch tree (OTel
# side + Telegraf). Mirror of restore_all.sh.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"${ROOT_DIR}/backup_otel_patches.sh" "$@"
"${ROOT_DIR}/backup_telegraf_patches.sh" "$@"
