#!/usr/bin/env bash
# Archived Telegraf overlay helper. The OpenTelemetry MVP does not invoke this.
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PATCH_DIR="${ROOT_DIR}/future-support/telegraf/telegraf-patch"
TELEGRAF_DIR="${ROOT_DIR}/future-support/telegraf/telegraf"
[ -d "${PATCH_DIR}" ] || { echo "missing ${PATCH_DIR}" >&2; exit 1; }
[ -d "${TELEGRAF_DIR}" ] || { echo "missing ${TELEGRAF_DIR}" >&2; exit 1; }
while IFS= read -r -d '' file; do
  rel="${file#${PATCH_DIR}/}"
  case "${rel}" in go.mod|go.sum|*/go.mod|*/go.sum) continue;; esac
  mkdir -p "${TELEGRAF_DIR}/$(dirname "${rel}")"
  cp "${file}" "${TELEGRAF_DIR}/${rel}"
done < <(find "${PATCH_DIR}" -type f -print0)
echo "Telegraf future-support overlay restored"
