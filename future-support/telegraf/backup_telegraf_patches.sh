#!/usr/bin/env bash
# Archived Telegraf maintenance helper. The OpenTelemetry MVP does not invoke
# this script; it copies edits from the future-support Telegraf submodule back
# to its tracked patch overlay.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SRC_TELEGRAF="${ROOT_DIR}/future-support/telegraf/telegraf"
DEST_DIR="${ROOT_DIR}/future-support/telegraf/telegraf-patch"

copy_path() {
  local src="$1" dest="$2"
  if [[ -d "${src}" ]]; then
    mkdir -p "${dest}"; cp -R "${src}/." "${dest}/"
  elif [[ -f "${src}" ]]; then
    mkdir -p "$(dirname "${dest}")"; cp "${src}" "${dest}"
  fi
}

mapfile -t CHANGED < <(git -C "${SRC_TELEGRAF}" status --porcelain)
if [[ ${#CHANGED[@]} -eq 0 ]]; then
  echo "No modified files detected in ${SRC_TELEGRAF}"; exit 0
fi
for entry in "${CHANGED[@]}"; do
  status="${entry:0:2}"; path="${entry:3}"
  [[ "${status}" =~ ^D ]] && continue
  [[ "${path}" == *" -> "* ]] && path="${path##* -> }"
  copy_path "${SRC_TELEGRAF}/${path}" "${DEST_DIR}/${path}"
done
echo "Copied ${#CHANGED[@]} changed paths into ${DEST_DIR}"
