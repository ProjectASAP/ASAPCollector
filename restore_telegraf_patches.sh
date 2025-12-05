#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_ROOT="${ROOT_DIR}/telegraf-plugins/plugins"
DEST_ROOT="${ROOT_DIR}/telegraf/plugins"

if [[ ! -d "${SRC_ROOT}" ]]; then
	echo "Source directory ${SRC_ROOT} does not exist" >&2
	exit 1
fi

shopt -s dotglob

copied=0
for path in "${SRC_ROOT}"/*; do
	rel="$(basename "${path}")"
	dest="${DEST_ROOT}/${rel}"
	rm -rf "${dest}"
	cp -R "${path}" "${dest}"
	echo "Restored ${rel} into ${dest}"
	copied=$((copied + 1))
done

echo "Restored ${copied} items from telegraf-plugins/plugins into telegraf/plugins/"
