#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_ROOT="${ROOT_DIR}/opentelemetry-collector-plugins"
DEST_ROOT="${ROOT_DIR}/opentelemetry-collector"

if [[ ! -d "${SRC_ROOT}" ]]; then
	echo "Source directory ${SRC_ROOT} does not exist" >&2
	exit 1
fi

if [[ ! -d "${DEST_ROOT}" ]]; then
	echo "Destination repo ${DEST_ROOT} does not exist" >&2
	exit 1
fi

restored=0
while IFS= read -r -d '' file; do
	rel_path="${file#"${SRC_ROOT}/"}"
	dest_path="${DEST_ROOT}/${rel_path}"

	mkdir -p "$(dirname "${dest_path}")"
	cp "${file}" "${dest_path}"
	restored=$((restored + 1))
	echo "Restored ${rel_path}"
done < <(find "${SRC_ROOT}" -type f -print0)

if (( restored == 0 )); then
	echo "No files to restore from ${SRC_ROOT}"
else
	echo "Restored ${restored} file(s) into ${DEST_ROOT}"
fi
