#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_COLLECTOR="${ROOT_DIR}/opentelemetry-collector"
DEST_DIR="${ROOT_DIR}/opentelemetry-collector-plugins"

copy_path() {
	local src="$1"
	local dest="$2"

	if [[ -d "${src}" ]]; then
		rm -rf "${dest}"
		mkdir -p "$(dirname "${dest}")"
		cp -R "${src}" "${dest}"
		echo "Copied directory: ${dest}"
	elif [[ -f "${src}" ]]; then
		mkdir -p "$(dirname "${dest}")"
		cp "${src}" "${dest}"
		echo "Copied file: ${dest}"
	else
		echo "Skipping ${src} (not found)"
	fi
}

if [[ ! -d "${SRC_COLLECTOR}" ]]; then
	echo "Source repo not found at ${SRC_COLLECTOR}" >&2
	exit 1
fi

if ! git -C "${SRC_COLLECTOR}" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
	echo "Directory ${SRC_COLLECTOR} is not a git repository (did you check out the collector source?)" >&2
	exit 1
fi

mapfile -t CHANGED < <(git -C "${SRC_COLLECTOR}" status --porcelain)

if [[ ${#CHANGED[@]} -eq 0 ]]; then
	echo "No modified files detected in ${SRC_COLLECTOR}"
	exit 0
fi

for entry in "${CHANGED[@]}"; do
	status="${entry:0:2}"
	path="${entry:3}"

	# Skip deletions; we only copy modified/added/renamed paths.
	if [[ "${status}" =~ ^D ]]; then
		continue
	fi

	# Handle renamed paths from git status output.
	if [[ "${path}" == *" -> "* ]]; then
		path="${path##* -> }"
	fi

	src_path="${SRC_COLLECTOR}/${path}"
	dest_path="${DEST_DIR}/${path}"
	copy_path "${src_path}" "${dest_path}"
done

echo "Copied ${#CHANGED[@]} changed paths into ${DEST_DIR}"
