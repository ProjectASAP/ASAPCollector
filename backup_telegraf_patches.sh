#!/usr/bin/env bash
# Reverse of restore_telegraf_patches.sh — folds modified files in
# the telegraf submodule back into telegraf-patch/. Note: unlike
# restore_telegraf_patches.sh this script does NOT skip go.mod /
# go.sum, because if you've actually modified those in the submodule
# you probably want them captured. Be careful: the patch tree's
# allsketches plugin ships its own go.mod for standalone iteration;
# blindly copying telegraf's mono-module go.mod will overwrite that.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_TELEGRAF="${ROOT_DIR}/telegraf"
DEST_DIR="${ROOT_DIR}/telegraf-patch"

copy_path() {
	local src="$1"
	local dest="$2"

	if [[ -d "${src}" ]]; then
		mkdir -p "${dest}"
		cp -R "${src}/." "${dest}/"
		echo "Copied directory: ${dest}"
	elif [[ -f "${src}" ]]; then
		mkdir -p "$(dirname "${dest}")"
		cp "${src}" "${dest}"
		echo "Copied file: ${dest}"
	else
		echo "Skipping ${src} (not found)"
	fi
}

mapfile -t CHANGED < <(git -C "${SRC_TELEGRAF}" status --porcelain)

if [[ ${#CHANGED[@]} -eq 0 ]]; then
	echo "No modified files detected in ${SRC_TELEGRAF}"
	exit 0
fi

for entry in "${CHANGED[@]}"; do
	status="${entry:0:2}"
	path="${entry:3}"

	if [[ "${status}" =~ ^D ]]; then
		continue
	fi

	if [[ "${path}" == *" -> "* ]]; then
		path="${path##* -> }"
	fi

	src_path="${SRC_TELEGRAF}/${path}"
	dest_path="${DEST_DIR}/${path}"
	copy_path "${src_path}" "${dest_path}"
done

echo "Copied ${#CHANGED[@]} changed paths into ${DEST_DIR}"
