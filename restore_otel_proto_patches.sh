#!/usr/bin/env bash
# Stage opentelemetry-proto-patch/ onto the opentelemetry-proto
# submodule's working tree.
#
# The patch tree carries the ASAP sketch wire-format additions
# (countminsketchencoding / countsketchencoding / ddsketchencoding /
# hllsketchencoding / kllsketchencoding constants and their generated
# Go bindings) on top of the upstream proto schema. Same source-of-
# truth-in-main / overlay-onto-submodule pattern as the collector and
# contrib restore scripts.
#
# Idempotent: re-running overwrites destination files. Revived after
# cleanup PR #363.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_ROOT="${ROOT_DIR}/opentelemetry-proto-patch"
DEST_ROOT="${ROOT_DIR}/opentelemetry-proto"

restore_repo() {
	local patch_dir="$1"
	local repo_dir="$2"

	if [[ ! -d "${patch_dir}" ]]; then
		echo "Source directory ${patch_dir} does not exist" >&2
		exit 1
	fi

	if [[ ! -d "${repo_dir}" ]]; then
		echo "Destination repo ${repo_dir} does not exist" >&2
		exit 1
	fi

	local restored=0
	while IFS= read -r -d '' file; do
		local rel_path="${file#"${patch_dir}/"}"
		local dest_path="${repo_dir}/${rel_path}"

		mkdir -p "$(dirname "${dest_path}")"
		cp "${file}" "${dest_path}"
		restored=$((restored + 1))
	done < <(find "${patch_dir}" -type f -print0)

	if (( restored == 0 )); then
		echo "No files to restore from ${patch_dir}"
	else
		echo "Restored ${restored} file(s) into ${repo_dir}"
	fi
}

restore_repo "${SRC_ROOT}" "${DEST_ROOT}"
