#!/usr/bin/env bash
# Stage opentelemetry-collector-patch/ onto the opentelemetry-collector
# submodule's working tree.
#
# The patch tree under opentelemetry-collector-patch/ is the
# source-of-truth for files we add to / replace in the upstream
# collector at v0.141.0 (e.g. processor/selfmonitor/). Those files are
# committed in ASAPCollector main; the submodule is at a clean upstream
# tag and should NOT carry committed patches (we don't push them to the
# upstream repo). This script copies patch-tree files into the
# submodule's working tree at build time so OCB resolves the import
# `go.opentelemetry.io/collector/processor/selfmonitor` (and the rest)
# via the existing `replace go.opentelemetry.io/collector/processor =>
# ../../../opentelemetry-collector/processor` directive in the patched
# processors' go.mod files.
#
# Idempotent: re-running overwrites destination files in place. Safe
# to call from build_asap_otel.sh; previously deleted in cleanup PR
# #363 alongside the per-sketch col cmd dirs, revived to unblock fresh
# builds of asap-otel.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_ROOT="${ROOT_DIR}/opentelemetry-collector-patch"
DEST_ROOT="${ROOT_DIR}/opentelemetry-collector"

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
