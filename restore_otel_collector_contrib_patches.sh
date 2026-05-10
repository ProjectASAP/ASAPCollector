#!/usr/bin/env bash
# Stage opentelemetry-collector-contrib-patch/ onto the
# opentelemetry-collector-contrib submodule's working tree.
#
# Mirrors restore_otel_collector_patches.sh — the patch tree is the
# source-of-truth (committed in ASAPCollector main), and this script
# copies it onto the upstream-pinned submodule at build time. Required
# because the patched sketch processors (ddsketch / KLL / HLL /
# countsketch / countminsketch / gorillas3) live under the contrib
# submodule's processor/ tree from OCB's point of view.
#
# Idempotent: re-running overwrites destination files. Revived after
# cleanup PR #363 retired the patch-overlay scripts but left the import
# graph still pointing at submodule paths.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_ROOT="${ROOT_DIR}/opentelemetry-collector-contrib-patch"
DEST_ROOT="${ROOT_DIR}/opentelemetry-collector-contrib"

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
