#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_ROOT="${ROOT_DIR}/opentelemetry-go-patch"
DEST_ROOT="${ROOT_DIR}/opentelemetry-go"

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
