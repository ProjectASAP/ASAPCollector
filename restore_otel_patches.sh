#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

restore_repo() {
	local patch_dir="$1"
	local repo_dir="$2"

	if [[ ! -d "${patch_dir}" ]]; then
		echo "No patch directory found at ${patch_dir}, skipping."
		return
	}
	if [[ ! -d "${repo_dir}" ]]; then
		echo "Skipping missing repo ${repo_dir}"
		return
	}

	local restored=0
	while IFS= read -r -d '' file; do
		local rel_path="${file#"${patch_dir}/"}"
		local dest_path="${repo_dir}/${rel_path}"

		mkdir -p "$(dirname "${dest_path}")"
		cp "${file}" "${dest_path}"
		((restored++))
	done < <(find "${patch_dir}" -type f -print0)

	if (( restored == 0 )); then
		echo "No files to restore from ${patch_dir}"
	else
		echo "Restored ${restored} file(s) from ${patch_dir} into ${repo_dir}"
	fi
}

restore_repo "${ROOT_DIR}/opentelemetry-collector-patch" "${ROOT_DIR}/opentelemetry-collector"
restore_repo "${ROOT_DIR}/opentelemetry-go-patch" "${ROOT_DIR}/opentelemetry-go"
