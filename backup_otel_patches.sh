#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

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

process_repo() {
	local repo_dir="$1"
	local dest_dir="$2"

	if [[ ! -d "${repo_dir}" ]]; then
		echo "Skipping missing repo ${repo_dir}"
		return
	fi

	mapfile -t CHANGED < <(git -C "${repo_dir}" status --porcelain)

	if [[ ${#CHANGED[@]} -eq 0 ]]; then
		echo "No modified files detected in ${repo_dir}"
		return
	fi

	for entry in "${CHANGED[@]}"; do
		local status="${entry:0:2}"
		local path="${entry:3}"

		if [[ "${status}" =~ ^D ]]; then
			continue
		fi

		if [[ "${path}" == *" -> "* ]]; then
			path="${path##* -> }"
		fi

		local src_path="${repo_dir}/${path}"
		local dest_path="${dest_dir}/${path}"
		copy_path "${src_path}" "${dest_path}"
	done

	echo "Copied ${#CHANGED[@]} changed paths into ${dest_dir}"
}

process_repo "${ROOT_DIR}/opentelemetry-collector" "${ROOT_DIR}/opentelemetry-collector-patch"
process_repo "${ROOT_DIR}/opentelemetry-go" "${ROOT_DIR}/opentelemetry-go-patch"
