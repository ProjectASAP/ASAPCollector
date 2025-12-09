#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_CLIENT="${ROOT_DIR}/opentelemetry-go"
DEST_DIR="${ROOT_DIR}/opentelemetry-go-patch"

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

ensure_git_repo() {
	local repo_dir="$1"

	if [[ ! -d "${repo_dir}" ]]; then
		echo "Source repo not found at ${repo_dir}" >&2
		exit 1
	fi

	# Pass safe.directory inline to avoid global git config issues.
	if ! git -C "${repo_dir}" -c safe.directory="${repo_dir}" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
		echo "Directory ${repo_dir} is not a git repository (did you check out the client source?)" >&2
		exit 1
	fi
}

backup_repo() {
	local repo_dir="$1"
	local dest_dir="$2"

	ensure_git_repo "${repo_dir}"

	mapfile -t CHANGED < <(git -C "${repo_dir}" -c safe.directory="${repo_dir}" status --porcelain)

	if [[ ${#CHANGED[@]} -eq 0 ]]; then
		echo "No modified files detected in ${repo_dir}"
		return
	fi

	for entry in "${CHANGED[@]}"; do
		local status="${entry:0:2}"
		local path="${entry:3}"

		# Skip deletions; we only copy modified/added/renamed paths.
		if [[ "${status}" =~ ^D ]]; then
			continue
		fi

		# Handle renamed paths from git status output.
		if [[ "${path}" == *" -> "* ]]; then
			path="${path##* -> }"
		fi

		local src_path="${repo_dir}/${path}"
		local dest_path="${dest_dir}/${path}"
		copy_path "${src_path}" "${dest_path}"
	done

	echo "Copied ${#CHANGED[@]} changed paths into ${dest_dir}"
}

backup_repo "${SRC_CLIENT}" "${DEST_DIR}"
