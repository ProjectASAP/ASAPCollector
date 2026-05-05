#!/usr/bin/env bash
# restore_telegraf_patches.sh — apply telegraf-patch/ overlay onto the
# upstream telegraf submodule.
#
# Phase 4 step D wires the unified `allsketches` processor (one plugin,
# all five sketch types — see docs/design-asap-telegraf-integration.md
# §3) into the Telegraf binary. This script stages the plugin source
# and its registration overlay into the submodule:
#
#   telegraf-patch/                              → telegraf/
#     plugins/processors/all/allsketches.go      → plugins/processors/all/allsketches.go
#     processors/allsketches/*.go *.conf README  → plugins/processors/allsketches/...
#
# The processors/allsketches/ source-of-truth ships its own go.mod /
# go.sum so contributors can iterate on the plugin standalone, but the
# plugin must live inside Telegraf's main module when wired into a
# binary (Telegraf is a single-module repo). The script therefore
# excludes go.mod / go.sum when staging into the submodule.
#
# Pre-`allsketches` experimental aggregator/output patches were
# deleted in the cleanup that landed alongside this script — the
# unified `allsketches` plugin supersedes them per the design doc.
#
# Idempotent: re-running re-copies files; no-ops if patches don't differ.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PATCH_DIR="${ROOT_DIR}/telegraf-patch"
TELEGRAF_DIR="${ROOT_DIR}/telegraf"

if [[ ! -d "${PATCH_DIR}" ]]; then
	echo "Patch directory ${PATCH_DIR} does not exist" >&2
	exit 1
fi

if [[ ! -d "${TELEGRAF_DIR}" ]]; then
	echo "Telegraf submodule ${TELEGRAF_DIR} does not exist (run 'git submodule update --init telegraf')" >&2
	exit 1
fi

copied=0

# copy_file <src> <dest>
copy_file() {
	local src="$1"
	local dest="$2"
	if [[ ! -f "${src}" ]]; then
		echo "Source file missing: ${src}" >&2
		exit 1
	fi
	mkdir -p "$(dirname "${dest}")"
	cp "${src}" "${dest}"
	copied=$((copied + 1))
}

# copy_tree <src_dir> <dest_dir> — copy every regular file under <src>
# to the matching path under <dest>, skipping go.mod / go.sum so the
# plugin folds into Telegraf's mono-module.
copy_tree() {
	local src="$1"
	local dest="$2"
	if [[ ! -d "${src}" ]]; then
		echo "Source directory missing: ${src}" >&2
		exit 1
	fi
	while IFS= read -r -d '' file; do
		local rel_path="${file#"${src}/"}"
		case "${rel_path}" in
			go.mod|go.sum|*/go.mod|*/go.sum) continue ;;
		esac
		local dest_path="${dest}/${rel_path}"
		mkdir -p "$(dirname "${dest_path}")"
		cp "${file}" "${dest_path}"
		copied=$((copied + 1))
	done < <(find "${src}" -type f -print0)
}

# 1. allsketches plugin source.
copy_tree "${PATCH_DIR}/processors/allsketches" \
	"${TELEGRAF_DIR}/plugins/processors/allsketches"

# 2. allsketches registration overlay into plugins/processors/all/.
copy_file "${PATCH_DIR}/plugins/processors/all/allsketches.go" \
	"${TELEGRAF_DIR}/plugins/processors/all/allsketches.go"

echo "Restored ${copied} file(s) from telegraf-patch/ into telegraf/"
