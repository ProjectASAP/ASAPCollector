#!/usr/bin/env bash
# build_asap_telegraf.sh — Build the asap-telegraf Telegraf distribution.
#
# asap-telegraf is upstream Telegraf with the ASAP allsketches
# processor wired in (one plugin, all five sketch types — see
# docs/design-asap-telegraf-integration.md §3). The build:
#
#   1. Adds local replace directives for asap-precompute-go and
#      sketchlib-go so the build resolves them from sibling checkouts
#      instead of the module proxy.
#   2. Runs `go build` on telegraf/cmd/telegraf.
#
# Output: telegraf/asap-telegraf
#
# Usage:
#   ./build_asap_telegraf.sh                  # builds asap-telegraf
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

for arg in "$@"; do
	case "$arg" in
		--skip-patches) ;;  # accepted for backward compatibility — no-op now
		                    # that patch overlays have been removed.
		*) echo "Unknown argument: $arg" >&2; exit 1 ;;
	esac
done

TELEGRAF_DIR="${ROOT_DIR}/telegraf"
SKETCHLIB_GO_DIR="${ROOT_DIR}/../sketchlib-go"
ASAP_PRECOMPUTE_GO_DIR="${ROOT_DIR}/asap-precompute-go"

if [[ ! -d "${TELEGRAF_DIR}/cmd/telegraf" ]]; then
	echo "Telegraf submodule not initialized at ${TELEGRAF_DIR} — run 'git submodule update --init telegraf'" >&2
	exit 1
fi

# Step 2: Wire sibling checkouts via replace directives + require lines.
#
# Defensive grep: after `go build` writes back a `// indirect` require
# entry, a plain `grep -q sketchlib-go` matches even when no replace is
# present. Match the replace line specifically (mirroring
# build_asap_otel.sh).
#
# A bare `replace` without a `require` makes Go refuse the build with
# "module … is replaced but not required" — the unified `allsketches`
# plugin folds into Telegraf's mono-module so its require entries (which
# upstream Telegraf does not have) must be added here too. Versions
# match the standalone allsketches go.mod's `v0.0.0` placeholder; the
# replace points the resolver at the local checkout regardless.
cd "${TELEGRAF_DIR}"
if ! grep -qE "^replace[[:space:]]+github\.com/ProjectASAP/sketchlib-go" go.mod 2>/dev/null; then
	echo "==> Adding replace directive for sketchlib-go"
	go mod edit -replace="github.com/ProjectASAP/sketchlib-go=${SKETCHLIB_GO_DIR}"
fi
if ! grep -qE "^replace[[:space:]]+github\.com/ProjectASAP/asap-precompute-go" go.mod 2>/dev/null; then
	echo "==> Adding replace directive for asap-precompute-go"
	go mod edit -replace="github.com/ProjectASAP/asap-precompute-go=${ASAP_PRECOMPUTE_GO_DIR}"
fi
if ! grep -qE "github\.com/ProjectASAP/asap-precompute-go[[:space:]]+v" go.mod 2>/dev/null; then
	echo "==> Adding require directive for asap-precompute-go"
	go mod edit -require="github.com/ProjectASAP/asap-precompute-go@v0.0.0"
fi
if ! grep -qE "github\.com/ProjectASAP/sketchlib-go[[:space:]]+v" go.mod 2>/dev/null; then
	echo "==> Adding require directive for sketchlib-go"
	go mod edit -require="github.com/ProjectASAP/sketchlib-go@v0.0.0"
fi
cd "${ROOT_DIR}"

# Step 3: Tidy the module graph so transitive deps (e.g. asap-precompute-go's
# uses of telegraf core packages) land in go.sum, then build.
echo "==> Running go mod tidy..."
cd "${TELEGRAF_DIR}"
GONOSUMCHECK="github.com/ProjectASAP/*" GONOSUMDB="github.com/ProjectASAP/*" \
	go mod tidy

echo "==> Building asap-telegraf..."
GONOSUMCHECK="github.com/ProjectASAP/*" GONOSUMDB="github.com/ProjectASAP/*" \
	go build -o asap-telegraf ./cmd/telegraf

BINARY="${TELEGRAF_DIR}/asap-telegraf"
echo ""
echo "Build successful: ${BINARY}"
