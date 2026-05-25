#!/usr/bin/env bash
# build_asap_otel.sh — Build the asap-otel OpenTelemetry Collector distribution.
#
# asap-otel includes ALL sketch processors (ddsketch, KLL, HLL,
# countsketch, countminsketch) in a single binary.
#
# Usage:
#   ./build_asap_otel.sh            # builds asap-otel (re-applies patches)
#   ./build_asap_otel.sh --skip-patches  # skip re-applying patches (faster
#                                        # incremental rebuilds when the
#                                        # submodule overlay is already current)
#
# The resulting binary is written to:
#   opentelemetry-collector-contrib-patch/cmd/asap-otel/asap-otel
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

REQUIRED_OCB_VERSION="v0.141.0"
OCB_MODULE="go.opentelemetry.io/collector/cmd/builder"
SKIP_PATCHES=false

# Parse arguments
for arg in "$@"; do
  case "$arg" in
    --skip-patches) SKIP_PATCHES=true ;;
    *) echo "Unknown argument: $arg" >&2; exit 1 ;;
  esac
done

# Locate or install the OCB builder at the required version
find_or_install_builder() {
  local gobin
  gobin="$(go env GOPATH)/bin"

  # Check if the installed builder is already the right version
  if command -v builder &>/dev/null && builder version 2>/dev/null | grep -qF "${REQUIRED_OCB_VERSION}"; then
    echo "$(command -v builder)"
    return
  fi
  if [[ -x "${gobin}/builder" ]] && "${gobin}/builder" version 2>/dev/null | grep -qF "${REQUIRED_OCB_VERSION}"; then
    echo "${gobin}/builder"
    return
  fi

  echo "Installing OCB ${REQUIRED_OCB_VERSION}..." >&2
  GOBIN="${gobin}" go install "${OCB_MODULE}@${REQUIRED_OCB_VERSION}"
  echo "${gobin}/builder"
}

# Step 1: Apply patch overlays onto the OTel submodules.
#
# The patch trees (opentelemetry-collector-patch/, -contrib-patch/,
# opentelemetry-proto-patch/, opentelemetry-go-patch/) are the
# source-of-truth for files we add to / replace in the upstream
# submodules pinned at v0.141.0. The submodules themselves stay clean
# (we never push these changes to upstream OTel); the restore scripts
# stage the patch trees onto the submodules' working trees so OCB and
# the patched processors' replace directives resolve.
if [[ "${SKIP_PATCHES}" == false ]]; then
  echo "==> Applying patches to submodules..."
  bash "${ROOT_DIR}/restore_otel_patches.sh"
  echo ""
fi

# Step 2: Locate the correct builder
echo "==> Locating OCB ${REQUIRED_OCB_VERSION}..."
BUILDER="$(find_or_install_builder)"
echo "    Using builder: ${BUILDER}"
echo ""

# Step 3: Build
PATCH_DIR="${ROOT_DIR}/opentelemetry-collector-contrib-patch"
CONFIG="${PATCH_DIR}/cmd/asap-otel/builder-config.yaml"

echo "==> Building asap-otel..."
cd "${PATCH_DIR}"
"${BUILDER}" --config "${CONFIG}"

# Step 4: Add sketchlib-go replace (private module) and rebuild
ASAP_OTEL_DIR="${PATCH_DIR}/cmd/asap-otel"
# OCB always re-emits go.mod with `sketchlib-go ... // indirect`
# in the require block (every sketch processor pulls it in), so a
# plain `grep -q sketchlib-go` matched even when no replace was
# present — the build then resolved sketchlib-go via the module
# proxy / sumdb, pinning to whatever commit was published months
# ago instead of the local checkout. Match the replace line
# specifically.
if ! grep -qE "^replace[[:space:]]+github\.com/ProjectASAP/sketchlib-go" "${ASAP_OTEL_DIR}/go.mod" 2>/dev/null; then
  echo "replace github.com/ProjectASAP/sketchlib-go => ${ROOT_DIR}/../sketchlib-go" >> "${ASAP_OTEL_DIR}/go.mod"
fi
if ! grep -qE "^replace[[:space:]]+github\.com/ProjectASAP/asap-precompute-go" "${ASAP_OTEL_DIR}/go.mod" 2>/dev/null; then
  echo "replace github.com/ProjectASAP/asap-precompute-go => ${ROOT_DIR}/asap-precompute-go" >> "${ASAP_OTEL_DIR}/go.mod"
fi
# asap-gorilla-go is the cold-fragment encoder used by the asapedgeprocessor.
# Same OCB-indirect trap as sketchlib-go above: without an explicit replace the
# build resolves the PUBLISHED module (e.g. v0.2.0) from the proxy/cache and
# silently ignores LOCAL asap-gorilla-go edits (this is exactly how a build
# missed #432's idle-series eviction). Pin it to the in-repo checkout so local
# changes always ship.
if ! grep -qE "^replace[[:space:]]+github\.com/ProjectASAP/asap-gorilla-go" "${ASAP_OTEL_DIR}/go.mod" 2>/dev/null; then
  echo "replace github.com/ProjectASAP/asap-gorilla-go => ${ROOT_DIR}/asap-gorilla-go" >> "${ASAP_OTEL_DIR}/go.mod"
fi

cd "${ASAP_OTEL_DIR}"
GONOSUMCHECK="github.com/ProjectASAP/*" GONOSUMDB="github.com/ProjectASAP/*" go build -o asap-otel . 2>&1

BINARY="${ASAP_OTEL_DIR}/asap-otel"
echo ""
echo "Build successful: ${BINARY}"
