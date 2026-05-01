#!/usr/bin/env bash
# build_sketchcollector.sh — Build the sketchcollector OpenTelemetry Collector distribution.
#
# sketchcollector includes ALL sketch processors (ddsketch, KLL, HLL,
# countsketch, countminsketch) in a single binary.
#
# Usage:
#   ./build_sketchcollector.sh            # builds sketchcollector
#   ./build_sketchcollector.sh --skip-patches  # skip re-applying patches (if already applied)
#
# The resulting binary is written to:
#   opentelemetry-collector-contrib-patch/cmd/sketchcollector/sketchcollector
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

# Step 1: Apply patches to submodules
if [[ "${SKIP_PATCHES}" == false ]]; then
  echo "==> Applying patches to submodules..."
  cd "${ROOT_DIR}"
  bash restore_otel_collector_patches.sh
  bash restore_otel_collector_contrib_patches.sh
  bash restore_otel_proto_patches.sh
  echo ""
fi

# Step 2: Locate the correct builder
echo "==> Locating OCB ${REQUIRED_OCB_VERSION}..."
BUILDER="$(find_or_install_builder)"
echo "    Using builder: ${BUILDER}"
echo ""

# Step 3: Build
PATCH_DIR="${ROOT_DIR}/opentelemetry-collector-contrib-patch"
CONFIG="${PATCH_DIR}/cmd/sketchcollector/builder-config.yaml"

echo "==> Building sketchcollector..."
cd "${PATCH_DIR}"
"${BUILDER}" --config "${CONFIG}"

# Step 4: Add sketchlib-go replace (private module) and rebuild
SKETCHCOL_DIR="${PATCH_DIR}/cmd/sketchcollector"
# OCB always re-emits go.mod with `sketchlib-go ... // indirect`
# in the require block (every sketch processor pulls it in), so a
# plain `grep -q sketchlib-go` matched even when no replace was
# present — the build then resolved sketchlib-go via the module
# proxy / sumdb, pinning to whatever commit was published months
# ago instead of the local checkout. Match the replace line
# specifically.
if ! grep -qE "^replace[[:space:]]+github\.com/ProjectASAP/sketchlib-go" "${SKETCHCOL_DIR}/go.mod" 2>/dev/null; then
  echo "replace github.com/ProjectASAP/sketchlib-go => ${ROOT_DIR}/../sketchlib-go" >> "${SKETCHCOL_DIR}/go.mod"
fi

cd "${SKETCHCOL_DIR}"
GONOSUMCHECK="github.com/ProjectASAP/*" GONOSUMDB="github.com/ProjectASAP/*" go build -o sketchcollector . 2>&1

BINARY="${SKETCHCOL_DIR}/sketchcollector"
echo ""
echo "Build successful: ${BINARY}"
