#!/usr/bin/env bash
# setup.sh — One-time environment setup for DataCollector development.
#
# What this script does:
#   1. Verifies or installs Go (>= MIN_GO_VERSION) to /usr/local/go
#   2. Adds Go and GOPATH/bin to PATH for the current shell and ~/.bashrc
#   3. Installs the OCB (OpenTelemetry Collector Builder) binary at the
#      version required to build the ddsketchcol distribution
#   4. Initialises git submodules (if not already done)
#   5. Applies all patch overlays to the submodules
#
# Usage:
#   ./setup.sh              # full setup
#   ./setup.sh --no-go      # skip Go installation (if already managed externally)
#   ./setup.sh --no-patches # skip patch application
#
# Safe to re-run: each step is skipped if already satisfied.

set -euo pipefail

# ─── Versions ────────────────────────────────────────────────────────────────
# Minimum Go version required by opentelemetry-collector-contrib (go 1.25.4).
MIN_GO_VERSION="1.25.4"
# Go toolchain version to install when Go is absent or below the minimum.
GO_INSTALL_VERSION="1.26.0"
# OCB version that matches the ddsketchcol distribution target (v0.141.0).
OCB_VERSION="0.141.0"
# ─────────────────────────────────────────────────────────────────────────────

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_GO=true
APPLY_PATCHES=true

# Parse flags
for arg in "$@"; do
  case "$arg" in
    --no-go)      INSTALL_GO=false ;;
    --no-patches) APPLY_PATCHES=false ;;
    *) echo "Unknown argument: $arg" >&2; exit 1 ;;
  esac
done

# ─── Helpers ─────────────────────────────────────────────────────────────────

info()    { echo "  $*"; }
success() { echo "[OK] $*"; }
section() { echo; echo "==> $*"; }

# Compare two semver strings; returns 0 if $1 >= $2.
version_ge() {
  # Use sort -V (version sort) to determine order.
  [ "$(printf '%s\n%s' "$1" "$2" | sort -V | head -1)" = "$2" ]
}

# Detect machine architecture and return the Go download suffix.
go_arch() {
  local arch
  arch="$(uname -m)"
  case "$arch" in
    x86_64)  echo "amd64" ;;
    aarch64|arm64) echo "arm64" ;;
    armv6l)  echo "armv6l" ;;
    i686)    echo "386" ;;
    *) echo "Unsupported architecture: $arch" >&2; exit 1 ;;
  esac
}

# ─── Step 1: Go ──────────────────────────────────────────────────────────────

section "Checking Go installation"

if [[ "$INSTALL_GO" == false ]]; then
  info "Skipping Go installation (--no-go)."
else
  CURRENT_GO_VERSION=""
  if command -v go &>/dev/null; then
    CURRENT_GO_VERSION="$(go version | grep -oP '\d+\.\d+(\.\d+)?' | head -1)"
    info "Found go ${CURRENT_GO_VERSION} at $(command -v go)"
  else
    info "Go not found on PATH."
  fi

  if [[ -n "$CURRENT_GO_VERSION" ]] && version_ge "$CURRENT_GO_VERSION" "$MIN_GO_VERSION"; then
    success "Go ${CURRENT_GO_VERSION} satisfies minimum ${MIN_GO_VERSION}. Skipping installation."
  else
    ARCH="$(go_arch)"
    TARBALL="go${GO_INSTALL_VERSION}.linux-${ARCH}.tar.gz"
    DOWNLOAD_URL="https://dl.google.com/go/${TARBALL}"

    info "Downloading Go ${GO_INSTALL_VERSION} (${ARCH}) from ${DOWNLOAD_URL} ..."
    TMP_DIR="$(mktemp -d)"
    trap 'rm -rf "${TMP_DIR}"' EXIT

    curl -fsSL "${DOWNLOAD_URL}" -o "${TMP_DIR}/${TARBALL}"

    info "Installing to /usr/local/go (requires sudo) ..."
    sudo rm -rf /usr/local/go
    sudo tar -C /usr/local -xzf "${TMP_DIR}/${TARBALL}"

    success "Go ${GO_INSTALL_VERSION} installed to /usr/local/go"
  fi
fi

# ─── Step 2: PATH setup ──────────────────────────────────────────────────────

section "Setting up PATH"

GO_BIN="/usr/local/go/bin"
GOPATH_BIN="$(go env GOPATH 2>/dev/null || echo "${HOME}/go")/bin"

# Export for the current shell session
export PATH="${GO_BIN}:${GOPATH_BIN}:${PATH}"

# Persist to ~/.bashrc if not already present
BASHRC="${HOME}/.bashrc"
ADDED_TO_BASHRC=false

add_to_bashrc() {
  local line="$1"
  if ! grep -qxF "$line" "${BASHRC}" 2>/dev/null; then
    echo "$line" >> "${BASHRC}"
    ADDED_TO_BASHRC=true
  fi
}

add_to_bashrc "export PATH=\"${GO_BIN}:\${PATH}\""
add_to_bashrc "export PATH=\"${GOPATH_BIN}:\${PATH}\""

if [[ "$ADDED_TO_BASHRC" == true ]]; then
  info "Added Go paths to ${BASHRC}"
  info "Run 'source ~/.bashrc' (or open a new terminal) to pick them up permanently."
else
  info "Go paths already present in ${BASHRC}"
fi

success "PATH: ${GO_BIN} and ${GOPATH_BIN} are active for this session."

# Verify Go is usable
go version

# ─── Step 3: OCB (OpenTelemetry Collector Builder) ───────────────────────────

section "Checking OCB (OpenTelemetry Collector Builder) v${OCB_VERSION}"

OCB_BINARY="${GOPATH_BIN}/builder"
NEED_OCB=true

if [[ -x "${OCB_BINARY}" ]]; then
  INSTALLED_OCB="$("${OCB_BINARY}" version 2>&1 | grep -oP 'v[\d.]+' | head -1 || true)"
  if [[ "${INSTALLED_OCB}" == "v${OCB_VERSION}" ]]; then
    success "OCB ${INSTALLED_OCB} already installed at ${OCB_BINARY}."
    NEED_OCB=false
  else
    info "Found OCB ${INSTALLED_OCB:-unknown} — need v${OCB_VERSION}. Reinstalling."
  fi
fi

if [[ "$NEED_OCB" == true ]]; then
  info "Installing OCB v${OCB_VERSION} ..."
  GOBIN="${GOPATH_BIN}" go install \
    "go.opentelemetry.io/collector/cmd/builder@v${OCB_VERSION}"
  success "OCB v${OCB_VERSION} installed at ${OCB_BINARY}."
fi

# ─── Step 4: Git submodules ──────────────────────────────────────────────────

section "Initialising git submodules"

cd "${ROOT_DIR}"

# Check if submodules are populated (telegraf is the largest; use it as a probe)
if [[ ! -f "${ROOT_DIR}/telegraf/go.mod" ]]; then
  info "Submodules not yet initialised. Running git submodule update --init --recursive ..."
  git submodule update --init --recursive
  success "Submodules initialised."
else
  info "Submodules already initialised."

  # Still update in case the recorded commit has advanced
  info "Syncing submodules to recorded commits ..."
  git submodule update --recursive
  success "Submodules up-to-date."
fi

# ─── Step 5: Apply patch overlays ────────────────────────────────────────────

section "Applying patch overlays to submodules"

if [[ "$APPLY_PATCHES" == false ]]; then
  info "Skipping patch application (--no-patches)."
else
  info "Restoring opentelemetry-collector patches ..."
  bash "${ROOT_DIR}/restore_otel_collector_patches.sh"

  info "Restoring opentelemetry-collector-contrib patches ..."
  bash "${ROOT_DIR}/restore_otel_collector_contrib_patches.sh"

  info "Restoring opentelemetry-proto patches ..."
  bash "${ROOT_DIR}/restore_otel_proto_patches.sh"

  info "Restoring opentelemetry-go patches ..."
  bash "${ROOT_DIR}/restore_otel_client_patches.sh"

  info "Restoring telegraf patches ..."
  bash "${ROOT_DIR}/restore_telegraf_patches.sh"

  success "All patch overlays applied."
fi

# ─── Done ────────────────────────────────────────────────────────────────────

echo
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " Setup complete."
echo ""
echo " Go:      $(go version)"
echo " OCB:     $("${OCB_BINARY}" version 2>&1)"
echo " Builder: ${OCB_BINARY}"
echo ""
echo " To build ddsketchcol:"
echo "   ./build_ddsketchcol.sh --skip-patches"
echo ""
if [[ "$ADDED_TO_BASHRC" == true ]]; then
  echo " NOTE: Run 'source ~/.bashrc' to make Go available in new terminals."
  echo ""
fi
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
