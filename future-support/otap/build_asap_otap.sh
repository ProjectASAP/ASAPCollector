#!/usr/bin/env bash
# build_asap_otap.sh — Build the asap-otap OTAP-Rust distribution.
#
# asap-otap is upstream OTAP Dataflow (the OpenTelemetry next-gen
# Arrow-native streaming engine, in-tree at
# `otel-arrow/rust/otap-dataflow/`) with the unified `asap_sketches`
# processor wired into its `linkme` plugin registry. One binary, all
# five sketch types (ddsketch / KLL / HLL / countsketch / countminsketch
# — see docs/design-asap-otap-rust-integration.md §3 for the
# rationale).
#
# This script mirrors `build_asap_otel.sh` (Phase 4 step D for
# OTel) and `build_asap_telegraf.sh` (Phase 4 step D for Telegraf).
# Steps:
#
#   1. Initialize the `otel-arrow` submodule (pinned in `.gitmodules`).
#   2. Stage `otap-patch/all/` into OTAP's workspace as a new crate
#      (`crates/asap-sketches-registry/`). The crate carries the
#      `#[distributed_slice(OTAP_PROCESSOR_FACTORIES)]` registration
#      that brings the `asap_sketches` plugin into the binary's link
#      scope.
#   3. Patch the OTAP workspace `Cargo.toml` to add the new crate as a
#      member.
#   4. Patch the binary's `Cargo.toml` and `src/main.rs` to take a
#      side-effect dep on `asap_sketches_registry` so the linkme
#      static is reachable at link time (the same pattern OTAP uses
#      for `otap_df_contrib_nodes` / `otap_df_core_nodes`).
#   5. `cargo build --release --bin df_engine` from the patched OTAP
#      source tree.
#   6. Copy the binary to `otel-arrow/rust/otap-dataflow/target/release/asap-otap`
#      so the rest of the toolchain (Dockerfile, deploy scripts) can
#      address the right name. (OTAP upstream renames take several
#      release cycles to land; copying is cheaper than carrying a
#      [[bin]] rename in our patch.)
#
# Output:
#   otel-arrow/rust/otap-dataflow/target/release/asap-otap
#
# ## Plugin-registry inspection
#
# OTAP Dataflow has no `--list-plugins` flag in its CLI. Instead, the
# binary prints the registered URN list as part of its startup banner
# via `otap_df_controller::startup::system_info()`, which clap's
# *short* help (`-h`) reproduces via `after_help`. Note that the
# *long* help (`--help`) uses `after_long_help` (the EXAMPLES block)
# and does NOT print the system info banner — use `-h`. Check the
# registration with:
#
#   ./otel-arrow/rust/otap-dataflow/target/release/asap-otap -h \
#     | grep asap_sketches
#
# A successful build prints `urn:asap:processor:asap_sketches` under
# the "Available Component URNs: Processors:" line. The §11 row D
# exit criterion ("a `asap-otap` binary that lists `asap_sketches`
# in its plugin registry") corresponds to this output.
#
# ## Prereqs
#
# - The `asap-precompute-rs` crate must build with the `otap` feature.
#   Phase C (PR #259) shipped this. Verify with:
#     cargo test -p asap-precompute-rs --features otap
# - The `otel-arrow` submodule pin in `.gitmodules` is currently
#   `29de46bb4dbff6e48b595459188f912b49373eed` (2026-05-05). The
#   pin tracks an `0.1.0`-pre-1.0 OTAP release; quarterly upgrade
#   cadence per design doc §10. To bump: update the SHA in
#   `.gitmodules`, re-run this script, run the full test suite, then
#   commit the new pointer.
#
# ## Env vars
#
# Mirroring `build_asap_otel.sh`'s "inline env vars to remove a
# footgun" treatment of `GOPRIVATE` / `GOTOOLCHAIN`, this script sets
# the Rust-side equivalents in-script:
#
# - `CARGO_NET_GIT_FETCH_WITH_CLI=true` — robust against custom
#   credential helpers / SSH-over-HTTPS rewriting.
# - `RUST_BACKTRACE=1` — surfaces clean backtraces from build-script
#   panics on first failure (linkme cross-crate registration
#   diagnostics live in Cargo's build script output).
# - `CARGO_TERM_COLOR=auto` — preserves the user's TTY default.
#
# ## Usage
#
#   ./build_asap_otap.sh                 # builds asap-otap
#   ./build_asap_otap.sh --skip-patches  # skip re-applying patches
#                                         # (useful for fast incremental
#                                         # rebuilds after editing the
#                                         # registration crate)
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

SKIP_PATCHES=false
for arg in "$@"; do
  case "$arg" in
    --skip-patches) SKIP_PATCHES=true ;;
    *) echo "Unknown argument: $arg" >&2; exit 1 ;;
  esac
done

OTAP_SUBMODULE_DIR="${ROOT_DIR}/future-support/otap/otel-arrow"
OTAP_WORKSPACE_DIR="${OTAP_SUBMODULE_DIR}/rust/otap-dataflow"
OTAP_BINARY_DIR="${OTAP_WORKSPACE_DIR}"  # main.rs + Cargo.toml live at the workspace root

PATCH_DIR="${ROOT_DIR}/future-support/otap/otap-patch"
REGISTRATION_SRC_DIR="${PATCH_DIR}/all"
REGISTRATION_DEST_DIR="${OTAP_WORKSPACE_DIR}/crates/asap-sketches-registry"

# Inline env vars per the convention `build_asap_otel.sh`
# established (follow-up: "inline env vars into script
# removes a footgun for new contributors"). Cargo respects them when
# inherited from the parent shell.
export CARGO_NET_GIT_FETCH_WITH_CLI=true
export RUST_BACKTRACE=1
export CARGO_TERM_COLOR=auto

# Step 1: Initialize submodule (idempotent; no-ops if already present).
echo "==> Ensuring otel-arrow submodule is initialized..."
if [[ ! -d "${OTAP_WORKSPACE_DIR}" ]] || [[ -z "$(ls -A "${OTAP_WORKSPACE_DIR}" 2>/dev/null)" ]]; then
  git -C "${ROOT_DIR}" submodule update --init --recursive future-support/otap/otel-arrow
fi
if [[ ! -f "${OTAP_WORKSPACE_DIR}/Cargo.toml" ]]; then
  echo "OTAP submodule did not initialize correctly at ${OTAP_WORKSPACE_DIR}" >&2
  echo "Run: git submodule update --init --recursive future-support/otap/otel-arrow" >&2
  exit 1
fi

# Step 2: Stage `otap-patch/all/` into the OTAP workspace as a new
# `asap-sketches-registry` crate. Idempotent — re-running re-copies
# files. The on-disk crate name is `asap-sketches-registry`
# (kebab-case, matching the OTAP convention), but the lib's
# `mod.rs`-as-`lib.rs` pattern keeps the file path
# `otap-patch/all/mod.rs` legible against the design doc §6 layout.
if [[ "${SKIP_PATCHES}" == false ]]; then
  echo "==> Staging registration crate into OTAP workspace..."
  rm -rf "${REGISTRATION_DEST_DIR}"
  mkdir -p "${REGISTRATION_DEST_DIR}"
  cp "${REGISTRATION_SRC_DIR}/Cargo.toml" "${REGISTRATION_DEST_DIR}/Cargo.toml"
  cp "${REGISTRATION_SRC_DIR}/mod.rs" "${REGISTRATION_DEST_DIR}/mod.rs"
fi

# Step 3: Patch OTAP's workspace Cargo.toml to declare the registration
# crate as a workspace member + dependency. The `crates/*` glob in
# OTAP's existing `members =` list already picks up the new crate; we
# only need to add the `workspace.dependencies` entry so the binary
# crate can `workspace = true` it.
WORKSPACE_TOML="${OTAP_WORKSPACE_DIR}/Cargo.toml"
if [[ "${SKIP_PATCHES}" == false ]]; then
  if ! grep -q "^asap_sketches_registry " "${WORKSPACE_TOML}"; then
    echo "==> Adding asap_sketches_registry to OTAP workspace.dependencies..."
    # Insert the dep right after the existing `otap-df-otap = { path =`
    # line so the diff stays small and locality-preserving.
    awk '
      /^otap-df-otap = \{ path = / && !done {
        print
        print "asap_sketches_registry = { path = \"crates/asap-sketches-registry\" }"
        done = 1
        next
      }
      { print }
    ' "${WORKSPACE_TOML}" > "${WORKSPACE_TOML}.tmp"
    mv "${WORKSPACE_TOML}.tmp" "${WORKSPACE_TOML}"
  fi
fi

# Step 4: Patch the binary crate's Cargo.toml to take a side-effect
# dep on the registration crate. Identical pattern to OTAP's existing
# `otap-df-contrib-nodes` / `otap-df-core-nodes` deps. Note that the
# binary's Cargo.toml IS the workspace root Cargo.toml (OTAP folds
# its `df_engine` binary into the workspace root); the grep below
# specifically detects the `.workspace = true` form so we don't get
# fooled by the `asap_sketches_registry = { path = ... }` line we
# just added in step 3 to the same file's `[workspace.dependencies]`.
BINARY_TOML="${OTAP_BINARY_DIR}/Cargo.toml"
if [[ "${SKIP_PATCHES}" == false ]]; then
  if ! grep -qE "^asap_sketches_registry\.workspace = true" "${BINARY_TOML}"; then
    echo "==> Adding asap_sketches_registry to df_engine binary deps..."
    awk '
      /^otap-df-otap\.workspace = true/ && !done {
        print
        print "asap_sketches_registry.workspace = true"
        done = 1
        next
      }
      { print }
    ' "${BINARY_TOML}" > "${BINARY_TOML}.tmp"
    mv "${BINARY_TOML}.tmp" "${BINARY_TOML}"
  fi
fi

# Step 5: Patch `src/main.rs` to take a side-effect import on the
# registration crate. Identical pattern to upstream's
# `use otap_df_contrib_nodes as _;` / `use otap_df_core_nodes as _;`
# lines (kept side-effect so the linkme distributed_slice statics
# are visible in `OTAP_PIPELINE_FACTORY` at runtime).
MAIN_RS="${OTAP_BINARY_DIR}/src/main.rs"
if [[ "${SKIP_PATCHES}" == false ]]; then
  if ! grep -q "asap_sketches_registry" "${MAIN_RS}"; then
    echo "==> Adding asap_sketches_registry side-effect import to main.rs..."
    awk '
      /^use otap_df_contrib_nodes as _;/ && !done {
        print
        print "// Keep this side-effect import so the crate is linked and its `linkme`"
        print "// distributed-slice registration (the unified `asap_sketches`"
        print "// processor) is visible in `OTAP_PIPELINE_FACTORY` at runtime."
        print "use asap_sketches_registry as _;"
        done = 1
        next
      }
      { print }
    ' "${MAIN_RS}" > "${MAIN_RS}.tmp"
    mv "${MAIN_RS}.tmp" "${MAIN_RS}"
  fi
fi

# Step 6: Build.
echo "==> Building asap-otap (cargo build --release --bin df_engine)..."
cd "${OTAP_WORKSPACE_DIR}"
cargo build --release --bin df_engine

# Step 7: Copy the binary under the asap-otap name so downstream
# tooling (Dockerfile, deploy scripts) addresses the canonical name
# without depending on an upstream Cargo.toml edit.
SRC_BIN="${OTAP_WORKSPACE_DIR}/target/release/df_engine"
DEST_BIN="${OTAP_WORKSPACE_DIR}/target/release/asap-otap"
if [[ ! -x "${SRC_BIN}" ]]; then
  echo "Build completed but binary not found at ${SRC_BIN}" >&2
  exit 1
fi
cp "${SRC_BIN}" "${DEST_BIN}"

echo ""
echo "Build successful: ${DEST_BIN}"
echo ""
echo "Verify the plugin registry contains asap_sketches:"
echo "  ${DEST_BIN} -h | grep asap_sketches"
