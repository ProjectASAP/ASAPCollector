#!/bin/bash
# Build all collector binaries needed for evaluation.
#
# Builds:
#   1. sketchcol — unified collector with all sketch processors
#   2. Individual sketch collectors (ddsketchcol, hllcol, etc.)
#   3. e2esdkbench — benchmark load generator
#   4. controller — Rust controller binary

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CONTRIB_DIR="$REPO_DIR/opentelemetry-collector-contrib-patch"

echo "=== Building evaluation binaries ==="
echo ""

# 1. Check for Go builder
BUILDER_BIN="${BUILDER_BIN:-$(which builder 2>/dev/null || echo "")}"
if [ -z "$BUILDER_BIN" ]; then
    echo "[1/4] Installing OTel Collector builder..."
    go install go.opentelemetry.io/collector/cmd/builder@latest
    BUILDER_BIN="$(go env GOPATH)/bin/builder"
fi
echo "[1/4] Builder: $BUILDER_BIN"

# 2. Build individual collectors
echo ""
echo "[2/4] Building individual sketch collectors..."
for col in ddsketchcol countsketchcol countminsketchcol; do
    CONFIG="$CONTRIB_DIR/cmd/$col/builder-config.yaml"
    if [ -f "$CONFIG" ]; then
        echo "  Building $col..."
        (cd "$CONTRIB_DIR" && "$BUILDER_BIN" --config "$CONFIG" 2>&1 | tail -1) || echo "  [warn] $col build failed"
    fi
done

# 3. Build e2esdkbench
echo ""
echo "[3/4] Building e2esdkbench..."
(cd "$REPO_DIR/opentelemetry-app" && go build -o "$REPO_DIR/e2esdkbench" ./cmd/e2esdkbench) || echo "  [warn] e2esdkbench build failed"

# 4. Build controller
echo ""
echo "[4/4] Building controller..."
(cd "$REPO_DIR/controller" && cargo build --release 2>&1 | tail -1) || echo "  [warn] controller build failed"

echo ""
echo "=== Build complete ==="
echo "Binaries:"
for bin in \
    "$REPO_DIR/e2esdkbench" \
    "$REPO_DIR/controller/target/release/controller" \
    "$CONTRIB_DIR/cmd/ddsketchcol/dist/ddsketchcol" \
    "$CONTRIB_DIR/cmd/countsketchcol/dist/countsketchcol" \
    "$CONTRIB_DIR/cmd/countminsketchcol/dist/countminsketchcol"; do
    if [ -f "$bin" ]; then
        echo "  [ok] $bin"
    else
        echo "  [missing] $bin"
    fi
done
