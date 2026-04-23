#!/usr/bin/env bash
# run-paper-6.2-sweeps.sh — land all three §6.2 sub-sweep CSVs
# under deploy/eval-results/three-axis/.
#
# Each sub-sweep holds two of (W, L, agg) constant and varies the
# third. See docs/sdk-aggregation-cost.md §"Paper §6
# mapping" for the plan.
#
# Produces (under eval-results/three-axis/):
#   6.2a-time-${TS}.csv
#   6.2b-label-${TS}.csv
#   6.2c-encoding-${TS}.csv
#
# Runtime at defaults (SOAK_S=180, BYTES_WIN=20): ~1 hour total.
# Override SOAK_S for a quick smoke run.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUT_DIR="${OUT_DIR:-$REPO_ROOT/deploy/eval-results/three-axis}"
mkdir -p "$OUT_DIR"

TS="${TS:-$(date +%Y%m%d)}"

# BYTES_WIN must be >= the longest EXPORTER_SDK_WINDOW we sweep, so
# every cell's sample captures at least one full reader flush. See
# PR #194 — when BYTES_WIN < W, producer_bytes_out_per_s ends up
# sampling between ticks and reports near-zero.
SOAK_S="${SOAK_S:-180}"
BYTES_WIN_DEFAULT=20
BYTES_WIN="${BYTES_WIN:-$BYTES_WIN_DEFAULT}"
CARDINALITY="${CARDINALITY:-1000}"
FREQ_HZ="${FREQ_HZ:-10}"
SCALE="${SCALE:-N1}"

echo "=============================="
echo "§6.2 three-axis sweeps — $TS"
echo "  SOAK_S=$SOAK_S  BYTES_WIN=$BYTES_WIN  CARDINALITY=$CARDINALITY"
echo "  FREQ_HZ=$FREQ_HZ  SCALE=$SCALE"
echo "  OUT_DIR=$OUT_DIR"
echo "=============================="

run_sweep() {
    local name="$1"
    local windows="$2"
    local projections="$3"
    local aggs="$4"
    local out="$OUT_DIR/${name}-${TS}.csv"
    local log="$OUT_DIR/${name}-${TS}.log"
    echo ""
    echo "==> $name → $out"
    # Passing env via the command prefix (not `env $@`) so
    # space-separated values inside WINDOWS / PROJECTIONS / AGGS
    # survive. The child script tokenises them with IFS.
    WINDOWS="$windows" PROJECTIONS="$projections" AGGS="$aggs" \
        SOAK_S="$SOAK_S" BYTES_WIN="$BYTES_WIN" \
        CARDINALITY="$CARDINALITY" FREQ_HZ="$FREQ_HZ" SCALE="$SCALE" \
        "$SCRIPT_DIR/run-three-axis-sweep.sh" > "$out" 2> "$log"
    echo "    $(wc -l < "$out") rows, $(wc -l < "$log") log lines"
}

# ── 6.2a time axis ──────────────────────────────────────────────
# Fix L=keep-all, agg=dd-full. Sweep W ∈ {1s, 15s, 60s, 300s}.
# Expect bw to drop roughly linearly with 1/W (fewer flushes)
# and sketch accuracy to hold since DDSketch's per-series
# summary converges inside each window.
run_sweep "6.2a-time" \
    "1s 15s 60s 300s" \
    ":" \
    "dd-full"

# ── 6.2b label axis ─────────────────────────────────────────────
# Fix W=60s, agg=dd-full. Sweep L from full down to empty.
# Expect bw to drop roughly linearly with reduced-cardinality
# (each dropped dim folds attribute sets together into one
# sketch).
run_sweep "6.2b-label" \
    "60s" \
    ": zone,rack,node zone,rack zone -" \
    "dd-full"

# ── 6.2c encoding axis ──────────────────────────────────────────
# Fix W=60s, L=zone,rack. Sweep encoding shape.
# Expect raw-buffer to dominate bytes (one dp per event),
# *-full to be middle (one sketch per attribute-set per window),
# *-delta to be smallest (sparse diff). kll-delta not yet
# available (see PROGRESS.md).
run_sweep "6.2c-encoding" \
    "60s" \
    "zone,rack" \
    "raw-buffer dd-full dd-delta kll cms-full cms-delta hll-full hll-delta"

echo ""
echo "=============================="
echo "done. CSVs in $OUT_DIR"
ls -la "$OUT_DIR"
