#!/bin/bash
# bench_matched_accuracy.sh – matched-accuracy quantile-sketch head-to-head.
#
# Runs the Go benchmark in matched_accuracy/ for each Zipf skew in
# {1.01, 1.5, 2.5}, all on a 1M-sample stream, and aggregates the results
# into one CSV. Companion to bench_sdk_e2e.sh.
#
# Usage:
#   ./bench_matched_accuracy.sh                       # full sweep, 1M samples each
#   ./bench_matched_accuracy.sh --n 100000            # smoke run
#   ./bench_matched_accuracy.sh --skew 1.5 --n 1000000   # one skew only
#   ./bench_matched_accuracy.sh --output results/foo.csv

set -euo pipefail
export LC_NUMERIC=C

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$SCRIPT_DIR/matched_accuracy"
OUTPUT="$BENCH_DIR/results/matched_accuracy.csv"

SKEWS=(1.01 1.5 2.5)
N=1000000
SINGLE_SKEW=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --skew)   SINGLE_SKEW="$2"; shift 2 ;;
        --n)      N="$2"; shift 2 ;;
        --output) OUTPUT="$2"; shift 2 ;;
        -h|--help)
            head -15 "$0" | grep "^#" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "Unknown argument: $1"
            exit 1
            ;;
    esac
done

if [[ -n "$SINGLE_SKEW" ]]; then
    SKEWS=("$SINGLE_SKEW")
fi

mkdir -p "$(dirname "$OUTPUT")"

echo "=========================================================="
echo "  matched-accuracy benchmark"
echo "  skews:    ${SKEWS[*]}"
echo "  samples:  $N"
echo "  output:   $OUTPUT"
echo "=========================================================="

cd "$BENCH_DIR"

first=1
for s in "${SKEWS[@]}"; do
    echo ""
    echo ">>> skew=$s"
    if [[ "$first" -eq 1 ]]; then
        go run ./ --skew "$s" --n "$N" --out "$OUTPUT"
        first=0
    else
        go run ./ --skew "$s" --n "$N" --out "$OUTPUT" --append
    fi
done

echo ""
echo "Done. Combined CSV: $OUTPUT"
