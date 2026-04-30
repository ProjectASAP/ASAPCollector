#!/bin/bash
# bench_cardinality_crossover.sh — sketch-vs-raw crossover sweep.
#
# Sweeps series cardinality N in {100, 1k, 10k, 100k, 1M, 5M} and records,
# for both CountSketch and CountMinSketch, the bytes produced by
# SerializeToBytes() vs. the raw cost of just emitting all (series_id,
# value) tuples. Output is a CSV at:
#   otel_collector_benchmark/cardinality_crossover/results/cardinality_crossover.csv
#
# This is pure Go, no external services required.
#
# Usage:
#   ./bench_cardinality_crossover.sh             # full sweep (minutes)
#   ./bench_cardinality_crossover.sh --smoke     # N=10k only (seconds)
#   ./bench_cardinality_crossover.sh --out PATH  # custom output CSV

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$SCRIPT_DIR/cardinality_crossover"

SMOKE=""
OUT="$BENCH_DIR/results/cardinality_crossover.csv"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --smoke)
            SMOKE="-smoke"
            OUT="$BENCH_DIR/results/cardinality_crossover_smoke.csv"
            shift
            ;;
        --out)
            OUT="$2"
            shift 2
            ;;
        -h|--help)
            sed -n '2,20p' "$0"
            exit 0
            ;;
        *)
            echo "Unknown arg: $1" >&2
            exit 2
            ;;
    esac
done

mkdir -p "$(dirname "$OUT")"

cd "$BENCH_DIR"

echo "=== building cardinality_crossover ==="
go build ./...

echo "=== running cardinality_crossover ${SMOKE:-(full sweep)} ==="
echo "    output: $OUT"
go run . $SMOKE -out "$OUT"

echo
echo "Done. CSV at: $OUT"
