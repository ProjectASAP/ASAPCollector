#!/bin/bash
# =============================================================================
# Paper §6 Evaluation Driver
# =============================================================================
#
# Produces all tables for the paper's evaluation section:
#   Table 3: Per-sketch bandwidth, CPU, memory, accuracy
#   Table 4: Scalability (vary series count)
#   Table 5: Delta compression ablation
#   Table 6: TCO comparison vs baseline
#
# Usage:
#   ./evaluation/run_eval.sh [--quick]     # --quick: short runs for CI
#
# Prerequisites:
#   - Go 1.22+
#   - Built collector binaries (run ./evaluation/build_collectors.sh first)
#   - Controller binary (cargo build --release -p controller)
#
# Output:
#   evaluation/results/<timestamp>/
#     table3_per_sketch.csv
#     table4_scalability.csv
#     table5_delta_ablation.csv
#     table6_tco.json
#     raw/  (per-run CSVs and summaries)
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results/$(date +%Y%m%d_%H%M%S)"
RAW_DIR="$RESULTS_DIR/raw"
mkdir -p "$RAW_DIR"

# Quick mode for CI
QUICK="${1:-}"
if [ "$QUICK" = "--quick" ]; then
    DURATION="10s"
    SERIES_LIST="100"
    SPS=10
    echo "[quick mode] duration=${DURATION}, series=${SERIES_LIST}, sps=${SPS}"
else
    DURATION="60s"
    SERIES_LIST="100 500 1000 5000 10000"
    SPS=10
fi

BENCH_APP="$REPO_DIR/opentelemetry-app/cmd/e2esdkbench"
BENCH_BIN="$REPO_DIR/e2esdkbench"
CONTROLLER_BIN="$REPO_DIR/controller/target/release/controller"

SKETCH_TYPES="ddsketch kll hll countsketch countminsketch baseline"

# =============================================================================
# Helper: run a single benchmark
# =============================================================================
run_bench() {
    local sketch="$1"
    local series="$2"
    local sps="$3"
    local duration="$4"
    local output_dir="$5"
    local extra_flags="${6:-}"

    echo "  [bench] sketch=$sketch series=$series sps=$sps duration=$duration"

    # Use e2esdkbench for SDK-level metrics (bandwidth, CPU, memory)
    "$BENCH_BIN" \
        --sketch-type="$sketch" \
        --endpoint="localhost:4317" \
        --series="$series" \
        --samples-per-sec-per-series="$sps" \
        --duration="$duration" \
        --output-dir="$output_dir" \
        $extra_flags \
        2>&1 | tee "$output_dir/${sketch}_${series}s_${sps}sps.log"
}

# =============================================================================
# Helper: extract metrics from summary JSON
# =============================================================================
extract_summary() {
    local json_file="$1"
    local field="$2"
    python3 -c "
import json, sys
with open('$json_file') as f:
    d = json.load(f)
print(d.get('$field', 'N/A'))
" 2>/dev/null || echo "N/A"
}

# =============================================================================
# Build bench tool if needed
# =============================================================================
echo "=== Building e2esdkbench ==="
if [ ! -f "$BENCH_BIN" ]; then
    (cd "$REPO_DIR/opentelemetry-app" && go build -o "$BENCH_BIN" ./cmd/e2esdkbench)
fi

# =============================================================================
# Table 3: Per-Sketch Bandwidth, CPU, Memory
# =============================================================================
echo ""
echo "=== Table 3: Per-Sketch Performance ==="
echo "sketch,series,sps,bandwidth_bps,cpu_user_ms,cpu_sys_ms,heap_mb,duration_s" > "$RESULTS_DIR/table3_per_sketch.csv"

SERIES_DEFAULT=500
for sketch in $SKETCH_TYPES; do
    outdir="$RAW_DIR/table3_${sketch}"
    mkdir -p "$outdir"

    # Start collector with the right config
    CONFIG="$REPO_DIR/opentelemetry-app/config-${sketch}.yaml"
    if [ "$sketch" = "baseline" ]; then
        CONFIG="$REPO_DIR/opentelemetry-app/config-baseline.yaml"
    fi

    if [ -f "$CONFIG" ]; then
        echo "  [table3] Running $sketch..."
        # NOTE: In a full run, we'd start the collector here.
        # For now, just record the config path.
        echo "# Config: $CONFIG" > "$outdir/config_used.txt"

        # If collector is already running on :4317, run the bench directly.
        # Otherwise, this is a dry-run that records what would be measured.
        if nc -z localhost 4317 2>/dev/null; then
            run_bench "$sketch" "$SERIES_DEFAULT" "$SPS" "$DURATION" "$outdir"

            # Extract from summary JSON
            SUMMARY=$(ls "$outdir"/*summary.json 2>/dev/null | head -1)
            if [ -n "$SUMMARY" ]; then
                BW=$(extract_summary "$SUMMARY" "total_grpc_bytes")
                CPU_U=$(extract_summary "$SUMMARY" "cpu_user_ms")
                CPU_S=$(extract_summary "$SUMMARY" "cpu_sys_ms")
                HEAP=$(extract_summary "$SUMMARY" "peak_heap_mb")
                DUR=$(extract_summary "$SUMMARY" "duration_secs")
                echo "$sketch,$SERIES_DEFAULT,$SPS,$BW,$CPU_U,$CPU_S,$HEAP,$DUR" >> "$RESULTS_DIR/table3_per_sketch.csv"
            fi
        else
            echo "  [skip] No collector on :4317 — record dry-run entry"
            echo "$sketch,$SERIES_DEFAULT,$SPS,N/A,N/A,N/A,N/A,N/A" >> "$RESULTS_DIR/table3_per_sketch.csv"
        fi
    else
        echo "  [skip] No config for $sketch at $CONFIG"
        echo "$sketch,$SERIES_DEFAULT,$SPS,N/A,N/A,N/A,N/A,N/A" >> "$RESULTS_DIR/table3_per_sketch.csv"
    fi
done

# =============================================================================
# Table 4: Scalability (vary series count)
# =============================================================================
echo ""
echo "=== Table 4: Scalability ==="
echo "sketch,series,sps,bandwidth_bps,throughput_mps" > "$RESULTS_DIR/table4_scalability.csv"

for series in $SERIES_LIST; do
    for sketch in ddsketch countsketch baseline; do
        outdir="$RAW_DIR/table4_${sketch}_${series}s"
        mkdir -p "$outdir"

        if nc -z localhost 4317 2>/dev/null; then
            run_bench "$sketch" "$series" "$SPS" "$DURATION" "$outdir"
            SUMMARY=$(ls "$outdir"/*summary.json 2>/dev/null | head -1)
            if [ -n "$SUMMARY" ]; then
                BW=$(extract_summary "$SUMMARY" "total_grpc_bytes")
                THROUGHPUT=$(extract_summary "$SUMMARY" "actual_mps")
                echo "$sketch,$series,$SPS,$BW,$THROUGHPUT" >> "$RESULTS_DIR/table4_scalability.csv"
            fi
        else
            echo "$sketch,$series,$SPS,N/A,N/A" >> "$RESULTS_DIR/table4_scalability.csv"
        fi
    done
done

# =============================================================================
# Table 5: Delta Compression Ablation
# =============================================================================
echo ""
echo "=== Table 5: Delta Ablation ==="
echo "sketch,mode,bandwidth_bps,compression_ratio" > "$RESULTS_DIR/table5_delta_ablation.csv"

for sketch in ddsketch countsketch countminsketch; do
    for mode in full delta; do
        outdir="$RAW_DIR/table5_${sketch}_${mode}"
        mkdir -p "$outdir"

        if nc -z localhost 4317 2>/dev/null; then
            EXTRA=""
            if [ "$mode" = "delta" ]; then
                EXTRA="--delta"
            fi
            run_bench "$sketch" "500" "$SPS" "$DURATION" "$outdir" "$EXTRA"
            SUMMARY=$(ls "$outdir"/*summary.json 2>/dev/null | head -1)
            if [ -n "$SUMMARY" ]; then
                BW=$(extract_summary "$SUMMARY" "total_grpc_bytes")
                echo "$sketch,$mode,$BW,N/A" >> "$RESULTS_DIR/table5_delta_ablation.csv"
            fi
        else
            echo "$sketch,$mode,N/A,N/A" >> "$RESULTS_DIR/table5_delta_ablation.csv"
        fi
    done
done

# =============================================================================
# Table 6: TCO Comparison (via controller API)
# =============================================================================
echo ""
echo "=== Table 6: TCO ==="

if nc -z localhost 8080 2>/dev/null; then
    curl -s -X POST http://localhost:8080/api/v1/tco \
        -H "Content-Type: application/json" \
        -d '{
            "workload": {
                "series_count": 100000,
                "samples_per_sec": 10.0,
                "bytes_per_sample": 100,
                "scrape_interval_secs": 15,
                "queries_per_sec": 1.0,
                "query_window_secs": 300,
                "retention_days": 30,
                "sketch_compression_ratio": 0.05,
                "delta_compression_ratio": 0.3
            }
        }' | python3 -m json.tool > "$RESULTS_DIR/table6_tco.json"
    echo "  TCO saved to $RESULTS_DIR/table6_tco.json"
else
    echo "  [skip] Controller not running on :8080"
    echo '{"note": "controller not running, run with: cargo run --release -p controller"}' > "$RESULTS_DIR/table6_tco.json"
fi

# =============================================================================
# Summary
# =============================================================================
echo ""
echo "=== Results ==="
echo "Output directory: $RESULTS_DIR"
echo ""
for f in "$RESULTS_DIR"/table*.csv "$RESULTS_DIR"/table*.json; do
    if [ -f "$f" ]; then
        echo "--- $(basename $f) ---"
        head -5 "$f"
        echo ""
    fi
done

echo "Done. Use these CSVs/JSONs for paper §6 tables."
