#!/bin/bash
# bench_delta.sh – Delta-transmission performance benchmark.
#
# Compares full-sketch vs. sparse-delta transmission for all collector-side
# aggregation modes and all sketch types that support delta encoding.
#
# Three aggregation modes tested per sketch type:
#   sdkSketch  SDK pre-aggregates into sketches; collector receives sketch
#              payloads and (optionally) computes a delta before forwarding.
#   colBatch   SDK sends raw Float64Gauge data; collector's sketch processor
#              aggregates per batch and (optionally) emits delta payloads.
#   colWindow  SDK sends raw Float64Gauge data; collector's sketch processor
#              accumulates over a tumbling time window and (optionally) emits
#              delta payloads.
#
# Sketch types vs delta support:
#   ddsketch       ✓ delta   (batch + window)
#   kll            ✗ no delta (always full)
#   hll            ✓ delta   (window mode only)
#   countsketch    ✓ delta   (batch + window)
#   countminsketch ✓ delta   (batch + window)
#
# Metrics captured per run:
#   SDK bandwidth      gRPC wire bytes via /proc/net/dev loopback delta
#   SDK CPU            process user+sys time via syscall.Getrusage
#   SDK memory         heap allocation via runtime.ReadMemStats
#   Collector CPU      sampled from ps every second
#   Collector memory   sampled from ps every second
#
# Additionally runs deltaaccbench (Go program) to report sketch query accuracy
# and delta compression ratios without a live collector.
#
# Usage:
#   ./bench_delta.sh [--sketch <type|all>] [--mode <sdk|batch|window|all>]
#                   [--duration <Ns>] [--rates "N1 N2 ..."]
#                   [--series N] [--output-dir <path>]
#
# Examples:
#   ./bench_delta.sh --sketch all --mode all --duration 60s
#   ./bench_delta.sh --sketch countminsketch --mode batch --rates "10000 50000"

set -euo pipefail
export LC_NUMERIC=C

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CONTRIB_PATCH_DIR="$WORKSPACE_DIR/opentelemetry-collector-contrib-patch"
APP_DIR="$WORKSPACE_DIR/opentelemetry-app"
CMD_DIR="$CONTRIB_PATCH_DIR/cmd"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
SKETCH_ARG="all"
MODE_ARG="all"
DURATION_FLAG="60s"
OUTPUT_DIR="$(cd "$SCRIPT_DIR" && pwd)/benchmark_results/delta"
SERIES=1000
RATES_ARG="10000 50000"

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --sketch)     SKETCH_ARG="$2";    shift 2 ;;
        --mode)       MODE_ARG="$2";      shift 2 ;;
        --duration)   DURATION_FLAG="$2"; shift 2 ;;
        --output-dir) OUTPUT_DIR="$(cd "$(dirname "$2")" 2>/dev/null && pwd)/$(basename "$2")"; shift 2 ;;
        --series)     SERIES="$2";        shift 2 ;;
        --rates)      RATES_ARG="$2";     shift 2 ;;
        -h|--help)
            head -50 "$0" | grep "^#" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "Unknown argument: $1"; exit 1 ;;
    esac
done

DURATION_SEC=$(echo "$DURATION_FLAG" | sed 's/[^0-9]//g')
[[ -z "$DURATION_SEC" ]] && DURATION_SEC=60

# Expand "all"
if [[ "$SKETCH_ARG" == "all" ]]; then
    SKETCH_TYPES=("ddsketch" "kll" "hll" "countsketch" "countminsketch")
else
    IFS=',' read -ra SKETCH_TYPES <<< "$SKETCH_ARG"
fi

if [[ "$MODE_ARG" == "all" ]]; then
    AGG_MODES=("sdkSketch" "colBatch" "colWindow")
else
    IFS=',' read -ra AGG_MODES <<< "$MODE_ARG"
fi

read -ra RATES <<< "$RATES_ARG"

# ---------------------------------------------------------------------------
# Delta support matrix
# sketch_supports_delta <sketch> <mode> → 0=yes 1=no
# ---------------------------------------------------------------------------
sketch_supports_delta() {
    local sketch="$1" mode="$2"
    case "$sketch" in
        kll)                          return 1 ;;
        hll)
            # HLL delta is only meaningful in window mode (processor comment).
            [[ "$mode" == "colWindow" || "$mode" == "sdkSketch" ]] && return 0 || return 1 ;;
        ddsketch|countsketch|countminsketch) return 0 ;;
        *) return 1 ;;
    esac
}

# ---------------------------------------------------------------------------
# Collector binary + config resolver
# Returns: COLLECTOR_BIN, COLLECTOR_CONFIG (for delta_off), COLLECTOR_CONFIG_DELTA
# ---------------------------------------------------------------------------
resolve_collector() {
    local sketch="$1" mode="$2" delta="$3"
    case "$sketch" in
        ddsketch)
            COLLECTOR_BIN="$CMD_DIR/ddsketchcol/ddsketchcol"
            COLLECTOR_BUILD_CFG="$CMD_DIR/ddsketchcol/builder-config.yaml"
            if [[ "$delta" == "on" ]]; then
                case "$mode" in
                    colBatch)   COLLECTOR_CONFIG="$CMD_DIR/ddsketchcol/config-batch-delta.yaml" ;;
                    colWindow|sdkSketch) COLLECTOR_CONFIG="$CMD_DIR/ddsketchcol/config-window-delta.yaml" ;;
                esac
            else
                COLLECTOR_CONFIG="$CMD_DIR/ddsketchcol/config-window.yaml"
            fi
            ;;
        kll)
            COLLECTOR_BIN="$CONTRIB_PATCH_DIR/KLL"
            COLLECTOR_BUILD_CFG="$CMD_DIR/kll/build-config.yaml"
            COLLECTOR_CONFIG="$CMD_DIR/kll/config-window.yaml"
            ;;
        hll)
            COLLECTOR_BIN="$CONTRIB_PATCH_DIR/HLL"
            COLLECTOR_BUILD_CFG="$CMD_DIR/hllcol/build-config.yaml"
            if [[ "$delta" == "on" ]]; then
                COLLECTOR_CONFIG="$CMD_DIR/hllcol/config-window-delta.yaml"
            elif [[ "$mode" == "colBatch" ]]; then
                COLLECTOR_CONFIG="$CMD_DIR/hllcol/config-batch.yaml"
            else
                COLLECTOR_CONFIG="$CMD_DIR/hllcol/config-window.yaml"
            fi
            ;;
        countsketch)
            COLLECTOR_BIN="$CMD_DIR/countsketchcol/dist/countsketchcol"
            COLLECTOR_BUILD_CFG="$CMD_DIR/countsketchcol/builder-config.yaml"
            if [[ "$delta" == "on" ]]; then
                case "$mode" in
                    colBatch)   COLLECTOR_CONFIG="$CMD_DIR/countsketchcol/config-batch-delta.yaml" ;;
                    colWindow|sdkSketch) COLLECTOR_CONFIG="$CMD_DIR/countsketchcol/config-window-delta.yaml" ;;
                esac
            else
                case "$mode" in
                    colBatch)   COLLECTOR_CONFIG="$CMD_DIR/countsketchcol/config-batch.yaml" ;;
                    colWindow|sdkSketch) COLLECTOR_CONFIG="$CMD_DIR/countsketchcol/config-window.yaml" ;;
                esac
            fi
            ;;
        countminsketch)
            COLLECTOR_BIN="$CMD_DIR/countminsketchcol/dist/countminsketchcol"
            COLLECTOR_BUILD_CFG="$CMD_DIR/countminsketchcol/builder-config.yaml"
            if [[ "$delta" == "on" ]]; then
                case "$mode" in
                    colBatch)   COLLECTOR_CONFIG="$CMD_DIR/countminsketchcol/config-batch-delta.yaml" ;;
                    colWindow|sdkSketch) COLLECTOR_CONFIG="$CMD_DIR/countminsketchcol/config-window-delta.yaml" ;;
                esac
            else
                case "$mode" in
                    colBatch)   COLLECTOR_CONFIG="$CMD_DIR/countminsketchcol/config-batch.yaml" ;;
                    colWindow|sdkSketch) COLLECTOR_CONFIG="$CMD_DIR/countminsketchcol/config-window.yaml" ;;
                esac
            fi
            ;;
        baseline)
            COLLECTOR_BIN="$CMD_DIR/nopcol/dist/nopcol"
            COLLECTOR_BUILD_CFG="$CMD_DIR/nopcol/builder-config.yaml"
            COLLECTOR_CONFIG="$CMD_DIR/nopcol/config-bench.yaml"
            ;;
        *)
            echo "[ERROR] Unknown sketch: $sketch"; exit 1 ;;
    esac
}

# ---------------------------------------------------------------------------
# Build collector binary if missing
# ---------------------------------------------------------------------------
build_collector() {
    local sketch="$1"
    resolve_collector "$sketch" "sdkSketch" "off"
    if [[ -f "$COLLECTOR_BIN" && "${BUILD_ALWAYS:-0}" != "1" ]]; then
        echo "  -> Using existing: $COLLECTOR_BIN"
        return
    fi
    echo "  -> Building collector for $sketch ..."
    BUILDER_BIN="${BUILDER_BIN:-$HOME/go/bin/builder}"
    if [[ ! -x "$BUILDER_BIN" ]]; then
        echo "[ERROR] OTel builder not found at $BUILDER_BIN"; exit 1
    fi
    cd "$CONTRIB_PATCH_DIR"
    GONOSUMDB="github.com/ProjectASAP/*" GOPRIVATE="github.com/ProjectASAP/*" \
        "$BUILDER_BIN" --config "$COLLECTOR_BUILD_CFG"
    echo "  -> Build done: $COLLECTOR_BIN"
}

# ---------------------------------------------------------------------------
# Collector resource monitor → col_<tag>_resource.csv
# ---------------------------------------------------------------------------
start_col_monitor() {
    local pid="$1" outfile="$2"
    echo "timestamp,cpu_percent,mem_mb" > "$outfile"
    (
        while kill -0 "$pid" 2>/dev/null; do
            stats=$(ps -p "$pid" -o %cpu,rss --no-headers 2>/dev/null | awk '{$1=$1};1')
            if [[ -n "$stats" ]]; then
                cpu=$(echo "$stats" | cut -d' ' -f1)
                mem_kb=$(echo "$stats" | cut -d' ' -f2)
                [[ -z "$cpu" ]]    && cpu=0
                [[ -z "$mem_kb" ]] && mem_kb=0
                mem_mb=$(echo "scale=2; $mem_kb / 1024" | bc)
                echo "$(date +%s),$cpu,$mem_mb" >> "$outfile"
            fi
            sleep 1
        done
    ) &
    COL_MONITOR_PID=$!
}

# ---------------------------------------------------------------------------
# Parse avg/peak from a two-column CSV (timestamp, value)
# ---------------------------------------------------------------------------
csv_stats() {
    local file="$1" col="$2"
    awk -F',' -v c="$col" '
        NR>1 { val=$c+0; sum+=val; if(val>pk) pk=val; cnt++ }
        END  { if(cnt>0) printf "%.2f %.2f", sum/cnt, pk; else printf "0 0" }
    ' "$file"
}

# ---------------------------------------------------------------------------
# Parse SDK summary JSON
# ---------------------------------------------------------------------------
sdk_json_field() {
    local json="$1" field="$2"
    python3 -c "import json,sys; d=json.load(open('$json')); print(d.get('$field', 0))" 2>/dev/null || echo 0
}

# ---------------------------------------------------------------------------
# Single e2e run: start collector → run SDK bench → stop → collect stats
# Returns result row in RUN_RESULT (space-separated)
# ---------------------------------------------------------------------------
run_one() {
    local sketch="$1" mode="$2" delta="$3" rate="$4" outdir="$5"

    local tag="${sketch}_${mode}_delta${delta}_${rate}mps"

    resolve_collector "$sketch" "$mode" "$delta"

    # Free ports.
    lsof -ti:4317 | xargs kill -9 2>/dev/null || true
    lsof -ti:8888 | xargs kill -9 2>/dev/null || true
    lsof -ti:8889 | xargs kill -9 2>/dev/null || true
    sleep 2

    # Start collector.
    cd "$CONTRIB_PATCH_DIR"
    "$COLLECTOR_BIN" --config "$COLLECTOR_CONFIG" \
        > "$outdir/collector_${tag}.log" 2>&1 &
    COLLECTOR_PID=$!
    echo "    Collector PID=$COLLECTOR_PID (warming up 5s)..."
    sleep 5

    if ! kill -0 "$COLLECTOR_PID" 2>/dev/null; then
        echo "    [ERROR] Collector failed to start; check $outdir/collector_${tag}.log"
        RUN_RESULT="ERROR"
        return
    fi

    # Start resource monitor.
    COL_RES_CSV="$outdir/col_${tag}_resource.csv"
    start_col_monitor "$COLLECTOR_PID" "$COL_RES_CSV"

    # Determine SDK sketch type and load pattern.
    # For sdkSketch mode the SDK pre-aggregates; for colBatch/colWindow it sends raw gauges.
    local sdk_sketch_type="$sketch"
    if [[ "$mode" == "colBatch" || "$mode" == "colWindow" ]]; then
        sdk_sketch_type="baseline"
    fi

    local samples_per_sec
    samples_per_sec=$(echo "scale=6; $rate / $SERIES" | bc)

    echo "    Running SDK bench (sketch=$sdk_sketch_type, mode=$mode, delta=$delta, rate=$rate)..."
    cd "$APP_DIR"
    GONOSUMDB="github.com/ProjectASAP/*" GOPRIVATE="github.com/ProjectASAP/*" \
        go run ./cmd/e2esdkbench \
            --sketch-type="$sdk_sketch_type" \
            --endpoint="localhost:4317" \
            --series="$SERIES" \
            --samples-per-sec-per-series="$samples_per_sec" \
            --duration="$DURATION_FLAG" \
            --rate-label="$rate" \
            --output-dir="$outdir" \
        2>&1 | tee "$outdir/sdk_${tag}.log"
    SDK_EXIT=${PIPESTATUS[0]}

    # Stop collector.
    kill "$COL_MONITOR_PID" 2>/dev/null || true
    kill "$COLLECTOR_PID"   2>/dev/null || true
    wait "$COLLECTOR_PID"   2>/dev/null || true
    sleep 1

    if [[ "$SDK_EXIT" -ne 0 ]]; then
        echo "    [WARNING] SDK exited with $SDK_EXIT"
    fi

    # Parse results.
    SDK_JSON="$outdir/${sdk_sketch_type}_${rate}mps_summary.json"
    if [[ ! -f "$SDK_JSON" ]]; then
        echo "    [WARNING] SDK summary not found: $SDK_JSON"
        RUN_RESULT="MISSING"
        return
    fi

    local bw_avg bw_peak heap_avg heap_peak cpu_pct
    bw_avg=$(sdk_json_field   "$SDK_JSON" avg_bandwidth_bps)
    bw_peak=$(sdk_json_field  "$SDK_JSON" peak_bandwidth_bps)
    heap_avg=$(sdk_json_field "$SDK_JSON" avg_heap_alloc_mb)
    heap_peak=$(sdk_json_field "$SDK_JSON" peak_heap_alloc_mb)
    cpu_pct=$(sdk_json_field  "$SDK_JSON" sdk_cpu_percent)

    local col_cpu_avg col_cpu_peak col_mem_avg col_mem_peak
    read col_cpu_avg col_cpu_peak <<< "$(csv_stats "$COL_RES_CSV" 2)"
    read col_mem_avg col_mem_peak <<< "$(csv_stats "$COL_RES_CSV" 3)"

    # Convert bw to KB/s.
    local bw_avg_kb bw_peak_kb
    bw_avg_kb=$(echo "scale=2; $bw_avg / 1024" | bc)
    bw_peak_kb=$(echo "scale=2; $bw_peak / 1024" | bc)

    RUN_RESULT="$sketch $mode $delta $rate $bw_avg_kb $bw_peak_kb $heap_avg $heap_peak $cpu_pct $col_cpu_avg $col_cpu_peak $col_mem_avg $col_mem_peak"
}

# ---------------------------------------------------------------------------
# Print aggregate results table
# ---------------------------------------------------------------------------
print_header() {
    printf "\n| %-14s | %-10s | %-6s | %8s | %11s | %11s | %9s | %9s | %9s | %9s |\n" \
        "Sketch" "AggMode" "Delta" "Rate MPS" \
        "BW avg KB/s" "BW pk KB/s" \
        "Heap avg" "SDK CPU%" \
        "Col CPU%" "Col Mem MB"
    printf "|%s|\n" "$(printf '%.0s-' {1..120})"
}

print_row() {
    local sk="$1" mode="$2" delta="$3" rate="$4" bw_avg="$5" bw_peak="$6" \
          heap_avg="$7" heap_peak="$8" sdk_cpu="$9" col_cpu="${10}" \
          col_cpu_peak="${11}" col_mem="${12}" col_mem_peak="${13}"

    printf "| %-14s | %-10s | %-6s | %8s | %11s | %11s | %9s | %9s | %9s | %9s |\n" \
        "$sk" "$mode" "$delta" "$rate" \
        "${bw_avg} KB/s" "${bw_peak} KB/s" \
        "${heap_avg} MB" "${sdk_cpu}%" \
        "${col_cpu}%" "${col_mem} MB"
}

# ---------------------------------------------------------------------------
# Write aggregate CSV
# ---------------------------------------------------------------------------
write_aggregate_csv() {
    local csvfile="$OUTPUT_DIR/aggregate_delta_summary.csv"
    {
        echo "sketch_type,agg_mode,delta,rate_mps,bw_avg_kbps,bw_peak_kbps,heap_avg_mb,heap_peak_mb,sdk_cpu_pct,col_cpu_avg_pct,col_cpu_peak_pct,col_mem_avg_mb,col_mem_peak_mb"
        for row in "${ALL_ROWS[@]}"; do
            echo "$row" | tr ' ' ','
        done
    } > "$csvfile"
    echo ""
    echo "Aggregate CSV: $csvfile"
}

# ---------------------------------------------------------------------------
# Run accuracy benchmark (self-contained Go program)
# ---------------------------------------------------------------------------
run_accuracy_bench() {
    local acc_outdir="$OUTPUT_DIR/accuracy"
    mkdir -p "$acc_outdir"

    echo ""
    echo "=========================================================="
    echo "  RUNNING ACCURACY BENCHMARK (no live collector needed)"
    echo "=========================================================="

    cd "$APP_DIR"
    GONOSUMDB="github.com/ProjectASAP/*" GOPRIVATE="github.com/ProjectASAP/*" \
        go run ./cmd/deltaaccbench \
            --windows=10 \
            --inserts=2000 \
            --zipf-s=1.1 \
            --zipf-max=5000 \
            --delta-threshold=1.0 \
            --output-dir="$acc_outdir" \
        2>&1 | tee "$acc_outdir/deltaaccbench.log"

    echo ""
    echo "  Accuracy results: $acc_outdir"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
mkdir -p "$OUTPUT_DIR"

echo ""
echo "=========================================================="
echo "  DELTA TRANSMISSION BENCHMARK"
echo "  Sketches: ${SKETCH_TYPES[*]}"
echo "  Modes:    ${AGG_MODES[*]}"
echo "  Rates:    ${RATES[*]} MPS"
echo "  Duration: $DURATION_FLAG per run"
echo "  Output:   $OUTPUT_DIR"
echo "=========================================================="

# Build all required collectors.
echo ""
echo ">>> Building collectors..."
for sketch in "${SKETCH_TYPES[@]}"; do
    build_collector "$sketch"
done

# Storage for all result rows.
declare -a ALL_ROWS

print_header

for sketch in "${SKETCH_TYPES[@]}"; do
    for mode in "${AGG_MODES[@]}"; do
        local_outdir="$OUTPUT_DIR/${sketch}_${mode}"
        mkdir -p "$local_outdir"

        echo ""
        echo "----------------------------------------------------------"
        echo "  Sketch: $sketch  Mode: $mode"
        echo "----------------------------------------------------------"

        # Determine which delta variants to test.
        local_deltas=("off")
        if sketch_supports_delta "$sketch" "$mode"; then
            local_deltas=("off" "on")
        fi

        for delta in "${local_deltas[@]}"; do
            for rate in "${RATES[@]}"; do
                echo ""
                echo "  >> sketch=$sketch  mode=$mode  delta=$delta  rate=$rate MPS"

                run_one "$sketch" "$mode" "$delta" "$rate" "$local_outdir"

                if [[ "$RUN_RESULT" == "ERROR" || "$RUN_RESULT" == "MISSING" ]]; then
                    echo "  [SKIP] Run did not complete successfully."
                    continue
                fi

                # Append to aggregate.
                ALL_ROWS+=("$RUN_RESULT")

                # Print table row.
                read -ra FIELDS <<< "$RUN_RESULT"
                print_row "${FIELDS[@]}"
            done
        done
    done
done

# Write aggregate CSV.
write_aggregate_csv

# Run accuracy benchmark.
run_accuracy_bench

echo ""
echo "=========================================================="
echo "  DONE. All results in: $OUTPUT_DIR"
echo "=========================================================="
