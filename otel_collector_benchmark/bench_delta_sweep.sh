#!/bin/bash
# bench_delta_sweep.sh -- Sweep window_duration x delta_threshold for
# delta-transmission enabled sketch collectors.
#
# This script extends bench_delta.sh (which runs ONE configuration) into a
# matrix of cells. For every (processor, window, threshold) cell it:
#   1. Renders delta_sweep_config_template.yaml with the cell's knobs.
#   2. Starts the collector binary with that config.
#   3. Runs e2esdkbench at a fixed nominal rate for $DURATION_SEC seconds.
#   4. Samples collector CPU%, RSS via ps, and bandwidth out via a loopback
#      byte counter on the prometheus exporter port (8889).
#   5. Tears the collector down, parses results, appends one CSV row.
#
# Outputs (under --output-dir, default results/delta_sweep):
#   results/delta_sweep.csv -- machine-readable matrix
#   results/delta_sweep.md  -- markdown summary table per processor
#   results/<processor>_w<window>_t<threshold>/  -- raw per-cell logs
#
# Usage:
#   ./bench_delta_sweep.sh                            # full sweep, defaults
#   ./bench_delta_sweep.sh --smoke                    # 1 cell, 10s, plumbing test
#   ./bench_delta_sweep.sh --processors countminsketch \
#                          --windows "1s 5s 30s 5m"   \
#                          --thresholds "0 0.1 1.0"   \
#                          --duration 60s --rate 30000
#
# Defaults are intentionally SMALL (10s/cell) so the script doubles as a
# smoke test. To reproduce the paper figures, pass --duration 60s --rate
# 30000 (and consider --warmup 10).

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
TEMPLATE="$SCRIPT_DIR/delta_sweep_config_template.yaml"

# ---------------------------------------------------------------------------
# Defaults (small -> smoke-friendly)
# ---------------------------------------------------------------------------
PROCESSORS_ARG="countminsketch countsketch"
WINDOWS_ARG="1s 5s 30s 5m"
THRESHOLDS_ARG="0 0.1 1.0"
DURATION_FLAG="10s"     # paper: 60s
RATE=30000              # nominal MPS of synthetic load
SERIES=1000
WARMUP=3                # seconds between collector start and load gen
SMOKE=0
OUTPUT_DIR="$SCRIPT_DIR/results/delta_sweep"

# ---------------------------------------------------------------------------
# Arg parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --processors)   PROCESSORS_ARG="$2"; shift 2 ;;
        --windows)      WINDOWS_ARG="$2";    shift 2 ;;
        --thresholds)   THRESHOLDS_ARG="$2"; shift 2 ;;
        --duration)     DURATION_FLAG="$2";  shift 2 ;;
        --rate)         RATE="$2";           shift 2 ;;
        --series)       SERIES="$2";         shift 2 ;;
        --warmup)       WARMUP="$2";         shift 2 ;;
        --output-dir)   OUTPUT_DIR="$2";     shift 2 ;;
        --smoke)        SMOKE=1;             shift   ;;
        -h|--help)
            sed -n '1,40p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

# Smoke mode = the literal smallest matrix that exercises the plumbing.
if [[ "$SMOKE" -eq 1 ]]; then
    PROCESSORS_ARG="countminsketch"
    WINDOWS_ARG="1s"
    THRESHOLDS_ARG="0"
    DURATION_FLAG="10s"
fi

DURATION_SEC=$(echo "$DURATION_FLAG" | sed 's/[^0-9]//g')
[[ -z "$DURATION_SEC" ]] && DURATION_SEC=10

read -ra PROCESSORS <<< "$PROCESSORS_ARG"
read -ra WINDOWS    <<< "$WINDOWS_ARG"
read -ra THRESHOLDS <<< "$THRESHOLDS_ARG"

mkdir -p "$OUTPUT_DIR"
CSV="$OUTPUT_DIR/delta_sweep.csv"
MD="$OUTPUT_DIR/delta_sweep.md"

echo "=========================================================="
echo "  DELTA SWEEP"
echo "  Processors: ${PROCESSORS[*]}"
echo "  Windows:    ${WINDOWS[*]}"
echo "  Thresholds: ${THRESHOLDS[*]}"
echo "  Duration:   $DURATION_FLAG  Rate: $RATE MPS  Series: $SERIES"
echo "  Output:     $OUTPUT_DIR"
[[ "$SMOKE" -eq 1 ]] && echo "  *** SMOKE MODE ***"
echo "=========================================================="

# ---------------------------------------------------------------------------
# Per-processor metadata: binary path + processor name + processor-specific
# YAML block to inject into the template.
# ---------------------------------------------------------------------------
processor_binary() {
    case "$1" in
        countminsketch) echo "$CMD_DIR/countminsketchcol/dist/countminsketchcol" ;;
        countsketch)    echo "$CMD_DIR/countsketchcol/dist/countsketchcol" ;;
        *) echo ""; return 1 ;;
    esac
}

processor_yaml_name() {
    case "$1" in
        countminsketch) echo "countmin" ;;
        countsketch)    echo "countsketch" ;;
    esac
}

processor_block() {
    # Indented 4 spaces because the template indents the processor body
    # under "${PROCESSOR}:".
    case "$1" in
        countminsketch)
            cat <<'EOF'
    mode: window
    metric_name: "countmin_sketch"
    rows: 5
    columns: 2000
    group_by: []
EOF
            ;;
        countsketch)
            cat <<'EOF'
    mode: window
    epsilon: 0.02
    delta: 0.99
EOF
            ;;
    esac
}

# ---------------------------------------------------------------------------
# Bandwidth sampler -- counts bytes sent on loopback via /proc/net/dev
# Bytes ATTRIBUTABLE to the collector are approximated by tracking the
# scrape port the prometheus exporter uses (8889): every successful scrape
# from a downstream system would generate traffic. Without a downstream
# scraper running we instead report "bytes generated" by polling /metrics
# ourselves once per second (so the sample size = response size + http
# framing). This gives a stable proxy for the per-window emit cost.
# ---------------------------------------------------------------------------
sample_bandwidth() {
    local outfile="$1"
    local pid="$2"
    echo "timestamp,bytes" > "$outfile"
    (
        while kill -0 "$pid" 2>/dev/null; do
            local size
            size=$(curl -s --max-time 1 -o /dev/null -w '%{size_download}' http://127.0.0.1:8889/metrics 2>/dev/null || echo 0)
            echo "$(date +%s),$size" >> "$outfile"
            sleep 1
        done
    ) &
    BW_MONITOR_PID=$!
}

# Collector CPU / RSS sampler.
sample_collector() {
    local pid="$1" outfile="$2"
    echo "timestamp,cpu_percent,rss_kb" > "$outfile"
    (
        while kill -0 "$pid" 2>/dev/null; do
            local stats cpu rss
            stats=$(ps -p "$pid" -o %cpu=,rss= 2>/dev/null | awk '{$1=$1};1')
            cpu=$(echo "$stats" | awk '{print $1+0}')
            rss=$(echo "$stats" | awk '{print $2+0}')
            echo "$(date +%s),$cpu,$rss" >> "$outfile"
            sleep 1
        done
    ) &
    COL_MONITOR_PID=$!
}

# Average column 2 of a CSV (skip header).
csv_avg() {
    awk -F',' 'NR>1 { sum+=$2; n++ } END { if(n>0) printf "%.2f", sum/n; else printf "0" }' "$1"
}

csv_peak() {
    awk -F',' 'NR>1 { if($2+0 > pk) pk=$2+0 } END { printf "%.2f", pk }' "$1"
}

# ---------------------------------------------------------------------------
# Header for CSV
# ---------------------------------------------------------------------------
echo "processor,window,threshold,bandwidth_bytes_per_sec,cpu_pct,heap_peak_mb,output_input_ratio,duration_sec,rate_mps,status" > "$CSV"

# ---------------------------------------------------------------------------
# Render config + run one cell
# ---------------------------------------------------------------------------
run_cell() {
    local proc="$1" window="$2" threshold="$3"
    local bin proc_yaml block
    bin=$(processor_binary "$proc")
    proc_yaml=$(processor_yaml_name "$proc")
    block=$(processor_block "$proc")

    local cell_dir="$OUTPUT_DIR/${proc}_w${window}_t${threshold}"
    mkdir -p "$cell_dir"
    local cfg="$cell_dir/config.yaml"

    # Render the template. We use python because envsubst collapses newlines
    # inside ${PROC_BLOCK}, which would break YAML.
    TEMPLATE_PATH="$TEMPLATE" CFG_PATH="$cfg" PROC_NAME="$proc_yaml" \
    WIN_VAL="$window" THR_VAL="$threshold" BLOCK_VAL="$block" \
    python3 -c '
import os
tmpl = open(os.environ["TEMPLATE_PATH"]).read()
out = (tmpl
    .replace("${PROCESSOR}", os.environ["PROC_NAME"])
    .replace("${WINDOW}",    os.environ["WIN_VAL"])
    .replace("${THRESHOLD}", os.environ["THR_VAL"])
    .replace("${PROC_BLOCK}", os.environ["BLOCK_VAL"].rstrip()))
open(os.environ["CFG_PATH"], "w").write(out)
'

    echo ""
    echo "----------------------------------------------------------"
    echo "  CELL  proc=$proc  window=$window  threshold=$threshold"
    echo "  cfg=$cfg"
    echo "----------------------------------------------------------"

    # Pre-flight: binary must exist.
    if [[ ! -x "$bin" ]]; then
        echo "  [SKIP] collector binary missing: $bin"
        echo "$proc,$window,$threshold,0,0,0,0,$DURATION_SEC,$RATE,SKIP_NO_BINARY" >> "$CSV"
        return
    fi

    # Free ports from any prior run.
    for port in 4317 4318 8888 8889; do
        lsof -ti:"$port" 2>/dev/null | xargs -r kill -9 2>/dev/null || true
    done
    sleep 1

    # Start collector.
    "$bin" --config "$cfg" > "$cell_dir/collector.log" 2>&1 &
    local cpid=$!
    sleep "$WARMUP"

    if ! kill -0 "$cpid" 2>/dev/null; then
        echo "  [ERROR] collector exited during warmup; see $cell_dir/collector.log"
        echo "$proc,$window,$threshold,0,0,0,0,$DURATION_SEC,$RATE,COLLECTOR_DIED" >> "$CSV"
        return
    fi

    # Start samplers.
    sample_collector "$cpid" "$cell_dir/collector_resource.csv"
    sample_bandwidth "$cell_dir/bandwidth.csv" "$cpid"

    # Run load gen.
    local samples_per_sec
    samples_per_sec=$(echo "scale=6; $RATE / $SERIES" | bc)

    pushd "$APP_DIR" >/dev/null
    GONOSUMDB="github.com/ProjectASAP/*" GOPRIVATE="github.com/ProjectASAP/*" \
        go run ./cmd/e2esdkbench \
            --sketch-type=baseline \
            --endpoint=localhost:4317 \
            --series="$SERIES" \
            --samples-per-sec-per-series="$samples_per_sec" \
            --duration="$DURATION_FLAG" \
            --rate-label="$RATE" \
            --output-dir="$cell_dir" \
        > "$cell_dir/sdk.log" 2>&1 || echo "  [WARN] sdk bench non-zero exit"
    popd >/dev/null

    # Stop monitors + collector.
    kill "$BW_MONITOR_PID"  2>/dev/null || true
    kill "$COL_MONITOR_PID" 2>/dev/null || true
    kill "$cpid"            2>/dev/null || true
    wait "$cpid"            2>/dev/null || true
    sleep 1

    # Parse stats.
    local bw_avg cpu_avg rss_peak rss_mb sdk_in sdk_out ratio
    bw_avg=$(csv_avg "$cell_dir/bandwidth.csv")
    cpu_avg=$(awk -F',' 'NR>1 { sum+=$2; n++ } END { if(n>0) printf "%.2f", sum/n; else printf "0" }' "$cell_dir/collector_resource.csv")
    rss_peak=$(awk -F',' 'NR>1 { if($3+0 > pk) pk=$3+0 } END { printf "%.0f", pk }' "$cell_dir/collector_resource.csv")
    rss_mb=$(echo "scale=2; ${rss_peak:-0} / 1024" | bc)

    # Output/input ratio: derive from bytes sent by SDK (input) vs bytes
    # served by collector /metrics (output). The SDK summary JSON has
    # avg_bandwidth_bps; we use it as the input proxy.
    local sdk_summary
    sdk_summary="$cell_dir/baseline_${RATE}mps_summary.json"
    if [[ -f "$sdk_summary" ]]; then
        sdk_in=$(python3 -c "import json; print(json.load(open('$sdk_summary')).get('avg_bandwidth_bps',0))" 2>/dev/null || echo 0)
    else
        sdk_in=0
    fi
    if [[ "${sdk_in%.*}" -gt 0 ]]; then
        ratio=$(echo "scale=4; $bw_avg / $sdk_in" | bc)
    else
        ratio=0
    fi

    echo "  -> bw_avg=${bw_avg} B/s  cpu=${cpu_avg}%  heap_peak=${rss_mb} MB  ratio=${ratio}"
    echo "$proc,$window,$threshold,$bw_avg,$cpu_avg,$rss_mb,$ratio,$DURATION_SEC,$RATE,OK" >> "$CSV"
}

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
for proc in "${PROCESSORS[@]}"; do
    for window in "${WINDOWS[@]}"; do
        for threshold in "${THRESHOLDS[@]}"; do
            run_cell "$proc" "$window" "$threshold"
        done
    done
done

# ---------------------------------------------------------------------------
# Markdown summary
# ---------------------------------------------------------------------------
{
    echo "# Delta-Transmission Sweep Results"
    echo ""
    echo "Generated: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo ""
    echo "Cells: $(awk 'NR>1' "$CSV" | wc -l) (duration ${DURATION_FLAG} per cell, rate ${RATE} MPS, ${SERIES} series)"
    echo ""
    for proc in "${PROCESSORS[@]}"; do
        echo "## $proc"
        echo ""
        echo "| window | threshold | bw B/s | cpu % | heap peak MB | out/in | status |"
        echo "|---|---|---|---|---|---|---|"
        awk -F',' -v p="$proc" 'NR>1 && $1==p { printf "| %s | %s | %s | %s | %s | %s | %s |\n", $2,$3,$4,$5,$6,$7,$10 }' "$CSV"
        echo ""
    done
} > "$MD"

echo ""
echo "=========================================================="
echo "  DONE"
echo "  CSV: $CSV"
echo "  MD:  $MD"
echo "=========================================================="
