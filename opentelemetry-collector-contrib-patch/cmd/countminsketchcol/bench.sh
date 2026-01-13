#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTRIB_PATCH_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
WORKSPACE_DIR="$(cd "$CONTRIB_PATCH_DIR/.." && pwd)"

BUILDER_BIN="$HOME/go/bin/builder"
BUILDER_CONFIG="$SCRIPT_DIR/builder-config.yaml"
COLLECTOR_BIN="$SCRIPT_DIR/dist/countminsketchcol"
CONFIG_FILE="$SCRIPT_DIR/config.yaml"
RESULT_DIR="$WORKSPACE_DIR/otel_collector_benchmark/benchmark_results/countminsketchcol"
LOAD_GEN_DIR="$WORKSPACE_DIR/otel_collector_benchmark"

# Telemetry endpoint for metrics
TELEMETRY_URL="http://localhost:8888/metrics"

# Duration for each test
DURATION_SEC=60
DURATION="${DURATION_SEC}s"

# Load Settings
WORKERS=10
HOSTS=10
METRICS=10
RATES=(10000 20000 30000 40000 50000)
# =================================================

# Force international number format (prevents math errors)
export LC_NUMERIC=C

mkdir -p "$RESULT_DIR"
BIN_NAME=$(basename "$COLLECTOR_BIN")

echo "=========================================================="
echo "   COUNTMIN SKETCH PROCESSOR BENCHMARK (Zipf Distribution)"
echo "=========================================================="
echo " Builder      : $BUILDER_BIN"
echo " Binary       : $COLLECTOR_BIN"
echo " Config       : $CONFIG_FILE"
echo " Results      : $RESULT_DIR"
echo " Duration     : $DURATION per scenario"
echo " Rates        : ${RATES[*]}"
echo " Telemetry URL: $TELEMETRY_URL"
echo "=========================================================="

# --- BUILD COLLECTOR ---
echo ""
echo ">>> Building collector..."
cd "$CONTRIB_PATCH_DIR"
$BUILDER_BIN --config "$BUILDER_CONFIG"
if [ $? -ne 0 ]; then
    echo "[ERROR] Build failed!"
    exit 1
fi
echo ">>> Build successful!"

# --- CLEANUP FUNCTION ---
cleanup() {
    echo ""
    echo "Stopping all background processes..."
    # Kill collector by full path to avoid killing this script
    pkill -f "$COLLECTOR_BIN" 2>/dev/null
    kill $MONITOR_PID 2>/dev/null
    kill $MEMORY_MONITOR_PID 2>/dev/null
    kill $LOAD_GEN_PID 2>/dev/null
    exit
}
trap cleanup SIGINT

# Ensure clean state - kill collector by full path
pkill -f "$COLLECTOR_BIN" 2>/dev/null
sleep 2

# --- HELPER FUNCTIONS ---

# Get cumulative counter value from Prometheus metrics
# Handles metrics with labels like: metric_name{label="value"} 123
get_metric_value() {
    local metric_name=$1
    local url=$2
    # Sum all values for this metric (in case there are multiple label combinations)
    curl -s "$url" | grep "^${metric_name}" | grep -v "^#" | awk '{sum += $NF} END {print sum+0}'
}

# Get memory value in MB from Prometheus metrics
get_memory_mb() {
    local url=$1
    local bytes=$(curl -s "$url" | grep "^otelcol_process_memory_rss_bytes" | grep -v "^#" | awk '{print $2}')
    if [ -z "$bytes" ] || [ "$bytes" == "0" ]; then
        echo "0"
    else
        # Use awk to handle scientific notation and convert to MB
        echo "$bytes" | awk '{printf "%.2f", $1 / 1048576}'
    fi
}

# --- MAIN LOOP ---
for RATE in "${RATES[@]}"; do
    echo ""
    echo ">>> [SCENARIO] Testing Target Load: ${RATE} MPS"
    
    # START COLLECTOR
    cd "$CONTRIB_PATCH_DIR"
    $COLLECTOR_BIN --config "$CONFIG_FILE" > /dev/null 2>&1 &
    
    echo "    -> Warming up collector (5s)..."
    sleep 5

    COLLECTOR_PID=$(pgrep -f "$COLLECTOR_BIN" | head -n 1)
    if [ -z "$COLLECTOR_PID" ]; then
        echo "    [ERROR] Collector failed to start."
        exit 1
    fi
    echo "    -> Collector PID: $COLLECTOR_PID"

    # RECORD START METRICS (for delta calculations)
    CPU_START=$(get_metric_value "otelcol_process_cpu_seconds_total" "$TELEMETRY_URL")
    RECEIVER_START=$(get_metric_value "otelcol_receiver_accepted_metric_points_total" "$TELEMETRY_URL")
    EXPORTER_START=$(get_metric_value "otelcol_exporter_sent_metric_points_total" "$TELEMETRY_URL")
    
    # Set defaults if metrics not available yet
    [ -z "$CPU_START" ] && CPU_START=0
    [ -z "$RECEIVER_START" ] && RECEIVER_START=0
    [ -z "$EXPORTER_START" ] && EXPORTER_START=0
    
    echo "    -> Start metrics recorded (CPU: $CPU_START, Receiver: $RECEIVER_START, Exporter: $EXPORTER_START)"

    # START RESOURCE MONITOR (Background - using ps for CPU sampling)
    METRICS_FILE="$RESULT_DIR/resource_${RATE}mps.csv"
    echo "timestamp,cpu_percent,memory_mb" > "$METRICS_FILE"

    (
        while kill -0 $COLLECTOR_PID 2>/dev/null; do
            STATS=$(ps -p $COLLECTOR_PID -o %cpu,rss --no-headers 2>/dev/null | awk '{$1=$1};1')
            if [ ! -z "$STATS" ]; then
                CPU=$(echo $STATS | cut -d' ' -f1)
                MEM_KB=$(echo $STATS | cut -d' ' -f2)
                [ -z "$CPU" ] && CPU=0
                [ -z "$MEM_KB" ] && MEM_KB=0
                MEM_MB=$(echo "scale=2; $MEM_KB / 1024" | bc)
                echo "$(date +%s),$CPU,$MEM_MB" >> "$METRICS_FILE"
            fi
            sleep 1
        done
    ) &
    MONITOR_PID=$!

    # START MEMORY PEAK MONITOR (Background - using Prometheus metrics)
    MEMORY_FILE="$RESULT_DIR/memory_${RATE}mps.csv"
    echo "timestamp,memory_mb" > "$MEMORY_FILE"
    
    (
        END_TIME=$(( $(date +%s) + DURATION_SEC ))
        while [ $(date +%s) -lt $END_TIME ]; do
            MEM_MB=$(get_memory_mb "$TELEMETRY_URL")
            echo "$(date +%s),$MEM_MB" >> "$MEMORY_FILE"
            sleep 1
        done
    ) &
    MEMORY_MONITOR_PID=$!

    # START LATENCY TEST (Background)
    LATENCY_FILE="$RESULT_DIR/latency_${RATE}mps.csv"
    echo "timestamp,latency_ms,http_code" > "$LATENCY_FILE"
    
    (
        END_TIME=$(( $(date +%s) + DURATION_SEC ))
        while [ $(date +%s) -lt $END_TIME ]; do
            RESPONSE=$(curl -o /dev/null -s -w "%{time_total},%{http_code}" "$TELEMETRY_URL")
            TIME_SEC=$(echo $RESPONSE | cut -d',' -f1 | tr ',' '.')
            HTTP_CODE=$(echo $RESPONSE | cut -d',' -f2)
            [ -z "$TIME_SEC" ] && TIME_SEC=0
            LATENCY_MS=$(awk -v t="$TIME_SEC" 'BEGIN {print t * 1000}')
            echo "$(date +%s),$LATENCY_MS,$HTTP_CODE" >> "$LATENCY_FILE"
            sleep 0.2
        done
    ) &
    LATENCY_PID=$!

    # START LOAD GENERATOR (Foreground)
    LOG_FILE="$RESULT_DIR/loadgen_${RATE}.log"
    echo "    -> Generating Load with Zipf distribution..."
    
    # Calculate interval to achieve target rate
    # Rate = (workers * hosts * metrics) / interval
    # interval = (workers * hosts * metrics) / rate
    TOTAL_METRICS_PER_BATCH=$((WORKERS * HOSTS * METRICS))
    
    # Calculate interval to achieve target rate
    # Use microseconds for better precision when needed (Go's time.Duration supports this)
    INTERVAL_SEC=$(echo "scale=6; $TOTAL_METRICS_PER_BATCH / $RATE" | bc)
    INTERVAL_US=$(echo "scale=0; ($INTERVAL_SEC * 1000000) / 1" | bc)
    
    # Ensure minimum interval of 1 microsecond
    if [ "$INTERVAL_US" -lt 1 ]; then
        INTERVAL_US=1
    fi
    
    # Use microseconds for precision, Go will parse "33333us" correctly
    INTERVAL="${INTERVAL_US}us"
    
    # Convert to readable format for display
    if [ "$INTERVAL_US" -ge 1000 ]; then
        INTERVAL_DISPLAY=$(echo "scale=3; $INTERVAL_US / 1000" | bc)"ms"
    else
        INTERVAL_DISPLAY="${INTERVAL_US}us"
    fi
    echo "    -> Calculated interval: ${INTERVAL_DISPLAY} (${TOTAL_METRICS_PER_BATCH} metrics/batch @ ${RATE} MPS)"
    
    cd "$LOAD_GEN_DIR"
    go run main.go \
        --endpoint="localhost:4317" \
        --workers=$WORKERS \
        --hosts=$HOSTS \
        --metrics=$METRICS \
        --interval=$INTERVAL \
        --duration=$DURATION \
        --type=gauge > "$LOG_FILE" 2>&1 &
    LOAD_GEN_PID=$!
    
    # Wait for load generator to finish
    wait $LOAD_GEN_PID

    echo "    -> Test finished. Analyzing..."

    # RECORD END METRICS (for delta calculations)
    CPU_END=$(get_metric_value "otelcol_process_cpu_seconds_total" "$TELEMETRY_URL")
    RECEIVER_END=$(get_metric_value "otelcol_receiver_accepted_metric_points_total" "$TELEMETRY_URL")
    EXPORTER_END=$(get_metric_value "otelcol_exporter_sent_metric_points_total" "$TELEMETRY_URL")
    
    [ -z "$CPU_END" ] && CPU_END=0
    [ -z "$RECEIVER_END" ] && RECEIVER_END=0
    [ -z "$EXPORTER_END" ] && EXPORTER_END=0

    # STOP EVERYTHING
    kill $MONITOR_PID 2>/dev/null
    kill $MEMORY_MONITOR_PID 2>/dev/null
    kill $LATENCY_PID 2>/dev/null
    kill $COLLECTOR_PID 2>/dev/null
    wait $COLLECTOR_PID 2>/dev/null

    # CALCULATE RESULTS
    
    # A. CPU Usage (Delta calculation for cumulative counter)
    CPU_DELTA=$(echo "scale=4; $CPU_END - $CPU_START" | bc)
    CPU_PERCENT=$(echo "scale=2; ($CPU_DELTA / $DURATION_SEC) * 100" | bc)
    
    # B. Throughput (Delta calculation for cumulative counters)
    METRICS_RECEIVED=$(echo "$RECEIVER_END - $RECEIVER_START" | bc)
    METRICS_SENT=$(echo "$EXPORTER_END - $EXPORTER_START" | bc)
    
    if [ "$METRICS_RECEIVED" == "" ] || [ "$METRICS_RECEIVED" == "0" ]; then
        ACTUAL_MPS=0
        LOSS_RATE="0"
    else
        ACTUAL_MPS=$(echo "scale=0; $METRICS_RECEIVED / $DURATION_SEC" | bc)
        if [ "$METRICS_RECEIVED" -gt 0 ]; then
            LOSS_RATE=$(echo "scale=4; (($METRICS_RECEIVED - $METRICS_SENT) / $METRICS_RECEIVED) * 100" | bc)
        else
            LOSS_RATE="0"
        fi
    fi

    # C. CPU/RAM from ps monitoring
    AVG_CPU=$(awk -F',' 'NR>1 {sum+=$2; count++} END {if (count>0) printf "%.2f", sum/count; else print "0"}' "$METRICS_FILE")
    MAX_MEM_PS=$(awk -F',' 'NR>1 {if ($3>max) max=$3} END {print max+0}' "$METRICS_FILE")
    
    # D. Memory peak from Prometheus metrics
    MAX_MEM_PROM=$(awk -F',' 'NR>1 {if ($2>max) max=$2} END {printf "%.2f", max+0}' "$MEMORY_FILE")

    # E. Latency
    tail -n +2 "$LATENCY_FILE" | cut -d',' -f2 | sort -n > sorted_lat.tmp
    LATENCY_STATS=$(awk '
    BEGIN {count=0; sum=0}
    {val=$1+0; data[count]=val; sum+=val; count++}
    END {
        if(count==0) {printf "0 0 0 0"; exit}
        avg=sum/count;
        p95=data[int(count*0.95)];
        p99=data[int(count*0.99)];
        printf "%.2f %.2f %.2f %.2f", avg, p95, p99, count
    }' sorted_lat.tmp)
    rm -f sorted_lat.tmp
    read LAT_AVG LAT_P95 LAT_P99 LAT_COUNT <<< "$LATENCY_STATS"

    # PRINT SUMMARY
    echo "    --------------------------------------------------"
    echo "    [SUMMARY RESULTS]"
    echo "    Target Rate        : $RATE MPS"
    echo "    -----------------"
    echo "    CPU (Delta Calc)   : ${CPU_DELTA}s total, ${CPU_PERCENT}% avg"
    echo "    Avg CPU (ps)       : ${AVG_CPU}%"
    echo "    Peak RAM (ps)      : ${MAX_MEM_PS} MB"
    echo "    Peak RAM (prom)    : ${MAX_MEM_PROM} MB"
    echo "    -----------------"
    echo "    Metrics Received   : ${METRICS_RECEIVED}"
    echo "    Metrics Sent       : ${METRICS_SENT}"
    echo "    Actual Throughput  : ${ACTUAL_MPS} MPS"
    echo "    Data Loss Rate     : ${LOSS_RATE}%"
    echo "    -----------------"
    echo "    Query Latency (Avg): ${LAT_AVG} ms"
    echo "    Query Latency (P95): ${LAT_P95} ms"
    echo "    Query Latency (P99): ${LAT_P99} ms"
    echo "    Total Queries      : ${LAT_COUNT}"
    echo "    --------------------------------------------------"

    sleep 2

done

echo ""
echo "=========================================================="
echo "All benchmarks completed. Results in $RESULT_DIR"
echo "=========================================================="
