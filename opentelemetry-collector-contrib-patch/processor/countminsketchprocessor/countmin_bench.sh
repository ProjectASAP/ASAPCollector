#!/bin/bash

# ================= CONFIGURATION =================
BASE_PATH="../../cmd/countminsketchcol"
COLLECTOR_BIN="$BASE_PATH/countminsketchcol"
CONFIG_FILE="$BASE_PATH/config-bench.yaml"
RESULT_DIR="./benchmark_results/"

# Duration for each test
DURATION_SEC=30
DURATION="${DURATION_SEC}s"

# Load Settings
WORKERS=10
RATES=(10000 20000 30000 40000 50000)

# Latency Test Settings
QUERY_URL="http://localhost:8888/metrics" # Change this if you have a specific query endpoint
# =================================================

# Force international number format (prevents math errors)
export LC_NUMERIC=C

mkdir -p $RESULT_DIR
BIN_NAME=$(basename "$COLLECTOR_BIN")

echo "=========================================================="
echo "   CountMinSketch BENCHMARK    "
echo "=========================================================="
echo " Binary   : $BIN_NAME"
echo " Duration : $DURATION per scenario"
echo " Rates    : ${RATES[*]}"
echo " Query URL: $QUERY_URL"
echo "=========================================================="

# --- CLEANUP FUNCTION ---
cleanup() {
    echo ""
    echo "Stopping all background processes..."
    pkill -f "$BIN_NAME" 2>/dev/null
    kill $MONITOR_PID 2>/dev/null
    kill $LATENCY_PID 2>/dev/null
    exit
}
trap cleanup SIGINT

# Ensure clean state
pkill -f "$BIN_NAME" 2>/dev/null
sleep 2

# --- MAIN LOOP ---
for RATE in "${RATES[@]}"; do
    echo ""
    echo ">>> [SCENARIO] Testing Target Load: ${RATE} MPS"
    
    # 1. START COLLECTOR
    $COLLECTOR_BIN --config $CONFIG_FILE > /dev/null 2>&1 &
    
    echo "    -> Warming up collector (5s)..."
    sleep 5

    COLLECTOR_PID=$(pgrep -f "$COLLECTOR_BIN" | head -n 1)
    if [ -z "$COLLECTOR_PID" ]; then
        echo "    [ERROR] Collector failed to start."
        exit 1
    fi
    echo "    -> Collector PID: $COLLECTOR_PID"

    # 2. START RESOURCE MONITOR (Background)
    METRICS_FILE="$RESULT_DIR/resource_${RATE}mps.csv"
    echo "timestamp,cpu_percent,memory_mb" > $METRICS_FILE

    (
        while kill -0 $COLLECTOR_PID 2>/dev/null; do
            STATS=$(ps -p $COLLECTOR_PID -o %cpu,rss --no-headers | awk '{$1=$1};1')
            if [ ! -z "$STATS" ]; then
                CPU=$(echo $STATS | cut -d' ' -f1)
                MEM_KB=$(echo $STATS | cut -d' ' -f2)
                [ -z "$CPU" ] && CPU=0
                [ -z "$MEM_KB" ] && MEM_KB=0
                MEM_MB=$(echo "scale=2; $MEM_KB / 1024" | bc)
                echo "$(date +%s),$CPU,$MEM_MB" >> $METRICS_FILE
            fi
            sleep 1
        done
    ) &
    MONITOR_PID=$!

    # 3. START LATENCY TEST (Background)
    LATENCY_FILE="$RESULT_DIR/latency_${RATE}mps.csv"
    echo "timestamp,latency_ms,http_code" > $LATENCY_FILE
    
    (
        END_TIME=$(( $(date +%s) + DURATION_SEC ))
        while [ $(date +%s) -lt $END_TIME ]; do
            # Measure time_total, replace comma with dot
            RESPONSE=$(curl -o /dev/null -s -w "%{time_total},%{http_code}" "$QUERY_URL")
            TIME_SEC=$(echo $RESPONSE | cut -d',' -f1 | tr ',' '.')
            HTTP_CODE=$(echo $RESPONSE | cut -d',' -f2)
            
            [ -z "$TIME_SEC" ] && TIME_SEC=0
            
            # Calc ms
            LATENCY_MS=$(awk -v t="$TIME_SEC" 'BEGIN {print t * 1000}')
            echo "$(date +%s),$LATENCY_MS,$HTTP_CODE" >> $LATENCY_FILE
            sleep 0.2 # 5 Requests per second
        done
    ) &
    LATENCY_PID=$!

    # 4. START LOAD GENERATOR (Foreground - Main Waiter)
    LOG_FILE="$RESULT_DIR/telemetrygen_${RATE}.log"
    echo "    -> Generating Load & Measuring Latency..."
    
    telemetrygen metrics \
        --otlp-insecure \
        --otlp-endpoint="localhost:4317" \
        --rate $RATE \
        --duration $DURATION \
        --workers $WORKERS \
        --metrics 1 > "$LOG_FILE" 2>&1

    echo "    -> Test finished. Analyzing..."

    # 5. STOP EVERYTHING
    kill $MONITOR_PID 2>/dev/null
    kill $LATENCY_PID 2>/dev/null
    kill $COLLECTOR_PID 2>/dev/null
    wait $COLLECTOR_PID 2>/dev/null

    # 6. CALCULATE RESULTS
    
    # A. Throughput (MPS)
    TOTAL_SENT=$(grep "metrics generated" "$LOG_FILE" | awk '{sum+=$NF} END {print sum+0}' | tr -d '"')
    if [ "$TOTAL_SENT" -eq 0 ]; then ACTUAL_MPS=0; else ACTUAL_MPS=$((TOTAL_SENT / DURATION_SEC)); fi

    # B. CPU/RAM
    AVG_CPU=$(awk -F',' '{sum+=$2} END {if (NR>1) printf "%.2f", sum/(NR-1)}' $METRICS_FILE)
    MAX_MEM=$(awk -F',' 'NR>1 {if ($3>max) max=$3} END {print max}' $METRICS_FILE)

    # C. Latency Stats
    # We use a temporary sort file to calculate percentiles
    tail -n +2 $LATENCY_FILE | cut -d',' -f2 | sort -n > sorted_lat.tmp
    
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
    
    rm sorted_lat.tmp
    read LAT_AVG LAT_P95 LAT_P99 LAT_COUNT <<< "$LATENCY_STATS"

    # 7. PRINT SUMMARY
    echo "    --------------------------------------------------"
    echo "    [SUMMARY RESULTS]"
    echo "    Target Rate      : $RATE MPS"
    echo "    Actual Throughput: $ACTUAL_MPS MPS"
    echo "    -----------------"
    echo "    Avg CPU Usage    : ${AVG_CPU}%"
    echo "    Peak RAM Usage   : ${MAX_MEM} MB"
    echo "    -----------------"
    echo "    Query Latency (Avg): ${LAT_AVG} ms"
    echo "    Query Latency (P95): ${LAT_P95} ms"
    echo "    Query Latency (P99): ${LAT_P99} ms"
    echo "    Total Queries      : ${LAT_COUNT}"
    echo "    --------------------------------------------------"

done

echo ""
echo "=========================================================="
echo "All benchmarks completed. Results in $RESULT_DIR"
echo "=========================================================="