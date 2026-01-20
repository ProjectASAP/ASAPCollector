#!/bin/bash

# ================= CONFIGURATION =================
BASE_PATH="../../cmd/countminsketchcol"
COLLECTOR_BIN="$BASE_PATH/countminsketchcol"
CONFIG_FILE="$BASE_PATH/config-bench.yaml" 
RESULT_DIR="./benchmark_results"

# MATCH THIS WITH config-bench.yaml "metric_name"
SAMPLE_METRIC_NAME="cms_bench_result"
SAMPLE_FILE_PREFIX="samples"

# MATCH THIS WITH config-bench.yaml "window_interval"
WINDOW_SEC=10

# MATCH THIS WITH config-bench.yaml "prometheus" exporter port
QUERY_URL="http://localhost:9000/metrics" 

# Duration for each test
DURATION_SEC=60
DURATION="${DURATION_SEC}s"

# Load Settings
WORKERS=10
RATES=(10000 20000 30000 40000 50000)
# =================================================

export LC_NUMERIC=C
mkdir -p $RESULT_DIR
BIN_NAME=$(basename "$COLLECTOR_BIN")

echo "=========================================================="
echo "   CountMinSketch BENCHMARK (New API Version) "
echo "=========================================================="
echo " Binary   : $BIN_NAME"
echo " Duration : $DURATION per scenario"
echo " Window   : ${WINDOW_SEC}s"
echo " Rates    : ${RATES[*]}"
echo "=========================================================="

cleanup() {
    echo ""
    echo "Stopping all background processes..."
    pkill -f "$BIN_NAME" 2>/dev/null
    kill $MONITOR_PID 2>/dev/null
    kill $LATENCY_PID 2>/dev/null
    kill $SAMPLE_PID 2>/dev/null
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

    # 2. START RESOURCE MONITOR
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

    # 3. START LATENCY TEST
    LATENCY_FILE="$RESULT_DIR/latency_${RATE}mps.csv"
    echo "timestamp,latency_ms,http_code" > $LATENCY_FILE
    
    (
        END_TIME=$(( $(date +%s) + DURATION_SEC ))
        while [ $(date +%s) -lt $END_TIME ]; do
            RESPONSE=$(curl -o /dev/null -s -w "%{time_total},%{http_code}" "$QUERY_URL")
            TIME_SEC=$(echo $RESPONSE | cut -d',' -f1 | tr ',' '.')
            HTTP_CODE=$(echo $RESPONSE | cut -d',' -f2)
            [ -z "$TIME_SEC" ] && TIME_SEC=0
            LATENCY_MS=$(awk -v t="$TIME_SEC" 'BEGIN {print t * 1000}')
            echo "$(date +%s),$LATENCY_MS,$HTTP_CODE" >> $LATENCY_FILE
            sleep 0.2
        done
    ) &
    LATENCY_PID=$!

    # 4. START SAMPLE COUNT SCRAPER
    SAMPLE_FILE="$RESULT_DIR/${SAMPLE_FILE_PREFIX}_${RATE}mps.csv"
    echo "timestamp,sample_count" > $SAMPLE_FILE

    (
        END_TIME=$(( $(date +%s) + DURATION_SEC ))
        while [ $(date +%s) -lt $END_TIME ]; do
            curl -s "$QUERY_URL" \
            | grep "$SAMPLE_METRIC_NAME" \
            | grep -o 'sample_count="[0-9]*"' \
            | cut -d'"' -f2 \
            | awk -v ts="$(date +%s)" '{print ts "," $1}' >> "$SAMPLE_FILE"
            sleep 1
        done
    ) &
    SAMPLE_PID=$!

    # 5. START LOAD GENERATOR
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

    # 6. STOP EVERYTHING
    kill $MONITOR_PID 2>/dev/null
    kill $LATENCY_PID 2>/dev/null
    kill $SAMPLE_PID 2>/dev/null
    kill $COLLECTOR_PID 2>/dev/null
    wait $COLLECTOR_PID 2>/dev/null

    # 7. CALCULATE RESULTS
    
    # A. Throughput
    TOTAL_SENT=$(grep "metrics generated" "$LOG_FILE" | awk '{sum+=$NF} END {print sum+0}' | tr -d '"')
    if [ "$TOTAL_SENT" -eq 0 ]; then ACTUAL_MPS=0; else ACTUAL_MPS=$((TOTAL_SENT / DURATION_SEC)); fi

    # B. CPU/RAM
    AVG_CPU=$(awk -F',' '{sum+=$2} END {if (NR>1) printf "%.2f", sum/(NR-1)}' $METRICS_FILE)
    MAX_MEM=$(awk -F',' 'NR>1 {if ($3>max) max=$3} END {print max}' $METRICS_FILE)

    # C. Latency
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

    # D. Sample Count Stats
    MAX_SAMPLE_COUNT=$(awk -F',' 'NR>1 {if ($2>max) max=$2} END {print max+0}' $SAMPLE_FILE)

    # 8. PRINT SUMMARY
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
    echo "    Total Queries      : ${LAT_COUNT}"
    echo "    --------------------------------------------------"
   
    # --- F. Per-Window Breakdown (UPDATED FOR TUMBLING WINDOW) ---
    echo "    [Windowed Throughput Analysis]"
    
    # Logic Update:
    # 1. Processor emits 'sample_count' which is ABSOLUTE count for that window (reset to 0 each time).
    # 2. Prometheus exporter holds the value (Gauge) until next update.
    # 3. We take the MAX value seen in each 10s bucket as the true count for that window.
    # 4. We DO NOT subtract previous window, because the counter is not cumulative.

    awk -F',' -v window=$WINDOW_SEC -v duration=$DURATION_SEC '
    {
        if (NR==1) next
        if (min_ts == 0 || $1 < min_ts) min_ts = $1
        
        # Handle fragmentation: sum all concurrent sketches for this exact timestamp first
        sum_by_ts[$1] += $2
    }
    END {
        # 1. Bucket by Window
        for (ts in sum_by_ts) {
            rel_time = ts - min_ts
            w_idx = int(rel_time / window)
            
            # Since it is a Gauge that holds value, the Max value seen in the window 
            # represents the final count emitted by the processor for that window.
            if (sum_by_ts[ts] > window_max[w_idx]) {
                window_max[w_idx] = sum_by_ts[ts]
            }
        }

        # 2. Print Absolute Values (No Delta Calculation)
        total_windows = int(duration / window)
        
        for (i = 0; i < total_windows; i++) {
            val = window_max[i]
            if (val == 0) val = "N/A (Wait)"
            
            printf "    Window %d (%02ds - %02ds): %s metrics\n", i+1, i*window, (i+1)*window, val
        }
    }' "$SAMPLE_FILE" | sort -k 2

done

echo ""
echo "=========================================================="
echo "All benchmarks completed. Results in $RESULT_DIR"
echo "=========================================================="
