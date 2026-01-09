#!/bin/bash

help() {
    echo "Benchmark the OTEL - Telegraf pipeline"
    echo "Flags:"
    echo "  -c 40000                          num metrics to send per second, 0 for unbounded"
    echo "  -d 30                             duration in seconds"
    echo "  -w 10                             num workers"
    echo "  -o configs/otel-bench.yaml      path to otel config file"
    echo "  -t configs/telegraf-bench.conf  path to telegraf config file"
    echo
    echo "Total number of metrics sent = count * duration * workers"
}

_count=(40000)
_duration=(30)
_workers=(10)
_otelPath=("./configs/otel-bench.yaml")
_telegrafPath=("./configs/telegraf-bench.conf")

while getopts :c:d:o:t:w: flag; do
    case $flag in
        c) _count+=("$OPTARG");;
        d) _duration+=("$OPTARG");;
        o) _otelPath+=("$OPTARG");;
        t) _telegrafPath+=("$OPTARG");;
        w) _workers+=("$OPTARG");;
        *) {
            help
            exit 1
        };;
    esac
done

count="${_count[-1]}"
duration="${_duration[-1]}"
workers="${_workers[-1]}"
otelPath="${_otelPath[-1]}"
telegrafPath="${_telegrafPath[-1]}"

# shellcheck disable=SC2164
absSrc=$( (cd "$(dirname "$0")"; pwd) )
# shellcheck disable=SC2164
if [[ "${#_otelPath[@]}" -eq 1 ]]; then otelPath="$absSrc/$otelPath"
else otelPath="$(cd -- "$(dirname -- "$otelPath")"; pwd)/$(basename -- "$otelPath")"
fi

# shellcheck disable=SC2164
if [[ "${#_telegrafPath[@]}" -eq 1 ]]; then telegrafPath="$absSrc/$telegrafPath"
else telegrafPath="$(cd -- "$(dirname -- "$telegrafPath")"; pwd)/$(basename -- "$telegrafPath")"
fi

# ============================= run otel and telegraf

# script assumes we're in source directory
cd "$(dirname "$0")" || exit 1

# clear outputs
rm -rf out 2> /dev/null
mkdir out 2> /dev/null

otelOut="out/otel-out"
otelMonitorOut="out/otel-monitor"
telegrafOut="out/telegraf-out"
telegrafMonitorOut="out/telegraf-monitor"
telemetrygenOut="out/telemetrygen-out"

# capture both stdout and stderr into same file (this is what terminal sees; gives better sense of temporality)
(./bin/otel/OTEL --config "$otelPath" |& cat) > "$otelOut" &
otelPid=$(pgrep -f "OTEL" | head -n 1)

(./bin/telegraf/telegraf --config "$telegrafPath" |& cat) > "$telegrafOut" &
telegrafPid=$(pgrep -f "telegraf" | head -n 1)

# wait a bit to make sure neither process exits (error)
echo "Starting processes..."
sleep 5

if (! kill -0 "$otelPid" 2> /dev/null) || (! kill -0 "$telegrafPid" 2> /dev/null); then
    echo "One of OTEL or Telegraf quit prematurely, exiting..."

    # one (or both) of the pids dne, so kill will error on at least one of them (pid dne)
    kill "$otelPid" 2> /dev/null
    kill "$telegrafPid" 2> /dev/null
    exit 1
fi

echo "========== PARAMETERS ============"
echo "Rate:            $count"
echo "Duration:        ${duration}s"
echo "Workers:         $workers"
echo "Total Metrics:   $(( "$count*$duration*$workers" ))"
echo
echo "OTEL Config:     $otelPath"
echo "Telegraf Config: $telegrafPath"
echo "=================================="
echo
echo "Running..."

echo "timestamp,cpu%,memory_mb" > "$otelMonitorOut"
echo "timestamp,cpu%,memory_mb" > "$telegrafMonitorOut"
./monitor.sh "$otelPid" >> "$otelMonitorOut" &
./monitor.sh "$telegrafPid" >> "$telegrafMonitorOut" &

startTime=$(date +"%s.%3N")

# total metrics = rate * duration * workers
(telemetrygen metrics \
    --otlp-insecure \
    --otlp-endpoint="localhost:4317" \
    --rate "$count" \
    --duration "$duration"s \
    --workers "$workers" \
    --unique-timeseries \
|& cat) > "$telemetrygenOut"

# TODO: replace telemetrygen with the load generator here https://github.com/ProjectASAP/DataCollector/pull/25
# (once its merged)

# note that this is not 100% accurate - we do not account for the telemetrygen start up time
# currently, telemetrygen appears to take ~10% longer than the desired duration for duration > 60s
endTime=$(date +"%s.%3N")
trueDuration=$(bc <<< "$endTime - $startTime")

kill "$otelPid"
kill "$telegrafPid"

echo "Finished running. Calculating benchmarks..."
echo

# otel stats (from opentelemetry-collector-contrib-patch/processor/kllprocessor/kll_bench.sh)
otelSent=$(grep "metrics generated" "$telemetrygenOut" | awk '{sum+=$NF} END {print sum+0}' | tr -d '"')
if [[ "$otelSent" -eq 0 ]]; then
    otelMps=0
else
    otelMps=$(bc <<< "$otelSent / $trueDuration")
fi

# telegraf stats
py=$(command -v python || command -v python3)
stats=$("$py" ../telegraf_benchmarks/summarize_telegraf_metrics.py --results-dir out)
echo -e "$stats" > ./out/telegraf-bench-results
values=$(echo -e "$stats" |
    # get the sum window, total gathered, written, and dropped values
    # also get max latency and averages
    awk -F " " \
        -v window=0 -v gathered=0 -v written=0 -v dropped=0 \
        -v lg_avg=0 -v lg_max=0 -v lw_avg=0 -v lw_max=0 \
        -v c_g=0 -v c_w=0 \
        '/Window:/ {gsub(",", "", $0);
            window+=$2; gathered+=$5; written+=$7; dropped+=$9;
            c_g=$5; c_w=$7;
        }
        /Latency:/ {gsub("=", " ", $0);
            if ($7 > lg_max) lg_max=$7;
            if ($14 > lw_max) lw_max=$14;
            lg_avg+=(c_g * $4);
            lw_avg+=(c_w * $11);
        }
        END {
            if (gathered != 0) { lg_avg /= gathered } else lg_avg=0;
            if (written != 0) { lw_avg /= written } else lw_avg=0;
            print window, gathered, written, dropped, lg_avg, lg_max, lw_avg, lw_max;
        }' | \
    tail -n 1)
# shellcheck disable=SC2206
values=($values)
read -r window gathered written dropped gatherLatencyAvg gatherLatencyMax writeLatencyAvg writeLatencyMax <<< "${values[@]}"

# -5 to account for 5s start up time, +2s as buffer
if (( $(bc <<< "($window - 3) < $trueDuration") )); then
    echo "WARN: Recorded duration in Telegraf benchmark logs ($window) is less than expected duration. You may need to increase rotation_max_archives in the Telegraf config."
fi

gathered_s=$(bc <<< "$gathered/$trueDuration")
written_s=$(bc <<< "$written/$trueDuration")

stat() {
    awk -F " " \
        -v cpu_max=0 -v cpu_avg=0 -v mem_max=0 -v mem_avg=0 -v count=0 \
        '{  count++;
            cpu_avg+=$2; mem_avg+=$3;
            if ($2 > cpu_max) cpu_max=$2;
            if ($3 > mem_max) mem_max=$3 }
        END {
            if (count == 0) count=1;
            print cpu_avg / count "%:" cpu_max "%:" mem_avg / count " MB:" mem_max " MB"
        }' \
    < "$1"
}

otelStats=$(stat "$otelMonitorOut")
telegrafStats=$(stat "$telegrafMonitorOut")

echo "============ SUMMARY ============="
echo "Duration:     ${trueDuration}s"
echo "Dropped:      $dropped"
echo
echo "Metric Throughput"
echo "  OTEL in/out:  $otelSent ($otelMps/s)"
echo "  Telegraf in:  $gathered ($gathered_s/s)"
echo "  Telegraf out: $written ($written_s/s)"
echo
echo -e "Telegraf Latency:Average:Max
  In  (gather):$gatherLatencyAvg ms:$gatherLatencyMax ms
  Out (write):$writeLatencyAvg ms:$writeLatencyMax ms" | column -t -s':'
echo
echo -e "Statistics:Avg CPU:Max CPU:Avg Memory:Max Memory
  OTEL:${otelStats}
  Telegraf:${telegrafStats}" | column -t -s':'
echo "=================================="