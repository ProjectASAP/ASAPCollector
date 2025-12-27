#!/bin/bash

help() {
    echo "Benchmark the OTEL - Telegraf pipeline"
    echo "Flags:"
    echo "  -c 40000                          num metrics to send per second, 0 for unbounded"
    echo "  -d 30                             duration in seconds"
    echo "  -w 10                             num workers"
    echo "  -o ./configs/otel-bench.yaml      path to otel config file"
    echo "  -t ./configs/telegraf-bench.conf  path to telegraf config file"
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

# otel and telegraf path are relative to script location, but if user provides a path we want that to be relative to where they are
base=$(pwd)
# shellcheck disable=SC2164
if [[ "${#_otelPath[@]}" -eq 1 ]]; then
    cd "$(dirname "$0")"
    otelPath="$(cd -- "$(dirname -- "$otelPath")"; pwd)/$(basename -- "$otelPath")"
    cd "$base"
else
    otelPath="$(cd -- "$(dirname -- "$otelPath")"; pwd)/$(basename -- "$otelPath")"
fi

# shellcheck disable=SC2164
if [[ "${#_telegrafPath[@]}" -eq 1 ]]; then
    cd "$(dirname "$0")"
    telegrafPath="$(cd -- "$(dirname -- "$telegrafPath")"; pwd)/$(basename -- "$telegrafPath")"
    cd "$base"
else
    telegrafPath="$(cd -- "$(dirname -- "$telegrafPath")"; pwd)/$(basename -- "$telegrafPath")"
fi

# ============================= run otel and telegraf

# script assumes we're in source directory
cd "$(dirname "$0")" || exit 1

# clear outputs
rm -rf out 2> /dev/null
mkdir out 2> /dev/null

otelOut="out/otel-out"
telegrafOut="out/telegraf-out"
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

start_time=$(date +"%s.%3N")

# total metrics = rate * duration * workers
(telemetrygen metrics \
    --otlp-insecure \
    --otlp-endpoint="localhost:4317" \
    --rate "$count" \
    --duration "$duration"s \
    --workers "$workers" \
    --unique-timeseries \
|& cat) > "$telemetrygenOut"

kill "$otelPid"
kill "$telegrafPid"

# TODO: telemetrygen seems to take 10% longer than given duration
# TODO: warn if telegraf duration is less than expected; likely not enough rotation archives in conf

end_time=$(date +"%s.%3N")
diff=$(bc <<< "$end_time - $start_time")
echo "DURATION: $diff"

# otel stats (from opentelemetry-collector-contrib-patch/processor/kllprocessor/kll_bench.sh)
otelSent=$(grep "metrics generated" "$telemetrygenOut" | awk '{sum+=$NF} END {print sum+0}' | tr -d '"')
if [ "$otelSent" -eq 0 ]; then otelMps=0; else otelMps=$((otelSent / duration)); fi

echo
echo "OTEL Metrics/s: $otelMps"
echo

# telegraf stats
py=$(command -v python || command -v python3)
stats=$("$py" ../telegraf_benchmarks/summarize_telegraf_metrics.py --results-dir out)
echo -e "$stats"
values=$(echo -e "$stats" |
    # get the sum window, total gathered, written, and dropped values
    awk -F " " \
        -v window=0 -v gathered=0 -v written=0 -v dropped=0 \
        '/Window:/ {gsub(",", "", $0); window+=$2; gathered+=$5; written+=$7; dropped+=$9}
        END {print window, gathered, written, dropped}' | \
    tail -n 1)
# shellcheck disable=SC2206
values=($values)

window="${values[0]}"
gathered="${values[1]}"
written="${values[2]}"
dropped="${values[3]}"

if (( $(bc <<< "$window == 0") )); then
    window=1
fi

gathered_s=$(bc <<< "$gathered/$window")
written_s=$(bc <<< "$written/$window")

echo "============ SUMMARY ============="
echo "Duration:     $window"
echo "Dropped:      $dropped"
echo
echo "OTEL in/out:  $otelSent ($otelMps/s)"
echo "Telegraf in:  $gathered ($gathered_s/s)"
echo "Telegraf out: $written ($written_s/s)"
echo "=================================="