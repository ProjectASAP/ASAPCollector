#!/bin/bash

# utility file to monitor the cpu and memory usage of a process (given PID)
# writes timestamp, cpu%, memory (mb), to stdout 2x a second
# adapted from opentelemetry-collector-contrib-patch/processor/kllprocessor/kll_bench.sh

if [[ "$#" -ne 1 ]]; then 
    echo "Incorrect usage. Please pass in PID as first and only argument."
    exit 1
fi

pid="$1"
if ! kill -0 "$pid" 2> /dev/null ; then
    echo "PID=$pid not found"
    exit 2
fi

while kill -0 "$pid" 2>/dev/null; do
    STATS=$(ps -p "$pid" -o %cpu,rss --no-headers | awk '{$1=$1};1')
    if [ ! -z "$STATS" ]; then
        CPU=$(echo "$STATS" | cut -d' ' -f1)
        MEM_KB=$(echo "$STATS" | cut -d' ' -f2)
        [ -z "$CPU" ] && CPU=0
        [ -z "$MEM_KB" ] && MEM_KB=0
        MEM_MB=$(echo "scale=2; $MEM_KB / 1024" | bc)
        echo "$(date +%s) $CPU $MEM_MB"
    fi
    sleep 0.5
done