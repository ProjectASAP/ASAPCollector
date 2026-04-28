#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUT_DIR="${OUT_DIR:-$REPO_ROOT/deploy/eval-results/prom-client}"

PHASE="${PHASE:-combined}"
CARDINALITY="${CARDINALITY:-1000}"
FREQ_HZ="${FREQ_HZ:-100}"
SCRAPE_INTERVAL="${SCRAPE_INTERVAL:-1s}"
UPDATE_MODE="${UPDATE_MODE:-cached}"
INSTRUMENTS="${INSTRUMENTS:-counter,gauge}"
PROFILE_SECONDS="${PROFILE_SECONDS:-30}"
SOAK_S="${SOAK_S:-60}"
HOST_PORT="${HOST_PORT:-18000}"
CONTAINER_NAME="${CONTAINER_NAME:-prom-client-profiler}"
IMAGE="${IMAGE:-asap/fake-exporter:dev}"
BUILD_IMAGE="${BUILD_IMAGE:-0}"
SKETCHLIB_GO="${SKETCHLIB_GO:-$REPO_ROOT/../sketchlib-go}"

case "$PHASE" in
    update-only|scrape-only|combined) ;;
    *) echo "PHASE must be update-only, scrape-only, or combined; got $PHASE" >&2; exit 2 ;;
esac

if [[ "$PHASE" == "scrape-only" ]]; then
    RUN_FREQ_HZ="0"
else
    RUN_FREQ_HZ="$FREQ_HZ"
fi

safe() {
    printf '%s' "$1" | tr ',/: ' '____'
}

SLUG="phase-$(safe "$PHASE")_mode-$(safe "$UPDATE_MODE")_inst-$(safe "$INSTRUMENTS")_c${CARDINALITY}_f$(safe "$RUN_FREQ_HZ")_s$(safe "$SCRAPE_INTERVAL")"
PROFILE_DIR="$OUT_DIR/profiles"
LOG_DIR="$OUT_DIR/logs"
mkdir -p "$PROFILE_DIR" "$LOG_DIR"

CPU_PROFILE="$PROFILE_DIR/${SLUG}.cpu.pb"
HEAP_PROFILE="$PROFILE_DIR/${SLUG}.heap.pb"
CPU_TOP="$PROFILE_DIR/${SLUG}.cpu.top.txt"
HEAP_TOP="$PROFILE_DIR/${SLUG}.heap.top.txt"
SCRAPE_LOG="$LOG_DIR/${SLUG}.scrapes.csv"
CONTAINER_LOG="$LOG_DIR/${SLUG}.container.log"
STATS_BEFORE="$LOG_DIR/${SLUG}.stats-before.json"
STATS_AFTER="$LOG_DIR/${SLUG}.stats-after.json"

cleanup() {
    docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if [[ "$BUILD_IMAGE" == "1" ]]; then
    echo "# building $IMAGE" >&2
    DOCKER_BUILDKIT=1 docker build \
        -f "$REPO_ROOT/deploy/docker/Dockerfile.fake-exporter" \
        --build-context "sketchlib-go=$SKETCHLIB_GO" \
        -t "$IMAGE" "$REPO_ROOT" >&2
fi

cleanup

echo "# starting $CONTAINER_NAME: phase=$PHASE mode=$UPDATE_MODE card=$CARDINALITY freq=$RUN_FREQ_HZ scrape=$SCRAPE_INTERVAL instruments=$INSTRUMENTS" >&2
docker run -d \
    --name "$CONTAINER_NAME" \
    -p "127.0.0.1:${HOST_PORT}:8000" \
    -e EXPORTER_CLIENT=prometheus \
    -e EXPORTER_PROM_ADDR=0.0.0.0:8000 \
    -e EXPORTER_PROM_UPDATE_MODE="$UPDATE_MODE" \
    -e EXPORTER_PROM_INSTRUMENTS="$INSTRUMENTS" \
    -e EXPORTER_CARDINALITY="$CARDINALITY" \
    -e EXPORTER_FREQ_HZ="$RUN_FREQ_HZ" \
    "$IMAGE" >/dev/null

BASE_URL="http://127.0.0.1:${HOST_PORT}"
ready=0
for _ in $(seq 1 80); do
    if curl -fsS --max-time 2 "$BASE_URL/metrics" >/dev/null; then
        ready=1
        break
    fi
    sleep 0.25
done
if [[ "$ready" != "1" ]]; then
    docker logs "$CONTAINER_NAME" >&2 || true
    echo "producer did not become ready at $BASE_URL/metrics" >&2
    exit 1
fi

echo "# soaking ${SOAK_S}s" >&2
sleep "$SOAK_S"

docker stats "$CONTAINER_NAME" --no-stream --format '{{json .}}' > "$STATS_BEFORE"
: > "$SCRAPE_LOG"

scrape_loop() {
    local interval="$1"
    local seconds="$2"
    local log_file="$3"
    local deadline
    deadline=$(( $(date +%s) + seconds ))
    while (( $(date +%s) < deadline )); do
        local start_ns end_ns duration_ms bytes status
        start_ns="$(date +%s%N)"
        status=0
        bytes="$(curl -sS --max-time 30 -o /dev/null -w '%{size_download}' "$BASE_URL/metrics")" || status=$?
        end_ns="$(date +%s%N)"
        duration_ms=$(( (end_ns - start_ns) / 1000000 ))
        if [[ -z "$bytes" ]]; then
            bytes=0
        fi
        printf '%s,%s,%s\n' "$bytes" "$duration_ms" "$status" >> "$log_file"
        sleep "$interval"
    done
}

scrape_pid=""
if [[ "$PHASE" == "scrape-only" || "$PHASE" == "combined" ]]; then
    scrape_loop "$SCRAPE_INTERVAL" "$PROFILE_SECONDS" "$SCRAPE_LOG" &
    scrape_pid="$!"
fi

echo "# capturing CPU profile for ${PROFILE_SECONDS}s" >&2
curl -fsS --max-time "$((PROFILE_SECONDS + 20))" \
    "$BASE_URL/debug/pprof/profile?seconds=$PROFILE_SECONDS" \
    -o "$CPU_PROFILE"

if [[ -n "$scrape_pid" ]]; then
    wait "$scrape_pid"
fi

docker stats "$CONTAINER_NAME" --no-stream --format '{{json .}}' > "$STATS_AFTER"
curl -fsS --max-time 20 "$BASE_URL/debug/pprof/heap" -o "$HEAP_PROFILE"
docker logs "$CONTAINER_NAME" > "$CONTAINER_LOG" 2>&1 || true

go tool pprof -top "$CPU_PROFILE" > "$CPU_TOP" 2>&1 || true
go tool pprof -top "$HEAP_PROFILE" > "$HEAP_TOP" 2>&1 || true

python3 "$SCRIPT_DIR/prom-client-cell-summary.py" \
    --header \
    --phase "$PHASE" \
    --update-mode "$UPDATE_MODE" \
    --instruments "$INSTRUMENTS" \
    --cardinality "$CARDINALITY" \
    --freq-hz "$RUN_FREQ_HZ" \
    --scrape-interval "$SCRAPE_INTERVAL" \
    --profile-seconds "$PROFILE_SECONDS" \
    --soak-s "$SOAK_S" \
    --stats-before "$STATS_BEFORE" \
    --stats-after "$STATS_AFTER" \
    --scrape-log "$SCRAPE_LOG" \
    --cpu-profile "$CPU_PROFILE" \
    --heap-profile "$HEAP_PROFILE" \
    --cpu-top "$CPU_TOP" \
    --heap-top "$HEAP_TOP"
