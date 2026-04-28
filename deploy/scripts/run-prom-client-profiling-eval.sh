#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUT_DIR="${OUT_DIR:-$REPO_ROOT/deploy/eval-results/prom-client}"
mkdir -p "$OUT_DIR"

TS="${TS:-$(date +%Y%m%d)}"
OUT_CSV="${OUT_CSV:-$OUT_DIR/prom-client-profiling-${TS}.csv}"
OUT_LOG="${OUT_LOG:-$OUT_DIR/prom-client-profiling-${TS}.log}"

SOAK_S="${SOAK_S:-60}"
PROFILE_SECONDS="${PROFILE_SECONDS:-30}"
CARDINALITIES="${CARDINALITIES:-1000 10000}"
FREQS="${FREQS:-10 100 1000}"
SCRAPE_INTERVALS="${SCRAPE_INTERVALS:-1s 15s 60s}"
UPDATE_MODES="${UPDATE_MODES:-cached dynamic}"
INSTRUMENTS="${INSTRUMENTS:-counter,gauge}"
PHASES="${PHASES:-update-only scrape-only combined}"
IMAGE="${IMAGE:-asap/fake-exporter:dev}"
BUILD_IMAGE="${BUILD_IMAGE:-1}"
SKETCHLIB_GO="${SKETCHLIB_GO:-$REPO_ROOT/../sketchlib-go}"
HOST_PORT="${HOST_PORT:-18000}"

echo "Prometheus client profiling — $TS" | tee "$OUT_LOG"
{
    echo "  OUT_CSV=$OUT_CSV"
    echo "  SOAK_S=$SOAK_S PROFILE_SECONDS=$PROFILE_SECONDS"
    echo "  CARDINALITIES=$CARDINALITIES"
    echo "  FREQS=$FREQS"
    echo "  SCRAPE_INTERVALS=$SCRAPE_INTERVALS"
    echo "  UPDATE_MODES=$UPDATE_MODES"
    echo "  INSTRUMENTS=$INSTRUMENTS"
    echo "  PHASES=$PHASES"
} | tee -a "$OUT_LOG"

if [[ "$BUILD_IMAGE" == "1" ]]; then
    echo "==> building $IMAGE" | tee -a "$OUT_LOG"
    DOCKER_BUILDKIT=1 docker build \
        -f "$REPO_ROOT/deploy/docker/Dockerfile.fake-exporter" \
        --build-context "sketchlib-go=$SKETCHLIB_GO" \
        -t "$IMAGE" "$REPO_ROOT" 2>&1 | tee -a "$OUT_LOG"
fi

tmp_row="$(mktemp)"
header_written=0
: > "$OUT_CSV"

run_cell() {
    local phase="$1"
    local update_mode="$2"
    local cardinality="$3"
    local freq_hz="$4"
    local scrape_interval="$5"

    echo "==> phase=$phase mode=$update_mode C=$cardinality F=$freq_hz S=$scrape_interval" | tee -a "$OUT_LOG"
    PHASE="$phase" \
        UPDATE_MODE="$update_mode" \
        CARDINALITY="$cardinality" \
        FREQ_HZ="$freq_hz" \
        SCRAPE_INTERVAL="$scrape_interval" \
        INSTRUMENTS="$INSTRUMENTS" \
        SOAK_S="$SOAK_S" \
        PROFILE_SECONDS="$PROFILE_SECONDS" \
        OUT_DIR="$OUT_DIR" \
        IMAGE="$IMAGE" \
        BUILD_IMAGE=0 \
        SKETCHLIB_GO="$SKETCHLIB_GO" \
        HOST_PORT="$HOST_PORT" \
        "$SCRIPT_DIR/run-prom-client-profile-cell.sh" > "$tmp_row" 2>> "$OUT_LOG"

    if (( header_written == 0 )); then
        cat "$tmp_row" >> "$OUT_CSV"
        header_written=1
    else
        tail -n +2 "$tmp_row" >> "$OUT_CSV"
    fi
}

# shellcheck disable=SC2206
CARDINALITIES_ARR=($CARDINALITIES)
# shellcheck disable=SC2206
FREQS_ARR=($FREQS)
# shellcheck disable=SC2206
SCRAPE_INTERVALS_ARR=($SCRAPE_INTERVALS)
# shellcheck disable=SC2206
UPDATE_MODES_ARR=($UPDATE_MODES)
# shellcheck disable=SC2206
PHASES_ARR=($PHASES)

for mode in "${UPDATE_MODES_ARR[@]}"; do
    for card in "${CARDINALITIES_ARR[@]}"; do
        for phase in "${PHASES_ARR[@]}"; do
            case "$phase" in
                update-only)
                    for freq in "${FREQS_ARR[@]}"; do
                        run_cell "$phase" "$mode" "$card" "$freq" "none"
                    done
                    ;;
                scrape-only)
                    for scrape_interval in "${SCRAPE_INTERVALS_ARR[@]}"; do
                        run_cell "$phase" "$mode" "$card" "0" "$scrape_interval"
                    done
                    ;;
                combined)
                    for freq in "${FREQS_ARR[@]}"; do
                        for scrape_interval in "${SCRAPE_INTERVALS_ARR[@]}"; do
                            run_cell "$phase" "$mode" "$card" "$freq" "$scrape_interval"
                        done
                    done
                    ;;
                *)
                    echo "unknown phase $phase" >&2
                    exit 2
                    ;;
            esac
        done
    done
done

rm -f "$tmp_row"

echo "done: $OUT_CSV" | tee -a "$OUT_LOG"
