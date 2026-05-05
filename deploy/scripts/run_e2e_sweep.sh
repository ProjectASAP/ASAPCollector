#!/usr/bin/env bash
# e2e sweep runner (P7) — drives the matrix
#   {DDSketch, KLL, CountSketch, CountMinSketch, HLL}  (sketch family)
# × {N=1, 10}                                          (agent count)
# × {scrape=100ms, 1s}                                 (SDK window)
# × {cardinality=1e3, 1e4, 1e5}                        (active series)
#
# Per cell: brings the stack up, soaks for SOAK_S seconds, runs
# the PromQL replay client (P5), and the plan-transition driver
# (P6) once mid-soak. Replay output, transition output and 1 Hz
# sampler output land in `--out-dir/<cell-key>/`.
#
# Bring-down between cells happens via `docker compose down -v` so
# every cell starts from a clean state — avoids cross-cell
# leak in the cold-store volume.
#
# Usage:
#
#   ./run_e2e_sweep.sh \\
#       --out-dir /tmp/e2e-sweep-$(date +%Y%m%d-%H%M%S) \\
#       --soak-secs 60 \\
#       --pre-transition-secs 20 \\
#       --skip-cells "kll,cms"   # comma-separated sketch families to skip
set -euo pipefail

OUT_DIR=""
SOAK_S=120
PRE_TRANSITION_S=30
SKIP_CELLS=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --out-dir) OUT_DIR="$2"; shift 2 ;;
        --soak-secs) SOAK_S="$2"; shift 2 ;;
        --pre-transition-secs) PRE_TRANSITION_S="$2"; shift 2 ;;
        --skip-cells) SKIP_CELLS="$2"; shift 2 ;;
        -h|--help)
            sed -n '/^# Usage:/,/^set -euo/{/^set -euo/!p}' "$0"
            exit 0 ;;
        *) echo "unknown arg: $1" >&2; exit 1 ;;
    esac
done

if [[ -z "$OUT_DIR" ]]; then
    echo "--out-dir required" >&2
    exit 1
fi

mkdir -p "$OUT_DIR"
COMPOSE_DIR="$(cd "$(dirname "$0")/../docker-compose" && pwd)"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# (sketch_family, agent_yaml, exporter_agg, kind_filter)
# kind_filter is the JSON queries entry kind that exercises this
# sketch — if a query in the suite has a different kind, the
# accuracy reducer (P8) maps via the `kind` field, not by family
# name.
SKETCHES=(
    "ddsketch:sketchcol-agent-b3-delta.yaml:dd-delta:quantile"
    "kll:sketchcol-agent-b3-delta.yaml:kll-full:quantile"
    "cs:sketchcol-agent-b3-delta.yaml:cs-delta:topk"
    "cms:sketchcol-agent-b3-delta.yaml:cms-delta:topk"
    "hll:sketchcol-agent-b3-delta.yaml:hll-delta:count_unique"
)

NS=(1 10)
SCRAPES_MS=(100 1000)
CARDINALITIES=(1000 10000 100000)

skip_re="^(${SKIP_CELLS//,/|})$"

cell_count=0
cell_skipped=0
for sk in "${SKETCHES[@]}"; do
    IFS=':' read -r FAM AGENT_YAML AGG KIND <<< "$sk"
    if [[ "$SKIP_CELLS" != "" && "$FAM" =~ $skip_re ]]; then
        echo "[skip] sketch=$FAM"
        cell_skipped=$((cell_skipped + len_each_axis))
        continue
    fi
    for N in "${NS[@]}"; do
        for SCRAPE_MS in "${SCRAPES_MS[@]}"; do
            for CARD in "${CARDINALITIES[@]}"; do
                CELL="${FAM}_N${N}_w${SCRAPE_MS}ms_c${CARD}"
                CELL_DIR="${OUT_DIR}/${CELL}"
                mkdir -p "$CELL_DIR"
                echo
                echo "[cell ${cell_count}] ${CELL}"
                cell_count=$((cell_count + 1))

                AGENTS_YAML="${COMPOSE_DIR}/agents-N${N}.yml"
                if [[ ! -f "$AGENTS_YAML" ]]; then
                    echo "  no overlay for N=${N} → generating via gen-agents.sh"
                    AGENTS_YAML="${COMPOSE_DIR}/agents-N${N}.gen.yml"
                    "${COMPOSE_DIR}/gen-agents.sh" "$N" > "$AGENTS_YAML"
                fi

                # Down any prior stack.
                (cd "$COMPOSE_DIR" && \
                  AGENT_CONFIG="$AGENT_YAML" \
                  docker compose \
                    -f base.yml -f "$AGENTS_YAML" \
                    -f baseline-b3-delta.yml -f e2e-overlay.yml \
                    down -v) > "${CELL_DIR}/down.log" 2>&1 || true

                # Up.
                (cd "$COMPOSE_DIR" && \
                  AGENT_CONFIG="$AGENT_YAML" \
                  EXPORTER_FREQ_HZ="$(echo "scale=2; 1000 / ${SCRAPE_MS}" | bc)" \
                  EXPORTER_CARDINALITY="$CARD" \
                  EXPORTER_SDK_WINDOW="${SCRAPE_MS}ms" \
                  EXPORTER_SDK_AGG="$AGG" \
                  docker compose \
                    -f base.yml -f "$AGENTS_YAML" \
                    -f baseline-b3-delta.yml -f e2e-overlay.yml \
                    up -d) > "${CELL_DIR}/up.log" 2>&1

                # Wait for stack to settle.
                sleep 8

                # Replay client (background) for the full soak.
                python3 "${SCRIPT_DIR}/promql_replay.py" \
                    --target http://localhost:19091 \
                    --controller http://localhost:18080 \
                    --queries "${SCRIPT_DIR}/queries-e2e.json" \
                    --qps 5 \
                    --duration "$SOAK_S" \
                    --out "${CELL_DIR}/replay.jsonl" \
                    > "${CELL_DIR}/replay.log" 2>&1 &
                REPLAY_PID=$!

                # Plan-transition driver runs concurrently. The
                # transition query is `histogram_quantile(0.999, ...)`
                # which is unlikely to be on the active plan
                # (default plans use 0.99 / 0.5 quantiles per
                # backend-streaming.yaml).
                python3 "${SCRIPT_DIR}/plan_transition.py" \
                    --target http://localhost:19091 \
                    --controller http://localhost:18080 \
                    --transition-query 'histogram_quantile(0.999, sum by (le) (http_requests_total_latency_ms))' \
                    --transition-out "${CELL_DIR}/transition.jsonl" \
                    --sample-out "${CELL_DIR}/sample.jsonl" \
                    --soak-secs "$SOAK_S" \
                    --pre-transition-secs "$PRE_TRANSITION_S" \
                    > "${CELL_DIR}/plan_transition.log" 2>&1 &
                TRANSITION_PID=$!

                # Wait for both.
                wait "$REPLAY_PID" "$TRANSITION_PID"

                # Pull the per-cell measurement row BEFORE bringing
                # the stack down — Prom dies with the stack so the
                # gateway / agent / backend Prom counters won't
                # survive teardown. `--replay-jsonl` populates
                # `backend_query_p99_ms` from the client-side
                # JSONL, which IS preserved on the host (paper
                # blocker #3, item 3).
                python3 "${SCRIPT_DIR}/measure-baseline.py" \
                    --baseline "$FAM-cell" \
                    --scale "N${N}" \
                    --rate "$(echo "scale=2; 1000 / ${SCRAPE_MS}" | bc)" \
                    --cardinality "$CARD" \
                    --replay-jsonl "${CELL_DIR}/replay.jsonl" \
                    > "${CELL_DIR}/measurement.csv" \
                    2> "${CELL_DIR}/measurement.log" || true

                # Snapshot the cold-store ground truth into the
                # cell directory so the reducer doesn't need to
                # re-scrape the live volume after teardown.
                BACKEND_CONT="$(docker compose -f "${COMPOSE_DIR}/base.yml" -f "$AGENTS_YAML" -f "${COMPOSE_DIR}/baseline-b3-delta.yml" -f "${COMPOSE_DIR}/e2e-overlay.yml" ps -q backend 2>/dev/null | head -n 1 || true)"
                if [[ -n "$BACKEND_CONT" ]]; then
                    docker cp "${BACKEND_CONT}:/var/asap/cold/raw" "${CELL_DIR}/cold-truth" \
                        > "${CELL_DIR}/cold-snapshot.log" 2>&1 || true
                fi

                # Down with volume cleanup so the next cell starts
                # cold.
                (cd "$COMPOSE_DIR" && \
                  AGENT_CONFIG="$AGENT_YAML" \
                  docker compose \
                    -f base.yml -f "$AGENTS_YAML" \
                    -f baseline-b3-delta.yml -f e2e-overlay.yml \
                    down -v) >> "${CELL_DIR}/down.log" 2>&1 || true

                # Capacity check: ensure we don't run out of disk
                # for the cold snapshot. 100k cardinality × 100ms
                # scrape × 60s = ~60M raw events ≈ 4-6 GB JSONL.
                if [[ -d "${CELL_DIR}/cold-truth" ]]; then
                    SIZE=$(du -sh "${CELL_DIR}/cold-truth" 2>/dev/null | cut -f1 || true)
                    echo "  cold-truth size: ${SIZE:-?}"
                fi
                echo "  cell done: ${CELL_DIR}"
            done
        done
    done
done

echo
echo "sweep complete: ${cell_count} cells under ${OUT_DIR}"
