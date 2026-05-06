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
ONLY_NS=""
ONLY_SCRAPES_MS=""
ONLY_CARDINALITIES=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --out-dir) OUT_DIR="$2"; shift 2 ;;
        --soak-secs) SOAK_S="$2"; shift 2 ;;
        --pre-transition-secs) PRE_TRANSITION_S="$2"; shift 2 ;;
        --skip-cells) SKIP_CELLS="$2"; shift 2 ;;
        # Override-axis flags. Each accepts a comma-separated list of
        # values that REPLACES the corresponding default axis. Used
        # to re-run a small subset of cells (post-fix verification).
        --ns) ONLY_NS="$2"; shift 2 ;;
        --scrapes-ms) ONLY_SCRAPES_MS="$2"; shift 2 ;;
        --cardinalities) ONLY_CARDINALITIES="$2"; shift 2 ;;
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

# (sketch_family, agent_yaml, exporter_agg, kind_filter,
#  overlay_yaml, queries_file, transition_query,
#  force_replan_metric, force_replan_sketch_a, force_replan_sketch_b,
#  companion_metric, companion_sketch_a, companion_sketch_b)
#
# companion_* fields cover the GaugeVec scrape-ordering quirk:
# `asap_active_plan_id` exposes one (metric, plan_id) tuple per
# registered workload metric, ordered by metric name. The
# replay/transition tracker reads the FIRST tuple it sees. When
# we replan only the cell's primary metric, the alphabetically-
# first metric's plan_id stays unchanged and the tracker thinks
# nothing happened. POSTing a companion replan touches the OTHER
# metric so both id labels flip.
# kind_filter is the JSON queries entry kind that exercises this
# sketch — if a query in the suite has a different kind, the
# accuracy reducer (P8) maps via the `kind` field, not by family
# name.
#
# overlay_yaml selects the per-family backend-streaming/inference
# YAML pair (configs/backend-{streaming,inference}-<family>.yaml),
# layered on top of e2e-overlay.yml. The default `e2e-overlay.yml`
# (DDSketch) is used when overlay_yaml is empty.
#
# queries_file is the per-family promql_replay query suite, using
# metric names that match the family's metric_suffix
# (HLL → *_hll, KLL → *_kll, DDSketch → *_quantile, CS/CMS → raw).
# Without per-family queries, the replay client's queries fell
# through the inference YAML to cold storage, dominating mixed
# query p99 (claim ③ pre-fix sweep observation).
#
# transition_query is the off-plan probe shape — must be a
# pattern the query engine recognises (so it parses) but NOT in
# the per-family backend-inference-*.yaml (so it forces a
# capability-miss → controller replan → t_plan_ready fires).
SKETCHES=(
    "ddsketch:sketchcol-agent-b3-delta.yaml:dd-delta:quantile::queries-e2e.json:quantile_over_time(0.99, http_requests_total_latency_ms_quantile[10m]):http_requests_total_latency_ms:DDSketch:KLL:http_requests_total:HLL:CountSketch"
    "kll:sketchcol-agent-kll-direct.yaml:kll-full:quantile:e2e-overlay-kll.yml:queries-e2e-kll.json:quantile_over_time(0.99, http_requests_total_latency_ms_kll[10m]):http_requests_total_latency_ms:KLL:DDSketch:http_requests_total:HLL:CountSketch"
    "cs:sketchcol-agent-cs-direct.yaml:cs-delta:topk:e2e-overlay-cs.yml:queries-e2e-cs.json:topk(20, http_requests_total):http_requests_total:CountSketch:CountMinSketch:http_requests_total_latency_ms:DDSketch:KLL"
    "cms:sketchcol-agent-cms-direct.yaml:cms-delta:topk:e2e-overlay-cms.yml:queries-e2e-cms.json:topk(20, http_requests_total):http_requests_total:CountMinSketch:CountSketch:http_requests_total_latency_ms:DDSketch:KLL"
    "hll:sketchcol-agent-hll-direct.yaml:hll-delta:count_unique:e2e-overlay-hll.yml:queries-e2e-hll.json:count_over_time(http_requests_total_hll[10m]):http_requests_total:HLL:HLL:http_requests_total_latency_ms:DDSketch:KLL"
)

NS=(1 10)
SCRAPES_MS=(100 1000)
CARDINALITIES=(1000 10000 100000)

if [[ -n "$ONLY_NS" ]]; then
    IFS=',' read -r -a NS <<< "$ONLY_NS"
fi
if [[ -n "$ONLY_SCRAPES_MS" ]]; then
    IFS=',' read -r -a SCRAPES_MS <<< "$ONLY_SCRAPES_MS"
fi
if [[ -n "$ONLY_CARDINALITIES" ]]; then
    IFS=',' read -r -a CARDINALITIES <<< "$ONLY_CARDINALITIES"
fi

skip_re="^(${SKIP_CELLS//,/|})$"

cell_count=0
cell_skipped=0
for sk in "${SKETCHES[@]}"; do
    IFS=':' read -r FAM AGENT_YAML AGG KIND OVERLAY_YAML QUERIES_FILE TRANSITION_QUERY \
        FORCE_REPLAN_METRIC FORCE_REPLAN_SKETCH_A FORCE_REPLAN_SKETCH_B \
        COMPANION_METRIC COMPANION_SKETCH_A COMPANION_SKETCH_B <<< "$sk"
    if [[ "$SKIP_CELLS" != "" && "$FAM" =~ $skip_re ]]; then
        echo "[skip] sketch=$FAM"
        # Each sketch axis spans (NS × SCRAPES_MS × CARDINALITIES) cells.
        cell_skipped=$((cell_skipped + ${#NS[@]} * ${#SCRAPES_MS[@]} * ${#CARDINALITIES[@]}))
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

                # Per-family overlay: layer e2e-overlay-<family>.yml
                # on top of e2e-overlay.yml so the backend mounts the
                # right backend-{streaming,inference}-<family>.yaml.
                # When OVERLAY_YAML is empty, the default DDSketch
                # mounts from e2e-overlay.yml win.
                EXTRA_OVERLAY_ARGS=()
                if [[ -n "$OVERLAY_YAML" ]]; then
                    EXTRA_OVERLAY_ARGS=(-f "$OVERLAY_YAML")
                fi

                # Down any prior stack.
                (cd "$COMPOSE_DIR" && \
                  AGENT_CONFIG="$AGENT_YAML" \
                  docker compose \
                    -f base.yml -f "$AGENTS_YAML" \
                    -f baseline-b3-delta.yml -f e2e-overlay.yml \
                    "${EXTRA_OVERLAY_ARGS[@]}" \
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
                    "${EXTRA_OVERLAY_ARGS[@]}" \
                    up -d) > "${CELL_DIR}/up.log" 2>&1

                # Wait for stack to settle.
                sleep 8

                # Replay client (background) for the full soak. The
                # per-family queries file points at the metric names
                # the cell's overlay actually emits (HLL → *_hll, KLL
                # → *_kll, DDSketch → *_quantile, CS/CMS → unsuffixed).
                # Falls back to the default queries-e2e.json (DDSketch)
                # if the family didn't pin one.
                CELL_QUERIES="${SCRIPT_DIR}/${QUERIES_FILE:-queries-e2e.json}"
                python3 "${SCRIPT_DIR}/promql_replay.py" \
                    --target http://localhost:19091 \
                    --controller http://localhost:18080 \
                    --queries "$CELL_QUERIES" \
                    --qps 5 \
                    --duration "$SOAK_S" \
                    --out "${CELL_DIR}/replay.jsonl" \
                    > "${CELL_DIR}/replay.log" 2>&1 &
                REPLAY_PID=$!

                # Plan-transition driver runs concurrently. Per-family
                # transition query is a *supported* PromQL pattern that
                # is NOT in the cell's backend-inference-*.yaml — so it
                # forces a capability-miss → controller replan path
                # (claim ④). Pre-fix the trigger was histogram_quantile
                # which the engine's pattern matcher rejects outright,
                # so it never reached the controller.
                CELL_TRANSITION_Q="${TRANSITION_QUERY:-histogram_quantile(0.999, sum by (le) (http_requests_total_latency_ms))}"
                # Toggle the forced sketch_type on each cell so back-to-back
                # cells in a family produce a different plan_id (the planner
                # hashes the sketch+mode+delta tuple, so submitting the same
                # spec twice produces an identical hash → no plan_id flip).
                if (( cell_count % 2 == 0 )); then
                    FORCE_SKETCH="$FORCE_REPLAN_SKETCH_A"
                    COMP_SKETCH="$COMPANION_SKETCH_A"
                else
                    FORCE_SKETCH="$FORCE_REPLAN_SKETCH_B"
                    COMP_SKETCH="$COMPANION_SKETCH_B"
                fi
                # `--max-wait-secs 30` (default 120 s) caps the
                # post-replan probe budget. The forced replan
                # changes the agent's sketch_type which renames
                # the emitted metric (e.g. *_quantile → *_kll
                # when we hint KLL), so the original transition
                # query never matches the new metric and
                # `t_first_hit` never fires. Without this cap each
                # cell would block 240+ s on retry budgets.
                # `t_plan_ready` is the success criterion for
                # claim ④; t_first_hit / t_steady are nice-to-have
                # signals that need a separate trigger-query vs
                # new-plan-metric pairing PR.
                python3 "${SCRIPT_DIR}/plan_transition.py" \
                    --target http://localhost:19091 \
                    --controller http://localhost:18080 \
                    --transition-query "$CELL_TRANSITION_Q" \
                    --transition-out "${CELL_DIR}/transition.jsonl" \
                    --sample-out "${CELL_DIR}/sample.jsonl" \
                    --soak-secs "$SOAK_S" \
                    --pre-transition-secs "$PRE_TRANSITION_S" \
                    --max-wait-secs 30 \
                    --force-replan-metric "${FORCE_REPLAN_METRIC:-}" \
                    --force-replan-sketch "${FORCE_SKETCH:-}" \
                    --force-replan-companion-metric "${COMPANION_METRIC:-}" \
                    --force-replan-companion-sketch "${COMP_SKETCH:-}" \
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
                # `--bytes-sample-window 15` (was default 5s) +
                # `--bytes-sample-warmup 3` close the HLL-N1
                # docker-stats NaN gap (claim ② consistency, fix #3):
                # at HLL N=1 + 100ms scrape the agent flushes between
                # samples, so a 5 s window can land in dead air and
                # produce 0 B/s deltas. 15 s is wide enough to span
                # multiple flushes; the warm-up sample evicts any
                # stale "container just started" snapshot.
                python3 "${SCRIPT_DIR}/measure-baseline.py" \
                    --baseline "$FAM-cell" \
                    --scale "N${N}" \
                    --rate "$(echo "scale=2; 1000 / ${SCRAPE_MS}" | bc)" \
                    --cardinality "$CARD" \
                    --replay-jsonl "${CELL_DIR}/replay.jsonl" \
                    --bytes-sample-window 15 \
                    --bytes-sample-warmup 3 \
                    > "${CELL_DIR}/measurement.csv" \
                    2> "${CELL_DIR}/measurement.log" || true

                # Snapshot the cold-store ground truth into the
                # cell directory so the reducer doesn't need to
                # re-scrape the live volume after teardown.
                EXTRA_OVERLAY_PATHS=""
                if [[ -n "$OVERLAY_YAML" ]]; then
                    EXTRA_OVERLAY_PATHS="-f ${COMPOSE_DIR}/${OVERLAY_YAML}"
                fi
                BACKEND_CONT="$(docker compose -f "${COMPOSE_DIR}/base.yml" -f "$AGENTS_YAML" -f "${COMPOSE_DIR}/baseline-b3-delta.yml" -f "${COMPOSE_DIR}/e2e-overlay.yml" $EXTRA_OVERLAY_PATHS ps -q backend 2>/dev/null | head -n 1 || true)"
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
                    "${EXTRA_OVERLAY_ARGS[@]}" \
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
