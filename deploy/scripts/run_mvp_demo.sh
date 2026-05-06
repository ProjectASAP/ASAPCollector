#!/usr/bin/env bash
# run_mvp_demo.sh — issue-46 MVP demo driver.
#
# v3 redesign (cardinality + multi-agent): the v1/v2 demo ran a single
# agent at cardinality=10000 with the all-five-sketches overlay, which
# OOM'd inside the 1 GiB agent ceiling and forced a 4 GiB hack. That
# was beyond the design point. The realistic deployment is many
# distributed edge collectors, each at cardinality 100–1000, all
# feeding a single backend whose total cardinality is N × per-agent.
# So v3 defaults to:
#
#   * per-agent cardinality = 1000 series (CARD=1000)
#   * N=10 agents (NUM_AGENTS=10) → total backend cardinality = 10000
#   * the agent mem_limit hack from v2 is dropped — at 1000 per agent
#     the all-five-sketches working set fits inside the 1 GiB ceiling
#     `agents-N10.yml` ships with.
#
# Drives two paired runs (ASAP all-sketches + Gorilla-S3 cold archive
# vs raw OTLP streaming) and emits the X/Y/Z deltas the MVP_REPORT
# tabulates.
#
# Steps:
#   1. ASAP cell — `baseline-b6-gorilla-s3.yml` overlay. Brings up
#      stack, soaks WARMUP_S, drives `promql_replay` for SOAK_S +
#      one ad-hoc cold-fallback query, snapshots metrics.
#   2. RAW cell — `baseline-b0a-raw-stream.yml` overlay. Same workload,
#      same soak, same replay queries. Provides the X/Y/Z denominator.
#   3. Reduce — call `mvp_report.py` to compute deltas + write
#      `MVP_REPORT_v3.md`.
#
# Usage:
#   bash deploy/scripts/run_mvp_demo.sh
#
# Output:
#   deploy/eval-results/${OUT_BASE}/
#     asap/{measurement.csv, accuracy.csv, replay.jsonl, ad_hoc_query_response.json}
#     raw/{measurement.csv, accuracy.csv, replay.jsonl}
#     MVP_REPORT_v3.md
set -euo pipefail

# ── knobs ────────────────────────────────────────────────────────
WARMUP_S="${WARMUP_S:-60}"
SOAK_S="${SOAK_S:-60}"
# Per-agent cardinality. v3 default = 1000 (was 10000 in v2). The
# realistic deployment shape is many edge collectors each at
# 100–1000; the all-five-sketches working set at 10000 / agent was
# beyond the design point and OOM'd inside the 1 GiB ceiling.
CARD="${CARD:-1000}"
# Number of distributed edge agents. v3 default = 10 (was 1 in v1/v2).
# Total backend cardinality = NUM_AGENTS × CARD = 10000 by default,
# matching the paper's claim about backend-side aggregate cardinality.
NUM_AGENTS="${NUM_AGENTS:-10}"
FREQ_HZ="${FREQ_HZ:-1}"             # 1 Hz scrape (1s window)
SDK_WINDOW="${SDK_WINDOW:-1000ms}"
QPS="${QPS:-5}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_DIR="$(cd "$SCRIPT_DIR/../docker-compose" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Compose project name. The MVP demo uses the same project name +
# host port range as `run_e2e_sweep.sh`, so the two stacks CANNOT
# run concurrently. `wait_for_sweep_idle` below polls until any
# active sweep finishes. Single-cell, single-script — keeping the
# port mapping simple matters more here than coexistence.
HOST_BACKEND_QUERY_PORT="19091"
HOST_CONTROLLER_PORT="18080"
HOST_PROM_PORT="9090"

SWEEP_WAIT_CAP_S="${SWEEP_WAIT_CAP_S:-3600}"
# OUT_BASE may be overridden via env so re-runs (e.g. after a backend
# rebuild) land in a sibling directory instead of clobbering the
# original artifacts. v3 uses a sibling directory by default.
OUT_BASE="${OUT_BASE:-${REPO_ROOT}/deploy/eval-results/mvp-2026-05-06-v3}"
ASAP_DIR="${OUT_BASE}/asap"
RAW_DIR="${OUT_BASE}/raw"

mkdir -p "$ASAP_DIR" "$RAW_DIR"

# Ad-hoc cold-fallback query — a PromQL shape NOT in the warm-tier
# inference table, so the backend's EngineRouter falls through to
# the cold tier (GorillaQueryEngine in Phase-6+). The raw counter
# `http_requests_total` is only routed for `topk` / `sum_over_time`
# / `count_over_time` in the unified inference table — `count(...)`
# of the bare metric is NOT, so the engine misses warm-tier and
# falls back. This is the criterion-5 probe.
COLD_QUERY="count(http_requests_total)"

# Replay queries — one per kind, mixed warm + cold tiers. Reusing
# the existing queries-e2e.json shape (instant queries the e2e
# replay client + accuracy reducer already understand).
REPLAY_QUERIES_FILE="${ASAP_DIR}/replay-queries.json"
cat > "$REPLAY_QUERIES_FILE" <<'JSON'
[
    {"kind": "quantile",     "promql": "quantile_over_time(0.99, http_requests_total_latency_ms_quantile[1m])"},
    {"kind": "quantile",     "promql": "quantile_over_time(0.5, http_requests_total_latency_ms_quantile[1m])"},
    {"kind": "sum",          "promql": "sum_over_time(http_requests_total[1m])"},
    {"kind": "count_unique", "promql": "count(http_requests_total)"},
    {"kind": "topk",         "promql": "topk(10, http_requests_total)"}
]
JSON

# ── helpers ──────────────────────────────────────────────────────
log() { printf '[mvp] %s\n' "$*"; }

# Best-effort coordination with the host-wide e2e sweep. Both stacks
# use the same compose project name (default `docker-compose`), the
# same host ports (19xxx), and the same volume names — so they MUST
# NOT run concurrently. If a `run_e2e_sweep.sh` is alive when the
# MVP demo starts, wait for it to finish (with a generous cap) so
# the MVP doesn't tear the sweep's stack out from under it.
wait_for_sweep_idle() {
    local waited=0
    # Match the actual `bash run_e2e_sweep.sh ...` process at line
    # start so wrapper / babysitter loops that incidentally have the
    # string `run_e2e_sweep` in argv (e.g. while-loops polling for
    # cell completion) don't trigger a false positive.
    while pgrep -f "^bash[[:space:]]+.*run_e2e_sweep\.sh" >/dev/null 2>&1; do
        if (( waited >= SWEEP_WAIT_CAP_S )); then
            log "WARN: run_e2e_sweep.sh still alive after ${SWEEP_WAIT_CAP_S}s — proceeding anyway"
            break
        fi
        if (( waited == 0 )); then
            log "run_e2e_sweep.sh detected — waiting for it to finish (cap ${SWEEP_WAIT_CAP_S}s)"
        fi
        sleep 30
        waited=$((waited + 30))
    done
}

teardown() {
    local cell_dir="$1"; shift
    log "  tearing down stack..."
    (cd "$COMPOSE_DIR" && \
       docker compose "$@" down -v >> "${cell_dir}/down.log" 2>&1 || true)
}

run_cell() {
    # Args: cell_label cell_dir overlay_yaml agent_config
    local label="$1" dir="$2" overlay="$3" agent_cfg="$4"
    log "── cell: ${label} (${overlay} + ${agent_cfg}, N=${NUM_AGENTS} agents) ──"
    mkdir -p "$dir"

    local -a COMPOSE_ARGS=(
        -f base.yml
        -f "agents-N${NUM_AGENTS}.yml"
        -f "$overlay"
        -f e2e-overlay.yml
    )

    # Down any prior stack first.
    teardown "$dir" "${COMPOSE_ARGS[@]}"

    # Up.
    log "  bringing up stack..."
    (cd "$COMPOSE_DIR" && \
       AGENT_CONFIG="$agent_cfg" \
       EXPORTER_FREQ_HZ="$FREQ_HZ" \
       EXPORTER_CARDINALITY="$CARD" \
       EXPORTER_SDK_WINDOW="$SDK_WINDOW" \
       docker compose "${COMPOSE_ARGS[@]}" up -d) > "${dir}/up.log" 2>&1

    # Stack-settle (mirrors `run_e2e_sweep.sh`'s `sleep 8`). Without
    # this the warm-up loop would race the gateway / backend
    # readiness checks; sketch ingest only starts once the OTLP
    # gRPC port is bound.
    log "  stack settle..."
    sleep 8

    # Warm-up phase — let the warm-tier sketch accumulators fill and
    # the gorillas3 processor write its first chunk to MinIO before
    # the measurement window opens. Without this, the replay queries
    # hit empty warm tier and the cold-fallback ad-hoc query lands
    # before any chunks exist in MinIO.
    log "  warm-up ${WARMUP_S}s..."
    sleep "$WARMUP_S"

    # Replay client (background) — hits the host-published backend
    # query port from base.yml.
    log "  replay (qps=${QPS}, soak=${SOAK_S}s)..."
    python3 "${SCRIPT_DIR}/promql_replay.py" \
        --target "http://localhost:${HOST_BACKEND_QUERY_PORT}" \
        --controller "http://localhost:${HOST_CONTROLLER_PORT}" \
        --queries "$REPLAY_QUERIES_FILE" \
        --qps "$QPS" \
        --duration "$SOAK_S" \
        --out "${dir}/replay.jsonl" \
        > "${dir}/replay.log" 2>&1 &
    REPLAY_PID=$!

    wait "$REPLAY_PID"
    log "  replay done."

    # Ad-hoc cold-fallback query — captures the response body so
    # the report can verify `data_source: gorilla_archive` (Phase-6+
    # backend) or surface the LocalFsColdStore fallback marker
    # actually present.
    log "  ad-hoc cold-fallback query: ${COLD_QUERY}"
    curl -sG "http://localhost:${HOST_BACKEND_QUERY_PORT}/api/v1/query" \
        --data-urlencode "query=${COLD_QUERY}" \
        > "${dir}/ad_hoc_query_response.json" 2>"${dir}/ad_hoc_query.err" \
        || true

    # Pull measurement BEFORE teardown — Prom dies with the stack.
    log "  measurement..."
    python3 "${SCRIPT_DIR}/measure-baseline.py" \
        --prom "http://localhost:${HOST_PROM_PORT}" \
        --baseline "${label}" \
        --scale "N${NUM_AGENTS}" \
        --rate "$FREQ_HZ" \
        --cardinality "$CARD" \
        --replay-jsonl "${dir}/replay.jsonl" \
        --bytes-sample-window 15 \
        > "${dir}/measurement.csv" \
        2> "${dir}/measurement.log" || true

    # Snapshot the cold-store ground truth into the cell directory
    # so the accuracy reducer can read it after teardown.
    BACKEND_CONT="$(cd "$COMPOSE_DIR" && docker compose "${COMPOSE_ARGS[@]}" \
        ps -q backend 2>/dev/null | head -n1 || true)"
    if [[ -n "$BACKEND_CONT" ]]; then
        docker cp "${BACKEND_CONT}:/var/asap/cold/raw" "${dir}/cold-truth" \
            > "${dir}/cold-snapshot.log" 2>&1 || true
    fi

    # MinIO chunk listing for the gorilla-archive bucket — a structural
    # signal that the gorillas3 processor wrote something. Empty in
    # the raw cell; populated in the ASAP cell.
    MINIO_CONT="$(cd "$COMPOSE_DIR" && docker compose "${COMPOSE_ARGS[@]}" \
        ps -q minio 2>/dev/null | head -n1 || true)"
    if [[ -n "$MINIO_CONT" ]]; then
        docker run --rm --network container:"$MINIO_CONT" \
            --entrypoint sh minio/mc:latest -c \
            "mc alias set asap http://localhost:9000 asap asap-local-only >/dev/null 2>&1; mc ls --recursive asap/asap-gorilla 2>/dev/null || true" \
            > "${dir}/gorilla_chunks.txt" 2>&1 || true
    fi

    # Accuracy reduce (per cell — needs both replay.jsonl + cold-truth).
    if [[ -d "${dir}/cold-truth" ]]; then
        python3 "${SCRIPT_DIR}/accuracy_reduce.py" \
            --cell-dir "$dir" \
            --out "${dir}/accuracy.csv" \
            > "${dir}/accuracy.log" 2>&1 || true
    fi

    # Down.
    teardown "$dir" "${COMPOSE_ARGS[@]}"
    log "  cell ${label} done → ${dir}"
}

# Wait if a sweep is in progress so we don't fight over docker.
wait_for_sweep_idle

# ── ASAP cell (warm sketches + cold gorilla-S3 archive) ───────────
run_cell "asap-allsketch-gs3" "$ASAP_DIR" \
    "baseline-b6-gorilla-s3.yml" "sketchcol-agent-b6-gorilla-s3.yaml"

# ── RAW baseline cell ────────────────────────────────────────────
run_cell "raw-stream"          "$RAW_DIR"  \
    "baseline-b0a-raw-stream.yml" "sketchcol-agent-b0a-raw-stream.yaml"

# ── Reduce → MVP_REPORT_v3.md ────────────────────────────────────
log "── reducing → MVP_REPORT_v3.md ──"
python3 "${SCRIPT_DIR}/mvp_report.py" \
    --asap-dir "$ASAP_DIR" \
    --raw-dir  "$RAW_DIR" \
    --num-agents "$NUM_AGENTS" \
    --per-agent-cardinality "$CARD" \
    --out      "${OUT_BASE}/MVP_REPORT_v3.md"

log "MVP demo complete. Report: ${OUT_BASE}/MVP_REPORT_v3.md"
