#!/usr/bin/env bash
# run-baseline-sweep.sh — iterate over (baseline, rate, cardinality)
# triples, bring up the stack, soak, and emit one measure-baseline
# row per configuration.
#
# Output is a single CSV to stdout with the header written once at
# the top. Run as:
#
#   ./run-baseline-sweep.sh 2>run.log > sweep.csv
#
# Defaults run a quick smoke sweep (all 6 baselines at N=1,
# default rate/cardinality, 60s soak). Override via env:
#
#   BASELINES="b2-full b3-delta"         which baselines to run
#   SCALE="N1"                            compose-overlay to use
#   RATES="1000 5000 10000"               EXPORTER_RATE sweep
#   CARDS="500 1000 5000"                 EXPORTER_CARDINALITY sweep
#   WINDOWS="5s 30s 60s 300s"             SKETCH_WINDOW sweep (B4 only)
#   SOAK_S=120                            per-config soak seconds
#   SCRIPT_DIR=.../deploy/scripts         override lookup path
#   DRIVE_QUERIES=1                       run metricsql_replay during soak
#                                         (lights up backend_query_p99_ms;
#                                         requires e2e-overlay-style stack)
#   QUERY_QPS=5                           QPS for the optional replay
#   QUERY_DURATION_S=30                   seconds of replay (≤ SOAK_S)
#   REPLAY_OUT_DIR=/tmp                   where to dump per-baseline JSONL
#
# WINDOWS is honored only when the baseline is `b4-tunable` —
# other baselines have their window hard-coded in the YAML. When
# WINDOWS is unset, the default B4 window (60s via the overlay
# env var) applies.
#
# The caller is responsible for having the compose stack's images
# built (asap/asap-otel:dev, asap/query-backend:dev, asap/fake-
# exporter:dev). Each iteration does a full `docker compose down`
# to ensure a clean state.
set -euo pipefail

SCRIPT_DIR="${SCRIPT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
COMPOSE_DIR="${SCRIPT_DIR}/../docker-compose"

BASELINES="${BASELINES:-b0a-raw-stream b0b-raw-batched b1-serf b2-full b3-delta b5-gorilla}"
SCALE="${SCALE:-N1}"
RATES="${RATES:-1000}"
CARDS="${CARDS:-1000}"
WINDOWS="${WINDOWS:-}"  # empty = use default window; non-empty only for B4
SOAK_S="${SOAK_S:-180}"  # ≥3 min so rate()/60s windows yield ≥2 samples
DRIVE_QUERIES="${DRIVE_QUERIES:-0}"
QUERY_QPS="${QUERY_QPS:-5}"
QUERY_DURATION_S="${QUERY_DURATION_S:-30}"
REPLAY_OUT_DIR="${REPLAY_OUT_DIR:-/tmp}"

# Emit header once.
head=1
for baseline in $BASELINES; do
  # Only iterate WINDOWS when the baseline actually consumes it.
  # For non-B4 baselines the inner loop runs once with a sentinel
  # empty value so the window_env stays unset. Using an array so
  # the empty sentinel survives word-splitting (a literal space
  # string splits to zero tokens — that was a bug in v1).
  if [[ "$baseline" == "b4-tunable" && -n "$WINDOWS" ]]; then
    # shellcheck disable=SC2206
    windows_for_this=($WINDOWS)
  else
    windows_for_this=("")
  fi
  for rate in $RATES; do
    for card in $CARDS; do
     for window in "${windows_for_this[@]}"; do
      if [[ "$baseline" == "b4-tunable" && -n "$WINDOWS" ]]; then
        tag="${baseline}-w${window}"
        window_env="$window"
        echo "# === baseline=${baseline} window=${window} rate=${rate} card=${card} ===" >&2
      else
        tag="$baseline"
        window_env=""
        echo "# === baseline=${baseline} rate=${rate} card=${card} ===" >&2
      fi

      # Map baseline tag to AGENT_CONFIG filename. Step 2g (2026-05)
      # renamed the b0/b5 agent configs from `…-prometheus.yaml` to
      # `…-victoriametrics.yaml` to match the VictoriaMetrics-backed
      # sink (was Prometheus). The b1 file kept its historical name
      # to avoid touching the b1 config which was already migrated.
      case "$baseline" in
        b0a-raw-stream)  cfg="asap-otel-agent-b0a-raw-stream.yaml" ;;
        b0b-raw-batched) cfg="asap-otel-agent-b0b-raw-batched.yaml" ;;
        b1-serf)         cfg="asap-otel-agent-b1-serf-prometheus.yaml" ;;
        b2-full)         cfg="asap-otel-agent-b2-full.yaml" ;;
        b3-delta)        cfg="asap-otel-agent-b3-delta.yaml" ;;
        b4-tunable)      cfg="asap-otel-agent-b4-tunable.yaml" ;;
        b5-gorilla)      cfg="asap-otel-agent-b5-gorilla-victoriametrics.yaml" ;;
        *) echo "unknown baseline: $baseline" >&2; exit 1 ;;
      esac

      cd "$COMPOSE_DIR"

      docker compose \
        -f base.yml -f "agents-${SCALE}.yml" -f "baseline-${baseline}.yml" \
        down >/dev/null 2>&1 || true

      # SKETCH_WINDOW is consumed by the B4 yaml's
      # ${env:SKETCH_WINDOW} expansion; agents on other baselines
      # accept the env harmlessly. Using `env` as the wrapper so
      # an empty window doesn't require separate branches.
      env_args=(
        EXPORTER_RATE="$rate"
        EXPORTER_CARDINALITY="$card"
        AGENT_CONFIG="$cfg"
      )
      if [[ -n "$window_env" ]]; then
        env_args+=(SKETCH_WINDOW="$window_env")
      fi
      env "${env_args[@]}" docker compose \
        -f base.yml -f "agents-${SCALE}.yml" -f "baseline-${baseline}.yml" \
        up -d >/dev/null 2>&1

      echo "# soaking ${SOAK_S}s..." >&2
      sleep "$SOAK_S"

      # Optionally drive a short PromQL replay against the
      # backend so `backend_query_p99_ms` lights up. Without this
      # the column is NaN for every cell — the backend's
      # asap_query_duration_seconds histogram only fires on
      # serviced queries (paper blocker #3, item 3). The replay
      # also feeds a client-side p99 fallback into
      # measure-baseline.py via --replay-jsonl, so the column is
      # populated even when Prometheus dies between cells.
      replay_args=()
      if (( DRIVE_QUERIES == 1 )); then
        replay_jsonl="${REPLAY_OUT_DIR}/replay-${tag}-${rate}-${card}.jsonl"
        echo "# driving queries qps=${QUERY_QPS} dur=${QUERY_DURATION_S}s → ${replay_jsonl}" >&2
        python3 "${SCRIPT_DIR}/metricsql_replay.py" \
          --target http://localhost:19091 \
          --controller http://localhost:18080 \
          --queries "${SCRIPT_DIR}/queries-e2e.json" \
          --qps "$QUERY_QPS" \
          --duration "$QUERY_DURATION_S" \
          --out "$replay_jsonl" \
          --no-plan-poll \
          >/dev/null 2>&1 || true
        replay_args+=(--replay-jsonl "$replay_jsonl")
      fi

      # `head=1` path emits the CSV header; subsequent iterations
      # emit only data rows.
      if (( head == 1 )); then
        python3 "${SCRIPT_DIR}/measure-baseline.py" \
          --baseline "$tag" --scale "$SCALE" \
          --rate "$rate" --cardinality "$card" \
          ${replay_args[@]+"${replay_args[@]}"}
        head=0
      else
        python3 "${SCRIPT_DIR}/measure-baseline.py" \
          --baseline "$tag" --scale "$SCALE" \
          --rate "$rate" --cardinality "$card" \
          ${replay_args[@]+"${replay_args[@]}"} \
          | tail -n +2
      fi
     done  # window
    done
  done
done

echo "# sweep complete." >&2
