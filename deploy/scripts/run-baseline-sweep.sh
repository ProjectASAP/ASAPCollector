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
#   SOAK_S=120                            per-config soak seconds
#   SCRIPT_DIR=.../deploy/scripts         override lookup path
#
# The caller is responsible for having the compose stack's images
# built (asap/sketchcol:dev, asap/query-backend:dev, asap/fake-
# exporter:dev). Each iteration does a full `docker compose down`
# to ensure a clean state.
set -euo pipefail

SCRIPT_DIR="${SCRIPT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
COMPOSE_DIR="${SCRIPT_DIR}/../docker-compose"

BASELINES="${BASELINES:-b0-raw b1-serf b2-full b3-delta b5-gorilla}"
SCALE="${SCALE:-N1}"
RATES="${RATES:-1000}"
CARDS="${CARDS:-1000}"
SOAK_S="${SOAK_S:-60}"

# Emit header once.
head=1
for baseline in $BASELINES; do
  for rate in $RATES; do
    for card in $CARDS; do
      echo "# === baseline=${baseline} rate=${rate} card=${card} ===" >&2

      # Map baseline tag to AGENT_CONFIG filename.
      case "$baseline" in
        b0-raw)    cfg="sketchcol-agent-b0-raw.yaml" ;;
        b1-serf)   cfg="sketchcol-agent-b1-serf.yaml" ;;
        b2-full)   cfg="sketchcol-agent-b2-full.yaml" ;;
        b3-delta)  cfg="sketchcol-agent-b3-delta.yaml" ;;
        b4-tunable) cfg="sketchcol-agent-b4-tunable.yaml" ;;
        b5-gorilla) cfg="sketchcol-agent-b5-gorilla.yaml" ;;
        *) echo "unknown baseline: $baseline" >&2; exit 1 ;;
      esac

      cd "$COMPOSE_DIR"

      docker compose \
        -f base.yml -f "agents-${SCALE}.yml" -f "baseline-${baseline}.yml" \
        down >/dev/null 2>&1 || true

      EXPORTER_RATE="$rate" EXPORTER_CARDINALITY="$card" AGENT_CONFIG="$cfg" \
        docker compose \
        -f base.yml -f "agents-${SCALE}.yml" -f "baseline-${baseline}.yml" \
        up -d >/dev/null 2>&1

      echo "# soaking ${SOAK_S}s..." >&2
      sleep "$SOAK_S"

      # `head=1` path emits the CSV header; subsequent iterations
      # emit only data rows.
      if (( head == 1 )); then
        python3 "${SCRIPT_DIR}/measure-baseline.py" \
          --baseline "$baseline" --scale "$SCALE" \
          --rate "$rate" --cardinality "$card"
        head=0
      else
        python3 "${SCRIPT_DIR}/measure-baseline.py" \
          --baseline "$baseline" --scale "$SCALE" \
          --rate "$rate" --cardinality "$card" \
          | tail -n +2
      fi
    done
  done
done

echo "# sweep complete." >&2
