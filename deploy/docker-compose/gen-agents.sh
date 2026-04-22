#!/usr/bin/env bash
# Regenerate agents-N<K>.yml for an arbitrary N. The compose
# files are deliberately flat (no `deploy: replicas:`) so each
# agent has a distinct AGENT_ID label — see base.yml comment.
#
# Each agent mounts sketchcol-agent.yaml and receives OTLP from
# its own fake-exporter instance (fake-exporter-$i targets
# agent-$i:4317). Flow per replica:
#
#   fake-exporter-$i ──OTLP──▶ agent-$i (sketches) ──OTLP──▶ gateway ──▶ backend
#
# Usage: ./gen-agents.sh 100 > agents-N100.yml
set -euo pipefail
N="${1:-10}"
cat <<EOF
# Overlay: $N edge agents + $N workload producers. Auto-generated
# by \`./gen-agents.sh $N > agents-N$N.yml\`. Do not edit by hand;
# edit \`gen-agents.sh\` if the per-agent template changes.
#
# Per-replica flow:
#
#   fake-exporter-i ──OTLP──▶ agent-i ──OTLP──▶ gateway ──promRW──▶ backend
#
# Each agent mounts \`sketchcol-agent.yaml\` and runs DDSketch + HLL
# on the pipeline, so the bytes reaching the gateway are already
# sketched (paper §6.2 bandwidth-reduction signal scales with N).

x-agent: &agent-base
  image: asap/sketchcol:dev
  depends_on:
    - gateway
    - controller
  command:
    - "--config=/etc/otel/config.yaml"
  volumes:
    # Default baseline = B2 full-sketch. Override via env var
    # AGENT_CONFIG to mount a different pipeline yaml — the
    # paper baselines' compose overlays (baseline-b*.yml) set
    # it before \`docker compose up\`. Compose expands the
    # \${VAR:-default} syntax at container start.
    - ../configs/\${AGENT_CONFIG:-sketchcol-agent-b2-full.yaml}:/etc/otel/config.yaml:ro
  deploy:
    resources:
      limits:
        # DDSketch + HLL state at cardinality=1000 runs ~400MB;
        # 1 GiB leaves headroom for burst windows. Bump further
        # for higher-cardinality workloads.
        cpus: "1.0"
        memory: 1024M

x-producer: &producer-base
  image: asap/fake-exporter:dev

services:
  # The base.yml fake-exporter service is overridden here to
  # target agent-1 so its definition stays meaningful.
  fake-exporter:
    environment:
      EXPORTER_TARGET: "agent-1:4317"
      # Paper §6 workload-sweep knobs. Defaults match the N=1
      # smoke-test. Override at bring-up:
      #
      #   EXPORTER_RATE=10000 EXPORTER_CARDINALITY=5000 \\
      #     docker compose -f base.yml -f agents-N1.yml up -d
      EXPORTER_RATE: "\${EXPORTER_RATE:-1000}"
      EXPORTER_CARDINALITY: "\${EXPORTER_CARDINALITY:-1000}"
EOF

for ((i=1; i<=N; i++)); do
  cat <<EOF

  agent-$i:
    <<: *agent-base
    hostname: agent-$i
    environment:
      AGENT_ID: "agent-$i"
      CONTROLLER_OPAMP_URL: "ws://controller:4320/v1/opamp"
      SKETCH_RUNTIME_GRPC_ENDPOINT: "http://controller:4321"
      RUST_LOG: "info"
      # B4 tunable-window baseline reads \${env:SKETCH_WINDOW}
      # in sketchcol-agent-b4-tunable.yaml. Other baselines
      # ignore this env. Default 60s.
      SKETCH_WINDOW: "\${SKETCH_WINDOW:-60s}"
EOF
done

# Fake-exporter-1 is already defined as the base's `fake-exporter`
# override above; emit fake-exporter-2..N for i >= 2.
for ((i=2; i<=N; i++)); do
  cat <<EOF

  fake-exporter-$i:
    <<: *producer-base
    depends_on:
      - agent-$i
    environment:
      EXPORTER_TARGET: "agent-$i:4317"
      EXPORTER_RATE: "\${EXPORTER_RATE:-1000}"
      EXPORTER_CARDINALITY: "\${EXPORTER_CARDINALITY:-1000}"
EOF
done
