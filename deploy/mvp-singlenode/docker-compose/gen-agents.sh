#!/usr/bin/env bash
# Regenerate agents-N<K>.yml for an arbitrary N. The compose
# files are deliberately flat (no `deploy: replicas:`) so each
# agent has a distinct AGENT_ID label — see base.yml comment.
#
# Each agent mounts asap-otel-agent.yaml and receives OTLP from
# its own otel-app instance (otel-app-$i targets
# agent-$i:4317). Flow per replica:
#
#   otel-app-$i ──OTLP──▶ agent-$i (sketches) ──OTLP──▶ backend
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
#   otel-app-i ──OTLP──▶ agent-i ──OTLP──▶ backend
#
# Each agent mounts \`asap-otel-agent.yaml\` and runs DDSketch + HLL
# on the pipeline, so the bytes reaching the backend are already
# sketched (bandwidth-reduction signal scales with N).
#
# Issue #400: the asap-gateway hop was retired; agents push OTLP
# straight to backend:4317.

x-agent: &agent-base
  image: asap/asap-otel:dev
  depends_on:
    - backend
    - controller
  # The opampextension's \`remote_config_path\` (ASAPCollector#391)
  # writes the controller-pushed RemoteConfig back to
  # \`/etc/otel/config.yaml\` and exits, expecting Docker to restart
  # the container so the new config takes effect. Both of the
  # following are required for that flow to work:
  #   - the mount must be RW (no \`:ro\`) so the on-disk write succeeds
  #   - \`restart: unless-stopped\` so Docker brings the agent back up
  # See /mydata/mvp-smoke-test/compose/smoke-overlay.yml for the
  # working reference that established this contract.
  restart: unless-stopped
  command:
    - "--config=/etc/otel/config.yaml"
  volumes:
    # Default baseline = B2 full-sketch. Override via env var
    # AGENT_CONFIG to mount a different pipeline yaml — the
    # paper baselines' compose overlays (baseline-b*.yml) set
    # it before \`docker compose up\`. Compose expands the
    # \${VAR:-default} syntax at container start.
    - ../configs/\${AGENT_CONFIG:-asap-otel-agent-b2-full.yaml}:/etc/otel/config.yaml
  deploy:
    resources:
      limits:
        # DDSketch + HLL state at cardinality=1000 runs ~400MB;
        # 1 GiB leaves headroom for burst windows. Bump further
        # for higher-cardinality workloads.
        cpus: "1.0"
        memory: 1024M

x-producer: &producer-base
  image: asap/otel-app:dev

services:
  # The base.yml otel-app service is overridden here to
  # target agent-1 so its definition stays meaningful. compose
  # \`command:\` fully replaces base.yml's command, so the full flag
  # list is restated with -target pointed at the agent.
  otel-app:
    command:
      - "-target=agent-1:4317"
      # workload-sweep knobs. Defaults match the N=1 smoke-test.
      # Override at bring-up, e.g.:
      #   OTELAPP_FREQ_HZ=10000 OTELAPP_CARDINALITY=5000 \\
      #     docker compose -f base.yml -f agents-N1.yml up -d
      - "-cardinality=\${OTELAPP_CARDINALITY:-1000}"
      - "-freq-hz=\${OTELAPP_FREQ_HZ:-10}"
      - "-sdk-window=\${OTELAPP_SDK_WINDOW:-15s}"
      - "-sdk-projection=\${OTELAPP_SDK_PROJECTION:-}"
      - "-agg=\${OTELAPP_SDK_AGG:-default}"
      - "-max-buffer-per-series=\${OTELAPP_MAX_BUFFER_PER_SERIES:-0}"
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
      # in asap-otel-agent-b4-tunable.yaml. Other baselines
      # ignore this env. Default 60s.
      SKETCH_WINDOW: "\${SKETCH_WINDOW:-60s}"
EOF
done

# otel-app-1 is already defined as the base's `otel-app` override
# above; emit otel-app-2..N for i >= 2. These inherit only `image`
# from x-producer, so a minimal -target/-cardinality command is all
# they need — every other flag falls back to the binary default.
for ((i=2; i<=N; i++)); do
  cat <<EOF

  otel-app-$i:
    <<: *producer-base
    depends_on:
      - agent-$i
    command:
      - "-target=agent-$i:4317"
      - "-cardinality=\${OTELAPP_CARDINALITY:-1000}"
EOF
done
