# OTel Production: Distributed Agents + Gateway Configuration

## Topology

```
App pods
  │ OTLP gRPC
  ▼
agent.yaml  (DaemonSet, one per node)
  │ loadbalancing → DNS → otelcol-gateway-lb headless svc
  ▼
gateway-lb.yaml  (Deployment, stateless, scale freely)
  │ loadbalancing → DNS → otelcol-processing headless svc
  ▼                       (consistent hash by service)
gateway-processing.yaml  (Deployment, stateful sketch windows)
  │
  ▼
Backend (OTLP) + Prometheus scrape endpoint
```

---

## `agent.yaml` — Per-host / DaemonSet

Tier 0: runs on every host or as a K8s DaemonSet. Collects local OTLP, enriches with resource metadata, forwards to the gateway tier.

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318
  hostmetrics:
    collection_interval: 30s
    scrapers:
      cpu:
      memory:
      load:
      disk:
      network:

processors:
  memory_limiter:
    check_interval: 1s
    limit_mib: 512
    spike_limit_mib: 128
  resourcedetection:
    detectors: [env, system, docker]   # swap for [env, eks, ec2] on AWS
    timeout: 5s
    override: false
  batch:
    send_batch_size: 5000
    timeout: 200ms

exporters:
  loadbalancing:
    routing_key: service          # pin all metrics from a service to one gateway
    resolver:
      dns:
        hostname: otelcol-gateway-lb.observability.svc.cluster.local
        port: 4317
        interval: 30s
        timeout: 5s
    protocol:
      otlp:
        tls:
          insecure: false
          ca_file: /etc/otel/tls/ca.crt
        timeout: 10s
        sending_queue:
          enabled: true
          num_consumers: 4
          queue_size: 1000
        retry_on_failure:
          enabled: true
          initial_interval: 5s
          max_interval: 30s
          max_elapsed_time: 300s

extensions:
  health_check:
    endpoint: 0.0.0.0:13133
  pprof:
    endpoint: 0.0.0.0:1777

service:
  extensions: [health_check, pprof]
  telemetry:
    logs:
      level: warn
    metrics:
      level: basic
      address: 0.0.0.0:8888
  pipelines:
    metrics:
      receivers: [otlp, hostmetrics]
      processors: [memory_limiter, resourcedetection, batch]
      exporters: [loadbalancing]
```

---

## `gateway-lb.yaml` — Load-Balancing / Routing Tier

Tier 1: stateless, scales horizontally behind a standard LB. Consistent-hash routes metrics by service to the same processing-tier pod.

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
        max_recv_msg_size_mib: 64
      http:
        endpoint: 0.0.0.0:4318

processors:
  memory_limiter:
    check_interval: 1s
    limit_mib: 1024
    spike_limit_mib: 256
  batch:
    send_batch_size: 8000
    timeout: 200ms

exporters:
  loadbalancing:
    routing_key: service
    resolver:
      # K8s headless Service for the processing-tier Deployment
      dns:
        hostname: otelcol-processing.observability.svc.cluster.local
        port: 4317
        interval: 30s
        timeout: 5s
    protocol:
      otlp:
        tls:
          insecure: false
          ca_file: /etc/otel/tls/ca.crt
        timeout: 10s
        sending_queue:
          enabled: true
          num_consumers: 8
          queue_size: 2000
        retry_on_failure:
          enabled: true
          initial_interval: 5s
          max_interval: 60s
          max_elapsed_time: 300s

extensions:
  health_check:
    endpoint: 0.0.0.0:13133

service:
  extensions: [health_check]
  telemetry:
    logs:
      level: warn
    metrics:
      level: basic
      address: 0.0.0.0:8888
  pipelines:
    metrics:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [loadbalancing]
```

---

## `gateway-processing.yaml` — Sketch Processing Tier

Tier 2: stateful, one sketch instance per service (consistent-hash guaranteed). Applies KLL quantile sketches in window mode across all incoming metrics.

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
        max_recv_msg_size_mib: 64

processors:
  memory_limiter:
    check_interval: 1s
    limit_mib: 2048
    spike_limit_mib: 512

  # KLL quantile sketch — accumulates a 30s window before flushing
  KLL:
    mode: window
    window_duration: 30s
    k: 256
    quantiles: [0.5, 0.90, 0.95, 0.99]
    transmit_sketch: false    # export quantile values, not raw sketch bytes
    drop_original: true       # drop raw histograms/gauges after sketching
    aggregate_by: [region, env]
    label_matchers:
      env: prod

  batch:
    send_batch_size: 5000
    timeout: 500ms

exporters:
  otlp/backend:
    endpoint: backend.observability.svc.cluster.local:4317
    tls:
      insecure: false
      ca_file: /etc/otel/tls/ca.crt
    sending_queue:
      enabled: true
      num_consumers: 8
      queue_size: 5000
    retry_on_failure:
      enabled: true
      initial_interval: 5s
      max_interval: 60s
      max_elapsed_time: 600s

  # Optional: prometheus scrape endpoint on the processing tier
  prometheus:
    endpoint: 0.0.0.0:9090
    namespace: otelcol_processed

extensions:
  health_check:
    endpoint: 0.0.0.0:13133
  pprof:
    endpoint: 0.0.0.0:1777

service:
  extensions: [health_check, pprof]
  telemetry:
    logs:
      level: warn
    metrics:
      level: normal
      address: 0.0.0.0:8888
  pipelines:
    metrics:
      receivers: [otlp]
      processors: [memory_limiter, KLL, batch]
      exporters: [otlp/backend, prometheus]
```

---

## Key Design Decisions

| Decision | Reason |
|---|---|
| `routing_key: service` | All metrics for a service land on the same processing pod — required for `window` mode sketch correctness |
| DNS resolver → headless K8s Service | Automatically tracks pod IPs as the Deployment scales; no static list to maintain |
| `mode: window` + `window_duration: 30s` | Accumulates cross-batch data for better sketch accuracy vs. per-batch flush |
| `memory_limiter` first in every chain | Prevents OOM crashes under traffic spikes |
| TLS enabled on all hops | Production requirement — swap `ca_file` paths for your cert bundle |

## Swapping Sketch Processors

To use a different sketch on the processing tier, replace the `KLL` block with the corresponding processor and its config keys — the pipeline structure stays the same.

| Processor | Key config fields |
|---|---|
| `KLL` | `k`, `quantiles`, `mode`, `window_duration` |
| `HLL` | `precision`, `mode`, `window_duration` |
| `DDSketch` | `relative_accuracy`, `mode`, `window_duration` |
| `countminsketch` | `depth`, `width`, `mode`, `window_duration` |
| `countsketch` | `depth`, `width`, `mode`, `window_duration` |

All processors share: `transmit_sketch`, `drop_original`, `aggregate_by`, `label_matchers`.
