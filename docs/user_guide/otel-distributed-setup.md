# OpenTelemetry Distributed Collector Setup

## Overview

This document summarizes how OpenTelemetry (OTel) Collector is deployed in a distributed architecture, covering the three standard deployment patterns, multi-tier data flow, and the key mechanisms that enable stateful horizontal scaling.

**References:**
- [Deploy the Collector](https://opentelemetry.io/docs/collector/deploy/) — OpenTelemetry official docs
- [Gateway Deployment Pattern](https://opentelemetry.io/docs/collector/deploy/gateway/) — OpenTelemetry official docs
- [Collector Architecture](https://opentelemetry.io/docs/collector/architecture/) — OpenTelemetry official docs
- [Scaling the Collector](https://opentelemetry.io/docs/collector/scaling/) — OpenTelemetry official docs
- [Agent Deployment Pattern](https://opentelemetry.io/docs/collector/deployment/agent/) — OpenTelemetry official docs
- [`loadbalancingexporter` README](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/loadbalancingexporter/README.md) — opentelemetry-collector-contrib

---

## Internal Pipeline Model

Every collector instance is built around a pipeline model:

```
Receivers → Processors (chained) → Exporters
```

- **Receivers** listen on ports (OTLP gRPC/HTTP, Jaeger, Prometheus scrape) or pull data actively.
- **Processors** transform data in sequence: attribute enrichment, filtering, batching, sampling, span-to-metrics, etc.
- **Exporters** forward to backends or to downstream collector instances.
- Pipelines are signal-typed: separate `traces`, `metrics`, `logs` pipelines per collector.
- **Connectors** bridge two pipelines internally (e.g., traces → metrics via spanmetrics connector).

---

## Three Deployment Patterns

### 1. No-Collector (Direct SDK Export)

Applications export directly to a backend. Simple, but couples application config to backend details. Not suitable for distributed setups.

> Reference: [Agent deployment pattern](https://opentelemetry.io/docs/collector/deployment/agent/)

### 2. Agent Pattern

A collector runs as a daemon on every host (VM) or as a **DaemonSet** pod on Kubernetes. Each agent:

- Collects host-level telemetry (host metrics, local logs, process data)
- Performs resource detection (adds host/pod/cloud metadata)
- Receives OTLP from local applications
- Forwards to a backend or to a gateway tier

Use when: host-local data collection or per-pod sidecar collection is needed.

> Reference: [Agent deployment pattern](https://opentelemetry.io/docs/collector/deployment/agent/)

### 3. Gateway Pattern

One or more collector instances run as a **centralized standalone service** (e.g., a Kubernetes Deployment behind a Service endpoint), typically one per cluster, data center, or region. The gateway:

- Applies centralized policies (sampling, filtering, credential management)
- Processes spans together — critical for tail-based sampling
- Routes to one or more backends

**Production recommendation: Agent + Gateway combined.**

> Reference: [Gateway deployment pattern](https://opentelemetry.io/docs/collector/deploy/gateway/)

---

## Two-Tier / Multi-Tier Data Flow

```
Applications
    │ OTLP
    ▼
Agents (per host / DaemonSet)       ← local collection + resource detection
    │ OTLP
    ▼
Load-Balancing Collector Tier       ← consistent-hash routing by traceID/service
    │ OTLP (hashed routing)
    ▼
Processing Collector Tier           ← tail sampling, span-metrics (stateful)
    │
    ▼
Backends (Jaeger, Prometheus, OTLP backend, etc.)
```

The two-tier split is driven by **stateful processors**: tail sampling and span-to-metrics must see **all spans for a given trace on the same collector instance**. With multiple replicas and naive round-robin, spans for the same trace land on different instances, corrupting sampling decisions and metric aggregations. The `loadbalancingexporter` solves this.

> Reference: [Scaling the Collector](https://opentelemetry.io/docs/collector/scaling/)

---

## Key Mechanism: `loadbalancingexporter`

From [`opentelemetry-collector-contrib`](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/loadbalancingexporter/README.md) — uses **consistent hashing** to deterministically route to the same backend for a given key.

### Routing Keys

| Key | Use Case |
|-----|----------|
| `traceID` | Default for traces — all spans of a trace go to the same backend |
| `service` | Default for metrics — all data from a service goes to the same backend |
| `metric` | Route by metric name |
| `resource` | Route by resource attributes |
| `streamID` | Route metrics by datapoint hash |
| `attributes` | Custom routing on span/metric attributes |

### Backend Discovery (Resolvers)

| Resolver | How It Works |
|----------|--------------|
| `static` | Fixed list of hostnames in config |
| `dns` | Periodically resolves a hostname to IPs — standard for K8s headless services |
| `k8s` | Watches Kubernetes EndpointSlices for dynamic pod discovery |
| `aws_cloud_map` | Discovers instances from AWS service registry |

### Example Configuration (Tier 1 Routing Collector)

```yaml
exporters:
  loadbalancing:
    routing_key: traceID
    resolver:
      dns:
        hostname: otelcol-backend.observability.svc.cluster.local
    protocol:
      otlp:
        tls:
          insecure: true
```

Point the DNS resolver at a **headless Kubernetes Service** for the Tier 2 Deployment, and it auto-discovers all pod IPs as they scale.

---

## Scaling Notes

| Signal / Component | Strategy |
|-------------------|----------|
| Stateless receivers (OTLP) | Scale horizontally with standard load balancers |
| Prometheus scrapers | Use **Target Allocator** (OTel Operator) to shard scrape targets across replicas |
| Stateful processors (tail sampling, span-metrics) | Two-tier pattern — pin traffic per trace/service via `loadbalancingexporter` |
| gRPC transport | Requires L7 LB (Envoy/Istio/cloud) or DNS resolver — L4 round-robin breaks persistent connections |

### Metrics to Watch

| Metric | Meaning |
|--------|---------|
| `otelcol_processor_refused_spans` | Memory limiter rejections — add collector capacity |
| `otelcol_exporter_queue_size` | Scale when hitting 60–70% of queue capacity |
| `otelcol_exporter_send_failed_spans` | Backend failures — fix backend, not collector scale |

> Reference: [Scaling the Collector](https://opentelemetry.io/docs/collector/scaling/)

---

## Processor Chain Best Practices

Always order processors as:

```
memory_limiter → [enrichment/transform processors] → batch
```

- `memory_limiter` **first** — prevents OOM crashes under load
- `batch` **last before exporters** — aggregates spans/metrics to reduce backend request volume

> Reference: [Collector Architecture](https://opentelemetry.io/docs/collector/architecture/)
