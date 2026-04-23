# Deploy TODO

_Last updated: 2026-04-23 (post PRs #168–#185)._

The multi-agent scaffold + baseline matrix + sweep harness are
in. Top-level blockers for the paper have moved upstream (see
`DataCollector/TODO.md`); the remaining work in this directory
is instrumentation completeness, Helm templates, and polish.

## Done (PRs #168–#185)

- `deploy/docker-compose/` — `base.yml` + `agents-N{1,10,100}.yml`
  overlays + `gen-agents.sh`
- 7 baseline overlays (b0a, b0b, b1, b2, b3, b4, b5)
- Dockerfiles: `Dockerfile.{sketchcol, sketchcol-stock, backend,
  controller, fake-exporter}`
- Configs: prom scrape, Grafana datasource, per-baseline
  sketchcol-agent YAML, gateway YAML, backend streaming YAML
- `deploy/fake-exporter/` — synthetic producer + trace-replay
  mode + demo trace CSV
- `deploy/scripts/run-baseline-sweep.sh` +
  `deploy/scripts/measure-baseline.py`
- N=1 sweep + N=10 sweep CSVs archived under `eval-results/`
- Helm chart skeleton: `Chart.yaml`, `values.yaml` with paper's
  resource envelope

## Instrumentation gaps (P1 — blocks the cost / query figures)

The SDK cost sweeps defined in
[`../docs/sdk-cost-evaluation.md`](../docs/sdk-cost-evaluation.md)
require **producer-side** measurements; the existing multi-agent
sweep CSV also has several `nan` cells on the collector side.
Both tracked top-level in `DataCollector/TODO.md`.

### Producer-side columns (new, P0)

- [ ] **`producer_cpu_cores`** — `docker stats` or cgroup read
      on the instrumented-application container. Needed to
      measure what each SDK `agg_type` costs the producer
      (`*-delta` vs `*-full`, sketch vs `raw-buffer`).
- [ ] **`producer_rss_mib`** — same source. `agg_type=raw-buffer`
      is expected to have the largest producer RSS (sample
      buffer); we need to measure the knee vs window `W`.
- [ ] **`producer_bytes_out_per_s`** — container
      `container_network_transmit_bytes_total{name="fake-exporter"}`,
      `rate()` over the measurement window. Primary bandwidth
      axis; inferring from gateway-side counters conflates
      multiple producers at `N > 1`.

- [ ] **Byte counters on raw and Gorilla baselines.**
      `agent_in_kib_per_s` / `agent_out_kib_per_s` are `nan`
      on b0a, b0b, b1, b5. Either emit the counters from the
      processor (sketchcol already has `bytes_in / bytes_out`)
      or fall back to container network RX/TX scraped via
      cAdvisor.
- [ ] **Gateway points/s on sketch baselines.**
      `gateway_points_per_s` / `gateway_out_series_per_s` are
      `nan` on b1, b2, b3, b5. The gateway emits them for raw
      traffic but not for sketch-carrying OTLP — needs a
      counter that increments on sketch data point arrivals.
- [ ] **Backend samples/s on sketch baselines.** Same root
      cause as the gateway gap.
- [ ] **Per-processor overhead.** sketchcol exposes
      `processor_samples_total / processor_cpu_seconds` per
      processor. `measure-baseline.py` doesn't record these
      per-processor; add a column group.
- [ ] **Grafana dashboards.** `configs/grafana-datasources.yml`
      provisions the datasource. No dashboard JSONs checked in.
      One dashboard per evaluation axis: producer CPU, producer
      bandwidth, query latency, workload drift, N-scale.

## Query side of the sweep (P1)

- [ ] **PromQL replay process.** Co-located with the load
      generator, issues a query suite (avg / p99 / rate / topK
      × {1m, 5m, 1h} windows) at steady rate for the SOAK
      window. Records per-query latency + ε from the response.
- [ ] **Sweep CSV columns.** Add `query_p50_ms`,
      `query_p99_ms`, `cold_bytes_served`, `barrier_drops`.

Blocks the query-latency evaluation tracked at the top-level
`TODO.md`.

## Helm templates (F5 post-paper; P2 for reproducibility archive)

The `values.yaml` + `Chart.yaml` land. Templates don't — writing
them wants an initial pass on a real cluster to validate the
readiness probes, resource requests, and network policies.
Ordered for landing one-at-a-time:

- [ ] `templates/_helpers.tpl` — label selectors, name prefix
- [ ] `templates/controller.yaml` — Deployment + Service
- [ ] `templates/backend.yaml` — Deployment + Service +
      PVC for sketch-DB disk
- [ ] `templates/gateway.yaml` — Deployment + Service
- [ ] `templates/agents.yaml` — Deployment (replicas =
      `{{ .Values.agents.count }}`) + headless Service for
      Prometheus DNS SD
- [ ] `templates/minio.yaml` — StatefulSet + PVC + Service
      (gate on `.Values.minio.enabled`)
- [ ] `templates/prometheus.yaml` + `templates/grafana.yaml`

## Compose polish

- [ ] **Per-agent `AGENT_ID` label.** The static enumeration
      in `agents-N*.yml` works for N ≤ 100 but makes Prometheus
      target lists verbose. Once the agent emits its own
      hostname as a label, the scrape config collapses to a
      single DNS-SD rule.
- [ ] **CI check** that compose files parse clean and
      `base.yml + agents-N<K>.yml + baseline-*.yml` merge
      produces a valid combined config.
- [ ] **`deploy/k8s/` plain manifests** as an alternative to
      Helm for operators who don't want Helm. Lowest priority.

## Fault injection (P3; tracked top-level in `TODO.md`)

- [ ] `fault-injection/controller-kill.sh` — docker kill,
      assert queries continue from last-known plan.
- [ ] `fault-injection/agent-kill.sh` — docker kill of one
      agent, assert controller marks it degraded and replans.
- [ ] `fault-injection/network-partition.sh` — `docker network
      disconnect` agent ⇄ controller, assert agent runs last
      config, reconciliation at heal.

ChaosMesh variants on K8s go under the Helm chart.

## Not in scope here

- **N=10 throughput collapse** — lives at the system level
  (producer SDK + kernel + docker proxy), tracked top-level
  in `TODO.md`.
- **Google cluster trace fetcher** — `datasets_eval/` territory,
  tracked top-level in `TODO.md`.
- **Controller feedback loop over real workload** — depends on
  the throughput, query-side, and workload items above; tracked
  top-level.
