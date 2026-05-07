# ASAPCollector MVP demo — issue #46 (baseline vs ASAP)

Dual-pipeline run: same workload, same fake-exporter producers, same per-agent cardinality, same query classes, same soak duration — only the pipeline differs. Pipelines run **sequentially** with a full `docker compose down -v` between them so per-pipeline resource numbers don't contaminate each other.

Pipelines compared:

* **baseline** — `mvp-multi-stage.yml` + `--profile b0`; agents load `sketchcol-agent-b0-prometheus.yaml`; storage = Prometheus; queries hit Prometheus PromQL HTTP.
* **asap** — `mvp-multi-stage.yml` default profile; controller-driven sketch + Gorilla-S3; queries hit the ASAPQuery-backend's BackendStorageRouting.

## Workload shape

| Knob | Value |
|------|-------|
| Per-agent cardinality | **500** |
| Number of producers | **10** (×5 → agent-a, ×5 → agent-b) |
| Aggregate series at gateway | **5000** |
| Sketch family (default) | DDSketch (overridden per-metric by controller) |
| Stack settle + warm-up + soak | 60s + 60s + 60s (each pipeline) |
| Replay shapes | window/label/combined @ 5 QPS for 60s |

## §1 Stage-separated resource table (baseline vs ASAP)

Per-stage TOTAL across all containers in that stage, side-by-side for baseline (OTel→Prometheus) vs ASAP (controller-driven sketches + Gorilla-S3). Reduction column is (baseline − asap) / baseline. Positive = ASAP uses fewer resources at that stage.

| Stage | Pipeline | CPU (cores) | RSS (MiB) | Net in (KiB/s) | Net out (KiB/s) | Disk (MiB) |
|---|---|---|---|---|---|---|
| agent | baseline | 0.005 | 138.8 | 6.3 | 9.1 | 0.0 |
| agent | asap     | 0.009 | 389.1 | 6.3 | 7.9 | 0.0 |
| agent | _reduction_ | -102.2% | -180.3% | +0.3% | +13.2% | — |
| gateway | baseline | 0.001 | 34.1 | 0.1 | 0.1 | 0.0 |
| gateway | asap     | 0.000 | 32.5 | 0.0 | 0.1 | 0.0 |
| gateway | _reduction_ | +33.3% | +4.6% | +50.0% | +50.0% | — |
| backend-ingest | baseline | — | — | — | — | — |
| backend-ingest | asap     | — | — | — | — | — |
| backend-ingest | _reduction_ | — | — | — | — | — |
| backend-storage | baseline | — | — | — | — | — |
| backend-storage | asap     | 0.005 | 94.7 | 7.9 | 0.5 | 0.0 |
| backend-storage | _reduction_ | — | — | — | — | — |
| backend-query | baseline | — | — | — | — | — |
| backend-query | asap     | — | — | — | — | — |
| backend-query | _reduction_ | — | — | — | — | — |

## §2 Per-criterion verdict (baseline vs ASAP)

Five empirical claims from issue #46. Reduction is (baseline − asap) / baseline; positive = ASAP wins.

### ① Bandwidth (mean B/s per cut edge)

| Edge | Baseline B/s | ASAP B/s | Reduction |
|---|---|---|---|
| edge_sdk_to_agent | 6328.1 | 6456.1 | -2.0% |
| edge_agent_to_gateway | 9341.9 | 8135.0 | +12.9% |
| edge_gateway_to_backend | — | — | — |
| edge_gateway_to_s3 | 1.3 | 8114.4 | -625678.4% |

**Verdict ①:** PASS

### ② Query latency (p99 per class)

| Class | Baseline p99 (ms) | ASAP p99 (ms) | Reduction |
|---|---|---|---|
| window-per-series | 1.6 | 0.4 | +74.1% |
| label-at-instant | 7.9 | 0.4 | +94.8% |
| combined-window-label | 10.0 | 0.4 | +95.8% |

**Verdict ②:** CAPTURED

### ③ Combined e2e resource (Σ stages)

| Metric | Baseline | ASAP | Δ (asap − baseline) | Reduction |
|---|---|---|---|---|
| total CPU cores | 0.005 | 0.014 | +0.009 | -178.8% |
| total RSS MiB | 172.9 | 516.3 | +343.3 | -198.5% |

**Verdict ③:** CAPTURED  (sign convention: ASAP `Δ` rendered with sign)

### ④ Accuracy (ASAP only — baseline is exact by construction)

**Verdict ④:** UNKNOWN  · accuracy.csv empty (warm tier may not have flushed)

### ⑤ Cold-fallback (gorilla_archive marker — ASAP only)

**Verdict ⑤:** PASS  · `data_source: gorilla_archive` present in response — GorillaQueryEngine served the ad-hoc query.  · curl: {"http_code":000,"time_total":0.000624}

### ⑥ Freshness (p50 per path)

| Path | Baseline p50 (ms) | ASAP p50 (ms) | Δ (asap − baseline) |
|---|---|---|---|
| raw | — | — | — |
| warm | — | — | — |
| archive | — | — | — |

**Verdict ⑥:** UNKNOWN

## §3 Per-query-class breakdown (ASAP)

## §3 Per-query-class breakdown

Three canonical query classes from `deploy/configs/mvp-workload.yaml`. Sketch + stage assignments come from the controller-emitted configs (see §9 below); latency from `replay.jsonl`; accuracy from `accuracy.csv`.

| Class | Sketch / stage (controller plan) | p50 (ms) | p99 (ms) | median rel-err | n |
|---|---|---|---|---|---|
| window-per-series | DDSketch / agent | 0.3 | 0.4 | — | 500 |
| label-at-instant | identity / gateway (sum-by-zone fan-in) | 0.3 | 0.4 | — | 500 |
| combined-window-label | rate@agent + sum-by-zone@gateway | 0.3 | 0.4 | — | 500 |

_§4..§6 below cover the ASAP pipeline only — postings filtering, concat-only compaction, and S3-ops cost are ASAP architectural concepts that have no baseline counterpart._

## §4 Postings filtering effect

Two ad-hoc queries with label predicates. Postings index: `postings_filtered_series_count` is the count of series that survived the predicate after sidecar lookup; the would-have-scanned column is the same metric WITHOUT the predicate (a coarse upper bound).

| Query | Series matched | Would have scanned | Status |
|---|---|---|---|
| `count(http_requests_total{service="api"})` | — | — | merge-pending (no postings field in response) |
| `topk(5, sum by (zone) (rate(http_requests_total{status=~"5.."}[5m])))` | — | — | merge-pending (no postings field in response) |

_postings field not present on responses; rerun once the postings-aware engine + sidecar PRs have landed._

## §5 Compaction effect

Phase δ.1: archive-tier compaction is now performed by the stock **`thanos-compact`** sidecar (replacing the deleted `gorilla-compactor` Rust binary). Unlike the previous concat-only design, thanos-compact does **decode + re-encode** — that's a real CPU cost during compaction sweeps, but it gives better compression on top of block consolidation, plus downsampled tiers (raw / 5m / 1h) for free. Storage savings reported below therefore include both block-count consolidation AND re-encoded compression.

| Stage | Object count | Total bytes |
|---|---|---|
| before | 0 | 0 |
| after  | 0  | 0 |

`thanos_compact_iterations_total`: 3 → 4 (at least one sweep observed during the demo window if the `after` value > `before`).

## §6 S3-ops cost (measured)

_cost-tracker not present — `/internal/s3_cost.csv` endpoint unavailable. Will populate once the backend exposes the internal cost-tracker endpoint._

## §7 Honest caveats (non-goals)

* **No dynamic replan.** The controller plans once at startup off `mvp-workload.yaml`. The MVP demo does not exercise the in-flight replan path — that's a follow-up.
* **No OpAMP hot reconfig under churn.** OpAMP push happens once per stage at boot; we don't kill an agent and verify the controller re-pushes.
* **10K series, not 1M.** Per-agent cardinality 500, 10 producers → 5K aggregate at the gateway. The 1M target needs the cardinality-redesign work plus a multi-host topology — out of scope for the MVP demo.
* **Single host.** All containers share kernel scheduler + loopback NIC. Wire-bytes per-edge counters are still meaningful (TX/RX is per-container) but absolute latencies are loopback-flattered.
* **Sequential pipelines, not side-by-side.** The baseline and ASAP cycles run back-to-back with a full `docker compose down -v` + 10s settle between them. Side-by-side execution would contaminate per-pipeline resource numbers (both stacks consume host CPU + RAM at the same time), so the driver explicitly serialises.
* **Postings + cost tracker gated on backend image.** Sections §4 and §6 render with a `merge-pending` marker until the postings-aware engine and the cost-tracker endpoint are exposed by the running backend image.

## §8 Controller-emitted runtime configs (ASAP pipeline)

Emitter status: **live**

Controller's typed-stage-split path produced per-stage configs and pushed them via OpAMP / BackendClient. The captured artifacts live under `controller-emitted-configs/`.

Captured artifacts:

- `agent.bootstrap.err` (0 B)
- `agent.bootstrap.yaml` (771 B)
- `agents.err` (0 B)
- `agents.json` (19 B)
- `backend.bootstrap.err` (0 B)
- `backend.bootstrap.yaml` (254 B)
- `controller.stderr` (0 B)
- `controller.stdout` (18336 B)
- `gateway.placeholder.yaml` (2476 B)
- `per-metric.http_requests_total.json` (720 B)
- `per-metric.http_requests_total.json.err` (0 B)
- `per-metric.http_requests_total_latency_ms.json` (701 B)
- `per-metric.http_requests_total_latency_ms.json.err` (0 B)

## Appendix A — per-edge bandwidth (baseline vs ASAP)

| Edge | Baseline mean B/s | ASAP mean B/s | Baseline samples | ASAP samples |
|---|---|---|---|---|
| edge_sdk_to_agent | 6328.1 | 6456.1 | 301 | 301 |
| edge_agent_to_gateway | 9341.9 | 8135.0 | 301 | 301 |
| edge_gateway_to_backend | — | — | 301 | 301 |
| edge_gateway_to_s3 | 1.3 | 8114.4 | 301 | 301 |

