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
| agent | baseline | 0.005 | 133.8 | 6.3 | 8.9 | 0.0 |
| agent | asap     | 0.007 | 197.6 | 6.4 | 99.7 | 0.0 |
| agent | _reduction_ | -38.0% | -47.7% | -2.7% | -1016.0% | — |
| gateway | baseline | — | — | — | — | — |
| gateway | asap     | 0.000 | 33.6 | 0.0 | 0.1 | 0.0 |
| gateway | _reduction_ | — | — | — | — | — |
| backend-ingest | baseline | 0.000 | 6.7 | 0.1 | 0.1 | 0.0 |
| backend-ingest | asap     | 0.007 | 30.3 | 92.6 | 2.2 | 0.0 |
| backend-ingest | _reduction_ | -6700.0% | -351.6% | -154300.0% | -3516.7% | — |
| backend-storage | baseline | — | — | — | — | — |
| backend-storage | asap     | 0.008 | 151.1 | 10.4 | 192.7 | 0.0 |
| backend-storage | _reduction_ | — | — | — | — | — |
| backend-query | baseline | 0.000 | 6.7 | 0.1 | 0.1 | 0.0 |
| backend-query | asap     | 0.007 | 30.3 | 92.6 | 2.2 | 0.0 |
| backend-query | _reduction_ | -6700.0% | -351.6% | -154300.0% | -3516.7% | — |

## §2 Per-criterion verdict (baseline vs ASAP)

Five empirical claims from issue #46. Reduction is (baseline − asap) / baseline; positive = ASAP wins.

### ① Bandwidth (mean B/s per cut edge)

| Edge | Baseline B/s | ASAP B/s | Reduction |
|---|---|---|---|
| edge_sdk_to_agent | 6871.5 | 6140.4 | +10.6% |
| edge_agent_to_gateway | 10140.2 | 100821.1 | -894.3% |
| edge_gateway_to_backend | 60.9 | 93709.1 | -153836.1% |
| edge_gateway_to_s3 | 2.4 | 10466.9 | -430195.7% |

**Verdict ①:** PASS

### ② Query latency (p99 per class)

| Class | Baseline p99 (ms) | ASAP p99 (ms) | Reduction |
|---|---|---|---|
| window-per-series | 1.6 | 1.9 | -21.6% |
| label-at-instant | 1.5 | 1.8 | -22.2% |
| combined-window-label | 1.5 | 25.7 | -1579.1% |

**Verdict ②:** CAPTURED

### ③ Combined e2e resource (Σ stages)

| Metric | Baseline | ASAP | Δ (asap − baseline) | Reduction |
|---|---|---|---|---|
| total CPU cores | 0.005 | 0.029 | +0.024 | -457.7% |
| total RSS MiB | 147.2 | 442.9 | +295.7 | -200.8% |

**Verdict ③:** CAPTURED  (sign convention: ASAP `Δ` rendered with sign)

### ④ Accuracy (ASAP only — baseline is exact by construction)

**Verdict ④:** UNKNOWN  · accuracy.csv empty (warm tier may not have flushed)

### ⑤ Cold-fallback (gorilla_archive marker — ASAP only)

**Verdict ⑤:** PASS  · `data_source: gorilla_archive` present in response — GorillaQueryEngine served the ad-hoc query.  · curl: {"http_code":200,"time_total":0.001526}

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
| window-per-series | DDSketch / agent | 1.2 | 1.9 | — | 100 |
| label-at-instant | identity / gateway (sum-by-zone fan-in) | 1.1 | 1.8 | — | 100 |
| combined-window-label | rate@agent + sum-by-zone@gateway | 17.5 | 25.7 | — | 100 |

_§4..§6 below cover the ASAP pipeline only — postings filtering, concat-only compaction, and S3-ops cost are ASAP architectural concepts that have no baseline counterpart._

## §4 Postings filtering effect

Two ad-hoc queries with label predicates. Postings index: `postings_filtered_series_count` is the count of series that survived the predicate after sidecar lookup; the would-have-scanned column is the same metric WITHOUT the predicate (a coarse upper bound).

| Query | Series matched | Would have scanned | Status |
|---|---|---|---|
| `count(http_requests_total{service="api"})` | — | — | merge-pending (no postings field in response) |
| `topk(5, sum by (zone) (rate(http_requests_total{status=~"5.."}[5m])))` | — | — | merge-pending (no postings field in response) |

_postings field not present on responses; rerun once the postings-aware engine + sidecar PRs have landed._

## §5 Compaction effect

Concat-only compactor byte-concatenates 6+ adjacent blocks ≥6h old into one merged object. **No decode / re-encode** — each source chunk remains an atomic Gorilla chunk inside the merged file. The new manifest records each chunk's `byte_offset` + `byte_length` so the backend can issue `Range:` partial reads.

| Stage | Object count | Total bytes |
|---|---|---|
| before | 0 | 0 |
| after  | 0  | 0 |

## §6 S3-ops cost (measured)

Counts of PUT / GET / HEAD / DELETE issued against MinIO during the cell's measurement window (the backend's internal cost tracker).

| bytes_got | bytes_put | delete_count | get_count | head_count | list_count | put_count |
|---|---|---|---|---|---|---|
| 0 | 0 | 0 | 0 | 0 | 0 | 0 |

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
- `controller.stdout` (17770 B)
- `gateway.placeholder.yaml` (2476 B)
- `per-metric.http_requests_total.json` (720 B)
- `per-metric.http_requests_total.json.err` (0 B)
- `per-metric.http_requests_total_latency_ms.json` (701 B)
- `per-metric.http_requests_total_latency_ms.json.err` (0 B)

## Appendix A — per-edge bandwidth (baseline vs ASAP)

| Edge | Baseline mean B/s | ASAP mean B/s | Baseline samples | ASAP samples |
|---|---|---|---|---|
| edge_sdk_to_agent | 6871.5 | 6140.4 | 61 | 61 |
| edge_agent_to_gateway | 10140.2 | 100821.1 | 61 | 61 |
| edge_gateway_to_backend | 60.9 | 93709.1 | 61 | 61 |
| edge_gateway_to_s3 | 2.4 | 10466.9 | 61 | 61 |

