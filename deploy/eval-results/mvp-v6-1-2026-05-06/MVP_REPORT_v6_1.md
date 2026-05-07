# ASAPCollector MVP demo — issue #46 (v6, controller-driven multi-stage)

Single-cell controller-driven run: 10 producers → 2 agents → 1 gateway → 1 backend (+ MinIO archive). Controller plans from `deploy/configs/mvp-v6-workload.yaml`; per-stage configs are emitted via the typed-stage-split path (`USE_TYPED_STAGE_SPLIT=1`). Six criteria + per-class latency + per-edge bandwidth in this report.

## Workload shape

| Knob | v6 value |
|------|---------|
| Per-agent cardinality | **500** |
| Number of producers | **10** (×5 → agent-a, ×5 → agent-b) |
| Aggregate series at gateway | **5000** |
| Sketch family (default) | DDSketch (overridden per-metric by controller) |
| Stack settle + warm-up + soak | 60s + 60s + 60s |
| Replay shapes | window/label/combined @ 5 QPS for 60s |

## §1 Stage-separated resource table

Per-stage TOTAL across all containers in that stage. The v6 multi-stage topology has 10 producer / 2 agent / 1 gateway / 1 backend / 1 storage (MinIO) containers; the rows below reduce all relevant containers per stage to a single number. CPU is mean cores over the 60s window, RSS is mean MiB, net is window-rate KiB/s (from `docker stats` NetIO deltas), disk is end-of-window MiB.

| Stage | CPU (cores) | RSS (MiB) | Net in (KiB/s) | Net out (KiB/s) | Disk (MiB) |
|---|---|---|---|---|---|
| agent | 0.008 | 149.7 | 6.5 | 106.7 | 0.0 |
| gateway | — | — | — | — | — |
| backend-ingest | 0.005 | 28.8 | 93.0 | 1.8 | 0.0 |
| backend-storage | — | — | — | — | — |
| backend-query | 0.005 | 28.8 | 93.0 | 1.8 | 0.0 |

## §2 Per-criterion verdict (6 criteria)

| # | Criterion | Verdict | Detail |
|---|---|---|---|
| 1 | Bandwidth (per-edge B/s) | **FAIL** | sdk→agent=6877.9 B/s, agent→gateway=114712.5 B/s, gateway→backend=100079.1 B/s, gateway→s3=15186.8 B/s |
| 2 | Query latency (p50/p99 per class) | **PASS** | window-per-series: p50=1.1ms p99=1.9ms n=100; label-at-instant: p50=0.9ms p99=1.5ms n=100; combined-window-label: p50=0.9ms p99=1.5ms n=100 |
| 3 | Combined resource (sum of stages) | **CAPTURED** | total cpu_cores=0.019, total rss_mib=207.2 (stages: agent, backend-ingest, backend-query) |
| 4 | Accuracy (rel-err per class) | **UNKNOWN** | accuracy.csv empty (warm tier may not have flushed) |
| 5 | Cold-fallback (gorilla_archive marker) | **PASS** | `data_source: gorilla_archive` present in response — GorillaQueryEngine served the ad-hoc query.  · curl: {"http_code":404,"time_total":0.000619} |
| 6 | Freshness (p50/p99 per path) | **UNKNOWN** | no freshness samples on any path |

## §3 Per-query-class breakdown

Three canonical query classes from `deploy/configs/mvp-v6-workload.yaml`. Sketch + stage assignments come from the controller-emitted configs (see §9 below); latency from `replay.jsonl`; accuracy from `accuracy.csv`.

| Class | Sketch / stage (controller plan) | p50 (ms) | p99 (ms) | median rel-err | n |
|---|---|---|---|---|---|
| window-per-series | DDSketch / agent | 1.1 | 1.9 | — | 100 |
| label-at-instant | identity / gateway (sum-by-zone fan-in) | 0.9 | 1.5 | — | 100 |
| combined-window-label | rate@agent + sum-by-zone@gateway | 0.9 | 1.5 | — | 100 |

## §4 Postings filtering effect

Two ad-hoc queries with label predicates. Per the v5 postings index: `postings_filtered_series_count` is the count of series that survived the predicate after sidecar lookup; the would-have-scanned column is the same metric WITHOUT the predicate (a coarse upper bound).

| Query | Series matched | Would have scanned | Status |
|---|---|---|---|
| `count(http_requests_total{service="api"})` | — | — | v5-merge-pending (no postings field in response) |
| `topk(5, sum by (zone) (rate(http_requests_total{status=~"5.."}[5m])))` | — | — | v5-merge-pending (no postings field in response) |

_v5 postings field not present on responses; rerun once `#295 ASAPCollector` and `#90 ASAPQuery-backend` land._

## §5 Compaction effect

Concat-only compactor (v5) byte-concatenates 6+ adjacent blocks ≥6h old into one merged object. **No decode / re-encode** — each source chunk remains an atomic Gorilla chunk inside the merged file. The new manifest records each chunk's `byte_offset` + `byte_length` so the backend can issue `Range:` partial reads.

| Stage | Object count | Total bytes |
|---|---|---|
| before | 0 | 0 |
| after  | 0  | 0 |

## §6 S3-ops cost (measured)

Counts of PUT / GET / HEAD / DELETE issued against MinIO during the cell's measurement window (the v5 backend's internal cost tracker).

| bytes_got | bytes_put | delete_count | get_count | head_count | list_count | put_count |
|---|---|---|---|---|---|---|
| 0 | 0 | 0 | 0 | 0 | 0 | 0 |

## §7 Honest caveats (non-goals)

* **No dynamic replan.** The controller plans once at startup off `mvp-v6-workload.yaml`. v6 does not exercise the in-flight replan path — that's a follow-up.
* **No OpAMP hot reconfig under churn.** OpAMP push happens once per stage at boot; we don't kill an agent and verify the controller re-pushes. v5's `ReplannerOpampGateway` covers some of this; v6's typed-stage-split path doesn't.
* **10K series, not 1M.** Per-agent cardinality 500, 10 producers → 5K aggregate at the gateway. The 1M target needs the cardinality-redesign work plus a multi-host topology — out of scope for v6.
* **Single host.** All containers share kernel scheduler + loopback NIC. Wire-bytes per-edge counters are still meaningful (TX/RX is per-container) but absolute latencies are loopback-flattered.
* **B0 Prometheus reference is opt-in.** The v6 driver does NOT bring up B0 in the same compose stack as the ASAP backend (port collision on 19090). To get an A-vs-B comparison row, run a separate B0 cycle and join the stages.csv files manually.
* **Postings + cost tracker gated on v5 merge.** Sections §4 and §6 render with a `v5-merge-pending` marker until PRs #295 (collector) and #90 (backend) land.

## §8 Controller-emitted runtime configs

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
- `controller.stdout` (18769 B)
- `gateway.placeholder.yaml` (2362 B)
- `per-metric.http_requests_total.json` (720 B)
- `per-metric.http_requests_total.json.err` (0 B)
- `per-metric.http_requests_total_latency_ms.json` (701 B)
- `per-metric.http_requests_total_latency_ms.json.err` (0 B)

## Appendix A — per-edge bandwidth

| Edge | Mean B/s | Samples |
|---|---|---|
| edge_sdk_to_agent | 6877.9 | 61 |
| edge_agent_to_gateway | 114712.5 | 61 |
| edge_gateway_to_backend | 100079.1 | 61 |
| edge_gateway_to_s3 | 15186.8 | 61 |

