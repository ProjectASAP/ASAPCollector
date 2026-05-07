# ASAP vs. Databricks Pantheon + Hydra

## Why this doc

Databricks published an architecture overview of their internal monitoring stack
(Pantheon + Hydra) on 2026-04-29:

> *10 trillion samples per day: scaling beyond traditional monitoring infra at
> Databricks*
> https://www.databricks.com/blog/10-trillion-samples-day-scaling-beyond-traditional-monitoring-infra-databricks

Their stack ingests **10+ trillion samples/day** across **5 billion active
timeseries** in **~70 cloud regions**, and tackles a problem set that overlaps
substantially with ASAP's: cardinality control, raw-retention cost, query
freshness, and the cost of running a large PromQL surface.

The system is a useful real-world data point for ASAP's positioning. This doc
compares the two stacks side-by-side, calls out where ASAP's novelty is real
versus where Databricks is decisively ahead, and proposes paper framing that
acknowledges the overlap honestly.

## Pantheon + Hydra summary

The Databricks stack has two query backends federated through a single set of
operator-facing interfaces (Grafana / Databricks SQL):

### Pantheon — TSDB layer

A fork of CNCF Thanos optimised for Databricks scale.

- **160+ Thanos instances globally**; largest hosts **~300M in-memory series**
  and serves **~1,000 PromQL queries/second**.
- **Tiered storage**: in-memory (30 min for ephemeral workloads, 2 h for
  persistent services) → on-disk (24 h) → object storage (long-term).
- **Three isolated Kubernetes StatefulSets per Receive group** rather than a
  single hash ring; preserves three-way replication with quorum writes.
- **At-least-once uploads**: only 2 of 3 StatefulSets upload blocks to object
  storage to reduce redundant traffic while keeping durability via replication.
- **Multi-tenant** via rule-based attribution at the router layer.
- **Self-healing controllers**: rollout operator, hashring controller,
  autoscaling/self-healing controller; remediates dozens of failures weekly.

Pantheon serves the **hot** path: alerting, dashboards, real-time PromQL.

### Aggregation pipeline (Telegraf + Dicer)

The cardinality firewall in front of Pantheon.

- Built on Telegraf with a Databricks-internal "auto-sharder" service, Dicer.
- **Thousands of operator-authored aggregation rules** drop expensive labels at
  ingestion time (especially for serverless workloads producing millions of
  short-lived series per day).
- Sustained throughput **>1 GB/s** in the largest region.
- During one infrastructure incident producing a **2–5× metric surge**,
  Telegraf absorbed most of the load and Pantheon only saw a **20% surge**.

This is the "bend the curve" of cardinality growth so Pantheon's scaling is
decoupled from infrastructure scaling.

### Hydra — lakehouse-based raw platform

For the high-cardinality troubleshooting cases that aggregation removes.

- **Apache Spark Structured Streaming** with exactly-once semantics ingests
  raw metrics into **Delta Lake on object storage**.
- **Databricks Auto Loader** for incremental file discovery without manual
  listing.
- Partitioned by region; independent streaming jobs per region for independent
  autoscaling and isolated blast radius.
- Stores **20 billion unaggregated active timeseries** with **5-minute
  end-to-end freshness**.
- **50× cheaper storage than Pantheon** (Thanos object-store layout vs. Delta
  Lake compression).
- Query interface: **PromQL via Grafana** (a PromQL-to-SQL conversion layer)
  *and* direct SQL via Databricks for advanced analysis.

## ASAP summary (brief — see Design.tex / paper for detail)

ASAP is a controller-planned, sketch-based, three-tier observability stack:

- **Edge runtimes**: sketches (DDSketch, KLL, HLL, CountSketch, Count-Min
  Sketch) embedded in the OpenTelemetry SDK and three collector binaries
  (`sketchcol` Go, `sketchotap` Rust, `sketchtelegraf` Go) with
  bit-identical wire format. Agents emit sparse **delta-of-sketch** envelopes
  over a modified-OTLP wire.
- **Controller**: observes the live PromQL query log and plans, per metric, a
  triple `(W, L, agg_type)` — window, label projection, encoding — that is
  hot-reloaded into agents, gateways, and the backend via OpAMP. The
  controller's stage-allocator decides at *which* tier (SDK / agent / gateway
  / backend) sketches actually run.
- **Backend**: a single PromQL HTTP surface that capability-matches each query
  to one of three internal tiers:
  - **Warm** — sketch precompute store + `SimpleEngine`. Returns the answer
    annotated with the sketch's `(ε, δ, kind)` accuracy envelope.
  - **Archive** — `GorillaQueryEngine` over self-describing Gorilla-XOR chunks
    on S3. Returns *exact* PromQL answers (`ε = 0`, `δ = 0`) for queries the
    warm tier cannot answer.
  - **Cold-fallback** (being deprecated, see "Open question" below) — raw
    JSONL parts for surprise queries on un-archived metrics.

## Side-by-side comparison

| Axis | Pantheon + Hydra (Databricks) | ASAP |
|------|-------------------------------|------|
| **Where summarisation runs** | Gateway only (Telegraf intermediates aggregation rules) | Anywhere on the SDK / agent / gateway / backend chain — chosen per metric by the controller's stage-allocator |
| **What summarisation means** | Drop labels, aggregate samples — *dimensional* loss, no formal accuracy bound | Sketch summarisation within projection `L` — *value* loss with `(ε, δ)` envelope |
| **Source-to-gateway wire** | Prometheus remote-write (raw samples) | Modified-OTLP with typed sketch envelopes; sparse delta-of-sketch |
| **Approximation visibility** | Implicit — operator picks the rule, downstream consumers don't see error bound | Explicit — `(ε, δ, kind)` carried in every PromQL response's `infos` field |
| **Tier topology** | 2 federated backends: Pantheon (hot) + Hydra (raw lakehouse) | 3 internal tiers under **one** PromQL surface: warm sketch + Gorilla-S3 archive + cold-fallback (deprecating) |
| **Raw retention** | Hydra: Spark Structured Streaming → Delta Lake → 5 min freshness | Gorilla-S3 archive (XOR chunks on S3) → exact PromQL via `GorillaQueryEngine` |
| **Cardinality control** | Thousands of operator-authored aggregation rules | Controller mines PromQL log + cost model; replans online via OpAMP |
| **Query languages** | PromQL on Pantheon; PromQL-to-SQL conversion on Hydra | PromQL only; capability-routed to the right tier internally |
| **Deployment shape** | Kubernetes StatefulSets, multi-cloud, multi-tenant, self-healing | Research prototype; single-host paired-sweep; Docker Compose |

## Scale gap

| Metric | Databricks (production) | ASAP (current eval) |
|--------|-------------------------|---------------------|
| Active series | 5 × 10⁹ | 10⁴ (paired sweep) |
| Daily samples | 10¹³+ | single-host bench |
| Regions | 70+ | 1 host |
| Largest instance | ~3 × 10⁸ in-memory series | n/a |
| Production hardening | self-healing, multi-cloud, multi-tenant | research prototype |

ASAP's evaluation is six orders of magnitude smaller than Databricks'
production scale. Direct numerical comparisons are not meaningful; the
contribution ASAP claims is *architectural* (different design choices that
become more attractive at smaller deployment scales or in resource-constrained
edge settings) rather than scale-comparable.

## Where ASAP claims novelty (relative to Pantheon + Hydra)

1. **Sketches can run anywhere along the chain, automatically planned.**
   Databricks' aggregation runs at one fixed tier (Telegraf gateway), and the
   rules are operator-authored. ASAP's controller stage-allocator picks
   per-metric placement (SDK / agent / gateway / backend) using a cost model
   that costs candidates in units that span tiers (sketch state RAM, wire
   bytes per cut edge, edge CPU, gateway CPU, backend CPU). The DAG fan-in
   credit (`workload_cost`) particularly favours **gateway placement** when
   many agents emit the same metric and the cross-host merge can be done
   once at the gateway instead of N times at the backend.

2. **Bounded approximation as a first-class API contract.**
   Every ASAP PromQL response surfaces `(ε, δ, kind)` in `infos`. Autoscalers,
   alerters, and dashboards can decide whether to act on the approximate
   answer or escalate to the exact archive tier. Databricks aggregation
   drops information silently — downstream consumers cannot tell when a
   metric they are reading has been pre-aggregated.

3. **Workload-driven controller planning.**
   Databricks operators hand-author thousands of aggregation rules. ASAP's
   controller mines the PromQL query log and plans per-metric placement
   automatically, replanning online when the query workload drifts. This
   automates what Databricks does manually; viable at smaller scale,
   not yet demonstrated at Databricks scale.

4. **One PromQL surface over multiple internal tiers.**
   Databricks federates two backends (Pantheon for hot, Hydra for raw) accessed
   via different query languages (PromQL vs. PromQL-to-SQL). ASAP routes a
   single PromQL query to warm / archive / cold tiers internally via
   `capability_matching` — the caller cannot tell which tier answered except
   via the `data_source` tag in `infos`. Whether this unification scales to
   Databricks-style global federation is open.

## Where Databricks is decisively ahead

1. **Operational scale and maturity.** 70+ regions, 160+ Thanos instances,
   self-healing controllers, multi-cloud, multi-tenant, ~1k PromQL/s on the
   largest instance. ASAP has none of this.
2. **Federated multi-region story.** ASAP currently has no design for global
   federation; Databricks routes by tenant attribution at the router layer.
3. **Production cost evidence.** "Millions saved" + "50× cheaper Hydra
   storage" are real-money numbers from a production deployment. ASAP has
   efficiency ratios but no production cost model.
4. **Always-on raw retention.** Hydra stores all 20 billion raw timeseries
   continuously — no reliance on the controller having pre-decided which
   metrics to archive. ASAP's Gorilla-S3 archive is opt-in per metric (see
   "Open question" below).

## Synthesis — how the two could inform each other

- ASAP's **on-device sketches** could plausibly replace Databricks' Telegraf
  aggregation rules. Same intent (cardinality control), but with a formal
  accuracy bound surfaced and an automated controller picking the rules. Saves
  wire bandwidth between the source and the Telegraf gateway — relevant when
  sources are remote, edge, or cost-sensitive.
- ASAP's **Gorilla-S3 archive tier** is roughly equivalent to Hydra's
  Delta-Lake-on-object-storage. Both are "raw on cheap object store, queryable
  on demand". Implementations differ (Gorilla XOR chunks + custom engine vs.
  Delta Parquet + Spark Structured Streaming), but the *architectural shape*
  matches.
- ASAP's **capability-matching** isn't something Databricks discusses — they
  keep Pantheon and Hydra as two visible surfaces. ASAP's unified surface is
  cleaner *if* a single backend can do the routing without becoming a SPOF.
- Databricks' **always-streaming Hydra** is a useful design pattern for ASAP:
  the controller's per-metric archive opt-in is fragile when surprise queries
  land on un-archived metrics. An always-archive default at the gateway
  (regardless of plan) would close that gap.

## Paper framing

For Related Work / Discussion in the paper, the defensible positioning is:

> ASAP automates Databricks-style aggregation rules at a freely-chosen tier
> along the SDK→agent→gateway→backend chain, with bounded-error guarantees
> surfaced in the response, under a single PromQL surface that internally
> routes to a sketch warm tier, an exact Gorilla-XOR archive tier, or a
> raw-fallback tier.

The Databricks blog is a real-world data point that the *problem* (cardinality,
raw retention cost, query freshness) is industrially relevant. The *solution
shape* differs: Databricks chose operational discipline plus tiered storage;
ASAP chooses formal approximation plus workload-driven planning. The paper's
contribution is showing that the latter is feasible and identifying the
operating points where it wins.

## Open question — cold-fallback JSONL deprecation

Discussion 2026-05-06: the JSONL cold-fallback tier is uncompressed and
duplicates the role of the Gorilla-S3 archive tier. Proposal: deprecate the
JSONL path entirely and promote `GorillaQueryEngine` to be the exact tier
serving everything the warm tier cannot answer (Prometheus-/VictoriaMetrics-
style PromQL surface, but over Gorilla-compressed chunks).

This requires:
1. **Always-archive every metric** to Gorilla-S3 at the gateway, regardless
   of plan, so surprise queries on un-archived metrics still have something to
   read. Mirrors Databricks' always-streaming Hydra.
2. **Make `GorillaQueryEngine` PromQL-complete** — current implementation
   handles streaming-additive (`sum / count / avg / min / max / rate / increase`)
   and buffered (`quantile_over_time / topk`); needs `histogram_quantile`,
   label-grouped aggregations, vector matching joins, range-vector functions,
   and the rest of the PromQL surface for parity with Prometheus or
   VictoriaMetrics.
3. **Delete code and docs**: `LocalFsColdStore` impl, `parse_jsonl`, the
   gateway raw-tee exporter, `Design.tex` §"Cold-fallback tier", and the
   `cold-tier scan bytes` line item in the cost model.

Tracked separately from this comparison doc.

## References

- Databricks blog: *10 trillion samples per day: scaling beyond traditional
  monitoring infra at Databricks* —
  https://www.databricks.com/blog/10-trillion-samples-day-scaling-beyond-traditional-monitoring-infra-databricks
- ASAP design — `docs/design-archive-tier.md`,
  `Super_resolution_ingestion_with_sketching_VLDB_or_SIGMOD/Design.tex`
- Thanos — https://thanos.io/
- Telegraf — https://www.influxdata.com/time-series-platform/telegraf/
- Delta Lake — https://delta.io/
