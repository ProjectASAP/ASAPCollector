# Paper outline — across-data-lifecycle sketch observability

Working title candidates:
- *"ASAP: Across Data Lifecycle Sketch-based Observability Pipeline"*
- *"Controller-Planned Edge Sketches for Observability at Scale"*
- *"Sketching from the Edge to the Query: End-to-End Approximate Observability"*

Target: VLDB 2026 / SIGMOD 2026.

## Main idea (one sentence)

The edge OTel collector uses sketches to summarize observability
data at source; raw data flows in parallel to cheap S3 as a
cold backup; an approximate query engine serves queries over
the sketch corpus with fallback to S3 on misses. A **controller
plans this end-to-end pipeline across the data lifecycle**
(collection → transmission → storage → query), driven by query
workload + SLAs.

## Why this is a paper

- **Novel system**. First (that we know of) to push sketches
  all the way to the edge under controller-driven planning,
  with explicit cold-S3 fallback.
- **Across-lifecycle optimization**. Existing work treats
  collection / transmission / storage / query as independent
  layers optimized separately. We demonstrate **joint**
  planning — the controller decides *which sketches run where*
  based on observed query workload + SLAs + resource budgets
  at each stage.
- **Measurable benefits at every layer**:
  - Edge collector: CPU / memory reduction vs. raw-forward
  - Transmission: bandwidth reduction (agent → gateway →
    backend)
  - Storage: smaller hot tier, S3 cold tier at $X / TB
  - Query: lower latency + lower resource usage at backend
- **Formal correctness**: combining sketches across schema
  reconfigure boundaries, deterministic backfill for historical
  accuracy, no query data cliff when the plan evolves.

## Architecture (one figure — key for §3)

```
┌──────────────────────────┐
│  data sources (K8s, VM,  │
│  apps → OTel SDK)        │
└──────────┬───────────────┘
           │
           ▼
┌──────────────────────────────────────────┐
│  Edge OTel collector  (sketchcol)        │
│  ├─ sketch processors (CMS/KLL/HLL/DDS)  │
│  │  per (metric, labels, window)         │
│  ├─→ agent→gateway (sketches, OTLP)      │
│  └─→ agent→S3 (raw, cheap cold)          │
└──────────┬────────────────────┬──────────┘
           │                     │
    ┌──────▼──────┐       ┌──────▼──────┐
    │   Gateway   │       │     S3      │
    │  (optional, │       │ (raw cold)  │
    │  aggregate) │       │             │
    └──────┬──────┘       └──────▲──────┘
           │                     │
           ▼                     │
┌──────────────────────┐         │
│  ASAPQuery-backend   │         │
│  (sketch DB)         │   cold  │
│  ├─ schema timeline  │  miss   │
│  ├─ backfill         │─────────┘
│  └─ query engine     │
│    (PromQL surface)  │
└──────────▲───────────┘
           │
           │ query
           │
┌──────────┴───────────┐
│    user / service    │
└──────────────────────┘

     ┌───────────────────────────┐
     │  Controller  (NOVELTY)    │
     │  observes: query log,     │
     │   SLAs, agent capabilities│
     │  decides: which sketches  │
     │   where, at what params,  │
     │   for which metrics       │
     │  pushes via OpAMP:        │
     │   → agents                │
     │   → gateway               │
     │   → backend               │
     │  replans on:              │
     │   SLA violation,          │
     │   workload drift,         │
     │   query miss rate         │
     └───────────────────────────┘
```

## Contributions (for §1)

1. **Edge-sketching pipeline with formal guarantees**
   (correctness under schema reconfiguration, deterministic
   backfill for historical accuracy, no data cliff across
   boundaries).
2. **Cross-lifecycle cost model** (Pareto over accuracy ×
   latency × $) driven by both static SLAs and observed query
   workload — the first such end-to-end planner for
   observability.
3. **Controller-driven replanning** that reacts online to SLA
   violations, workload drift, and query misses, without
   restart.
4. **Open-source artifact** with reproducible experiments on
   published observability traces (Google cluster 2011/2019,
   Alibaba 2017/2018).

## Sections

1. **Introduction** — motivating example (canonical "query
   misses because someone changed the metric labels 6 months
   ago and the old sketches aren't a match") + contribution list
2. **Background** — observability stack today (Prometheus, OTel,
   TSDBs), sketches 101, why current systems don't do
   edge-to-query joint planning
3. **Architecture** — edge-to-query lifecycle + controller role
   (the big figure above)
4. **Sketch DB** — schema timeline, query dispatch across
   schema boundaries, cold fallback
5. **Controller** — 5-layer planning pipeline (query →
   language AST → sketch algebra → optimizer → physical plan),
   cost model, replanning triggers
6. **Evaluation**
    - 6.1 Setup (workloads, baselines, deployment)
    - 6.2 End-to-end benefits: B0 vs B3 on CPU, memory,
      bandwidth, latency
    - 6.3 Ablation: B1 vs B3 (no sketches), B2 vs B3 (no
      controller)
    - 6.4 Accuracy vs resource tradeoff: ε sweep, Pareto
    - 6.5 Workload evolution: reconfig frequency × benefit
    - 6.6 Failure modes: controller / agent / network partition
7. **Related Work** — sketch DBs (Druid approximate, Pyramid,
   Moment-based), observability (Prometheus, VictoriaMetrics,
   M3, Thanos, Mimir), cross-tier query planning (ClickHouse
   materialized views, Snowflake result cache)
8. **Conclusion + future work** — compaction, multi-sketch-type
   combine, OLAP extension (forward-reference `asap-fusion` as
   follow-up)

## Experiments and the claims they back

| Paper claim | Experiment | Figure |
|---|---|---|
| "N% collector CPU reduction" | B1 vs B3 on Google cluster trace, 24h | stacked bar: CPU per node per baseline |
| "M× bandwidth reduction" | B1 vs B3 bandwidth over agent→backend link, mean + P99 | time-series of bytes/s + summary table |
| "Query P99 latency: PromQL native vs sketch-answered" | B0 vs B3 on realistic query replay | latency CDF |
| "ε accuracy at M× resource savings" | Accuracy sweep at fixed workload; Pareto curve | accuracy vs cost Pareto scatter |
| "Controller responds to workload drift in T seconds" | Replan latency after injected drift | time-series with event markers |
| "Cold S3 fallback adds <K ms P99" | Warm-vs-cold hit latency histograms | latency CDF with hot/cold split |
| "Scales linearly in number of agents" | B3 throughput at N ∈ {1, 10, 100} agents | throughput curve |
| "Resilient to controller failure" | Kill controller mid-workload; queries continue | time-series showing continuity |

## Novelty story — what to emphasize

The controller is the key originality. Emphasize:

1. **End-to-end scope**: controller plans from **source
   collection** through **transmission encoding** through
   **storage layout** through **query dispatch**. Not just
   storage. Not just query. The whole lifecycle.
2. **Cost model crosses layers**: the decision "should this
   metric become a CMS at the edge" depends on edge CPU
   budget AND on-wire bandwidth AND backend memory AND
   expected query ε. No existing observability system
   optimizes jointly over all four.
3. **Online replanning with historical correctness**: when
   the plan changes (new query pattern, SLA drift), existing
   data doesn't become inaccessible. Backfill gives us a
   guarantee that historical windows can be retroactively
   materialized into the new sketch shape, and the schema
   timeline lets queries span pre-change and post-change
   transparently.
4. **First-class cold tier**: raw data lives in S3 cheaply.
   Hot queries hit sketches; cold or exact queries fall back
   to S3. This is the *reason* it's OK to be approximate at
   the edge — exact is always recoverable if needed.

## Explicit non-goals (for §1 last paragraph)

- **asap-fusion / tabular data model**. Approximate OLAP over
  relational data is a different story; follow-up paper.
- **Inter-window compaction**. sketchDB v1 does not compact;
  future work.
- **Cross-sketch-type combine** (e.g. KLL(k=200) + KLL(k=100)
  merge). Operational practice: new agg_id for param / type
  changes. The schema timeline makes this clean without a
  heterogeneous combiner.
- **Multi-query shared-scan / operator fusion**. Different
  optimization target; possible future paper.

## ASAPQuery-backend: paper-relevant vs. out-of-scope parts

Ed: since the paper is about the whole lifecycle, not just the
backend, we only feature the backend parts that directly
support the story.

**In scope**:
- `SimpleMapStore` — sketch storage, LSM-like parts layout
- Schema registry + lifecycle (Active / Retired / Expired)
- Schema timeline + cross-boundary query dispatch
- Backfill — for the historical-accuracy claim
- Query engine's PromQL surface + barrier counter (for
  observability story)
- Cold fallback to S3 (TODO item)

**Not in scope / mention briefly or omit**:
- SQL query surface — we don't evaluate SQL workloads
- ElasticDSL — same
- DataFusion integration paths in `precompute_engine/` that
  don't surface in the core sketchDB story
- Individual benchmark bins (`bench_precompute_sketch`,
  `test_e2e_precompute`) — supporting infra, not in the paper

## Submission timeline

| Milestone | Target |
|---|---|
| sketchDB cold-fallback + accuracy library done | +1 month |
| Multi-agent stack + instrumentation ready | +1.5 months |
| All baselines + workloads running | +2 months |
| Main experiments done, figures drafted | +2.5 months |
| Full draft v1 | +3 months |
| Revision + reproducibility archive | +3.5 months |
| Submit | +4 months |

## Open writing questions

1. **How much space to spend on the 5-layer model?** It's the
   core technical framing. Probably 1-1.5 pages in §3 + §5.
2. **How to present the cost model?** Brief formal summary
   (§5) + full spec in appendix.
3. **How to present backfill?** A one-page subsection in §4
   with the key correctness claim and the time-disjoint
   invariant as a theorem box.
4. **Replated work depth?** Half a page is enough for VLDB;
   distinguish primarily on "edge + lifecycle-jointly-planned"
   axis — existing work is either edge-only or
   backend-only.
