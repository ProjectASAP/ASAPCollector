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
- **Measurable benefits — five evaluation dimensions**. The
  paper stands on these five empirical claims, demonstrated
  end-to-end (system architecturally working is necessary but
  not sufficient — the data must back each claim):
  1. **Reduced transmission bandwidth.** Sketch envelopes on
     the agent → backend wire are smaller than raw samples,
     vs. raw and vs. compression baselines (b0a / b0b / b1 /
     b5).
     - *Evidence:* `deploy/scripts/run_e2e_sweep.sh` (P7) →
       per-cell `bytes_in / bytes_out` columns →
       `deploy/scripts/e2e_plots.py` (P9) bandwidth-vs-N plot.
       Single-host pre-compare:
       `otel_collector_benchmark/cardinality_crossover/`
       (sketch-bytes vs raw-bytes across `N ∈ {100…5M}`).
  2. **Low edge collector CPU overhead at runtime.** Sketch
     processors don't blow the agent's CPU budget vs.
     raw-forwarding.
     - *Evidence:* P7 sweep producer-side `cpu_pct` column;
       `otel_collector_benchmark/bench_2node_sim.sh` for
       per-node CPU; SDK label-axis profile (paper blocker
       #2 of `PROGRESS.md`).
  3. **Low edge collector memory overhead at runtime.**
     Sketch-processor RSS stays bounded under load and over
     long soaks (no leaks).
     - *Evidence:* P7 sweep `rss` column;
       `otel_collector_benchmark/bench_soak.sh` (long-running
       steady-state with minute-resolution RSS / heap /
       fd-count and slope-based leak verdict).
  4. **Backend query accuracy.** Every PromQL answer falls
     inside the sketch's theoretical accuracy envelope
     (ε / δ / kind), and is quantitatively close to ground
     truth. The accuracy envelope is already surfaced in the
     `infos` of every response.
     - *Evidence:* `deploy/fake-exporter/raw_tee.go` (P4)
       writes ground truth to MinIO/S3 raw JSONL;
       `deploy/scripts/accuracy_reduce.py` (P8) joins query
       answers vs. truth and computes per-row relative
       error / top-K recall; bound-derivation crib in
       `ASAPQuery-backend/TODO.md` "Accuracy-profile library
       per sketch type" + `sketch-bench/docs/DESIGN.md`.
  5. **Fast query computation / short query latency.**
     Backend p50 / p99 query latency is production-usable.
     Headline target: ≤2× warm-hot for cold-fallback;
     warm-tier is the headline number.
     - *Evidence:* `deploy/scripts/metricsql_replay.py` (P5)
       captures p50 / p99 per query at fixed QPS; P9
       `query_latency_cdf.png` plot;
       `deploy/scripts/plan_transition.py` (P6)
       `t_query_in / t_first_hit / t_steady` against the
       controller's plan-id stream.

  **Combined headline claim** (the Pareto): total resource
  usage (edge + backend) at equivalent query coverage is
  lower than the raw-sample baseline. Materialized as P9's
  `pareto_acc_vs_thru.png` (accuracy vs. throughput /
  bandwidth across the full sweep matrix). This is the figure
  the paper's contribution rests on.

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
│  Edge OTel collector  (asap-otel)        │
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
    - 6.2 SDK-side three-axis ablation (see below + the
      authoritative [`sdk-cost-evaluation.md`](sdk-cost-evaluation.md)).
      Sub-sweeps 6.2a / 6.2b / 6.2c / 6.2d decompose the
      bandwidth-reduction claim into its three independent
      factors: time window `W`, label projection `L`,
      encoding `agg_type`.
    - 6.3 Cross-layer placement: same `agg_type` at SDK
      vs agent vs backend, CPU / mem / bw tradeoff
    - 6.4 Accuracy vs resource Pareto: ε sweep at fixed
      `(W, L, agg_type)` operating point, Pareto curve
    - 6.5 Planner quality: given query sets `Q_1,…,Q_k`,
      does the controller's `(W, L, agg_type)` output match
      hand-tuned ground truth? Independent of SDK emit cost.
    - 6.6 Workload evolution: online replan latency after
      injected drift; controller-in-loop end-to-end
    - 6.7 Failure modes: controller / agent / network partition
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
| **"Bw reduction = time-factor × label-factor × encoding-factor"** | **6.2a / 6.2b / 6.2c (SDK-side three-axis ablation, each axis swept independently)** | **3 curves, each a mean + P99 band** |
| "End-to-end bw reduction on realistic queries" | **6.2d** — best `(W, L, agg_type)` per metric under `Q` vs `raw-buffer` at full label set + `W=15s` | Single stacked-bar: product of three factors |
| "Query P99 latency: PromQL native vs sketch-answered" | B0 vs B3 on realistic query replay | latency CDF |
| "ε accuracy at M× resource savings" | Accuracy sweep at fixed `(W, L, agg_type)` operating point | accuracy vs cost Pareto scatter |
| "Cross-layer placement doesn't matter for correctness, but CPU/mem tradeoff differs" | **6.3** — same `agg_type` at SDK vs agent vs backend | stacked CPU/mem per layer |
| **"Planner's `(W, L, agg_type)` choice matches hand-tuned ideal within X%"** | **6.5** — offline planner vs ground truth over synthetic `Q` sets | match-rate curve |
| "Controller responds to workload drift in T seconds" | Replan latency after injected drift | time-series with event markers |
| "Cold S3 fallback adds <K ms P99" | Warm-vs-cold hit latency histograms | latency CDF with hot/cold split |
| "Scales linearly in number of agents" | B3 at N ∈ {1, 10, 100}, under fixed `(W, L, agg_type)` | bw / CPU per-agent stability curve |
| "Resilient to controller failure" | Kill controller mid-workload; queries continue | time-series showing continuity |

The §6.2 sub-sweeps are defined in detail in
[`sdk-cost-evaluation.md`](sdk-cost-evaluation.md).
Reviewer-facing: each of the three factors is an independently
measurable quantity, so a skeptical reader can drop one factor
(e.g., "I don't buy the `L` factor because `by (...)` queries
aren't common in your workload") and still see what the
remaining two buy.

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
