# Controller Optimization Problem

## Overview

The controller optimization problem takes as input a set of **PromQL or SQL queries** that will be executed by [ASAPQuery](https://github.com/ProjectASAP/ASAPQuery) and produces an optimal **collection and computation plan** across the full data lifecycle. At each stage the controller decides what to compute, which sketch or aggregation to apply, whether to compress or preserve raw samples, and where to store results. The objective is to minimize resource consumption (memory, CPU, bandwidth, storage cost, query latency) subject to accuracy SLAs.

### Data Plane — Stages and Data Lifecycle

```
  ──────────────────────────────────────── write path ───────────────────────────────────────────►
  ◄─────────────────────────────────────── query path ───────────────────────────────────────────

  ┌─────────────┐  raw/pre-agg  ┌──────────────────────┐  sketch/raw  ┌──────────────────────────┐
  │  App + SDK  │──────────────►│  Agent OTel Collector │─────────────►│  Backend OTel Collector  │
  │             │               │                      │               │                          │
  │ optional    │               │ • sketch insert       │               │ • merge sketches          │
  │ pre-agg     │               │ • label pruning       │               │ • window aggregation      │
  └─────────────┘               │ • Gorilla/Serf compr  │               │ • Gorilla/Serf compr      │
                                │ • delta encoding      │               └────────────┬─────────────┘
                                └──────────────────────┘                            │ sketches / raw
                                                                                    ▼
                                                                    ┌───────────────────────────────┐
                                                                    │          ASAPQuery            │
                                                                    │                               │
                                                                    │  Precompute Engine ──────────►│ cached results
                                                                    │  Query Engine      ◄──────────│ user queries
                                                                    └──────────┬────────┬───────────┘
                                                                               │        │
                                                                        hot    │        │ cold
                                                                               ▼        ▼
                                                                    ┌──────────────┐  ┌──────────┐
                                                                    │  ClickHouse  │  │   S3     │
                                                                    │  / TSDB      │  │ (archive)│
                                                                    └──────────────┘  └──────────┘
```

### Control Plane — Overview

The controller ingests the query registry and drives configuration across every data-plane stage. It is decomposed into nine subproblems (SP-1 through SP-9); SP-8 closes the feedback loop back into SP-5/SP-6 using live telemetry, and SP-9 extends SP-3 with AST-aware hierarchical stage assignment.

```
  PromQL / SQL queries
  (ASAPQuery Query Registry)
             │
             ▼
  ┌─────────────────────────────────────────────────────────────────────────┐
  │                      CONTROL PLANE (Controller)                         │
  │                                                                         │
  │  SP-1  Workload Extractor      parse queries → QueryWorkload[]          │
  │    │                                                                    │
  │  SP-2  Sketch Enumerator       agg type → valid (sketch, params)[]      │
  │    │                                                                    │
  │  SP-3  Stage Assignment        assign sketch/agg to SDK|Agent|          │
  │    │                           Backend|Precompute|DB                    │
  │    │                                                                    │
  │  SP-9  AST-Aware Stage Split   split SketchExpr tree across stages:     │
  │    │                           leaf nodes → OTel Collector,             │
  │    │                           upper nodes → Precompute Engine          │
  │    │                                                                    │
  │  SP-4  Storage & Compression   raw preserve? Gorilla/Serf/delta?        │
  │    │                           TSDB hot vs S3 cold? precompute?         │
  │    │                                                                    │
  │  SP-5  Cost Model  ◄───────────────────────────────────────────┐        │
  │    │   offline benchmarks · heuristic analytic · online EMA    │        │
  │    │                                                           │        │
  │  SP-6  Optimizer               Pareto / heuristic / learned    │        │
  │    │                                                           │        │
  │  Plan Store                    versioned · diffable            │        │
  │    │                                                           │        │
  │  SP-7  Config Delivery         OpAMP · HTTP · storage API      │        │
  │                                                                │        │
  │  SP-8  Feedback Collector      scrape actuals → update SP-5 ───┘        │
  │                                trigger re-plan on SLA drift             │
  └────────────┬───────────────────────────┬────────────────────────────────┘
               │ OpAMP push                │ HTTP / storage API
               ▼                          ▼
       SDK + OTel Collectors        ASAPQuery Precompute Engine
       (Agent + Backend)            ClickHouse / TSDB / S3
```

Detailed component diagrams for each plane are in [Control Plane Architecture](#control-plane-architecture) and [Data Plane Architecture](#data-plane-architecture) below.

---

## Problem Decomposition

The optimization problem decouples into eight subproblems (SP-1 through SP-8) that are solved in sequence, with SP-8 feeding back into SP-6/SP-1 as an online feedback loop.

---

### SP-1: Query Workload Extraction

**Input:** A set of PromQL or SQL query strings registered in the ASAPQuery query registry.

**Output:** For each query, a `QueryWorkload` struct that characterizes what is required from the data pipeline:

```
QueryWorkload {
    metric_name:      string
    aggregation_types: [Quantile | Cardinality | Frequency | Sum | Count | ...]
    label_filters:    map[string]string       // e.g. service="web"
    group_by_dims:    []string                // e.g. ["host.name"]
    time_window:      Duration                // e.g. 5m
    query_frequency:  Duration                // how often this query runs
    accuracy_sla:     float64                 // e.g. 0.01 = 1% relative error
    latency_sla:      Duration                // max acceptable staleness
    exact_required:   bool                    // true if sketch error is unacceptable
}
```

**Task:** Parse PromQL/SQL syntax, identify the aggregation functions in use, determine which label dimensions must be preserved to answer the query, and estimate query frequency from the workload registry or execution history.

---

### SP-2: Sketch Candidate Enumeration

**Input:** A `QueryWorkload` from SP-1.

**Output:** The set of valid `(sketch_type, sketch_params)` pairs that can answer the query within its accuracy SLA.

Each aggregation type maps to a candidate sketch family:

| Aggregation type | Candidate sketch types |
|---|---|
| Quantile (p50, p99, …) | DDSketch, KLL |
| Count distinct | HLL |
| Frequency / heavy hitters | CountMinSketch, CountSketch |
| Exact sum / count | No sketch — aggregated counter only |
| Point query / anomaly detection | Raw samples required — no sketch |

For each candidate sketch type the enumerator produces one or more parameterizations that meet the accuracy SLA (e.g., DDSketch `relative_accuracy=0.01`, HLL `precision=14`).

If `exact_required=true` the only candidate is raw samples, and SP-2–SP-4 collapse to a raw-preservation decision.

**Combinatorial note:** A single query may require multiple aggregation types over the same metric (e.g., p99 and count distinct simultaneously). In that case SP-2 produces a set of parallel sketch candidates, one per aggregation type.

---

### SP-3: Stage Assignment

**Input:** The sketch candidates from SP-2 and the data lifecycle stages available.

**Output:** For each sketch or aggregation operation, an assignment to one or more stages where it will be computed or merged.

**Stages:**

| Stage | Compute location | Typical role |
|---|---|---|
| SDK | In-process instrumentation | Pre-aggregation at highest cardinality |
| Agent Collector | Edge OTel collector per host/pod | Sketch or raw collection with label pruning |
| Backend Collector | Centralized OTel collector | Merge sketches across agents; apply backend-level windows |
| ASAPQuery Precompute Engine | Query-side materialization | Run query against accumulated sketches; cache result |
| DB-side query | ClickHouse / TSDB query engine | Exact aggregation at query time over stored raw or compressed data |

**Key decisions per sketch or operation:**

- Which is the earliest stage at which the aggregation is safe to apply? (Determined by which label dimensions survive to each stage.)
- Should the computation happen at one stage only, or should partial results be computed at the edge and merged at the backend (hierarchical aggregation)?
- Is a precomputed materialization in ASAPQuery worthwhile given the query frequency and latency SLA?

**Stage assignment rules:**

```
if exact_required:
    → SDK and Agent: forward raw samples
    → DB: query-time exact aggregation
    → Precompute: only if query_frequency is high and latency_sla allows caching

if sketch_feasible:
    if query needs per-host breakdown:
        → Agent: sketch with aggregate_by=[host.name, ...]
        → Backend: merge sketches, apply query window
    if query needs only global aggregation:
        → Agent: sketch with aggregate_by=[]  (one global sketch per agent)
        → Backend: merge all agent sketches
    if latency_sla >= time_window:
        → Precompute: materialize query result at the backend, cache in ASAPQuery
```

---

### SP-4: Storage and Compression Decisions

**Input:** The stage assignment from SP-3 and the storage infrastructure available (TSDB, ClickHouse, S3).

**Output:** For each data type (raw samples, sketches, precomputed results), a storage and compression policy.

**Decisions:**

**Raw sample preservation:**
- Preserve raw samples alongside sketches when: exact point queries are needed, anomaly detection requires per-sample precision, or compliance requires full fidelity archival.
- Drop raw samples when: the accuracy SLA is fully met by sketches and no exact point queries exist.

**Raw sample compression (transport and storage):**
- `Gorilla` (lossless XOR timestamp+value compression): always applicable, reduces raw bandwidth 30–70%.
- `Serf` (lossy XOR with configurable error bound): applicable when slight compression error is tolerable; reduces further.
- No compression: only when data volume is very low and CPU is constrained.

**Delta transmission for sketches:**
- Send full sketch per flush vs. delta (only changed cells) — governed by the fill-rate cost model (see SP-5).

**Storage tiering:**
- `TSDB / ClickHouse (hot)`: recent data requiring low query latency; incurs compute and storage cost at query time.
- `S3 (cold)`: long-retention archival; low storage cost but high query latency (scan cost). Factor S3 query cost into precompute decisions.
- Precomputed results in ASAPQuery: zero query latency for cached results; cost is materialization CPU at precompute time.

---

### SP-5: Cost Model

**Input:** A fully specified candidate plan (sketch type + params, stage assignments, storage/compression decisions) and a `QueryWorkload`.

**Output:** Cost vector `(memory, cpu, bandwidth, insert_latency, query_latency, throughput, storage_cost, accuracy_loss)` for the candidate plan.

The cost model has three modes, usable independently or in combination:

**Offline profiling:**
- Benchmarked sketch insert/query CPU and memory per sketch type and parameter set.
- Benchmarked compression ratios at fill-rate anchor points (1%, 5%, 20%).
- DAG profiling: cost of chained sketch operations (edge sketch → merge → precompute query).
- Used to seed cost estimates before any production data is available.

**Heuristic analytic model:**
- Fill-rate estimation from traffic distribution type (Zipf, Uniform, Bursty).
- Compression ratio interpolated from offline anchor points.
- Bandwidth: `B(sketch) = sketch_bytes_per_flush × flush_rate_hz × series_count`.
- Memory: `M(sketch) = snapshot_bytes × partitions_count`.
- Query latency: function of whether precompute cache is hit, TSDB scan size, or S3 scan cost.

**Online profiling (feedback from SP-8):**
- Actual fill rates, sketch sizes, CPU utilization observed from running agents.
- Actual query latencies observed from ASAPQuery execution logs.
- Actual TSDB / S3 scan costs from storage billing metrics.
- EMA-adjusted model constants updated per deployment environment.

**Key cost quantities:**

| Dimension | Definition |
|---|---|
| Memory | Sketch snapshot size in-memory per stage × partition count |
| CPU | Insert cost µs/sample + query computation cost per flush |
| Bandwidth | Bytes/s transmitted between each adjacent stage pair |
| Insert latency | Time from sample arrival to sketch update |
| Query latency | Time from query arrival to result (cache hit vs. miss vs. DB scan) |
| Throughput | Max samples/s sustainable within CPU budget |
| Storage cost | TSDB retention cost + S3 scan cost per query |
| Accuracy loss | Sketch relative error (bounded) + optional lossy compression error |

---

### SP-6: Optimization / Plan Selection

**Input:** The set of candidate plans from SP-2–SP-4 with cost vectors from SP-5, plus the `QueryWorkload` constraints.

**Output:** The selected `CollectionPlan` — a concrete assignment of sketch types, stage allocations, compression policies, and storage configurations for each metric and query workload.

**Constraint satisfaction (hard constraints):**
```
accuracy_loss   ≤ accuracy_sla
memory          ≤ memory_budget_per_stage
query_latency   ≤ latency_sla
```

Plans violating any hard constraint are pruned before optimization.

**Objective (soft, multi-dimensional):**
```
minimize  w_bw × bandwidth
        + w_cpu × cpu
        + w_mem × memory
        + w_store × storage_cost
        + w_lat × query_latency
```

Weights `w_*` are operator-configured or derived from the current bottleneck (bandwidth-constrained vs. CPU-constrained vs. cost-constrained deployment).

**Solution strategies (in order of increasing complexity):**

1. **Rule-based heuristic (Phase 1):** deterministic priority ordering — apply sketch if sample rate ≥ 10 Hz, apply delta if compression ratio ≥ 2×, assign to earliest feasible stage.
2. **Pareto-optimal selection (Phase 2):** enumerate the Pareto frontier over `(bandwidth, cpu, storage_cost)` using the cost model; let operator or policy select the operating point.
3. **Offline-trained model (Phase 3):** gradient-boosted regression trained on `(workload, plan) → cost_vector` triples collected from production; replaces heuristic cost model internals.

**Fallback chain:**
```
UseDelta(sketch) → UseFullSketch → UseRaw → RawWithGorilla
```
Each level is tried if the next more-aggressive option violates a constraint.

---

### SP-7: Plan Delivery

**Input:** The selected `CollectionPlan` from SP-6.

**Output:** Live configuration pushed to each data-plane component.

**Delivery mechanisms:**

| Target component | Protocol | Config format |
|---|---|---|
| SDK | SDK Config API or env var | JSON / gRPC |
| Agent OTel Collector | OpAMP (WebSocket, persistent) | OTel YAML |
| Backend OTel Collector | OpAMP | OTel YAML |
| ASAPQuery Precompute Engine | HTTP REST | Precompute job spec (JSON) |
| ClickHouse / TSDB retention | Storage config API | Retention and tiering policy |

The Plan Store maintains a versioned, diffable log of all applied plans and supports rollback if SP-8 detects regression after a plan change.

---

### SP-8: Feedback and Re-planning

**Input:** Live telemetry from data-plane components.

**Output:** Updated cost model constants (fed into SP-5) and re-plan trigger when SLA is violated or resources are wasted.

**Feedback sources:**

| Source | Signal collected |
|---|---|
| Agent Collector `/metrics` | Actual sketch fill rate, CPU usage, flush size, insert throughput |
| Backend Collector `/metrics` | Merge throughput, merged sketch size, bandwidth consumed |
| ASAPQuery execution logs | Actual query latency, cache hit/miss rate, precompute job timing |
| ClickHouse / TSDB metrics | Query scan cost, storage utilization |
| S3 billing / access logs | Scan frequency and byte cost per query |

**Re-plan triggers:**
- Actual accuracy < accuracy SLA (under-serving).
- Actual bandwidth or CPU > budget × 1.2 (over-provisioned).
- Actual query latency > latency SLA.
- Plan `ValidUntil` expiry (time-based re-evaluation).
- Significant shift in query workload (new queries registered, existing queries deleted).

---

### SP-9: AST-Aware Hierarchical Stage Assignment

**Input:** The optimized `SketchExpr` tree produced by SP-1 (query parser + algebraic optimizer) and the stage assignment from SP-3.

**Output:** A per-node stage assignment that splits the `SketchExpr` tree across pipeline stages — leaf-level operations on the OTel Collector, upper-level operations on the ASAPQuery Precompute Engine.

**Motivation:** SP-3 currently assigns a single flat sketch type to the whole pipeline. Every stage receives the same sketch configuration. For compositional queries (e.g. `TopK(Partition(Window(Agg(Source))))`) this wastes precompute capacity: the collector re-does work the precompute engine could absorb, and the precompute engine receives fully-materialized sketches when it only needs to evaluate the top of the tree.

The `SketchExpr` IR already exists (built by `query_parser/` and optimized by `sketch_rules.rs`) but is discarded after parsing — it never reaches the planner. SP-9 routes it there.

**Node-to-stage mapping:**

| `SketchExpr` node | Natural stage | Rationale |
|---|---|---|
| `Source` | SDK / Agent OTel Collector | Raw sample ingestion |
| `Filter` | Agent OTel Collector | Label-based predicate push-down at edge |
| `Window` | Agent OTel Collector | Time-windowed sketch accumulation per flush |
| `Agg` (leaf sketch: DDSketch, HLL, CountSketch) | Agent OTel Collector | Sketch insertion at earliest safe stage |
| `Partition` | Backend OTel Collector | Group-by across agents after merge |
| `Merge` | Backend OTel Collector | Sketch linearity — merge N agent sketches |
| `Dedup` | Backend OTel Collector | HLL deduplication absorbed at merge stage |
| `TopK` | ASAPQuery Precompute Engine | Heavy-hitter extraction over merged sketches |
| `ExactAgg` | DB-side query (ClickHouse) | Exact computation required — no sketch |

**Algorithm (tree split):**

```
fn assign_stages(expr: SketchExpr) -> StagedPlan:
    walk expr bottom-up:
        Source, Filter, Window, Agg → assign to Agent OTel Collector
        Partition, Merge, Dedup     → assign to Backend OTel Collector
        TopK                        → assign to Precompute Engine
        ExactAgg                    → assign to DB-side query
    emit:
        agent_config    ← nodes assigned to Agent stage
        backend_config  ← nodes assigned to Backend stage
        precompute_jobs ← nodes assigned to Precompute stage
        db_query        ← nodes assigned to DB stage
```

**What changes from SP-3:**

- SP-3 produces a single `(sketch_type, params)` shared across all stages.
- SP-9 produces a per-stage sub-tree: the agent receives a `Window + Agg` sub-plan, the backend receives a `Merge + Partition` sub-plan, and the precompute engine receives a `TopK` or upper-aggregation query over the merged sketches already in the backend.
- The `PrecomputeJob.query_expr` becomes the upper sub-tree serialized as a PromQL expression, rather than a hardcoded `quantile_over_time(0.99, ...)` template.

**Current implementation gap:**

The `SketchExpr` tree is built and optimized in `query_parser/` but is **flattened to `Vec<AggType>` in `analyzer.rs`** before reaching the planner. The planner (`rules.rs`) never sees the tree structure. To implement SP-9:

1. Thread `SketchExpr` (or a normalized form) through `QueryWorkload` alongside `aggregations`.
2. Add a `split_expr_by_stage()` function in `planner/` that walks the tree and emits per-stage sub-plans.
3. Replace the hardcoded `build_query_expr()` template in `config/precompute.rs` with serialization of the upper sub-tree.
4. Extend `CollectionPlan` to carry a per-stage sub-expression so `config/agent.rs` and `config/backend.rs` can emit the right processor chain.

**Interaction with SP-3 and SP-6:**

SP-9 is a refinement of SP-3, not a replacement. The SP-3 flat assignment remains as the fallback when the query maps to a single aggregation type with no compositional structure. SP-6 (optimizer) can score both plans (flat vs. AST-split) using SP-5 costs and select the cheaper option.

---

## Data Plane Architecture

The diagram below shows the **internal structure of each data-plane stage** — what runs inside each component, what data flows between them, and how storage is tiered. The control plane (above) configures every decision point shown here but is not depicted.

```
  ┌──────────────────────────────────────────────────────────────────────────────────────────┐
  │  Instrumented Application                                                                │
  │                                                                                          │
  │  ┌──────────────────────────────────────────────────────────┐                           │
  │  │  SDK (optional pre-aggregation)                          │                           │
  │  │  • counter / histogram accumulation before export        │                           │
  │  │  • reduces series cardinality at the source              │                           │
  │  └──────────────────────────┬───────────────────────────────┘                           │
  └─────────────────────────────┼────────────────────────────────────────────────────────────┘
                                │  OTLP (raw or pre-aggregated)
                                ▼
  ┌──────────────────────────────────────────────────────────────────────────────────────────┐
  │  Agent OTel Collector  (one per host / pod)                                              │
  │                                                                                          │
  │  ┌─────────────────────────────────────────────────────────────────────────────────┐    │
  │  │  Sketch Processor                                                               │    │
  │  │  • sketch type: DDSketch | KLL | HLL | CountMinSketch | CountSketch             │    │
  │  │  • mode: window (fixed duration) | batch (per-flush)                            │    │
  │  │  • aggregate_by: label dimensions to preserve                                   │    │
  │  └─────────────────────────────────────────────────────────────────────────────────┘    │
  │  ┌─────────────────────────────────────────────────────────────────────────────────┐    │
  │  │  Compression Processor                                                          │    │
  │  │  • Gorilla (lossless XOR) | Serf (lossy XOR) | none                            │    │
  │  │  • delta encoding: send only changed sketch cells                               │    │
  │  └─────────────────────────────────────────────────────────────────────────────────┘    │
  └──────────────────────────────────────┬───────────────────────────────────────────────────┘
                                         │  sketch payload (full or delta) or raw OTLP
                                         ▼
  ┌──────────────────────────────────────────────────────────────────────────────────────────┐
  │  Backend OTel Collector  (centralized)                                                   │
  │                                                                                          │
  │  ┌─────────────────────────────────────────────────────────────────────────────────┐    │
  │  │  Sketch Merge Processor                                                         │    │
  │  │  • merge sketches from N agents (sketches are mergeable by construction)        │    │
  │  │  • apply backend-level time window if agents use batch mode                     │    │
  │  └─────────────────────────────────────────────────────────────────────────────────┘    │
  │  ┌─────────────────────────────────────────────────────────────────────────────────┐    │
  │  │  Compression Processor                                                          │    │
  │  │  • Gorilla / Serf on merged sketch output before forwarding                     │    │
  │  └─────────────────────────────────────────────────────────────────────────────────┘    │
  └──────────────────────────────────────┬───────────────────────────────────────────────────┘
                                         │  merged sketches
                                         ▼
  ┌──────────────────────────────────────────────────────────────────────────────────────────┐
  │  ASAPQuery                                                                               │
  │                                                                                          │
  │  ┌──────────────────────────────────┐    ┌──────────────────────────────────────────┐   │
  │  │  Precompute Engine               │    │  Query Engine                            │   │
  │  │                                  │    │                                          │   │
  │  │  • pull sketches on arrival      │    │  • evaluate PromQL / SQL at query time   │   │
  │  │  • run registered queries        │    │  • serve from precompute cache (fast)    │   │
  │  │  • cache scalar results          │───►│  • fall back to DB scan (slow)           │◄──┼── user queries
  │  │  • evict on TTL or re-plan       │    │  • merge raw + sketch results            │   │
  │  └──────────────────┬───────────────┘    └──────────────────────────────────────────┘   │
  └─────────────────────┼────────────────────────────────────────────────────────────────────┘
                        │ write to storage
          ┌─────────────┴─────────────┐
          │ hot path                  │ cold path
          ▼                          ▼
  ┌──────────────────┐      ┌──────────────────────────────────┐
  │  ClickHouse      │      │  S3 (object store)               │
  │  / TSDB          │      │                                  │
  │                  │      │  • long-retention archival        │
  │  • recent data   │      │  • raw samples or sketch blobs    │
  │  • low-latency   │      │  • high scan cost at query time   │
  │    point queries │      │  • tiered from TSDB on TTL        │
  │  • sketch blobs  │      └──────────────────────────────────┘
  │  • raw samples   │
  └──────────────────┘
```

---

## Control Plane Architecture

The diagram below shows the **control plane components and their relationships**. Data-plane components (collectors, DB, S3) appear only as targets that receive configuration — their internal data paths are not shown.

```
  PromQL / SQL Queries
  (ASAPQuery Query Registry)
          │
          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          CONTROL PLANE                                  │
│                                                                         │
│  ┌──────────────────────────────────────────┐                           │
│  │  SP-1: Query Workload Extractor          │                           │
│  │                                          │                           │
│  │  parse PromQL / SQL                      │                           │
│  │  → metric, agg types, dims,              │                           │
│  │    windows, SLAs, query frequency        │                           │
│  └──────────────────────┬───────────────────┘                           │
│                         │  QueryWorkload[]                              │
│                         ▼                                               │
│  ┌──────────────────────────────────────────┐                           │
│  │  SP-2: Sketch Space Enumerator           │                           │
│  │                                          │                           │
│  │  agg type → valid (sketch, params) pairs │                           │
│  │  exact_required → raw-only candidate     │                           │
│  └──────────────────────┬───────────────────┘                           │
│                         │  SketchCandidates[]                          │
│                         ▼                                               │
│  ┌──────────────────────────────────────────┐                           │
│  │  SP-3: Stage Assignment Solver           │                           │
│  │                                          │                           │
│  │  assigns each sketch / aggregation to:   │                           │
│  │  SDK | Agent | Backend | Precompute | DB │                           │
│  │                                          │                           │
│  │  decisions: earliest safe stage,         │                           │
│  │  hierarchical merge, precompute benefit  │                           │
│  └──────────────────────┬───────────────────┘                           │
│                         │  StagedPlan[]                                │
│                         ▼                                               │
│  ┌──────────────────────────────────────────┐                           │
│  │  SP-4: Storage & Compression Decider     │                           │
│  │                                          │                           │
│  │  • preserve raw samples?                 │                           │
│  │  • compression: Gorilla / Serf / none    │                           │
│  │  • delta sketch transmission?            │                           │
│  │  • TSDB (hot) vs S3 (cold) tiering       │                           │
│  │  • precompute materialization policy     │                           │
│  └──────────────────────┬───────────────────┘                           │
│                         │  CandidatePlan[]                             │
│                         ▼                                               │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │  SP-5: Cost Model                                                │  │
│  │                                                                  │  │
│  │  ┌────────────────┐  ┌────────────────┐  ┌───────────────────┐  │  │
│  │  │ Offline        │  │ Heuristic      │  │ Online (feedback) │  │  │
│  │  │ Profiling DB   │  │ Analytic Model │  │ from SP-8         │  │  │
│  │  │ (benchmarks,   │  │ (fill-rate,    │  │ (actual fill rate,│  │  │
│  │  │  DAG costs)    │  │  compression   │  │  CPU, latency,    │  │  │
│  │  │                │  │  ratios)       │  │  storage cost)    │  │  │
│  │  └────────┬───────┘  └───────┬────────┘  └─────────┬─────────┘  │  │
│  │           └─────────────────►│◄────────────────────┘            │  │
│  │                              │                                   │  │
│  │  per CandidatePlan outputs:  ▼                                   │  │
│  │  memory · CPU · bandwidth · insert/query latency ·               │  │
│  │  throughput · storage cost · accuracy loss                       │  │
│  └──────────────────────┬───────────────────────────────────────────┘  │
│                         │  (plan, cost_vector)[]                      │
│                         ▼                                               │
│  ┌──────────────────────────────────────────┐                           │
│  │  SP-6: Optimizer / Plan Selector         │                           │
│  │                                          │                           │
│  │  Phase 1: rule-based heuristic           │                           │
│  │  Phase 2: Pareto frontier enumeration    │                           │
│  │  Phase 3: learned cost model             │                           │
│  │                                          │                           │
│  │  prune constraint violations →           │                           │
│  │  select optimal CollectionPlan           │                           │
│  └──────────────────────┬───────────────────┘                           │
│                         │  CollectionPlan                              │
│                         ▼                                               │
│  ┌──────────────────────────────────────────┐                           │
│  │  Plan Store                              │                           │
│  │  (versioned · diffable · rollbackable)   │                           │
│  └──────────────────────┬───────────────────┘                           │
│                         │                                               │
│                         ▼                                               │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │  SP-7: Config Generator & Delivery                               │  │
│  │                                                                  │  │
│  │  ┌──────────────────────┐  ┌──────────────────────────────────┐  │  │
│  │  │  OTel YAML Generator │  │  ASAPQuery Precompute API Client │  │  │
│  │  │  + OpAMP Server      │  │  (register / deregister jobs)    │  │  │
│  │  └──────────┬───────────┘  └──────────────┬───────────────────┘  │  │
│  │             │                             │                       │  │
│  │  ┌──────────▼────────────┐  ┌─────────────▼──────────────────┐  │  │
│  │  │  SDK Config API       │  │  Storage Config API             │  │  │
│  │  │  (pre-aggregation)    │  │  (TSDB retention / S3 tiering)  │  │  │
│  │  └───────────────────────┘  └────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │  SP-8: Feedback Collector                                        │  │
│  │                                                                  │  │
│  │  sources: collector /metrics · ASAPQuery execution logs ·        │  │
│  │           ClickHouse / TSDB metrics · S3 access logs             │  │
│  │                                                                  │  │
│  │  signals: actual fill rate · sketch size · CPU · bandwidth ·     │  │
│  │           query latency · cache hit rate · storage scan cost     │  │
│  │                                                                  │  │
│  │  actions: update SP-5 cost model constants (EMA)                 │  │
│  │           trigger SP-6 re-plan on SLA drift or waste             │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                  │ OpAMP push        │ HTTP API         │ storage API
                  ▼                  ▼                   ▼
          ┌──────────────┐  ┌──────────────────┐  ┌────────────────────┐
          │  SDK         │  │  ASAPQuery        │  │  ClickHouse / TSDB │
          │  Agent OTel  │  │  Precompute       │  │  S3 archive        │
          │  Backend OTel│  │  Engine           │  │                    │
          └──────────────┘  └──────────────────┘  └────────────────────┘
              (data plane — internal paths not shown)
```

---

## Data Lifecycle View

The control plane manages decisions across every hop in the data lifecycle. The table below maps each lifecycle transition to the subproblems that govern it.

| Lifecycle hop | Data in flight | SP governing the decision |
|---|---|---|
| App → SDK | Raw samples or SDK-level pre-aggregation | SP-3, SP-4 |
| SDK → Agent Collector | Raw OTLP or pre-aggregated counters | SP-3, SP-4 |
| Agent Collector (internal) | Sketch insertion, window management, delta encoding | SP-2, SP-3, SP-4, SP-5 |
| Agent → Backend Collector | Sketch payload (full or delta) or raw OTLP | SP-4, SP-5, SP-6 |
| Backend Collector (internal) | Sketch merge across agents, backend-level window | SP-3, SP-5 |
| Backend Collector → ASAPQuery Precompute | Merged sketches | SP-3, SP-5 |
| Precompute Engine (internal) | Query execution over sketches → scalar results | SP-3, SP-5, SP-6 |
| Precompute → DB / S3 | Materialized query results + raw/sketch archival | SP-4, SP-5, SP-6 |
| DB / S3 → ASAPQuery query | Point queries, range scans, exact aggregations | SP-4, SP-5 |

---

## SP-7 Implementation Gap: ASAPQuery Precompute Delivery

### Current state

`PrecomputeClient` (`controller/src/config/precompute.rs`) is implemented and exported, but is **never instantiated or called from `main.rs`**. The controller builds `PrecomputeJob` structs via `build_precompute_jobs()` and reports their count in the plan response (`"precompute_jobs": N`), but the HTTP call to ASAPQuery is never made. Precompute delivery is therefore a no-op in the current implementation.

### Required controller changes

**1. Add `ASAP_QUERY_URL` environment variable**

```
ASAP_QUERY_URL=http://asapquery:8090   # optional; skip precompute delivery if unset
```

Instantiate `PrecomputeClient` in `AppState` as `Option<PrecomputeClient>`.

**2. Register jobs from `handle_plan()`**

After `plan.precompute` is populated, call `client.register()` for each job and persist the returned `job_id` values alongside the plan in the store. Failure to register should be logged and reflected in the plan response but must not fail the plan itself.

```rust
// in handle_plan(), after build_precompute_jobs():
if let Some(ref client) = st.precompute_client {
    for job in &plan.precompute {
        match client.register(job).await {
            Ok(resp) => { /* store resp.job_id with plan */ }
            Err(e)   => warn!("precompute register failed: {e}"),
        }
    }
}
```

**3. Deregister stale jobs on re-plan and rollback**

In `handle_rollback()` and the replanner (`replan.rs`), call `client.deregister(job_id)` for every job ID stored against the previous plan version before applying the new plan. This prevents stale materializations from accumulating in ASAPQuery.

**4. Plan response additions**

Add `precompute_job_ids: [string]` to the plan JSON response so callers can track which jobs were registered.

### Testing the sketch-to-OTel-datapath assignment

**A. Per-sketch-type YAML unit test (Rust)**

Add a `#[tokio::test]` in `config/agent.rs` (or a new `tests/sketch_otel_assignment.rs`) that calls `generate_agent_config()` for each aggregation type and asserts the resulting YAML processor key matches the expected sketch:

| `aggregations` | `plan.agent_config.sketch_type` | Expected YAML processor key |
|---|---|---|
| `["quantile"]` | `DDSketch` | `ddsketch:` |
| `["cardinality"]` | `HLL` | `hll:` |
| `["frequency"]` | `CountSketch` | `countsketch:` |

**B. E2E shell test extension**

Extend `tests/otel_controller_e2e_test.sh` to loop over sketch types. For each iteration: submit plan → fetch YAML → assert processor key → start matching collector binary → run `e2esdkbench --sketch-type=<type>` → assert `otelcol_processor_accepted_metric_points > 0`.

**C. OpAMP assignment path test**

Add a test that connects a minimal fake OpAMP client (a `tokio::spawn` WebSocket echo) before submitting a plan, then asserts the `RemoteConfig` YAML pushed by the controller contains the right processor key. This validates the OpAMP delivery path independently of the HTTP config provider.

---

## SP-3/SP-7: ASAPQuery Requirements for E2E Precomputation

For the precompute stage assignment in SP-3 to function end-to-end, ASAPQuery must implement the following components. The controller's `PrecomputeClient` defines the exact wire contract.

### API surface (HTTP REST)

```
POST   /api/v1/precompute/jobs          register a new precompute job
DELETE /api/v1/precompute/jobs/{id}     deregister and evict a job
GET    /api/v1/precompute/jobs          list active jobs and their status
```

**Request body for `POST /api/v1/precompute/jobs`:**

```json
{
  "query":       "quantile_over_time(0.99, latency{service=\"web\"}[5m])",
  "granularity": "1m",
  "source":      "backend-collector:4317",
  "sketch_type": "ddsketch",
  "store_path":  "precomputed/latency/p99/5m"
}
```

**Response:**

```json
{
  "job_id":     "job-abc123",
  "status":     "created",
  "created_at": "2026-03-26T00:00:00Z"
}
```

### Required internal components

| Component | Responsibility |
|---|---|
| **Job registry** | Persist active jobs; survive restart; expose `GET /jobs` for status |
| **Sketch puller** | On each `granularity` tick, fetch accumulated sketch blobs from `source` (backend collector OTLP or Prometheus endpoint) |
| **Query executor** | Evaluate `query_expr` using sketch-native functions (table below) |
| **Result cache** | Store scalar or vector result keyed by `store_path`; serve zero-latency on cache hit |
| **Scheduler** | Trigger each job at its `granularity` interval; honour `valid_until` TTL sent by the controller on re-plan |
| **Query router** | When a user PromQL/SQL query matches a registered `store_path`, short-circuit to cache instead of DB scan |
| **SP-8 metrics endpoint** | Expose `/metrics` (Prometheus) with job execution latency, cache hit rate, and sketch pull errors so the controller's feedback collector can feed them into the EMA cost model |

### Query executor — sketch function mapping

The `query_expr` strings emitted by the controller (`build_query_expr()` in `config/precompute.rs`) use the following function names. ASAPQuery must implement or alias each:

| Function | Sketch type | Semantics |
|---|---|---|
| `quantile_over_time(φ, metric[window])` | DDSketch | Query the DDSketch for quantile φ over the accumulated window |
| `count_distinct_over_time(metric[window])` | HLL | Return the HLL cardinality estimate for the window |
| `top_k_over_time(k, metric[window])` | CountSketch / CountMinSketch | Return the top-k heavy hitters from the sketch for the window |

### Integration with SP-8 feedback loop

The controller's `monitor/mod.rs` scrapes collector `/metrics` endpoints. To close the feedback loop for precomputation, ASAPQuery must expose these metrics on its own `/metrics` endpoint:

| Metric name | Type | Description |
|---|---|---|
| `asapquery_precompute_job_duration_seconds` | Histogram | Wall time to execute one precompute job cycle |
| `asapquery_precompute_cache_hits_total` | Counter | Queries served from precomputed cache |
| `asapquery_precompute_cache_misses_total` | Counter | Queries that fell through to DB scan |
| `asapquery_precompute_sketch_pull_errors_total` | Counter | Failed sketch fetches from source |

These map directly to the "ASAPQuery execution logs" row in the SP-8 feedback sources table and allow the EMA cost model to observe actual precompute job timing and cache efficiency.

---

## Subproblem Summary

| # | Subproblem | Input | Output | Primary technique |
|---|---|---|---|---|
| SP-1 | Query Workload Extraction | PromQL / SQL strings | `QueryWorkload[]` | AST parsing, query registry |
| SP-2 | Sketch Candidate Enumeration | `QueryWorkload` | Valid `(sketch_type, params)[]` | Type-based lookup table |
| SP-3 | Stage Assignment | Sketch candidates + stages | `StagedPlan[]` | Rule-based / ILP |
| SP-4 | Storage & Compression | Staged plan + infra | Compression + tiering policy | Heuristic / cost threshold |
| SP-5 | Cost Estimation | Candidate plan + workload | Cost vector per plan | Offline benchmarks + online EMA |
| SP-6 | Optimization | Candidate plans + cost vectors | `CollectionPlan` | Pareto / heuristic / learned |
| SP-7 | Plan Delivery | `CollectionPlan` | Live configs pushed to components | OpAMP + HTTP API |
| SP-8 | Feedback & Re-planning | Live telemetry | Updated cost model + re-plan trigger | EMA + SLA monitoring |
| SP-9 | AST-Aware Stage Split | `SketchExpr` tree + stage map | Per-stage sub-plans (agent / backend / precompute) | Tree partitioning |

---

## Related Documents

- [`control-plane-design.md`](control-plane-design.md) — controller architecture, OpAMP integration, Rust implementation plan
- [`controller-delta-decision-design.md`](controller-delta-decision-design.md) — detailed spec for SP-4/SP-5 delta cost model and unit-test cases
- [`delta-transmission-design.md`](delta-transmission-design.md) — delta wire protocol and sparse encoding math
- [`serf-compression-architecture.md`](serf-compression-architecture.md) — Serf/Gorilla transport-layer compression pipeline
