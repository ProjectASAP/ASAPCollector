# Query-to-Sketch Translation: How QL Maps to Sketch Execution

This document explains how a PromQL or SQL query is translated through the
controller's five-layer architecture and ultimately mapped to sketch-based
distributed execution.

## 1. Five-Layer Architecture

The controller is structured as a five-layer pipeline.  Each layer has a
clear input, output, and responsibility:

```
Query workloads
  │
  │  Layer 1 — Query Language
  │  (PromQL, SQL, DataFusion, ElasticDSL, ...)
  ▼
Language-specific AST
  │
  │  Layer 2 — Language Logical Plan
  │  (each language's own relational/query algebra)
  ▼
Language Logical Plan
  │
  │  Layer 3 — Sketch Logical Plan (Sketch Algebra)
  │  (language-independent, implementation-independent)
  ▼
Sketch Logical Plan
  │
  │  Layer 4 — Sketch Optimizer
  │  (rewrite rules on the sketch logical plan)
  ▼
Optimised Sketch Logical Plan
  │
  │  Layer 5 — Physical Execution Plan
  │  (concrete implementations for a specific deployment)
  ▼
Physical Plan (OTel processors, PromSketch, DB queries, on-device)
```

### What each layer owns

| Layer | Input | Output | Responsibility |
|---|---|---|---|
| **1. Query Language** | query string | language AST | grammar, parsing |
| **2. Language Logical Plan** | AST | language-specific relational plan | language semantics (PromQL instant/range vectors, SQL frames, Elastic buckets) |
| **3. Sketch Logical Plan** | language plan | sketch algebra tree (`QueryExpr`) | **what** to compute: aggregation intent + accuracy requirement + window semantics — no sketch names, no implementation details |
| **4. Sketch Optimizer** | sketch plan + deployment constraints | optimised sketch plan | cost-aware rewrites: push-down, fusion, elimination, budget-driven deferral — considers physical deployment constraints (memory budgets, network topology, available backends) |
| **5. Physical Plan** | optimised plan + deployment config | executable plan | **how** to execute: DDSketch vs KLL, OTel vs PromSketch, tumbling ticker vs EH buckets, data exchange format |

### Key design principle

**Layers 1–3 are query-language-independent and workload-independent.**
They define *what* to compute without reference to any specific query language,
sketch implementation, or deployment topology.  A `Quantile { φ=0.99, accuracy=0.01 }`
intent is the same whether it came from PromQL, SQL, DataFusion, or ElasticDSL,
and whether the deployment is a single node or a 1000-agent fleet.

**Layer 4 is deployment-constraint-aware.**
The optimizer considers physical deployment constraints — memory budgets per stage,
network bandwidth, available backends — when applying cost-based rewrite rules
(e.g., deferring a sketch from Agent to Backend when the agent memory budget is
exceeded, or fusing TopK when the downstream merge is expensive).

**Layer 5 is deployment-specific.**
The physical planner commits to concrete implementations based on the specific
setup: OTel Collector processors, PromSketch EH stores, on-device ring buffers,
or database-side SQL.

### `AggIntent` — the Layer 3 aggregation vocabulary

| `AggIntent` variant | Meaning | Physical candidates (Layer 5) |
|---|---|---|
| `Quantile { quantiles, accuracy }` | "I need quantile estimates at these φ values within this error" | DDSketch, KLL, t-digest, PromSketch EHKLL |
| `Cardinality { accuracy }` | "I need a distinct-count estimate within this error" | HLL, UnivMon, PromSketch EHUniv |
| `Frequency { accuracy }` | "I need frequency estimates within this error" | CountSketch, CountMinSketch |
| `Extrema { min, max }` | "I need exact min/max" | ExactMinMax, DDSketch at φ=0/1 |
| `PerPartition { inner, keys }` | "Run inner once per distinct key tuple" | Hydra, per-key sketch instances |
| `Exact(Sum\|Count\|Avg\|Min\|Max)` | "No sketch benefit — exact computation" | Raw passthrough, DB-side |

The flow:
- **Layers 1–2** (parsers): "this query needs a quantile at φ=0.99" → `Aggregate { Quantile(0.99) }`
- **Layer 3** (lowering): `Aggregate` → `SketchAgg { AggIntent::Quantile }` (shared by all languages)
- **Layer 4** (optimizer): rewrites the plan considering deployment constraints
- **Layer 5** (physical planner): "for this deployment, DDSketch is cheapest" or "PromSketch EHKLL is better because it's co-located"

## 2. Sketch Logical Plan: `QueryExpr` (Layer 3)

`QueryExpr` (`algebra/expr.rs`) is the sketch algebra IR — a **logical plan** that
normalises all query languages into a common algebraic form.

| | AST (syntax tree) | Logical Plan (QueryExpr) |
|---|---|---|
| **Structure** | Mirrors the grammar | Mirrors relational algebra operators |
| **Semantics** | Preserves syntactic details | Preserves only operator semantics |
| **Sketch types** | N/A | Implementation-independent intents (`AggIntent`) |
| **Language** | Language-specific | Language-independent (shared by SQL, PromQL, etc.) |

QueryExpr has **25 operator variants** organized into categories:

**Relational core** — standard relational algebra:
- `Source` — base metric / table (leaf node)
- `Filter { pred, input }` — selection (σ)
- `Project { cols, input }` — projection (π)
- `Aggregate { keys, aggs, having, input }` — grouping + aggregation (γ)
- `Join { kind, pred, left, right }` — relational join (⋈)
- `SetOp { kind, all, left, right }` — UNION / INTERSECT / EXCEPT
- `Sort { keys, input }` — ORDER BY
- `Limit { n, offset, input }` — LIMIT / OFFSET

**Sketch-specific** — operators that express sketch computation intent:
- `SketchAgg { op: AggIntent, col, input }` — sketch aggregation intent (what, not how)
- `WindowedAgg { agg: AggIntent, window: WindowSpec, col, input }` — bundled window + sketch agg (window defines sketch lifecycle)
- `Partition { keys, input }` — GROUP BY distribution for distributed sketches
- `Dedup { col, input }` — deduplication (absorbed by cardinality sketches)
- `TopK { k, by, input }` — top-K heavy-hitter query
- `Merge { inputs }` — sketch merge (linearity: sketch(A∪B) = merge(sketch(A), sketch(B)))
- `JoinSketch { join_key, outer, inner }` — sketch-aware join push-down

**Time / streaming** — window operators:
- `Window { duration, slide, input }` — standalone time window (batching)

**PromQL-specific** — operators that preserve PromQL semantics:
- `HistogramQuantile { phi, input }` — `histogram_quantile(φ, …)`
- `PromQLSubquery { range, resolution, input }` — `expr[range:resolution]`
- `BinaryOp { op, lhs, rhs, vector_match }` — vector binary arithmetic with matching

**Structural** — subqueries and bindings:
- `Subquery`, `LetBinding`, `Ref`, `WindowFunc`

### Window operators: `Window` vs `WindowedAgg`

| Operator | Use | Why separate |
|---|---|---|
| `Window { duration, slide }` | Standalone time batching (no sketch) | Used when the sketch op is a separate `SketchAgg` child node |
| `WindowedAgg { agg, window, col }` | Bundled window + sketch aggregation | In sketch systems the window defines the sketch lifecycle (when to flush/reset). Bundling lets the physical planner choose the best implementation (OTel tumbling flush vs PromSketch EH vs DB time_bucket). |

`WindowSpec` supports five window kinds:

| `WindowKind` | Semantics | Example |
|---|---|---|
| `Tumbling { size }` | Fixed-size, non-overlapping | PromQL implicit, SQL `TUMBLE(ts, '5m')`, Elastic `fixed_interval` |
| `Sliding { size, slide }` | Fixed-size, overlapping | PromQL `[5m]` range vector, SQL `HOP(ts, '1m', '5m')` |
| `Unbounded` | All samples, no time dimension | SQL `GROUP BY key` without time |
| `Landmark` | From epoch to now (cumulative) | Running aggregates |
| `Session { gap }` | Gap-based, closes after inactivity | Elastic session windows |

## 3. Parsing Algorithm: QL String → QueryExpr (Logical Plan)

### 3.1 PromQL parsing algorithm

**Entry**: `parse_promql_expr(query)` → calls `promql-parser` crate → walks AST via `walk_qe`.

The parser carries a **context** (`WalkCtx`) downward through the tree, propagating:
- `partition`: GROUP BY keys from an outer `by (dims)` clause
- `topk`: the K value from an outer `topk(k, …)` aggregate
- `outer_count`: whether the outer operator is `count(…)` (affects sketch selection)

**Algorithm** — `walk_qe(ast_node, ctx)`:

```
match ast_node:

  VectorSelector {name, labels}:
    → Source(name) + Filter(labels) + SketchAgg(Exact(Sum))
    # Bare metric selector: pass through raw data

  Call {func_name, args}:
    match func_name:
      "quantile_over_time":
        → extract φ from arg[0], extract metric/filters/window from arg[1]
        → SketchAgg(DDSketch([φ]), Window(Filter(Source)))

      "histogram_quantile":
        → extract φ from arg[0], extract inner rate/matrix from arg[1]
        → HistogramQuantile(φ, SketchAgg(DDSketch([φ]), Window(Filter(Source))))

      "count_over_time":
        → if ctx.outer_count: SketchAgg(HLL, ...)        # count(count_over_time(...)) = cardinality
        → else:               SketchAgg(CountMin, ...)     # plain frequency count

      "avg_over_time":      → SketchAgg(DDSketch([0.5]), ...)    # median as proxy
      "min_over_time":      → SketchAgg(DDSketch([0.0]), ...)    # or ExactMinMax if no GROUP BY
      "max_over_time":      → SketchAgg(DDSketch([1.0]), ...)    # or ExactMinMax if no GROUP BY
      "sum_over_time":      → SketchAgg(Exact(Sum), ...)         # not sketchable
      "rate"/"increase":    → SketchAgg(CountMin, ...)

    # After creating the SketchAgg, if ctx.topk is set, override op to CountSketch
    # Then wrap with Partition(ctx.partition) if present

  Aggregate {op, modifier, param, inner_expr}:
    match op:
      "topk"/"bottomk":
        → extract k from param
        → set ctx.topk = k, propagate ctx.partition from modifier
        → recurse into inner_expr with new context
        → wrap result in TopK(k, by) + Partition(keys)

      "count":
        → set ctx.outer_count = true
        → recurse (inner count_over_time will become HLL)
        → wrap result in Partition(keys)

      "sum"/"avg"/"min"/"max":
        → propagate partition, recurse into inner
        → wrap result in Partition(keys)

      "quantile":
        → extract φ from param
        → SketchAgg(DDSketch([φ]), inner) + Partition(keys)

      "stddev"/"stdvar":
        → SketchAgg(DDSketch([0.25, 0.75]), inner) + Partition(keys)

  Binary {op, lhs, rhs, matching}:
    → recurse lhs and rhs independently
    → BinaryOp(op, lhs, rhs, VectorMatch from matching)

  Subquery {expr, range, step}:
    → PromQLSubquery(range, step, recurse(expr))

  Paren {inner}:
    → recurse(inner)        # transparent
```

### 3.2 SQL parsing algorithm

**Entry**: `parse_sql_expr(sql)` → calls `sqlparser` crate → walks SQL AST.

The parser is **bottom-up** — it builds QueryExpr nodes layer by layer from the
clauses of a SELECT statement:

**Algorithm** — `extract_select_qe(select, order_by, limit, offset)`:

```
1. Extract source table name from FROM clause
   → Source(table_name)

2. If WHERE clause present:
   → convert SQL expression to ScalarExpr recursively
   → Filter(ScalarExpr, Source)

3. If JOIN present:
   → extract inner table, join kind, ON predicate
   → Join(kind, ScalarExpr, left=Filter(Source), right=Source(inner))

4. Collect aggregation functions from SELECT projection:
   → walk each SelectItem looking for COUNT, SUM, AVG, MIN, MAX
   → for each: create AggItem { func, col, alias, distinct }
   → COUNT(DISTINCT x) → AggItem { func: CountDistinct }
   → COUNT(*) → AggItem { func: Count }
   → AVG(x) → AggItem { func: Avg }

5. If aggregation items found + GROUP BY present:
   → Aggregate(keys=GROUP_BY, aggs=[AggItem...], having, input=step_3_result)
   If no aggregation items:
   → Project(cols, input=step_3_result)

6. If ORDER BY present:
   → Sort(keys, input=step_5_result)

7. If LIMIT present:
   → Limit(n, offset, input=step_6_result)

8. If UNION ALL / INTERSECT / EXCEPT:
   → parse each branch independently
   → SetOp(kind, all, left, right)
```

**SQL function → AggFunc mapping**:

| SQL function | AggFunc | Sketch candidate |
|---|---|---|
| `COUNT(*)` with GROUP BY | `Count` | CountSketch / CountMinSketch |
| `COUNT(*)` without GROUP BY | `Count` | Exact (no sketch benefit) |
| `COUNT(DISTINCT col)` | `CountDistinct` | HLL |
| `SUM(col)` | `Sum` | Exact (not sketchable) |
| `AVG(col)` | `Avg` | DDSketch (p50 proxy) or Exact(Avg) |
| `MIN(col)` | `Min` | DDSketch (φ=0.0) or ExactMinMax |
| `MAX(col)` | `Max` | DDSketch (φ=1.0) or ExactMinMax |

Note: the parser emits `Aggregate { func: Avg }` — it does **not** emit sketch ops.
Sketch assignment happens later in the optimizer (R9 HydraConversion) and allocator.
The SQL parser only produces relational operators; the PromQL parser is more aggressive
and emits `SketchAgg` nodes directly because PromQL functions like `quantile_over_time`
have a 1-to-1 mapping to sketch types.

## 4. Concrete Example: PromQL

### Query
```promql
quantile_over_time(0.99, http_request_duration{env="prod"}[5m])
```

### Step 1 — Parse to QueryExpr

The PromQL parser (`query_parser/promql.rs`) recognises `quantile_over_time` as a
sketch-eligible function and **emits a `SketchAgg` node directly** — PromQL functions
have a 1-to-1 mapping to sketch types, so the parser can commit to the sketch
operator at parse time:

```
SketchAgg {
  op: DDSketch { quantiles: [0.99], epsilon: 0.01 },
  col: SampleValue,
  input: Window {
    duration: 5m,
    input: Filter {
      pred: Column("env") = Literal("prod"),
      input: Source("http_request_duration")
    }
  }
}
```

The mapping rules for PromQL → QueryExpr:

| PromQL construct | QueryExpr node |
|---|---|
| metric selector `m{l="v"}` | `Source("m")` + `Filter { pred }` |
| range vector `[5m]` | `Window { duration: 5m }` |
| `quantile_over_time(φ, …)` | `SketchAgg { op: DDSketch([φ]) }` |
| `count_over_time(…)` | `SketchAgg { op: CountMin }` or `CountSketch` |
| `histogram_quantile(φ, rate(…))` | `HistogramQuantile { phi: φ }` |
| `topk(k, …)` | `TopK { k }` wrapping inner |
| `by (dims)` | `Partition { keys: By(dims) }` |
| `a + b` (vector binary) | `BinaryOp { op: Add, vector_match }` |

### Step 2 — Optimize

The optimizer applies rewrite rules.  For this simple query, only **R1 (PredicatePushDown)**
is relevant — the filter is already below the window, so no change.  The tree is returned as-is.

### Step 3 — Stage-split

`split_expr_by_stage` walks the tree bottom-up and assigns:

| Node | Stage | Reason |
|---|---|---|
| `Source("http_request_duration")` | Agent | leaf |
| `Filter { env="prod" }` | Agent | reduces volume early |
| `Window { 5m }` | Agent | time batching |
| `SketchAgg { DDSketch }` | Agent | sketch fits in agent memory budget |

Result:
```
StagedPlan {
  agent: AgentSubPlan {
    sketch_type: DDSketch,
    sketch_params: DDSketch { relative_accuracy: 0.01, quantiles: [0.99] },
    window_secs: 300,
    label_filters: ["env=prod"],
  },
  backend: BackendSubPlan { has_merge: false },
  precompute: PrecomputeSubPlan { active: false },
  db: DbSubPlan { active: false },
}
```

### Step 4 — Config emit

Agent OTel Collector YAML:
```yaml
receivers:
  otlp:
    protocols:
      grpc: { endpoint: 0.0.0.0:4317 }
      http: { endpoint: 0.0.0.0:4318 }
processors:
  ddsketch:
    mode: Window
    window_duration: 5m
    relative_accuracy: 0.01
    quantiles: [0.99]
    label_matchers: ["env=prod"]
    transmit_sketch: true
exporters:
  prometheus: { endpoint: 0.0.0.0:8889 }
service:
  pipelines:
    metrics:
      receivers: [otlp]
      processors: [ddsketch]
      exporters: [prometheus]
```

### How it executes

1. **Agent** receives raw OTLP metric samples → applies `env="prod"` filter →
   batches into 5-minute windows → inserts each sample into a DDSketch →
   emits the DDSketch (or delta-compressed diff) to the backend
2. **Backend** (if multiple agents) merges DDSketches from N agents
3. **Query time**: extract the 0.99 quantile from the merged DDSketch → returns a single number

---

## 5. Concrete Example: PromQL with Top-K

### Query
```promql
topk(10, count_over_time(requests{env="prod"}[1m]) by (service))
```

### Step 1 — Parse to QueryExpr

The PromQL parser directly emits sketch-aware nodes — `TopK`, `SketchAgg(CountSketch)`,
and `Partition` — because PromQL functions have a 1-to-1 sketch mapping.  Compare
this with the SQL examples below, where the parser emits generic `Aggregate` +
`Sort` + `Limit` and the optimizer transforms them into sketch nodes later:

```
Partition {
  keys: By(["service"]),
  input: TopK {
    k: 10,
    by: ["service"],
    input: SketchAgg {
      op: CountSketch { width: 2000, depth: 5 },
      col: SampleValue,
      input: Window {
        duration: 1m,
        input: Filter {
          pred: Column("env") = Literal("prod"),
          input: Source("requests")
        }
      }
    }
  }
}
```

Note: both `CountSketch` and `CountMinSketch` are valid candidates for top-K
frequency queries.  The parser emits a default; the `CostModelPlanner` scores
both candidates and picks the cheapest that meets the accuracy SLA.

### Step 2 — Optimize

R1 (PredicatePushDown): filter already below window — no change.
R5 (TopKFusion): TopK wrapping CountSketch is already the canonical form — no change.

### Step 3 — Stage-split

| Node | Stage | Reason |
|---|---|---|
| Source, Filter, Window | Agent | leaf + volume reduction + time batching |
| SketchAgg { CountSketch } | Agent | sketch fits in memory budget |
| Partition { service } | Backend | GROUP BY distribution |
| TopK { 10 } | Precompute | top-K extraction requires merged data |

Result:
```
StagedPlan {
  agent: AgentSubPlan {
    sketch_type: CountSketch,
    window_secs: 60,
    label_filters: ["env=prod"],
  },
  backend: BackendSubPlan {
    group_by: ["service"],
    has_merge: true,
  },
  precompute: PrecomputeSubPlan {
    active: true,
    topk: 10,
    query_expr: "topk(10, count_over_time(requests{env=\"prod\"}[1m]) by (service))",
  },
}
```

### How it executes

1. **Agent** receives raw samples → filters `env="prod"` → builds a CountSketch
   per 1-minute window → emits to backend
2. **Backend** receives sketches from N agents → merges CountSketches, grouped by `service`
3. **Precompute** receives merged sketches → extracts top-10 services by frequency →
   returns `[(service_a, 4521), (service_b, 3892), …]`

---

## 6. Concrete Example: SQL

### Query
```sql
SELECT symbol, AVG(price) FROM trades GROUP BY symbol
```

### Step 1 — Parse to QueryExpr

Unlike the PromQL parser, the SQL parser **does not emit `SketchAgg` nodes**.  It
produces relational `Aggregate` nodes with generic `AggFunc` variants (Avg, Count,
Sum, etc.).  Sketch assignment happens later — the optimizer and allocator decide
whether and which sketch type to use based on the aggregation function, GROUP BY
keys, and accuracy SLA.

The SQL parser (`query_parser/sql.rs`) maps the SELECT to:

```
Aggregate {
  keys: ["symbol"],
  aggs: [AggItem { func: Avg, col: Named("price"), alias: "avg" }],
  having: None,
  input: Source("trades")
}
```

Note: `Avg` here is a generic `AggFunc`, not a sketch op.  The allocator will later
determine that `AVG` is **non-mergeable** (`avg(A∪B) ≠ merge(avg(A), avg(B))`), so
it cannot be pushed down to a sketch on the Agent.

### Step 3 — Stage-split

| Node | Stage | Reason |
|---|---|---|
| Source("trades") | Agent | leaf |
| Aggregate { Avg } | DB | AVG is non-mergeable — requires all raw data |

Result:
```
StagedPlan {
  agent: AgentSubPlan { sketch_type: None },
  backend: BackendSubPlan { },
  precompute: PrecomputeSubPlan { active: false },
  db: DbSubPlan { active: true, query_expr: "avg by (symbol) (trades)" },
}
```

### How it executes

Since AVG cannot be sketched, the Agent passes raw samples through.  The DB
(ClickHouse / TSDB) computes the exact AVG per symbol on the full dataset.

---

## 7. Concrete Example: SQL with Sketch Opportunity

### Query
```sql
SELECT region, COUNT(DISTINCT user_id)
FROM sessions
GROUP BY region
ORDER BY cnt DESC LIMIT 10
```

### Step 1 — Parse to QueryExpr

Again, the SQL parser emits **relational operators only** — `Aggregate` with
`CountDistinct`, `Sort`, and `Limit`.  No sketch ops yet:

```
Limit {
  n: 10,
  offset: 0,
  input: Sort {
    keys: [SortKey { col: "cnt", desc: true }],
    input: Aggregate {
      keys: ["region"],
      aggs: [AggItem { func: CountDistinct, col: Named("user_id"), alias: "cnt" }],
      input: Source("sessions")
    }
  }
}
```

### Step 2 — Optimize

This is where the SQL path diverges from PromQL: the **optimizer rewrites
relational nodes into sketch-aware nodes**.  PromQL already emitted `SketchAgg`
at parse time; SQL goes through the optimizer to reach the same representation.

**R5 (TopKFusion)**: `Limit(10, Sort(desc, Aggregate))` → fused into `TopK { k: 10 }`

**R9 (HydraConversion)**: the optimizer recognises `CountDistinct` + `GROUP BY region`
and suggests HLL as the sketch type.

After optimization:
```
TopK {
  k: 10,
  by: ["region"],
  input: SketchAgg {
    op: HLL { registers: 14 },
    col: Named("user_id"),
    input: Source("sessions")
  }
}
```

### Step 3 — Stage-split

| Node | Stage | Reason |
|---|---|---|
| Source("sessions") | Agent | leaf |
| SketchAgg { HLL } | Agent | HLL memory = 2^14 = 16KB — fits budget |
| TopK { 10 } | Precompute | top-K needs merged data |

### How it executes

1. **Agent**: builds one HLL per region per window → emits to backend
2. **Backend**: merges HLLs from N agents (HLL merge = set union)
3. **Precompute**: extracts cardinality estimates per region → sorts → returns top 10

---

## 8. Sketch Directory: Which Sketch for Which Operation?

The sketch directory (`algebra/directory.rs`) maps aggregation types to candidate
sketch families.  The `CostModelPlanner` scores all candidates and picks the
cheapest that meets the accuracy SLA.

### Candidates per Aggregation Type

| Aggregation | Candidates (default first) | When non-default is chosen |
|---|---|---|
| Quantile | **DDSketch**, KLL | KLL when memory-constrained |
| Cardinality | **HLL** | Single candidate |
| Frequency | **CountSketch**, CountMinSketch | Based on cost model scoring |

### Sketch Type → OTel Collector Processor

| SketchType | Go processor | Key parameters |
|---|---|---|
| DDSketch | `ddsketch` | relative_accuracy, quantiles |
| KLL | `KLL` | k, quantiles |
| HLL | `HLL` | (fixed precision in Go code) |
| CountSketch | `countsketch` | epsilon, delta |
| CountMinSketch | `countmin` | rows, cols, metric_name |

### Stage Assignment Rules

| QueryExpr node | Default stage | Deferral trigger |
|---|---|---|
| Source, Filter, Window, SketchAgg | **Agent** | Memory budget exceeded → Backend |
| Partition, Merge, Dedup, Exact(Sum/Count/Min/Max) | **Backend** | Memory exceeded → Precompute |
| TopK, HistogramQuantile, BinaryOp, PromQLSubquery | **Precompute** | — |
| Exact(Avg) | **DB** | Non-mergeable — cannot distribute |

When an Agent sketch exceeds the memory budget, it is deferred to Backend.
If it also exceeds the Backend budget, it moves to Precompute.  Every deferral
is logged in `StagedPlan.deferral_log` for observability.
