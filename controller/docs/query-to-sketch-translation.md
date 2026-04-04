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
Physical Plan (edge processors, backend sketchDB, backend original DB, object store)
```

### What each layer owns

| Layer | Input | Output | Responsibility |
|---|---|---|---|
| **1. Query Language** | query string | language AST | grammar, parsing |
| **2. Language Logical Plan** | AST | language-specific relational plan | language semantics (PromQL instant/range vectors, SQL frames, Elastic buckets) |
| **3. Sketch Logical Plan** | language plan | sketch algebra tree (`QueryExpr`) | **what** to compute: aggregation intent + accuracy requirement + window semantics — no sketch names, no implementation details |
| **4. Sketch Optimizer** | sketch plan + deployment constraints | optimised sketch plan | cost-aware rewrites: push-down, fusion, elimination, budget-driven deferral — considers physical deployment constraints (memory budgets, network topology, available backends) |
| **5. Physical Plan** | optimised plan + deployment config | executable plan | **how** to execute: edge processors (sketch build), backend sketchDB (merge + query), backend original DB (exact), object store (raw backup) |

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
setup: edge processors, backend sketchDB, backend original DB, or object store.

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
- **Layer 5** (physical planner): "for this deployment, DDSketch at the edge is cheapest" or "KLL at the backend sketchDB is better for this workload"

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
| `WindowedAgg { agg, window, col }` | Bundled window + sketch aggregation | In sketch systems the window defines the sketch lifecycle (when to flush/reset). Bundling lets the physical planner choose the best implementation (edge tumbling flush vs backend sketchDB EH vs original DB time_bucket). |

`WindowSpec` supports five window kinds:

| `WindowKind` | Semantics | Example |
|---|---|---|
| `Tumbling { size }` | Fixed-size, non-overlapping | PromQL implicit, SQL `TUMBLE(ts, '5m')`, Elastic `fixed_interval` |
| `Sliding { size, slide }` | Fixed-size, overlapping | PromQL `[5m]` range vector, SQL `HOP(ts, '1m', '5m')` |
| `Unbounded` | All samples, no time dimension | SQL `GROUP BY key` without time |
| `Landmark` | From epoch to now (cumulative) | Running aggregates |
| `Session { gap }` | Gap-based, closes after inactivity | Elastic session windows |

## 3. Layers 1–3: Query Language → Language Plan → Sketch Algebra

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

## 4. Concrete Example: PromQL (all 5 layers)

### Query
```promql
quantile_over_time(0.99, http_request_duration{env="prod"}[5m])
```

### Layer 1 — Language AST

The `promql-parser` crate parses the string into a PromQL AST:
`Call("quantile_over_time", [NumberLiteral(0.99), MatrixSelector("http_request_duration", {env="prod"}, 5m)])`

### Layer 2 — Language Logical Plan (parser output)

The PromQL parser emits **relational operators only** — `Aggregate { AggFunc }` + `Window`,
no sketch names:

```
Aggregate {
  keys: [],
  aggs: [AggItem { func: Quantile(0.99), col: SampleValue }],
  input: Window {
    duration: 5m,
    input: Filter {
      pred: Column("env") = Literal("prod"),
      input: Source("http_request_duration")
    }
  }
}
```

### Layer 3 — Sketch Logical Plan (after lowering)

The shared `lower_to_sketch_algebra()` pass converts `Aggregate { Quantile }` to
`AggIntent::Quantile` and fuses with `Window` into `WindowedAgg`:

```
WindowedAgg {
  agg: Quantile { quantiles: [0.99], accuracy: 0.01 },
  window: WindowSpec { kind: Tumbling { size: 5m } },
  col: SampleValue,
  input: Filter {
    pred: Column("env") = Literal("prod"),
    input: Source("http_request_duration")
  }
}
```

Note: no sketch implementation names — just "I need a quantile at φ=0.99 with ≤1% error."

### Layer 4 — Optimizer

R1 (PredicatePushDown): filter is already below the window — no change. Tree is returned as-is.

### Layer 5 — Physical Plan

`physical::resolve(Quantile { [0.99], 0.01 })` → `PhysicalAggOp { sketch_type: DDSketch, sketch_params: DDSketch { relative_accuracy: 0.01, quantiles: [0.99] } }`

`resolve_window(Tumbling { 5m }, AgentCollector)` → `OtelTumblingFlush { 5m }`

Stage-split → all at Agent. Config emit → OTel Collector YAML:
```yaml
processors:
  ddsketch:
    mode: Window
    window_duration: 5m
    relative_accuracy: 0.01
    quantiles: [0.99]
    label_matchers: ["env=prod"]
```

### Execution

1. **Agent** receives raw samples → filters `env="prod"` → batches 5m windows → DDSketch → emit
2. **Backend** merges DDSketches from N agents
3. **Query time**: extract 0.99 quantile from merged DDSketch

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

### Layer 3 — Sketch Logical Plan (after lowering)

`lower_to_sketch_algebra()` converts `Aggregate { Count }` → `SketchAgg { Frequency }`,
fuses with `Window` → `WindowedAgg`, and wraps with `Partition`:

```
TopK {
  k: 10,
  by: ["service"],
  input: Partition {
    keys: By(["service"]),
    input: WindowedAgg {
      agg: Frequency { accuracy: 0.01 },
      window: WindowSpec { kind: Tumbling { size: 1m } },
      col: SampleValue,
      input: Filter {
        pred: env = "prod",
        input: Source("requests")
      }
    }
  }
}
```

Note: `Frequency`, not `CountSketch` — implementation-independent. Both CountSketch
and CountMinSketch are valid candidates; the physical planner decides.

### Layer 4 — Optimizer

R1 (PredicatePushDown): filter already below window — no change.

### Layer 5 — Physical Plan

`physical::resolve(Frequency { 0.01 })` → `PhysicalAggOp { sketch_type: CountSketch, ... }`

Stage-split:
| Node | Stage | Reason |
|---|---|---|
| Source, Filter | Agent | leaf + volume reduction |
| WindowedAgg { Frequency } | Agent | sketch fits in memory budget |
| Partition { service } | Backend | GROUP BY distribution |
| TopK { 10 } | Precompute | global ranking requires merged data |

### Execution

1. **Agent** → filters → builds CountSketch per 1m window → emits to backend
2. **Backend** → merges per service
3. **Precompute** → extracts top-10 services by frequency

---

## 6. Concrete Example: SQL (all 5 layers)

### Query
```sql
SELECT symbol, AVG(price) FROM trades GROUP BY symbol
```

### Layer 1 — Language AST

`sqlparser` produces: `Select { projection: [Identifier("symbol"), Function(AVG, "price")], from: [Table("trades")], group_by: [Identifier("symbol")] }`

### Layer 2 — Language Logical Plan

Both parsers emit the same kind of output — relational `Aggregate { AggFunc }`:

```
Aggregate {
  keys: ["symbol"],
  aggs: [AggItem { func: Avg, col: Named("price"), alias: "avg" }],
  having: None,
  input: Source("trades")
}
```

### Layer 3 — Sketch Logical Plan (after lowering)

`lower_to_sketch_algebra()` converts `Avg` → `Quantile { [0.5], 0.01 }` (median proxy):

```
Partition {
  keys: By(["symbol"]),
  input: SketchAgg {
    op: Quantile { quantiles: [0.5], accuracy: 0.01 },
    col: Named("price"),
    input: Source("trades")
  }
}
```

However, `Avg` is **non-mergeable** (`avg(A∪B) ≠ merge(avg(A), avg(B))`).
The stage-split will route this to DB for exact computation.

### Layer 4 — Optimizer

No rewrites applicable.

### Layer 5 — Physical Plan

Stage-split assigns `Quantile(Avg proxy)` → DB stage (non-mergeable).
The DB computes exact AVG per symbol on the full dataset.

---

## 7. Concrete Example: SQL with TUMBLE window (all 5 layers)

### Query
```sql
SELECT region, COUNT(DISTINCT user_id) AS cnt
FROM sessions
GROUP BY region, TUMBLE(ts, INTERVAL '5' MINUTE)
ORDER BY cnt DESC LIMIT 10
```

### Layer 1–2 — Parse to relational operators

The SQL parser detects `TUMBLE(ts, INTERVAL '5' MINUTE)` in GROUP BY and emits
a `Window` node. `COUNT(DISTINCT user_id)` becomes `AggFunc::CountDistinct`:

```
Limit {
  n: 10,
  input: Sort {
    keys: [{ col: "cnt", desc: true }],
    input: Aggregate {
      keys: ["region"],
      aggs: [AggItem { func: CountDistinct, col: Named("user_id"), alias: "cnt" }],
      input: Window {
        duration: 5m,
        input: Source("sessions")
      }
    }
  }
}
```

### Layer 3 — Sketch Logical Plan (after lowering)

`lower_to_sketch_algebra()` converts `CountDistinct` → `Cardinality { 0.01 }` and
fuses `Window + Aggregate` → `WindowedAgg`:

```
Limit {
  n: 10,
  input: Sort {
    input: Partition {
      keys: By(["region"]),
      input: WindowedAgg {
        agg: Cardinality { accuracy: 0.01 },
        window: WindowSpec { kind: Tumbling { size: 5m } },
        col: Named("user_id"),
        input: Source("sessions")
      }
    }
  }
}
```

### Layer 4 — Optimizer

**R5 (TopKFusion)**: `Limit(10, Sort(desc, ...))` → fused into `TopK { k: 10 }`

### Layer 5 — Physical Plan

`physical::resolve(Cardinality { 0.01 })` → `PhysicalAggOp { sketch_type: HLL, sketch_params: HLL { precision: 14 } }`

`resolve_window(Tumbling { 5m }, AgentCollector)` → `OtelTumblingFlush { 5m }`

Stage-split:
| Node | Stage | Reason |
|---|---|---|
| Source("sessions") | Agent | leaf |
| WindowedAgg { Cardinality } | Agent | HLL 16KB — fits budget |
| Partition { region } | Backend | GROUP BY distribution |
| TopK { 10 } | Precompute | global ranking |

### Execution

1. **Agent**: builds one HLL per region per 5m window → emits to backend
2. **Backend**: merges HLLs from N agents (HLL merge = set union)
3. **Precompute**: extracts cardinality per region → top 10

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
