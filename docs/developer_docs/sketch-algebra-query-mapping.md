# Sketch Algebra — Query Mapping Reference

This document defines how PromQL and SQL queries compile to the shared
**sketch algebra** IR (`SketchExpr`), the algebraic rewrite rules the
optimizer applies, and the aggregation-function → sketch-type table.

Both parsers are front-ends that emit the same `SketchExpr` tree.
The optimizer then applies rewrite rules before the planner converts
the tree to agent configurations.

---

## 1. Sketch Algebra Operators

| Operator | Symbol | Description |
|---|---|---|
| `Source(name)` | — | Base relation or metric stream |
| `Filter(preds, input)` | σ | Evaluate predicates on each tuple/sample before ingestion |
| `Window(duration, input)` | ψ | Time window applied to the stream (PromQL range; SQL time predicate) |
| `Partition(keys, input)` | γ | Group by key-tuple; one sketch instance per distinct value |
| `Agg(op, col, input)` | α | The sketch aggregation itself |
| `Dedup(col, input)` | δ | Deduplicate on `col` before ingestion |
| `TopK(k, input)` | τ | Retain only the top-K entries from the sketch result |
| `Merge(inputs)` | ⊕ | Merge sketches from multiple branches (requires mergeability) |
| `JoinSketch(key, outer, inner)` | ⋈ₛₖ | Pre-aggregate sketch on inner side by join key, merge after join |

### 1.1 Sketch Aggregation Operators (`SketchAggOp`)

| `SketchAggOp` | Sketch structure | Mergeable | Default params |
|---|---|---|---|
| `CountMin { width, depth }` | Count-Min Sketch | ✓ | width=2000, depth=5 |
| `CountSketch { k }` | Count Sketch (heavy hitter) | ✓ | k from query |
| `HLL { registers }` | HyperLogLog | ✓ | registers=14 (~0.8 % error) |
| `DDSketch { quantiles, epsilon }` | DDSketch | ✓ | epsilon=0.01 |
| `ExactMinMax { min, max }` | Exact running min/max | ✓ | — |
| `Exact(Count)` | Integer counter | ✓ | — |
| `Exact(Sum)` | f64 accumulator | ✓ | — |
| `Exact(Avg)` | (sum, count) pair | **✗** | use DDSketch(p50) in distributed contexts |
| `Exact(Min)` / `Exact(Max)` | Running extremum | ✓ | use ExactMinMax |

> **Note on `Exact(Avg)`:** average-of-averages ≠ average.  In distributed
> collection each agent must emit a `(sum, count)` pair, not a precomputed
> average.  The controller recomputes `avg = sum / count` after merging.
> Alternatively, map to `DDSketch(quantiles=[0.5])` which is mergeable at
> the cost of approximation error ε.

---

## 2. Aggregation Function → Sketch Mapping

### 2.1 SQL aggregation functions

The mapping depends on three context flags captured during AST traversal:
- **G** — GROUP BY present
- **T** — top-K pattern (ORDER BY col DESC LIMIT k)
- **J** — JOIN present in the path to the aggregation

| SQL expression | G | T | J | `SketchAggOp` | Notes |
|---|---|---|---|---|---|
| `COUNT(*)` | ✗ | ✗ | ✗ | `Exact(Count)` | Global count; no sketch benefit |
| `COUNT(*)` | ✓ | ✗ | ✗ | `CountMin` | Frequency per group |
| `COUNT(*)` | ✓ | ✓ | ✗ | `CountSketch(k)` | Heavy-hitter top-K (canonical, unbiased). `CountMin(k)`-with-heap is also valid via the CMS-Heap pattern (Cormode & Muthukrishnan 2005) — opt in via `sketch_family_override: CountMinSketch`. |
| `COUNT(*)` WHERE pred | ✗ | ✗ | ✗ | `CountMin` | Push filter; sketch for predicate-key frequency |
| `COUNT(DISTINCT col)` | ✗ | ✗ | ✗ | `HLL` | Global distinct count |
| `COUNT(DISTINCT col)` | ✓ | ✗ | ✗ | `Hydra(HLL, keys)` | Per-group distinct count |
| `COUNT(DISTINCT col)` | ✓ | ✓ | ✗ | `TopK(k, Hydra(HLL, keys))` | Top-K distinct-count groups |
| `SUM(col)` | any | any | ✗ | `Exact(Sum)` | Exact; no sketch benefit |
| `AVG(col)` | ✗ | ✗ | ✗ | `Exact(Avg)` | No group; trivially exact |
| `AVG(col)` | ✓ | ✗ | ✗ | `DDSketch([0.5])` | Median proxy; mergeable |
| `MIN(col)` | ✗ | ✗ | ✗ | `ExactMinMax(min=true)` | Cheaper than DDSketch |
| `MAX(col)` | ✗ | ✗ | ✗ | `ExactMinMax(max=true)` | Cheaper than DDSketch |
| `MIN(col)` | ✓ | ✗ | ✗ | `DDSketch([0.0])` | Extreme-quantile per group |
| `MAX(col)` | ✓ | ✗ | ✗ | `DDSketch([1.0])` | Extreme-quantile per group |
| `MIN(col)` + `MAX(col)` | ✓ | ✗ | ✗ | `DDSketch([0.0, 1.0])` | Single sketch, both extremes |
| `AGG(col)` | any | any | ✓ | `JoinSketch(key, outer, inner_with_Agg)` | Pre-agg on inner; see §4 |

#### Multi-aggregation in one SELECT

Each aggregation column gets its own `SketchAggOp`.  They run over the
same filtered/windowed input stream and are collected in parallel.

Example — `SELECT RegionID, SUM(x), COUNT(*) AS c, AVG(w), COUNT(DISTINCT u) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10`:

```
TopK(10,
  Partition([RegionID],
    Filter([],
      Merge([
        Agg(Exact(Sum),         col=x,   Source(hits)),
        Agg(CountSketch(k=10),  col=*,   Source(hits)),
        Agg(DDSketch([0.5]),    col=w,   Source(hits)),
        Agg(Hydra(HLL,[RegionID]), col=u, Source(hits)),
      ])
    )
  )
)
```

Coverage = `Partial` (SUM is exact alongside sketched columns).

#### Multi-dimensional GROUP BY

When GROUP BY has more than one key, wrap the inner `SketchAggOp` in
`Hydra` (sketch-of-sketches):

```
-- GROUP BY (k1, k2)
Partition([k1, k2], Agg(op, ...))
  →  Agg(Hydra(op, [k1, k2]), ...)   -- via rewrite rule R4
```

Alternatively, maintain a flat per-(k1,k2) sketch if the key space is
small enough to enumerate.

#### HAVING

`HAVING p(keys)` stays **above** the `Agg` node (post-sketch filter on
group keys).  It is not pushed into the sketch.

```
Filter(p_having, Agg(op, Partition(keys, Filter(p_where, Source(t)))))
```

#### UNION ALL

```sql
SELECT α(x) FROM R  UNION ALL  SELECT α(x) FROM S
```

requires mergeability:

```
Merge([
  Agg(op, Source(R)),
  Agg(op, Source(S)),
])
```

Rejected (compile error) when `op` is `Exact(Avg)` — use `(sum, count)` instead.

#### SELECT DISTINCT / COUNT(DISTINCT)

- `COUNT(DISTINCT col)` → `HLL` (dedup is inherent in HLL; `Dedup` node omitted via R6).
- `SELECT DISTINCT … COUNT(col)` → `Dedup(col, Source(t))` pushed before `Agg(CountMin, …)`.

---

### 2.2 SQL WHERE predicates → `Filter` predicates

All WHERE predicates are captured and pushed down to the `Filter` node,
regardless of operator:

| SQL predicate | `FilterOp` |
|---|---|
| `col = 'v'` | `Eq` |
| `col <> 'v'` | `Ne` |
| `col > v` | `Gt` |
| `col >= v` | `Ge` |
| `col < v` | `Lt` |
| `col <= v` | `Le` |
| `col LIKE '%v%'` | `Like` |
| `col NOT LIKE '%v%'` | `NotLike` |
| `col IS NULL` | `IsNull` |
| `col IS NOT NULL` | `IsNotNull` |

Compound predicates (`AND`, `OR`) are decomposed recursively.
`OR` predicates that span multiple columns cannot be fully pushed to the
collector and are marked `filter_side: Controller` (evaluated at merge time).

---

### 2.3 PromQL expressions

PromQL label matchers map to `Filter` predicates; the range vector maps
to `Window`; `by`/`without` map to `Partition`.

| PromQL expression | `SketchExpr` tree | `SketchAggOp` |
|---|---|---|
| `quantile_over_time(φ, m{f}[w]) by (d)` | `Partition(d, Window(w, Filter(f, Agg(DDSketch([φ]), Source(m)))))` | `DDSketch([φ])` |
| `histogram_quantile(φ, rate(m{f}[w])) by (d)` | same shape; inner `rate()` unwrapped | `DDSketch([φ])` |
| `avg_over_time(m{f}[w]) by (d)` | `Partition(d, Window(w, Filter(f, Agg(DDSketch([0.5]), Source(m)))))` | `DDSketch([0.5])` |
| `min_over_time(m{f}[w]) by (d)` | same | `DDSketch([0.0])` |
| `max_over_time(m{f}[w]) by (d)` | same | `DDSketch([1.0])` |
| `min_over_time(m{f}[w])` *(no by)* | `Window(w, Filter(f, Agg(ExactMinMax(min=true), Source(m))))` | `ExactMinMax` |
| `max_over_time(m{f}[w])` *(no by)* | same | `ExactMinMax` |
| `stddev_over_time(m{f}[w]) by (d)` | same as avg shape | `DDSketch([0.25, 0.75])` (IQR proxy) |
| `count_over_time(m{f}[w]) by (d)` | `Partition(d, Window(w, Filter(f, Agg(CountMin, Source(m)))))` | `CountMin` |
| `sum_over_time(m{f}[w]) by (d)` | same shape | `Exact(Sum)` |
| `topk(k, count_over_time(m{f}[w]) by (d))` | `TopK(k, Partition(d, Window(w, ...)))` | `CountSketch(k)` |
| `topk(k, avg_over_time(m{f}[w]) by (d))` | `TopK(k, Partition(d, Window(w, ...)))` | `DDSketch([0.5])` |
| `count(count_over_time(m{f}[w]) by (d))` | `Agg(HLL, Partition(d, Window(w, ...)))` | `HLL` (cardinality of active groups) |
| `sum by (d) (avg_over_time(m{f}[w]))` | outer Aggregate wraps inner Call; `by` from outer | `DDSketch([0.5])` |
| `changes(m{f}[w]) by (d)` | `Partition(d, Window(w, Filter(f, Agg(CountMin, Source(m)))))` | `CountMin` |
| `last_over_time(m{f}[w])` | `exact_required = true`; `coverage = None` | — |
| `delta(m{f}[w])` / `deriv(m{f}[w])` | `exact_required = true` | — |
| `predict_linear(m{f}[w], t)` | `exact_required = true` | — |
| `m{f}` *(bare selector)* | `exact_required = true` | — |
| `m_a{f} / m_b{f}` *(binary op)* | `exact_required = true`; both sides needed | — |

#### PromQL label matcher → `FilterOp`

| PromQL matcher | `FilterOp` |
|---|---|
| `key="val"` | `Eq` |
| `key!="val"` | `Ne` |
| `key=~"regex"` | `Regex` |
| `key!~"regex"` | `NotRegex` |

#### PromQL `without` clause

`without (d1, d2)` is the complement of `by`.  Because the full label set
is not known at parse time, it is stored as:

```rust
Partition::Without { excluded: vec!["d1", "d2"] }
```

The planner resolves the complement against the metric's actual label schema
at plan execution time.

---

## 3. Algebraic Rewrite Rules

The optimizer applies these rules bottom-up to produce the most efficient tree.

```
R1 — Filter push-down (always beneficial)
  Filter(p, Agg(op, X))  →  Agg(op, Filter(p, X))
  Condition: p is on a base column, not on the sketch output

R2 — HAVING / WHERE split
  Filter(p_key ∧ p_val, Agg(op, Partition(keys, X)))
    →  Filter(p_val,            -- stays above Agg (HAVING)
         Agg(op,
           Partition(keys,
             Filter(p_key, X)   -- pushed below Agg (WHERE on key)
           )
         )
       )

R3 — Sketch linearity over Merge  (α distributes over ⊕)
  Agg(op, Merge([X, Y]))  →  Merge([Agg(op, X), Agg(op, Y)])
  Condition: op is Mergeable
  Effect: each branch (agent) builds its own sketch; controller merges

R4 — Multi-key Partition → Hydra
  Partition([k1, k2, ...], Agg(op, X))
    →  Agg(Hydra(op, [k1, k2, ...]), X)
  Condition: |keys| > 1

R5 — Join push-down
  Agg(op, X ⋈_k Y)
    →  JoinSketch(k,
         outer = X,
         inner = Partition([k], Agg(op, Y))
       )
  Effect: sketch built on Y per join-key k; joined with X; MERGE after

R6 — Dedup elimination for HLL
  Agg(HLL, Dedup(col, X))  →  Agg(HLL, X)
  Reason: HLL inherently deduplicates

R7 — Window / Filter commutativity
  Window(w, Filter(p, X))  →  Filter(p, Window(w, X))
  Effect: filter applied before windowing (reduces stream size)

R8 — TopK absorption into CountSketch
  TopK(k, Partition(keys, Agg(CountSketch(k2), X)))
    →  Partition(keys, Agg(CountSketch(k=k), X))
  Condition: k == k2 (top-K already encoded in sketch)
```

---

## 4. Join Push-Down Detail

For a query of the form:

```sql
SELECT R.k, α(S.b)
FROM R JOIN S ON R.id = S.id
GROUP BY R.k
```

The naive plan builds `α` over the joined stream.
The sketch-algebra plan applies **R5**:

```
JoinSketch(
  join_key = "id",
  outer    = Source(R),
  inner    = Partition([id], Agg(op, Source(S)))
)
```

Execution:

1. Build `Agg(op, S)` grouped by `id` → one sketch per distinct `id`.
2. For each row in `R`, look up the sketch for `R.id` in the inner result.
3. Group by `R.k`, `Merge` the collected sketches.

This avoids materialising the full join before sketching and is the only
valid strategy when `S` is too large to join first.

For self-joins or multi-way joins, R5 is applied recursively, innermost
join first.

---

## 5. Mergeability Reference

The `Merge` node (R3) is only valid when all input `SketchAggOp`s are
mergeable.  The compiler rejects non-mergeable ops in `Merge` contexts.

| `SketchAggOp` | Mergeable | Merge operation |
|---|---|---|
| `HLL` | ✓ | bitwise OR of registers |
| `DDSketch` | ✓ | element-wise add of buckets |
| `CountMin` | ✓ | element-wise max of matrix cells |
| `CountSketch` | ✓ | element-wise sum of arrays |
| `ExactMinMax(min)` | ✓ | `min(min_A, min_B)` |
| `ExactMinMax(max)` | ✓ | `max(max_A, max_B)` |
| `Exact(Count)` | ✓ | `count_A + count_B` |
| `Exact(Sum)` | ✓ | `sum_A + sum_B` |
| `Exact(Avg)` | **✗** | avg-of-avgs ≠ avg; carry `(sum, count)` instead |
| `Exact(Min)` | ✓ | `min(min_A, min_B)` |
| `Exact(Max)` | ✓ | `max(max_A, max_B)` |
| `Hydra(inner, keys)` | ✓ iff inner is mergeable | merge corresponding inner sketches |

`Exact(Avg)` in a `Merge` context must be rewritten to `(Exact(Sum), Exact(Count))`
before the `Merge` node; the controller computes `avg = total_sum / total_count`.

---

## 6. Sketch Coverage Classification

After compilation and optimization, each `SketchExpr` is classified:

| Coverage | Meaning |
|---|---|
| `Full` | All aggregation columns mapped to mergeable sketches |
| `Partial` | Some columns sketch-mapped, others require exact passthrough (e.g. `MIN(URL)` alongside `COUNT(*)`) |
| `None` | No sketch applicable; query requires exact execution (`sum_over_time`, bare selector, binary PromQL op) |

`Partial` coverage is valid: the controller runs exact passthrough for the
non-sketch columns and sketch-merge for the rest.

---

## 7. ClickBench Case Studies

Concrete mappings for representative ClickBench queries.

| SQL | Coverage | `SketchExpr` summary |
|---|---|---|
| `SELECT COUNT(*) FROM hits` | None | `Exact(Count)` — global count, no sketch |
| `SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0` | Full | `Filter([AdvEngineID≠0], Agg(CountMin, Source(hits)))` |
| `SELECT SUM(x), COUNT(*), AVG(w) FROM hits` | None | no GROUP BY; all exact |
| `SELECT COUNT(DISTINCT UserID) FROM hits` | Full | `Agg(HLL(UserID), Source(hits))` |
| `SELECT MIN(EventDate), MAX(EventDate) FROM hits` | Full | `Agg(ExactMinMax, Source(hits))` |
| `SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC` | Full | `Partition([AdvEngineID], Filter([AdvEngineID≠0], Agg(CountMin, Source(hits))))` |
| `SELECT SearchPhrase, COUNT(*) AS c FROM hits GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10` | Full | `TopK(10, Partition([SearchPhrase], Agg(CountSketch(10), Source(hits))))` |
| `SELECT RegionID, COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10` | Full | `TopK(10, Partition([RegionID], Agg(Hydra(HLL,[RegionID]), Source(hits))))` |
| `SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10` | Full | `TopK(10, Partition([MobilePhone,MobilePhoneModel], Agg(Hydra(HLL,[MobilePhone,MobilePhoneModel]), Filter([MobilePhoneModel≠''], Source(hits)))))` |
| `SELECT RegionID, SUM(x), COUNT(*) AS c, AVG(w), COUNT(DISTINCT u) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10` | Partial | `TopK(10, Partition([RegionID], Merge([Exact(Sum,x), CountSketch(10,*), DDSketch([0.5],w), Hydra(HLL(u),[RegionID])])))` |
| `SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10` | Partial | sketch covers `COUNT(*)` → `CountSketch`; `MIN(URL)` → exact passthrough |

---

## 8. DEBS 2022 PromQL Case Studies

| PromQL | DEBS Query | `SketchAggOp` | `QueryHint` |
|---|---|---|---|
| `avg_over_time(financial.last_trade_price[5m]) by (symbol)` | Q1 EMA | `DDSketch([0.5])` | `DebsEma` |
| `topk(10, count_over_time(financial.last_trade_price[5m]) by (symbol))` | Q3 TopK | `CountSketch(10)` | `DebsTopK{k:10}` |
| `min_over_time(financial.last_trade_price[5m]) by (symbol)` | Q4 price stats | `DDSketch([0.0])` | `DebsPriceStats` |
| `max_over_time(financial.last_trade_price[5m]) by (symbol)` | Q4 price stats | `DDSketch([1.0])` | `DebsPriceStats` |
| `quantile_over_time(0.25, financial.last_trade_price[5m]) by (symbol)` | Q5/Q9 volatility | `DDSketch([0.25,0.75])` | `DebsVolatility` |
| `count(count_over_time(financial.last_trade_price[5m]) by (symbol))` | Q6 cardinality | `HLL` | `DebsCardinality` |
| `quantile_over_time(0.5, financial.last_trade_price[5m]) by (symbol)` | Q7 TWAP | `DDSketch([0.5])` | `DebsTwap` |
| `financial.last_trade_price{symbol="RDSA.NL"}` | Q10–12 RSI/MACD | — (exact) | `ExactRequired` |
