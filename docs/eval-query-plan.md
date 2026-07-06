# Evaluation query plan — the queries to run on the top-5 datasets

> **Audience:** anyone building or running the §6 evaluation. For *each of the five
> datasets* shortlisted in
> [`use-case-dataset-survey.md` §5b](use-case-dataset-survey.md), this doc writes down
> the **concrete queries we plan to run** — warm (sketch-answered) and cold (exact
> replay) — tagged to the routing **mode** (M1/M2), the **sketch families**, the **seven
> axes**, and the **§6 figure/claim** each query feeds
> ([`evaluation-plan-figures.md`](evaluation-plan-figures.md)).
>
> Companion to [`use-case-dataset-survey.md`](use-case-dataset-survey.md) (modes +
> warm-eligibility predicate, §0a; the top-5 + scale, §5b),
> [`pipeline-query-catalog.md`](pipeline-query-catalog.md) (what the pipeline can answer
> today) and [`sketch-algebra-query-mapping.md`](sketch-algebra-query-mapping.md)
> (query → sketch algebra).
>
> **Status:** query-plan reference, 2026-06-14. Datasets 1 (Google cluster) and 2
> (DEBS-2022) are **wired** (`datasets_eval/google_cluster/`, `datasets_eval/debs/`);
> 3–5 are the **build-out** — the queries below are the spec their harnesses implement.

---

## 0. Conventions — full evaluation = query × **every** supported sketch

**Full evaluation runs each warm query against *every* sketch that supports its kind**,
on identical data, and scores each independently — so every query is a **head-to-head
across sketch families** (the Fig 3c DDSketch-vs-KLL comparison, generalised to all
kinds and all datasets). A query of kind *K* expands to one run per sketch in the
"all supported sketches" column below:

| query kind | **all supported sketches (each run + scored)** | metric suffix per sketch | comparison reported |
|---|---|---|---|
| **quantile** (p50/p99/…) | **DDSketch**, **KLL** | `_q_ddsketch`, `_q_kll` | median vs tail rel-err, wire bytes (DD ≈ −26% wire, KLL wins median) |
| **topk / heavy-hitter** | **CountSketch-heap**, **CountMinSketch-heap** | `_topk_cs`, `_topk_cms` | recall@k, by-count vs by-value-sum, wire |
| **frequency** (point count of an item) | **CountMinSketch**, **CountSketch** | `_freq_cms`, `_freq_cs` | one-sided (CountMinSketch) vs unbiased (CountSketch) error |
| **cardinality** (distinct) | **HLL** *(only family)* | `_card_hll` | rel-err vs `1.04/√m` |
| **sum** | **Sum** *(lossless)* | `_sum` (bare metric) | exact (sanity) |
| **count** | **Count** *(lossless)* | bare metric | exact (sanity) |

Each warm query is the MetricsQL/PromQL + ground-truth record from
`datasets_eval/multisketch/queries-*.json`:

```json
{ "id": "...", "kind": "quantile|topk|frequency|count_unique|sum|count",
  "metricsql": "quantile_over_time(0.99, <metric>_q_<sketch>[300s])",
  "sketches": ["ddsketch", "kll"],
  "gt": { "op": "...", "metric": "...", "...": "..." } }
```

`<sketch>` is expanded over the `sketches` list at harness time → one scored run each.
The `gt` is computed from the **raw replay** (exact); we score `rel-err = |warm − gt| /
|gt|` against the joint envelope `ε_sk + ε_s + ε_cdm` (recall for topk). Default window =
**300 s tumbling**, wall-clock-anchored (the Fig 3c fix).

**In the per-dataset tables below**, the *sketches run* column lists the full set; the
metricsql shows the `_<sketch>` slot the harness expands. **Cold queries** are exact
point/range replay from the Gorilla/`intchunk` archive (M2 mandatory raw + rare M1
forensic). **M2 short-circuit:** a decision query `agg ⋛ τ` is answered from the warm
interval `[v−ε, v+ε]` — warm-only if `v+ε < τ` or `v−ε > τ`; only `τ ∈ [v−ε, v+ε]` drills
cold. Headline M2 metric = **warm-only-resolved fraction**, swept over `τ`, `ε`, and
*sketch family* (different ε per family → different short-circuit rate).

---

## 1. Google cluster 2019 — **M1** (observability/resource)  ✅ wired

Metrics: `gct_cpu_rate`, `gct_memory_usage`; series key `(machine, job, task/instance)`;
group labels `zone`/`cell`/`service`. Use case: fleet / per-cell resource SLO dashboards.

### Warm queries (each run on **all supported sketches**)

| id | metricsql (`_<sketch>` expanded) | kind | sketches run (each scored) | axes | feeds |
|---|---|---|---|---|---|
| `gct-cpu-p99` | `quantile_over_time(0.99, gct_cpu_rate_q_<sketch>[300s])` | quantile | **DDSketch, KLL** | 2,6 | Fig 3a, Fig 3c, Fig 7 |
| `gct-cpu-p50` | `quantile_over_time(0.50, gct_cpu_rate_q_<sketch>[300s])` | quantile | **DDSketch, KLL** | 2,6 | Fig 3a, Fig 3c |
| `gct-cpu-p99-by-cell` | `quantile_over_time(0.99, gct_cpu_rate_q_<sketch>[300s])` *(by `cell`)* | quantile | **DDSketch, KLL** | 2,4,6 | Fig 3, repeated-dashboard |
| `gct-topk-host` | `topk(10, sum by (machine) (gct_cpu_rate_topk_<sketch>))` | topk | **CountSketch-heap, CountMinSketch-heap** | 2,6 | Fig 3c topk recall (CountSketch-heap vs CountMinSketch-heap) |
| `gct-freq-service` | `count_over_time(gct_cpu_rate_freq_<sketch>{service="svc-000003"}[300s])` | frequency | **CountMinSketch, CountSketch** | 2,6 | one-sided vs unbiased freq |
| `gct-card-service` | `count(gct_cpu_rate_card_hll)` | cardinality | **HLL** | 2,6 | Fig 3c per-series distinct |
| `gct-sum-cpu` | `sum(gct_cpu_rate_sum)` | sum | **Sum** (exact) | 2 | Fig 3c exact check |
| `gct-sum-mem-by-zone` | `sum by (zone) (gct_memory_usage_sum)` | sum | **Sum** | 2,4 | Fig 3c |
| `gct-cpu-alert` | `quantile_over_time(0.99, gct_cpu_rate_q_<sketch>[300s]) > 0.8` | threshold | **DDSketch, KLL** | 2,3 | Fig 5 alert / repeated |

### Cold queries (rare forensic — disjoint)

| id | query | feeds |
|---|---|---|
| `gct-forensic-point` | exact CPU of `instance=X` at `t=2019-05-12T03:14Z` (range replay) | Fig 11 cold path (rare M1) |

---

## 2. DEBS-2022 — **M2** (finance/tick)  ✅ wired

Metric: `debs_last_price` (Gauge `financial.last_trade_price`, value = `last`), attrs
`symbol`/`exchange`/`sectype`; `debs_volume`. 5-min tumbling, Berlin wall-clock. Use
case: live VWAP / price-quantile dashboards + threshold alerts on a **skewed** symbol
fleet; MiFID audit / backtest on the *same* series → cold.

### Warm queries (each run on **all supported sketches**)

| id | metricsql (`_<sketch>` expanded) | kind | sketches run (each scored) | axes | feeds |
|---|---|---|---|---|---|
| `debs-price-p50` | `quantile_over_time(0.50, debs_last_price_q_<sketch>{symbol="ASML.NL"}[300s])` | quantile | **DDSketch, KLL** | 2,7 | Fig 3 accuracy (EMA/VWAP proxy) |
| `debs-price-p99` | `quantile_over_time(0.99, debs_last_price_q_<sketch>{symbol="ASML.NL"}[300s])` | quantile | **DDSketch, KLL** | 2,7 | Fig 3c per-family |
| `debs-vwap` | `sum(debs_last_price_sum * debs_volume_sum) / sum(debs_volume_sum)` *(per symbol)* | sum | **Sum** (exact) | 2,7 | VWAP exactness |
| `debs-roll-vol` | `sum_over_time(debs_volume_sum{symbol="ASML.NL"}[300s])` | sum | **Sum** | 2,4,7 | rolling-volume dashboard |
| `debs-topk-active` | `topk(10, sum by (symbol) (debs_ticks_topk_<sketch>))` | topk | **CountSketch-heap, CountMinSketch-heap** | 2,7 | most-active-symbol board |
| `debs-coord-sample` | per-symbol ingest under the ε-floor `p_i = 1/(1+ε²·rate_i)` (skew sweep) | sampling | **DDSketch, KLL** (under `p`) | 1,7 | **Fig 9** coordinated 32× |

### M2 short-circuit (decision queries — warm-first, cold on ambiguity; per sketch)

| id | decision query | sketches | resolves warm-only when | drills cold when |
|---|---|---|---|---|
| `debs-move-alert` | `quantile_over_time(0.99, debs_last_price_q_<sketch>{symbol=…}[300s]) > τ` | **DDSketch, KLL** | `v+ε < τ` or `v−ε > τ` | `τ ∈ [v−ε, v+ε]` |

### Cold queries (mandatory raw — cheap via edge Gorilla)

| id | query | feeds |
|---|---|---|
| `debs-audit-replay` | trade-by-trade exact replay for `(symbol, window)` (MiFID) | Fig 11 cold (finance-tick edge-Gorilla) |
| `debs-backtest` | exact tick series for `symbol` over the week | Mode-2 cold half |

---

## 3. Alibaba microservices 2021/2022 — **M1** (observability/traces)  ◻ to add

Metrics: `alibaba_ms_latency` (span duration, per `service`), `alibaba_ms_calls`,
distinct-caller stream `alibaba_ms_callers`. Use case: per-service p50/p99 latency SLO
alerting (RED) + distinct-caller cardinality; incident trace replay → cold. **Metric-
identity split:** the latency *metric* is warm; the raw span archive is a *separate*
artifact → cold (condition 2 provable).

### Warm queries (each run on **all supported sketches**)

| id | metricsql (`_<sketch>` expanded) | kind | sketches run (each scored) | axes | feeds |
|---|---|---|---|---|---|
| `ms-lat-p99` | `quantile_over_time(0.99, alibaba_ms_latency_q_<sketch>{service="S"}[300s])` | quantile | **DDSketch, KLL** | 2,6,7 | **Fig 7** `quantile_over_time` at scale |
| `ms-lat-p50` | `quantile_over_time(0.50, alibaba_ms_latency_q_<sketch>{service="S"}[300s])` | quantile | **DDSketch, KLL** | 2,6,7 | Fig 3c per-family |
| `ms-callers-card` | `count(alibaba_ms_callers_card_hll{service="S"})` | cardinality | **HLL** | 2,6 | distinct-caller fan-in |
| `ms-req-rate` | `sum by (service) (rate(alibaba_ms_calls[300s]))` | count | **Count** (exact) | 2,4,6 | RED rate dashboard |
| `ms-topk-slow` | `topk(10, sum by (service) (alibaba_ms_latency_topk_<sketch>))` | topk | **CountSketch-heap, CountMinSketch-heap** | 2,6 | slowest-service board |
| `ms-lat-slo-alert` | `quantile_over_time(0.99, alibaba_ms_latency_q_<sketch>{service="S"}[300s]) > 0.5` | threshold | **DDSketch, KLL** | 2,3 | Fig 5 / repeated SLO |

### Cold queries (separate span artifact — disjoint)

| id | query | feeds |
|---|---|---|
| `ms-trace-replay` | exact span tree for `trace_id=…` (incident forensics) | Fig 11 cold (the half anchors lack) |

---

## 4. Azure VM 2019 — **M1** (observability/resource)  ◻ to add

Metrics: `azure_vm_cpu` (per-VM CPU, 5-min); cold counter `azure_vm_billing`
(**separate** series). ~2.6 M VMs = ~2.6 M series. Use case: per-VM CPU capacity/SLO
dashboards at fleet scale; billing/chargeback → cold. Textbook metric-identity split;
stresses the controller-allocation figure.

### Warm queries (each run on **all supported sketches**)

| id | metricsql (`_<sketch>` expanded) | kind | sketches run (each scored) | axes | feeds |
|---|---|---|---|---|---|
| `vm-cpu-p99` | `quantile_over_time(0.99, azure_vm_cpu_q_<sketch>[300s])` | quantile | **DDSketch, KLL** | 2,6 | **Fig 12** alloc @ ~2.6 M cardinality |
| `vm-cpu-p50` | `quantile_over_time(0.50, azure_vm_cpu_q_<sketch>[300s])` | quantile | **DDSketch, KLL** | 2,6 | Fig 3c per-family |
| `vm-cpu-p99-by-sub` | `quantile_over_time(0.99, azure_vm_cpu_q_<sketch>[300s])` *(by `subscription`)* | quantile | **DDSketch, KLL** | 2,4,6 | Fig 12 / repeated |
| `vm-card-vms` | `count(azure_vm_cpu_card_hll)` | cardinality | **HLL** | 2,6 | fleet-size distinct |
| `vm-avg-util-by-sub` | `sum by (subscription) (azure_vm_cpu_sum) / count by (subscription) (azure_vm_cpu)` | sum/count | **Sum, Count** | 2,4,6 | capacity dashboard |
| `vm-cpu-alert` | `quantile_over_time(0.99, azure_vm_cpu_q_<sketch>[300s]) > 0.9` | threshold | **DDSketch, KLL** | 2,3 | Fig 5 / repeated |

### Cold queries (separate billing series — disjoint)

| id | query | feeds |
|---|---|---|
| `vm-billing-replay` | exact per-VM billing counter for `(vm, billing_period)` (dispute-grade) | Fig 11 cold (separate series) |

---

## 5. Binance/Kraken crypto tick — **M2** (finance/tick)  ◻ to add

Metrics: `crypto_trade_price`, `crypto_trade_vol` (per `pair`). Hundreds of pairs, 24/7,
very-high per-series frequency. Use case: live crypto VWAP/quantile alerts + "is this
pair behaving oddly?" triage; strategy **backtest** replays raw on the *same* series →
cold. The Mode-2 / short-circuit showcase + the two orthogonal edge compressions.

### Warm queries (each run on **all supported sketches**)

| id | metricsql (`_<sketch>` expanded) | kind | sketches run (each scored) | axes | feeds |
|---|---|---|---|---|---|
| `cx-price-p50` | `quantile_over_time(0.50, crypto_trade_price_q_<sketch>{pair="BTCUSDT"}[300s])` | quantile | **DDSketch, KLL** | 2,7 | Fig 3 accuracy (high-freq) |
| `cx-price-p99` | `quantile_over_time(0.99, crypto_trade_price_q_<sketch>{pair="BTCUSDT"}[300s])` | quantile | **DDSketch, KLL** | 2,7 | Fig 3c per-family |
| `cx-vwap` | `sum(crypto_trade_price_sum * crypto_trade_vol_sum) / sum(crypto_trade_vol_sum)` *(per pair)* | sum | **Sum** (exact) | 2,7 | VWAP exactness |
| `cx-roll-vol` | `sum_over_time(crypto_trade_vol_sum{pair="BTCUSDT"}[300s])` | sum | **Sum** | 2,4,7 | rolling-volume board |
| `cx-topk-active` | `topk(10, sum by (pair) (crypto_trades_topk_<sketch>))` | topk | **CountSketch-heap, CountMinSketch-heap** | 2,7 | most-active-pair board |

### M2 short-circuit (the headline metric for this dataset; per sketch)

| id | decision query | sketches | resolves warm-only when | drills cold when |
|---|---|---|---|---|
| `cx-vol-alert` | `quantile_over_time(0.99, crypto_trade_price_q_<sketch>{pair=…}[300s]) > τ` | **DDSketch, KLL** | `v+ε < τ` or `v−ε > τ` | `τ ∈ [v−ε, v+ε]` |
| `cx-anomaly-triage` | "which pair/window is anomalous?" → warm screen, then drill | **DDSketch, KLL** | warm screen settles it | flagged window → cold |

Sweep `τ`, `ε`, **and sketch family** → report the **warm-only-resolved fraction** vs
forced-to-cold (the bound-based short-circuit headline) and the **cold-IO pruning ratio**
— DDSketch's relative-error bound vs KLL's rank-error bound give *different* ambiguous
bands, so the short-circuit rate is itself a per-family result.

### Cold queries (mandatory raw — cheap via edge Gorilla)

| id | query | feeds |
|---|---|---|
| `cx-backtest` | exact tick series for `pair` over N months (strategy replay) | Mode-2 cold half; edge-Gorilla cheap-cold |

---

## 6. What each dataset's query set proves (coverage)

| dataset | mode | warm query kinds × sketches | cold query | headline figure/claim |
|---|---|---|---|---|
| Google cluster 2019 | M1 | quantile {DD,KLL}, topk {CS-heap, CMS-heap}, freq {CMS, CS}, card {HLL}, sum, threshold | rare forensic point | Fig 3 accuracy, Fig 7 latency, Pareto |
| DEBS-2022 | M2 | quantile {DD,KLL}, VWAP {Sum}, topk {CS-heap, CMS-heap}, **coordinated sampling** | audit/backtest replay | **Fig 9** 32× sampling, edge-Gorilla cold |
| Alibaba microservices | M1 | quantile {DD,KLL} @scale, card {HLL}, rate {Count}, topk {CS-heap, CMS-heap} | trace replay (separate span) | high-card disjoint (Fig 11), Fig 7 |
| Azure VM 2019 | M1 | quantile {DD,KLL} @~2.6 M, card {HLL}, sum/count, threshold | billing replay (separate series) | **Fig 12** controller alloc |
| Crypto tick | M2 | quantile {DD,KLL}, VWAP {Sum}, topk {CS-heap, CMS-heap}, **short-circuit** | backtest replay | **bound-based short-circuit** per family |

**Read:** every dataset runs the **same query kinds against the same full sketch sets**
(quantile → DDSketch + KLL, topk → CountSketch-heap + CountMinSketch-heap, frequency → CountMinSketch +
CountSketch, cardinality → HLL, sum/count → lossless) — so each query is a per-dataset
**head-to-head across families** on identical real data. What differs is the **mode**
(M1 disjoint vs M2 co-resident) and therefore whether the cold query is a *rare disjoint
forensic* (M1) or a *first-class, frequently-paired exact replay* (M2); the two M2 sets
additionally run the **short-circuit decision protocol**, whose warm-only-resolved
fraction is itself reported **per sketch family** (different ε ⇒ different ambiguous band).

---

## 7. References

- [`use-case-dataset-survey.md`](use-case-dataset-survey.md) — modes + predicate (§0a), top-5 + scale (§5b).
- [`evaluation-plan-figures.md`](evaluation-plan-figures.md) — Fig 3/5/7/9/11/12, the joint bound.
- [`pipeline-query-catalog.md`](pipeline-query-catalog.md) — what the pipeline answers today.
- [`sketch-algebra-query-mapping.md`](sketch-algebra-query-mapping.md) — query → sketch algebra.
- `datasets_eval/multisketch/queries-*.json` — the warm-query record format reused here.
- `datasets_eval/debs/DEBS_2022/02_benchmark_queries.md` — the DEBS Q1–Q13 benchmark queries.

---

*End of query plan.*
