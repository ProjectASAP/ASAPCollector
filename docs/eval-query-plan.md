# Evaluation query plan — the queries to run on the top-5 datasets

> **Audience:** anyone building or running the §6 evaluation. For *each of the five
> datasets* shortlisted in
> [`use-case-dataset-survey.md` §5b](use-case-dataset-survey.md), this doc writes down
> the **concrete queries we plan to run** — warm (sketch-answered) and cold (exact
> replay) — tagged to the routing **mode** (M1/M2), the **sketch family**, the **seven
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

## 0. Conventions — how a query is expressed and scored

**Warm queries** are MetricsQL/PromQL over the sketch-typed metrics the edge emits,
exactly as in `datasets_eval/multisketch/queries-*.json` — each query is a record:

```json
{ "id": "...", "kind": "quantile|topk|count_unique|frequency|sum|count",
  "metricsql": "quantile_over_time(0.99, <metric>[300s])",
  "gt": { "op": "...", "metric": "...", "...": "..." } }
```

The `gt` (ground truth) is computed from the **raw replay** (exact), and we score
`rel-err = |warm − gt| / |gt|` against the joint envelope `ε_sk + ε_s + ε_cdm`
(recall for topk). Default window = **300 s tumbling**, wall-clock-anchored (the Fig 3c
fix). Sketch family per query kind:

| query kind | sketch family | metric suffix used below |
|---|---|---|
| quantile (p50/p99) | **DDSketch / KLL** | `_q_ddsketch` / `_q_kll` |
| topk / heavy-hitter | **CountSketch / CMS-heap** | `_topk_cs` / `_topk_cms` |
| cardinality (distinct) | **HLL** | `_card_hll` |
| frequency (point) | **Count-Min** | `_freq_cms` |
| sum / count | **Sum / Count** (lossless) | (bare metric) |

**Cold queries** are exact point/range replay served from the Gorilla/`intchunk`
archive tier (lossless). They appear for **M2** datasets (mandatory raw: audit /
backtest / forensic) and as the **rare M1 forensic** point-lookup.

**M2 bound-based short-circuit** (§0a of the survey). A *decision* query `agg ⋛ τ` is
answered from the warm interval `[v−ε, v+ε]`: return warm if `v+ε < τ` (certainly below)
or `v−ε > τ` (certainly above); only the **ambiguous band** `[τ−ε, τ+ε]` drills into
cold. The headline M2 metric is the **warm-only-resolved fraction** — share of decision
queries answered without touching cold — swept over `τ` and `ε`.

---

## 1. Google cluster 2019 — **M1** (observability/resource)  ✅ wired

Metrics: `gct_cpu_rate`, `gct_memory_usage`; series key `(machine, job, task/instance)`;
group labels `zone`/`cell`/`service`. Use case: fleet / per-cell resource SLO dashboards.

### Warm queries (the answer — raw never wanted)

| id | metricsql | kind / family | axes | feeds |
|---|---|---|---|---|
| `gct-cpu-p99` | `quantile_over_time(0.99, gct_cpu_rate_q_ddsketch[300s])` | quantile / DDSketch | 2,6 | Fig 3a accuracy, Fig 7 latency |
| `gct-cpu-p50` | `quantile_over_time(0.50, gct_cpu_rate_q_kll[300s])` | quantile / KLL | 2,6 | Fig 3a, Fig 3c per-family |
| `gct-cpu-p99-by-cell` | `quantile_over_time(0.99, gct_cpu_rate_q_ddsketch[300s])` *(grouped by `cell`)* | quantile / DDSketch | 2,4,6 | Fig 3, repeated-dashboard |
| `gct-sum-cpu` | `sum(gct_cpu_rate)` | sum / Sum (exact) | 2 | Fig 3c (exact check) |
| `gct-sum-mem-by-zone` | `sum by (zone) (gct_memory_usage)` | sum / Sum | 2,4 | Fig 3c |
| `gct-topk-host` | `topk(10, sum by (machine) (gct_cpu_rate_topk_cs))` | topk / CountSketch | 2,6 | Fig 3c topk recall |
| `gct-topk-host-cms` | `topk(10, sum by (machine) (gct_cpu_rate_topk_cms))` | topk / CMS-heap | 2,6 | Fig 3c (CMS-heap vs CS) |
| `gct-card-service` | `count(gct_cpu_rate_card_hll)` | cardinality / HLL | 2,6 | Fig 3c per-series distinct |
| `gct-cpu-alert` | `quantile_over_time(0.99, gct_cpu_rate_q_ddsketch[300s]) > 0.8` | threshold (repeated) | 2,3 | Fig 5 alert / repeated |

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

### Warm queries (fast alert / dashboard path)

| id | metricsql | kind / family | axes | feeds |
|---|---|---|---|---|
| `debs-price-p50` | `quantile_over_time(0.50, debs_last_price_q_ddsketch{symbol="ASML.NL"}[300s])` | quantile / DDSketch | 2,7 | Fig 3 accuracy (EMA/VWAP proxy) |
| `debs-price-p99` | `quantile_over_time(0.99, debs_last_price_q_kll{symbol="ASML.NL"}[300s])` | quantile / KLL | 2,7 | Fig 3c per-family |
| `debs-vwap` | `sum(debs_last_price * debs_volume) / sum(debs_volume)` *(per symbol)* | sum / Sum (exact) | 2,7 | VWAP exactness |
| `debs-roll-vol` | `sum_over_time(debs_volume{symbol="ASML.NL"}[300s])` | sum / Sum | 2,4,7 | rolling-volume dashboard |
| `debs-topk-active` | `topk(10, sum by (symbol) (debs_ticks_topk_cs))` | topk / CountSketch | 2,7 | most-active-symbol board |
| `debs-coord-sample` | per-symbol ingest under `p_i ∝ √(f_i/rate_i)` (skew sweep) | sampling | 1,7 | **Fig 9** coordinated 32× |

### M2 short-circuit (decision queries — warm-first, cold on ambiguity)

| id | decision query | resolves warm-only when | drills cold when |
|---|---|---|---|
| `debs-move-alert` | `quantile_over_time(0.99, debs_last_price_q_ddsketch{symbol=…}[300s]) > τ` | `v+ε < τ` or `v−ε > τ` | `τ ∈ [v−ε, v+ε]` |

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

### Warm queries

| id | metricsql | kind / family | axes | feeds |
|---|---|---|---|---|
| `ms-lat-p99` | `quantile_over_time(0.99, alibaba_ms_latency_q_ddsketch{service="S"}[300s])` | quantile / DDSketch | 2,6,7 | **Fig 7** `quantile_over_time` at scale |
| `ms-lat-p50` | `quantile_over_time(0.50, alibaba_ms_latency_q_kll{service="S"}[300s])` | quantile / KLL | 2,6,7 | Fig 3c per-family |
| `ms-callers-card` | `count(alibaba_ms_callers_card_hll{service="S"})` | cardinality / HLL | 2,6 | distinct-caller fan-in (HLL) |
| `ms-req-rate` | `sum by (service) (rate(alibaba_ms_calls[300s]))` | count / Count | 2,4,6 | RED rate dashboard |
| `ms-topk-slow` | `topk(10, sum by (service) (alibaba_ms_latency_topk_cs))` | topk / CountSketch | 2,6 | slowest-service board |
| `ms-lat-slo-alert` | `quantile_over_time(0.99, alibaba_ms_latency_q_ddsketch{service="S"}[300s]) > 0.5` | threshold (repeated) | 2,3 | Fig 5 / repeated SLO |

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

### Warm queries

| id | metricsql | kind / family | axes | feeds |
|---|---|---|---|---|
| `vm-cpu-p99` | `quantile_over_time(0.99, azure_vm_cpu_q_ddsketch[300s])` | quantile / DDSketch | 2,6 | **Fig 12** alloc @ ~2.6 M cardinality |
| `vm-cpu-p99-by-sub` | `quantile_over_time(0.99, azure_vm_cpu_q_ddsketch[300s])` *(by `subscription`)* | quantile / DDSketch | 2,4,6 | Fig 12 / repeated |
| `vm-card-vms` | `count(azure_vm_cpu_card_hll)` | cardinality / HLL | 2,6 | fleet-size distinct (HLL) |
| `vm-avg-util-by-sub` | `sum by (subscription) (azure_vm_cpu) / count by (subscription) (azure_vm_cpu)` | sum/count | 2,4,6 | capacity dashboard |
| `vm-cpu-alert` | `quantile_over_time(0.99, azure_vm_cpu_q_ddsketch[300s]) > 0.9` | threshold (repeated) | 2,3 | Fig 5 / repeated |

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

### Warm queries (fast alert / triage path)

| id | metricsql | kind / family | axes | feeds |
|---|---|---|---|---|
| `cx-price-p50` | `quantile_over_time(0.50, crypto_trade_price_q_ddsketch{pair="BTCUSDT"}[300s])` | quantile / DDSketch | 2,7 | Fig 3 accuracy (high-freq) |
| `cx-price-p99` | `quantile_over_time(0.99, crypto_trade_price_q_kll{pair="BTCUSDT"}[300s])` | quantile / KLL | 2,7 | Fig 3c per-family |
| `cx-vwap` | `sum(crypto_trade_price * crypto_trade_vol) / sum(crypto_trade_vol)` *(per pair)* | sum / Sum (exact) | 2,7 | VWAP exactness |
| `cx-roll-vol` | `sum_over_time(crypto_trade_vol{pair="BTCUSDT"}[300s])` | sum / Sum | 2,4,7 | rolling-volume board |
| `cx-topk-active` | `topk(10, sum by (pair) (crypto_trades_topk_cs))` | topk / CountSketch | 2,7 | most-active-pair board |

### M2 short-circuit (the headline metric for this dataset)

| id | decision query | resolves warm-only when | drills cold when |
|---|---|---|---|
| `cx-vol-alert` | `quantile_over_time(0.99, crypto_trade_price_q_ddsketch{pair=…}[300s]) > τ` | `v+ε < τ` or `v−ε > τ` | `τ ∈ [v−ε, v+ε]` |
| `cx-anomaly-triage` | "which pair/window is anomalous?" → warm screen, then drill | warm screen settles it | flagged window → cold |

Sweep `τ` and `ε` → report the **warm-only-resolved fraction** vs forced-to-cold (the
bound-based short-circuit headline), and the **cold-IO pruning ratio**.

### Cold queries (mandatory raw — cheap via edge Gorilla)

| id | query | feeds |
|---|---|---|
| `cx-backtest` | exact tick series for `pair` over N months (strategy replay) | Mode-2 cold half; edge-Gorilla cheap-cold |

---

## 6. What each dataset's query set proves (coverage)

| dataset | mode | warm query families exercised | cold query | headline figure/claim |
|---|---|---|---|---|
| Google cluster 2019 | M1 | quantile, sum, topk, cardinality, threshold | rare forensic point | Fig 3 accuracy, Fig 7 latency, Pareto |
| DEBS-2022 | M2 | quantile, VWAP-sum, topk, **coordinated sampling** | audit/backtest replay | **Fig 9** 32× sampling, finance-tick edge-Gorilla cold |
| Alibaba microservices | M1 | quantile (@scale), HLL cardinality, rate, topk | trace replay (separate span) | high-card disjoint (Fig 11), Fig 7 quantile path |
| Azure VM 2019 | M1 | quantile (@~2.6 M), HLL, sum/count, threshold | billing replay (separate series) | **Fig 12** controller alloc at fleet cardinality |
| Crypto tick | M2 | quantile, VWAP-sum, topk, **short-circuit decisions** | backtest replay | **bound-based short-circuit** + two-axis edge compression |

**Read:** the warm query *families* are the same five everywhere (quantile / topk /
cardinality / sum / threshold) — what differs is the **mode** (M1 disjoint vs M2
co-resident) and therefore whether the cold query is a *rare disjoint forensic* (M1) or a
*first-class, frequently-paired exact replay* (M2). The two M2 sets additionally run the
**short-circuit decision protocol** (warm-only-resolved fraction), which the M1 sets do
not need.

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
