# Use-case & dataset survey — warm-sketch tier vs cold-archive tier

<!-- Design metadata -->

## TL;DR

Survey used to select workloads and datasets for warm-summary and cold-archive evaluation.

**Status:** draft

**MVP relationship:** supporting.

This document is design-level: it defines scope, behavior, constraints, and trade-offs; implementation details are intentionally out of scope.


> **Audience:** anyone choosing public workloads to back ASAP's §6 evaluation,
> or sanity-checking that the **disjoint warm/cold routing** story matches a real
> use case.
> [`paper-outline.md`](paper-outline.md),
> and [`distributed-nitrosketch-coordinated-sampling.md`](distributed-nitrosketch-coordinated-sampling.md).
>
> **Status:** survey / dataset-selection reference, 2026-06-12.

---

## 0. The story this survey validates (terminology pinned to the repo)

ASAP routes **every series disjointly** into exactly one of two tiers — never both
(the archive-tier design):

- **Warm sketch tier** — `(ε, δ)`-bounded approximate state (DDSketch / KLL /
  CountMinSketch / CountSketch / HLL / Sum). **Lossy within a bounded ε; the per-series
  raw sample stream is discarded.** Good for high-volume aggregation / quantile /
  topk / cardinality queries amortised over a window. Bandwidth-efficient on the
  wire (sketch envelopes ~10–100× smaller than raw); sampling (`p`) and CDM ε-gated
  delta emission apply **only here**.
- **Cold archive tier** — Gorilla-XOR (TSDB blocks) / `intchunk` best-of-N codec.
  **Lossless, raw samples preserved**, cold-IO-class latency. The price of
  exact / historical / forensic / regulator-visible replay.

> *"A series is sketched **xor** cold-archived, so a dropped warm sample truly has
> no other consumer"* — [`distributed-nitrosketch-coordinated-sampling.md`](distributed-nitrosketch-coordinated-sampling.md).

The **controller allocates the partition from the query workload** (Fig 12,
the archive-tier mode-selection design): a series is
warm if every query on it is approximate / aggregate (quantile / topk / cardinality /
sum) **and** the cost model says sketch beats raw; **cold** if **any** query on it needs
exact / historical replay. Total-resource cost on the Pareto is therefore **additive**:
`warm-half cost + cold-half cost`, partitioned across series (Fig 11(d)).

We already evaluate on two **anchor** datasets:
- **Google cluster trace 2019** — resource / high-cardinality → accuracy, Pareto,
  cardinality / sum (`datasets_eval/google_cluster/`).
- **DEBS-2022 (Deutsche Börse / Infront tick data)** — financial / skewed activity →
  coordinated sampling, topk, the ε-gate / delta regime (`datasets_eval/debs/`).

Beyond the two anchor *domains* (cloud observability, finance) this survey adds a
**third domain — product analytics / clickstream** (§3b): the domain where
approximate aggregates (DAU/MAU via HLL, top events via CountSketch/CMS) are already
the industry default, and whose **GDPR/CCPA right-to-erasure** is the most universal —
and *publicly-downloadable* — cold-raw motivation in the survey.

This survey verifies those two and finds **more**, mapping each to the warm/cold split
and to the user's seven query/data axes.

### The seven axes (column legend for the matrix)

1. **Large data volume** — high total ingest (events/s × series).
2. **Aggregation queries** — quantile / sum / count / topk / cardinality (the warm-tier sweet spot).
3. **Repeated queries** — the same query re-issued (dashboards, scheduled SLO checks).
4. **Overlapping queries** — sliding/rolling windows that share sub-ranges.
5. **Long-lookback queries** — queries reaching far back (historical / backtest / forensic).
6. **High-cardinality** — many distinct series / active keys.
7. **High-frequency-per-series** — many samples/s on a single series.

---

## 0a. Two routing modes & the warm-eligibility predicate

**Warm-eligibility predicate (when may a series be sketched at all).** A series is a
*clean* warm candidate only when **both** hold:

1. **Predictable queries** — every query on it is predefined and highly repeated
   (standing alerting rules, scheduled SLO checks, fixed dashboards), so the sketch
   type / size / freshness can be provisioned ahead and amortised over the repeats
   (axes 2/3/4).
2. **No latent raw need** — no future query on *that same series* will ever need
   exact / historical / forensic replay. The raw stream is discarded once sketched, so
   this must be *provable*, not hoped for.

Condition 2 is the sharp one. The safest way to satisfy it is **metric-identity
separation**: the warm series and the raw-needing series are *physically different
metrics* (per-VM CPU → warm **vs** the per-VM billing counter → cold; latency-quantile
metric → warm **vs** the raw span → cold). When the *same* series carries both an
aggregate query and a raw claim — a price series queried live for VWAP **and** replayed
raw for backtest — condition 2 fails and the series **cannot go cleanly warm**. That
failure is exactly what **Mode 2** exists for.

### Mode 1 — disjoint (warm xor cold) — the storage / bandwidth play

The base story (Fig 11): each series lands in **exactly one** tier. Because a
warm-sampled drop "has no other consumer", sampling (`p`) and CDM ε-gating can shed
data, and total cost is **additive** (`warm-half + cold-half`). **Win: storage +
bandwidth** (raw is never stored for warm series). **Requires the full predicate
(both conditions).**

### Mode 2 — co-resident (warm **and** cold) — the latency / IO-pruning play

A series keeps an **authoritative cold raw** copy **and** a **warm sketch summary** on
top. The cold raw is mandatory anyway (backtest / audit / forensic); the warm sketch is
a **lossy accelerator** over it. **A different value proposition from Mode 1:**

- **Does *not* save storage or bandwidth** — raw still ships in full to cold and is
  stored losslessly; sampling / CDM can no longer drop it. The warm sketch is *added*
  cost (small). **But "full lossless" ≠ "expensive":** on **high-frequency,
  low-cardinality** streams (finance tick) the cold raw is a dense single-series time
  stream that **edge Gorilla-XOR / `intchunk` time-dimension compression crushes**
  (~1.3–3.5 B/sample, Fig 11), so the cold-ship cost is small in absolute terms and the
  bandwidth caveat is largely neutralised — Mode 2 is **cheapest exactly where the
  warm-eligibility predicate fails** (see §1 finance-tick note).
- **Saves query latency + compute + cold IO** — repeated aggregate queries answer from
  the pre-aggregated sketch (~18 ms, Fig 7) instead of re-scanning cold blocks; the warm
  tier acts as a **materialised approximate view + a triage / drill-down filter**.

**The core optimisation — bound-based short-circuit.** The warm sketch answers with a
*bounded* interval `[v−ε, v+ε]`; the planner uses the bound to decide whether cold is
needed *at all*:

- decision query `agg ⋛ τ` (e.g. alerting `p99 > τ`): if `v+ε < τ` → **certainly
  below**, return warm; if `v−ε > τ` → **certainly above**, fire from warm; only the
  **ambiguous band** `[τ−ε, τ+ε]` falls through to an exact cold read.
- Most queries sit far from `τ` → cold is touched for a **small fraction** of queries →
  large cold-IO pruning, and the short-circuit is **provably correct** (ε is bounded).

**Stacking optimisations:**
- **warm-as-skip-index** — keep a per-cold-block sketch digest (quantile / min-max / HLL);
  when a drill-down *is* needed, read only the blocks whose digest overlaps the predicate
  (data-skipping / zone-map, sketch-grade).
- **two-phase answer** — return the warm approximate value immediately (dashboard first
  paint), refine from cold in the background when exactness is requested.
- **one-sided sketches for one-sided decisions** — pick a sketch whose error direction
  matches the decision (CountMinSketch is one-sided) so warm can *certify* "definitely below/above"
  without cold.
- **sampling still allowed on the warm copy** — `p` widens `ε_s`, which only widens the
  ambiguous band (more drill-downs): a tunable edge-cost ↔ cold-read trade (the cold raw
  is never sampled).

### Adaptive controller — Mode 2 as a safe default that *collapses* to Mode 1

Log each series' **cold hit-rate**. A Mode-2 series whose cold copy is *never* consulted
is empirically condition-2-clean → **demote to Mode 1 disjoint-warm** (drop the raw,
reclaim storage). A series whose warm bound *constantly* forces cold reads → **promote
to cold-primary** (drop the wasted sketch). So warm / cold / both becomes
**observed-behaviour-driven**, extending the Fig 12 allocator beyond the static
query-set parse.

### Which mode each dataset wants

| series pattern | mode | examples |
|---|---|---|
| aggregate query only, raw provably unused (metric-identity separated) | **Mode 1** | Azure VM **CPU**, Google instance CPU, Azure Functions **duration**, Alibaba per-service **latency metric**, **product-analytics DAU/topk rollups** (vs the raw event log) |
| raw **mandatory anyway** (backtest / audit / forensic) **and** common queries approximate | **Mode 2** | **finance tick** (live VWAP/q + backtest replay), DEBS / TAQ (dashboard + MiFID audit), Wikimedia (topk + forensic), **product analytics** (dashboard + **GDPR/CCPA** export-erasure of the same user series) |
| raw needed, queries rarely aggregate | cold-primary | LOBSTER event-exact microstructure |

**Mode 2 rescues the "condition-2 failures".** The datasets that *can't* go cleanly warm
(finance tick / regulated / forensic) are exactly the ones where cold is mandatory — so
the warm sketch is **pure upside** (its only cost is the small sketch storage + compute),
serving fast approximate dashboards / alerts and **triaging which narrow slice of cold to
replay**. Conversely, on a clean Mode-1 dataset (Azure VM CPU) Mode 2 would *waste*
storage by keeping raw nobody reads.

---

## 1. Summary matrix — datasets × the seven axes

✓ = strongly exercises it · ~ = partially / conditionally · ✗ = not really.
**Tier** = where the *bulk* of series land under our story (most datasets are mixed;
the per-dataset cards give the split).
**Mode** (§0a) = **M1** disjoint warm xor cold (clean warm by metric-identity
separation — the storage/bandwidth play) · **M2** co-resident warm+cold
(raw mandatory anyway; warm is an accelerator + drill-down triage — the latency/IO-prune
play) · *cold-lean* = M2 dominated by the exact-replay side.

| # | Dataset | Domain | 1 Vol | 2 Agg | 3 Rep | 4 Ovlp | 5 Long | 6 Card | 7 Freq | Dominant tier | Mode |
|---|---|---|---|---|---|---|---|---|---|---|---|
| **A1** | **Google cluster 2019** (anchor) | observability/resource | ✓ | ✓ | ✓ | ✓ | ~ | ✓ | ~ (5-min) | **warm** (resource quantiles) + cold (audit) | **M1** |
| A2 | Alibaba cluster 2018 | observability/resource | ✓ | ✓ | ✓ | ✓ | ~ | ✓ | ~ (10–300s) | warm + cold | **M1** |
| A3 | Alibaba microservices 2021/2022 | observability/traces | ✓ | ✓ | ✓ | ✓ | ✓ | ✓✓ | ✓ | **warm** (trace latency q) + cold (trace replay) | **M1** |
| A4 | Azure VM trace 2017/2019 | observability/resource | ✓ | ✓ | ✓ | ✓ | ~ | ✓✓ | ~ (5-min) | **warm** (fleet quantiles) + cold (billing) | **M1** |
| A5 | Azure Functions 2019 | observability/metrics | ✓ | ✓ | ✓ | ✓ | ~ | ✓✓ | ✓ (per-min invokes) | **warm** + cold (billing) | **M1** |
| A6 | OpenTelemetry Demo / synthetic | observability/all | ~ | ✓ | ✓ | ✓ | ✗ | ~ | ~ | warm (controllable) | **M1** |
| A7 | Wikimedia pageviews/webrequest | observability/CDN | ✓ | ✓ | ✓ | ✓ | ✓ | ✓✓ | ✓ | **warm** (topk/HLL) + cold (forensic) | **M2** |
| **F1** | **DEBS-2022 Deutsche Börse** (anchor) | finance/tick | ✓ | ✓ | ✓ | ✓ | ~ | ~ (5.5k sym) | ✓ (skewed) | **warm** (VWAP/q) + cold (audit) | **M2** |
| F2 | LOBSTER (NASDAQ LOB) | finance/order book | ✓ | ✓ | ~ | ✓ | ✓ | ~ | ✓✓ | **cold** (event-exact) + warm (depth q) | **M2** *(cold-lean)* |
| F3 | NYSE Daily TAQ | finance/trades+quotes | ✓✓ | ✓ | ✓ | ✓ | ✓ | ✓✓ | ✓✓ | **cold** (MiFID/SEC audit) + warm (VWAP) | **M2** |
| F4 | Deutsche Börse PDS (Xetra/Eurex) | finance/OHLCV 1-min | ~ | ✓ | ✓ | ✓ | ✓ | ✓ | ✗ (pre-agg) | warm (already aggregated) | **M1** *(warm-only check)* |
| F5 | Binance/Kraken/Coinbase tick | finance/crypto tick | ✓ | ✓ | ✓ | ✓ | ✓ | ~ | ✓✓ | **warm** (q/VWAP) + cold (backtest replay) | **M2** |
| **P1** | **Taobao UserBehavior 2017** | product-analytics/clickstream | ✓ | ✓ | ✓ | ✓ | ~ | ✓✓ | ✗ (sparse/user) | **warm** (DAU-HLL / topk) + cold (GDPR / ML-feature raw) | **M1+M2** |
| P2 | REES46 eCommerce 2019 | product-analytics/clickstream | ✓ | ✓ | ✓ | ✓ | ~ | ✓✓ | ✗ | **warm** (funnel / topk / HLL) + cold (export / audit) | **M2** |
| P3 | Wikipedia clickstream | product-analytics/web | ~ | ✓ | ✓ | ✓ | ✓ | ✓✓ | ✗ (pre-agg) | warm (topk referrers) | **M1** *(warm-only check)* |

**Headline read of the matrix:** the user's axes split cleanly along the tier line —
**(2) aggregation, (3) repeated, (4) overlapping, (6) high-cardinality, (7)
high-frequency** are exactly the warm-tier sweet spot, while **(5) long-lookback** is
the axis that pulls series **cold** (exact/historical replay). **(1) large volume** is
necessary for either tier to matter. No single public dataset maxes every axis at
once — see §4 honest gaps.

**Finance tick is the high-frequency / low-cardinality corner — which makes it the
*ideal* Mode 2 fit.** Tick streams (F1/F3/F5) are **high per-series frequency (axis 7
✓✓)** but **bounded cardinality (axis 6 ~)** — series key = symbol, so hundreds (crypto)
to a few thousand (DEBS ~5.5k) keys, *not* the millions-of-series fleet scale of resource
traces (A4 ✓✓). Only TAQ reaches high cardinality, and only by counting venue×symbol
quote series (and it's access-gated). That corner is exactly where **Mode 2 is cheap on
both halves**:
- **cold half (time axis, lossless):** the mandatory cold-raw copy is a *dense,
  high-frequency single-series time stream* — the **best case for edge-side Gorilla-XOR /
  `intchunk` time-dimension compression** (Fig 11: ~1.3–3.5 B/sample on smooth /
  fixed-decimal data), and **low cardinality ⇒ few open chunks ⇒ low encode RSS** (1.79
  KB/open series × few series). So Mode 2's usual "raw still ships in full" caveat is
  largely **neutralised** here — the exact cold half is *small in absolute bytes*.
- **warm half (value axis, lossy):** the sketch compresses the *value* dimension into
  bounded-ε aggregates for the fast alert / dashboard path.

Net: on a low-cardinality tick stream you get **two orthogonal compressions at the edge**
— **Gorilla on the time axis (cheap, exact, cold)** + **sketch on the value axis (fast,
ε-bounded, warm)** — which is why finance tick is the strongest **Mode 2** workload in
the survey, not despite needing cold raw but *because* its cold raw is so cheap to ship.

---

## 2. Cloud observability — per-dataset cards

### A1 — Google cluster trace 2019 *(anchor — already ours)*

- **What:** per-instance Borg resource-usage stream (CPU/memory) over **8 cells, all of
  May 2019**; `instance_usage` carries **CPU-usage histograms per 5-minute period**
  (not a point sample), plus `instance_events`, `machine_events`, `collection_events`.
- **Volume / scale:** **~2.4 TiB compressed**; "several hundred GiB to ~1 TiB per cell"
  (8 cells). BigQuery-only access due to size. *(Exact `instance_usage` row count is
  not published per-cell — approx/unverified; the repo subsamples
  `instance_usage-000000000000.csv.gz` of cell `a`.)*
- **Cardinality:** **high** — millions of `(machine, job, task/instance)` tuples; this
  is the cardinality regime SDK-side sketch aggregation targets (`datasets_eval/google_cluster/README.md`).
- **Per-series frequency:** **low-moderate** — 5-minute sampling cadence per instance;
  high-frequency comes from *aggregate* rate, not per-series.
- **Warm vs cold:** **per-instance CPU/memory utilization → warm DDSketch/KLL** —
  queried only as fleet/percell quantiles ("p99 CPU across the cell"), raw per-instance
  point never needed → lossy-OK. **Cold:** capacity-planning audit / "exact CPU of
  instance X at 03:14 on May 12" forensic point-in-time (rare; pulls that metric cold).
- **Axes:** 1 ✓, 2 ✓ (quantile/sum over the fleet), 3 ✓ (the same SLO dashboards),
  4 ✓ (rolling windows), 5 ~ (1-month span limits true long-lookback), 6 ✓, 7 ~ (5-min).
- **Obtain:** <https://github.com/google/cluster-data> ·
  <https://github.com/google/cluster-data/blob/master/ClusterData2019.md>.
- **In repo:** measured — accuracy sweep (Fig 3a, p99 rel-err 0.34–1.86% across `p`),
  latency CDF (Fig 7). Already the resource anchor.

### A2 — Alibaba cluster trace 2018

- **What:** co-located batch + online-service jobs on a production cluster; machine
  usage, batch task instances, container/online-service metrics.
- **Volume / scale:** **4,201,015 batch jobs + 370,540 online-service jobs on 4,023
  machines over 8 days**; **>450 GB uncompressed across 6 files**.
- **Cardinality:** **high** — millions of task instances + per-machine series.
- **Per-series frequency:** **moderate** — machine/container usage sampled on the order
  of 10s–300s.
- **Warm vs cold:** per-machine CPU/mem utilization → **warm** (cluster quantiles);
  per-task scheduling/exit events → **cold** if exact post-mortem replay is required.
- **Axes:** like A1 (1,2,3,4,6 ✓; 5 ~ at 8 days; 7 ~). Adds **co-location skew** — a
  good complement to A1 for the controller-allocation story (mixed approximate vs exact
  metrics on the same host).
- **Obtain:** <https://github.com/alibaba/clusterdata> (`cluster-trace-v2018`).

### A3 — Alibaba microservices trace 2021 / 2022

- **What:** distributed-tracing + call-graph data — **20,000+ microservices** over
  **12 hours** from **>10,000 bare-metal nodes** (2021); 2022 adds microarchitectural
  metrics (AMTrace).
- **Volume / scale:** very large (call-graph edges per request); the
  highest-cardinality observability option here.
- **Cardinality:** **very high (✓✓)** — services × instances × call edges; this is the
  dataset for the high-cardinality axis.
- **Per-series frequency:** **high** — per-request spans.
- **Warm vs cold:** **per-service latency → warm DDSketch/KLL** (p50/p99 latency SLOs
  are quantile queries — exactly Fig 7's `quantile_over_time` path); **distinct-callers
  → warm HLL** (cardinality). **Cold:** *individual* trace replay for incident forensics
  ("show me the exact span tree for trace-id … last Tuesday") needs the raw event —
  lossless, cold. Clean illustration of the disjoint split: latency-quantile series go
  warm, the raw span archive goes cold.
- **Axes:** 1 ✓, 2 ✓, 3 ✓, 4 ✓, 5 ✓ (forensic lookback is the cold motivation), 6 ✓✓,
  7 ✓.
- **Obtain:** <https://github.com/alibaba/clusterdata> (`cluster-trace-microservices-v2021/2022`).

### A4 — Azure Public Dataset VM trace (V1 2017 / V2 2019)

- **What:** sanitized first-party Azure VM workload — 5-minute CPU-utilization
  readings + VM-info + subscription tables.
- **Volume / scale:** **V1 (2017): ~2 M VMs, ~1.2 B utilization readings**;
  **V2 (2019): ~2.6 M VMs, ~1.9 B utilization readings.**
- **Cardinality:** **very high (✓✓)** — millions of VMs = millions of series.
- **Per-series frequency:** **low** — 5-minute readings (like A1).
- **Warm vs cold:** **per-VM CPU utilization → warm DDSketch** (fleet quantiles for
  capacity/SLO; raw per-VM point not needed). **Cold:** per-VM **billing / chargeback
  counters** — exact replay required (a customer dispute can't be answered with an
  ε-bounded number) → lossless cold. This is the textbook *"per-host CPU → warm vs
  billing counter → cold"* example from the task framing.
- **Axes:** 1 ✓, 2 ✓, 3 ✓, 4 ✓, 5 ~, 6 ✓✓, 7 ~.
- **Obtain:** <https://github.com/Azure/AzurePublicDataset>.

### A5 — Azure Functions trace 2019

- **What:** serverless invocation counts — **per-minute invocations per function** +
  trigger group + execution-duration distributions, July 2019.
- **Volume / scale:** large invocation counts; the **per-function duration percentiles**
  are explicitly published as *distributions* — a native warm-sketch fit.
- **Cardinality:** **very high (✓✓)** — many functions × applications.
- **Per-series frequency:** **higher than VM traces** — per-minute invocation series,
  bursty.
- **Warm vs cold:** **invocation rate → warm Sum/Count; execution-duration → warm
  KLL/DDSketch** (the trace ships duration *percentiles* — exactly what a quantile
  sketch reconstructs). **Cold:** per-invocation **billing** records (exact) → cold.
- **Axes:** 1 ✓, 2 ✓, 3 ✓, 4 ✓, 5 ~, 6 ✓✓, 7 ✓.
- **Obtain:** <https://github.com/Azure/AzurePublicDataset> (`AzureFunctionsDataset2019`).

### A6 — OpenTelemetry Demo / synthetic generators

- **What:** the OTel "Astronomy Shop" demo emits metrics + traces + logs across ~15
  microservices; pairs naturally with this repo's `otel-app` SDK generator.
- **Volume / scale:** **operator-controllable** (load-generator driven) — not a fixed
  corpus, so it's the *knob* dataset, not a scale claim.
- **Cardinality / frequency:** tunable; useful precisely because you can dial axes 1/6/7
  to stress a specific figure (e.g. push cardinality to find the cost-model crossover).
- **Warm vs cold:** mirror A3 — RED metrics (rate/errors/duration) → warm; raw trace
  export → cold. Its real value is as the **controllable** workload for Fig 12 (sweep
  the query set, watch the allocation move) rather than a citable scale number.
- **Axes:** 2 ✓, 3 ✓, 4 ✓; 1/6/7 ~ (only as configured); 5 ✗ (no history).
- **Obtain:** <https://github.com/open-telemetry/opentelemetry-demo>.

### A7 — Wikimedia pageviews / webrequest

- **What:** **pageviews** = hourly per-page aggregate dumps (public, since 2015);
  **webrequest** = every hit to Wikimedia's CDN (page HTML, images, API) — the raw
  request log (internal Hive, but the derived pageview dumps are fully public).
- **Volume / scale:** webrequest is the full CDN firehose (billions of hits/day across
  the edge fleet); public pageview dumps are **hourly per-page gzipped text**, retained
  with raw webrequest **purged after 90 days for privacy**. *(Exact req/s
  approx/unverified — published artifact is the hourly aggregate, not the raw rate.)*
- **Cardinality:** **very high (✓✓)** — distinct pages/URLs is a classic heavy-hitter +
  cardinality workload.
- **Per-series frequency:** **high** at the request level.
- **Warm vs cold:** **top-pages → warm CountSketch-heap/CountMinSketch-heap (topk/heavy-hitter);
  distinct-clients → warm HLL (cardinality)** — the canonical sketch use cases. **Cold:**
  per-request forensic / abuse investigation needs the raw log — *but Wikimedia's own
  90-day purge is the real-world cold-retention constraint*, and the public artifact is
  already the aggregate (the raw is privacy-gated). A good motivating example *and* an
  honest gap (the cold-raw half is not publicly downloadable).
- **Axes:** 1 ✓, 2 ✓, 3 ✓, 4 ✓, 5 ✓ (the dumps go back years), 6 ✓✓, 7 ✓.
- **Obtain:** <https://dumps.wikimedia.org/other/pageviews/> ·
  <https://dumps.wikimedia.org/other/pageview_complete/readme.html> ·
  webrequest schema: <https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Traffic/Webrequest>.

---

## 3. Finance — per-dataset cards

### F1 — DEBS-2022 Grand Challenge (Deutsche Börse / Infront tick data) *(anchor — already ours)*

- **What:** financial **tick data** (last-trade events) from **three European exchanges
  — Paris (FR), Amsterdam (NL), Frankfurt/Xetra (ETR)** over a full week in 2021;
  provided by Infront Financial Technology for the DEBS 2022 Grand Challenge.
- **Volume / scale:** **289 million tick events** over **5,504 equities & indices**.
  The repo maps the **09:00–09:30 CEST slice = 685,822 events / 3,912 symbols**
  (`datasets_eval/debs/`); top-1% of symbols = 11.3% of activity (max ASML rate 3,141
  ticks vs min 1 — genuine activity skew).
- **Cardinality:** **moderate-high** — ~5.5k symbols (series key = symbol).
- **Per-series frequency:** **high & skewed (✓)** — hot symbols at thousands of
  ticks/window, a long quiet tail at ~1.
- **Warm vs cold:** **VWAP / price quantiles / rolling-volume aggregates → warm
  DDSketch/Sum**; the skew is exactly what the coordinated-`p` sampling exploits (hot
  ASML → `p≈0.031`, rare symbol → `p≈0.990`, **32× differentiation**). **Cold:**
  trade-by-trade **MiFID II / regulatory audit replay** → lossless raw.
- **Axes:** 1 ✓, 2 ✓, 3 ✓, 4 ✓, 5 ~ (one week), 6 ✓, 7 ✓.
- **Obtain:** Zenodo DOI <https://doi.org/10.5281/zenodo.6382482> ·
  paper <https://arxiv.org/abs/2206.13237>.
- **In repo:** measured — the §6 financial cross-check (accuracy 0.9–1.1% ≈ DDSketch α,
  coordinated sampling 32×, ε-gate/delta regime); see `datasets_eval/debs/cdm_eval/`.

### F2 — LOBSTER (NASDAQ reconstructed limit order book)

- **What:** **event-by-event limit-order-book reconstruction** for any NASDAQ-traded
  stock from June 2007 (Humboldt University). Message file = limit-order arrivals,
  cancellations, executions, market orders, halts; book file = reconstructed depth.
  **Free sample files for AAPL, AMZN, GOOG, INTC, MSFT.**
- **Volume / scale:** **hundreds of thousands of book updates per active stock per
  trading day** (e.g. ~300k–600k snapshots/stock on a busy day), nanosecond-precision
  timestamps. Full coverage is a paid subscription; samples are free.
- **Cardinality:** **moderate** per-stock (price levels), high across the universe of
  symbols.
- **Per-series frequency:** **very high (✓✓)** — sub-millisecond LOB events; the
  highest per-series frequency in this survey.
- **Warm vs cold:** **order-book depth/imbalance quantiles, rolling spread → warm**;
  **but the LOB is fundamentally an event-exact, replay-driven artifact** — most LOB
  research needs the *exact* event sequence (fill-probability, microstructure) →
  **cold-dominant**. A good honest case where the split leans **cold** (exact replay is
  the point), with only the aggregate-summary queries going warm. Illustrates "where the
  split is ambiguous" from the quality bar.
- **Axes:** 1 ✓, 2 ✓, 3 ~, 4 ✓, 5 ✓ (backtesting), 6 ~, 7 ✓✓.
- **Obtain:** <https://lobsterdata.com> (free samples at
  <https://data.lobsterdata.com/info/DataStructure.php>).

### F3 — NYSE Daily TAQ (Trade and Quote)

- **What:** **all trades and all quotes** for every issue on NYSE, Nasdaq and regional
  exchanges — consolidated tape, **microsecond timestamps**, **>10,000 issues across 16
  US exchanges**, history from 1993 to present.
- **Volume / scale:** the **largest-volume (✓✓)** finance option — full-market trades +
  the (far larger) quote stream; multi-billion-message days at peak. *(Exact daily
  record/byte counts vary by day and aren't published as a single figure —
  approx/unverified; obtain via WRDS for academics.)*
- **Cardinality:** **very high (✓✓)** — 10k+ symbols × per-venue quote series.
- **Per-series frequency:** **very high (✓✓)** — quote updates dominate, microsecond
  cadence on liquid names.
- **Warm vs cold:** **VWAP / volume / price quantiles per symbol → warm
  DDSketch/Sum/KLL** (the quote *firehose* is the canonical "queried only as aggregates,
  raw quotes never individually needed" warm case — discarding the per-quote stream is
  the big win). **Cold:** **SEC / MiFID-style audit & exact trade reconstruction** →
  lossless raw — the strongest real-world cold-raw / compliance motivation in the
  survey. Clean disjoint split: quotes → warm aggregates, trades → cold audit.
- **Axes:** 1 ✓✓, 2 ✓, 3 ✓, 4 ✓, 5 ✓, 6 ✓✓, 7 ✓✓.
- **Obtain:** <https://www.nyse.com/market-data/historical/daily-taq> · academic via WRDS
  <https://wrds-www.wharton.upenn.edu/pages/about/data-vendors/nyse-trade-and-quote-taq/>.
  *(Access-gated — not freely downloadable; cite as the high-end scale reference.)*

### F4 — Deutsche Börse Public Dataset (Xetra / Eurex, AWS Open Data)

- **What:** trade data **pre-aggregated to 1-minute OHLCV** (open/high/low/close,
  #trades, volume) per security from the Xetra and Eurex systems.
- **Volume / scale:** modest (already minute-aggregated, not tick) — two S3 buckets in
  eu-central-1; **freely downloadable** (NC license).
- **Cardinality:** **moderate** — all tradeable Xetra/Eurex securities.
- **Per-series frequency:** **low (✗)** — 1-minute bars (the raw ticks are already
  collapsed by the publisher).
- **Warm vs cold:** the data is **already an aggregate** — it *is* the warm-tier output
  shape (OHLCV bars). Useful as a **ground-truth aggregate** to validate warm-sketch
  answers against, or a low-frequency long-history series. Little cold motivation (no
  raw ticks to preserve). Its honest role: **a check, not a stressor** — and a freely
  downloadable, license-clean stand-in for the access-gated TAQ.
- **Axes:** 2 ✓, 3 ✓, 4 ✓, 5 ✓ (multi-year), 6 ✓; 1 ~, 7 ✗.
- **Obtain:** <https://registry.opendata.aws/deutsche-boerse-pds/>.

### F5 — Binance / Kraken / Coinbase public crypto tick data

- **What:** **tick-by-tick trade data** (and aggTrades) per trading pair, free bulk
  download. Binance: `data.binance.vision` (daily/monthly aggTrades + trades per pair).
  Kraken: full per-pair trade history CSVs from market inception. Coinbase: historical
  exchange datasets.
- **Volume / scale:** large and **continuously growing** — BTCUSDT and other majors run
  to **millions of trades/day**; full multi-pair history is many tens of GB. *(Per-pair
  daily counts vary widely — approx/unverified; download per-pair to measure.)*
- **Cardinality:** **moderate** — hundreds of trading pairs (not host-fleet scale), but
  24/7 (no market close) so continuous.
- **Per-series frequency:** **very high (✓✓)** on majors; bursty around volatility.
- **Warm vs cold:** **per-pair price quantiles / VWAP / rolling volume → warm
  DDSketch/Sum** (the high-frequency, aggregate-queried case). **Cold:** **backtesting
  replay** needs the exact trade sequence (a strategy backtest is a long-lookback,
  exact-replay query) → lossless cold. Crypto is the **freely-downloadable, license-open
  stand-in for TAQ** — it covers the high-frequency + long-lookback finance axes without
  TAQ's access gate.
- **Axes:** 1 ✓, 2 ✓, 3 ✓, 4 ✓, 5 ✓ (backtest), 6 ~, 7 ✓✓.
- **Obtain:** <https://github.com/binance/binance-public-data> /
  <https://data.binance.vision> · Kraken
  <https://support.kraken.com/articles/360047543791> · Coinbase Institutional market
  data. Aggregated CSVs: <https://www.cryptodatadownload.com/data/>.

---

## 3b. Product analytics — per-dataset cards

The **third domain** (after cloud observability and finance). Product analytics
(Amplitude / Mixpanel / PostHog / Heap-style event tracking) is the domain where
**approximate aggregates are already the cultural default** — DAU/MAU, funnels and
top-events are answered with HLL / sketches at scale across the whole industry
(Apache Druid, ClickHouse, BigQuery `APPROX_COUNT_DISTINCT`, Mixpanel), so the
warm-tier "lossy within ε" pitch is *uncontroversial* here. It naturally headlines
the two families the observability/finance anchors under-exercise — **HLL**
(unique users) and **CountSketch/CMS-heap** (top events/features) — and carries the
most *universal* cold-raw motivation in the survey: **GDPR/CCPA right-to-erasure &
data-export**, which forces exact replay/deletion of one user's raw events (a clean
Mode-2 drill-down). Unlike the access-gated finance/billing cold-raw cases (§4),
product analytics has **freely-downloadable** public clickstream corpora.

### P1 — Taobao UserBehavior 2017 (Alibaba) *(recommended product-analytics add)*

- **What:** real user-behavior log from Taobao — `(user_id, item_id, category_id,
  behavior, timestamp)` with `behavior ∈ {pv, cart, fav, buy}` over **2017-11-25 →
  2017-12-03 (9 days)**. The canonical public clickstream / funnel dataset.
- **Volume / scale:** **~987,994 users, ~4 M items, ~100 M behavior events**
  (~3.5 GB CSV); freely downloadable (Alibaba Tianchi / `github.com/alibaba`,
  same publisher as the A2/A3 cluster traces already cited).
- **Cardinality:** **very high (✓✓)** — ~1 M users × ~4 M items; user_id is the
  textbook HLL key.
- **Per-series frequency:** **low / sparse (✗)** — a single user emits events
  sporadically; like resource traces, the volume is *aggregate*, not per-series.
- **Warm vs cold:** **DAU/MAU & unique-buyers → warm HLL; top items/categories →
  warm CountSketch-heap/CMS; pv→cart→buy funnel counts → warm Sum/CMS;
  dwell/value quantiles → warm DDSketch/KLL** — all predefined, repeated dashboard
  queries (condition 1 ✓). **Cold:** **GDPR right-to-erasure / data-export** and
  **ML-feature pipelines** need the exact per-user raw event stream → lossless cold.
  The split is clean by **metric-identity separation** (the DAU rollup vs the raw
  event log are different artifacts → Mode 1) *and* exposes a Mode-2 case where the
  same `user_id` is both aggregated (dashboard) and exactly replayed (GDPR export)
  → bound-based drill-down to one user.
- **Axes:** 1 ✓, 2 ✓ (HLL/topk/funnel), 3 ✓ (standing product dashboards), 4 ✓
  (rolling 7/28-day active users), 5 ~ (retention cohorts, but 9-day span limits it),
  6 ✓✓, 7 ✗.
- **Obtain:** <https://tianchi.aliyun.com/dataset/649> ·
  <https://github.com/alibaba> (UserBehavior).

### P2 — REES46 eCommerce behavior 2019

- **What:** multi-category online-store event stream — `view / cart / remove /
  purchase` events with `user_id, product_id, category, brand, price, user_session`,
  Oct–Nov 2019. A richer-property funnel/segmentation corpus.
- **Volume / scale:** **~285 M events** (~9 GB across two months); free on Kaggle.
- **Cardinality:** **very high (✓✓)** — millions of users × products × sessions.
- **Per-series frequency:** **low (✗)** per user/session.
- **Warm vs cold:** **funnel conversion, revenue Sum, top brands/products
  (CountSketch-heap), distinct-purchasers (HLL), basket-value quantiles (DDSketch)
  → warm**; **per-user export / fraud-investigation raw → cold.** Strong **Mode-2**
  case (live segmentation dashboard + exact export on the same user series).
- **Axes:** 1 ✓, 2 ✓, 3 ✓, 4 ✓, 5 ~, 6 ✓✓, 7 ✗.
- **Obtain:** <https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store>.

### P3 — Wikipedia clickstream

- **What:** monthly `(referrer → article)` navigation **click counts** — already a
  per-pair aggregate, public since 2015.
- **Volume / scale:** tens of millions of (referrer, article) pairs/month, gzipped
  TSV; freely downloadable.
- **Cardinality:** **very high (✓✓)** — distinct referrer×article pairs.
- **Per-series frequency:** **n/a (✗)** — the artifact is *already* the monthly
  aggregate (no raw click stream published).
- **Warm vs cold:** like F4 Deutsche-Börse-PDS, it **is** the warm-tier output shape
  — a **topk/heavy-hitter ground-truth** to validate CountSketch-heap answers
  against, and a long-history (years of monthly dumps) low-frequency series. Little
  cold motivation (raw is privacy-purged, not published). Role: **a check, not a
  stressor** — the license-clean, public stand-in for real (private) clickstream.
- **Axes:** 2 ✓, 3 ✓, 4 ✓, 5 ✓ (years of dumps), 6 ✓✓; 1 ~, 7 ✗.
- **Obtain:** <https://dumps.wikimedia.org/other/clickstream/>.

**Honest caveat for the domain:** product analytics adds **axis 6 (very-high
cardinality) + axis 2 (HLL/topk aggregation)** and a *public* GDPR cold-raw story —
but it does **not** add **axis 7 (high-frequency-per-series)** (a user's events are
sparse), and the *richest* real data (production Amplitude/Mixpanel) is private, so
lead measurements on the free Taobao/REES46 proxies and cite real SaaS scale only
for motivation. It also partly overlaps A7 Wikimedia (topk/HLL) — frame it as the
generalized, GDPR-motivated business-analytics version, not a fully orthogonal axis.

---

## 4. Honest gaps

- **No single public dataset is "high-cardinality AND long-retention-raw" at once.**
  The cold-raw motivation (exact replay over a long horizon at fleet cardinality) is
  exactly what real operators *don't* publish — it's expensive and often
  privacy/compliance-gated. Azure/Google give high cardinality but **short spans**
  (1 month) and **already-purged or histogram-summarized** raw; the long-history sets
  (Deutsche Börse PDS, pageview dumps) are **already aggregated**. So axis 5
  (long-lookback) + axis 6 (high-cardinality) + raw retention only co-occur in
  **private** corpora.
- **The strongest cold-raw / compliance cases are access-gated or private.** NYSE TAQ
  (SEC/MiFID audit) is the textbook cold-raw motivation but is WRDS/subscription-gated;
  Wikimedia webrequest (forensic) is privacy-purged at 90 days; real billing/audit
  counters (the cleanest "must be exact" series) are proprietary. We cite these for the
  *motivation* and use freely-downloadable stand-ins (crypto tick, Deutsche Börse PDS)
  for the *measurements*.
- **Per-series high-frequency at fleet cardinality is rare in resource traces.** Google/
  Azure/Alibaba are 5-min/10-min cadence — high *aggregate* rate but low *per-series*
  frequency. Axis 7 at scale comes from **finance tick** (TAQ, LOBSTER, crypto) and
  **microservice traces** (A3), not resource traces.
- **Several cited scale numbers are publisher-summarized, not raw counts** (Google 2019
  per-cell row count, TAQ daily volume, crypto per-pair counts, Wikimedia req/s) —
  flagged *approx/unverified* in the cards; download/BigQuery to pin them if a figure
  becomes load-bearing.

---

## 5. What this means for ASAP's eval

We already have the two anchors: **Google cluster 2019** (resource / high-cardinality →
accuracy, Pareto, cardinality/sum) and **DEBS-2022** (finance / skewed → coordinated
sampling, topk, ε-gate/delta). They cover axes 1/2/3/4/6 well and 7 partially (DEBS
skew). The axes they **under-cover** are **(5) long-lookback** and **(7)
high-frequency-per-series** — and neither anchor strongly exercises the **cold-raw /
exact-replay** half of the disjoint story (both are dominated by warm-aggregate queries).

**Top 3 datasets to add (each newly covers a specific gap):**

1. **Alibaba microservices trace 2021/2022 (A3)** — newly covers **(6) very-high
   cardinality + (7) high per-series frequency + (5) forensic long-lookback** in *one*
   observability workload, and is the cleanest **disjoint-routing demo**: per-service
   latency → warm KLL/DDSketch (Fig 7 quantile path) **xor** raw span archive → cold
   (incident replay). Directly exercises the cold half that the current anchors don't.
   Free, downloadable.

2. **Binance/Kraken public crypto tick (F5)** — freely-downloadable, license-open,
   **24/7 high-frequency (7 ✓✓)** finance stream with a genuine **long-lookback
   backtest → cold-raw** query class (5 ✓). It's the practical stand-in for the
   access-gated TAQ and gives us the high-frequency + exact-replay axes DEBS's one-week
   slice can't. Adds a second, *continuous* finance axis next to DEBS.

3. **Azure VM trace 2019 (A4)** — **~2.6 M VMs / ~1.9 B readings (6 ✓✓)** is the
   highest *clean* cardinality of any resource trace and the textbook **warm-vs-cold
   split**: per-VM CPU → warm DDSketch (fleet quantiles, raw discarded) **xor** per-VM
   billing counter → cold (exact, dispute-grade). It stresses the controller-allocation
   figure (Fig 12) at a cardinality Google's subsample doesn't reach.

**Honorable mentions:** **NYSE TAQ (F3)** as the *cited* high-end scale + the strongest
real compliance/cold-raw motivation (even if access-gated, name it in §6 framing);
**Wikimedia pageviews (A7)** as the canonical **topk (CountSketch-heap) + distinct (HLL)**
workload with a real 90-day cold-retention story; **Deutsche Börse PDS (F4)** as a
license-clean OHLCV ground-truth to validate warm-sketch aggregate answers against.

### 5a. Mode-aware recommendation (under the warm-eligibility predicate)

Re-reading the picks through the **two-condition predicate** and the **two modes**
(§0a) sharpens *which* mode each dataset evaluates — and **demotes one earlier "warm"
pick** (crypto tick) from Mode 1 to Mode 2.

**Mode 1 (disjoint, clean warm) — the four cleanest, by metric-identity separation:**

1. **Alibaba microservices 2021/2022 (A3)** — per-service latency *metric* → warm
   KLL/DDSketch; raw spans are a *separate* artifact → cold. The split is clean *by
   construction* (different metrics), so condition 2 is provable. Strongest Mode-1
   disjoint demo + the cold half the anchors lack.
2. **Azure VM 2019 (A4)** — per-VM CPU → warm (fleet quantiles, raw discarded); the
   billing counter is a *separate* series → cold. Textbook metric-identity split at the
   highest clean cardinality (~2.6 M VMs); stresses Fig 12.
3. **Azure Functions 2019 (A5)** — the trace ships duration *distributions*: the warm
   query is *literally* a predefined percentile SLO; billing records are the separate
   cold series. Cleanest fit for condition 1.
4. **Google cluster 2019 (A1, anchor)** — per-instance CPU/mem → fixed SLO/capacity
   dashboards (repeated), no standing raw consumer. Keep as the warm-accuracy anchor.

**Mode 2 (co-resident, accelerate-then-drill-down) — where raw is mandatory anyway:**

- **Binance/Kraken crypto tick (F5)** — *demoted from a Mode-1 warm pick*: the **same**
  price series wanted live (VWAP/quantile alert) is replayed **raw** for backtest, so
  condition 2 fails → it is a **Mode-2** dataset. Cold raw kept for backtest; warm sketch
  added for fast live alerts + "is this pair behaving oddly?" triage. Free, 24/7,
  high-frequency — the best public **Mode-2 evaluation** workload.
- **NYSE TAQ (F3) / DEBS audit (F1)** — MiFID/SEC require the raw trade tape regardless;
  warm VWAP/quantile sketches ride on top for dashboards and to **triage which narrow
  window/symbol to pull from cold** for the audit.
- **Wikimedia (A7)** — forensic raw retained (90-day); warm topk/HLL gives cheap
  dashboards + narrows the forensic cold replay.

**Use-case scenarios by mode:**

| scenario | mode | warm role | cold role |
|---|---|---|---|
| fleet SLO / capacity dashboard (CPU p99, latency p99) | 1 | the answer (raw never wanted) | — |
| serverless duration percentiles + invocation rate | 1 | the answer | (separate billing series) |
| live crypto VWAP / quantile alert **+** strategy backtest | 2 | fast alert + triage | authoritative replay (backtest) |
| trading dashboard **+** MiFID/SEC audit | 2 | dashboard + audit-window triage | regulator-grade exact tape |
| CDN topk pages / distinct clients **+** abuse forensics | 2 | topk/HLL dashboard + triage | forensic raw log |

**Net:** Mode 1 evaluates the *storage / bandwidth Pareto* (A1/A3/A4/A5); Mode 2
evaluates the *latency + cold-IO-pruning* story (F5/F3/A7), with the **bound-based
short-circuit** as the headline metric — the fraction of queries answered warm-only vs
forced to drill into cold.

### 5b. Top 5 datasets to use in evaluation (the actionable shortlist)

The five that together cover **both modes, both domains, and every axis the anchors
under-cover**. Two are already wired (anchors); three are the build-out.

| # | dataset | domain | mode | evaluates (figure / claim) | status |
|---|---|---|---|---|---|
| 1 | **Google cluster 2019** (A1) | observability/resource | **M1** | warm accuracy in ε-envelope (Fig 3), latency CDF (Fig 7), storage/bw Pareto | ✅ wired |
| 2 | **DEBS-2022** (F1) | finance/tick | **M2** | coordinated sampling 32× (Fig 9), ε-gate/delta, finance-tick edge-Gorilla cold | ✅ wired |
| 3 | **Alibaba microservices 2021/22** (A3) | observability/traces | **M1** | high-card disjoint routing (Fig 11), latency-quantile warm xor span cold | ◻ to add |
| 4 | **Azure VM 2019** (A4) | observability/resource | **M1** | controller allocation at ~2.6 M-series cardinality (Fig 12), metric-identity split | ◻ to add |
| 5 | **Binance/Kraken crypto tick** (F5) | finance/tick | **M2** | bound-based short-circuit + two-axis edge compression (Mode 2), high-freq/low-card | ◻ to add |

**Scale per dataset — cardinality (axis 6) × per-series frequency (axis 7):**

| # | dataset | cardinality (# series) | per-series frequency | overall scale |
|---|---|---|---|---|
| 1 | Google cluster 2019 | **high** — millions of `(machine, job, task/instance)` tuples (repo subsamples 1 cell) | **low** — 1 sample / **5 min** per instance | ~2.4 TiB compressed, 8 cells, all May 2019 |
| 2 | DEBS-2022 | **moderate ~** — **~5,500** symbols (series key = symbol; repo slice **3,912**) | **high & skewed** — hot ASML **~3,141** ticks/window, quiet tail **~1** | **289 M** ticks over 5.5k symbols, 1 week |
| 3 | Alibaba microservices 2021/22 | **very high ✓✓** — **20,000+** microservices × instances × call-edges, on **>10,000** nodes | **high** — per-**request** spans (sub-second) | 12 h trace, call-graph edges/request |
| 4 | Azure VM 2019 | **very high ✓✓** — **~2.6 M** VMs = ~2.6 M series | **low** — 1 reading / **5 min** per VM | **~1.9 B** utilization readings |
| 5 | Binance/Kraken crypto tick | **low–moderate ~** — **hundreds** of trading pairs (24/7, no close) | **very high ✓✓** — majors **millions** of trades/day, bursty at volatility | many tens of GB multi-pair, continuously growing |

*(Read: the two **M1** observability adds — A3 microservices and A4 VM — supply the
**high-cardinality ✓✓** axis; the two **M2** finance picks — F1 DEBS and F5 crypto —
supply the **high-frequency-per-series ✓✓** axis. No single one maxes both, §4.)*

**Per-dataset use case + what it evaluates:**

1. **Google cluster 2019 — M1, the warm-accuracy anchor.**
   *Use case:* fleet / per-cell resource SLO dashboards ("p99 CPU across the cell",
   capacity quantiles) — standing, repeated queries; the raw per-instance point is never
   wanted. *Evaluates:* warm accuracy in the joint ε-envelope (Fig 3a, p99 rel-err
   0.34–1.86 % across `p`), per-family accuracy (5/6), query-latency CDF (Fig 7, warm p50
   18.3 ms), and the Mode-1 storage/bandwidth Pareto. *Warm:* per-instance CPU/mem →
   DDSketch/KLL. *Cold:* rare forensic point-in-time.

2. **DEBS-2022 — M2, the coordinated-sampling + finance-tick anchor.**
   *Use case:* live VWAP / price-quantile dashboards + threshold alerts on a **skewed**
   symbol fleet; MiFID audit / backtest on the *same* series → cold. *Evaluates:*
   coordinated sampling `p_i ∝ √(f_i/rate_i)` (**32× differentiation**, Fig 9), the
   ε-gate/delta regime, accuracy on real skew (0.9–1.1 % ≈ α), and the finance-tick
   Mode-2 story (cheap edge-Gorilla cold + warm sketch — two orthogonal edge
   compressions). *Warm:* VWAP/quantile/volume → DDSketch/Sum. *Cold:* trade-by-trade
   audit (cheap via edge Gorilla-XOR/`intchunk`).

3. **Alibaba microservices 2021/2022 — M1, the high-cardinality disjoint demo.**
   *Use case:* per-service p50/p99 latency SLO alerting (RED metrics) + distinct-caller
   cardinality; incident trace replay → cold. *Evaluates:* disjoint routing by
   **metric-identity separation** (Fig 11) — covers the cold half the anchors lack —
   very-high cardinality (Fig 12), and the warm `quantile_over_time` path at scale
   (Fig 7). *Warm:* latency metric → KLL/DDSketch, distinct-callers → HLL. *Cold:* raw
   span archive (a *separate* artifact, so condition 2 is provable).

4. **Azure VM 2019 — M1, the controller-allocation stressor.**
   *Use case:* per-VM CPU capacity/SLO dashboards across **~2.6 M VMs**; billing /
   chargeback → cold. *Evaluates:* controller allocation (Fig 12) at the highest *clean*
   cardinality, the textbook **metric-identity** warm-vs-cold split, and the cost-model
   crossover. *Warm:* per-VM CPU → DDSketch fleet quantiles. *Cold:* per-VM billing
   counter (separate series, dispute-grade exact).

5. **Binance/Kraken crypto tick — M2, the Mode-2 / short-circuit showcase.**
   *Use case:* live crypto VWAP/quantile alerts + "is this pair behaving oddly?" triage;
   strategy **backtest** replays raw on the *same* series → cold. *Evaluates:* the
   **bound-based short-circuit** (fraction answered warm-only vs forced to cold), the
   **two orthogonal edge compressions** (Gorilla time-axis cold + sketch value-axis warm)
   on a high-freq/low-card stream, and a second *continuous*, free, license-open finance
   axis next to DEBS. *Warm:* per-pair quantile/VWAP → DDSketch/Sum. *Cold:* exact tick
   replay for backtest (cheap via edge Gorilla).

**Coverage check:** **Mode 1** = {A1, A3, A4} (storage/bandwidth Pareto) · **Mode 2** =
{F1, F5} (latency + cold-IO-pruning short-circuit). **Observability** = {A1, A3, A4} ·
**Finance** = {F1, F5}. **Cardinality ✓✓** from A3/A4 · **high-frequency ✓✓** from F1/F5
· **long-lookback / cold-replay** from A3 (forensic) + F5 (backtest). Anchors {A1, F1}
are wired; **{A3, A4, F5} are the three to build**.

---

## 6. References

### Datasets
- Google cluster-data 2019 — <https://github.com/google/cluster-data/blob/master/ClusterData2019.md>
- Alibaba clusterdata (2018, microservices 2021/2022) — <https://github.com/alibaba/clusterdata>
- Azure Public Dataset (VM V1/V2, Functions 2019) — <https://github.com/Azure/AzurePublicDataset>
- OpenTelemetry Demo — <https://github.com/open-telemetry/opentelemetry-demo>
- Wikimedia pageviews / webrequest — <https://dumps.wikimedia.org/other/pageviews/> ·
  <https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Traffic/Webrequest>
- DEBS-2022 Grand Challenge — Zenodo <https://doi.org/10.5281/zenodo.6382482> ·
  paper <https://arxiv.org/abs/2206.13237>
- LOBSTER — <https://lobsterdata.com> · <https://data.lobsterdata.com/info/DataStructure.php>
- NYSE Daily TAQ — <https://www.nyse.com/market-data/historical/daily-taq> ·
  WRDS <https://wrds-www.wharton.upenn.edu/pages/about/data-vendors/nyse-trade-and-quote-taq/>
- Deutsche Börse PDS (AWS Open Data) — <https://registry.opendata.aws/deutsche-boerse-pds/>
- Taobao UserBehavior 2017 — <https://tianchi.aliyun.com/dataset/649> · <https://github.com/alibaba>
- REES46 eCommerce behavior 2019 — <https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store>
- Wikipedia clickstream — <https://dumps.wikimedia.org/other/clickstream/>
- Binance public data — <https://github.com/binance/binance-public-data> ·
  Kraken <https://support.kraken.com/articles/360047543791> ·
  CryptoDataDownload <https://www.cryptodatadownload.com/data/>

### In-repo anchors
- [`paper-outline.md`](paper-outline.md) — §6 claims, the five evaluation dimensions.
- [`distributed-nitrosketch-coordinated-sampling.md`](distributed-nitrosketch-coordinated-sampling.md) — coordinated sampling, the disjoint-routing requirement, the joint bound.
- `datasets_eval/google_cluster/`, `datasets_eval/debs/` — the two anchor harnesses.

---

*End of survey.*
