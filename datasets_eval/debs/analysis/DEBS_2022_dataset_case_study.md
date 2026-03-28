## DEBS 2022 - dataset fields

| Field | Meaning |
|--------|---------|
| `symbol` | Instrument ID with exchange suffix (e.g. `RDSA.NL`) |
| `sectype` | `E` equity, `I` index |
| `last` | Last trade price |
| `trading_time` | `HH:MM:SS.ssss` (CEST) — time of last trade print |
| `date` | `DD-MM-YYYY` — wall-clock date populated on every row |

**Corpus**

- [Zenodo record — DEBS 2022 Grand Challenge: Trading Data](https://doi.org/10.5281/zenodo.6382482)  
- [arXiv:2206.13237 — *The DEBS 2022 Grand Challenge* (dataset and queries)](https://arxiv.org/abs/2206.13237)

### Notes

- For **last-trade / challenge queries**, use **`Date`** + **`Trading time`** (CEST wall clock) when **`Trading time`** is present. The challenge spec refers to a `Trading date` field, but **`Trading date` is empty in every file here**; use **`Date`** instead.
- The generic **`Time`** column is **not** the same as **`Trading time`**: it timestamps **each row's** update (quotes, trades, indices, etc.). 
- The **frequency** and **window** analysis scripts under [`analysis/code/`](code/) use **`Date` + `Time`** so stats reflect the **full feed**; use **`Date` + `Trading time`** for OTLP `financial.last_trade_price` timestamps when emitting last-trade events.

---

## Evaluation streams and layout

Two logical streams are used:

| Stream | Directory | Purpose |
|--------|-----------|---------|
| **Full feed** | [`data/`](data/) | Raw daily CSVs as published. Use for **throughput**, parsers, and any query variant where an **event** is **any row** (e.g. cardinality / activity-style tests). |
| **Price (last-trade) stream** | [`data_filtered/`](data_filtered/) | Rows with both **`Last`** and **`Trading time`** non-empty. Built with [`analysis/code/filter_data.py`](code/filter_data.py). Use for **Q1–Q5, Q7–Q12** and any logic defined on **`last`** with last-trade times. |

**Analysis and query tooling**

- Python scripts in [`analysis/code/`](code/) accept **`--dataset data`** (default) or **`--dataset data_filtered`**..
- **`analyze_frequency.py`** computes inter-arrival gap statistics (min, max, mean, median, p95, p99 in ms) on the globally sorted **`Date`+`Time`** event stream per file, and also breaks these down per tumbling window.
- **`analyze_windows.py`** counts events falling into each tumbling window (1min, 5min, 15min, 30min, 1hour) using the **`Date`+`Time`** clock.
- **`analyze_cardinality.py`** reports distinct **`symbol`** (stem of `ID` before the last `.`), **`exchange`** (suffix after the last `.`, e.g. `NL`), **`sectype`** (from `SecType`), and unique **`(symbol, exchange, sectype)`** triples—aligned with the OTLP attributes on `financial.last_trade_price` in the mapping below.

**Commands**

```bash
cd datasets_eval/debs/analysis/code
python3 filter_data.py
python3 analyze_frequency.py --dataset data
python3 analyze_windows.py --dataset data_filtered
python3 analyze_cardinality.py --dataset data
```

---

## Time windows (DEBS)

- **Type:** non-overlapping tumbling windows  
- **Size:** 5 minutes (300 s)  
- **Alignment:** clock-aligned (e.g. 09:00–09:05, 09:05–09:10)

---

## OTLP mapping

- **Metric:** `financial.last_trade_price` (Gauge)  
- **Value:** `last`  
- **Labels:** `symbol`, `exchange` (from suffix), `sectype`  
- **Timestamp:** `Date` + `Trading time` (CEST, `Europe/Berlin`). The challenge spec names a `Trading date` field that is empty in this corpus; `Date` is the populated date column on every row.  
- **Window:** 5-minute tumbling unless noted  

**Controller:** `POST /api/v1/plan` with `metric_name`, `aggregations` (`quantile` / `frequency` / `cardinality`), `time_window: "5m"`, workload hints; `GET /api/v1/config/{metric_name}` for YAML.

---

## Evaluation Configuration Matrix

Days 13–14 Nov (Saturday/Sunday) are excluded from all benchmarks — day 13 has near-zero filtered activity and day 14 has no filtered rows at all. All numbers below come from working days 08–12 Nov 2021.

### Dataset statistics — `data/` (full feed)

Sourced from `analysis/results/data/summaries/`.

**Frequency** (`frequency_summary.csv`) — inter-arrival gaps (ms) on globally sorted `Date`+`Time` per file:

| file | min\_ms | max\_ms | mean\_ms | median\_ms | p95\_ms | p99\_ms |
|------|---------|---------|---------|-----------|--------|--------|
| 08-11-21 | 0.0 | 1 771 000 | 1.60 | 0.0 | 0.0 | 0.0 |
| 09-11-21 | 0.0 | 3 070 000 | 1.47 | 0.0 | 0.0 | 0.0 |
| 10-11-21 | 0.0 | 2 556 000 | 1.33 | 0.0 | 0.0 | 0.0 |
| 11-11-21 | 0.0 | 2 670 932 | 1.55 | 0.0 | 0.0 | 0.0 |
| 12-11-21 | 0.0 | 3 614 029 | 1.54 | 0.0 | 0.0 | 0.0 |
| 13-11-21 | 0.0 | 2 940 000 | 2 457 | 0.0 | 1 896 | 40 956 |
| 14-11-21 | 0.0 | 10 030 979 | 4 157 | 0.0 | 1 826 | 21 000 |

**Window summary** (`window_summary.csv`) — working days only; `avg_samples` counts all row types (quotes + trades + indices):

| file | window\_size | total\_windows | avg\_samples | min\_samples | max\_samples |
|------|------------|--------------|------------|------------|------------|
| 08-11-21 | 1min | 1202 | 44 911 | 1 | 248 441 |
| 09-11-21 | 1min | 1238 | 47 497 | 1 | 257 640 |
| 10-11-21 | 1min | 1205 | 53 596 | 1 | 275 558 |
| 11-11-21 | 1min | 1102 | 50 394 | 1 | 227 444 |
| 12-11-21 | 1min | 1197 | 46 867 | 1 | 261 019 |
| 08-11-21 | 5min | 264 | 204 479 | 1 | 1 003 543 |
| 09-11-21 | 5min | 272 | 216 183 | 1 | 1 158 890 |
| 10-11-21 | 5min | 271 | 238 312 | 1 | 1 105 708 |
| 11-11-21 | 5min | 255 | 217 783 | 1 | 986 224 |
| 12-11-21 | 5min | 261 | 214 942 | 1 | 957 966 |
| 08-11-21 | 15min | 92 | 586 767 | 1 | 2 504 798 |
| 09-11-21 | 15min | 94 | 625 550 | 1 | 3 199 963 |
| 10-11-21 | 15min | 93 | 694 437 | 1 | 2 908 623 |
| 11-11-21 | 15min | 93 | 597 147 | 1 | 2 502 454 |
| 12-11-21 | 15min | 91 | 616 481 | 1 | 2 412 614 |
| 08-11-21 | 30min | 48 | 1 124 637 | 1 | 4 548 949 |
| 09-11-21 | 30min | 48 | 1 225 036 | 1 | 5 839 241 |
| 10-11-21 | 30min | 48 | 1 345 471 | 1 | 5 311 913 |
| 11-11-21 | 30min | 47 | 1 181 588 | 3 | 4 545 312 |
| 12-11-21 | 30min | 47 | 1 193 612 | 3 | 4 388 699 |
| 08-11-21 | 1hour | 24 | 2 249 273 | 73 | 7 549 156 |
| 09-11-21 | 1hour | 24 | 2 450 071 | 88 | 10 030 375 |
| 10-11-21 | 1hour | 24 | 2 690 942 | 97 | 8 550 548 |
| 11-11-21 | 1hour | 24 | 2 313 944 | 3 | 7 443 823 |
| 12-11-21 | 1hour | 24 | 2 337 490 | 78 | 7 948 308 |

**Cardinality** (`cardinality_overall.csv`):

| dimension | unique\_count\_global |
|-----------|---------------------|
| symbol | 5502 |
| exchange | 3 |
| sectype | 2 |
| symbol\_exchange\_sectype | 5502 |

---

### Dataset statistics — `data_filtered/` (last-trade stream)

Sourced from `analysis/results/data_filtered/summaries/`. Only rows with both `Last` and `Trading time` non-empty are retained.

**Frequency** (`frequency_summary.csv`) — inter-arrival gaps (ms):

| file | min\_ms | max\_ms | mean\_ms | median\_ms | p95\_ms | p99\_ms |
|------|---------|---------|---------|-----------|--------|--------|
| 08-11-21 | 0.0 | 11 404 000 | 6.86 | 0.0 | 0.0 | 0.0 |
| 09-11-21 | 0.0 | 11 402 000 | 6.80 | 0.0 | 0.0 | 0.0 |
| 10-11-21 | 0.0 | 11 637 000 | 6.70 | 0.0 | 0.0 | 0.0 |
| 11-11-21 | 0.0 | 7 804 622 | 6.83 | 0.0 | 0.0 | 0.0 |
| 12-11-21 | 0.0 | 11 460 000 | 6.86 | 0.0 | 0.0 | 0.0 |
| 13-11-21 | 0.0 | 11 405 000 | 12 870 | 0.0 | 14 856 | 203 046 |
| 14-11-21 | — | — | — | — | — | — |

**Window summary** (`window_summary.csv`) — working days only; `avg_samples` counts last-trade rows only:

| file | window\_size | total\_windows | avg\_samples | min\_samples | max\_samples |
|------|------------|--------------|------------|------------|------------|
| 08-11-21 | 1min | 942 | 12 587 | 1 | 54 337 |
| 09-11-21 | 1min | 939 | 12 739 | 1 | 57 103 |
| 10-11-21 | 1min | 938 | 12 932 | 1 | 57 881 |
| 11-11-21 | 1min | 939 | 12 671 | 1 | 55 987 |
| 12-11-21 | 1min | 937 | 12 653 | 1 | 55 891 |
| 08-11-21 | 5min | 197 | 60 186 | 1 | 128 886 |
| 09-11-21 | 5min | 193 | 61 981 | 1 | 131 663 |
| 10-11-21 | 5min | 193 | 62 852 | 1 | 132 469 |
| 11-11-21 | 5min | 194 | 61 328 | 1 | 130 535 |
| 12-11-21 | 5min | 193 | 61 428 | 1 | 130 478 |
| 08-11-21 | 15min | 70 | 169 382 | 1 | 354 028 |
| 09-11-21 | 15min | 68 | 175 917 | 1 | 340 967 |
| 10-11-21 | 15min | 68 | 178 390 | 1 | 342 945 |
| 11-11-21 | 15min | 69 | 172 430 | 1 | 340 097 |
| 12-11-21 | 15min | 68 | 174 347 | 1 | 342 348 |
| 08-11-21 | 30min | 38 | 312 019 | 1 | 689 130 |
| 09-11-21 | 30min | 37 | 323 307 | 1 | 676 037 |
| 10-11-21 | 30min | 37 | 327 852 | 1 | 668 955 |
| 11-11-21 | 30min | 38 | 313 096 | 1 | 663 775 |
| 12-11-21 | 30min | 37 | 320 422 | 1 | 672 885 |
| 08-11-21 | 1hour | 21 | 564 606 | 1 | 1 329 688 |
| 09-11-21 | 1hour | 21 | 569 636 | 1 | 1 313 121 |
| 10-11-21 | 1hour | 21 | 577 643 | 1 | 1 325 437 |
| 11-11-21 | 1hour | 22 | 540 802 | 1 | 1 296 336 |
| 12-11-21 | 1hour | 21 | 564 553 | 1 | 1 318 590 |

**Cardinality** (`cardinality_overall.csv`):

| dimension | unique\_count\_global |
|-----------|---------------------|
| symbol | 5182 |
| exchange | 3 |
| sectype | 2 |
| symbol\_exchange\_sectype | 5182 |

Per-day active symbols (`cardinality_summary.csv`): **5174–5179** on working days; 0 on day-14.

---

### Per-query evaluation matrix

**Benchmark baselines**

- **Throughput baseline:** full feed (`data/`), working days 08–12 Nov, 5-min tumbling windows, ~216K events/window (range 204K–238K), mean inter-arrival ~1.5 ms.
- **Latency baseline:** filtered feed (`data_filtered/`), same days, mean inter-arrival ~6.9 ms, ~12 last-trade events per symbol per 5-min window.

**Derived per-symbol averages (filtered)**

- 5-min window: ~61K global ÷ ~5178 active symbols ≈ **~12 last-trade events/symbol/window**
- 15-min window: ~172K global ÷ ~5178 ≈ **~33 last-trade events/symbol/window**

| Q | Dataset | Window | Global avg/window | Per-symbol avg/window | Test types | Active series/day | Cross-series aggregation |
|---|---------|--------|------------------|-----------------------|-----------|-------------------|--------------------------|
| Q1 EMA indicators | `data_filtered` | 5-min | ~61K | ~12 | Sketch-finance + Latency | 5178 | None — per-symbol |
| Q2 EMA crossover | `data_filtered` | 5-min (derived from Q1) | ~61K (inherited) | ~12 (inherited) | Throughput + Latency | 5178 | None — per-symbol |
| Q3 Top-K movers | `data` | 5-min | ~216K | ~39 | Sketch-finance + Throughput | 5497 | All 5502 → top-K (K=10) |
| Q4 Price stats | `data_filtered` | 5-min | ~61K | ~12 | Sketch-finance + Throughput | 5178 | None — per-symbol |
| Q5 Realized volatility | `data_filtered` | 5-min | ~61K | ~12 | Sketch-finance + Latency | 5178 | None — per-symbol |
| Q6 Distinct cardinality | `data` | 5-min | ~216K | N/A | Sketch-finance + Throughput | 5497 | All 5502 → 1 count/window |
| Q7 TWAP | `data_filtered` | 5-min | ~61K | ~12 | Sketch-finance + Throughput | 5178 | None — per-symbol |
| Q8 Price anomaly | `data_filtered` | **15-min** | ~172K | ~33 | Sketch-finance + Latency | 5178 | None — per-symbol |
| Q9 Bollinger bands | `data_filtered` | 5-min rolling N=3 bars | ~61K | ~12 | Throughput + Latency | 5178 | None — per-symbol |
| Q10 RSI | `data_filtered` | N/A (full stream state) | N/A | N/A | Throughput + Latency | 5178 | None — per-symbol |
| Q11 MACD | `data_filtered` | N/A (full stream state) | N/A | N/A | Throughput + Latency | 5178 | None — per-symbol |
| Q12 Stochastic | `data_filtered` | N/A (rolling n=14 ticks) | N/A | N/A | Throughput + Latency | 5178 | None — per-symbol |

**Rationale for key choices**

- **Q2 — no sketch test:** EMA crossover is a boolean sign-flip on Q1 outputs; no sketch replaces exact sign detection. Value is measuring throughput/latency of the downstream chain.
- **Q3 — full feed:** CountSketch ranks by event frequency across all row types; the full feed (~216K/window) provides maximum cardinality load. Ground truth for price-move variant is computed from filtered prices.
- **Q6 — full feed:** HLL counts distinct active symbols across all row types (5502 unique); filtered under-counts activity by discarding non-trade rows.
- **Q8 — 15-min window:** at 5-min the per-symbol count is only ~12, too sparse for IQR/z-score to be meaningful. 15-min gives ~33 samples/symbol/window, sufficient to evaluate sketch accuracy. The query spec already notes the longer-window option.
- **Q9–Q12 — no sketch test:** these require sequential per-symbol state (rolling windows, EMA chains, min/max lookback) that no single-window sketch can replace. Benchmarking value is the cost of stateful stream processing.

---

## Q1 - DEBS EMA indicators (per symbol)

**Purpose:** Trend from short vs long EMA; tests per-symbol state, ordering, windowed aggregation under ~5504 series.

**Formula:** $\mathrm{EMA}_t = \alpha p_t + (1-\alpha)\mathrm{EMA}_{t-1}$, $\alpha = 2/(n+1)$; $n \in \{38,100\}$.

**Data requirements:**
- DEBS CSV columns: `ID.[Exchange]` (symbol), `SecType` (sectype), `Last` (last), `Trading time`, `Date`
- OTLP metric: Gauge `financial.last_trade_price`; value = `last`; attributes `symbol`, `exchange` (suffix of ID, e.g. `.NL` → `NL`), `sectype`; timestamp from `Date` + `Trading time` (CEST, `Europe/Berlin`)
- Windows: 5-minute tumbling (300 s), clock-aligned to Berlin local wall clock

**Approach:**
- Use `ddsketchprocessor` or `kllprocessor`, `mode: window`, `window_duration: 300s`, `aggregate_by: [symbol]`, `quantiles: [0.5]`
- Set `drop_original: false` during validation to retain raw gauges alongside sketch output
- Set `transmit_sketch: false` if downstream requires gauge-compatible output
- Controller: `aggregations: ["quantile"]`

**Validation:**  
- **Ground truth:** CSV → exact EMA per symbol per window.  
- **Sketch path:** replay OTLP → scrape quantiles/counts; compare to EMA from raw if retained.  
- **Metrics:** abs/rel error on EMA (or proxy agreement).  
- **Success:** rel error < 1% for ≥95% of (symbol, window); no missing windows; ordering sane.

**Evaluation configuration:**

| Parameter | Value |
|-----------|-------|
| Dataset | `data_filtered` |
| Window size | 5-min (300 s) |
| Avg samples / window (global, all symbols) | ~61K |
| Avg samples / window (per symbol) | ~12 |
| Unique series (active per day) | 5178 |
| Cross-series aggregation | None — independent per-symbol sketches |
| Test types | **Sketch-finance** (ddsketch/kll quantiles as EMA proxy) + **Latency** |

**References:**

- [DEBS 2022 call for solutions — **Query 1** (exponential moving average trend indicators, 5-minute windows)](https://2022.debs.org/call-for-grand-challenge-solutions/)  
- [arXiv:2206.13237 — Grand Challenge **Query 1** specification](https://arxiv.org/abs/2206.13237)  
- [Wikipedia — *Moving average* § Exponential moving average](https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average)

---

## Q2 - DEBS EMA crossover (buy/sell advice)

**Purpose:** Bullish when $\mathrm{EMA}_{38}-\mathrm{EMA}_{100}$ crosses from ≤0 to >0; bearish when ≥0 to <0. Tests chaining Q1→Q2 and no duplicate signals.

**Formula:** $\mathrm{diff}_t = \mathrm{EMA}_{38,t}-\mathrm{EMA}_{100,t}$; bullish: $\mathrm{diff}_{t-1}\le 0 \land \mathrm{diff}_t>0$; bearish: $\mathrm{diff}_{t-1}\ge 0 \land \mathrm{diff}_t<0$.

**Data requirements:**
- Inputs: per symbol, ordered by event time — EMA(38) and EMA(100) at each 5-minute window boundary (from pipeline/Prometheus scrape or recomputed from ticks)
- If recomputing from ticks — DEBS CSV columns: `ID.[Exchange]`, `SecType`, `Last`, `Trading time`, `Date`
- OTLP metric: Gauge `financial.last_trade_price` (value = `last`, labels `symbol`, `exchange`, `sectype`, timestamp from `Date` + `Trading time`); 5-minute tumbling windows for per-window EMA outputs

**Approach:**
- Downstream script on Prometheus export or CSV — not a built-in processor
- Per symbol: compare sign of `EMA_38 − EMA_100` across consecutive windows
- Emit bullish signal when diff crosses from ≤0 to >0; emit bearish signal when ≥0 to <0
- Deduplicate: suppress repeated signal if same direction is already active
- Optional: future custom processor for in-collector crossover detection

**Validation:**  
- **Ground truth:** crossovers from ground-truth EMAs.  
- **Sketch path:** crossovers from sketch/raw-derived EMAs.  
- **Metrics:** precision, recall, F1; duplicate count; optional time tolerance (e.g. ±5 min).  
- **Success:** precision > 90%, recall > 90%, zero duplicate alerts.

**Evaluation configuration:**

| Parameter | Value |
|-----------|-------|
| Dataset | `data_filtered` |
| Window size | 5-min (derived from Q1 EMA outputs) |
| Avg samples / window (global, all symbols) | ~61K (inherited from Q1) |
| Avg samples / window (per symbol) | ~12 (inherited from Q1) |
| Unique series (active per day) | 5178 |
| Cross-series aggregation | None — per-symbol sign-flip detection |
| Test types | **Throughput + Latency** (Sketch-finance: not applicable — exact sign-flip detection required) |

**References:**

- [DEBS 2022 call for solutions — **Query 2** (buy/sell advice from EMA crossover)](https://2022.debs.org/call-for-grand-challenge-solutions/)  
- [arXiv:2206.13237 — Grand Challenge **Query 2** specification](https://arxiv.org/abs/2206.13237)

---

## Q3 - Nexmark Q8 adapted (windowed top-K movers)

**Purpose:** Top-$K$ symbols per window by activity or price move; tests heavy-hitters + ranking under cardinality.

**Formula:** e.g. $\mathrm{score}(s,w)=\max_{t\in w} p_t - \min_{t\in w} p_t$, or $|p_{\mathrm{end}}-p_{\mathrm{start}}|$, or $\max p$; take top $K$ (e.g. $K=10$). Frequency-only variant: rank by event count.

**Data requirements:**
- DEBS CSV columns: `ID.[Exchange]`, `SecType`, `Last`, `Trading time`, `Date`
- OTLP metric: Gauge `financial.last_trade_price` (value = `last`, labels `symbol`, `exchange`, `sectype`); event time from `Date` + `Time` for the full feed (all row types) or `Date` + `Trading time` for the last-trade variant
- Windows: 5-minute tumbling; any row type counts for the frequency-only variant; only `Last`-populated rows count for the price-move variant

**Approach:**
- Use `countsketchprocessor`, `mode: window`, `window_size: 300s`, `aggregate_by: [symbol]`; tune `epsilon`/`delta` per README
- Controller: `aggregations: ["frequency"]`
- Downstream: sort CountSketch frequency estimates → take top-K (e.g. K=10) symbols per window
- For price-move score variant: retain raw `last` via `drop_original: false`; compute max − min per symbol in each window downstream

**Validation:**  
- **Ground truth:** per (window, symbol) counts and/or scores → top-$K$.  
- **Sketch path:** rank CountSketch frequency (or combined pipeline).  
- **Metrics:** top-$K$ set overlap, Spearman $\rho$, freq error on top-$K$.  
- **Success:** overlap ≥ 80%, $\rho$ > 0.7, freq rel error < 5% on top-$K$.

**Evaluation configuration:**

| Parameter | Value |
|-----------|-------|
| Dataset | `data` (full feed) |
| Window size | 5-min (300 s) |
| Avg samples / window (global, all symbols) | ~216K |
| Avg samples / window (per symbol) | ~39 |
| Unique series (active per day) | 5497 |
| Cross-series aggregation | **All 5502 series → top-K (K=10) per window** via CountSketch sort |
| Test types | **Sketch-finance** (CountSketch frequency ranking) + **Throughput** |

**References:**

- [Apache Beam — Nexmark suite: **Query 5** (*Hot items*), **Query 7** (*Highest bid*), **Query 8** (*Monitor new users*)](https://beam.apache.org/documentation/sdks/java/testing/nexmark/)  
- [GitHub — `nexmark/nexmark` (query definitions in repo)](https://github.com/nexmark/nexmark)

---

## Q4 - Per-symbol price stats (high / low / last / range)

**Purpose:** Window high, low, last, range; tests min/max style outputs vs sketches.

**Formula:** $\mathrm{high}=\max p_t$, $\mathrm{low}=\min p_t$, $\mathrm{last}=$ last tick in window, $\mathrm{range}=\mathrm{high}-\mathrm{low}$.

**Data requirements:**
- DEBS CSV columns: `ID.[Exchange]`, `SecType`, `Last`, `Trading time`, `Date`
- OTLP metric: Gauge `financial.last_trade_price` (value = `last`, labels `symbol`, `exchange`, `sectype`, timestamp from `Date` + `Trading time`)
- Windows: 5-minute tumbling per symbol for high/low/last/range aggregation

**Approach:**
- For approximate min/max: use `ddsketchprocessor` or `kllprocessor`, `mode: window`, `window_duration: 300s`, `aggregate_by: [symbol]`, `quantiles: [0.0, 1.0]` (or `[0.01, 0.99]` for robustness against extreme outliers)
- For exact min/max: use **NOP** processor and compute exact aggregates downstream from retained raw gauges
- Compute `range = high − low` downstream after scraping high/low from Prometheus
- Controller: `aggregations: ["quantile"]`

**Validation:**  
- **Ground truth:** exact min, max, last, range per (symbol, window).  
- **Sketch path:** compare to p00/p100 (or retained last).  
- **Metrics:** abs error on min/max; rel error on range.  
- **Success:** range rel error < 5% for ≥90% of cases; no absurd tails.

**Evaluation configuration:**

| Parameter | Value |
|-----------|-------|
| Dataset | `data_filtered` |
| Window size | 5-min (300 s) |
| Avg samples / window (global, all symbols) | ~61K |
| Avg samples / window (per symbol) | ~12 |
| Unique series (active per day) | 5178 |
| Cross-series aggregation | None — independent per-symbol sketches |
| Test types | **Sketch-finance** (ddsketch p0/p100 for approx min/max, exact NOP path) + **Throughput** |

**References:**

- [Tiger Data tutorial — *Analyze financial tick data* (OHLC / OHLCV aggregation)](https://docs.timescale.com/tutorials/latest/financial-tick-data/)

---

## Q5 - Realized volatility (per symbol, windowed)

**Purpose:** Dispersion of log returns; tests second-moment behavior and sparse windows.

**Formula:** $r_i=\ln(p_i/p_{i-1})$; $\sigma_w = \sqrt{\frac{1}{n-1}\sum(r_i-\bar r)^2}$.

**Data requirements:**
- DEBS CSV columns: `ID.[Exchange]`, `SecType`, `Last`, `Trading time`, `Date`
- OTLP metric: Gauge `financial.last_trade_price` (value = `last`, labels `symbol`, `exchange`, `sectype`, timestamp from `Date` + `Trading time`)
- Windows: 5-minute tumbling per symbol; volatility is computed from consecutive `last` prices ordered by event time within each window

**Approach:**
- For exact σ: use **NOP** processor; compute log returns $r_i = \ln(p_i/p_{i-1})$ for consecutive prices per symbol per window; compute sample std dev downstream
- For IQR proxy: use `ddsketchprocessor`, `mode: window`, `window_duration: 300s`, `aggregate_by: [symbol]`, `quantiles: [0.25, 0.75]`; estimate $\sigma \approx \mathrm{IQR}/1.349$ (normal approximation)
- Controller: `aggregations: ["quantile"]` for the sketch path

**Validation:**  
- **Ground truth:** $\sigma_w$ from ordered prices per window.  
- **Sketch path:** IQR-based estimate vs truth.  
- **Metrics:** abs/rel error on $\sigma$; optional regime bucket match.  
- **Success:** rel error < 10% for ≥90% of (symbol, window); regime accuracy > 85% if used.

**Evaluation configuration:**

| Parameter | Value |
|-----------|-------|
| Dataset | `data_filtered` |
| Window size | 5-min (300 s) |
| Avg samples / window (global, all symbols) | ~61K |
| Avg samples / window (per symbol) | ~12 |
| Unique series (active per day) | 5178 |
| Cross-series aggregation | None — per-symbol log-return sequences |
| Test types | **Sketch-finance** (ddsketch IQR proxy: σ ≈ IQR/1.349) + **Latency** |

**References:**

- [Wikipedia — *Volatility (finance)* (realized / historical volatility)](https://en.wikipedia.org/wiki/Volatility_(finance))

---

## Q6 - Distinct symbol cardinality (active per window)

**Purpose:** Count distinct symbols with ≥1 event in window; tests HLL.

**Formula:** $|\{s : \exists t\in w,\ \mathrm{event}(s,t)\}|$.

**Data requirements:**
- DEBS CSV columns: `ID.[Exchange]`, `Date`, `Time` (only symbol identity and event timestamp are needed; `Last` optional)
- OTLP metric: Gauge `financial.last_trade_price` with labels `symbol`, `exchange`, `sectype` on each point; value can be 1.0 or actual `last` if present; event time from `Date` + `Time` (full feed — all row types)
- Windows: 5-minute tumbling; aggregation counts **distinct `symbol` values per window** (global cardinality across all series, not per-symbol)

**Approach:**
- Use `hllprocessor`, `mode: window`, `window_duration: 300s`, precision = 14 (gives ~0.8% standard error)
- The HLL aggregates all arriving `symbol` labels across all 5502 series into a single distinct count per window
- Controller: `aggregations: ["cardinality"]`
- Downstream: compare HLL estimate to exact distinct-symbol count computed from raw data

**Validation:**  
- **Ground truth:** exact distinct count per window.  
- **Sketch path:** HLL estimate per window.  
- **Metrics:** abs/rel error.  
- **Success:** rel error < 2% per window; stable adjacent windows.

**Evaluation configuration:**

| Parameter | Value |
|-----------|-------|
| Dataset | `data` (full feed) |
| Window size | 5-min (300 s) |
| Avg samples / window (global, all symbols) | ~216K |
| Avg samples / window (per symbol) | N/A — global distinct count, not per-symbol |
| Unique series (active per day) | 5497 |
| Cross-series aggregation | **All 5502 series → 1 distinct count per window** via HLL |
| Test types | **Sketch-finance** (HLL cardinality estimation) + **Throughput** |

**References:**

- [Wikipedia — *HyperLogLog* (cardinality estimation)](https://en.wikipedia.org/wiki/HyperLogLog)  
- [Flajolet et al. — *HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm* (PDF)](https://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf)

---

## Q7 - TWAP (time-weighted average price, no volume)

**Purpose:** Simple average of ticks in window (DEBS has no volume for true VWAP).

**Formula:** $\mathrm{TWAP}_w = \frac{1}{n}\sum_{i=1}^n p_i$ (optional time-weighted variant with $\Delta t_i$ if you model durations).

**Data requirements:**
- DEBS CSV columns: `ID.[Exchange]`, `SecType`, `Last`, `Trading time`, `Date`
- OTLP metric: Gauge `financial.last_trade_price` (value = `last`, labels `symbol`, `exchange`, `sectype`, timestamp from `Date` + `Trading time`)
- Windows: 5-minute tumbling per symbol for mean/median price aggregation

**Approach:**
- Use `ddsketchprocessor`, `mode: window`, `window_duration: 300s`, `aggregate_by: [symbol]`, `quantiles: [0.5]` — p50 (median) serves as proxy to arithmetic mean for approximately symmetric price distributions
- Controller: `aggregations: ["quantile"]`
- Downstream: compare p50 from sketch to exact arithmetic mean per (symbol, window)
- Optional: retain raw gauges via `drop_original: false` for an exact mean benchmark alongside the sketch path

**Validation:**  
- **Ground truth:** arithmetic mean per (symbol, window).  
- **Sketch path:** p50 vs mean.  
- **Metrics:** abs/rel error; correlation mean vs median.  
- **Success:** rel error < 2% for ≥90% of windows; correlation > 0.95.

**Evaluation configuration:**

| Parameter | Value |
|-----------|-------|
| Dataset | `data_filtered` |
| Window size | 5-min (300 s) |
| Avg samples / window (global, all symbols) | ~61K |
| Avg samples / window (per symbol) | ~12 |
| Unique series (active per day) | 5178 |
| Cross-series aggregation | None — independent per-symbol sketches |
| Test types | **Sketch-finance** (ddsketch p50 as mean proxy) + **Throughput** |

**References:**

- [Wikipedia — *Time-weighted average price*](https://en.wikipedia.org/wiki/Time-weighted_average_price)

---

## Q8 - Price anomaly (z-score or IQR)

**Purpose:** Flag unusual prices vs window distribution.

**Formula:** $z_i=(p_i-\mu_w)/\sigma_w$; flag if $|z|>2.5$ (tune as needed). **IQR:** outlier if outside $[Q_1-1.5\,\mathrm{IQR},\, Q_3+1.5\,\mathrm{IQR}]$.

**Data requirements:**
- DEBS CSV columns: `ID.[Exchange]`, `SecType`, `Last`, `Trading time`, `Date`
- OTLP metric: Gauge `financial.last_trade_price` (value = `last`, labels `symbol`, `exchange`, `sectype`, timestamp from `Date` + `Trading time`)
- Windows: **15-minute tumbling** (900 s) — chosen over 5-min because per-symbol sample count is ~33 vs ~12, giving statistically meaningful IQR/z-score estimates

**Approach:**
- For exact z-score: use **NOP** processor; compute μ and σ per (symbol, window) downstream; flag $|z| > 2.5$ where $z_i = (p_i - \mu_w)/\sigma_w$
- For sketch-based IQR anomaly detection: use `ddsketchprocessor`, `mode: window`, `window_duration: 900s`, `aggregate_by: [symbol]`, `quantiles: [0.25, 0.5, 0.75]`; compute $\mathrm{IQR} = Q_3 - Q_1$; flag prices outside $[Q_1 - 1.5 \cdot \mathrm{IQR},\ Q_3 + 1.5 \cdot \mathrm{IQR}]$
- Controller: `aggregations: ["quantile"]`

**Validation:**  
- **Ground truth:** z-score flags from raw.  
- **Sketch path:** IQR flags vs ground truth.  
- **Metrics:** precision, recall, F1.  
- **Success:** precision > 70%, recall > 80%, F1 > 0.75 (tunable).

**Evaluation configuration:**

| Parameter | Value |
|-----------|-------|
| Dataset | `data_filtered` |
| Window size | **15-min** (900 s) — wider window for statistically meaningful per-symbol samples |
| Avg samples / window (global, all symbols) | ~172K |
| Avg samples / window (per symbol) | ~33 |
| Unique series (active per day) | 5178 |
| Cross-series aggregation | None — per-symbol IQR/z-score |
| Test types | **Sketch-finance** (ddsketch quantiles [0.25, 0.5, 0.75] for IQR anomaly flags) + **Latency** |

**References:**

- [Google Cloud Dataflow docs — *Anomaly detection with z-score* (Apache Beam notebook)](https://cloud.google.com/dataflow/docs/notebooks/anomaly_detection_zscore)  
- [Wikipedia — *Interquartile range* (Tukey fences / outlier rule)](https://en.wikipedia.org/wiki/Interquartile_range)

---

## Q9 - Bollinger bands (breakout)

**Purpose:** Band around SMA ± $k\sigma$; breakout beyond band.

**Formula:** $\mathrm{Middle}=\mathrm{SMA}_n$, $\mathrm{Upper}=\mathrm{SMA}_n+k\sigma_n$, $\mathrm{Lower}=\mathrm{SMA}_n-k\sigma_n$ (often $k=2$).

**Data requirements:**
- DEBS CSV columns: `ID.[Exchange]`, `SecType`, `Last`, `Trading time`, `Date`
- OTLP metric: Gauge `financial.last_trade_price` (value = `last`, labels `symbol`, `exchange`, `sectype`, timestamp from `Date` + `Trading time`)
- Windows: rolling span of N=3 consecutive 5-minute bars per symbol (15-min effective lookback); SMA and σ are recomputed at each new bar

**Approach:**
- Use **NOP** processor to retain raw `last` gauges per symbol — no single-window sketch can replace sequential SMA/σ across multiple bars
- Downstream per symbol: collect `last` prices in each 5-min tumbling window; maintain a rolling buffer of N=3 windows
- Compute $\mathrm{SMA}_N$ = mean of all prices across the N most recent windows; compute $\sigma_N$ = sample std dev of the same price set
- Derive Bollinger bands: $\mathrm{Upper} = \mathrm{SMA}_N + k \cdot \sigma_N$, $\mathrm{Lower} = \mathrm{SMA}_N - k \cdot \sigma_N$ (use $k=2$)
- Detect breakout events: flag any `last` price in the current window that falls outside $[\mathrm{Lower},\ \mathrm{Upper}]$

**Validation:**
- **Ground truth:** compute exact SMA_N and σ_N per (symbol, window) from ordered raw prices; derive exact band values and breakout flags.
- **Metrics:** compare band midpoint, width, and breakout event set to ground truth; define thresholds explicitly.
- **Success:** band midpoint rel error < 1%; breakout event precision > 80%, recall > 80%.

**Evaluation configuration:**

| Parameter | Value |
|-----------|-------|
| Dataset | `data_filtered` |
| Window size | 5-min (300 s); SMA/σ span = N=3 consecutive bars (15-min lookback) |
| Avg samples / window (global, all symbols) | ~61K per 5-min bar |
| Avg samples / window (per symbol) | ~12 per 5-min bar |
| Unique series (active per day) | 5178 |
| Cross-series aggregation | None — per-symbol rolling band computation |
| Test types | **Throughput + Latency** (Sketch-finance: not applicable — rolling sequential SMA/σ required) |

**References:**

- [Wikipedia — *Bollinger Bands*](https://en.wikipedia.org/wiki/Bollinger_Bands)

---

## Q10 - RSI

**Purpose:** Momentum 0–100 from avg gain/loss (typically 14 periods).

**Formula:** $\mathrm{RSI}=100-100/(1+\mathrm{RS})$, $\mathrm{RS}=\mathrm{avg\_gain}/\mathrm{avg\_loss}$ (EMA-smoothed).

**Data requirements:**
- DEBS CSV columns: `ID.[Exchange]`, `SecType`, `Last`, `Trading time`, `Date`
- OTLP metric: Gauge `financial.last_trade_price` (value = `last`, labels `symbol`, `exchange`, `sectype`, timestamp from `Date` + `Trading time`)
- Processing: per-symbol stream ordered by event time; RSI requires a lookback of n=14 consecutive price changes — state accumulates across the full stream, not isolated per window

**Approach:**
- Use **NOP** processor or downstream computation from raw CSV — not suited to single-window sketch aggregation
- Per symbol: compute price changes $\Delta p_i = p_i - p_{i-1}$ for each consecutive pair of `last` prices
- Separate gains ($\Delta p_i > 0$) and losses ($\Delta p_i < 0$)
- Apply Wilder's EMA-smoothed average: $\overline{g}_t = \alpha \cdot g_t + (1-\alpha) \cdot \overline{g}_{t-1}$, $\alpha = 1/14$; same for losses
- $\mathrm{RS} = \overline{g}/\overline{l}$; $\mathrm{RSI} = 100 - 100/(1 + \mathrm{RS})$
- Emit overbought signal when RSI > 70, oversold when RSI < 30

**Validation:** Ground truth from CSV vs pipeline output; metrics: value error and/or overbought/oversold event match.

**Evaluation configuration:**

| Parameter | Value |
|-----------|-------|
| Dataset | `data_filtered` |
| Window size | N/A — per-symbol full stream state (n=14 price-change lookback) |
| Avg samples / window (global, all symbols) | N/A |
| Avg samples / window (per symbol) | N/A |
| Unique series (active per day) | 5178 |
| Cross-series aggregation | None — per-symbol momentum state |
| Test types | **Throughput + Latency** (Sketch-finance: not applicable — sequential lookback state required) |

**References:**

- [Wikipedia — *Relative strength index*](https://en.wikipedia.org/wiki/Relative_strength_index)

---

## Q11 - MACD

**Purpose:** $\mathrm{MACD}=\mathrm{EMA}_{12}-\mathrm{EMA}_{26}$, $\mathrm{Signal}=\mathrm{EMA}_9(\mathrm{MACD})$.

**Data requirements:**
- DEBS CSV columns: `ID.[Exchange]`, `SecType`, `Last`, `Trading time`, `Date`
- OTLP metric: Gauge `financial.last_trade_price` (value = `last`, labels `symbol`, `exchange`, `sectype`, timestamp from `Date` + `Trading time`)
- Processing: per-symbol ordered ticks; MACD is a chain of EMAs on prices and on the MACD line — state carries across the full stream

**Approach:**
- Use **NOP** processor or downstream computation from raw CSV — single-window sketches are insufficient for chained EMA state
- Per symbol, maintain three running EMA states: EMA_12 ($\alpha = 2/13$), EMA_26 ($\alpha = 2/27$), and EMA_9 on the MACD line ($\alpha = 2/10$)
- On each new `last` price: update EMA_12 and EMA_26; MACD line = EMA_12 − EMA_26
- Update Signal line = EMA_9 applied to MACD line values; Histogram = MACD − Signal
- Emit MACD, Signal, and Histogram per symbol at each event tick or at 5-min window boundaries

**Validation:** Compare MACD/signal/histogram to ground truth from CSV; define acceptable error.

**Evaluation configuration:**

| Parameter | Value |
|-----------|-------|
| Dataset | `data_filtered` |
| Window size | N/A — per-symbol full stream state (EMA chain across full stream) |
| Avg samples / window (global, all symbols) | N/A |
| Avg samples / window (per symbol) | N/A |
| Unique series (active per day) | 5178 |
| Cross-series aggregation | None — per-symbol EMA chain |
| Test types | **Throughput + Latency** (Sketch-finance: not applicable — chained EMA state required) |

**References:**

- [Wikipedia — *MACD*](https://en.wikipedia.org/wiki/MACD)

---

## Q12 - Stochastic oscillator (%K / %D)

**Purpose:** Position of price within recent high–low range.

**Formula:** $\%K=100\cdot(C-L_n)/(H_n-L_n)$; $\%D=\mathrm{SMA}_3(\%K)$.

**Data requirements:**
- DEBS CSV columns: `ID.[Exchange]`, `SecType`, `Last`, `Trading time`, `Date`
- OTLP metric: Gauge `financial.last_trade_price` (value = `last`, labels `symbol`, `exchange`, `sectype`, timestamp from `Date` + `Trading time`)
- Processing: per symbol, rolling window of n=14 ticks (or n=14 bars) to find the highest high $H_n$ and lowest low $L_n$ in that span

**Approach:**
- Use **NOP** processor or downstream computation from raw CSV — quantile sketches only approximate extrema, producing incorrect %K/%D
- Per symbol: maintain a sliding deque of the n=14 most recent `last` prices; $H_n = \max(\mathrm{deque})$, $L_n = \min(\mathrm{deque})$
- $\%K = 100 \cdot (C - L_n) / (H_n - L_n)$, where $C$ = current `last` price
- Maintain a rolling buffer of 3 %K values; $\%D = \mathrm{SMA}_3(\%K)$ (3-period simple moving average of %K)
- Emit %K and %D per symbol at each tick or at window boundaries

**Validation:** Ground truth from CSV vs pipeline; error on %K/%D or zone crossings.

**Evaluation configuration:**

| Parameter | Value |
|-----------|-------|
| Dataset | `data_filtered` |
| Window size | N/A — per-symbol rolling n=14 ticks (or n bars) for H/L lookback |
| Avg samples / window (global, all symbols) | N/A |
| Avg samples / window (per symbol) | N/A |
| Unique series (active per day) | 5178 |
| Cross-series aggregation | None — per-symbol H/L range tracking |
| Test types | **Throughput + Latency** (Sketch-finance: not applicable — exact min/max over rolling window required) |

**References:**

- [Investopedia — *Stochastic oscillator* (definition and %K / %D)](https://www.investopedia.com/terms/s/stochasticoscillator.asp)  
- [Wikipedia — *Stochastic oscillator* § Calculation](https://en.wikipedia.org/wiki/Stochastic_oscillator)
