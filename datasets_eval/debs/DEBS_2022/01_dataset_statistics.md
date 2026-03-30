# DEBS 2022 — Dataset Statistics

## Dataset fields

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
- The **frequency** and **window** analysis scripts under [`analysis/code/`](../code/) use **`Date` + `Time`** so stats reflect the **full feed**; use **`Date` + `Trading time`** for OTLP `financial.last_trade_price` timestamps when emitting last-trade events.

---

## Evaluation streams and layout

Two logical streams are used:

| Stream | Directory | Purpose |
|--------|-----------|---------|
| **Full feed** | [`data/`](../data/) | Raw daily CSVs as published. Use for **throughput**, parsers, and any query variant where an **event** is **any row** (e.g. cardinality / activity-style tests). |
| **Price (last-trade) stream** | [`data_filtered/`](../data_filtered/) | Rows with both **`Last`** and **`Trading time`** non-empty. Built with [`analysis/code/filter_data.py`](../code/filter_data.py). Use for **Q1–Q5, Q7–Q12** and any logic defined on **`last`** with last-trade times. |

**Analysis and query tooling**

- Python scripts in [`analysis/code/`](../code/) accept **`--dataset data`** (default) or **`--dataset data_filtered`**.
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
