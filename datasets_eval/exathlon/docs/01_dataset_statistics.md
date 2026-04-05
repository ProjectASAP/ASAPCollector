# exathlon — Dataset Statistics

## Dataset description

The labeled Exathlon dataset was built by recording repeated executions of 10 different Spark streaming applications on a 4-node cluster. Each application processed user click streams from the WorldCup 1998 Website at a given input rate, formatted as `(user_id, timestamp, url)` tuples and replicated with a scale factor for long-running applications. Some applications also used join data stored in HDFS, in the form of `(page_rank - - url)` records.

---

## Dataset fields

| Field | Meaning |
|--------|---------|
| `t` | Event timestamp (epoch seconds) |
| `<metric columns...>` | Time-series metric values collected from Spark app telemetry + HPC node telemetry |

**Corpus**

- [Jacob et al. — *Exathlon: A Benchmark for Explainable Anomaly Detection over Time Series*, VLDB 2021](https://doi.org/10.14778/3476249.3476307)
- [GitHub — exathlonbenchmark/exathlon (dataset, ground-truth labels, benchmark code)](https://github.com/exathlonbenchmark/exathlon)

### Notes

- This corpus is multi-variate telemetry: one timestamp column (`t`) and thousands of metric columns per file.
- All analysis below uses files under `exathlon/data/raw/app*/`.
- Files are named `{app_id}_{fault_type}_{target_length}_{run_id}.csv`; fault type `0` = baseline (no injection), non-zero = injected fault type.
- Time-window tests are run on: `1min`, `5min`, `15min`, `30min`, `1hour`.

---

## Evaluation streams and layout

One logical stream is used:

| Stream | Directory | Purpose |
|--------|-----------|---------|
| **Raw telemetry stream** | `exathlon/data/raw/` | Primary source for frequency, window density, cardinality, and sketch-suitable aggregation analysis. |

**Analysis scripts and outputs**

- Code: `analysis/code/`
- Summaries: `analysis/results/summaries/`
- Per-window details: `analysis/results/detailed_windows/`
- `analyze_frequency.py` computes inter-arrival gap statistics (min, max, mean in seconds) on the sorted `t` column per file.
- `analyze_windows.py` counts rows per tumbling window (1min, 5min, 15min, 30min, 1hour) using `t`.
- `analyze_cardinality.py` reports distinct `time_series`, `entity`, `metric_base`, and `aggregation_suffix` dimensions.

**Commands**

```bash
cd DataCollector/datasets_eval/exathlon/analysis/code
python3 analyze_frequency.py
python3 analyze_windows.py
python3 analyze_cardinality.py
```

---

## Time windows (exathlon)

- **Type:** non-overlapping tumbling windows
- **Sizes tested:** `1min`, `5min`, `15min`, `30min`, `1hour`
- **Alignment:** starts from first timestamp in each file, then fixed-size strides

---

## Evaluation Configuration Matrix

All 93 files span 10 Spark applications (app1–app10), each with baseline runs (fault type `0`) and injected-fault runs (types 1–5). Numbers below are sourced from `analysis/results/summaries/`.

### Dataset statistics — `exathlon/data/raw/` (telemetry stream)

Sourced from `analysis/results/summaries/`.

**Frequency** (`frequency_summary.csv`) — inter-arrival time in seconds on sorted `t` per file (representative rows, one per app):

| file | samples | mean\_interval\_s | max\_interval\_s | mean\_frequency\_hz |
|------|--------:|------------------:|-----------------:|--------------------:|
| 1\_0\_1000000\_14.csv | 14,347 | 1.003137 | 4.0 | 0.996873 |
| 2\_0\_100000\_20.csv | 28,636 | 1.003143 | 2.0 | 0.996867 |
| 3\_0\_100000\_24.csv | 28,641 | 1.005237 | 61.0 | 0.994790 |
| 4\_0\_1000000\_31.csv | 7,170 | 1.003069 | 2.0 | 0.996941 |
| 5\_0\_100000\_33.csv | 28,704 | 1.003031 | 2.0 | 0.996978 |
| 6\_0\_100000\_42.csv | 28,669 | 1.003105 | 5.0 | 0.996905 |
| 7\_0\_100000\_53.csv | 28,703 | 1.003066 | 2.0 | 0.996943 |
| 8\_3\_200000\_73.csv | 46,641 | 1.003130 | 2.0 | 0.996879 |
| 9\_0\_100000\_1.csv | 28,688 | 1.003590 | 6.0 | 0.996422 |
| 10\_0\_100000\_10.csv | 14,311 | 1.003215 | 6.0 | 0.996796 |

Global across all 93 files:
- Samples per file (min / median / max): **2,474 / 28,669 / 129,197**
- Mean interval in seconds (min / median / max): **1.002397 / 1.003141 / 1.005237**
- Global minimum interval: **0.0 s** (duplicate/near-simultaneous timestamps exist)
- Global maximum interval: **61.0 s** (`10_0_100000_9.csv` and others)

**Window summary** (`window_summary.csv`) — all 93 files:

| window\_size | total\_windows (min–max) | avg\_samples (min / median / max) | min\_samples\_global | max\_samples\_global |
|---|---|---|---:|---:|
| 1min | 42–2160 | 58.653061 / 59.758333 / 59.848611 | 2 | 75 |
| 5min | 9–432 | 272.100000 / 298.500000 / 299.243056 | 7 | 323 |
| 15min | 3–144 | 680.250000 / 895.500000 / 897.729167 | 8 | 898 |
| 30min | 2–72 | 1204.666667 / 1791.000000 / 1795.458333 | 8 | 1,796 |
| 1hour | 1–36 | 1807.000000 / 3582.000000 / 3590.916667 | 8 | 3,592 |

Representative 5-min rows:

| file | total\_windows | avg\_samples | min\_samples | max\_samples |
|------|---------------:|-------------:|-------------:|-------------:|
| 1\_0\_1000000\_14.csv | 48 | 298.895833 | 291 | 300 |
| 2\_0\_100000\_20.csv | 96 | 298.291667 | 225 | 300 |
| 3\_0\_100000\_24.csv | 96 | 298.343750 | 239 | 300 |
| 4\_0\_1000000\_31.csv | 24 | 298.750000 | 291 | 300 |
| 5\_0\_100000\_33.csv | 96 | 299.000000 | 290 | 300 |
| 6\_0\_100000\_42.csv | 96 | 298.635417 | 258 | 300 |
| 7\_0\_100000\_53.csv | 96 | 298.989583 | 291 | 300 |
| 8\_3\_200000\_73.csv | 156 | 298.980769 | 286 | 300 |
| 9\_0\_100000\_1.csv | 96 | 298.833333 | 290 | 300 |
| 10\_0\_100000\_10.csv | 48 | 298.145833 | 256 | 300 |

**Cardinality** (`cardinality_overall.csv`):

| dimension | unique\_count\_global |
|---|---:|
| time\_series | 3,939 |
| entity | 10 |
| metric\_base | 3,629 |
| aggregation\_suffix | 13 |

Per file (`cardinality_summary.csv`):

| dimension | unique\_count\_per\_file |
|---|---:|
| time\_series\_cardinality | 2,283 |
| entity\_cardinality | 10 |
| metric\_base\_cardinality | 1,973 |

---

### Per-query evaluation matrix

**Benchmark baselines**

- **Sketch-telemetry baseline:** raw stream (`exathlon/data/raw/`), all 93 files, 5-min tumbling windows, ~298.5 rows/window (per-file median), mean inter-arrival ~1.003 s.
- **Latency baseline:** same stream and windows; latency measured on sketch construction and quantile query paths (Q1, Q5, Q8).
- **Throughput baseline:** cross-series aggregation paths (Q3, Q6) under 2,283 active series/file and 3,939 global.

**Derived per-entity averages**

- 5-min window: ~298.5 rows/file ÷ 10 entities ≈ **~30 rows/entity/window** (across all metrics for that entity)
- 15-min window: ~895.5 rows/file ÷ 10 entities ≈ **~90 rows/entity/window**

| Q | Dataset | Window | Avg rows/window (per file) | Active series/file | Test types | Cross-series aggregation |
|---|---------|--------|---------------------------:|-------------------:|-----------|--------------------------|
| Q1 Windowed quantiles | `exathlon/data/raw` | 5-min (primary) | ~298.5 | 2,283 | Sketch-telemetry + Latency | None — per (entity, metric\_base) |
| Q2 Tail amplification ratio | `exathlon/data/raw` | 5-min (derived from Q1) | ~298.5 (inherited) | 2,283 | Sketch-telemetry + Latency | None — per metric |
| Q3 Top-K heavy metrics | `exathlon/data/raw` | 5-min | ~298.5 | 2,283 | Sketch-telemetry + Throughput | All metrics → top-K (K=10) via CMS+SpaceSaving |
| Q4 Min/max/range | `exathlon/data/raw` | 1/5/15/30/60-min | varies | 2,283 | Sketch-telemetry + Throughput | None — per metric |
| Q5 IQR anomaly flags | `exathlon/data/raw` | **15-min** primary, 5-min compare | ~895.5 / ~298.5 | 2,283 | Sketch-telemetry + Latency | None — per metric |
| Q6 Distinct active metrics | `exathlon/data/raw` | 5-min | ~298.5 | 3,939 (global) | Sketch-telemetry + Throughput | All series → 1 count/window via HLL |
| Q7 Top-K entities by anomaly | `exathlon/data/raw` | 5-min and 15-min | ~298.5 / ~895.5 | 10 entities | Sketch-telemetry + Throughput | All entities → top-K (K=3) via CMS+SpaceSaving |
| Q8 Quantile drift | `exathlon/data/raw` | 5-min primary, 1-min stress | ~298.5 / ~59.8 | 2,283 | Sketch-telemetry + Latency | None — per metric |
| Q9 Saturation ratio | `exathlon/data/raw` | 5-min and 15-min | ~298.5 / ~895.5 | 2,283 | Sketch-telemetry (Hybrid) + Throughput | Per entity (10 entities) |
| Q10 EWMA change-point | `exathlon/data/raw` | N/A (stateful stream) | N/A | N/A | Throughput + Latency | None — per KPI stream |
| Q11 Cross-metric correlation | `exathlon/data/raw` | 15/30-min rolling | ~895.5 / ~1,791.0 | 2,283 | Throughput + Latency | None — per entity metric pair |
| Q12 Composite health score | `exathlon/data/raw` | 1/5/15/30/60-min | varies | 2,283 | Composite (Q2+Q5+Q9) | Per entity, then global rank |

**Rationale for key choices**

- **Q5 — 15-min primary window:** at 5-min, ~298.5 rows per file are spread across 2,283 series; 15-min gives ~895 rows, producing more stable IQR-flag precision/recall estimates. 5-min is retained as a stress comparison.
- **Q6 — global series space:** HLL targets the global 3,939-series space; per-file analysis (2,283 series) is used only to bound the active-series floor.
- **Q7 — K=3:** with only 10 distinct entity IDs, using K=3 meaningfully stresses CMS+SpaceSaving over a small universe without trivially ranking all entities.
- **Q10–Q11 — no sketch test:** sequential stateful computations (EWMA state, rolling Pearson/Spearman) cannot be replaced by single-window sketches. Benchmarking value is the cost of exact stateful stream processing.

---

## Research question answers

1. **What's the time interval between two data points?**  
   Around **1 second** (mean interval per file mostly `1.002–1.005 s`), so this dataset is high-frequency enough for minute-level window analytics.

2. **What's the cardinality of total time series?**  
   Global unique time series cardinality is **3,939** (`cardinality_overall.csv`), with **2,283** series columns present per file.

3. **What aggregation queries supported by sketches can potentially run over each dataset?**  
   For this dataset domain, sketch-friendly workloads include:
   - Distribution/quantile tracking (`KLL`, `DDSketch`)
   - Heavy-hitter/top-k detection (`Count-Min + SpaceSaving`)
   - Distinct-count activity tracking (`HyperLogLog`)
   - Windowed group-by aggregations over node/executor entities.
