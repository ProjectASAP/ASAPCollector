# DEBS 2022 — Benchmark Methodology

## 1. Overview

This document describes the end-to-end methodology used to benchmark the **DataCollector** against the DEBS 2022 Grand Challenge financial tick dataset. The benchmarks evaluate three dimensions:

- **Throughput** — how many events per second can the pipeline ingest and process
- **Latency** — end-to-end delay from event emission to sketch output availability
- **Financial Statistics Validation (Sketch-finance)** — accuracy of sketch-based aggregations compared to exact ground-truth computations

Dataset statistics and stream properties are documented in [`01_dataset_statistics.md`](01_dataset_statistics.md). Per-query definitions, sketch assignments, and success criteria are documented in [`02_benchmark_queries.md`](02_benchmark_queries.md).

---

## 2. Data Ingestion — DEBS CSV → OTLP

The DEBS 2022 dataset is distributed as daily CSV files. A **replay script** converts each row into an OTLP metric data point and sends it to the DataCollector.

**Field mapping:**

| CSV column | OTLP field |
|------------|------------|
| `ID.[Exchange]` stem (e.g. `RDSA`) | attribute `symbol` |
| `ID.[Exchange]` suffix (e.g. `NL`) | attribute `exchange` |
| `SecType` | attribute `sectype` |
| `Last` | Gauge data point value |
| `Date` + `Trading time` (CEST, `Europe/Berlin`) | data point timestamp (filtered stream) |
| `Date` + `Time` | data point timestamp (full feed) |

**Metric name:** `financial.last_trade_price` (Gauge)

**Two replay streams are used depending on the query:**

| Stream | Source directory | Rows included | Used for |
|--------|-----------------|---------------|----------|
| Full feed | `data/` | All row types (quotes, trades, indices) | Q3, Q6 — throughput/cardinality queries |
| Last-trade stream | `data_filtered/` | Rows with both `Last` and `Trading time` non-empty | Q1, Q2, Q4, Q5, Q7–Q12 — price-based queries |

Each row is emitted as an OTLP `ExportMetricsServiceRequest` via gRPC or HTTP to the DataCollector's OTLP receiver endpoint.

---

## 3. DataCollector Connector

The DataCollector acts as an **OpenTelemetry Collector** with a custom OTLP receiver. On receiving each `ExportMetricsServiceRequest`, it:

1. Parses the incoming Gauge data points for `financial.last_trade_price`
2. Extracts attributes (`symbol`, `exchange`, `sectype`) and the data point timestamp
3. Routes the data points through the processor pipeline configured by the active plan

---

## 4. Controller / Plan — Sketch Selection

Before a benchmark run, a **plan** is submitted to the DataCollector controller, which determines which sketch processor is activated for the metric.

**Submit a plan:**

```http
POST /api/v1/plan
{
  "metric_name": "financial.last_trade_price",
  "aggregations": ["quantile"],        // or "frequency", "cardinality"
  "time_window": "5m",
  "workload_hints": { ... }
}
```

**Inspect active config:**

```http
GET /api/v1/config/financial.last_trade_price
```

**Aggregation → processor mapping:**

| `aggregations` value | Processor activated | Sketch type |
|----------------------|--------------------|-----------  |
| `"quantile"` | `ddsketchprocessor` or `kllprocessor` | DDSketch / KLL quantile sketch |
| `"frequency"` | `countsketchprocessor` | Count-Min / Count Sketch |
| `"cardinality"` | `hllprocessor` | HyperLogLog |
| *(none / exact path)* | NOP processor | No sketch — raw gauges retained |

---

## 5. Per-Query Sketch Assignment

| Query | Processor | Aggregation | Notes |
|-------|-----------|-------------|-------|
| Q1 EMA indicators | `ddsketchprocessor` / `kllprocessor` | `quantile` | p50 as EMA proxy per symbol |
| Q2 EMA crossover | NOP | — | Exact sign-flip on Q1 EMA outputs |
| Q3 Top-K movers | `countsketchprocessor` | `frequency` | Heavy-hitter ranking across 5502 symbols |
| Q4 Price stats | `ddsketchprocessor` / NOP | `quantile` | p0/p100 for approx min/max; NOP for exact |
| Q5 Realized volatility | `ddsketchprocessor` / NOP | `quantile` | IQR proxy for σ; NOP for exact log-returns |
| Q6 Distinct cardinality | `hllprocessor` | `cardinality` | Global distinct symbol count per window |
| Q7 TWAP | `ddsketchprocessor` | `quantile` | p50 as mean proxy per symbol |
| Q8 Price anomaly | `ddsketchprocessor` / NOP | `quantile` | IQR [Q1, Q2, Q3] for outlier detection |
| Q9 Bollinger bands | NOP | — | Sequential SMA/σ across rolling N=3 bars |
| Q10 RSI | NOP | — | Per-symbol EMA-smoothed gain/loss state |
| Q11 MACD | NOP | — | Chained EMA_12, EMA_26, EMA_9 state |
| Q12 Stochastic | NOP | — | Exact min/max over rolling n=14 tick deque |

Full per-query detail (window sizes, evaluation configs, success criteria) is in [`02_benchmark_queries.md`](02_benchmark_queries.md).

---

## 6. Throughput Benchmark

**Goal:** Determine the maximum sustainable event ingestion and processing rate.

**Procedure:**

1. Configure the DataCollector with the target query's sketch processor (or NOP for exact-path queries)
2. Run the replay script in **max-speed mode** — no artificial throttle, events sent as fast as the network and receiver allow
3. Use the **full feed** (`data/`) for maximum load: ~216K events per 5-min window, ~1.5 ms mean inter-arrival
4. Run single-day replay (08-11-21) first, then multi-day sequential replay (08–12 Nov) to observe degradation under sustained load

**Metrics recorded:**

- Events ingested per second at the OTLP receiver
- Events processed per second through the sketch pipeline
- Peak throughput (burst) and sustained throughput (5-day replay)
- Backpressure signals or event drop indicators if any

---

## 7. Latency Benchmark

**Goal:** Measure the end-to-end pipeline delay at realistic event rates.

**Procedure:**

1. Configure the DataCollector with the target query's sketch processor
2. Run the replay script in **paced mode** — events sent at natural inter-arrival timing derived from the `Date`+`Time` timestamps in the CSV:
   - Full feed: ~1.5 ms mean inter-arrival
   - Filtered stream: ~6.9 ms mean inter-arrival
3. Record the timestamp when each event is emitted (OTLP send time) and when the sketch output becomes available (Prometheus scrape or collector export timestamp)

**Metrics recorded:**

- End-to-end latency distribution: p50, p95, p99
- Wall-clock pipeline delay: difference between OTLP send time and output availability
- Event-time lag: difference between the event's data timestamp and output timestamp

---

## 8. Financial Statistics Validation (Sketch-finance)

**Goal:** Verify that sketch outputs approximate exact financial metrics within acceptable error bounds.

**Procedure:**

For each query that uses a sketch processor (Q1, Q3–Q8), run two independent paths from the same CSV:

### Ground-truth path

1. Read the DEBS CSV directly (no replay, no OTLP)
2. Compute the exact financial metric per `(query, day, symbol, window)` using ordered prices:
   - Q1: exact EMA(38) and EMA(100) per symbol per 5-min window
   - Q3: exact event count and price-move score per symbol per 5-min window → top-K ranking
   - Q4: exact min, max, last, range per symbol per 5-min window
   - Q5: exact realized volatility (sample std dev of log returns) per symbol per 5-min window
   - Q6: exact distinct symbol count per 5-min window
   - Q7: exact arithmetic mean per symbol per 5-min window
   - Q8: exact z-score flags per symbol per 15-min window
3. **Persist results to disk** under `results/ground_truth/<query>/` (e.g. as CSV or Parquet files keyed by `(day, symbol, window_start)`) — computed once, reused for all future benchmark runs without reprocessing the raw data

### Sketch path

1. Replay the same CSV day as OTLP to the DataCollector at natural pace
2. Scrape sketch outputs from Prometheus or the collector's export endpoint at each window boundary
3. **Persist sketch outputs to disk** under `results/sketch_output/<query>/` in the same key structure as the ground-truth files

### Comparison

Load both stored files and compute per-query error metrics as defined in [`02_benchmark_queries.md`](02_benchmark_queries.md):

| Query | Primary metric | Success threshold |
|-------|---------------|-------------------|
| Q1 | Rel error on EMA proxy | < 1% for ≥95% of (symbol, window) |
| Q3 | Top-K set overlap, Spearman ρ | Overlap ≥ 80%, ρ > 0.7 |
| Q4 | Rel error on range | < 5% for ≥90% of cases |
| Q5 | Rel error on σ | < 10% for ≥90% of (symbol, window) |
| Q6 | Rel error on distinct count | < 2% per window |
| Q7 | Rel error on mean | < 2% for ≥90% of windows |
| Q8 | Precision / Recall / F1 on anomaly flags | Precision > 70%, Recall > 80%, F1 > 0.75 |

This run is performed at **natural pace on a single day** to isolate sketch accuracy from load effects.

---

## 9. Combined Run (Accuracy Under Load)

**Goal:** Confirm that sketch accuracy holds when the pipeline is under throughput stress.

**Procedure:**

1. Repeat the throughput benchmark (Section 6) with the same sketch configuration used during validation
2. Collect sketch outputs during the max-speed replay
3. Compare to the stored ground-truth files from Section 8
4. Record any degradation in sketch quality metrics relative to the natural-pace baseline

This serves as a secondary accuracy-under-load metric and can reveal issues such as event reordering, window misalignment, or sketch merging errors at high ingestion rates.

---

## 10. Pass/Fail Thresholds

Full per-query success criteria are defined in each query section of [`02_benchmark_queries.md`](02_benchmark_queries.md). The summary in Section 8 above lists the primary thresholds. A benchmark run **passes** when all applicable thresholds are met for the target query.

---

## 11. Environment

> *To be filled in before benchmark execution.*

| Item | Value |
|------|-------|
| CPU | — |
| RAM | — |
| OS | — |
| Go version | — |
| DataCollector version / commit | — |
| Prometheus version | — |
| Replay script version | — |
| DEBS dataset version | [Zenodo 6382482](https://doi.org/10.5281/zenodo.6382482) |

---

## 12. Results

> *To be filled in after benchmark execution.*

Results files will be stored at:

```
results/
├── ground_truth/
│   ├── Q1/   ← exact EMA per (day, symbol, window)
│   ├── Q3/   ← exact top-K per (day, window)
│   ├── Q4/   ← exact min/max/last/range per (day, symbol, window)
│   ├── Q5/   ← exact volatility per (day, symbol, window)
│   ├── Q6/   ← exact distinct count per (day, window)
│   ├── Q7/   ← exact mean per (day, symbol, window)
│   └── Q8/   ← exact anomaly flags per (day, symbol, window)
├── sketch_output/
│   └── <same structure as ground_truth/>
└── comparison/
    └── <error metric summaries per query>
```
