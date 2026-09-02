# DEBS 2022 — Benchmark Methodology

## 1. Overview

This document describes the end-to-end methodology used to benchmark the **DataCollector** against the DEBS 2022 Grand Challenge financial tick dataset. The benchmark focuses on **Financial Statistics Validation (Sketch-finance)** — verifying that sketch-based aggregations approximate exact financial metrics within acceptable error bounds.

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

**Submit a plan (benchmark branches — `debs_q*`):**

The benchmark run scripts (`datasets_eval/debs/benchmark/run.py`) use the named-aggregation API:

```http
POST /api/v1/plan
{
  "metric_name": "financial.last_trade_price",
  "aggregations": ["quantile"],
  "time_window": "5m",
  "group_by_labels": ["symbol"],
  "accuracy_sla": 0.01,
  "file_output_path": "/path/to/results/sketch_output/Q1/08-11-21.jsonl",
  "workload": { "series_count": 6000, "samples_per_sec_per_series": 100, ... }
}
```

The `file_output_path` field tells the controller to configure the collector's **file exporter** — sketch window outputs are written as OTLP-JSON lines (one line per window flush) to the given path. Each line is a standard OTLP `ExportMetricsServiceRequest` JSON object containing `resourceMetrics` → `scopeMetrics` → `metrics` → `dataPoints`.

**Submit a plan (canonical planner test — `run_planner_queries.py`):**

The canonical test script uses the `query_string` API:

```http
POST /api/v1/plan
{
  "query_string": "<SQL or PromQL>",
  "accuracy_sla": 0.01,
  "workload": {
    "series_count": 5178,
    "samples_per_sec_per_series": 1.0,
    "bytes_per_raw_sample": 100,
    "data_distribution": "zipf"
  }
}
```

**Inspect active collector config:**

```http
GET /api/v1/config/financial.last_trade_price
```

**Aggregation → processor mapping:**

| `aggregations` value | Processor activated | Sketch type |
|----------------------|--------------------|-----------  |
| `"quantile"` | `asap_edge (`family: ddsketch`)` or `asap_edge (`family: kll`)` | DDSketch / KLL quantile sketch |
| `"frequency"` | `asap_edge (`family: countsketch`)` | Count-Min / Count Sketch |
| `"cardinality"` | `asap_edge (`family: hll`)` | HyperLogLog |
| *(none / exact path)* | NOP processor | No sketch — raw gauges retained |

---

## 5. Per-Query Sketch Assignment

Planner responses from the unmodified controller (`05_canonical_planner_test.md`):

| Query | Sketch | Planner result | Notes |
|-------|--------|---------------|-------|
| Q1 EMA | — | **422** | Recursive CTE — not supported |
| Q2 EMA crossover | — | **422** | `LAG`/`CASE` — not supported |
| Q3 frequency | CountSketch | **200** | `topk(10, count_over_time)` (precompute, window, ×15) / `COUNT(*) GROUP BY symbol` (window, ×15) |
| Q4 high/low | KLL | **200** | `max/min_over_time` + SQL MAX/MIN; full sketch |
| Q4 last | ddsketch | **200** | `last_over_time`; batch mode, delta ×6.75 |
| Q5 realized vol | — | **422** | `STDDEV_SAMP` — not supported |
| Q6 cardinality | HLL | **200** | `count(count_over_time)` / `COUNT(DISTINCT)`; full (fill_rate ~100%) |
| Q7 TWAP | KLL | **200** | `avg_over_time` / `AVG`; p50 proxy; full sketch |
| Q8 anomaly | — | **422** | `STDDEV_SAMP` / `PERCENTILE_CONT` — not supported |
| Q9 Bollinger | — | **422** | `STDDEV_SAMP` in window — not supported |
| Q10 RSI | — | **422** | Recursive CTE — not supported |
| Q11 MACD | — | **422** | Recursive CTE — not supported |
| Q12 stochastic | KLL | **200** | `AVG` found in final `OVER()` clause (planner quirk); full sketch |
| Q13 top-K price | CountSketch | **200** | `topk(avg_over_time)`; same CountSketch path as Q3; delta ×15 |

Full per-query detail (window sizes, evaluation configs, success criteria) is in [`02_benchmark_queries.md`](02_benchmark_queries.md).

---

## 6. Financial Statistics Validation (Sketch-finance)

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
   - Q13: exact top-10 symbols by arithmetic mean price per 5-min window
3. **Persist results to disk** under `results/ground_truth/<query>/` (e.g. as CSV or Parquet files keyed by `(day, symbol, window_start)`) — computed once, reused for all future benchmark runs without reprocessing the raw data

### Sketch path

1. Replay the same CSV day as OTLP to the DataCollector at natural pace
2. The plan request includes `"file_output_path": "results/sketch_output/<query>/<day>.jsonl"` — the controller configures the collector's **file exporter** to write each window flush as one OTLP-JSON line to that path.
3. Each line is a complete `ExportMetricsServiceRequest` JSON object. `compare.py` reads this JSONL via `read_sketch_jsonl()`, which parses `resourceMetrics → scopeMetrics → metrics → dataPoints` into a flat `(flush_idx, time_unix_ns, metric, labels, value)` DataFrame.
4. **Persist sketch outputs to disk** under `results/sketch_output/<query>/` — one `.jsonl` per day, reused across comparison runs.

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
| Q13 | Top-K set overlap, Spearman ρ | Overlap ≥ 80%, ρ > 0.7 |

This run is performed at **natural pace on a single day** to isolate sketch accuracy from load effects.

---

## 7. Pass/Fail Thresholds

Full per-query success criteria are defined in each query section of [`02_benchmark_queries.md`](02_benchmark_queries.md). The summary in Section 6 above lists the primary thresholds. A benchmark run **passes** when all applicable thresholds are met for the target query.

---


---

## 8. Results

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
│   ├── Q8/   ← exact anomaly flags per (day, symbol, window)
│   └── Q13/   ← exact top-10 by mean price per (day, window)
├── sketch_output/
│   └── <same structure as ground_truth/>
└── comparison/
    └── <error metric summaries per query>
```
