# exathlon — Benchmark Methodology

## 1. Overview

This document describes the end-to-end methodology used to benchmark the **DataCollector** against the Exathlon labeled telemetry dataset. The benchmarks evaluate three dimensions:

- **Throughput** — how many OTLP data points per second can the pipeline ingest and process
- **Latency** — end-to-end delay from event emission to sketch output availability
- **Telemetry Statistics Validation (Sketch-telemetry)** — accuracy of sketch-based aggregations compared to exact ground-truth computations over system telemetry metrics

Dataset statistics and stream properties are documented in [`01_dataset_statistics.md`](01_dataset_statistics.md). Per-query definitions, sketch assignments, and success criteria are documented in [`02_benchmark_queries.md`](02_benchmark_queries.md).

---

## 2. Data Ingestion — Exathlon CSV → OTLP

The Exathlon dataset is distributed as per-run CSV files under `exathlon/data/raw/app*/`. Each file is **wide-format**: one row = one timestamp (`t`) with ~2,283 metric columns. A **replay script** pivots each row into one OTLP gauge data point per non-null metric column and sends them to the DataCollector.

**Field mapping:**

| CSV source | OTLP field |
|------------|------------|
| Filename prefix (e.g. `1` from `1_0_100000_15.csv`) | attribute `entity` |
| Column name stem (e.g. `cpu_time` from `cpu_time_max`) | attribute `metric_base` |
| Column name suffix (e.g. `max` from `cpu_time_max`) | attribute `aggregation_suffix` |
| Column value | Gauge data point value |
| `t` (epoch seconds → nanoseconds) | data point timestamp |

**Metric name:** full column name (e.g. `spark.cpu_time_max`) — one distinct OTLP metric name per column.

**Wide-to-narrow pivot:** each CSV row emits ~2,283 separate OTLP `ExportMetricsServiceRequest` gauge data points sharing the same timestamp `t`. At ~1 Hz row rate, the effective OTLP emission rate is ~**2,283 data points/second per file**.

**One replay stream is used:**

| Stream | Source directory | Rows included | Used for |
|--------|-----------------|---------------|----------|
| Raw telemetry stream | `exathlon/data/raw/` | All rows; all metric columns | Q1–Q12 — all queries |

Each data point is emitted as an OTLP `ExportMetricsServiceRequest` via gRPC or HTTP to the DataCollector's OTLP receiver endpoint.

---

## 3. DataCollector Connector

The DataCollector acts as an **OpenTelemetry Collector** with a custom OTLP receiver. On receiving each `ExportMetricsServiceRequest`, it:

1. Parses the incoming Gauge data points for each `spark.*` metric name
2. Extracts attributes (`entity`, `metric_base`, `aggregation_suffix`) and the data point timestamp
3. Routes the data points through the processor pipeline configured by the active plan

---

## 4. Controller / Plan — Sketch Selection

Before a benchmark run, a **plan** is submitted to the DataCollector controller, which determines which sketch processor is activated for the metric.

**Submit a plan:**

```http
POST /api/v1/plan
{
  "metric_name": "spark.*",
  "aggregations": ["quantile"],        // or "frequency", "cardinality"
  "time_window": "5m",
  "workload_hints": { ... }
}
```

**Inspect active config:**

```http
GET /api/v1/config/spark.*
```

**Aggregation → processor mapping:**

| `aggregations` value | Processor activated | Sketch type |
|----------------------|--------------------|-----------  |
| `"quantile"` | `ddsketchprocessor` or `kllprocessor` | DDSketch / KLL quantile sketch |
| `"frequency"` | `countsketchprocessor` | Count-Min + SpaceSaving |
| `"cardinality"` | `hllprocessor` | HyperLogLog |
| *(none / exact path)* | NOP processor | No sketch — raw gauges retained |

---

## 5. Per-Query Sketch Assignment

| Query | Processor | Aggregation | Notes |
|-------|-----------|-------------|-------|
| Q1 Windowed quantiles | `ddsketchprocessor` / `kllprocessor` | `quantile` | p50/p95/p99 per (entity, metric\_base, window) |
| Q2 Tail amplification ratio | NOP | — | Derived from Q1 quantile outputs; no separate sketch |
| Q3 Top-K heavy metrics | `countsketchprocessor` | `frequency` | Heavy-hitter ranking across 2,283 series per file |
| Q4 Min/max/range | `ddsketchprocessor` / NOP | `quantile` | p0/p100 for approx min/max; NOP for exact |
| Q5 IQR anomaly flags | `ddsketchprocessor` | `quantile` | Quantiles [0.25, 0.50, 0.75] for Tukey-fence detection |
| Q6 Distinct active metrics | `hllprocessor` | `cardinality` | Global distinct series count per window (3,939-space) |
| Q7 Top-K entities by anomaly | `countsketchprocessor` | `frequency` | CMS+SpaceSaving over anomaly-event metrics emitted by replay from Q5, not the raw telemetry stream |
| Q8 Quantile drift | `ddsketchprocessor` / `kllprocessor` | `quantile` | Inter-window p95 / p50 delta per metric |
| Q9 Saturation ratio | `hllprocessor` + NOP | hybrid | HLL for distinct exceeded metrics; NOP for total denominator |
| Q10 EWMA change-point | NOP | — | Sequential EWMA state; not sketch-native |
| Q11 Cross-metric correlation | NOP | — | Rolling Pearson/Spearman per entity pair; not sketch-native |
| Q12 Composite health score | Composite | — | Combines Q2 + Q5 + Q9 outputs downstream |

Full per-query detail (window sizes, evaluation configs, success criteria) is in [`02_benchmark_queries.md`](02_benchmark_queries.md).

---

## 6. Throughput Benchmark

**Goal:** Determine the maximum sustainable OTLP data point ingestion and processing rate under the wide-pivot load pattern.

**Procedure:**

1. Configure the DataCollector with the target query's sketch processor (or NOP for exact-path queries)
2. Run the replay script in **max-speed mode** — no artificial throttle; rows pivoted and emitted as fast as the network and receiver allow
3. Use the raw telemetry stream (`exathlon/data/raw/`) for maximum load on direct-telemetry queries: ~298.5 rows/5-min window × ~2,283 columns = ~**681K OTLP data points per 5-min window**
4. For Q7, replay the derived anomaly-event metric stream instead of the raw telemetry stream, because replay now sends anomaly-event metrics generated from the Q5 detector path
5. Run single-file replay (e.g. `1_0_1000000_14.csv`) first, then multi-file sequential replay across all 93 files to observe degradation under sustained load

**Metrics recorded:**

- OTLP data points ingested per second at the receiver
- Data points processed per second through the sketch pipeline
- Peak throughput (burst, single-file) and sustained throughput (full 93-file corpus)
- Backpressure signals or event drop indicators if any

---

## 7. Latency Benchmark

**Goal:** Measure the end-to-end pipeline delay at realistic event rates (~1 Hz row rate, ~2,283 data points per row).

**Procedure:**

1. Configure the DataCollector with the target query's sketch processor
2. Run the replay script in **paced mode** — rows emitted at natural inter-arrival timing derived from `t` timestamps in the CSV:
   - Raw telemetry stream: ~1.003 s mean inter-arrival per row → ~1 row/s per file
   - Effective data point rate: ~2,283 data points emitted per second per file
3. Record the timestamp when each row's data points are emitted (OTLP send time) and when sketch outputs become available (Prometheus scrape or collector export timestamp)

**Metrics recorded:**

- End-to-end latency distribution: p50, p95, p99
- Wall-clock pipeline delay: difference between OTLP send time and output availability
- Event-time lag: difference between the event's `t` timestamp and output timestamp

---

## 8. Telemetry Statistics Validation (Sketch-telemetry)

**Goal:** Verify that sketch outputs approximate exact telemetry aggregations within acceptable error bounds.

**Procedure:**

For each query that uses a sketch processor (Q1, Q3, Q5, Q6, Q7, Q8, Q9), run two independent paths from the same CSV file:

### Ground-truth path

1. Read the Exathlon CSV directly (no replay, no OTLP)
2. Compute the exact telemetry metric per `(query, file, entity, metric_base, window)`:
   - Q1: exact p50, p95, p99 per (entity, metric\_base) per window
   - Q3: exact threshold-exceedance count per metric per window → top-K ranking
   - Q5: exact IQR-flag precision/recall per metric per 15-min window
   - Q6: exact distinct active metric count per 5-min window
   - Q7: exact entity anomaly-event count per window → top-K entity ranking
   - Q8: exact inter-window quantile drift `|p95_t − p95_{t-1}|` per metric
   - Q9: exact saturation ratio `exceeded_metrics / total_metrics` per entity per window
3. **Persist results to disk** under `results/ground_truth/<query>/` (e.g. as CSV or Parquet files keyed by `(file, entity, metric_base, window_start)`) — computed once, reused for all future benchmark runs without reprocessing the raw data

### Sketch path

1. Replay the same CSV file as OTLP to the DataCollector at natural pace (~1 Hz)
2. Scrape sketch outputs from Prometheus or the collector's export endpoint at each window boundary
3. **Persist sketch outputs to disk** under `results/sketch_output/<query>/` in the same key structure as the ground-truth files

### Comparison

Load both stored files and compute per-query error metrics as defined in [`02_benchmark_queries.md`](02_benchmark_queries.md):

| Query | Primary metric | Success threshold |
|-------|---------------|-------------------|
| Q1 | Rel error on p50/p95/p99 | < 1% for ≥95% of (entity, metric\_base, window) |
| Q3 | Top-K set overlap, Spearman ρ | Overlap ≥ 80%, ρ > 0.7 |
| Q5 | Precision / Recall / F1 on IQR flags | Precision > 70%, Recall > 80%, F1 > 0.75 |
| Q6 | Rel error on distinct count | < 2% per window |
| Q7 | Top-K entity overlap/ranking vs exact | Overlap ≥ 80% |
| Q8 | Rel error on drift `\|p95_t − p95_{t-1}\|` | < 5% for ≥90% of (metric, window pair) |
| Q9 | Abs error on saturation ratio | < 0.02 for ≥90% of (entity, window) |

This run is performed at **natural pace on a representative baseline file per app** (fault type `0`) to isolate sketch accuracy from load effects.

---

## 9. Combined Run (Accuracy Under Load)

**Goal:** Confirm that sketch accuracy holds when the pipeline is under throughput stress.

**Procedure:**

1. Repeat the throughput benchmark (Section 6) with the same sketch configuration used during validation
2. Collect sketch outputs during the max-speed replay
3. Compare to the stored ground-truth files from Section 8
4. Record any degradation in sketch quality metrics relative to the natural-pace baseline

This serves as a secondary accuracy-under-load metric and can reveal issues such as event reordering, window misalignment, or sketch merging errors at high ingestion rates — particularly relevant given the wide-pivot pattern where a single CSV row produces ~2,283 simultaneous OTLP data points.

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
| Exathlon dataset version | [Jacob et al., VLDB 2021 — exathlonbenchmark/exathlon](https://github.com/exathlonbenchmark/exathlon) |

---

## 12. Results

> *To be filled in after benchmark execution.*

Results files will be stored at:

```
results/
├── ground_truth/
│   ├── Q1/   ← exact p50/p95/p99 per (file, entity, metric_base, window)
│   ├── Q3/   ← exact top-K metrics per (file, window)
│   ├── Q5/   ← exact IQR flags per (file, metric, window)
│   ├── Q6/   ← exact distinct metric count per (file, window)
│   ├── Q7/   ← exact top-K entities per (file, window)
│   ├── Q8/   ← exact quantile drift per (file, metric, window pair)
│   └── Q9/   ← exact saturation ratio per (file, entity, window)
├── sketch_output/
│   └── <same structure as ground_truth/>
└── comparison/
    └── <error metric summaries per query>
```
