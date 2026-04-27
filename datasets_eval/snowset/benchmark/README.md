# Snowset Benchmark

Baseline harness for evaluating sketch-based metric aggregation against the
[Snowset dataset](https://github.com/resource-disaggregation/snowset).
This benchmark mirrors the structure of `datasets_eval/debs/benchmark` but is
adapted for the Snowset main parquet dataset used by the benchmark plan and the
current benchmark code.

---

## Dataset

Input: `../data/snowset-main.parquet`  
Resolved path from code: `/users/siedeta/DataCollector/datasets_eval/snowset/data/snowset-main.parquet`

`common.py` opens this path as a partitioned parquet dataset via
`pyarrow.dataset`, not as `analysis/results/joined/full_join.parquet`.

Q6 is the exception: its baseline implementation reads the materialized joined
dataset at `../analysis/results/joined/full_join.parquet`, because concurrency
bands require the auxiliary `ts-explosion` timeline joined onto query metrics.

Dataset characteristics from the current code and benchmark plan:

- Partitioned parquet directory
- One row per completed query
- ~69 M rows
- 93 columns

Benchmark-relevant columns used by the current implementation:

| Column | Type | Notes |
|---|---|---|
| `createdTime` | timestamp | Query completion timestamp used for 5-minute windowing |
| `queryId` | int64 | Unique query identifier |
| `warehouseId` | int64 | Anonymised warehouse ID |
| `warehouseSize` | int64 | Warehouse size tier |
| `durationTotal` | int64 | Query duration (ms) |
| `persistentReadRequestsS3` | int64 | S3 persistent read requests, used by Q1 |
| `persistentReadBytesS3` | int64 | S3 persistent read bytes |
| `profHjRso` | int64 | Join-heavy profiling signal for archetype derivation |
| `profSortRso` | int64 | Sort-heavy profiling signal for archetype derivation |
| `profAggRso` | int64 | Agg-heavy profiling signal for archetype derivation |
| `profScanRso` | int64 | Scan-heavy profiling signal for archetype derivation |
| `profFilterRso` | int64 | Filter-heavy profiling signal for archetype derivation |

The benchmark plan in `docs/02_benchmark_queries_plan.md` also references
`snowset-main.parquet` directly in its DuckDB examples, including Q1 ground truth
queries over `persistentReadRequestsS3`.

### Query-plan alignment

Unlike the older `full_join.parquet` description, the current dataset already
contains the columns required by the benchmark plan and implementation:

| Plan requirement | Current benchmark behavior |
|---|---|
| `persistentReadRequestsS3` (Q1) | Used directly as the frequency weight |
| `profHjRso`, `profSortRso`, `profAggRso`, `profScanRso`, `profFilterRso` (Q4) | Used directly to derive `query_archetype` |

Q4 archetypes are derived from the dominant profiling column and mapped to:
`join_heavy`, `sort_heavy`, `agg_heavy`, `scan_heavy`, `filter_heavy`, or
`other`.

### Q6 joined baseline

Q6 uses the materialized joined parquet:

- Input: `../analysis/results/joined/full_join.parquet`
- Grain: one row per active-query second
- Required columns: `timestamp_sec`, `warehouseSize`, `durationTotal`
- Derived label: `concurrency_band = floor(concurrent_queries / 25) * 25`
- Current baseline scope: exact p95 accuracy by `(warehouseSize, concurrency_band)`
  in the last 5-minute window

---

## Prerequisites

```bash
pip install -r requirements.txt
```

- Collector binaries built under `opentelemetry-collector-contrib-patch/cmd/`
- Controller binary at `controller/target/release/controller`
  (override with `CONTROLLER_BIN` env var)

---

## Queries

| Query | Sketch | Window | Metric | Description |
|---|---|---|---|---|
| Q1 | CountSketch / CountMinSketch | 5 min | `warehouseId` frequency (weighted by `persistentReadRequestsS3`) | Heavy-hitter warehouse detection; `warehouseSize = 4` tier |
| Q2 | KLL | 5 min | `durationTotal` p95/p99 per `warehouseId` | Query tail-latency detection |
| Q3 | DDSketch | 5 min | `persistentReadBytesS3` p95/p99 per `warehouseId` (non-zero only) | S3 read storm detection |
| Q4 | CountSketch / CountMinSketch | 5 min | `query_archetype` frequency per `warehouseSize` | Operator bottleneck identification |
| Q5 | HyperLogLog | 5 min | Distinct `warehouseId` count | Fleet concurrency estimation |
| Q6 | KLL | 5 min | `durationTotal` p95 per `(warehouseSize, concurrency_band)` on joined data | Concurrency-vs-latency degradation baseline |

---

## Running benchmarks

### Single accuracy run

```bash
python3 datasets_eval/snowset/benchmark/run.py test \
  --query Q1 \
  --slice full \
  --mode sketch-snowset
```

### Throughput run

```bash
python3 datasets_eval/snowset/benchmark/run.py test \
  --query Q5 \
  --slice full \
  --mode throughput
```

### Full matrix (all queries, single slice)

```bash
python3 datasets_eval/snowset/benchmark/run.py matrix \
  --queries Q1 Q2 Q3 Q4 Q5 Q6 \
  --slices full
```

---

## `run.py test` arguments

| Argument | Default | Description |
|---|---|---|
| `--query` | `Q1` | Query to run (Q1–Q6) |
| `--slice` | `full` | Dataset slice tag for results labelling |
| `--mode` | `sketch-snowset` | `throughput` maps to max-speed replay; any other value is treated as paced replay |
| `--speed` | `1000` | Speed multiplier used by `replay.py` when replay mode is scaled; preserved for CLI compatibility |
| `--batch-size` | `5000` | OTLP batch size (data points per gRPC call) |
| `--skip-gt` | `0` | `1` to skip ground truth computation |
| `--sketch` | (auto) | Override sketch type: `ddsketch`, `kll`, `countsketch`, `countminsketch`, `hll` |
| `--collector` | (auto) | Override collector binary path |
| `--results-dir` | `results/` | Output directory |
| `--clear-results` | off | Delete `throughput.csv` and `latency.csv` before run |

## `run.py matrix` arguments

| Argument | Default | Description |
|---|---|---|
| `--queries` | Q1–Q6 | Space-separated query IDs |
| `--slices` | `full` | Space-separated slice tags |
| `--sketch-quantile` | `ddsketch` | Sketch for quantile queries (Q2, Q3, Q6) |
| `--sketch-freq` | `countsketch` | Sketch for frequency queries (Q1, Q4) |
| `--sketch-card` | `hll` | Sketch for cardinality queries (Q5) |
| `--skip-gt` | `1` | Skip GT computation (pre-compute separately) |
| `--results-dir` | `results/` | Output directory |

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `CONTROLLER` | `http://localhost:8080` | Controller API URL |
| `CONTROLLER_BIN` | `controller/target/release/controller` | Controller binary |
| `CONTROLLER_OPAMP_PORT` | `4320` | OpAMP port |
| `PROMETHEUS_METRICS_URL` | `http://localhost:8889/metrics` | Prometheus scrape endpoint |
| `AUTO_START_CONTROLLER` | `1` | Auto-start controller (`0` to disable) |
| `COLLECTOR_DDSKETCH` | (default path) | DDSketch collector binary override |
| `COLLECTOR_KLL` | (default path) | KLL collector binary override |
| `COLLECTOR_HLL` | (default path) | HLL collector binary override |
| `COLLECTOR_COUNTSKETCH` | (default path) | CountSketch collector binary override |
| `COLLECTOR_COUNTMINSKETCH` | (default path) | CountMinSketch collector binary override |

---

## Ground truth (standalone)

```bash
# All queries, full slice
python3 datasets_eval/snowset/benchmark/ground_truth/run_gt.py \
  --query all --slice all \
  --out-dir results/ground_truth

# Single query
python3 datasets_eval/snowset/benchmark/ground_truth/run_gt.py \
  --query Q5 --slice full

# Verify generated files
python3 datasets_eval/snowset/benchmark/ground_truth/check_gt.py
```

---

## Results layout

| Path | Description |
|---|---|
| `results/report.md` | Accuracy table, throughput, latency |
| `results/benchmark_results.md` | Aggregated markdown summary produced by `summarize.py` |
| `results/ground_truth/QN/full.csv` | Exact offline reference values |
| `results/sketch_output/QN/full.csv` | Raw Prometheus scrape rows |
| `results/comparison/QN_full.csv` | Per-run accuracy metric and pass/fail |
| `results/throughput.csv` | Events/sec statistics per run |
| `results/latency.csv` | Send-time statistics per run |
| `results/collector.log` | Collector stderr |
| `results/controller.log` | Controller startup/runtime log |

---

## Accuracy thresholds

| Query | Metric | Threshold |
|---|---|---|
| Q1 | `cms_s3_frequency_rel_error` | ≤ 0.05 |
| Q2 | `kll_duration_p99_rel_error` | ≤ 0.05 |
| Q3 | `dds_s3bytes_p99_rel_error` | ≤ 0.05 |
| Q4 | `cms_archetype_share_error` | ≤ 0.05 |
| Q5 | `hll_warehouse_cardinality_rel_error` | ≤ 0.05 |
| Q6 | `kll_duration_p95_concurrency_band_rel_error` | ≤ 0.05 |
