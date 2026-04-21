# 3. Sketch Use Cases by Column Group

Each column group maps naturally to one or more sketch families.

## Timing / Latency Columns

| Columns | Sketch types | Rationale |
| --- | --- | --- |
| `durationTotal`, `durationExec`, `durationCompiling`, `scheduleTime` | `Quantile (DDSketch, KLL, t-digest)` | Wide-range latency distribution. `p50/p99/p999` are the canonical ask. Heavy-tailed sketches beat histograms here. |
| `durationTotal`, `durationExec`, `durationCompiling`, `scheduleTime` | `Heavy hitters (top-k slow queries)` | Surface the slowest query groups and recurring latency outliers. |
| `profIdle`, `profCpu`, `profPersistentReadS3`, `profIntDataReadLocalSSD`, etc. | `Quantile` | Profiling buckets mostly concentrate in a few categories; quantiles summarize typical and tail cost. |
| `profIdle`, `profCpu`, `profPersistentReadS3`, `profIntDataReadLocalSSD`, etc. | `Frequency (CMS)` | Frequency sketches identify the dominant profiling bottlenecks. |

## Resource Usage Columns

| Columns | Sketch types | Rationale |
| --- | --- | --- |
| `memoryUsed`, `userCpuTime`, `systemCpuTime` | `Quantile` | Continuous resource consumption; quantiles capture typical and tail usage, including SLO-style bounds. |
| `memoryUsed`, `userCpuTime`, `systemCpuTime` | `Reservoir sampling` | Sampling preserves raw examples for downstream ML and inspection. |
| `serverCount`, `warehouseSize`, `perServerCores` | `Top-K (heavy hitters)` | Discrete, low-cardinality values; useful for questions like "Which warehouse size appears most often?" |
| `serverCount`, `warehouseSize`, `perServerCores` | `Frequency (CMS)` | Classical frequency counting over repeated resource configurations. |

## I/O Volume Columns

| Columns | Sketch types | Rationale |
| --- | --- | --- |
| `persistentReadBytesS3`, `persistentWriteBytesS3`, `scanBytes`, `intDataWriteBytesS3` | `Quantile` | Byte counts are heavily skewed; quantiles summarize typical and tail I/O. |
| `persistentReadBytesS3`, `persistentWriteBytesS3`, `scanBytes`, `intDataWriteBytesS3` | `Top-K (data hogs)` | Highlights the queries or groups responsible for the most data movement. |
| `persistentReadRequestsS3`, `intDataNetSentRequests` | `Frequency (CMS)` | Request counts per query or bucket are naturally frequency-oriented. |
| `persistentReadRequestsS3`, `intDataNetSentRequests` | `Quantile` | Quantiles help detect especially chatty workloads or network-heavy queries. |

## Entity / Cardinality Columns

| Columns | Sketch types | Rationale |
| --- | --- | --- |
| `warehouseId`, `databaseId` | `Distinct count (HyperLogLog)` | Anonymized IDs are suited to cardinality estimation, such as "how many unique warehouses are active?" |
| `warehouseId`, `databaseId` | `Top-K` | Identify the busiest warehouses or databases. |
| `warehouseId`, `databaseId` | `Frequency (CMS)` | Supports repeated ID counting under memory constraints. |
| `producedRows`, `returnedRows`, `scanFiles`, `scanOriginalFiles` | `Quantile` | Output and scan cardinality often have long tails; quantiles capture row-count and file-count spread. |
| `producedRows`, `returnedRows`, `scanFiles`, `scanOriginalFiles` | `Top-K` | Surfaces runaway result sets and the noisiest scans. |

## Operator Profiling Columns (`profSortRso`, `profHjRso`, `profAggRso`, etc.)

| Columns | Sketch types | Rationale |
| --- | --- | --- |
| `profSortRso`, `profHjRso`, `profAggRso`, `profFilterRso`, `profBloomRso`, `profPercentileRso` | `Frequency (CMS) per operator` | Treat each operator column as a stream of `(queryId -> cost)` so the sketch identifies the costliest operator families across queries. |
| `profSortRso`, `profHjRso`, `profAggRso`, `profFilterRso`, `profBloomRso`, `profPercentileRso` | `Quantile per operator` | Summarizes the distribution of time spent in each operator family. |
| `profSortRso`, `profHjRso`, `profAggRso`, `profFilterRso`, `profBloomRso`, `profPercentileRso` | `Top-K (dominant operator per query)` | Helps identify which operator dominates a query and which operators dominate overall workloads. |

## Snowset Analysis Code

A Snowset analysis package similar to `datasets_eval/debs/analysis/code` is available under:

- [utils.py](../snowset/analysis/code/utils.py)
- [analyze_cardinality.py](../snowset/analysis/code/analyze_cardinality.py)
- [analyze_frequency.py](../snowset/analysis/code/analyze_frequency.py)
- [analyze_windows.py](../snowset/analysis/code/analyze_windows.py)

The scripts write CSV outputs to:

- [analysis/results/snowset-main/summaries](</datasets_eval/snowset/analysis/results/snowset-main/summaries>)
- [analysis/results/snowset-main/detailed_windows](<datasets_eval/snowset/analysis/results/snowset-main/detailed_windows>)

> **Note on `ts-explosion` (auxiliary dataset):** The `ts-explosion` dataset is not analyzed independently. It serves purely as a complement to the main dataset by providing `(timestamp, queryId)` pairs — one row per second a query was active — enabling time-series joins against `snowset-main`. Since it carries no query metrics of its own (only `sec` and `queryId`), all sketch-relevant analysis targets `snowset-main` exclusively. See [`dataset_overview.md`](../analysis/dataset_overview.md) for a structural comparison of both datasets.

Current analysis covers the full `snowset-main` parquet dataset:

- `datasets_eval/snowset/data/snowset-main.parquet`

## Analysis Results

### 1. Cardinality Summary

| Dataset | Rows | Unique `queryId` | Unique `warehouseId` | Unique `databaseId` |
| --- | ---: | ---: | ---: | ---: |
| `snowset-main` | 69,182,074 | 69,182,074 | 2,051 | 11,134 |

Interpretation:

- Every row in `snowset-main` is a distinct query, so `queryId` is the primary key.
- `warehouseId` (2,051 unique) and `databaseId` (11,134 unique) have far lower cardinality than `queryId` — ideal candidates for HyperLogLog distinct-count sketches and Count-Min Sketch frequency estimation.

### 2. Frequency / Interarrival Summary

| Dataset | Min ms | Max ms | Mean ms | Median ms | P95 ms | P99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `snowset-main` | 0.0 | 17,929.0 | 16.66 | 10.0 | 55.0 | 124.0 |

Interpretation:

- Query arrivals are dense: the median interarrival time is only `10 ms` and the p99 is `124 ms`.
- The maximum gap of `~17.9 s` is modest, indicating a continuously active workload without major idle stretches across the full dataset.
- Compared to the earlier single-shard analysis (median `788 ms`, max `~10.9M ms`), the full dataset shows a far more uniform and high-frequency arrival pattern — the extreme gaps in part-0 were an artifact of the shard boundary.

### 3. Window Summary

| Window | Total windows | Avg samples | Min samples | Max samples | Std samples |
| --- | ---: | ---: | ---: | ---: | ---: |
| `1min` | 1,153 | 60,001.8 | 15,007 | 83,239 | 8,631.1 |
| `5min` | 1,153 | 60,001.8 | 16,263 | 96,577 | 12,005.1 |
| `15min` | 1,153 | 60,001.8 | 16,263 | 141,522 | 19,226.6 |
| `30min` | 641 | 107,928.4 | 43,949 | 143,982 | 14,998.4 |
| `1hour` | 321 | 215,520.5 | 43,949 | 278,918 | 30,224.3 |

Interpretation:

- Each window is dense: even the minimum 1-min window contains over 15,000 queries, making sketches over individual windows statistically meaningful.
- Variability grows with window size (std rising from ~8.6K for 1-min to ~30.2K for 1-hour), reflecting bursty but sustained query throughput.
- The `1min` and `5min` windows both resolve to 1,153 distinct windows because windows are query-aligned rather than calendar-aligned in this analysis; longer windows (30min, 1hr) collapse into fewer, larger bins.