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

- [utils.py](../analysis/code/utils.py)
- [analyze_cardinality.py](../analysis/code/analyze_cardinality.py)
- [analyze_frequency.py](../analysis/code/analyze_frequency.py)
- [analyze_windows.py](../analysis/code/analyze_windows.py)

The scripts write CSV outputs to:

- [analysis/results/snowset-main/summaries](</datasets_eval/snowset/analysis/results/snowset-main/summaries>)
- [analysis/results/snowset-main/detailed_windows](<datasets_eval/snowset/analysis/results/snowset-main/detailed_windows>)
- [analysis/results/fully-joined/summaries](</datasets_eval/snowset/analysis/results/fully-joined/summaries>)
- [analysis/results/fully-joined/detailed_windows](<datasets_eval/snowset/analysis/results/fully-joined/detailed_windows>)

> **Note on `ts-explosion` (auxiliary dataset):** The raw `ts-explosion` dataset is not analyzed independently. It serves as a complement to the main dataset by providing `(timestamp, queryId)` pairs — one row per second a query was active — enabling time-series joins against `snowset-main`. The analysis target added here is the materialized full join, `analysis/results/joined/full_join.parquet`, which combines the auxiliary timestamps with the query-level metrics from `snowset-main`. See [`dataset_overview.md`](../analysis/dataset_overview.md) for a structural comparison of the source datasets.

Current analysis covers the full `snowset-main` parquet dataset and the materialized full join:

- `datasets_eval/snowset/data/snowset-main.parquet`
- `datasets_eval/snowset/analysis/results/joined/full_join.parquet`

## Main Dataset Analysis Results

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

## Fully-Joined (Main + Auxiliary) Analysis Results

The materialized full join combines query-level rows from `snowset-main` with the second-level activity stream from `ts-explosion`. That means the joined dataset has a different grain from the main dataset:

- In `snowset-main`, one row means one completed query.
- In the full join, one row means one active second of one query.

This is the key interpretive shift for all statistics below. The full join is not just the main dataset with an extra timestamp column; it is a time-expanded representation of query activity.

### Semantic Implications

| Question | `snowset-main` meaning | `fully-joined` meaning |
| --- | --- | --- |
| What does one row represent? | One completed query | One active second of one query |
| What does a row count measure? | Number of queries | Number of query-seconds |
| What do frequency counts emphasize? | Queries | Time-weighted query presence |
| What does a sum over repeated metrics represent? | Per-query totals across distinct rows | Time-attributed totals across repeated active timestamps |
| What stays directly comparable? | Distinct IDs and per-query metrics | Distinct IDs only; raw counts shift meaning |

In practice:

- Use `snowset-main` for query-centric questions such as per-query latency distributions, unique-query counts, or top-k heavy queries.
- Use `fully-joined` for time-centric questions such as concurrent activity, aggregate load over time, or time-windowed sketching.

### 1. Cardinality Summary

| Dataset | Rows | Unique `queryId` | Unique `warehouseId` | Unique `databaseId` |
| --- | ---: | ---: | ---: | ---: |
| `fully-joined` | 801,891,730 | 69,182,074 | 2,051 | 11,134 |

Interpretation:

- The full join expands `snowset-main` from `69,182,074` rows to `801,891,730` rows, a `11.59x` amplification. That increase is expected because queries are repeated once per active second.
- The row count should therefore be read as query-seconds, not unique queries.
- Distinct `queryId`, `warehouseId`, and `databaseId` counts remain unchanged relative to `snowset-main`, so the join preserves the entity domain even though it changes the row grain.
- Frequency-based sketches over IDs on the full join become time-weighted. For example, a warehouse with fewer but longer-running queries can appear more often than a warehouse with more short-lived queries.

### 2. Frequency / Interarrival Summary

| Dataset | Min ms | Max ms | Mean ms | Median ms | P95 ms | P99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `fully-joined` | 0.0 | 1.0 | 0.0014 | 0.0 | 0.0 | 0.0 |

Interpretation:

- Interarrival gaps collapse to near zero because many rows share the same second-level timestamp after the time expansion.
- This summary no longer describes the arrival process of distinct queries. Instead, it describes how densely the joined table packs active query-seconds onto a shared timestamp axis.
- The dominant `0 ms` gaps reflect repeated timestamps, not instantaneous bursts of new unique queries.
- For analysis purposes, the fully joined dataset should be interpreted as a concurrency stream over time, not as a query-arrival stream.

### 3. Window Summary

| Window | Total windows | Avg samples | Min samples | Max samples | Std samples |
| --- | ---: | ---: | ---: | ---: | ---: |
| `1min` | 20 | 40,094,586.5 | 9,306,737 | 47,839,844 | 7,942,573.0 |
| `5min` | 4 | 200,472,932.5 | 177,311,212 | 216,737,463 | 14,639,474.2 |
| `15min` | 2 | 400,945,865.0 | 199,892,200 | 601,999,530 | 201,053,665.0 |
| `30min` | 1 | 801,891,730.0 | 801,891,730 | 801,891,730 | 0.0 |
| `1hour` | 1 | 801,891,730.0 | 801,891,730 | 801,891,730 | 0.0 |

Interpretation:

- These windows count joined rows, so they measure active query-seconds per window rather than distinct queries per window.
- The exported full join spans only `20` one-minute windows, and each is extremely dense, ranging from `9.3M` to `47.8M` joined rows. This indicates very high concurrent activity once the data is expanded into per-second rows.
- Larger windows quickly collapse the dataset into one or two bins. At `30min` and `1hour`, the output effectively summarizes the whole export range rather than preserving much temporal structure.
- This makes `1min` and `5min` windows the most useful for time-centric sketching on the fully joined data: they still reflect changes in concurrent activity, while larger windows mostly wash that variation out.

### Takeaway

The fully joined dataset is best treated as a temporal activity view, not as a second version of the main query table. Its statistics are still useful, but their meanings have shifted:

- counts become query-seconds
- frequencies become time-weighted
- repeated metrics describe temporal presence, not distinct-query totals
 