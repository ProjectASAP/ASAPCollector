# Snowset Dataset Notes

This directory contains our local working copy of the [Snowset dataset documentation and artifacts](https://github.com/resource-disaggregation/snowset), adapted for dataset inspection, sketch-oriented analysis, and benchmark preparation inside this repository.

## What Is In This Directory

### `data/`

This directory stores the Snowset data files used locally. In this workspace, the main focus is on Parquet inputs:

- `snowset-main.parquet`
- `ts-explosion.parquet`

We also keep compressed archives such as `*.parquet.tar.gz` and helper extraction scripts here.

#### `data/download_extract_dataset.py`

This script automates the local Snowset parquet setup. It downloads the two official parquet archives from the upstream Snowset host and then extracts them into the chosen output directory.

By default it downloads:

- `snowset-main.parquet.tar.gz`
- `ts-explosion.parquet.tar.gz`

and extracts them into the same `data/` directory, producing the parquet directories used by the local analysis code.

The script also includes a safe extraction check before unpacking each tarball. It verifies that every archived path stays inside the target directory, which prevents path-traversal issues during extraction.

Main options:

- `--output-dir` changes where the archives and extracted parquet directories are written
- `--skip-download` skips network fetching and only extracts archives already present locally
- `--skip-extract` downloads the archives without unpacking them
- `--force-download` re-downloads the archives even if the files already exist

Typical usage:

```bash
python3 datasets_eval/snowset/data/download_extract_dataset.py
```

### `analysis/`

This section contains exploratory analysis code and notes written for this project. It includes:

- `overview.py` for quick row-level inspection of selected parquet files
- `code/` for reusable analysis scripts such as cardinality, interarrival, and window summaries
- `results/` for generated CSV summaries and detailed window outputs (organized by dataset, see below)
- `README.md` for dataset-specific interpretation and join notes

#### Running the analysis scripts

The three scripts in `analysis/code/` (`analyze_cardinality.py`, `analyze_frequency.py`, `analyze_windows.py`) each accept a `--dataset` flag so you can run them against one dataset at a time or both together.

```bash
cd datasets_eval/snowset/analysis/code

# Analyse snowset-main
python analyze_cardinality.py --dataset snowset-main
python analyze_frequency.py   --dataset snowset-main
python analyze_windows.py     --dataset snowset-main
```

Results are written into per-dataset subdirectories under `analysis/results/`:

| `--dataset` | Output directory |
| --- | --- |
| `snowset-main` | `analysis/results/snowset-main/` |

Each output directory contains:

- `summaries/cardinality_summary.csv` — row count and unique-value counts per entity column
- `summaries/join_summary.csv` — cross-dataset join coverage (only written in `all` mode)
- `summaries/frequency_summary.csv` — interarrival statistics
- `summaries/window_summary.csv` — per-window-size sample count statistics
- `detailed_windows/frequency_per_window.csv` — per-window interarrival detail
- `detailed_windows/window_details_<size>.csv` — per-window sample counts for each window size

Running each dataset independently is the recommended approach when memory is a concern, since loading all 2 000 part files for both datasets simultaneously is memory-intensive. The join summary (`join_summary.csv`) is the only output that requires both datasets and is therefore only produced in `all` mode.

**Prerequisites:** `pandas`, `pyarrow`, and `numpy` must be available in the active Python environment.

```bash
pip install pandas pyarrow numpy
```

#### `analysis/code/join_dataset.py`

This script joins `snowset-main` with `ts-explosion` using DuckDB and produces a per-second temporal aggregation of the combined workload. It is the entry point for any time-series analysis that needs query metrics (latency, memory, I/O) anchored to wall-clock timestamps.

**What it produces**

By default the script runs a temporal aggregation query and writes a CSV to `analysis/results/joined/temporal_agg.csv`. Each row in the output is one second of workload activity with the following aggregate columns derived from the join:

| Output column | Description |
| --- | --- |
| `timestamp_sec` | Second-level timestamp from `ts-explosion` |
| `active_queries` | Number of queries active at that second |
| `total_memory_bytes` | Sum of `memoryUsed` across all active queries |
| `total_s3_read_bytes` | Sum of `persistentReadBytesS3` |
| `total_scan_bytes` | Sum of `scanBytes` |
| `p99_duration_ms` | Approximate p99 of `durationTotal` across active queries |

With `--export-join` the script additionally writes a full row-level join as `analysis/results/joined/full_join.parquet`, repeating selected `snowset-main` metric columns alongside every `ts-explosion` row.

**Recommended usage**

```bash
# Standard run — temporal aggregation only (recommended for most use cases)
python join_dataset.py \
  --memory-limit 20GB \
  --threads 16 \
  --temp-dir datasets_eval/snowset/analysis/results/.duckdb-tmp

# Low-memory fallback — process one day at a time (Unix timestamps)
python join_dataset.py \
  --memory-limit 12GB \
  --threads 12 \
  --temp-dir datasets_eval/snowset/analysis/results/.duckdb-tmp \
  --ts-start 1519171200 \
  --ts-end   1519257600

# Full row-level join export — large output, ensure sufficient disk space
python join_dataset.py \
  --memory-limit 20GB \
  --threads 16 \
  --temp-dir datasets_eval/snowset/analysis/results/.duckdb-tmp \
  --export-join
```

| Flag | Default | Notes |
| --- | --- | --- |
| `--memory-limit` | `12GB` | Minimum recommended. 20GB is comfortable on this machine (31 GB RAM, 23 GB free). |
| `--threads` | auto | Set to `16` to leave 4 cores free for OS/IO scheduling. |
| `--temp-dir` | `/tmp` | **Override this.** `/tmp` is on the root partition (45 GB free). Use the NVMe path above (102 GB free). Spill files for the full join can reach 30–50 GB. |
| `--ts-start` / `--ts-end` | none | Unix seconds. Restrict processing to a time window for low-memory machines. |
| `--export-join` | off | Materialises the full one-to-many join. Output is 20–60 GB — only use when the row-level join is genuinely needed. |
| `--out-dir` | `analysis/results/joined/` | Destination for all output files. |


**Prerequisites:** `duckdb` must be available in the active Python environment.

```bash
pip install duckdb
```

### `benchmark/`

This directory is reserved for benchmark-oriented work built on top of Snowset. The intent is to turn dataset observations into repeatable benchmark or sketch-evaluation inputs.

### `docs/`

This directory contains project-facing documentation derived from the Snowset dataset. It is where we summarize schema groups, sketch use cases, benchmark query ideas, and analysis results in markdown form.

## Datasets Used Here

### Main Dataset: `snowset-main`

The upstream Snowset README describes the main dataset as a per-query table. Each row represents one unique query and includes query-level metadata and execution statistics such as:

- timing and latency
- I/O volumes and request counts
- resource usage
- scan and output metrics
- profiling counters by resource and operator

The main key is `queryId`, which uniquely identifies a query. This is the primary dataset for query-level analysis, sketch selection, and workload profiling.

### Auxiliary Dataset: `ts-explosion`

The upstream README describes `ts-explosion` as an auxiliary time-series expansion of the main dataset. Each row is a `(timestamp, queryId)` pair indicating that a query was active at that timestamp.

This dataset is useful when the question is time-based rather than query-based, for example:

- how many queries were active at a given second
- how active-query counts vary by window
- how to join per-query metrics onto a per-second activity stream

The upstream project notes that this auxiliary dataset can be derived from the main dataset, but it is provided precomputed for convenience.

## How The Two Datasets Relate

The join key is `queryId`.

- **Main dataset** (`snowset-main`): one row per query, with `queryId` as the unique key plus all ~80 metric columns (latency, I/O, CPU, profiling, …).
- **Auxiliary dataset** (`ts-explosion`): many rows per query — one row per `(timestamp, queryId)` pair. A query active for 5 time ticks produces 5 rows.

This is a **one-to-many join**: one row in main → many rows in auxiliary (one per timestamp the query was active).

### What the joined result looks like

```
timestamp | queryId | durationTotal | memoryUsed | scanBytes | ...
----------+---------+---------------+------------+-----------+----
T=100     | Q1      | 5000          | 2GB        | 10MB      | ...
T=101     | Q1      | 5000          | 2GB        | 10MB      | ...   ← same metrics, repeated
T=102     | Q1      | 5000          | 2GB        | 10MB      | ...
T=100     | Q2      | 800           | 500MB      | 1MB       | ...
```

The main dataset metrics are static per query (they are post-completion summaries), so they repeat on every timestamp row for that query. The join lets you ask temporally-structured questions like "what was the total memory committed across all concurrently active queries at timestamp T?"

### Why the join is valid — dataset compatibility

**Same `queryId` type.** Both datasets define `queryId` as `int64`. There is no casting or type coercion — DuckDB resolves the equi-join directly with no overhead.

**Same query universe.** `ts-explosion` is derived from `snowset-main`: it is a precomputed time-series expansion of the same query executions, not an independent source. Every `queryId` in `ts-explosion` has a matching row in `snowset-main`. Join coverage is 100% — no auxiliary row is left unmatched.

**Complementary, non-overlapping columns.**

| | `snowset-main` | `ts-explosion` |
| --- | --- | --- |
| Grain | One row per query | One row per active second per query |
| `queryId` role | Primary key (unique) | Foreign key (repeated) |
| Unique columns | ~80 metric columns | `sec` (timestamp only) |

Neither dataset duplicates the other's columns, so the join purely adds information: each `ts-explosion` row gains the full metric payload from its matching `snowset-main` row.

## Upstream Snowset Documentation Sections

The official Snowset repository is organized around a few core documentation pages:

### Upstream `README.md`

This is the high-level entry point. It explains what Snowset is, the two dataset variants, the paper connection, privacy notes, usage guidance, and the helper notebooks/scripts included in the repository.

### Upstream `download.md`

This page describes how to obtain the dataset files. In practice, this is the source for the downloadable CSV and Parquet artifacts referenced by the main README.

### Upstream `schema.md`

This page is the column-level reference for the main dataset and the auxiliary time-series data. It is the most important source when mapping Snowset columns to sketch families or benchmark features.

### Upstream `profile.md`

This page explains the `prof*` and `prof*Rso` columns. It is especially useful for understanding the resource-level and operator-level profiling counters in `snowset-main`.
