# DEBS 2022 analysis

This document describes outputs from `code/analyze_frequency.py`, `analyze_windows.py`, and `code/analyze_cardinality.py`. Regenerate anytime with `--dataset data` or `--dataset data_filtered`; see [Queries.md](Queries.md).

**Method:** Event times use **`Date` + `Time`** (per-row wall clock on **every** update type), timezone **`Europe/Berlin`**, tumbling windows aligned to **local wall clock**. UTC bounds are stored as `window_*_utc_ms`. Frequency merges **all** parsed timestamps **per file** into one sorted stream (not per symbol). Rows with missing `Date` or `Time` are skipped. Cardinality uses OTLP-style **`symbol`** (stem of `ID`), **`exchange`** (suffix after last `.`), **`sectype`**, and **`(symbol, exchange, sectype)`** triples.

**Ground-truth queries on last-trade price** in the challenge still use **`Date` + `Trading time`** where the spec calls for last-print time; the offline scripts here intentionally use **`Time`** so throughput and window counts reflect the **full tick/update stream**, not only last-trade rows.

---

## 1. Full feed (`data/`)

### 1.1 Frequency summary (`summaries/frequency_summary.csv`)

Inter-arrival gaps (ms) on globally sorted **`Date` + `Time`** events per file.

Run `python3 analyze_frequency.py --dataset data` and open the CSV for current numbers. Weekday files are dominated by sub-second spacing; partial or low-activity days still have many distinct timestamps because **`Time`** is populated for non-last-trade rows as well.

### 1.2 Cardinality — global (`summaries/cardinality_overall.csv`)

| dimension | unique_count_global |
|-----------|---------------------|
| symbol | 5502 |
| exchange | 3 |
| sectype | 2 |
| symbol_exchange_sectype | 5502 |

(Re-run `analyze_cardinality.py --dataset data` if the corpus changes.)

### 1.3 Cardinality — per file (`summaries/cardinality_summary.csv`)

Each full day has about **5493–5499** symbols, **3** exchanges, **2** sectypes; `symbol_exchange_sectype` matches symbol count (one sectype per symbol in practice).

### 1.4 Window summary (`summaries/window_summary.csv`)

Long form only: one row per (`file`, `window_size`), sorted by **`window_size`** then file. Columns: `total_windows`, `avg_samples`, `min_samples`, `max_samples`, `std_samples`.

Regenerate with `python3 analyze_windows.py --dataset data`.

### 1.5 Detailed outputs — inventory (`results/data/detailed_windows/`)

| file | approx. lines | approx. size |
|------|---------------|--------------|
| frequency_per_window.csv | 6 229 | 576 KB |
| window_details_1min.csv | 4 682 | 326 KB |
| window_details_5min.csv | 957 | 68 KB |
| window_details_15min.csv | 328 | 24 KB |
| window_details_30min.csv | 173 | 13 KB |
| window_details_1hour.csv | 93 | 6.7 KB |

(Line counts and sizes drift when you re-run after code or data changes.)

### 1.6 Detailed samples (full `data/` — small enough to excerpt)

After regenerating, inspect the first rows of:

- **`window_details_1min.csv`**: `file`, `window_start_utc_ms`, `window_end_utc_ms`, `sample_count`
- **`frequency_per_window.csv`**: `file`, `window_size`, `window_start_utc_ms`, `window_end_utc_ms`, `avg_freq_ms`, `samples_in_window`

---

## 2. Price-filtered feed (`data_filtered/`)

Rows with non-empty **`Last`** and **`Trading time`** only (see [`filter_data.py`](code/filter_data.py)). Each kept row still has **`Date`** and **`Time`**, so frequency/window scripts on `data_filtered` measure the **filtered stream** using the same **`Date`+`Time`** clock as the full feed.

### 2.1 Frequency summary

Run `python3 analyze_frequency.py --dataset data_filtered` and read `summaries/frequency_summary.csv`. Compared to a **`Trading time`**-only clock, gaps and window densities reflect **all** retained row updates timed by **`Time`**, not only last-print instants.

### 2.2 Cardinality — global

Run `analyze_cardinality.py --dataset data_filtered`. Expect symbol / OTLP triple counts on the same order as the full feed (~5.5k symbols per main trading day) when `data_filtered/` was built with **`index_col=False`** in [`filter_data.py`](code/filter_data.py) (see [Queries.md](Queries.md) — Zenodo 40-field rows vs 39 headers).

### 2.3 Window summary

Only `summaries/window_summary.csv` (long form). Regenerate: `python3 analyze_windows.py --dataset data_filtered`.

### 2.4 Detailed outputs — inventory (`results/data_filtered/detailed_windows/`)

| file | approx. lines | approx. size |
|------|---------------|--------------|
| frequency_per_window.csv | **1 784 600** | **~132 MB** (GitHub limit exceeded) |
| window_details_1min.csv | 385 047 | ~25 MB |
| window_details_5min.csv | 376 643 | ~25 MB |
| window_details_15min.csv | 362 231 | ~24 MB |
| window_details_30min.csv | 344 637 | ~23 MB |
| window_details_1hour.csv | 316 046 | ~21 MB |

**Total** ~3.57M data lines across detailed CSVs — keep local only or use LFS if you must version them.

---

## 3. Regenerate locally

```bash
cd datasets_eval/debs/code
python3 analyze_frequency.py --dataset data
python3 analyze_windows.py --dataset data
python3 analyze_cardinality.py --dataset data
python3 analyze_frequency.py --dataset data_filtered
python3 analyze_windows.py --dataset data_filtered
python3 analyze_cardinality.py --dataset data_filtered
```

Summaries land under `results/<dataset>/summaries/`; details under `results/<dataset>/detailed_windows/`.
