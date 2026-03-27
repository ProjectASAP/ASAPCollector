# DEBS 2022 analysis

This document snapshots outputs from `code/analyze_frequency.py`, `analyze_windows.py`, and `analyze_cardinality.py`. Regenerate anytime with `--dataset data` or `--dataset data_filtered`; see [Queries.md](Queries.md).

**Method:** Event times use **`Date` + `Trading time`**, timezone **`Europe/Berlin`**, tumbling windows aligned to **local wall clock**. UTC bounds are stored as `window_*_utc_ms`. Frequency merges **all** last-trade timestamps **per file** into one sorted stream (not per symbol). Cardinality uses OTLP-style **`symbol`** (stem of `ID`), **`exchange`** (suffix after last `.`), **`sectype`**, and **`(symbol, exchange, sectype)`** triples.

---

## 1. Full feed (`data/`)

### 1.1 Frequency summary (`summaries/frequency_summary.csv`)

Inter-arrival gaps (ms) on globally sorted `Date` + `Trading time` events per file.

| file | min_ms | max_ms | mean_ms | median_ms | p95_ms | p99_ms |
|------|--------|--------|---------|-----------|--------|--------|
| debs2022-gc-trading-day-08-11-21.csv | 0 | 14331601 | 3.01 | 0 | 7 | 19 |
| debs2022-gc-trading-day-09-11-21.csv | 0 | 14330885 | 2.84 | 0 | 6 | 18 |
| debs2022-gc-trading-day-10-11-21.csv | 0 | 14330781 | 2.67 | 0 | 6 | 16 |
| debs2022-gc-trading-day-11-11-21.csv | 0 | 14331037 | 2.98 | 0 | 7 | 18 |
| debs2022-gc-trading-day-12-11-21.csv | 0 | 14292180 | 2.97 | 0 | 7 | 17 |
| debs2022-gc-trading-day-13-11-21.csv | 0 | 0 | 0 | 0 | 0 | 0 |
| debs2022-gc-trading-day-14-11-21.csv | — | — | — | — | — | — |

**Why day 13 is all zeros (current pipeline):** With **`Date` + `Trading time`** only, that partial-day file collapses to **one distinct millisecond** for all last-trade times, so sorted gaps are **all 0 ms**.

**Why day 14 is NaN (current pipeline):** No usable **`Trading time`** rows → no timestamps → **NaN** in the frequency summary.

**Earlier “first commit” pipeline (`Date` + `Time`, Unix-epoch tumbling):** That treated **generic row time** for **all** updates, so days **13–14** still had spread-out **`Time`** values and showed **non-degenerate** frequency/window stats. Those numbers were **reasonable for full-feed / throughput** views; they are **not** the same event clock as **last-trade** (`Trading time`) used for query alignment today. Both are “correct” for their respective definitions.

### 1.2 Cardinality — global (`summaries/cardinality_overall.csv`)

| dimension | unique_count_global |
|-----------|---------------------|
| symbol | 5502 |
| exchange | 3 |
| sectype | 2 |
| symbol_exchange_sectype | 5502 |

### 1.3 Cardinality — per file (`summaries/cardinality_summary.csv`)

Each full day has about **5493–5499** symbols, **3** exchanges, **2** sectypes; `symbol_exchange_sectype` matches symbol count (one sectype per symbol in practice).

### 1.4 Window summaries (`summaries/`)

- **`window_summary.csv`** — long form: one row per (file, window_size), sorted by **window_size** then file.
- **`window_summary_by_window_size.csv`** — **grouped by `window_size`**: aggregates across all CSVs in the run (`sum_total_windows`, means of per-file stats where windows exist, etc.). This matches a **group-by on window size** across the corpus.
- **`window_summary_pivoted.csv`** — one row per **file**, wide columns `metric_windowSize` (handy per-day comparison, **not** a window-size group-by).

Excerpt below is from the **wide-by-file** file. Samples per tumbling window (last-trade events only). Main trading days (08–12) show large `avg_samples_*`; day **13** collapses to a single window with 5268 samples; day **14** has no windows.

| file | total_windows_1min | avg_samples_1min | max_samples_1min | total_windows_5min | avg_samples_5min | max_samples_5min |
|------|-------------------|------------------|------------------|--------------------|------------------|------------------|
| …-08-11-21.csv | 935 | 28900 | 103621 | 190 | 142220 | 459056 |
| …-09-11-21.csv | 936 | 30616 | 112954 | 191 | 150036 | 512177 |
| …-10-11-21.csv | 937 | 32442 | 119168 | 192 | 158325 | 491749 |
| …-11-11-21.csv | 936 | 29118 | 103441 | 191 | 142693 | 444138 |
| …-12-11-21.csv | 936 | 29201 | 100755 | 191 | 143102 | 433420 |
| …-13-11-21.csv | 1 | 5268 | 5268 | 1 | 5268 | 5268 |
| …-14-11-21.csv | 0 | — | — | 0 | — | — |

(Full pivoted columns: `total_windows_*`, `avg_samples_*`, `min_samples_*`, `max_samples_*`, `std_samples_*` for 1min, 5min, 15min, 30min, 1hour — see regenerated CSV.)

### 1.5 Detailed outputs — inventory (`results/data/detailed_windows/`)

| file | approx. lines | approx. size |
|------|---------------|--------------|
| frequency_per_window.csv | 6 229 | 576 KB |
| window_details_1min.csv | 4 682 | 326 KB |
| window_details_5min.csv | 957 | 68 KB |
| window_details_15min.csv | 328 | 24 KB |
| window_details_30min.csv | 173 | 13 KB |
| window_details_1hour.csv | 93 | 6.7 KB |

### 1.6 Detailed samples (full `data/` — small enough to excerpt)

**`window_details_1min.csv`** (first rows):

| file | window_start_utc_ms | window_end_utc_ms | sample_count |
|------|----------------------|-------------------|----------------|
| …-08-11-21.csv | 1636326000000 | 1636326060000 | 5296 |
| …-08-11-21.csv | 1636336800000 | 1636336860000 | 1426 |
| …-08-11-21.csv | 1636336860000 | 1636336920000 | 495 |

**`frequency_per_window.csv`** (first rows, 1min windows):

| file | window_size | window_start_utc_ms | window_end_utc_ms | avg_freq_ms | samples_in_window |
|------|-------------|----------------------|-------------------|-------------|-------------------|
| …-08-11-21.csv | 1min | 1636326000000 | 1636326060000 | 0.0 | 5296 |
| …-08-11-21.csv | 1min | 1636336800000 | 1636336860000 | 8.38 | 1426 |
| …-08-11-21.csv | 1min | 1636336860000 | 1636336920000 | 1.78 | 495 |

---

## 2. Price-filtered feed (`data_filtered/`)

Rows with non-empty **`Last`** and **`Trading time`** only.

### 2.1 Frequency summary

| file | min_ms | max_ms | mean_ms | median_ms | p95_ms | p99_ms |
|------|--------|--------|---------|-----------|--------|--------|
| …-08-11-21.csv | 0 | 31606713000 | 119992057 | 8466000 | 157072600 | 612481900 |
| …-09-11-21.csv | 0 | 31613375000 | 109435339 | 7439000 | 147427650 | 503099570 |
| …-10-11-21.csv | 0 | 31619595000 | 106315431 | 7287000 | 113383400 | 504064480 |
| …-11-11-21.csv | 0 | 31601884000 | 120473682 | 8473000 | 157242500 | 603976580 |
| …-12-11-21.csv | 0 | 31607531000 | 115945716 | 8361000 | 153456100 | 585656480 |
| …-13 / 14 | — | — | — | — | — | — |

Larger medians/means than the full-feed view: one **per-symbol** stream is no longer mixed across all IDs at the same millisecond, so gaps reflect **last-trade** spacing more than mass co-timestamp collisions.

### 2.2 Cardinality — global

| dimension | unique_count_global |
|-----------|---------------------|
| symbol | 2 |
| exchange | 0 |
| sectype | 6 |
| symbol_exchange_sectype | 12 |

**Update (fix):** Those values came from **corrupted `data_filtered/`** files: Zenodo rows have **40** CSV fields for **39** header columns, and pandas defaulted to using **`ID` as the row index**, so written CSVs had **`SecType` in the `ID` column**. [`filter_data.py`](code/filter_data.py) and [`utils.py`](code/utils.py) now use **`index_col=False`**. **Re-run** `python3 filter_data.py` to regenerate `data_filtered/`, then re-run `analyze_cardinality.py --dataset data_filtered` (and other analyses). Expect **symbol / OTLP triple counts on the same order as the full feed** (~5.5k symbols per main trading day).

### 2.3 Window summary — pivoted (excerpt)

| file | total_windows_1min | avg_samples_1min | max_samples_1min | total_windows_5min | avg_samples_5min |
|------|-------------------|------------------|------------------|--------------------|------------------|
| …-08-11-21.csv | 72559 | 1.06 | 207 | 70994 | 1.08 |
| …-09-11-21.csv | 80435 | 1.05 | 243 | 78748 | 1.07 |
| …-10-11-21.csv | 83077 | 1.04 | 257 | 81208 | 1.07 |
| …-11-11-21.csv | 73040 | 1.05 | 284 | 71395 | 1.07 |
| …-12-11-21.csv | 75935 | 1.05 | 274 | 74297 | 1.07 |
| …-13 / 14 | 0 | — | — | 0 | — |

Here **avg_samples ≈ 1** per window: within each Berlin 1-minute bucket, **at most one last-trade timestamp per instrument** is typical (sparse last prints vs dense mixed stream).

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