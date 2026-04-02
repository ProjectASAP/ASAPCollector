# DEBS Benchmark — Query Porting Guide
## How Q1 was implemented and how to replicate it for other queries

---

## What was built on `debs_q1`

Commit `b4ae912` implemented the Q1 accuracy benchmark with three design changes
compared to the original placeholder harness:

| Design choice | Old (harness skeleton) | New (debs_q1) |
|---|---|---|
| Replay speed | `--speed 100` (scaled 100×) | `--speed 1` (paced 1:1) |
| Processor mode | batch (latency_sla < time_window) | **window** (latency_sla omitted) |
| Data slice | full trading day | first N minutes of event time (default 60) |
| Accuracy check | none | `frac_lt_1pct ≥ 0.95` with warm-up skip |

The result on day `08-11-21`: **frac_lt_1pct = 1.0 → PASS**.

---

## Why these three choices

### 1:1 paced replay

The collector's 5-minute window ticker runs on wall-clock time. At 100× speed, 5 minutes
of wall-clock time covers 500 minutes of event time — the entire trading day ends up in one
or two collector windows. Ground truth is computed on event-time windows.

At 1:1 speed, wall-clock time = event time, so every 5-minute collector flush corresponds
exactly to one 5-minute event-time window in the ground truth.

`replay_technical_mode("sketch-finance")` was changed from `"scaled"` to `"paced"`.
`paced` mode in `replay.py` ignores `--speed-factor` entirely and sleeps to match event
timestamps precisely.

### Window mode

The controller selects the processor mode based on the plan body:

```
latency_sla absent          → ProcessorMode::Window  (accumulate across 5-min window)
latency_sla ≥ time_window   → ProcessorMode::Window
latency_sla < time_window   → ProcessorMode::Batch   (old benchmark path)
```

`build_plan_body` was changed to omit `latency_sla` when `bench_mode == "sketch-finance"`.
For `throughput` and `latency` modes, `latency_sla = "30s"` is still included to force
batch mode (which is the right choice for load testing).

### Time slice (`--accuracy-minutes`)

A full 8-hour trading day at 1:1 speed takes 8 hours. A 60-minute slice gives
12 windows × ~5 178 active symbols = ~62 000 (symbol, window) pairs — statistically
sufficient for `frac_lt_1pct` — and takes exactly 60 minutes of wall-clock time.

The cutoff is applied **inside the chunked CSV loader** (not post-concat) and in the
replay batch loop, so both paths slice exactly the same set of ticks:

- **`data_filtered` (Q1, etc.):** reference `ts_min` is taken from the first chunk that still
  has rows after `Last > 0` (and other filters). `cutoff_ms = ts_min + accuracy_minutes × 60 000`.
- **`data` full feed (Q3, Q6):** the raw day file starts at **midnight** with pre-trading rows
  where `Last` is empty or zero. The anchor is the **earliest `Last > 0` on or after 08:00
  Europe/Berlin** on that calendar day (`full_feed_session_open_ms_from_csv_path` in
  `common.py`). If there is no such row, the code falls back to the global minimum trade time
  (same as before the session floor). Quote rows with `Last = 0` after the anchor are kept.
  `cutoff_ms = anchor_ms + accuracy_minutes × 60 000`.

### Batch size at runtime

`--batch-size` controls how many data points are packed into one OTLP
`ExportMetricsServiceRequest`. It is independent of the processor mode.

`--batch-size 50` was used in some Q1 runs (producing ~10 Export calls for 480 events).
`--batch-size 5000` (the default) was used in others (producing ~2 Export calls for ~5 000
events). In window mode the processor accumulates all arrivals within the 5-minute wall-clock
window regardless of batch boundaries, so the final sketch is identical either way. Smaller
batch size → more frequent intermediate Prometheus snapshots during replay.

---

## Actual code changes made (reference for other queries)

### Infrastructure files — already done, do not change again

These files were modified on `debs_q1` and carry the full infrastructure. Every subsequent
query branch inherits them via merge.

#### `run.py`

- `replay_technical_mode`: returns `"paced"` for `sketch-finance` (was `"scaled"`).
- `build_plan_body(metric, query, sketch, bench_mode)`: omits `latency_sla` when
  `bench_mode == "sketch-finance"`.
- `--speed` default: `1` (was `100`).
- `--accuracy-minutes` argument: default `60`, passed to both `run_gt.py` and `replay.py`
  subprocesses when `mode == "sketch-finance"` and `accuracy_minutes > 0`.
- Speed warning: printed to stderr when `mode == "sketch-finance"` and `speed > 1`.

#### `replay.py`

- `--max-event-minutes` argument: default `0` (no cutoff). When set,
  `cutoff_ns = anchor_ns + minutes × 60 × 10⁹`.
- **Anchor:** for `dataset=data`, **`scan_full_feed_anchor_trade_ns`** does one full pass to
  find the minimum event time with `Last > 0` **on or after 08:00 Berlin** (see
  `FULL_FEED_SESSION_START_HOUR_BERLIN` in `common.py`), with fallback to the global minimum
  trade time if needed. Replay then reads the file again in `iter_batches`. For
  `data_filtered`, the anchor is the first row in the stream (all rows already have `Last > 0`
  from parsing).
- Every batch is trimmed to `anchor_ns ≤ ts ≤ cutoff_ns` (when cutoff is set). The file is
  not time-sorted; empty post-cutoff batches are skipped.
- `_parse_chunk_filtered` drops invalid / zero-price rows via `Last > 0` (last-trade stream).
  `_parse_chunk_full` keeps all rows; `Last` is coerced with `fillna(0.0)` for the anchor scan.
- Prints `event_time_span_s` in the `replay done` summary line.
- There is **no** `--skip-event-minutes` / `--accuracy-start-minutes` path: the trade-time
  anchor replaces skipping wall-clock minutes from file start.

#### `ground_truth/common.py`

- `load_filtered_day(csv_path, chunksize, max_event_minutes=None)`: in-loop cutoff using
  the first non-empty chunk’s `ts_ms.min()` **after** `Last > 0` filtering; skips empty
  post-cutoff chunks but does not break early (file is not time-sorted).
- `load_full_feed_day(csv_path, chunksize, max_event_minutes=None)` (Q3 / Q6): coalesces
  `Time` and `Trading time`, sets `Last` with `fillna(0.0)`. **`_full_feed_anchor_trade_ms`**
  scans the file once for the same anchor as replay (min trade time ≥ 08:00 Berlin, else
  global min), then a second pass keeps `first_ms ≤ ts_ms ≤ cutoff_ms`.

#### `ground_truth/run_gt.py`

- `--max-event-minutes` argument; passed to `run_ground_truth_task`. Meaning of the anchor
  depends on the query’s loader (`data_filtered` vs full-feed trade anchor above).
- `ACCEPTED_GT_QUERIES` and `ALL_GT_BATCH` narrowed to the queries present on the current
  branch (updated per branch).

#### `compare.py`

- `extract_sketch_quantile(dataframe, q_target, tol)`: generalized function replacing the
  old hardcoded p50 extraction. Handles both `ddsketch_quantile` and `kll_quantile` label
  keys, both dot and underscore variants.
- `extract_ddsketch_median`: now calls `extract_sketch_quantile(dataframe, 0.5)`.
- `_quantile_from_labels`: helper to parse the quantile label regardless of key name.
- Warm-up skip in `compare_q1`: drops the first `skip_warmup_windows=1` windows from the
  ground truth before the merge (EMA seeds cold on tick 1; that window has higher error).
- `_SKETCH_METRIC_PATTERN["Q1"] = r"ddsketch|kll"` registered.
- `_COMPARE_DISPATCH["Q1"] = compare_q1` registered.
- Removed the `Q6`-special-case branch in `run_comparison` (now unified).
- **Q3:** `_SKETCH_METRIC_PATTERN["Q3"] = r"countsketch"`, `compare_q3` (top-10 overlap +
  Spearman vs ground-truth counts per window), `_COMPARE_DISPATCH["Q3"]`.

---

## Q3 (CountSketch / full feed)

- **Dataset:** `data` in `QUERY_CONFIG`; ground truth in `ground_truth/q3.py` uses
  `load_full_feed_day` and per-`(symbol, window)` **event counts** (frequency variant in
  [02_benchmark_queries.md](../DEBS_2022/02_benchmark_queries.md)).
- **Trading session:** GT and replay both start at the **first `Last > 0`** tick, not midnight.
- **Workload hints** in `run.py` (`_QUERY_WORKLOAD["Q3"]`) tune CountSketch ε for ~5k series
  and realistic events per 5-minute window (not the generic default).

---

## Q4 (DDSketch / data_filtered — per-symbol high/low)

- **Dataset:** `data_filtered` in `QUERY_CONFIG`; ground truth in `ground_truth/q4.py` uses
  `load_filtered_day` (`Last > 0` already enforced) and computes exact per-`(symbol, window)`
  high, low, last_price, and price_range.
- **Trading session:** `data_filtered` + `Last > 0` means both GT and replay start from the
  first real trading tick, identical to Q1. No midnight pre-trading rows are included.
- **Comparison metric:** `frac_hilo_lt_2pct` — fraction of (symbol, window) pairs where
  **both** the DDSketch p100 estimate is within 2% of GT high **and** p0 is within 2% of GT
  low. Threshold: ≥ 0.90.
- **Why not range-vs-range:** Many symbols have near-zero 5-minute price ranges (< 1% of
  price), smaller than a single DDSketch bucket at 1% relative accuracy. DDSketch correctly
  collapses p0 and p100 to the same bucket center for such symbols, so `sketch_range = 0`
  while `gt_range > 0`, producing 100% range error. Comparing each of p0/p100 against
  gt_low/gt_high separately avoids this; all symbols pass the 2% tolerance on day `08-11-21`.
- **Batch size:** `--batch-size 50` (default) is important for sketch-finance mode. The
  window processor flushes every 5 minutes of wall clock. If all events fit in one large
  batch, that batch arrives near the end of paced replay, after the final periodic flush
  already fired without data. Small batch sizes spread arrivals over time so each 5-minute
  tick sees data.
- **Result on day `08-11-21`:** `frac_hilo_lt_2pct = 1.0 → PASS`.

---

## Per-query files — what each query branch must add

These are the **only** files that change per query. Everything else is inherited.

### 1. `ground_truth/qN.py` (new file)

Implement the exact metric computation:

```python
def compute_qN_<metric>(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Input:  dataframe with columns [symbol, ts_ms, Last, ...]
            sorted within each symbol by ts_ms
    Output: dataframe with columns [symbol, window_start_ms, <metric_col>, ...]
            one row per (symbol, window) — or one row per window for cross-symbol queries
    """
    ...

def run_qN(
    day: str,
    output_root: Path,
    chunksize: int,
    max_event_minutes: int | None = None,
) -> None:
    day_tag = day_tag_from_arg(day)
    # Use load_filtered_day for data_filtered queries (Q1,Q2,Q4,Q5,Q7,Q8,Q9-Q12)
    # Use load_full_feed_day for full-feed queries (Q3, Q6)
    loader_fn = functools.partial(load_filtered_day, max_event_minutes=max_event_minutes)
    dataframe, _ = timed_load("QN", day_tag, csv_path, loader_fn, chunksize)
    if dataframe.empty:
        log_phase("QN", day_tag, "skip empty")
        return
    log_phase("QN", day_tag, "compute")
    compute_qN_<metric>(dataframe).to_csv(output_root / "QN" / f"{day_tag}.csv", index=False)
```

### 2. `ground_truth/tasks.py`

Add one `elif` branch:

```python
elif query_id == "QN":
    from ground_truth.qN import run_qN
    run_qN(day, output_dir, chunksize, max_event_minutes=max_event_minutes)
```

### 3. `ground_truth/run_gt.py`

Add `"QN"` to both tuples:

```python
ACCEPTED_GT_QUERIES = (..., "QN")
ALL_GT_BATCH = (..., "QN")
```

### 4. `compare.py`

Add three things:

```python
# a) Pattern for get_best_snapshot_for_query
_SKETCH_METRIC_PATTERN["QN"] = r"ddsketch|kll"   # or "countsketch", "hll", etc.

# b) Comparison function
def compare_qN(
    ground_truth: pd.DataFrame,
    sketch_rows: pd.DataFrame,
    *,
    day: str = "",
) -> dict:
    # Extract relevant sketch values using existing helpers:
    #   extract_sketch_quantile(sketch_rows, q)   — for quantile sketches (ddsketch/kll)
    #   extract_countsketch_estimates(sketch_rows) — for frequency sketches (countsketch)
    # Merge with ground truth, compute error metric
    # Return: {"metric": "<name>", "value": float, "threshold": float, "pass": int}
    ...

# c) Register in dispatch table
_COMPARE_DISPATCH["QN"] = compare_qN
```

### 5. `run.py` — QUERY_CONFIG

Verify (do not add, it is already there) that `QUERY_CONFIG["QN"]` has the correct
`dataset`, `sketch_family`, `time_window`, and `group_by`:

```python
QUERY_CONFIG: dict[str, QueryCfg] = {
    ...
    "QN": QueryCfg(("quantile",), "5m", "data_filtered", ("symbol",), "quantile"),
    ...
}
```

---

## Running a query benchmark

```bash
cd datasets_eval/debs/benchmark

# Sketch-finance accuracy — window mode, 1:1 speed, first 60 minutes
python3 run.py test \
    --query QN \
    --day 08-11-21 \
    --mode sketch-finance \
    --accuracy-minutes 60

# Shorter slice (e.g. 10–15 min) for faster iteration (wall-clock ≈ slice length at speed 1)
python3 run.py test \
    --query QN \
    --day 08-11-21 \
    --mode sketch-finance \
    --accuracy-minutes 15

# With smaller OTLP batch size (more frequent Prometheus snapshots, same sketch accuracy)
python3 run.py test \
    --query QN \
    --day 08-11-21 \
    --mode sketch-finance \
    --accuracy-minutes 60 \
    --batch-size 50

# Throughput only — batch mode, max speed, no accuracy comparison
python3 run.py test \
    --query QN \
    --day 08-11-21 \
    --mode throughput \
    --speed 10000 \
    --skip-gt 1

# Ground truth only (standalone, no collector needed)
python3 ground_truth/run_gt.py \
    --query QN \
    --day 08-11-21 \
    --max-event-minutes 60 \
    --out-dir results/ground_truth
```

---

## Results interpretation

`results/report.md` after a sketch-finance run:

```
## Accuracy (sketch vs ground truth)
| query | day      | metric       | value | threshold | pass |
| QN    | 08-11-21 | <metric_name>| 0.97  | 0.95      | 1    |
```

- `pass = 1` → sketch approximation meets the accuracy threshold for this query.
- `pass = 0` → sketch is outside the acceptable error bound; investigate sketch parameters
  or the comparison logic.

`results/throughput.csv` — events/sec at the replay speed used.

`results/latency.csv` — p50/p95/p99 inter-arrival and send-lag across all runs.

---

## Query reference — dataset and sketch family

| Query | Dataset | Sketch | Processor | Notes |
|-------|---------|--------|-----------|-------|
| Q1  | `data_filtered` | DDSketch / KLL | quantile | p50 as EMA proxy |
| Q2  | `data_filtered` | NOP | — | sign-flip on Q1 outputs |
| Q3  | `data` | CountSketch | frequency | top-K by event count |
| Q4  | `data_filtered` | DDSketch / NOP | quantile | p0/p100 per-symbol high/low; compare separately, not range |
| Q5  | `data_filtered` | DDSketch / NOP | quantile | IQR proxy for σ |
| Q6  | `data` | HLL | cardinality | global distinct symbol count |
| Q7  | `data_filtered` | DDSketch | quantile | p50 as mean proxy |
| Q8  | `data_filtered` | DDSketch | quantile | IQR anomaly flags (15-min window) |
| Q9  | `data_filtered` | NOP | — | rolling Bollinger bands |
| Q10 | `data_filtered` | NOP | — | RSI (full-stream state) |
| Q11 | `data_filtered` | NOP | — | MACD (chained EMA state) |
| Q12 | `data_filtered` | NOP | — | Stochastic %K/%D (rolling deque) |
