# Exathlon Benchmark

Harness for replaying the Exathlon labeled Spark + HPC telemetry dataset through an OpenTelemetry collector, comparing sketch outputs to exact offline ground truth, and reporting accuracy, throughput, and latency.

---

## Prerequisites

- Python ≥ 3.9
  ```bash
  python3 -m venv venv
  source venv/bin/activate
  pip install -r requirements.txt
  ```
- Collector binaries built under `opentelemetry-collector-contrib-patch/cmd/`
- Controller binary at `controller/target/release/controller`  
  (override with `CONTROLLER_BIN` env var)

---

## Data setup

The raw Exathlon CSVs live under `datasets_eval/exathlon/exathlon/data/raw/app*/`. No download or filtering step is required — all 93 files are used as-is.

Each CSV has one timestamp column `t` (Unix epoch seconds, UTC) and **2 283 metric columns** named `{entity}_{metric_base}_{aggregation}`. The entity prefix is a numeric executor ID (`1`–`5`), `driver`, or an HPC node ID (`node5`–`node8`). Values of `-1.0` are sentinels meaning the metric was inactive at that timestamp and are skipped during replay.

Run the analysis scripts once to populate the summary CSVs used by the ground-truth scripts:

```bash
cd datasets_eval/exathlon/analysis/code
python3 analyze_frequency.py
python3 analyze_windows.py
python3 analyze_cardinality.py
```

Outputs land in `datasets_eval/exathlon/analysis/results/summaries/`.

---

## Benchmark components

```
benchmark/
├── run.py                  Orchestration: test / matrix subcommands
├── replay.py               Wide-to-narrow pivot; streams exathlon CSV as OTLP gauge metrics
├── scrape.py               Polls Prometheus metrics endpoint → CSV
├── compare.py              Compares Prometheus sketch output to ground-truth CSV
├── analyze.py              Aggregates send-times into latency metrics; writes report.md
├── summarize.py            Aggregates per-file comparison CSVs → markdown table
├── common.py               Shared constants, path helpers, column-name parser
├── requirements.txt        Python dependencies
└── ground_truth/
    ├── run_gt.py           Parallel coordinator for ground-truth computation
    ├── tasks.py            Per-query exact computation (Q1–Q9)
    └── common.py           Streaming CSV loader, window accumulator, threshold helpers
```

### replay.py — wide-to-narrow pivot

Each Exathlon CSV row is a snapshot of the entire cluster at one second. The replay performs a **wide-to-narrow pivot** on the fly: every non-sentinel column value in a row becomes one OTLP gauge data point emitted with three attributes:

| Attribute | Source | Example |
|---|---|---|
| `entity` | Column prefix before first `_` | `1`, `driver`, `node5` |
| `metric_base` | Middle portion of column name | `CodeGenerator_compilationTime` |
| `aggregation` | Last `_`-delimited token | `count`, `p95`, `value` |

At the natural ~1 Hz sampling rate, one row produces up to **2 283 data points**. The default `--batch-size 50000` therefore covers roughly 22 rows per gRPC export call.

### scrape.py — Prometheus polling

Polls `http://localhost:8889/metrics` every 2 seconds and appends rows to `results/sketch_output/QN/{safe_tag}.csv`. Columns: `scrape_wall_ns`, `metric`, `labels` (JSON), `value`.

### compare.py — sketch vs ground truth

Loads the ground-truth CSV for the given query and file, selects the best Prometheus scrape snapshot (most non-zero sketch rows), extracts estimates, and writes a one-row-per-metric comparison CSV to `results/comparison/`. For Q1 runs with `send_times.csv`, the comparator rebuilds the 5-minute exact reference from raw data around the selected scrape's observed replay event time, so it aligns with collector timer-driven sketch flushes instead of Unix-epoch buckets.

Q1 alignment notes:

- The canonical offline Q1 ground truth uses Unix-epoch 5-minute windows.
- DDSketch window mode flushes on the collector's wall-clock ticker, so its output window is not necessarily aligned to Unix-epoch or first-event replay buckets.
- `compare.py` first uses `send_times.csv` to select a scrape aligned with replay progress, then rebuilds the exact Q1 reference for the raw 5-minute event interval ending at the last emitted event before that scrape.
- This avoids comparing a DDSketch flush window against a shifted ground-truth window, which can otherwise make an accurate sketch look inaccurate.
- If the collector does not emit quantile `0.95`, Q1 falls back to comparing emitted `0.90` against exact p90 for the `frac_q95_lt_1pct` row; the row label is retained for report compatibility.

Accuracy metrics per query:

| Query | Metric | Pass threshold |
|---|---|---|
| Q1 | `frac_q50_lt_1pct`, `frac_q95_lt_1pct`, `frac_q99_lt_1pct` | ≥ 0.90 / 0.90 / 0.85 |
| Q3 | `topk_overlap`, `rank_correlation` | ≥ 0.80 / 0.70 |
| Q4 | `frac_min_lt_2pct`, `frac_max_lt_2pct` | ≥ 0.90 |
| Q5 | `frac_iqr_lt_10pct` | ≥ 0.85 |
| Q6 | `hll_rel_err` | ≤ 0.05 |
| Q7 | `entity_topk_overlap` | ≥ 0.80 |
| Q8 | `frac_drift_p95_lt_20pct` | ≥ 0.80 |
| Q9 | `sat_ratio_mae` | ≤ 0.05 |

### ground_truth/tasks.py — exact reference values

Computes exact answers offline from the raw CSV for Q1–Q9. Q2, Q10, Q11, and Q12 are throughput-only (no ground truth).

| Query | Algorithm | Window |
|---|---|---|
| Q1 | p50 / p95 / p99 per `(entity, metric_base)` | 5 min |
| Q3 | Top-10 metrics by threshold-exceedance count | 5 min |
| Q4 | Exact min / max / range per `(entity, metric_base)` | 1 / 5 / 15 / 30 / 60 min |
| Q5 | IQR bounds and anomaly rate per `(entity, metric_base)` | 15 min |
| Q6 | Distinct active `(entity, metric_base, aggregation)` count | 5 min |
| Q7 | Top-3 entities by IQR-anomaly event volume | 5 min |
| Q8 | `|p95_t − p95_{t-1}|` drift per `(entity, metric_base)` | 5 min |
| Q9 | Saturation ratio per entity (file-local p95 threshold) | 5 min |

**Threshold definition (Q3 / Q9):** For each `(entity, metric_base)`, the threshold equals the 95th percentile of all non-sentinel values in the file. This is computed in a single streaming pass before counting exceedances.

### analyze.py — latency and throughput report

Reads `results/send_times.csv` (emit wall time and event time of each OTLP export) and appends a row to `results/latency.csv`. Also reads all comparison and throughput CSVs and writes `results/report.md`.

### summarize.py — aggregate markdown table

Reads all per-file CSVs under `results/comparison/`, groups by `(query, metric, threshold)`, and writes `results/test_results.md` with avg / min / max and an all-pass indicator per metric.

---

## Running benchmarks

All scripts resolve paths relative to `__file__`, so they can be run from any directory.

### Step 1 — Pre-compute ground truth (one time per file set)

```bash
python3 datasets_eval/exathlon/benchmark/ground_truth/run_gt.py \
  --query all \
  --file all \
  --out-dir datasets_eval/exathlon/benchmark/results/ground_truth
```

To compute for a single query and file:

```bash
python3 datasets_eval/exathlon/benchmark/ground_truth/run_gt.py \
  --query Q1 \
  --file app1/1_0_10000_17
```

### Step 2 — Single accuracy run

```bash
python3 datasets_eval/exathlon/benchmark/run.py test \
  --query Q1 \
  --file app1/1_0_10000_17 \
  --mode sketch-telemetry
```

Quick smoke run (partial event-time replay):

```bash
python3 datasets_eval/exathlon/benchmark/run.py test \
  --query Q1 \
  --file app1/1_0_10000_17 \
  --mode sketch-telemetry \
  --accuracy-minutes 10
```

### Step 3 — Throughput / NOP queries

```bash
python3 datasets_eval/exathlon/benchmark/run.py test \
  --query Q2 \
  --file app1/1_0_10000_17 \
  --mode throughput
```

### Step 4 — Full matrix (all queries × all files)

```bash
python3 datasets_eval/exathlon/benchmark/run.py matrix \
  --queries Q1 Q3 Q4 Q5 Q6 Q7 Q8 Q9 \
  --files app1/1_0_10000_17 app5/5_0_50000_38 app9/9_0_100000_1
```

### Step 5 — Aggregate results

```bash
python3 datasets_eval/exathlon/benchmark/summarize.py \
  --results-dir datasets_eval/exathlon/benchmark/results
```

---

## `run.py test` arguments

| Argument | Default | Description |
|---|---|---|
| `--query` | `Q1` | Query ID to run (`Q1`–`Q12`) |
| `--file` | `app1/1_0_10000_17` | File tag for ground truth and results labelling |
| `--files` | same as `--file` | Comma-separated file tags to replay; defaults to `--file` |
| `--mode` | `sketch-telemetry` | `sketch-telemetry` (paced, accuracy eval) or `throughput` (max speed) |
| `--speed` | `100` | Speed multiplier for paced replay (`1` = real time, `100` = 100× faster) |
| `--batch-size` | `50000` | OTLP export batch size in data points (~22 CSV rows at 2 283 metrics/row) |
| `--accuracy-minutes` | `0` | Minutes of event time to replay; `0` = full file |
| `--skip-gt` | `0` | Set to `1` to skip ground-truth computation (use pre-computed CSVs) |
| `--sketch` | (auto) | Override sketch: `ddsketch`, `kll`, `countsketch`, `countminsketch`, `hll` |
| `--collector` | (auto) | Path to collector binary (overrides auto-selection by sketch family) |
| `--results-dir` | `results/` | Directory for output CSVs and logs |
| `--clear-results` | off | Remove `throughput.csv` and `latency.csv` before this run |

---

## `run.py matrix` arguments

| Argument | Default | Description |
|---|---|---|
| `--queries` | all Q1–Q12 | Space-separated query IDs |
| `--files` | 3 default files | Space-separated file tags |
| `--mode` | `sketch-telemetry` | Replay mode |
| `--speed` | `100` | Speed factor |
| `--batch-size` | `50000` | OTLP batch size |
| `--skip-gt` | `1` | Skip GT recomputation (pre-compute with `run_gt.py` first) |
| `--sketch-quantile` | `ddsketch` | Sketch for quantile queries (Q1, Q4, Q5, Q8) |
| `--sketch-freq` | `countsketch` | Sketch for frequency queries (Q3, Q7) |
| `--sketch-card` | `hll` | Sketch for cardinality queries (Q6, Q9) |
| `--collector` | (auto) | Path override for all collectors |
| `--results-dir` | `results/` | Output directory |
| `--clear-results` | off | Remove aggregate CSVs before the run |

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `CONTROLLER` | `http://localhost:8080` | Controller API base URL |
| `CONTROLLER_BIN` | `controller/target/release/controller` | Controller binary path |
| `CONTROLLER_OPAMP_PORT` | `4320` | OpAMP server port used when auto-starting the controller |
| `PROMETHEUS_METRICS_URL` | `http://localhost:8889/metrics` | Prometheus scrape endpoint |
| `AUTO_START_CONTROLLER` | `1` | Auto-start controller if unreachable (`0` = disabled) |
| `METRIC` | `system.telemetry` | OTLP metric name sent to the collector |
| `COLLECTOR_DDSKETCH` | (default path) | Override DDSketch collector binary |
| `COLLECTOR_KLL` | (default path) | Override KLL collector binary |
| `COLLECTOR_HLL` | (default path) | Override HLL collector binary |
| `COLLECTOR_COUNTSKETCH` | (default path) | Override CountSketch collector binary |
| `COLLECTOR_COUNTMINSKETCH` | (default path) | Override CountMinSketch collector binary |
| `COLLECTOR_NOP` | (default path) | Override NOP collector binary |
---

## Results layout

After a run, `results/` contains:

| Path | Description |
|---|---|
| `results/report.md` | Accuracy table, throughput summary, latency stats |
| `results/test_results.md` | Aggregated pass/fail table across all files (from `summarize.py`) |
| `results/ground_truth/QN/<safe_tag>.csv` | Exact offline reference values per query and file |
| `results/sketch_output/QN/<safe_tag>.csv` | Raw Prometheus scrape rows (scrape_wall_ns, metric, labels, value) |
| `results/comparison/QN_<safe_tag>.csv` | Per-run accuracy metric, value, threshold, pass flag |
| `results/throughput.csv` | Events/sec and total events per run |
| `results/latency.csv` | Inter-arrival and send-lag percentiles per run |
| `results/send_times.csv` | Per-export emit wall time and event timestamp |
| `results/collector.log` | Collector stdout/stderr |
| `results/controller.log` | Controller stdout/stderr (auto-started runs only) |

`<safe_tag>` is the file tag with `/` replaced by `_` (e.g. `app1_1_0_10000_17`).

---

## File tag format

File tags identify a single Exathlon CSV:

```
app1/1_0_10000_17
└─┬─┘ └──┬──────┘
  │       └── {app_id}_{fault_type}_{target_length}_{run_id}
  └────────── app directory (app1 – app10)
```

Fault types: `0` = baseline, `1`–`5` = injected faults (resource contention, process kills, etc.).

Default files used when no `--file` / `--files` is specified:

| Tag | App | Fault | Rows |
|---|---|---|---|
| `app1/1_0_10000_17` | Spark app 1 | Baseline | ~3 500 |
| `app5/5_0_50000_38` | Spark app 5 | Baseline | ~50 000 |
| `app9/9_0_100000_1` | Spark app 9 | Baseline | ~100 000 |

---

## Query reference

| Query | Sketch | GT | Window | Description |
|---|---|---|---|---|
| Q1 | DDSketch / KLL | Yes | 5 min | p50 / p95 / p99 per `(entity, metric_base)` |
| Q2 | NOP | No | 5 min | Tail amplification ratio — derived from Q1 (throughput only) |
| Q3 | CountSketch / CMS | Yes | 5 min | Top-10 metrics by threshold-exceedance count |
| Q4 | DDSketch / KLL | Yes | 1 / 5 / 15 / 30 / 60 min | Min / max / range per `(entity, metric_base)` |
| Q5 | DDSketch | Yes | 15 min | IQR-based anomaly flags (Tukey fences) per metric |
| Q6 | HLL | Yes | 5 min | Distinct active metric count per window |
| Q7 | CountSketch / CMS | Yes | 5 min | Top-3 entities by anomaly event volume |
| Q8 | DDSketch / KLL | Yes | 5 min | Inter-window p95 quantile drift per metric |
| Q9 | HLL | Yes | 5 min | Saturation ratio per entity (USE method) |
| Q10 | NOP | No | N/A | EWMA change-point on KPI stream (throughput only) |
| Q11 | NOP | No | 15 / 30 min | Cross-metric correlation per entity (throughput only) |
| Q12 | NOP | No | 1 / 5 / 15 / 30 / 60 min | Composite health score — combines Q2 + Q5 + Q9 (throughput only) |
