# DEBS Benchmark

Harness for replaying the DEBS 2022 Grand Challenge dataset through an OpenTelemetry collector, comparing sketch outputs to exact offline ground truth, and reporting accuracy and throughput.

---

## Prerequisites

- Python ≥ 3.9  
  ```bash
  pip install -r requirements.txt
  ```
- Collector binaries built under `opentelemetry-collector-contrib-patch/cmd/`
- Controller binary at `controller/target/release/controller`  
  (override with `CONTROLLER_BIN` env var)

---

## Data setup

```bash
# 1. Download raw DEBS CSVs (~23 GB)
bash datasets_eval/debs/download_data.sh

# 2. Build the price-stream filtered dataset (drops rows missing Last/Trading time,
#    and rows where Last ≤ 0)
python3 datasets_eval/debs/analysis/code/filter_data.py
```

Both steps must be done from the repo root. The filtered data lands in `datasets_eval/debs/data_filtered/`.

---

## Running benchmarks

All scripts resolve paths relative to their own location (`__file__`), so you can run them from any directory — the script directory, the repo root, or anywhere else. The `BENCH_ROOT` env var is not required.

### Single accuracy run (statistical queries)

```bash
python3 datasets_eval/debs/benchmark/run.py test \
  --query Q1 \
  --day 08-11-21 \
  --mode sketch-finance \
  --accuracy-minutes 10
```

### Throughput / NOP queries

```bash
python3 datasets_eval/debs/benchmark/run.py test \
  --query Q2 \
  --day 08-11-21 \
  --mode throughput
```

### Full matrix (all queries × all days)

```bash
python3 datasets_eval/debs/benchmark/run.py matrix \
  --queries Q1 Q3 Q4 Q5 Q6 Q7 Q8 \
  --days 08-11-21 09-11-21 10-11-21 11-11-21 12-11-21
```

---

## `run.py test` arguments

| Argument | Default | Description |
|---|---|---|
| `--query` | `Q1` | Query ID to run (`Q1`–`Q12`) |
| `--day` | `08-11-21` | Trading day for ground truth and results tagging |
| `--days` | same as `--day` | Comma-separated days to replay; defaults to `--day` |
| `--mode` | `sketch-finance` | `sketch-finance` (paced replay, accuracy eval) or `throughput` (max speed) |
| `--speed` | `100` | Speed multiplier for paced replay (`1` = real time, `100` = 100× faster) |
| `--batch-size` | `5000` | OTLP export batch size (number of data points per gRPC call) |
| `--accuracy-minutes` | `0` | Minutes of event time to replay; `0` means full day |
| `--skip-gt` | `0` | Set to `1` to skip ground truth computation (use pre-computed CSVs) |
| `--sketch` | (auto) | Override sketch type: `ddsketch`, `kll`, `countsketch`, `countminsketch`, `hll` |
| `--collector` | (auto) | Path to collector binary (overrides auto-selection) |
| `--results-dir` | `results/` | Directory for output CSVs and logs |
| `--clear-results` | off | Remove `throughput.csv` and `latency.csv` before this run |

---

## `run.py matrix` arguments

| Argument | Default | Description |
|---|---|---|
| `--queries` | all Q1–Q12 | Space-separated query IDs |
| `--days` | all 5 trading days | Space-separated day tags |
| `--mode` | `sketch-finance` | Replay mode |
| `--speed` | `100` | Speed factor |
| `--sketch-quantile` | `ddsketch` | Sketch for quantile queries (Q1, Q4, Q5, Q7, Q8) |
| `--sketch-freq` | `countsketch` | Sketch for frequency queries (Q3) |
| `--sketch-card` | `hll` | Sketch for cardinality queries (Q6) |
| `--skip-gt` | `1` | Skip GT computation (pre-compute separately with `run_gt.py`) |
| `--results-dir` | `results/` | Output directory |
| `--clear-results` | off | Remove aggregate CSVs before the run |

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `CONTROLLER` | `http://localhost:8080` | Controller API base URL |
| `CONTROLLER_BIN` | `controller/target/release/controller` | Controller binary path |
| `CONTROLLER_OPAMP_PORT` | `4320` | OpAMP server port |
| `COLLECTOR_EXPORT_PORT` | `8889` | Port freed before each run (sketch metrics exporter in generated YAML) |
| `COLLECTOR_READY_TIMEOUT_S` | `30` | Wait for OTLP gRPC to accept connections after collector start |
| `OTLP_GRPC_ENDPOINT` | `localhost:4327` | OTLP gRPC bind used by `run.py` + replay (must match `--set` overrides) |
| `OTLP_HTTP_ENDPOINT` | `localhost:4328` | OTLP HTTP bind used by `run.py` |
| `AUTO_START_CONTROLLER` | `1` | Auto-start controller if not reachable (`0` to disable) |
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
| `results/report.md` | Accuracy table, throughput, latency summary |
| `results/ground_truth/QN/<day>.csv` | Exact offline reference values |
| `results/sketch_output/QN/<day>.jsonl` | Per-window sketch output (OTLP-JSON via file exporter) |
| `results/comparison/QN_<day>.csv` | Per-run accuracy metric and pass/fail |
| `results/throughput.csv` | Events/sec statistics per run |
| `results/latency.csv` | Send-time statistics per run |
| `results/collector.log` | Collector stderr |

---

## Warmup behaviour

The collector uses 5-minute tumbling windows. Comparing sketch output to ground truth requires at least one complete window before evaluation.

- **Q1** (`skip_warmup_windows=1`): EMA38/EMA100 have a cold-start bias in the first window because the exponential average has not converged yet. The first window is skipped, and all subsequent windows are compared against ground truth.
- **Q3–Q8** (`skip_warmup_windows=0`): These queries are stateless within each window (no exponential state to warm up). Comparison uses JSONL window flushes from the file exporter.

Override with `compare.py --skip-warmup-windows N` if needed.

---

## Query reference

| Query | Sketch | GT | Window | Description |
|---|---|---|---|---|
| Q1 | DDSketch / KLL | Yes | 5m | EMA38 / EMA100 per symbol |
| Q2 | NOP | No | 5m | EMA crossover signals (throughput only) |
| Q3 | CountSketch | Yes | 5m | Top-K most active symbols by trade count |
| Q4 | DDSketch / KLL | Yes | 5m | Per-symbol price range (min / max) |
| Q5 | DDSketch / KLL | Yes | 5m | Per-symbol log-return volatility (σ) |
| Q6 | HLL | Yes | 5m | Distinct active symbol count |
| Q7 | DDSketch / KLL | Yes | 5m | Per-symbol mean price |
| Q8 | DDSketch / KLL | Yes | 15m | Per-symbol IQR |
| Q9–Q12 | NOP | No | 5m | Advanced indicators (throughput only) |
