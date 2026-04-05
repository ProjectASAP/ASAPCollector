# Exathlon Benchmark

Harness for replaying the Exathlon labeled Spark + HPC telemetry dataset through an OpenTelemetry collector, comparing sketch outputs to exact offline ground truth, and reporting accuracy and throughput.

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

The raw Exathlon CSVs are already present under `datasets_eval/exathlon/data/raw/app*/`. No download or filtering step is required — all 93 files are used as a single raw telemetry stream.

Run the analysis scripts once to generate the summary CSVs consumed by the ground-truth scripts:

```bash
cd DataCollector/datasets_eval/analysis/code
python3 analyze_frequency.py
python3 analyze_windows.py
python3 analyze_cardinality.py
```

Outputs land in `datasets_eval/exathlon/analysis/results/summaries/`.

> **Wide-to-narrow pivot:** each CSV row contains one timestamp (`t`) and ~2,283 metric columns. The replay script emits one OTLP gauge data point per non-null column per row, producing ~2,283 OTLP data points per row at ~1 Hz — approximately **2,283 data points/second per file** at natural pace.

---

## Running benchmarks

All commands are run from the repo root (or with `BENCH_ROOT` pointing to `datasets_eval/exathlon/benchmark/`).

---

## Query reference

| Query | Sketch | GT | Window | Description |
|---|---|---|---|---|
| Q1 | DDSketch / KLL | Yes | 5m | p50 / p95 / p99 per (entity, metric\_base) |
| Q2 | NOP | No | 5m | Tail amplification ratio — derived from Q1 (throughput only) |
| Q3 | CountMinSketch + SpaceSaving | Yes | 5m | Top-K (K=10) metrics by threshold-exceedance count |
| Q4 | DDSketch / KLL | Yes | 1/5/15/30/60m | Min / max / range per metric |
| Q5 | DDSketch | Yes | 15m | IQR-based anomaly flags (Tukey fences) per metric |
| Q6 | HLL | Yes | 5m | Distinct active metric count per window |
| Q7 | CountMinSketch + SpaceSaving | Yes | 5/15m | Top-K (K=3) entities by anomaly event volume |
| Q8 | DDSketch / KLL | Yes | 5m (1m stress) | Inter-window quantile drift `\|p95_t − p95_{t-1}\|` |
| Q9 | HLL + NOP (hybrid) | Yes | 5/15m | Saturation ratio per entity (USE method) |
| Q10 | NOP | No | N/A | EWMA change-point on KPI stream (throughput only) |
| Q11 | NOP | No | 15/30m rolling | Cross-metric correlation per entity (throughput only) |
| Q12 | Composite | No | 1/5/15/30/60m | Composite health score — combines Q2 + Q5 + Q9 outputs |
