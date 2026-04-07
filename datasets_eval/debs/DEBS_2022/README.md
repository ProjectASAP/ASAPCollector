# DEBS 2022 — Benchmark Documentation

This directory contains all documentation for benchmarking the **DataCollector** against the [DEBS 2022 Grand Challenge](https://2022.debs.org/call-for-grand-challenge-solutions/) financial tick dataset ([Zenodo 6382482](https://doi.org/10.5281/zenodo.6382482)).

---

## Contents

| File | Description |
|------|-------------|
| [`01_dataset_statistics.md`](01_dataset_statistics.md) | Dataset field definitions, evaluation streams (`data/` vs `data_filtered/`), OTLP mapping, time window properties, and the full statistical analysis of the DEBS 2022 corpus — including inter-arrival frequency, tumbling window event counts, and cardinality tables for both the full feed and last-trade stream. Also contains the per-query evaluation matrix mapping each of the 12 queries to its dataset, window size, and test type. |
| [`02_benchmark_queries.md`](02_benchmark_queries.md) | Specifications for all 12 benchmark queries (Q1–Q12): purpose, mathematical formula, data requirements, DataCollector approach (processor type and configuration), validation procedure, per-query success thresholds, and evaluation configuration tables. |
| [`03_benchmark_methodology.md`](03_benchmark_methodology.md) | End-to-end benchmark methodology: how DEBS CSV data is converted to OTLP and ingested by the DataCollector connector, how the controller/plan selects sketch processors, and how each of the three benchmark types (Throughput, Latency, Financial Statistics Validation) is conducted — including the ground-truth and sketch-output persistence strategy to avoid recomputation. Contains environment and results placeholders. |

---

## Quick orientation

```
DEBS CSV (data/ or data_filtered/)
        │
        │  replay script: CSV row → OTLP ExportMetricsServiceRequest
        │  metric: financial.last_trade_price (Gauge)
        │  attributes: symbol, exchange, sectype
        ▼
DataCollector OTLP receiver
        │
        │  POST /api/v1/plan  →  controller selects processor
        │    quantile   → ddsketchprocessor / kllprocessor
        │    frequency  → countsketchprocessor
        │    cardinality→ hllprocessor
        │    (exact)    → NOP processor
        ▼
Sketch / NOP processor pipeline
        │
        ▼
Prometheus scrape / collector export
        │
        ├── Throughput: events/s at peak and sustained load
        ├── Latency: p50/p95/p99 end-to-end delay
        └── Sketch-finance: compare to ground truth stored in results/ground_truth/
```

See [`03_benchmark_methodology.md`](03_benchmark_methodology.md) for full detail.
