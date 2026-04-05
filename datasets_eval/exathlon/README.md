# Exathlon Dataset Eval

This folder contains the Exathlon-specific assets used by the sketch benchmark pipeline.

## Folder contents

- `analysis/`
  - `code/`: analysis scripts for frequency, window, and cardinality profiling.
  - `results/`: generated CSV outputs used by benchmark and documentation.
- `benchmark/`
  - benchmark harness and scripts for running query evaluations.
- `docs/`
  - supporting notes: dataset statistics, benchmark query definitions, and methodology.
- `exathlon/data/raw/`
  - local raw Exathlon CSV files consumed by this evaluation pipeline.

## Download and prepare dataset

1. Clone the official Exathlon dataset repository (example):

```bash
git clone https://github.com/exathlonbenchmark/exathlon.git
```

2. Enter the cloned repo and run extraction:

```bash
cd dataset/exathlon
./extract_data.sh
```

3. Copy raw data into this project’s Exathlon eval path:

```bash
cd /path/to/sketchlib-ProjectASAP
mkdir -p DataCollector/datasets_eval/exathlon/exathlon/data/raw
cp -r dataset/exathlon/data/raw/* DataCollector/datasets_eval/exathlon/exathlon/data/raw/
```

After this, raw files should be available under:

- `DataCollector/datasets_eval/exathlon/exathlon/data/raw/`

