# Evaluation Suite (Paper §6)

Reproducible benchmarks for the SketchCollector paper evaluation.

## Quick start

```bash
# 1. Build all binaries
./evaluation/build_collectors.sh

# 2. Run evaluation (quick mode for CI)
./evaluation/run_eval.sh --quick

# 3. Full evaluation
./evaluation/run_eval.sh
```

## What it measures

| Table | Paper section | What | Metrics |
|-------|--------------|------|---------|
| Table 3 | §6 | Per-sketch performance | Bandwidth, CPU, memory per sketch type |
| Table 4 | §6 | Scalability | Throughput vs series count (100–10K) |
| Table 5 | §6 | Delta ablation | Full vs delta compression bandwidth |
| Table 6 | §6 | TCO comparison | Dollar cost: traditional TSDB vs sketch pipeline |

## Accuracy testing

```bash
# Compare sketch query results to ground truth
python3 evaluation/accuracy_test.py \
    --ground-truth raw_data.csv \
    --sketch-results sketch_results.csv \
    --output accuracy.csv
```

## Prerequisites

- Go 1.22+ (for collector and bench tools)
- Rust toolchain (for controller)
- Python 3 (for accuracy analysis)
- `nc` (netcat, for port checks)

## Output

```
evaluation/results/<timestamp>/
  table3_per_sketch.csv      # sketch type × bandwidth × CPU × memory
  table4_scalability.csv     # series count × throughput
  table5_delta_ablation.csv  # full vs delta bandwidth
  table6_tco.json           # TCO before/after comparison
  raw/                      # per-run CSVs and JSON summaries
```
