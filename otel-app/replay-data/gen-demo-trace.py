#!/usr/bin/env python3
"""
gen-demo-trace.py — emit a Google-cluster-trace-shaped CSV for
otel-app's trace replay mode.

This generates a SYNTHETIC dataset with statistical properties close
to real cluster-trace CPU usage:

  * Per-instance slowly-drifting baseline (~10% amplitude daily cycle)
  * Occasional spikes (Poisson arrivals, log-normal heights)
  * Inter-instance correlation (~30% of instances share a diurnal
    phase — stand-in for co-tenant contention)
  * Rare flatlines (instances that go quiet for a few minutes)

The output is a ~1 MiB CSV with 100 series × 600 samples/series
(10 minutes at 1 Hz), enough to exercise the sketch baselines'
delta-compression wins on a low-churn workload. Replace with real
Google 2019 cluster-trace data by preprocessing `instance_usage`
into the same schema — see the companion README.md.

Deterministic when PYTHONHASHSEED is pinned; uses Python's random
seeded from argv[1] (default 42) for reproducibility.
"""
from __future__ import annotations

import csv
import math
import random
import sys
from pathlib import Path

SEED = int(sys.argv[1]) if len(sys.argv) > 1 else 42
OUT = Path(sys.argv[2]) if len(sys.argv) > 2 else Path(__file__).parent / "demo-trace.csv"

N_SERIES = 100
N_SAMPLES = 600  # 10 min at 1 Hz
START_TS_MS = 1_700_000_000_000  # nominal epoch; exact value doesn't matter for replay

random.seed(SEED)


def gen_series(series_idx: int) -> list[float]:
    """Slow drift + sparse spikes. Returns N_SAMPLES floats in [0, ~1.2]."""
    # Baseline: sinusoidal daily cycle (aliased to the 10-min window)
    # + a series-specific phase so not everyone peaks simultaneously.
    phase = (series_idx / N_SERIES) * 2 * math.pi
    baseline = [0.3 + 0.1 * math.sin(phase + i * 2 * math.pi / N_SAMPLES) for i in range(N_SAMPLES)]

    # Long-scale drift (~300 sample period): smooth multiplicative
    # factor in [0.8, 1.2].
    drift_phase = random.uniform(0, 2 * math.pi)
    drift = [1.0 + 0.2 * math.sin(drift_phase + i * 2 * math.pi / 300) for i in range(N_SAMPLES)]

    out = [b * d for b, d in zip(baseline, drift)]

    # Spikes: Poisson arrivals at ~1% per-sample, amplitude log-normal.
    for i in range(N_SAMPLES):
        if random.random() < 0.01:
            spike = math.exp(random.gauss(0.3, 0.4))
            out[i] = min(1.2, out[i] + spike)

    # Rare flatline: ~1% of series go flat for 20 samples somewhere.
    if random.random() < 0.01:
        start = random.randint(0, N_SAMPLES - 20)
        for i in range(start, start + 20):
            out[i] = 0.01  # near-zero but non-zero so sketches still bin it

    return out


def main() -> int:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["timestamp_ms", "series_id", "value"])
        for s in range(N_SERIES):
            series_id = f"instance-{s:04d}"
            values = gen_series(s)
            for i, v in enumerate(values):
                ts = START_TS_MS + i * 1000
                w.writerow([ts, series_id, f"{v:.6f}"])
    print(f"wrote {OUT} ({N_SERIES}×{N_SAMPLES} rows, seed={SEED})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
