from __future__ import annotations

"""Q1 — Windowed distribution profiling (p50 / p95 / p99) per (entity, metric_base).

Purpose
-------
Validate quantile sketch fidelity for system telemetry distributions.

SRE teams track p50 / p95 / p99 latency and resource utilisation per
service / entity to define SLOs and detect degradation without storing raw
samples.  This script computes the **exact** values that a DDSketch or KLL
sketch should approximate within its error bound.

Formula
-------
For each (entity, metric_base, window):
    p50  = exact 50th percentile of all non-sentinel values
    p95  = exact 95th percentile
    p99  = exact 99th percentile

Derived metrics (Q2 — tail amplification ratio):
    tail_ratio_p99_p50 = p99 / p50   (spikiness / burstiness indicator)
    tail_ratio_p95_p50 = p95 / p50

Window sizes evaluated
----------------------
Primary  : 5 min  (~298.5 samples/window at the natural ~1 Hz row rate)
Sensitivity: 1 min, 15 min, 30 min, 1 hour

Each window size produces a separate output CSV under:
    <out_dir>/Q1/<safe_file_tag>_<window_label>.csv

A combined CSV with all window sizes is also written:
    <out_dir>/Q1/<safe_file_tag>.csv

Output columns
--------------
entity, metric_base, window_start_s, window_size_s, window_label,
p50, p95, p99, tail_ratio_p99_p50, tail_ratio_p95_p50, count

References
----------
- Jacob et al. — Exathlon: A Benchmark for Explainable Anomaly Detection
  over Time Series, VLDB 2021 (https://doi.org/10.14778/3476249.3476307)
- Masson, Rim, Lee — DDSketch: A Fast and Fully-Mergeable Quantile Sketch
  with Relative-Error Guarantees, PVLDB 2019
  (https://arxiv.org/abs/1908.10693)
- Karnin, Lang, Liberty — Optimal Quantile Approximation in Streams,
  FOCS 2016 (https://arxiv.org/abs/1603.05346)
- Beyer et al. — Site Reliability Engineering, O'Reilly 2016, ch. 4
  (https://sre.google/sre-book/service-level-objectives/)
"""

import argparse
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

_BENCHMARK_ROOT = Path(__file__).resolve().parent.parent
if str(_BENCHMARK_ROOT) not in sys.path:
    sys.path.insert(0, str(_BENCHMARK_ROOT))

from common import file_csv_path, file_tag_safe
from ground_truth.common import (
    WINDOW_1MIN_S,
    WINDOW_5MIN_S,
    WINDOW_15MIN_S,
    WINDOW_30MIN_S,
    WINDOW_1HR_S,
    _stream_long_chunks,
    log_phase,
)

# ---------------------------------------------------------------------------
# Window configuration
# ---------------------------------------------------------------------------

# (window_size_s, human-readable label)
ALL_WINDOWS: list[tuple[int, str]] = [
    (WINDOW_1MIN_S,  "1min"),
    (WINDOW_5MIN_S,  "5min"),   # primary
    (WINDOW_15MIN_S, "15min"),
    (WINDOW_30MIN_S, "30min"),
    (WINDOW_1HR_S,   "1hour"),
]

PRIMARY_WINDOW_S = WINDOW_5MIN_S

# Minimum number of values in a window required to compute a meaningful
# percentile.  Windows below this threshold are still included but flagged
# with count < MIN_WINDOW_SAMPLES so callers can filter them.
MIN_WINDOW_SAMPLES = 2


# ---------------------------------------------------------------------------
# Core computation — single-pass multi-window accumulation
# ---------------------------------------------------------------------------

def _accumulate_all_windows(
    csv_path: Path,
    windows: list[tuple[int, str]],
    chunksize: int,
) -> dict[int, dict[tuple, list]]:
    """Stream the CSV once and accumulate values for every window size.

    Returns
    -------
    ``{window_s: {(entity, metric_base, window_start_s): [float, ...]}}``

    A single streaming pass is shared across all requested window sizes,
    avoiding redundant CSV reads and melt operations.
    """
    from collections import defaultdict

    # Pre-initialise one accumulator dict per window size.
    acc: dict[int, dict] = {ws: defaultdict(list) for ws, _ in windows}
    window_sizes = [ws for ws, _ in windows]

    for chunk in _stream_long_chunks(csv_path, chunksize):
        ts = chunk["ts_s"].to_numpy(dtype=np.int64)
        entities = chunk["entity"].to_numpy()
        metric_bases = chunk["metric_base"].to_numpy()
        values = chunk["value"].to_numpy(dtype=np.float64)

        for ws in window_sizes:
            ws_starts = (ts // ws) * ws
            for i in range(len(values)):
                acc[ws][(entities[i], metric_bases[i], int(ws_starts[i]))].append(values[i])

    return acc


def _acc_to_dataframe(
    acc: dict[tuple, list],
    window_s: int,
    window_label: str,
) -> pd.DataFrame:
    """Convert one window-size accumulator dict to the Q1 output DataFrame."""
    _COLS = [
        "entity", "metric_base", "window_start_s", "window_size_s",
        "window_label", "p50", "p90", "p95", "p99",
        "tail_ratio_p99_p50", "tail_ratio_p95_p50", "count",
    ]
    if not acc:
        return pd.DataFrame(columns=_COLS)

    rows = []
    for (entity, mb, ws), values in acc.items():
        arr = np.asarray(values, dtype=np.float64)
        p50 = float(np.percentile(arr, 50))
        p90 = float(np.percentile(arr, 90))
        p95 = float(np.percentile(arr, 95))
        p99 = float(np.percentile(arr, 99))
        tail_99_50 = (p99 / p50) if p50 != 0.0 else float("nan")
        tail_95_50 = (p95 / p50) if p50 != 0.0 else float("nan")
        rows.append({
            "entity":             entity,
            "metric_base":        mb,
            "window_start_s":     ws,
            "window_size_s":      window_s,
            "window_label":       window_label,
            "p50":                p50,
            "p90":                p90,
            "p95":                p95,
            "p99":                p99,
            "tail_ratio_p99_p50": tail_99_50,
            "tail_ratio_p95_p50": tail_95_50,
            "count":              len(arr),
        })

    df = pd.DataFrame(rows)
    df.sort_values(["entity", "metric_base", "window_start_s"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def compute_q1_single_window(
    csv_path: Path,
    window_s: int,
    window_label: str,
    chunksize: int,
) -> pd.DataFrame:
    """Return exact p50/p95/p99 for every (entity, metric_base, window).

    Convenience wrapper around the single-pass accumulator for callers that
    only need one window size.

    Parameters
    ----------
    csv_path:
        Absolute path to the raw Exathlon CSV file.
    window_s:
        Tumbling window size in seconds.
    window_label:
        Human-readable label included in output (e.g. ``"5min"``).
    chunksize:
        Pandas CSV read chunksize (rows per chunk).

    Returns
    -------
    DataFrame with columns:
        entity, metric_base, window_start_s, window_size_s, window_label,
        p50, p95, p99, tail_ratio_p99_p50, tail_ratio_p95_p50, count
    """
    acc = _accumulate_all_windows(csv_path, [(window_s, window_label)], chunksize)
    return _acc_to_dataframe(acc[window_s], window_s, window_label)


# ---------------------------------------------------------------------------
# Multi-window runner
# ---------------------------------------------------------------------------

def run_q1(
    file_tag: str,
    output_dir: Path,
    chunksize: int = 200,
    windows: list[tuple[int, str]] | None = None,
) -> Path:
    """Compute Q1 ground truth for all window sizes in a single CSV pass.

    One CSV is written per window size:
        <output_dir>/Q1/<safe_tag>_<window_label>.csv

    A combined CSV containing all window sizes is written to:
        <output_dir>/Q1/<safe_tag>.csv

    Parameters
    ----------
    file_tag:
        Exathlon file tag, e.g. ``"app1/1_0_10000_17"``.
    output_dir:
        Root results directory (CSVs land under ``output_dir/Q1/``).
    chunksize:
        Rows per pandas read_csv chunk (default 200).
    windows:
        List of ``(window_size_s, label)`` pairs to compute.
        Defaults to :data:`ALL_WINDOWS` (1 min, 5 min, 15 min, 30 min, 1 hr).

    Returns
    -------
    Path to the combined output CSV.
    """
    if windows is None:
        windows = ALL_WINDOWS

    csv_path = file_csv_path(file_tag)
    if not csv_path.is_file():
        raise FileNotFoundError(f"Exathlon CSV not found: {csv_path}")

    tag = file_tag_safe(file_tag)
    out_dir = output_dir / "Q1"
    out_dir.mkdir(parents=True, exist_ok=True)

    log_phase("Q1", tag, "start",
              file=str(csv_path),
              windows=[lbl for _, lbl in windows],
              chunksize=chunksize)

    # Single streaming pass — accumulate all window sizes simultaneously.
    t0 = time.perf_counter()
    log_phase("Q1", tag, "streaming (single pass for all windows)")
    all_acc = _accumulate_all_windows(csv_path, windows, chunksize)
    stream_elapsed = time.perf_counter() - t0
    log_phase("Q1", tag, "streaming done", elapsed_s=f"{stream_elapsed:.1f}")

    all_parts: list[pd.DataFrame] = []

    for window_s, window_label in windows:
        df = _acc_to_dataframe(all_acc[window_s], window_s, window_label)

        per_window_path = out_dir / f"{tag}_{window_label}.csv"
        df.to_csv(per_window_path, index=False)

        log_phase("Q1", tag, f"window={window_label} done",
                  rows=len(df),
                  series=df[["entity", "metric_base"]].drop_duplicates().shape[0],
                  windows_n=df["window_start_s"].nunique(),
                  output=str(per_window_path))

        all_parts.append(df)

    combined = pd.concat(all_parts, ignore_index=True)
    combined_path = out_dir / f"{tag}.csv"
    combined.to_csv(combined_path, index=False)

    log_phase("Q1", tag, "done",
              total_rows=len(combined),
              output=str(combined_path))

    return combined_path


# ---------------------------------------------------------------------------
# Summary helpers (useful for inspection / debugging)
# ---------------------------------------------------------------------------

def summarise_q1(df: pd.DataFrame) -> pd.DataFrame:
    """Return a compact per-(window_size, entity) summary of the Q1 output.

    Computes across all metric_bases and windows:
        - median p50 / p95 / p99
        - median tail ratio p99/p50
        - total series and windows counted
    """
    return (
        df.groupby(["window_label", "window_size_s", "entity"])
        .agg(
            series_count=("metric_base", "nunique"),
            window_count=("window_start_s", "nunique"),
            median_p50=("p50", "median"),
            median_p95=("p95", "median"),
            median_p99=("p99", "median"),
            median_tail_99_50=("tail_ratio_p99_p50", "median"),
            median_samples_per_window=("count", "median"),
        )
        .reset_index()
        .sort_values(["window_size_s", "entity"])
        .reset_index(drop=True)
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Q1 ground truth: exact p50/p95/p99 per (entity, metric_base, window) "
            "for all window sizes (1/5/15/30/60 min)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples
--------
# Compute Q1 for a single file (all window sizes):
  python3 ground_truth/q1.py --file app1/1_0_10000_17

# Primary window only (5 min):
  python3 ground_truth/q1.py --file app1/1_0_10000_17 --windows 5min

# Multiple specific windows:
  python3 ground_truth/q1.py --file app1/1_0_10000_17 --windows 1min 5min 15min

# Custom output directory:
  python3 ground_truth/q1.py --file app9/9_0_100000_1 --out-dir /tmp/gt
""",
    )
    parser.add_argument(
        "--file",
        default="app1/1_0_10000_17",
        help="File tag to process (e.g. app1/1_0_10000_17).",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=_BENCHMARK_ROOT / "results" / "ground_truth",
        help="Root output directory.  Q1 CSVs land under <out-dir>/Q1/.",
    )
    parser.add_argument(
        "--windows",
        nargs="+",
        choices=["1min", "5min", "15min", "30min", "1hour"],
        default=None,
        help=(
            "Window sizes to compute.  "
            "Defaults to all: 1min 5min 15min 30min 1hour."
        ),
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=200,
        help="Pandas CSV read chunksize (rows per chunk, default 200).",
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Print a compact per-(window, entity) summary after computation.",
    )
    args = parser.parse_args()

    label_to_seconds: dict[str, int] = {
        "1min":  WINDOW_1MIN_S,
        "5min":  WINDOW_5MIN_S,
        "15min": WINDOW_15MIN_S,
        "30min": WINDOW_30MIN_S,
        "1hour": WINDOW_1HR_S,
    }

    if args.windows:
        selected_windows = [(label_to_seconds[lbl], lbl) for lbl in args.windows]
    else:
        selected_windows = None  # run_q1 defaults to ALL_WINDOWS

    combined_path = run_q1(
        file_tag=args.file,
        output_dir=args.out_dir,
        chunksize=args.chunksize,
        windows=selected_windows,
    )

    if args.summary:
        df = pd.read_csv(combined_path)
        summary = summarise_q1(df)
        print("\n--- Q1 summary ---")
        print(summary.to_string(index=False))
        print()


if __name__ == "__main__":
    main()
