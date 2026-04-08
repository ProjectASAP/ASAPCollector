from __future__ import annotations

"""Q2 — Tail amplification ratio (p99/p50, p95/p50) per (entity, metric_base, window).

Purpose
-------
Track instability / spikiness using a robust tail ratio derived from the same
quantile data as Q1.

SRE reliability monitoring — the p99/p50 ratio exposes bursty, non-steady-state
behaviour in cluster jobs (e.g. GC pauses, shuffle spills) that mean alone
misses.  This script computes the **exact** tail ratios that a DDSketch or KLL
sketch should approximate within its relative-error bound.

Formula
-------
For each (entity, metric_base, window):
    tail_ratio_p99_p50 = p99 / p50   (primary spikiness indicator)
    tail_ratio_p95_p50 = p95 / p50   (secondary, less extreme)

Both ratios are left as NaN when p50 == 0 to avoid division-by-zero artefacts.

Derivation strategy
-------------------
Q2 is derived from Q1 ground-truth outputs.  When a Q1 CSV is available the
``run_q2`` function loads it and re-extracts the ratio columns, avoiding a
second raw-data pass.  If Q1 output has not been computed yet the function
falls back to calling ``run_q1`` internally.

Window sizes evaluated
----------------------
Same as Q1 (configuration inherited):
    Primary  : 5 min
    Sensitivity: 1 min, 15 min, 30 min, 1 hr

Each window size produces a separate output CSV under:
    <out_dir>/Q2/<safe_file_tag>_<window_label>.csv

A combined CSV with all window sizes is also written:
    <out_dir>/Q2/<safe_file_tag>.csv

Output columns
--------------
entity, metric_base, window_start_s, window_size_s, window_label,
p50, p95, p99, tail_ratio_p99_p50, tail_ratio_p95_p50, count

References
----------
- Jacob et al. — Exathlon: A Benchmark for Explainable Anomaly Detection
  over Time Series, VLDB 2021 (https://doi.org/10.14778/3476249.3476307)
  Spark app telemetry exhibits strong tail amplification under injected faults
  (resource contention, process failures).
- Masson, Rim, Lee — DDSketch: A Fast and Fully-Mergeable Quantile Sketch
  with Relative-Error Guarantees, PVLDB 2019 (https://arxiv.org/abs/1908.10693)
  Relative-error guarantees make DDSketch accurate for tail quantiles.
- Schwartz, Wilkes — Practical Monitoring, O'Reilly 2018, ch. 6
  Tail ratio as a spikiness / instability signal in operational monitoring.
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
    log_phase,
)
from ground_truth.q1 import ALL_WINDOWS, run_q1

# ---------------------------------------------------------------------------
# Window configuration — mirrors Q1
# ---------------------------------------------------------------------------

PRIMARY_WINDOW_S = WINDOW_5MIN_S

_LABEL_TO_S: dict[str, int] = {
    "1min":  WINDOW_1MIN_S,
    "5min":  WINDOW_5MIN_S,
    "15min": WINDOW_15MIN_S,
    "30min": WINDOW_30MIN_S,
    "1hour": WINDOW_1HR_S,
}

# Q2 output columns (subset / re-ordering of Q1 + the two ratio columns).
_Q2_COLS = [
    "entity", "metric_base", "window_start_s", "window_size_s", "window_label",
    "p50", "p95", "p99", "tail_ratio_p99_p50", "tail_ratio_p95_p50", "count",
]


# ---------------------------------------------------------------------------
# Core computation — derived from Q1 output
# ---------------------------------------------------------------------------

def _q1_to_q2(q1_df: pd.DataFrame) -> pd.DataFrame:
    """Extract / recompute Q2 columns from a Q1 ground-truth DataFrame.

    All required columns (p50, p95, p99, tail_ratio_*) are already present in
    Q1 output.  This function selects and reorders them, recalculating the
    ratios in case the Q1 CSV was produced by an older version that did not
    include them.
    """
    df = q1_df.copy()

    # Recalculate ratios defensively — handles older Q1 outputs that may lack them.
    df["tail_ratio_p99_p50"] = np.where(
        df["p50"] != 0.0,
        df["p99"] / df["p50"],
        np.nan,
    )
    df["tail_ratio_p95_p50"] = np.where(
        df["p50"] != 0.0,
        df["p95"] / df["p50"],
        np.nan,
    )

    available = [c for c in _Q2_COLS if c in df.columns]
    df = df[available].copy()
    df.sort_values(["entity", "metric_base", "window_start_s"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def _load_or_compute_q1(
    file_tag: str,
    output_dir: Path,
    chunksize: int,
    windows: list[tuple[int, str]] | None,
) -> pd.DataFrame:
    """Return Q1 combined DataFrame, computing it first if the CSV is absent."""
    tag = file_tag_safe(file_tag)
    q1_combined = output_dir / "Q1" / f"{tag}.csv"

    if not q1_combined.is_file():
        log_phase("Q2", tag, "Q1 output not found — computing Q1 first")
        run_q1(file_tag, output_dir, chunksize=chunksize, windows=windows)

    return pd.read_csv(q1_combined)


# ---------------------------------------------------------------------------
# Multi-window runner
# ---------------------------------------------------------------------------

def run_q2(
    file_tag: str,
    output_dir: Path,
    chunksize: int = 200,
    windows: list[tuple[int, str]] | None = None,
) -> Path:
    """Compute Q2 ground truth (tail ratios) for all window sizes.

    Loads Q1 ground-truth CSVs (computing them if absent), then extracts and
    writes the tail-ratio view.

    One CSV is written per window size:
        <output_dir>/Q2/<safe_tag>_<window_label>.csv

    A combined CSV containing all window sizes is written to:
        <output_dir>/Q2/<safe_tag>.csv

    Parameters
    ----------
    file_tag:
        Exathlon file tag, e.g. ``"app1/1_0_10000_17"``.
    output_dir:
        Root results directory (CSVs land under ``output_dir/Q2/``).
    chunksize:
        Rows per pandas read_csv chunk passed through to Q1 if recomputation
        is needed (default 200).
    windows:
        List of ``(window_size_s, label)`` pairs to compute.
        Defaults to :data:`ALL_WINDOWS` (1 min, 5 min, 15 min, 30 min, 1 hr).

    Returns
    -------
    Path to the combined Q2 output CSV.
    """
    if windows is None:
        windows = ALL_WINDOWS

    csv_path = file_csv_path(file_tag)
    if not csv_path.is_file():
        raise FileNotFoundError(f"Exathlon CSV not found: {csv_path}")

    tag = file_tag_safe(file_tag)
    out_dir = output_dir / "Q2"
    out_dir.mkdir(parents=True, exist_ok=True)

    log_phase("Q2", tag, "start",
              file=str(csv_path),
              windows=[lbl for _, lbl in windows],
              chunksize=chunksize)

    t0 = time.perf_counter()
    q1_df = _load_or_compute_q1(file_tag, output_dir, chunksize, windows)
    load_elapsed = time.perf_counter() - t0
    log_phase("Q2", tag, "Q1 loaded", rows=len(q1_df), elapsed_s=f"{load_elapsed:.1f}")

    requested_labels = {lbl for _, lbl in windows}
    if "window_label" in q1_df.columns:
        q1_df = q1_df[q1_df["window_label"].isin(requested_labels)]

    all_parts: list[pd.DataFrame] = []

    for window_s, window_label in windows:
        subset = q1_df[q1_df["window_label"] == window_label] if "window_label" in q1_df.columns else q1_df
        df = _q1_to_q2(subset)

        per_window_path = out_dir / f"{tag}_{window_label}.csv"
        df.to_csv(per_window_path, index=False)

        log_phase("Q2", tag, f"window={window_label} done",
                  rows=len(df),
                  series=df[["entity", "metric_base"]].drop_duplicates().shape[0],
                  windows_n=df["window_start_s"].nunique() if not df.empty else 0,
                  median_tail_ratio_p99_p50=(
                      float(df["tail_ratio_p99_p50"].median())
                      if not df.empty and "tail_ratio_p99_p50" in df.columns else "n/a"
                  ),
                  output=str(per_window_path))

        all_parts.append(df)

    combined = pd.concat(all_parts, ignore_index=True)
    combined_path = out_dir / f"{tag}.csv"
    combined.to_csv(combined_path, index=False)

    log_phase("Q2", tag, "done",
              total_rows=len(combined),
              output=str(combined_path))

    return combined_path


# ---------------------------------------------------------------------------
# Summary helpers
# ---------------------------------------------------------------------------

def summarise_q2(df: pd.DataFrame) -> pd.DataFrame:
    """Return a compact per-(window_size, entity) summary of the Q2 output.

    Computes across all metric_bases and windows:
        - median / p95 / max of tail_ratio_p99_p50
        - median / p95 / max of tail_ratio_p95_p50
        - fraction of (entity, metric_base, window) triples where
          tail_ratio_p99_p50 > 2 (spiky series indicator)
    """
    df = df.dropna(subset=["tail_ratio_p99_p50", "tail_ratio_p95_p50"])

    result = (
        df.groupby(["window_label", "window_size_s", "entity"])
        .agg(
            series_count=("metric_base", "nunique"),
            window_count=("window_start_s", "nunique"),
            median_tail_99_50=("tail_ratio_p99_p50", "median"),
            p95_tail_99_50=("tail_ratio_p99_p50", lambda s: float(np.percentile(s, 95))),
            max_tail_99_50=("tail_ratio_p99_p50", "max"),
            median_tail_95_50=("tail_ratio_p95_p50", "median"),
            spiky_fraction=("tail_ratio_p99_p50", lambda s: float((s > 2).mean())),
        )
        .reset_index()
        .sort_values(["window_size_s", "entity"])
        .reset_index(drop=True)
    )
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Q2 ground truth: exact tail amplification ratios (p99/p50, p95/p50) "
            "per (entity, metric_base, window) derived from Q1 quantile ground truth."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples
--------
# Compute Q2 for a single file (all window sizes):
  python3 ground_truth/q2.py --file app1/1_0_10000_17

# Primary window only (5 min):
  python3 ground_truth/q2.py --file app1/1_0_10000_17 --windows 5min

# Multiple specific windows:
  python3 ground_truth/q2.py --file app1/1_0_10000_17 --windows 1min 5min 15min

# Custom output directory:
  python3 ground_truth/q2.py --file app9/9_0_100000_1 --out-dir /tmp/gt

# Print a summary table after computation:
  python3 ground_truth/q2.py --file app1/1_0_10000_17 --summary
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
        help="Root output directory.  Q2 CSVs land under <out-dir>/Q2/.",
    )
    parser.add_argument(
        "--windows",
        nargs="+",
        choices=list(_LABEL_TO_S),
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
        help="Pandas CSV read chunksize forwarded to Q1 if recomputation is needed (default 200).",
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Print a compact per-(window, entity) summary after computation.",
    )
    args = parser.parse_args()

    if args.windows:
        selected_windows = [(_LABEL_TO_S[lbl], lbl) for lbl in args.windows]
    else:
        selected_windows = None  # run_q2 defaults to ALL_WINDOWS

    combined_path = run_q2(
        file_tag=args.file,
        output_dir=args.out_dir,
        chunksize=args.chunksize,
        windows=selected_windows,
    )

    if args.summary:
        df = pd.read_csv(combined_path)
        summary = summarise_q2(df)
        print("\n--- Q2 summary ---")
        print(summary.to_string(index=False))
        print()


if __name__ == "__main__":
    main()
