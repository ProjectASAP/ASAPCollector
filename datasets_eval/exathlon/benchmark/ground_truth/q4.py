from __future__ import annotations

"""Q4 — Windowed min / max / range per (entity, metric_base).

Purpose
-------
Characterise the operational envelope of utilisation metrics such as CPU busy,
memory usage, and related host / executor telemetry.

Capacity planning and resource utilisation profiling use per-window minima,
maxima, and ranges to expose floor behaviour, saturation ceilings, and burst
amplitude across Spark executors and HPC nodes.

Formula
-------
For each (entity, metric_base, window):
    exact_min   = min(x_t)
    exact_max   = max(x_t)
    exact_range = exact_max - exact_min

The sketch counterpart approximates these with extreme quantiles:
    p0   ~= min
    p100 ~= max

Window sizes evaluated
----------------------
Primary  : 5 min
Sensitivity: 1 min, 15 min, 30 min, 1 hour

Each window size produces a separate output CSV under:
    <out_dir>/Q4/<safe_file_tag>_<window_label>.csv

A combined CSV with all window sizes is also written:
    <out_dir>/Q4/<safe_file_tag>.csv

Output columns
--------------
entity, metric_base, window_start_s, window_size_s, window_label,
exact_min, exact_max, exact_range, count

References
----------
- Jacob et al. — Exathlon: A Benchmark for Explainable Anomaly Detection
  over Time Series, VLDB 2021 (https://doi.org/10.14778/3476249.3476307)
- Gregg — Systems Performance: Enterprise and the Cloud, 2nd ed., Pearson 2020
  (https://www.brendangregg.com/systems-performance-2nd-edition-book.html)
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

ALL_WINDOWS: list[tuple[int, str]] = [
    (WINDOW_1MIN_S, "1min"),
    (WINDOW_5MIN_S, "5min"),
    (WINDOW_15MIN_S, "15min"),
    (WINDOW_30MIN_S, "30min"),
    (WINDOW_1HR_S, "1hour"),
]

PRIMARY_WINDOW_S = WINDOW_5MIN_S


# ---------------------------------------------------------------------------
# Core computation — single-pass multi-window accumulation
# ---------------------------------------------------------------------------

def _accumulate_all_windows(
    csv_path: Path,
    windows: list[tuple[int, str]],
    chunksize: int,
) -> dict[int, dict[tuple, list]]:
    """Stream the CSV once and accumulate values for every requested window."""
    from collections import defaultdict

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
    """Convert one window accumulator to the canonical Q4 output shape."""
    cols = [
        "entity",
        "metric_base",
        "window_start_s",
        "window_size_s",
        "window_label",
        "exact_min",
        "exact_max",
        "exact_range",
        "count",
    ]
    if not acc:
        return pd.DataFrame(columns=cols)

    rows = []
    for (entity, metric_base, window_start_s), values in acc.items():
        arr = np.asarray(values, dtype=np.float64)
        exact_min = float(arr.min())
        exact_max = float(arr.max())
        rows.append({
            "entity": entity,
            "metric_base": metric_base,
            "window_start_s": window_start_s,
            "window_size_s": window_s,
            "window_label": window_label,
            "exact_min": exact_min,
            "exact_max": exact_max,
            "exact_range": exact_max - exact_min,
            "count": len(arr),
        })

    df = pd.DataFrame(rows)
    df.sort_values(["entity", "metric_base", "window_start_s"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df[cols]


def compute_q4_single_window(
    csv_path: Path,
    window_s: int,
    window_label: str,
    chunksize: int,
) -> pd.DataFrame:
    """Return exact Q4 values for one window size."""
    acc = _accumulate_all_windows(csv_path, [(window_s, window_label)], chunksize)
    return _acc_to_dataframe(acc[window_s], window_s, window_label)


# ---------------------------------------------------------------------------
# Multi-window runner
# ---------------------------------------------------------------------------

def run_q4(
    file_tag: str,
    output_dir: Path,
    chunksize: int = 200,
    windows: list[tuple[int, str]] | None = None,
) -> Path:
    """Compute Q4 ground truth for one or more window sizes in a single pass."""
    if windows is None:
        windows = ALL_WINDOWS

    csv_path = file_csv_path(file_tag)
    if not csv_path.is_file():
        raise FileNotFoundError(f"Exathlon CSV not found: {csv_path}")

    tag = file_tag_safe(file_tag)
    out_dir = output_dir / "Q4"
    out_dir.mkdir(parents=True, exist_ok=True)

    log_phase(
        "Q4",
        tag,
        "start",
        file=str(csv_path),
        windows=[label for _, label in windows],
        chunksize=chunksize,
    )

    t0 = time.perf_counter()
    log_phase("Q4", tag, "streaming (single pass for all windows)")
    acc_by_window = _accumulate_all_windows(csv_path, windows, chunksize)
    stream_elapsed = time.perf_counter() - t0
    log_phase("Q4", tag, "streaming done", elapsed_s=f"{stream_elapsed:.1f}")

    all_parts: list[pd.DataFrame] = []

    for window_s, window_label in windows:
        t1 = time.perf_counter()
        df = _acc_to_dataframe(acc_by_window[window_s], window_s, window_label)
        per_window_path = out_dir / f"{tag}_{window_label}.csv"
        df.to_csv(per_window_path, index=False)

        elapsed = time.perf_counter() - t1
        log_phase(
            "Q4",
            tag,
            f"window={window_label} done",
            rows=len(df),
            windows_n=df["window_start_s"].nunique() if not df.empty else 0,
            elapsed_s=f"{elapsed:.1f}",
            output=str(per_window_path),
        )
        all_parts.append(df)

    if len(all_parts) == 1:
        combined_path = out_dir / f"{tag}_{windows[0][1]}.csv"
        log_phase("Q4", tag, "done (single window)", output=str(combined_path))
        return combined_path

    combined = pd.concat(all_parts, ignore_index=True)
    combined_path = out_dir / f"{tag}.csv"
    combined.to_csv(combined_path, index=False)

    log_phase("Q4", tag, "done", total_rows=len(combined), output=str(combined_path))
    return combined_path


# ---------------------------------------------------------------------------
# Summary helpers
# ---------------------------------------------------------------------------

def summarise_q4(df: pd.DataFrame) -> pd.DataFrame:
    """Return a compact per-(window_label, entity) summary of the Q4 output."""
    if df.empty:
        return pd.DataFrame()

    summary = (
        df.groupby(["window_label", "window_size_s", "entity"])
        .agg(
            windows_n=("window_start_s", "nunique"),
            median_min=("exact_min", "median"),
            median_max=("exact_max", "median"),
            median_range=("exact_range", "median"),
            mean_range=("exact_range", "mean"),
        )
        .reset_index()
    )
    summary.sort_values(["window_size_s", "entity"], inplace=True)
    summary.reset_index(drop=True, inplace=True)
    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

_LABEL_TO_S: dict[str, int] = {label: seconds for seconds, label in ALL_WINDOWS}


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Q4 ground truth: exact min / max / range per "
            "(entity, metric_base, window)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples
--------
# Compute Q4 for a single file across all standard windows:
  python3 ground_truth/q4.py --file app1/1_0_10000_17

# Restrict to 5-minute and 1-hour windows:
  python3 ground_truth/q4.py --file app1/1_0_10000_17 --windows 5min 1hour

# Custom output directory:
  python3 ground_truth/q4.py --file app9/9_0_100000_1 --out-dir /tmp/gt

# Print a summary table after computation:
  python3 ground_truth/q4.py --file app1/1_0_10000_17 --summary
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
        help="Root output directory. Q4 CSVs land under <out-dir>/Q4/.",
    )
    parser.add_argument(
        "--windows",
        nargs="+",
        choices=list(_LABEL_TO_S),
        default=None,
        help="Window sizes to compute. Defaults to all standard Q4 windows.",
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

    selected_windows = [(_LABEL_TO_S[label], label) for label in args.windows] if args.windows else None

    combined_path = run_q4(
        file_tag=args.file,
        output_dir=args.out_dir,
        chunksize=args.chunksize,
        windows=selected_windows,
    )

    if args.summary:
        df = pd.read_csv(combined_path)
        summary = summarise_q4(df)
        print("\n--- Q4 summary ---")
        print(summary.to_string(index=False))
        print()


if __name__ == "__main__":
    main()
