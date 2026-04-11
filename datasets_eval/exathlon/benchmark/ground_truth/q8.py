from __future__ import annotations

"""Q8 — Quantile drift between adjacent windows per (entity, metric_base).

Purpose
-------
Detect distribution shift over time.

Streaming concept drift detection in cluster telemetry — sudden shifts in p95
between consecutive windows signal phase transitions in Spark jobs (e.g.,
map → shuffle → reduce) or onset of resource pressure, enabling early warning
before thresholds are breached.

Formula
-------
For each (entity, metric_base) and each pair of consecutive windows (t-1, t):
    drift_p95(t) = |Q_0.95(t) - Q_0.95(t-1)|
    drift_p50(t) = |Q_0.50(t) - Q_0.50(t-1)|

This matches the PromQL template in ``docs/02_benchmark_queries.md``:

    quantile_over_time(0.95, exathlon_metric{entity!="",metric_base!=""}[5m])
      by (entity, metric_base)
    -
    quantile_over_time(0.95, exathlon_metric{entity!="",metric_base!=""}[5m]
      offset 5m) by (entity, metric_base)

Window sizes evaluated
----------------------
Primary : 5 min  (phase-transition granularity typical for Spark job stages)
Stress  : 1 min  (fine-grained drift detection under high ingestion rate)

Each window size produces a separate output CSV under:
    <out_dir>/Q8/<safe_file_tag>_<window_label>.csv

A combined CSV with all window sizes is also written:
    <out_dir>/Q8/<safe_file_tag>.csv

Output columns
--------------
entity, metric_base, window_start_s, window_size_s, window_label,
prev_window_start_s, prev_p50, curr_p50, drift_p50,
prev_p95, curr_p95, drift_p95, n_curr, n_prev

References
----------
- Jacob et al. — Exathlon: A Benchmark for Explainable Anomaly Detection
  over Time Series, VLDB 2021 (https://doi.org/10.14778/3476249.3476307)
  Fault injection experiments (resource contention, process failures) produce
  measurable distribution shifts across consecutive windows.
- Karnin, Lang, Liberty — Optimal Quantile Approximation in Streams,
  FOCS 2016 (https://arxiv.org/abs/1603.05346)
  Mergeable sketch structure enables efficient consecutive-window comparison.
- Gama et al. — A Survey on Concept Drift Adaptation, ACM CSUR 2014
  (https://doi.org/10.1145/2523813)
  Motivates window-to-window quantile comparison for streaming drift detection.
"""

import argparse
import sys
import time
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

_BENCHMARK_ROOT = Path(__file__).resolve().parent.parent
if str(_BENCHMARK_ROOT) not in sys.path:
    sys.path.insert(0, str(_BENCHMARK_ROOT))

from common import file_csv_path, file_tag_safe
from ground_truth.common import (
    WINDOW_5MIN_S,
    _stream_long_chunks,
    log_phase,
)

# ---------------------------------------------------------------------------
# Window configuration
# ---------------------------------------------------------------------------

PRIMARY_WINDOW_S = WINDOW_5MIN_S
PRIMARY_WINDOW_LABEL = "5min"

ALL_WINDOWS: list[tuple[int, str]] = [
    (WINDOW_5MIN_S, "5min"),
]

# Minimum samples in a window to compute a meaningful quantile.
MIN_WINDOW_SAMPLES = 2

_Q8_COLS = [
    "entity",
    "metric_base",
    "window_start_s",
    "window_size_s",
    "window_label",
    "prev_window_start_s",
    "prev_p50",
    "curr_p50",
    "drift_p50",
    "prev_p90",
    "curr_p90",
    "drift_p90",
    "prev_p95",
    "curr_p95",
    "drift_p95",
    "n_curr",
    "n_prev",
]


# ---------------------------------------------------------------------------
# Core computation — single-pass multi-window accumulation
# ---------------------------------------------------------------------------

def _accumulate_all_windows(
    csv_path: Path,
    windows: list[tuple[int, str]],
    chunksize: int,
) -> dict[int, dict[tuple, list[float]]]:
    """Stream the CSV once and accumulate values for every requested window size.

    Returns
    -------
    ``{window_s: {(entity, metric_base, window_start_s): [float, ...]}}``
    """
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
    acc: dict[tuple, list[float]],
    window_s: int,
    window_label: str,
) -> pd.DataFrame:
    """Convert one window-size accumulator to the Q8 drift output DataFrame.

    For each (entity, metric_base), sort the windows chronologically and emit
    one row per consecutive (prev, curr) pair, computing:
        drift_p50 = |curr_p50 - prev_p50|
        drift_p90 = |curr_p90 - prev_p90|  (fallback reference for compare)
        drift_p95 = |curr_p95 - prev_p95|
    """
    if not acc:
        return pd.DataFrame(columns=_Q8_COLS)

    # Build per-series window map: (entity, mb) → {window_start_s: (p50, p90, p95, n)}.
    series_windows: dict[tuple, dict[int, tuple[float, float, float, int]]] = defaultdict(dict)

    for (entity, mb, ws), values in acc.items():
        arr = np.asarray(values, dtype=np.float64)
        if len(arr) < MIN_WINDOW_SAMPLES:
            continue
        series_windows[(entity, mb)][ws] = (
            float(np.percentile(arr, 50)),
            float(np.percentile(arr, 90)),
            float(np.percentile(arr, 95)),
            int(len(arr)),
        )

    rows = []
    for (entity, mb), window_dict in series_windows.items():
        sorted_windows = sorted(window_dict.keys())
        for i in range(1, len(sorted_windows)):
            prev_ws = sorted_windows[i - 1]
            curr_ws = sorted_windows[i]
            prev_p50, prev_p90, prev_p95, n_prev = window_dict[prev_ws]
            curr_p50, curr_p90, curr_p95, n_curr = window_dict[curr_ws]
            rows.append({
                "entity":              entity,
                "metric_base":         mb,
                "window_start_s":      curr_ws,
                "window_size_s":       window_s,
                "window_label":        window_label,
                "prev_window_start_s": prev_ws,
                "prev_p50":            prev_p50,
                "curr_p50":            curr_p50,
                "drift_p50":           abs(curr_p50 - prev_p50),
                "prev_p90":            prev_p90,
                "curr_p90":            curr_p90,
                "drift_p90":           abs(curr_p90 - prev_p90),
                "prev_p95":            prev_p95,
                "curr_p95":            curr_p95,
                "drift_p95":           abs(curr_p95 - prev_p95),
                "n_curr":              n_curr,
                "n_prev":              n_prev,
            })

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=_Q8_COLS)
    df.sort_values(["entity", "metric_base", "window_start_s"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df[_Q8_COLS]


def compute_q8_single_window(
    csv_path: Path,
    window_s: int,
    window_label: str,
    chunksize: int,
) -> pd.DataFrame:
    """Return exact consecutive-window quantile drift for one window size.

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
    DataFrame with columns matching :data:`_Q8_COLS`.
    """
    acc = _accumulate_all_windows(csv_path, [(window_s, window_label)], chunksize)
    return _acc_to_dataframe(acc[window_s], window_s, window_label)


# ---------------------------------------------------------------------------
# Multi-window runner
# ---------------------------------------------------------------------------

def run_q8(
    file_tag: str,
    output_dir: Path,
    chunksize: int = 200,
    windows: list[tuple[int, str]] | None = None,
) -> Path:
    """Compute Q8 ground truth for one or more window sizes in a single pass.

    One CSV is written per window size:
        <output_dir>/Q8/<safe_tag>_<window_label>.csv

    A combined CSV containing all window sizes is written to:
        <output_dir>/Q8/<safe_tag>.csv

    Parameters
    ----------
    file_tag:
        Exathlon file tag, e.g. ``"app1/1_0_10000_17"``.
    output_dir:
        Root results directory (CSVs land under ``output_dir/Q8/``).
    chunksize:
        Rows per pandas read_csv chunk (default 200).
    windows:
        List of ``(window_size_s, label)`` pairs to compute.
        Defaults to :data:`ALL_WINDOWS` (5 min primary, 1 min stress).

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
    out_dir = output_dir / "Q8"
    out_dir.mkdir(parents=True, exist_ok=True)

    log_phase(
        "Q8",
        tag,
        "start",
        file=str(csv_path),
        windows=[lbl for _, lbl in windows],
        chunksize=chunksize,
    )

    t0 = time.perf_counter()
    log_phase("Q8", tag, "streaming (single pass for all windows)")
    all_acc = _accumulate_all_windows(csv_path, windows, chunksize)
    stream_elapsed = time.perf_counter() - t0
    log_phase("Q8", tag, "streaming done", elapsed_s=f"{stream_elapsed:.1f}")

    all_parts: list[pd.DataFrame] = []

    for window_s, window_label in windows:
        t1 = time.perf_counter()
        df = _acc_to_dataframe(all_acc[window_s], window_s, window_label)

        per_window_path = out_dir / f"{tag}_{window_label}.csv"
        df.to_csv(per_window_path, index=False)

        elapsed = time.perf_counter() - t1
        log_phase(
            "Q8",
            tag,
            f"window={window_label} done",
            rows=len(df),
            series_n=df[["entity", "metric_base"]].drop_duplicates().shape[0] if not df.empty else 0,
            windows_n=df["window_start_s"].nunique() if not df.empty else 0,
            median_drift_p95=(float(df["drift_p95"].median()) if not df.empty else "n/a"),
            elapsed_s=f"{elapsed:.1f}",
            output=str(per_window_path),
        )
        all_parts.append(df)

    combined = (
        pd.concat(all_parts, ignore_index=True)
        if all_parts
        else pd.DataFrame(columns=_Q8_COLS)
    )
    combined_path = out_dir / f"{tag}.csv"
    combined.to_csv(combined_path, index=False)

    log_phase("Q8", tag, "done", total_rows=len(combined), output=str(combined_path))
    return combined_path


# ---------------------------------------------------------------------------
# Summary helpers
# ---------------------------------------------------------------------------

def summarise_q8(df: pd.DataFrame) -> pd.DataFrame:
    """Return a compact per-(window_label, entity) summary of Q8 output.

    Aggregates across all metric_bases:
        - median and p95 of drift_p50 and drift_p95
        - count of (series, windows) included
    """
    if df.empty:
        return pd.DataFrame()

    summary = (
        df.groupby(["window_label", "window_size_s", "entity"])
        .agg(
            series_n=("metric_base", "nunique"),
            windows_n=("window_start_s", "nunique"),
            median_drift_p50=("drift_p50", "median"),
            p95_drift_p50=("drift_p50", lambda s: float(np.percentile(s, 95))),
            median_drift_p95=("drift_p95", "median"),
            p95_drift_p95=("drift_p95", lambda s: float(np.percentile(s, 95))),
        )
        .reset_index()
    )
    summary.sort_values(["window_size_s", "entity"], inplace=True)
    summary.reset_index(drop=True, inplace=True)
    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_window_args(window_labels: list[str] | None) -> list[tuple[int, str]]:
    if not window_labels:
        return [(PRIMARY_WINDOW_S, PRIMARY_WINDOW_LABEL)]
    label_to_window = {lbl: ws for ws, lbl in ALL_WINDOWS}
    windows = []
    for label in window_labels:
        if label not in label_to_window:
            raise ValueError(f"unsupported Q8 window label {label!r}")
        windows.append((label_to_window[label], label))
    return windows


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Q8 ground truth: exact consecutive-window quantile drift "
            "|p95_t - p95_{t-1}| and |p50_t - p50_{t-1}| per "
            "(entity, metric_base) for 5-min (primary) and 1-min (stress) windows."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples
--------
# Primary window (5 min) for one file:
  python3 ground_truth/q8.py --file app1/1_0_10000_17

# Both windows:
  python3 ground_truth/q8.py --file app1/1_0_10000_17 --windows 5min 1min

# Write summary after computation:
  python3 ground_truth/q8.py --file app1/1_0_10000_17 --summary
""",
    )
    parser.add_argument(
        "--file",
        required=True,
        help="Exathlon file tag, e.g. app1/1_0_10000_17.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=_BENCHMARK_ROOT / "results" / "ground_truth",
        help="Root output directory.  Q8 CSVs land under <out-dir>/Q8/.",
    )
    parser.add_argument(
        "--windows",
        nargs="+",
        choices=[lbl for _, lbl in ALL_WINDOWS],
        default=None,
        help="Window sizes to compute.  Defaults to primary (5min).",
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

    selected_windows = _parse_window_args(args.windows)
    combined_path = run_q8(
        file_tag=args.file,
        output_dir=args.out_dir,
        chunksize=args.chunksize,
        windows=selected_windows,
    )

    if args.summary:
        df = pd.read_csv(combined_path)
        summary = summarise_q8(df)
        print("\n--- Q8 summary ---")
        print(summary.to_string(index=False))
        print()

    print(combined_path)


if __name__ == "__main__":
    main()
