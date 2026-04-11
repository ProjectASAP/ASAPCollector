from __future__ import annotations

"""Q9 — Saturation ratio per entity (USE method).

Purpose
-------
Measure fraction of metrics crossing saturation threshold.

Resource saturation alerting under the USE method — the saturation ratio
(exceeded_metrics / total_metrics per entity) directly implements the "S"
of the USE (Utilization, Saturation, Errors) framework for Spark executor
and HPC node health.

Formula
-------
For each (entity, window w):
    sat_ratio(e, w) = exceeded_metrics(e, w) / total_metrics(e, w)

where:
    total_metrics(e, w)    = |{metric_base : any value observed in w for entity e}|
    exceeded_metrics(e, w) = |{metric_base : any value in w for entity e
                               exceeds the file-wide p95 threshold for (e, metric_base)}|

Threshold per (entity, metric_base) is the 95th-percentile of all non-sentinel
values for that group across the entire file (``THRESHOLD_QUANTILE`` in common.py).

This matches the SQL template in ``docs/02_benchmark_queries.md``:

    SELECT
      entity,
      COUNT(*) AS exceeded_metrics
    FROM metric_exceeded_events
    GROUP BY entity, TUMBLE(ts, INTERVAL '5' MINUTE)

Window sizes evaluated
----------------------
Primary : 5 min  (standard tumbling window for Spark job monitoring)
Extended: 15 min (coarser saturation signal, aligned with the Q5 IQR window)

Each window size produces a separate output CSV under:
    <out_dir>/Q9/<safe_file_tag>_<window_label>.csv

A combined CSV with all window sizes is also written:
    <out_dir>/Q9/<safe_file_tag>.csv

Output columns
--------------
entity, window_start_s, window_size_s, window_label,
saturated_metric_count, total_active_metric_count, exact_saturation_ratio

References
----------
- Jacob et al. — Exathlon: A Benchmark for Explainable Anomaly Detection
  over Time Series, VLDB 2021 (https://doi.org/10.14778/3476249.3476307)
  Threshold-crossing events in Spark executor metrics (CPU, memory, GC)
  are directly observable in the corpus.
- Gregg — The USE Method (https://www.brendangregg.com/usemethod.html)
  Saturation as a first-class resource health signal; motivates per-entity
  saturation ratio as the "S" component of the USE framework.
- Flajolet et al. — HyperLogLog: the analysis of a near-optimal cardinality
  estimation algorithm, DMTCS 2007
  (https://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf)
  HLL used for distinct metric counting in sketch-based evaluation.
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
    THRESHOLD_QUANTILE,
    WINDOW_15MIN_S,
    WINDOW_5MIN_S,
    _stream_long_chunks,
    compute_per_metric_thresholds,
    log_phase,
)

# ---------------------------------------------------------------------------
# Window configuration
# ---------------------------------------------------------------------------

PRIMARY_WINDOW_S = WINDOW_5MIN_S
PRIMARY_WINDOW_LABEL = "5min"

ALL_WINDOWS: list[tuple[int, str]] = [
    (WINDOW_5MIN_S, "5min"),
    (WINDOW_15MIN_S, "15min"),
]

_Q9_COLS = [
    "entity",
    "window_start_s",
    "window_size_s",
    "window_label",
    "saturated_metric_count",
    "total_active_metric_count",
    "exact_saturation_ratio",
]


# ---------------------------------------------------------------------------
# Core computation
# ---------------------------------------------------------------------------

def _accumulate_saturation(
    csv_path: Path,
    windows: list[tuple[int, str]],
    thresholds: dict[tuple, float],
    chunksize: int,
) -> dict[int, tuple[dict[tuple, set], dict[tuple, set]]]:
    """Stream the CSV once and accumulate active/saturated metric sets per window size.

    For each window size, tracks:
    - ``active_dict``    : (entity, window_start_s) → set of observed metric_bases
    - ``saturated_dict`` : (entity, window_start_s) → set of metric_bases with any
                           value exceeding the file-wide p95 threshold

    Returns
    -------
    ``{window_s: (active_dict, saturated_dict)}``
    """
    window_sizes = [ws for ws, _ in windows]
    result: dict[int, tuple[dict, dict]] = {
        ws: (defaultdict(set), defaultdict(set)) for ws in window_sizes
    }

    for chunk in _stream_long_chunks(csv_path, chunksize):
        if chunk.empty:
            continue

        ts = chunk["ts_s"].to_numpy(dtype=np.int64)
        entities = chunk["entity"].astype(str).to_numpy()
        metric_bases = chunk["metric_base"].astype(str).to_numpy()
        values = chunk["value"].to_numpy(dtype=np.float64)

        for ws in window_sizes:
            active_dict, saturated_dict = result[ws]
            window_starts = (ts // ws) * ws

            for i in range(len(values)):
                ew = (entities[i], int(window_starts[i]))
                mb = metric_bases[i]
                active_dict[ew].add(mb)
                thr = thresholds.get((entities[i], mb))
                if thr is not None and values[i] > thr:
                    saturated_dict[ew].add(mb)

    return result


def _build_dataframe(
    active_dict: dict[tuple, set],
    saturated_dict: dict[tuple, set],
    window_s: int,
    window_label: str,
) -> pd.DataFrame:
    """Convert active/saturated metric sets to the Q9 output DataFrame."""
    all_ew = set(active_dict.keys()) | set(saturated_dict.keys())
    if not all_ew:
        return pd.DataFrame(columns=_Q9_COLS)

    rows = []
    for (entity, ws) in sorted(all_ew):
        total = len(active_dict.get((entity, ws), set()))
        exceeded = len(saturated_dict.get((entity, ws), set()))
        rows.append({
            "entity":                   entity,
            "window_start_s":           ws,
            "window_size_s":            window_s,
            "window_label":             window_label,
            "saturated_metric_count":   exceeded,
            "total_active_metric_count": total,
            "exact_saturation_ratio":   exceeded / total if total > 0 else 0.0,
        })

    df = pd.DataFrame(rows, columns=_Q9_COLS)
    df.sort_values(["entity", "window_start_s"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def _build_cumulative_rows(
    all_acc: dict[int, tuple[dict[tuple, set], dict[tuple, set]]],
) -> pd.DataFrame:
    """Compute cumulative distinct saturated/active metric counts per entity.

    Takes the union of per-window sets across ALL window sizes to produce one
    ``window_label="cumulative"`` row per entity.  This is what the sketch HLL
    actually tracks: distinct metric_base names that crossed the threshold at
    any point during the replay, regardless of window boundaries.

    These cumulative rows are consumed by ``_compare_q9`` in compare.py to
    align the comparison against the cumulative (non-windowed) HLL estimate.
    """
    entity_active: dict[str, set] = defaultdict(set)
    entity_saturated: dict[str, set] = defaultdict(set)

    for _ws, (active_dict, saturated_dict) in all_acc.items():
        for (entity, _window_start), mb_set in active_dict.items():
            entity_active[entity].update(mb_set)
        for (entity, _window_start), mb_set in saturated_dict.items():
            entity_saturated[entity].update(mb_set)

    all_entities = sorted(set(entity_active.keys()) | set(entity_saturated.keys()))
    rows = []
    for entity in all_entities:
        total = len(entity_active.get(entity, set()))
        exceeded = len(entity_saturated.get(entity, set()))
        rows.append({
            "entity":                   entity,
            "window_start_s":           0,
            "window_size_s":            -1,
            "window_label":             "cumulative",
            "saturated_metric_count":   exceeded,
            "total_active_metric_count": total,
            "exact_saturation_ratio":   exceeded / total if total > 0 else 0.0,
        })

    if not rows:
        return pd.DataFrame(columns=_Q9_COLS)
    return pd.DataFrame(rows, columns=_Q9_COLS)


def compute_q9_single_window(
    csv_path: Path,
    window_s: int,
    window_label: str,
    chunksize: int,
) -> pd.DataFrame:
    """Return exact saturation ratio per (entity, window) for one window size.

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
    DataFrame with columns matching :data:`_Q9_COLS`.
    """
    thresholds = compute_per_metric_thresholds(csv_path, THRESHOLD_QUANTILE, chunksize)
    result = _accumulate_saturation(csv_path, [(window_s, window_label)], thresholds, chunksize)
    active_dict, saturated_dict = result[window_s]
    return _build_dataframe(active_dict, saturated_dict, window_s, window_label)


# ---------------------------------------------------------------------------
# Multi-window runner
# ---------------------------------------------------------------------------

def run_q9(
    file_tag: str,
    output_dir: Path,
    chunksize: int = 200,
    windows: list[tuple[int, str]] | None = None,
) -> Path:
    """Compute Q9 ground truth for one or more window sizes.

    The computation is two-pass:
    1. A threshold pass reads the CSV to compute the file-wide p95 per
       (entity, metric_base) group.
    2. A saturation pass streams the CSV again, accumulating active and
       saturated metric sets for all requested window sizes simultaneously.

    One CSV is written per window size:
        <output_dir>/Q9/<safe_tag>_<window_label>.csv

    A combined CSV containing all window sizes is written to:
        <output_dir>/Q9/<safe_tag>.csv

    Parameters
    ----------
    file_tag:
        Exathlon file tag, e.g. ``"app1/1_0_10000_17"``.
    output_dir:
        Root results directory (CSVs land under ``output_dir/Q9/``).
    chunksize:
        Rows per pandas read_csv chunk (default 200).
    windows:
        List of ``(window_size_s, label)`` pairs to compute.
        Defaults to :data:`ALL_WINDOWS` (5 min primary + 15 min extended).

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
    out_dir = output_dir / "Q9"
    out_dir.mkdir(parents=True, exist_ok=True)

    log_phase(
        "Q9",
        tag,
        "start",
        file=str(csv_path),
        windows=[lbl for _, lbl in windows],
        chunksize=chunksize,
    )

    # Pass 1: compute file-wide p95 thresholds per (entity, metric_base).
    t0 = time.perf_counter()
    log_phase("Q9", tag, "computing thresholds (pass 1)")
    thresholds = compute_per_metric_thresholds(csv_path, THRESHOLD_QUANTILE, chunksize)
    log_phase(
        "Q9", tag, "thresholds done",
        n_metrics=len(thresholds),
        elapsed_s=f"{time.perf_counter() - t0:.1f}",
    )

    # Pass 2: stream CSV and accumulate active/saturated sets for all window sizes.
    t1 = time.perf_counter()
    log_phase("Q9", tag, "streaming saturation counts (pass 2)")
    all_acc = _accumulate_saturation(csv_path, windows, thresholds, chunksize)
    log_phase("Q9", tag, "streaming done", elapsed_s=f"{time.perf_counter() - t1:.1f}")

    all_parts: list[pd.DataFrame] = []

    for window_s, window_label in windows:
        t2 = time.perf_counter()
        active_dict, saturated_dict = all_acc[window_s]
        df = _build_dataframe(active_dict, saturated_dict, window_s, window_label)

        per_window_path = out_dir / f"{tag}_{window_label}.csv"
        df.to_csv(per_window_path, index=False)

        log_phase(
            "Q9",
            tag,
            f"window={window_label} done",
            rows=len(df),
            entities_n=df["entity"].nunique() if not df.empty else 0,
            windows_n=df["window_start_s"].nunique() if not df.empty else 0,
            median_sat_ratio=(
                float(df["exact_saturation_ratio"].median()) if not df.empty else "n/a"
            ),
            elapsed_s=f"{time.perf_counter() - t2:.1f}",
            output=str(per_window_path),
        )
        all_parts.append(df)

    cumulative_df = _build_cumulative_rows(all_acc)
    log_phase("Q9", tag, "cumulative rows", entities_n=len(cumulative_df))

    combined = (
        pd.concat(all_parts + [cumulative_df], ignore_index=True)
        if all_parts
        else cumulative_df
    )
    combined_path = out_dir / f"{tag}.csv"
    combined.to_csv(combined_path, index=False)

    log_phase("Q9", tag, "done", total_rows=len(combined), output=str(combined_path))
    return combined_path


# ---------------------------------------------------------------------------
# Summary helpers
# ---------------------------------------------------------------------------

def summarise_q9(df: pd.DataFrame) -> pd.DataFrame:
    """Return a compact per-(window_label, entity) summary of Q9 output.

    Aggregates across all windows for each entity:
        - mean, median, and max saturation ratio
        - mean saturated and total active metric counts
        - number of windows observed
    """
    if df.empty:
        return pd.DataFrame()

    summary = (
        df.groupby(["window_label", "window_size_s", "entity"])
        .agg(
            windows_n=("window_start_s", "nunique"),
            mean_sat_ratio=("exact_saturation_ratio", "mean"),
            median_sat_ratio=("exact_saturation_ratio", "median"),
            max_sat_ratio=("exact_saturation_ratio", "max"),
            mean_saturated_count=("saturated_metric_count", "mean"),
            mean_total_count=("total_active_metric_count", "mean"),
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
            raise ValueError(f"unsupported Q9 window label {label!r}")
        windows.append((label_to_window[label], label))
    return windows


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Q9 ground truth: exact saturation ratio "
            "(exceeded_metrics / total_active_metrics) per entity and window. "
            "Threshold per (entity, metric_base) = file-wide p95."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples
--------
# Primary window (5 min) for one file:
  python3 ground_truth/q9.py --file app1/1_0_10000_17

# Both windows (5 min + 15 min):
  python3 ground_truth/q9.py --file app1/1_0_10000_17 --windows 5min 15min

# Write summary after computation:
  python3 ground_truth/q9.py --file app1/1_0_10000_17 --summary
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
        help="Root output directory.  Q9 CSVs land under <out-dir>/Q9/.",
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
    combined_path = run_q9(
        file_tag=args.file,
        output_dir=args.out_dir,
        chunksize=args.chunksize,
        windows=selected_windows,
    )

    if args.summary:
        df = pd.read_csv(combined_path)
        summary = summarise_q9(df)
        print("\n--- Q9 summary ---")
        print(summary.to_string(index=False))
        print()

    print(combined_path)


if __name__ == "__main__":
    main()
