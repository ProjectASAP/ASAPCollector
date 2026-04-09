from __future__ import annotations

"""Q3 — Top-K heavy metrics by threshold exceedance count per window.

Purpose
-------
Identify the most problematic metrics (frequent threshold breaches) per window.

Incident triage in cluster observability — ranking which metrics breach
thresholds most often per window guides operator attention and automated
remediation in large-scale Spark / HPC deployments.

Formula
-------
For each (entity, metric_base, aggregation, window):
    score(m, w) = sum_{t in w} 1[x_t > tau(m)]

where tau(m) is the file-wide p95 of all non-sentinel values for metric m.

Return the TopK items by score within each window.

Approach
--------
Exact computation (ground truth):
1.  Compute per-(entity, metric_base) thresholds as the p95 of all non-sentinel
    values across the entire file.  (``compute_per_metric_thresholds`` helper.)
2.  Stream the file in chunks, count how many values exceed the threshold for
    each (entity, metric_base, aggregation, window_start_s) group.
3.  Within each window, sort by exceedance_count descending and keep the top K.

The sketch counterpart uses Count-Min + SpaceSaving over exceedance events;
this module provides the exact reference that the sketch should approximate.

Validation metrics
------------------
- Top-K set overlap (Jaccard or intersection size / K) between sketch and exact.
- Rank correlation (Spearman / Kendall τ) on the shared top-K set.
- Mean absolute rank difference for entries appearing in both lists.

Window configuration
--------------------
Primary  : 5 min
K        : 10  (configurable via ``--top-k`` / ``k`` parameter)

Each window size produces a separate output CSV under:
    <out_dir>/Q3/<safe_file_tag>_<window_label>.csv

A combined CSV (if multiple windows are requested) is also written:
    <out_dir>/Q3/<safe_file_tag>.csv

Output columns
--------------
entity, metric_base, aggregation, window_start_s, window_size_s,
window_label, exceedance_count, rank

References
----------
- Jacob et al. — Exathlon: A Benchmark for Explainable Anomaly Detection
  over Time Series, VLDB 2021 (https://doi.org/10.14778/3476249.3476307)
  3,629 distinct metric bases per corpus create a realistic heavy-hitter workload.
- Cormode, Muthukrishnan — An Improved Data Stream Summary: The Count-Min
  Sketch and its Applications, J. Algorithms 2005
  (https://doi.org/10.1016/j.jalgor.2003.12.001)
- Metwally, Agrawal, El Abbadi — Efficient Computation of Frequent and Top-k
  Elements in Data Streams (SpaceSaving), ICDT 2005
  (https://doi.org/10.1007/978-3-540-30570-5_27)
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
    TOP_K_METRICS,
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

_LABEL_TO_S: dict[str, int] = {
    "5min": WINDOW_5MIN_S,
}

# Output column order.
_Q3_COLS = [
    "entity", "metric_base", "aggregation",
    "window_start_s", "window_size_s", "window_label",
    "exceedance_count", "rank",
]


# ---------------------------------------------------------------------------
# Core computation
# ---------------------------------------------------------------------------

def _compute_exceedance_counts(
    csv_path: Path,
    window_s: int,
    thresholds: dict[tuple, float],
    chunksize: int,
) -> dict[tuple, int]:
    """Stream the file and count threshold exceedances per group.

    Returns
    -------
    ``{(entity, metric_base, aggregation, window_start_s): exceedance_count}``
    """
    exc_counts: dict[tuple, int] = defaultdict(int)

    for chunk in _stream_long_chunks(csv_path, chunksize):
        chunk["window_start_s"] = (chunk["ts_s"] // window_s) * window_s
        ts_arr = chunk["ts_s"].to_numpy(dtype=np.int64)
        ws_arr = (ts_arr // window_s) * window_s
        entities = chunk["entity"].to_numpy()
        metric_bases = chunk["metric_base"].to_numpy()
        aggregations = chunk["aggregation"].to_numpy()
        values = chunk["value"].to_numpy(dtype=np.float64)

        for i in range(len(values)):
            thr = thresholds.get((entities[i], metric_bases[i]))
            if thr is not None and values[i] > thr:
                key = (
                    str(entities[i]),
                    str(metric_bases[i]),
                    str(aggregations[i]),
                    int(ws_arr[i]),
                )
                exc_counts[key] += 1

    return exc_counts


def _exc_counts_to_dataframe(
    exc_counts: dict[tuple, int],
    window_s: int,
    window_label: str,
    k: int,
) -> pd.DataFrame:
    """Convert exceedance count dict to a ranked top-K DataFrame for one window.

    Parameters
    ----------
    exc_counts:
        ``{(entity, metric_base, aggregation, window_start_s): count}``
    window_s:
        Window size in seconds (written to ``window_size_s`` column).
    window_label:
        Human-readable label (e.g. ``"5min"``).
    k:
        Maximum number of entries to keep per window.

    Returns
    -------
    DataFrame with columns defined by ``_Q3_COLS``.
    """
    if not exc_counts:
        return pd.DataFrame(columns=_Q3_COLS)

    # Group counts by window_start_s.
    window_groups: dict[int, list[tuple]] = defaultdict(list)
    for (entity, mb, agg, ws), cnt in exc_counts.items():
        window_groups[ws].append((entity, mb, agg, cnt))

    rows = []
    for ws in sorted(window_groups):
        entries = sorted(window_groups[ws], key=lambda x: x[3], reverse=True)[:k]
        for rank, (entity, mb, agg, cnt) in enumerate(entries, 1):
            rows.append({
                "entity":            entity,
                "metric_base":       mb,
                "aggregation":       agg,
                "window_start_s":    ws,
                "window_size_s":     window_s,
                "window_label":      window_label,
                "exceedance_count":  cnt,
                "rank":              rank,
            })

    df = pd.DataFrame(rows)[_Q3_COLS]
    df.sort_values(["window_start_s", "rank"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


# ---------------------------------------------------------------------------
# Multi-window runner
# ---------------------------------------------------------------------------

def run_q3(
    file_tag: str,
    output_dir: Path,
    k: int = TOP_K_METRICS,
    chunksize: int = 200,
    windows: list[tuple[int, str]] | None = None,
) -> Path:
    """Compute Q3 ground truth (exact top-K exceedance count) for one or more windows.

    Threshold per (entity, metric_base) is the file-wide p95 of all non-sentinel
    values.  The file is streamed twice: once to compute thresholds, once to count
    exceedances.

    One CSV is written per window size:
        <output_dir>/Q3/<safe_tag>_<window_label>.csv

    When more than one window is requested a combined CSV is also written:
        <output_dir>/Q3/<safe_tag>.csv

    Parameters
    ----------
    file_tag:
        Exathlon file tag, e.g. ``"app1/1_0_10000_17"``.
    output_dir:
        Root results directory (CSVs land under ``output_dir/Q3/``).
    k:
        Maximum number of top metrics to keep per window (default 10).
    chunksize:
        Rows per pandas read_csv chunk (default 200).
    windows:
        List of ``(window_size_s, label)`` pairs to compute.
        Defaults to ``[(WINDOW_5MIN_S, "5min")]``.

    Returns
    -------
    Path to the output CSV.  When multiple windows are requested this is the
    combined CSV; for a single window it is the per-window CSV.
    """
    if windows is None:
        windows = [(PRIMARY_WINDOW_S, PRIMARY_WINDOW_LABEL)]

    csv_path = file_csv_path(file_tag)
    if not csv_path.is_file():
        raise FileNotFoundError(f"Exathlon CSV not found: {csv_path}")

    tag = file_tag_safe(file_tag)
    out_dir = output_dir / "Q3"
    out_dir.mkdir(parents=True, exist_ok=True)

    log_phase("Q3", tag, "start",
              file=str(csv_path),
              windows=[lbl for _, lbl in windows],
              k=k,
              threshold_quantile=THRESHOLD_QUANTILE,
              chunksize=chunksize)

    # Pass 1 — compute per-metric thresholds (single pass over all columns).
    t0 = time.perf_counter()
    log_phase("Q3", tag, "computing thresholds (pass 1)")
    thresholds = compute_per_metric_thresholds(csv_path, THRESHOLD_QUANTILE, chunksize)
    thresh_elapsed = time.perf_counter() - t0
    log_phase("Q3", tag, "thresholds done",
              n_metrics=len(thresholds),
              elapsed_s=f"{thresh_elapsed:.1f}")

    all_parts: list[pd.DataFrame] = []

    for window_s, window_label in windows:
        log_phase("Q3", tag, f"counting exceedances window={window_label} (pass 2)")
        t1 = time.perf_counter()
        exc_counts = _compute_exceedance_counts(csv_path, window_s, thresholds, chunksize)
        count_elapsed = time.perf_counter() - t1

        df = _exc_counts_to_dataframe(exc_counts, window_s, window_label, k)

        per_window_path = out_dir / f"{tag}_{window_label}.csv"
        df.to_csv(per_window_path, index=False)

        log_phase("Q3", tag, f"window={window_label} done",
                  rows=len(df),
                  windows_n=df["window_start_s"].nunique() if not df.empty else 0,
                  total_exceedance_events=int(sum(exc_counts.values())),
                  elapsed_s=f"{count_elapsed:.1f}",
                  output=str(per_window_path))

        all_parts.append(df)

    if len(all_parts) == 1:
        combined_path = out_dir / f"{tag}_{windows[0][1]}.csv"
        log_phase("Q3", tag, "done (single window)", output=str(combined_path))
        return combined_path

    combined = pd.concat(all_parts, ignore_index=True)
    combined_path = out_dir / f"{tag}.csv"
    combined.to_csv(combined_path, index=False)

    log_phase("Q3", tag, "done",
              total_rows=len(combined),
              output=str(combined_path))
    return combined_path


# ---------------------------------------------------------------------------
# Summary helpers
# ---------------------------------------------------------------------------

def summarise_q3(df: pd.DataFrame) -> pd.DataFrame:
    """Return a compact per-(window_label, entity) summary of the Q3 output.

    For each (window_label, entity) group reports:
        - windows_n      : number of distinct windows in the output
        - median_top1_count : median exceedance count of the rank-1 metric
        - mean_count_rank1  : mean exceedance count of the rank-1 metric
        - total_exceedances : sum of all exceedance counts appearing in top-K
    """
    if df.empty:
        return pd.DataFrame()

    top1 = df[df["rank"] == 1].copy()

    summary_top1 = (
        top1.groupby(["window_label", "window_size_s", "entity"])
        .agg(
            windows_n=("window_start_s", "nunique"),
            median_top1_count=("exceedance_count", "median"),
            mean_top1_count=("exceedance_count", "mean"),
        )
        .reset_index()
    )

    summary_total = (
        df.groupby(["window_label", "window_size_s", "entity"])
        .agg(total_exceedances=("exceedance_count", "sum"))
        .reset_index()
    )

    result = summary_top1.merge(
        summary_total, on=["window_label", "window_size_s", "entity"], how="left"
    )
    result.sort_values(["window_size_s", "entity"], inplace=True)
    result.reset_index(drop=True, inplace=True)
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Q3 ground truth: exact top-K metrics by threshold-exceedance count "
            "per (window).  Threshold per metric = file-wide p95 of non-sentinel values."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples
--------
# Compute Q3 for a single file (primary 5-min window):
  python3 ground_truth/q3.py --file app1/1_0_10000_17

# Custom K:
  python3 ground_truth/q3.py --file app1/1_0_10000_17 --top-k 20

# Custom output directory:
  python3 ground_truth/q3.py --file app9/9_0_100000_1 --out-dir /tmp/gt

# Print a summary table after computation:
  python3 ground_truth/q3.py --file app1/1_0_10000_17 --summary
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
        help="Root output directory.  Q3 CSVs land under <out-dir>/Q3/.",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=TOP_K_METRICS,
        help=f"Number of top metrics to keep per window (default {TOP_K_METRICS}).",
    )
    parser.add_argument(
        "--windows",
        nargs="+",
        choices=list(_LABEL_TO_S),
        default=None,
        help="Window sizes to compute.  Currently supported: 5min (default).",
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

    if args.windows:
        selected_windows = [(_LABEL_TO_S[lbl], lbl) for lbl in args.windows]
    else:
        selected_windows = None  # run_q3 defaults to primary window

    combined_path = run_q3(
        file_tag=args.file,
        output_dir=args.out_dir,
        k=args.top_k,
        chunksize=args.chunksize,
        windows=selected_windows,
    )

    if args.summary:
        df = pd.read_csv(combined_path)
        summary = summarise_q3(df)
        print("\n--- Q3 summary ---")
        print(summary.to_string(index=False))
        print()


if __name__ == "__main__":
    main()
