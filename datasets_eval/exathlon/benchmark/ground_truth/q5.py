from __future__ import annotations

"""Q5 — IQR-based anomaly flags (Tukey fences) per (entity, metric_base).

Purpose
-------
Detect window-local outliers robustly.

Automated telemetry anomaly detection often needs a threshold-free rule that
still tolerates skewed, heavy-tailed metric distributions.  Q5 uses Tukey
fences derived from exact quartiles so the sketch side can be validated against
an interpretable baseline.

Formula
-------
For each (entity, metric_base, window):
    q1   = exact 25th percentile
    p50  = exact 50th percentile
    q3   = exact 75th percentile
    iqr  = q3 - q1
    lower_fence = q1 - 1.5 * iqr
    upper_fence = q3 + 1.5 * iqr

An event x_t is flagged when:
    x_t < lower_fence or x_t > upper_fence

Output columns
--------------
entity, metric_base, window_start_s, window_size_s, window_label,
q1, p50, q3, iqr, lower_fence, upper_fence, n_total, n_anomaly, anomaly_rate

References
----------
- Jacob et al. — Exathlon: A Benchmark for Explainable Anomaly Detection
  over Time Series, VLDB 2021 (https://doi.org/10.14778/3476249.3476307)
- Tukey — Exploratory Data Analysis, Addison-Wesley 1977
- Chandola, Banerjee, Kumar — Anomaly Detection: A Survey, ACM CSUR 2009
  (https://doi.org/10.1145/1541880.1541882)
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
    WINDOW_15MIN_S,
    WINDOW_5MIN_S,
    _stream_long_chunks,
    log_phase,
)

# ---------------------------------------------------------------------------
# Window configuration
# ---------------------------------------------------------------------------

PRIMARY_WINDOW_S = WINDOW_15MIN_S
PRIMARY_WINDOW_LABEL = "15min"

ALL_WINDOWS: list[tuple[int, str]] = [
    (WINDOW_15MIN_S, "15min"),
    (WINDOW_5MIN_S, "5min"),
]

# Tukey fences become unstable on tiny windows; keep the legacy minimum.
MIN_WINDOW_SAMPLES = 4

_Q5_COLS = [
    "entity",
    "metric_base",
    "window_start_s",
    "window_size_s",
    "window_label",
    "q1",
    "p50",
    "q3",
    "iqr",
    "lower_fence",
    "upper_fence",
    "n_total",
    "n_anomaly",
    "anomaly_rate",
]


# ---------------------------------------------------------------------------
# Core computation
# ---------------------------------------------------------------------------

def _accumulate_all_windows(
    csv_path: Path,
    windows: list[tuple[int, str]],
    chunksize: int,
) -> dict[int, dict[tuple, list[float]]]:
    """Stream the CSV once and accumulate values for every requested window."""
    from collections import defaultdict

    acc: dict[int, dict[tuple, list[float]]] = {ws: defaultdict(list) for ws, _ in windows}
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
    """Convert one window accumulator to the canonical Q5 output shape."""
    if not acc:
        return pd.DataFrame(columns=_Q5_COLS)

    rows = []
    for (entity, metric_base, window_start_s), values in acc.items():
        arr = np.asarray(values, dtype=np.float64)
        if len(arr) < MIN_WINDOW_SAMPLES:
            continue
        q1 = float(np.percentile(arr, 25))
        p50 = float(np.percentile(arr, 50))
        q3 = float(np.percentile(arr, 75))
        iqr = q3 - q1
        lower_fence = q1 - 1.5 * iqr
        upper_fence = q3 + 1.5 * iqr
        n_total = int(len(arr))
        n_anomaly = int(np.sum((arr < lower_fence) | (arr > upper_fence)))
        rows.append({
            "entity": entity,
            "metric_base": metric_base,
            "window_start_s": window_start_s,
            "window_size_s": window_s,
            "window_label": window_label,
            "q1": q1,
            "p50": p50,
            "q3": q3,
            "iqr": iqr,
            "lower_fence": lower_fence,
            "upper_fence": upper_fence,
            "n_total": n_total,
            "n_anomaly": n_anomaly,
            "anomaly_rate": n_anomaly / n_total,
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=_Q5_COLS)
    df.sort_values(["entity", "metric_base", "window_start_s"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df[_Q5_COLS]


def compute_q5_single_window(
    csv_path: Path,
    window_s: int,
    window_label: str,
    chunksize: int,
) -> pd.DataFrame:
    """Return exact quartiles and Tukey-fence anomaly stats for one window size."""
    acc = _accumulate_all_windows(csv_path, [(window_s, window_label)], chunksize)
    return _acc_to_dataframe(acc[window_s], window_s, window_label)


# ---------------------------------------------------------------------------
# Multi-window runner
# ---------------------------------------------------------------------------

def run_q5(
    file_tag: str,
    output_dir: Path,
    chunksize: int = 200,
    windows: list[tuple[int, str]] | None = None,
) -> Path:
    """Compute Q5 ground truth for one or more window sizes in a single pass."""
    if windows is None:
        windows = [(PRIMARY_WINDOW_S, PRIMARY_WINDOW_LABEL)]

    csv_path = file_csv_path(file_tag)
    if not csv_path.is_file():
        raise FileNotFoundError(f"Exathlon CSV not found: {csv_path}")

    tag = file_tag_safe(file_tag)
    out_dir = output_dir / "Q5"
    out_dir.mkdir(parents=True, exist_ok=True)

    log_phase(
        "Q5",
        tag,
        "start",
        file=str(csv_path),
        windows=[label for _, label in windows],
        chunksize=chunksize,
    )

    t0 = time.perf_counter()
    log_phase("Q5", tag, "streaming (single pass for all windows)")
    acc_by_window = _accumulate_all_windows(csv_path, windows, chunksize)
    stream_elapsed = time.perf_counter() - t0
    log_phase("Q5", tag, "streaming done", elapsed_s=f"{stream_elapsed:.1f}")

    all_parts: list[pd.DataFrame] = []

    for window_s, window_label in windows:
        t1 = time.perf_counter()
        df = _acc_to_dataframe(acc_by_window[window_s], window_s, window_label)
        per_window_path = out_dir / f"{tag}_{window_label}.csv"
        df.to_csv(per_window_path, index=False)

        elapsed = time.perf_counter() - t1
        log_phase(
            "Q5",
            tag,
            f"window={window_label} done",
            rows=len(df),
            windows_n=df["window_start_s"].nunique() if not df.empty else 0,
            median_iqr=(float(df["iqr"].median()) if not df.empty else "n/a"),
            elapsed_s=f"{elapsed:.1f}",
            output=str(per_window_path),
        )
        all_parts.append(df)

    combined = pd.concat(all_parts, ignore_index=True) if all_parts else pd.DataFrame(columns=_Q5_COLS)
    combined_path = out_dir / f"{tag}.csv"
    combined.to_csv(combined_path, index=False)

    log_phase("Q5", tag, "done", total_rows=len(combined), output=str(combined_path))
    return combined_path


# ---------------------------------------------------------------------------
# Summary helpers
# ---------------------------------------------------------------------------

def summarise_q5(df: pd.DataFrame) -> pd.DataFrame:
    """Return a compact per-(window_label, entity) summary of Q5 output."""
    if df.empty:
        return pd.DataFrame()

    summary = (
        df.groupby(["window_label", "window_size_s", "entity"])
        .agg(
            windows_n=("window_start_s", "nunique"),
            median_iqr=("iqr", "median"),
            mean_anomaly_rate=("anomaly_rate", "mean"),
            p95_anomaly_rate=("anomaly_rate", lambda s: float(np.percentile(s, 95))),
            total_anomalies=("n_anomaly", "sum"),
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

    label_to_window = dict(ALL_WINDOWS)
    windows = []
    for label in window_labels:
        if label not in label_to_window.values():
            raise ValueError(f"unsupported Q5 window label {label!r}")
        for window_s, window_label in ALL_WINDOWS:
            if window_label == label:
                windows.append((window_s, window_label))
                break
    return windows


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute offline Q5 ground truth for the Exathlon benchmark.",
    )
    parser.add_argument("--file", required=True, help="File tag, e.g. app1/1_0_10000_17")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=_BENCHMARK_ROOT / "results" / "ground_truth",
        help="Root output directory (Q5 CSVs land under <out-dir>/Q5/).",
    )
    parser.add_argument("--chunksize", type=int, default=200)
    parser.add_argument(
        "--window",
        action="append",
        choices=[label for _, label in ALL_WINDOWS],
        help="Optional window label(s) to compute. Repeat for multiple windows.",
    )
    parser.add_argument(
        "--write-summary",
        action="store_true",
        help="Also write a compact summary CSV next to the combined output.",
    )
    args = parser.parse_args()

    selected_windows = _parse_window_args(args.window)
    combined_path = run_q5(
        args.file,
        args.out_dir,
        chunksize=args.chunksize,
        windows=selected_windows,
    )

    if args.write_summary:
        df = pd.read_csv(combined_path)
        summary = summarise_q5(df)
        summary_path = combined_path.with_name(combined_path.stem + "_summary.csv")
        summary.to_csv(summary_path, index=False)
        print(summary_path)

    print(combined_path)


if __name__ == "__main__":
    main()
