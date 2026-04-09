from __future__ import annotations

"""Per-query offline ground-truth computation for the Exathlon benchmark.

Each public ``_gt_qN`` function reads the raw CSV for one file tag and writes
an exact-answer CSV to ``output_dir / QN / {safe_tag}.csv``.

``run_ground_truth_task`` is the entry point called by ``run_gt.py``.
"""

import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
import scipy.stats

_BENCHMARK_ROOT = Path(__file__).resolve().parent.parent
if str(_BENCHMARK_ROOT) not in sys.path:
    sys.path.insert(0, str(_BENCHMARK_ROOT))

from common import file_csv_path, file_tag_safe
from ground_truth.q1 import run_q1
from ground_truth.q2 import run_q2
from ground_truth.q3 import run_q3
from ground_truth.q4 import run_q4
from ground_truth.common import (
    THRESHOLD_QUANTILE,
    TOP_K_ENTITIES,
    TOP_K_METRICS,
    WINDOW_15MIN_S,
    WINDOW_1MIN_S,
    WINDOW_1HR_S,
    WINDOW_30MIN_S,
    WINDOW_5MIN_S,
    _stream_long_chunks,
    accumulate_window_distinct,
    accumulate_window_values,
    compute_per_metric_thresholds,
    load_exathlon_long,
    log_phase,
)

ACCEPTED_GT_QUERIES: tuple[str, ...] = ("Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8", "Q9")


# ---------------------------------------------------------------------------
# Helper: safe output path
# ---------------------------------------------------------------------------

def _out(output_dir: Path, query_id: str, tag: str) -> Path:
    p = output_dir / query_id / f"{tag}.csv"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


# ---------------------------------------------------------------------------
# Q1 — Windowed distribution profiling (p50 / p95 / p99) per (entity, metric_base)
# ---------------------------------------------------------------------------
# Delegated entirely to ground_truth/q1.py.  See that module for full
# documentation, window configuration, and the standalone CLI.


# ---------------------------------------------------------------------------
# Q3 — Top-K heavy metrics by threshold-exceedance count
# ---------------------------------------------------------------------------

def _gt_q3(
    csv_path: Path,
    window_s: int,
    k: int,
    out_path: Path,
    chunksize: int,
) -> None:
    """Exact top-K metrics by threshold-exceedance count per window.

    Threshold per (entity, metric_base) = p95 of all non-sentinel values
    in the file for that metric group.
    """
    thresholds = compute_per_metric_thresholds(csv_path, THRESHOLD_QUANTILE, chunksize)

    # Count exceedances per (entity, metric_base, aggregation, window_start_s).
    exc_counts: dict[tuple, int] = defaultdict(int)
    for chunk in _stream_long_chunks(csv_path, chunksize):
        chunk["window_start_s"] = (chunk["ts_s"] // window_s) * window_s
        for _, row in chunk.iterrows():
            thr = thresholds.get((row["entity"], row["metric_base"]))
            if thr is not None and row["value"] > thr:
                key = (
                    int(row["window_start_s"]),
                    str(row["entity"]),
                    str(row["metric_base"]),
                    str(row["aggregation"]),
                )
                exc_counts[key] += 1

    # Format as {window_start_s: [(key_str, count), ...]} then take top-K.
    window_counts: dict[int, list[tuple[str, int]]] = defaultdict(list)
    for (ws, entity, mb, agg), cnt in exc_counts.items():
        key_str = f"entity={entity};metric_base={mb};aggregation={agg}"
        window_counts[ws].append((key_str, cnt))

    rows = []
    for ws, entries in sorted(window_counts.items()):
        top = sorted(entries, key=lambda x: x[1], reverse=True)[:k]
        for rank, (key_str, cnt) in enumerate(top, 1):
            rows.append({
                "window_start_s": ws,
                "rank": rank,
                "key": key_str,
                "exact_count": cnt,
            })

    pd.DataFrame(rows).to_csv(out_path, index=False)


# ---------------------------------------------------------------------------
# Q4 — Windowed min / max / range per (entity, metric_base)
# ---------------------------------------------------------------------------

def _gt_q4(csv_path: Path, window_s: int, out_path: Path, chunksize: int) -> None:
    """Legacy helper for exact min, max, and range per window."""
    acc = accumulate_window_values(csv_path, window_s, chunksize)
    rows = []
    for (entity, mb, ws), values in acc.items():
        arr = np.asarray(values, dtype=np.float64)
        exact_min = float(arr.min())
        exact_max = float(arr.max())
        rows.append({
            "entity": entity,
            "metric_base": mb,
            "window_start_s": ws,
            "window_size_s": window_s,
            "exact_min": exact_min,
            "exact_max": exact_max,
            "exact_range": exact_max - exact_min,
            "count": len(arr),
        })
    pd.DataFrame(rows).to_csv(out_path, index=False)


# ---------------------------------------------------------------------------
# Q5 — IQR-based anomaly flags (Tukey fences) per (entity, metric_base)
# ---------------------------------------------------------------------------

def _gt_q5(csv_path: Path, window_s: int, out_path: Path, chunksize: int) -> None:
    """Exact IQR parameters and anomaly rates per (entity, metric_base, window)."""
    acc = accumulate_window_values(csv_path, window_s, chunksize)
    rows = []
    for (entity, mb, ws), values in acc.items():
        arr = np.asarray(values, dtype=np.float64)
        if len(arr) < 4:
            continue
        q1 = float(np.percentile(arr, 25))
        q3 = float(np.percentile(arr, 75))
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        n_anomaly = int(np.sum((arr < lower) | (arr > upper)))
        rows.append({
            "entity": entity,
            "metric_base": mb,
            "window_start_s": ws,
            "q1": q1,
            "q3": q3,
            "iqr": iqr,
            "lower_fence": lower,
            "upper_fence": upper,
            "n_total": len(arr),
            "n_anomaly": n_anomaly,
            "anomaly_rate": n_anomaly / len(arr),
        })
    pd.DataFrame(rows).to_csv(out_path, index=False)


# ---------------------------------------------------------------------------
# Q6 — Distinct active metrics per window
# ---------------------------------------------------------------------------

def _gt_q6(csv_path: Path, window_s: int, out_path: Path, chunksize: int) -> None:
    """Exact distinct (entity, metric_base, aggregation) count per window."""
    distinct = accumulate_window_distinct(csv_path, window_s, chunksize)
    rows = [
        {"window_start_s": ws, "exact_distinct_count": len(series_set)}
        for ws, series_set in sorted(distinct.items())
    ]
    pd.DataFrame(rows).to_csv(out_path, index=False)


# ---------------------------------------------------------------------------
# Q7 — Top-K entities by anomaly event volume
# ---------------------------------------------------------------------------

def _gt_q7(
    csv_path: Path,
    window_s: int,
    k: int,
    out_path: Path,
    chunksize: int,
) -> None:
    """Exact top-K entities by anomaly event count per window.

    Uses the same IQR-based anomaly definition as Q5: value is anomalous if
    it falls outside [Q1 - 1.5*IQR, Q3 + 1.5*IQR] for its
    (entity, metric_base, window).

    Step 1: compute IQR bounds per (entity, metric_base, window) from Q5.
    Step 2: count anomalous events per (entity, window).
    """
    # Build IQR bounds from accumulated window values.
    acc = accumulate_window_values(csv_path, window_s, chunksize)
    bounds: dict[tuple, tuple[float, float]] = {}
    for (entity, mb, ws), values in acc.items():
        arr = np.asarray(values, dtype=np.float64)
        if len(arr) < 4:
            continue
        q1 = float(np.percentile(arr, 25))
        q3 = float(np.percentile(arr, 75))
        iqr = q3 - q1
        bounds[(entity, mb, ws)] = (q1 - 1.5 * iqr, q3 + 1.5 * iqr)

    # Count anomaly events per (entity, window).
    entity_window_counts: dict[tuple, int] = defaultdict(int)
    for chunk in _stream_long_chunks(csv_path, chunksize):
        chunk["window_start_s"] = (chunk["ts_s"] // window_s) * window_s
        for _, row in chunk.iterrows():
            b = bounds.get((row["entity"], row["metric_base"], int(row["window_start_s"])))
            if b is None:
                continue
            lower, upper = b
            if row["value"] < lower or row["value"] > upper:
                entity_window_counts[(str(row["entity"]), int(row["window_start_s"]))] += 1

    # Take top-K per window.
    window_entity: dict[int, list[tuple[str, int]]] = defaultdict(list)
    for (entity, ws), cnt in entity_window_counts.items():
        window_entity[ws].append((entity, cnt))

    rows = []
    for ws, entries in sorted(window_entity.items()):
        top = sorted(entries, key=lambda x: x[1], reverse=True)[:k]
        for rank, (entity, cnt) in enumerate(top, 1):
            rows.append({
                "window_start_s": ws,
                "rank": rank,
                "entity": entity,
                "exact_count": cnt,
            })

    pd.DataFrame(rows).to_csv(out_path, index=False)


# ---------------------------------------------------------------------------
# Q8 — Quantile drift between adjacent windows
# ---------------------------------------------------------------------------

def _gt_q8(csv_path: Path, window_s: int, out_path: Path, chunksize: int) -> None:
    """Exact |p95_t − p95_{t-1}| and |p50_t − p50_{t-1}| per (entity, metric_base)."""
    acc = accumulate_window_values(csv_path, window_s, chunksize)

    # Collect (entity, metric_base) → {window_start_s: (p50, p95)} mapping.
    series_windows: dict[tuple, dict[int, tuple[float, float]]] = defaultdict(dict)
    for (entity, mb, ws), values in acc.items():
        arr = np.asarray(values, dtype=np.float64)
        series_windows[(entity, mb)][ws] = (
            float(np.percentile(arr, 50)),
            float(np.percentile(arr, 95)),
        )

    rows = []
    for (entity, mb), window_dict in series_windows.items():
        sorted_windows = sorted(window_dict.keys())
        for i in range(1, len(sorted_windows)):
            prev_ws = sorted_windows[i - 1]
            curr_ws = sorted_windows[i]
            prev_p50, prev_p95 = window_dict[prev_ws]
            curr_p50, curr_p95 = window_dict[curr_ws]
            rows.append({
                "entity": entity,
                "metric_base": mb,
                "window_start_s": curr_ws,
                "prev_window_start_s": prev_ws,
                "prev_p50": prev_p50,
                "curr_p50": curr_p50,
                "drift_p50": abs(curr_p50 - prev_p50),
                "prev_p95": prev_p95,
                "curr_p95": curr_p95,
                "drift_p95": abs(curr_p95 - prev_p95),
            })

    pd.DataFrame(rows).to_csv(out_path, index=False)


# ---------------------------------------------------------------------------
# Q9 — Saturation ratio per entity (USE method)
# ---------------------------------------------------------------------------

def _gt_q9(csv_path: Path, window_s: int, out_path: Path, chunksize: int) -> None:
    """Exact saturation ratio = saturated_metrics / total_active_metrics per (entity, window).

    A (entity, metric_base) is considered saturated in a window if any of its
    values in that window exceeds the file-wide p95 threshold for that group.
    """
    thresholds = compute_per_metric_thresholds(csv_path, THRESHOLD_QUANTILE, chunksize)

    # For each (entity, window): track which metric_bases are active and which are saturated.
    active: dict[tuple, set] = defaultdict(set)     # (entity, ws) → {metric_base}
    saturated: dict[tuple, set] = defaultdict(set)  # (entity, ws) → {metric_base}

    for chunk in _stream_long_chunks(csv_path, chunksize):
        chunk["window_start_s"] = (chunk["ts_s"] // window_s) * window_s
        for _, row in chunk.iterrows():
            ew = (str(row["entity"]), int(row["window_start_s"]))
            active[ew].add(row["metric_base"])
            thr = thresholds.get((row["entity"], row["metric_base"]))
            if thr is not None and row["value"] > thr:
                saturated[ew].add(row["metric_base"])

    all_ew = set(active.keys()) | set(saturated.keys())
    rows = []
    for (entity, ws) in sorted(all_ew):
        total = len(active.get((entity, ws), set()))
        exceeded = len(saturated.get((entity, ws), set()))
        rows.append({
            "entity": entity,
            "window_start_s": ws,
            "saturated_metric_count": exceeded,
            "total_active_metric_count": total,
            "exact_saturation_ratio": exceeded / total if total > 0 else 0.0,
        })

    pd.DataFrame(rows).to_csv(out_path, index=False)


# ---------------------------------------------------------------------------
# Dispatch table
# ---------------------------------------------------------------------------

def run_ground_truth_task(
    query_id: str,
    file_tag: str,
    output_dir: Path,
    chunksize: int = 200,
) -> None:
    """Compute ground truth for (query_id, file_tag) and write to output_dir."""
    csv_path = file_csv_path(file_tag)
    if not csv_path.is_file():
        raise FileNotFoundError(f"Exathlon CSV not found: {csv_path}")

    tag = file_tag_safe(file_tag)
    out = _out(output_dir, query_id, tag)

    log_phase(query_id, tag, "start")

    if query_id == "Q1":
        run_q1(file_tag, output_dir, chunksize)
    elif query_id == "Q2":
        run_q2(file_tag, output_dir, chunksize)
    elif query_id == "Q3":
        run_q3(file_tag, output_dir, k=TOP_K_METRICS, chunksize=chunksize)
    elif query_id == "Q4":
        run_q4(file_tag, output_dir, chunksize=chunksize)
    elif query_id == "Q5":
        _gt_q5(csv_path, WINDOW_15MIN_S, out, chunksize)
    elif query_id == "Q6":
        _gt_q6(csv_path, WINDOW_5MIN_S, out, chunksize)
    elif query_id == "Q7":
        _gt_q7(csv_path, WINDOW_5MIN_S, TOP_K_ENTITIES, out, chunksize)
    elif query_id == "Q8":
        _gt_q8(csv_path, WINDOW_5MIN_S, out, chunksize)
    elif query_id == "Q9":
        _gt_q9(csv_path, WINDOW_5MIN_S, out, chunksize)
    else:
        raise ValueError(f"No ground truth runner for query {query_id!r}.")

    log_phase(query_id, tag, "done", output=str(out))
