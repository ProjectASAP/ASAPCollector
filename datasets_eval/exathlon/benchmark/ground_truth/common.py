from __future__ import annotations

"""Shared utilities for exathlon offline ground-truth computation."""

import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Generator

import numpy as np
import pandas as pd

_BENCHMARK_ROOT = Path(__file__).resolve().parent.parent
if str(_BENCHMARK_ROOT) not in sys.path:
    sys.path.insert(0, str(_BENCHMARK_ROOT))

from common import (
    SENTINEL_VALUE,
    build_column_metadata,
    file_csv_path,
    file_tag_safe,
)

# ---------------------------------------------------------------------------
# Window constants (seconds)
# ---------------------------------------------------------------------------

WINDOW_1MIN_S = 60
WINDOW_5MIN_S = 300
WINDOW_15MIN_S = 900
WINDOW_30MIN_S = 1_800
WINDOW_1HR_S = 3_600

# Top-K parameter for Q3 / Q7.
TOP_K_METRICS = 10
TOP_K_ENTITIES = 3

# Threshold percentile used for Q3 (exceedance counting) and Q9 (saturation).
# Values above this file-local percentile are considered "saturated / exceeded."
THRESHOLD_QUANTILE = 0.95


# ---------------------------------------------------------------------------
# Column-level streaming helpers
# ---------------------------------------------------------------------------

def _stream_long_chunks(
    csv_path: Path,
    chunksize: int = 200,
) -> Generator[pd.DataFrame, None, None]:
    """Yield long-format DataFrames from a wide exathlon CSV.

    Each yielded DataFrame has columns:
        ts_s (int64), entity (str), metric_base (str), aggregation (str), value (float64)

    Sentinel (-1.0) and NaN values are filtered out.
    """
    header_df = pd.read_csv(csv_path, nrows=0)
    col_meta = build_column_metadata(list(header_df.columns))
    metric_cols = list(col_meta.keys())
    if not metric_cols:
        return

    # Pre-build a lookup DataFrame for fast join.
    meta_records = [
        {"col": col, "entity": e, "metric_base": mb, "aggregation": agg}
        for col, (e, mb, agg) in col_meta.items()
    ]
    meta_df = pd.DataFrame(meta_records)

    for chunk in pd.read_csv(
        csv_path,
        usecols=["t"] + metric_cols,
        chunksize=chunksize,
        dtype=object,
        low_memory=False,
    ):
        melted = chunk.melt(id_vars=["t"], var_name="col", value_name="value")
        melted["value"] = pd.to_numeric(melted["value"], errors="coerce")
        melted["ts_s"] = pd.to_numeric(melted["t"], errors="coerce").astype(np.int64)
        mask = melted["value"].notna() & (melted["value"] != SENTINEL_VALUE)
        melted = melted[mask].merge(meta_df, on="col", how="inner")
        if melted.empty:
            continue
        melted = melted[["ts_s", "entity", "metric_base", "aggregation", "value"]].copy()
        melted["value"] = melted["value"].astype(np.float64)
        yield melted


def load_exathlon_long(csv_path: Path, chunksize: int = 200) -> pd.DataFrame:
    """Return full long-format DataFrame for a file.

    Suitable for small-to-medium files (< 200k rows).  For very large files
    prefer the streaming helpers below.
    """
    parts: list[pd.DataFrame] = []
    for chunk in _stream_long_chunks(csv_path, chunksize=chunksize):
        parts.append(chunk)
    if not parts:
        return pd.DataFrame(
            columns=["ts_s", "entity", "metric_base", "aggregation", "value"]
        )
    return pd.concat(parts, ignore_index=True)


# ---------------------------------------------------------------------------
# Windowed accumulation (memory-bounded streaming)
# ---------------------------------------------------------------------------

def accumulate_window_values(
    csv_path: Path,
    window_s: int,
    chunksize: int = 200,
) -> dict[tuple, list]:
    """Stream CSV and accumulate values by (entity, metric_base, window_start_s).

    Returns a dict: {(entity, metric_base, window_start_s): [float, ...]}
    """
    acc: dict[tuple, list] = defaultdict(list)
    for chunk in _stream_long_chunks(csv_path, chunksize):
        chunk["window_start_s"] = (chunk["ts_s"] // window_s) * window_s
        for (entity, mb, ws), group in chunk.groupby(
            ["entity", "metric_base", "window_start_s"]
        ):
            acc[(entity, mb, int(ws))].extend(group["value"].tolist())
    return acc


def accumulate_window_distinct(
    csv_path: Path,
    window_s: int,
    chunksize: int = 200,
) -> dict[int, set]:
    """Return {window_start_s: set of (entity, metric_base, aggregation)} across the file."""
    distinct: dict[int, set] = defaultdict(set)
    for chunk in _stream_long_chunks(csv_path, chunksize):
        chunk["window_start_s"] = (chunk["ts_s"] // window_s) * window_s
        for (ws,), group in chunk.groupby(["window_start_s"]):
            ws_int = int(ws)
            for _, row in group[["entity", "metric_base", "aggregation"]].iterrows():
                distinct[ws_int].add((row["entity"], row["metric_base"], row["aggregation"]))
    return distinct


# ---------------------------------------------------------------------------
# Per-file threshold computation (for Q3 / Q9)
# ---------------------------------------------------------------------------

def compute_per_metric_thresholds(
    csv_path: Path,
    quantile: float = THRESHOLD_QUANTILE,
    chunksize: int = 200,
) -> dict[tuple, float]:
    """Return {(entity, metric_base): threshold_value} computed as `quantile`-th percentile
    of all non-sentinel values in the file for each (entity, metric_base) group.
    """
    acc: dict[tuple, list] = defaultdict(list)
    for chunk in _stream_long_chunks(csv_path, chunksize):
        for (entity, mb), group in chunk.groupby(["entity", "metric_base"]):
            acc[(entity, mb)].extend(group["value"].tolist())
    return {
        key: float(np.percentile(values, quantile * 100))
        for key, values in acc.items()
        if values
    }


# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------

def log_phase(query: str, tag: str, message: str, **extra: object) -> None:
    parts = [f"ground_truth {query} {tag}", message]
    for key, value in extra.items():
        parts.append(f"{key}={value}")
    print(" ".join(str(p) for p in parts), file=sys.stderr, flush=True)


def timed_load(
    label: str,
    tag: str,
    path: Path,
    loader,
    chunksize: int,
) -> tuple[pd.DataFrame, float]:
    log_phase(label, tag, "load", path=str(path))
    start = time.perf_counter()
    df = loader(path, chunksize)
    elapsed = time.perf_counter() - start
    log_phase(label, tag, "loaded", rows=len(df), seconds=f"{elapsed:.1f}")
    return df, elapsed
