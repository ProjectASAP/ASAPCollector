from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.dataset as ds

BENCH_ROOT = Path(__file__).resolve().parent.parent
if str(BENCH_ROOT) not in sys.path:
    sys.path.insert(0, str(BENCH_ROOT))

from common import PARQUET_PATH

WINDOW_5MIN_US = 300 * 1_000_000   # 5 minutes in microseconds

# Q4 archetype columns — present in snowset-main, not in full_join.
_PROF_COLS = [
    "profHjRso",
    "profSortRso",
    "profAggRso",
    "profScanRso",
    "profFilterRso",
]
_PROF_NAMES = [
    "join_heavy",
    "sort_heavy",
    "agg_heavy",
    "scan_heavy",
    "filter_heavy",
]


def floor_to_window(ts_series: pd.Series, window_us: int = WINDOW_5MIN_US) -> pd.Series:
    """Floor timezone-aware timestamp[us] series to 5-minute UTC window boundaries.

    Returns int64 microseconds since epoch (UTC), suitable as a stable window key.
    """
    us = ts_series.astype(np.int64)   # pandas int64 = nanoseconds for tz-aware
    # createdTime is timestamp[us, tz=UTC]; pandas represents tz-aware timestamps
    # in nanoseconds internally, so convert to microseconds first.
    us_epoch = us // 1_000             # ns → µs
    return (us_epoch // window_us * window_us)


def derive_archetype(df: pd.DataFrame) -> pd.Series:
    """Return the dominant operator archetype per row using profiling columns."""
    present = [c for c in _PROF_COLS if c in df.columns]
    if not present:
        return pd.Series(["other"] * len(df), index=df.index)
    mat = df[present].to_numpy(dtype=np.int64)
    max_vals = mat.max(axis=1)
    idx = mat.argmax(axis=1)
    names = [_PROF_NAMES[_PROF_COLS.index(c)] for c in present]
    labels = np.where(
        max_vals > 0,
        np.array(names, dtype=object)[idx],
        "other",
    )
    return pd.Series(labels, index=df.index)


def open_dataset() -> ds.Dataset:
    return ds.dataset(str(PARQUET_PATH), format="parquet")


def log_phase(label: str, msg: str, **kw: object) -> None:
    parts = [f"ground_truth {label}", msg]
    for k, v in kw.items():
        parts.append(f"{k}={v}")
    print(" ".join(parts), file=sys.stderr, flush=True)
