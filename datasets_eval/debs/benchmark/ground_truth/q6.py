from __future__ import annotations

import functools
import time
from pathlib import Path

import numpy as np
import pandas as pd

from ground_truth.common import (
    WINDOW_5MIN_MS,
    day_tag_from_arg,
    load_filtered_day,
    log_phase,
    timed_load,
    window_start_ms_vectorized,
)
from common import data_path, day_to_filename


def compute_q6_distinct(dataframe: pd.DataFrame) -> pd.DataFrame:
    ts = dataframe["ts_ms"].to_numpy(dtype=np.int64, copy=False)
    ws = window_start_ms_vectorized(ts, WINDOW_5MIN_MS)
    tmp = dataframe.assign(window_start_ms=ws)
    rows: list[dict] = []
    for wstart, group in tmp.groupby("window_start_ms", sort=False):
        exact = int(group["symbol"].nunique())
        rows.append({"window_start_ms": int(wstart), "exact_count": exact})
    return pd.DataFrame(rows)


def run_q6(
    day: str,
    output_root: Path,
    chunksize: int,
    max_event_minutes: int | None = None,
) -> None:
    day_tag = day_tag_from_arg(day)
    csv_path = data_path("data_filtered") / day_to_filename(day)
    sub = output_root / "Q6"
    sub.mkdir(parents=True, exist_ok=True)

    loader_fn = functools.partial(load_filtered_day, max_event_minutes=max_event_minutes)
    dataframe, _ = timed_load("Q6", day_tag, csv_path, loader_fn, chunksize)
    if dataframe.empty:
        log_phase("Q6", day_tag, "skip empty")
        return

    log_phase("Q6", day_tag, "compute")
    compute_start = time.perf_counter()
    out_path = sub / f"{day_tag}.csv"
    compute_q6_distinct(dataframe).to_csv(out_path, index=False)
    log_phase("Q6", day_tag, "done", out=str(out_path), seconds=f"{time.perf_counter() - compute_start:.1f}")
