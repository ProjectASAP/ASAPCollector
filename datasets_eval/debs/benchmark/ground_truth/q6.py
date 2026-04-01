from __future__ import annotations

import time
from pathlib import Path

import numpy as np
import pandas as pd

from ground_truth.common import (
    WINDOW_5MIN_MS,
    day_tag_from_arg,
    load_full_feed_day,
    log_phase,
    timed_load,
    window_start_ms_vectorized,
)
from common import data_path, day_to_filename


def compute_q6_distinct_symbols(dataframe: pd.DataFrame) -> pd.DataFrame:
    working = dataframe.copy()
    working["ws"] = window_start_ms_vectorized(
        working["ts_ms"].to_numpy(dtype=np.int64, copy=False), WINDOW_5MIN_MS
    )
    unique_counts = working.groupby("ws")["symbol"].nunique().reset_index(name="distinct")
    unique_counts["window_start_ms"] = unique_counts["ws"].astype(np.int64)
    return unique_counts[["window_start_ms", "distinct"]]


def run_q6(day: str, output_root: Path, chunksize: int) -> None:
    day_tag = day_tag_from_arg(day)
    csv_path = data_path("data") / day_to_filename(day)
    sub = output_root / "Q6"
    sub.mkdir(parents=True, exist_ok=True)

    dataframe, _ = timed_load("Q6", day_tag, csv_path, load_full_feed_day, chunksize)
    if dataframe.empty:
        log_phase("Q6", day_tag, "skip empty")
        return

    log_phase("Q6", day_tag, "compute")
    compute_start = time.perf_counter()
    out_path = sub / f"{day_tag}.csv"
    compute_q6_distinct_symbols(dataframe).to_csv(out_path, index=False)
    log_phase("Q6", day_tag, "done", out=str(out_path), seconds=f"{time.perf_counter() - compute_start:.1f}")
