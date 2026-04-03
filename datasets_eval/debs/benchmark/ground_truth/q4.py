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


def compute_q4_price_stats(dataframe: pd.DataFrame) -> pd.DataFrame:
    working = dataframe.copy()
    working["ws"] = window_start_ms_vectorized(
        working["ts_ms"].to_numpy(dtype=np.int64, copy=False), WINDOW_5MIN_MS
    )
    rows: list[dict] = []
    for (symbol, window_start), group in working.groupby(["symbol", "ws"]):
        group = group.sort_values("ts_ms")
        low = float(group["Last"].min())
        high = float(group["Last"].max())
        last = float(group["Last"].iloc[-1])
        rows.append(
            {
                "symbol": symbol,
                "window_start_ms": int(window_start),
                "high": high,
                "low": low,
                "last_price": last,
                "price_range": high - low,
            }
        )
    return pd.DataFrame(rows)


def run_q4(
    day: str,
    output_root: Path,
    chunksize: int,
    max_event_minutes: int | None = None,
) -> None:
    day_tag = day_tag_from_arg(day)
    csv_path = data_path("data_filtered") / day_to_filename(day)
    sub = output_root / "Q4"
    sub.mkdir(parents=True, exist_ok=True)

    loader_fn = functools.partial(load_filtered_day, max_event_minutes=max_event_minutes)
    dataframe, _ = timed_load("Q4", day_tag, csv_path, loader_fn, chunksize)
    if dataframe.empty:
        log_phase("Q4", day_tag, "skip empty")
        return

    log_phase("Q4", day_tag, "compute")
    compute_start = time.perf_counter()
    out_path = sub / f"{day_tag}.csv"
    compute_q4_price_stats(dataframe).to_csv(out_path, index=False)
    log_phase("Q4", day_tag, "done", out=str(out_path), seconds=f"{time.perf_counter() - compute_start:.1f}")
