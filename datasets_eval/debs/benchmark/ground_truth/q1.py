from __future__ import annotations

import functools
import time
from pathlib import Path

import numpy as np
import pandas as pd

from ground_truth.common import (
    ALPHA_EMA_100,
    ALPHA_EMA_38,
    WINDOW_5MIN_MS,
    day_tag_from_arg,
    load_filtered_day,
    log_phase,
    timed_load,
    window_start_ms_vectorized,
)
from common import data_path, day_to_filename


def compute_q1_ema(dataframe: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    for symbol, group in dataframe.groupby("symbol", sort=False):
        group = group.sort_values("ts_ms")
        timestamps_ms = group["ts_ms"].to_numpy(dtype=np.int64, copy=False)
        prices = group["Last"].astype(float).to_numpy()
        window_starts = window_start_ms_vectorized(timestamps_ms, WINDOW_5MIN_MS)
        ema38: float | None = None
        ema100: float | None = None
        previous_window_start: int | None = None
        for index in range(len(prices)):
            price = float(prices[index])
            window_start = int(window_starts[index])
            if previous_window_start is not None and window_start != previous_window_start:
                rows.append(
                    {
                        "symbol": symbol,
                        "window_start_ms": previous_window_start,
                        "ema38": ema38,
                        "ema100": ema100,
                    }
                )
            ema38 = price if ema38 is None else ALPHA_EMA_38 * price + (1 - ALPHA_EMA_38) * ema38
            ema100 = (
                price if ema100 is None else ALPHA_EMA_100 * price + (1 - ALPHA_EMA_100) * ema100
            )
            previous_window_start = window_start
        if previous_window_start is not None:
            rows.append(
                {
                    "symbol": symbol,
                    "window_start_ms": previous_window_start,
                    "ema38": ema38,
                    "ema100": ema100,
                }
            )
    return pd.DataFrame(rows)


def run_q1_only(
    day: str,
    output_root: Path,
    chunksize: int,
    max_event_minutes: int | None = None,
) -> None:
    day_tag = day_tag_from_arg(day)
    filtered_dir = data_path("data_filtered")
    csv_path = filtered_dir / day_to_filename(day)
    output_root.mkdir(parents=True, exist_ok=True)
    (output_root / "Q1").mkdir(parents=True, exist_ok=True)

    loader_fn = functools.partial(
        load_filtered_day, max_event_minutes=max_event_minutes
    )
    dataframe, _ = timed_load("Q1", day_tag, csv_path, loader_fn, chunksize)
    if dataframe.empty:
        log_phase("Q1", day_tag, "skip empty")
        return

    log_phase("Q1", day_tag, "compute")
    compute_start = time.perf_counter()
    path_q1 = output_root / "Q1" / f"{day_tag}.csv"
    compute_q1_ema(dataframe).to_csv(path_q1, index=False)
    log_phase(
        "Q1",
        day_tag,
        "done",
        out=str(path_q1),
        seconds=f"{time.perf_counter() - compute_start:.1f}",
    )
