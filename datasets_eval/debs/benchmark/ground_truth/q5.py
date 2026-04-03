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


def compute_q5_realized_volatility(dataframe: pd.DataFrame) -> pd.DataFrame:
    working = dataframe.copy()
    working["ws"] = window_start_ms_vectorized(
        working["ts_ms"].to_numpy(dtype=np.int64, copy=False), WINDOW_5MIN_MS
    )
    rows: list[dict] = []
    for (symbol, window_start), group in working.groupby(["symbol", "ws"]):
        group = group.sort_values("ts_ms")
        prices = group["Last"].astype(float).values
        if len(prices) < 2:
            continue
        positive_pairs = (prices[:-1] > 0) & (prices[1:] > 0)
        log_returns = np.log(prices[1:][positive_pairs]) - np.log(prices[:-1][positive_pairs])
        if len(log_returns) < 2:
            continue
        sigma = float(np.std(log_returns, ddof=1))
        rows.append({"symbol": symbol, "window_start_ms": int(window_start), "sigma": sigma})
    return pd.DataFrame(rows)


def run_q5(
    day: str,
    output_root: Path,
    chunksize: int,
    max_event_minutes: int | None = None,
) -> None:
    day_tag = day_tag_from_arg(day)
    csv_path = data_path("data_filtered") / day_to_filename(day)
    sub = output_root / "Q5"
    sub.mkdir(parents=True, exist_ok=True)

    loader_fn = functools.partial(load_filtered_day, max_event_minutes=max_event_minutes)
    dataframe, _ = timed_load("Q5", day_tag, csv_path, loader_fn, chunksize)
    if dataframe.empty:
        log_phase("Q5", day_tag, "skip empty")
        return

    log_phase("Q5", day_tag, "compute")
    compute_start = time.perf_counter()
    out_path = sub / f"{day_tag}.csv"
    compute_q5_realized_volatility(dataframe).to_csv(out_path, index=False)
    log_phase("Q5", day_tag, "done", out=str(out_path), seconds=f"{time.perf_counter() - compute_start:.1f}")
