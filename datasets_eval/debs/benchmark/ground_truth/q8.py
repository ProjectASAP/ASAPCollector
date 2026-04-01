from __future__ import annotations

import time
from pathlib import Path

import numpy as np
import pandas as pd

from ground_truth.common import (
    WINDOW_15MIN_MS,
    day_tag_from_arg,
    load_filtered_day,
    log_phase,
    timed_load,
    window_start_ms_vectorized,
)
from common import data_path, day_to_filename


def compute_q8_anomaly_flags(dataframe: pd.DataFrame) -> pd.DataFrame:
    working = dataframe.copy()
    working["ws"] = window_start_ms_vectorized(
        working["ts_ms"].to_numpy(dtype=np.int64, copy=False), WINDOW_15MIN_MS
    )
    rows: list[dict] = []
    for (symbol, window_start), group in working.groupby(["symbol", "ws"]):
        group = group.sort_values("ts_ms")
        prices = group["Last"].astype(float).values
        if len(prices) < 2:
            continue
        mean_price = float(np.mean(prices))
        std_price = float(np.std(prices, ddof=1))
        if std_price == 0:
            continue
        flag_count = int(np.sum(np.abs((prices - mean_price) / std_price) > 2.5))
        rows.append(
            {
                "symbol": symbol,
                "window_start_ms": int(window_start),
                "mu": mean_price,
                "sigma": std_price,
                "flag_count": flag_count,
                "tick_count": len(prices),
            }
        )
    return pd.DataFrame(rows)


def run_q8(day: str, output_root: Path, chunksize: int) -> None:
    day_tag = day_tag_from_arg(day)
    csv_path = data_path("data_filtered") / day_to_filename(day)
    sub = output_root / "Q8"
    sub.mkdir(parents=True, exist_ok=True)

    dataframe, _ = timed_load("Q8", day_tag, csv_path, load_filtered_day, chunksize)
    if dataframe.empty:
        log_phase("Q8", day_tag, "skip empty")
        return

    log_phase("Q8", day_tag, "compute")
    compute_start = time.perf_counter()
    out_path = sub / f"{day_tag}.csv"
    compute_q8_anomaly_flags(dataframe).to_csv(out_path, index=False)
    log_phase("Q8", day_tag, "done", out=str(out_path), seconds=f"{time.perf_counter() - compute_start:.1f}")
