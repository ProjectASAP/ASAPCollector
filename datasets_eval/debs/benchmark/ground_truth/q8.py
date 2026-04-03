from __future__ import annotations

import functools
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


def compute_q8_stats(dataframe: pd.DataFrame) -> pd.DataFrame:
    ts = dataframe["ts_ms"].to_numpy(dtype=np.int64, copy=False)
    ws = window_start_ms_vectorized(ts, WINDOW_15MIN_MS)
    tmp = dataframe.assign(window_start_ms=ws)
    rows: list[dict] = []
    for (symbol, wstart), group in tmp.groupby(["symbol", "window_start_ms"], sort=False):
        prices = group["Last"].astype(np.float64).to_numpy()
        n = len(prices)
        if n < 2:
            continue
        q1, q3 = np.percentile(prices, [25, 75])
        exact_q1 = float(q1)
        exact_q3 = float(q3)
        exact_iqr = exact_q3 - exact_q1
        mu = float(np.mean(prices))
        sigma = float(np.std(prices, ddof=1))
        n_outliers_z = 0
        if sigma > 0 and np.isfinite(sigma):
            z = np.abs((prices - mu) / sigma)
            n_outliers_z = int(np.sum(z > 2.5))
        rows.append(
            {
                "symbol": symbol,
                "window_start_ms": int(wstart),
                "exact_q1": exact_q1,
                "exact_q3": exact_q3,
                "exact_iqr": exact_iqr,
                "mu": mu,
                "sigma": sigma,
                "n": n,
                "n_outliers_z": n_outliers_z,
            }
        )
    return pd.DataFrame(rows)


def run_q8(
    day: str,
    output_root: Path,
    chunksize: int,
    max_event_minutes: int | None = None,
) -> None:
    day_tag = day_tag_from_arg(day)
    csv_path = data_path("data_filtered") / day_to_filename(day)
    sub = output_root / "Q8"
    sub.mkdir(parents=True, exist_ok=True)

    loader_fn = functools.partial(load_filtered_day, max_event_minutes=max_event_minutes)
    dataframe, _ = timed_load("Q8", day_tag, csv_path, loader_fn, chunksize)
    if dataframe.empty:
        log_phase("Q8", day_tag, "skip empty")
        return

    log_phase("Q8", day_tag, "compute")
    compute_start = time.perf_counter()
    out_path = sub / f"{day_tag}.csv"
    compute_q8_stats(dataframe).to_csv(out_path, index=False)
    log_phase("Q8", day_tag, "done", out=str(out_path), seconds=f"{time.perf_counter() - compute_start:.1f}")
