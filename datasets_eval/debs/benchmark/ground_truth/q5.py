from __future__ import annotations

import math
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


def _sigma_iqr_from_prices(prices: np.ndarray) -> float:
    """σ ≈ IQR/1.349 on prices in the window (matches DDSketch input, not log returns)."""
    if len(prices) < 2:
        return float("nan")
    p = prices.astype(np.float64, copy=False)
    q1, q3 = np.quantile(p, (0.25, 0.75))
    return float((q3 - q1) / 1.349)


def compute_q5_sigma(dataframe: pd.DataFrame) -> pd.DataFrame:
    ts = dataframe["ts_ms"].to_numpy(dtype=np.int64, copy=False)
    ws = window_start_ms_vectorized(ts, WINDOW_5MIN_MS)
    tmp = dataframe.assign(window_start_ms=ws)
    rows: list[dict] = []
    for (symbol, wstart), group in tmp.groupby(["symbol", "window_start_ms"], sort=False):
        group = group.sort_values("ts_ms")
        prices = group["Last"].to_numpy(dtype=np.float64, copy=False)
        if len(prices) < 2:
            continue
        positive_pairs = (prices[:-1] > 0) & (prices[1:] > 0)
        log_returns = np.log(prices[1:][positive_pairs]) - np.log(prices[:-1][positive_pairs])
        n = len(log_returns)
        if n < 2:
            continue
        sigma = float(np.std(log_returns, ddof=1))
        if not math.isfinite(sigma) or sigma <= 0:
            continue
        sigma_iqr = _sigma_iqr_from_prices(prices)
        if not math.isfinite(sigma_iqr):
            continue
        ref_price = float(np.median(prices))
        rows.append(
            {
                "symbol": symbol,
                "window_start_ms": int(wstart),
                "sigma": sigma,
                "sigma_iqr": sigma_iqr,
                "ref_price": ref_price,
                "n": n,
            }
        )
    return pd.DataFrame(rows)


def run_q5(day: str, output_root: Path, chunksize: int) -> None:
    day_tag = day_tag_from_arg(day)
    csv_path = data_path("data_filtered") / day_to_filename(day)
    sub = output_root / "Q5"
    sub.mkdir(parents=True, exist_ok=True)

    dataframe, _ = timed_load("Q5", day_tag, csv_path, load_filtered_day, chunksize)
    if dataframe.empty:
        log_phase("Q5", day_tag, "skip empty")
        return

    log_phase("Q5", day_tag, "compute")
    compute_start = time.perf_counter()
    out_path = sub / f"{day_tag}.csv"
    compute_q5_sigma(dataframe).to_csv(out_path, index=False)
    log_phase("Q5", day_tag, "done", out=str(out_path), seconds=f"{time.perf_counter() - compute_start:.1f}")
