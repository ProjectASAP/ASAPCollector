from __future__ import annotations

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


def compute_q3_topk(dataframe: pd.DataFrame, top_k: int = 10) -> pd.DataFrame:
    working = dataframe.copy()
    working["ws"] = window_start_ms_vectorized(
        working["ts_ms"].to_numpy(dtype=np.int64, copy=False), WINDOW_5MIN_MS
    )
    counts = working.groupby(["ws", "symbol"]).size().reset_index(name="n")
    range_scores = (
        working.dropna(subset=["Last"])
        .groupby(["ws", "symbol"])["Last"]
        .agg(lambda series: float(series.max() - series.min()))
        .reset_index(name="score")
    )
    rows: list[dict] = []
    for window_start, sub in counts.groupby("ws"):
        top = sub.nlargest(top_k, "n")
        rank = 1
        for sym, nval in zip(top["symbol"].values, top["n"].values):
            rows.append(
                {
                    "window_start_ms": int(window_start),
                    "rank": rank,
                    "symbol": sym,
                    "metric": "count",
                    "value": float(nval),
                }
            )
            rank += 1
    for window_start, sub in range_scores.groupby("ws"):
        top = sub.nlargest(top_k, "score")
        rank = 1
        for sym, score in zip(top["symbol"].values, top["score"].values):
            rows.append(
                {
                    "window_start_ms": int(window_start),
                    "rank": rank,
                    "symbol": sym,
                    "metric": "range",
                    "value": float(score),
                }
            )
            rank += 1
    return pd.DataFrame(rows)


def run_q3(day: str, output_root: Path, chunksize: int) -> None:
    day_tag = day_tag_from_arg(day)
    full_dir = data_path("data")
    csv_path = full_dir / day_to_filename(day)
    sub = output_root / "Q3"
    sub.mkdir(parents=True, exist_ok=True)

    dataframe, _ = timed_load("Q3", day_tag, csv_path, load_full_feed_day, chunksize)
    if dataframe.empty:
        log_phase("Q3", day_tag, "skip empty")
        return

    log_phase("Q3", day_tag, "compute")
    compute_start = time.perf_counter()
    out_path = sub / f"{day_tag}.csv"
    compute_q3_topk(dataframe).to_csv(out_path, index=False)
    log_phase("Q3", day_tag, "done", out=str(out_path), seconds=f"{time.perf_counter() - compute_start:.1f}")
