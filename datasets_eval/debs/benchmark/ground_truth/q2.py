from __future__ import annotations

import functools
import time
from pathlib import Path

import numpy as np
import pandas as pd

from ground_truth.common import day_tag_from_arg, load_filtered_day, log_phase, timed_load
from ground_truth.q1 import compute_q1_ema
from common import data_path, day_to_filename


def compute_q2_crossover(q1_dataframe: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    for symbol, group in q1_dataframe.groupby("symbol"):
        group = group.sort_values("window_start_ms")
        ema38 = group["ema38"].astype(float).to_numpy()
        ema100 = group["ema100"].astype(float).to_numpy()
        window_ms = group["window_start_ms"].to_numpy(dtype=np.int64, copy=False)
        previous_diff: float | None = None
        for index in range(len(group)):
            diff = float(ema38[index]) - float(ema100[index])
            if previous_diff is not None:
                if previous_diff <= 0 < diff:
                    rows.append(
                        {
                            "symbol": symbol,
                            "window_start_ms": int(window_ms[index]),
                            "signal": "bullish",
                        }
                    )
                elif previous_diff >= 0 > diff:
                    rows.append(
                        {
                            "symbol": symbol,
                            "window_start_ms": int(window_ms[index]),
                            "signal": "bearish",
                        }
                    )
            previous_diff = diff
    return pd.DataFrame(rows)


def run_q2_only(
    day: str,
    output_root: Path,
    chunksize: int,
    max_event_minutes: int | None = None,
) -> None:
    day_tag = day_tag_from_arg(day)
    csv_path = data_path("data_filtered") / day_to_filename(day)
    output_root.mkdir(parents=True, exist_ok=True)
    (output_root / "Q2").mkdir(parents=True, exist_ok=True)

    loader_fn = functools.partial(load_filtered_day, max_event_minutes=max_event_minutes)
    dataframe, _ = timed_load("Q2", day_tag, csv_path, loader_fn, chunksize)
    if dataframe.empty:
        log_phase("Q2", day_tag, "skip empty")
        return

    log_phase("Q2", day_tag, "compute")
    compute_start = time.perf_counter()
    q1 = compute_q1_ema(dataframe)
    path_q2 = output_root / "Q2" / f"{day_tag}.csv"
    compute_q2_crossover(q1).to_csv(path_q2, index=False)
    log_phase(
        "Q2",
        day_tag,
        "done",
        out=str(path_q2),
        seconds=f"{time.perf_counter() - compute_start:.1f}",
    )


def run_q1q2(
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
    (output_root / "Q2").mkdir(parents=True, exist_ok=True)

    loader_fn = functools.partial(load_filtered_day, max_event_minutes=max_event_minutes)
    dataframe, _ = timed_load("Q1Q2", day_tag, csv_path, loader_fn, chunksize)
    if dataframe.empty:
        log_phase("Q1Q2", day_tag, "skip empty")
        return

    log_phase("Q1Q2", day_tag, "compute")
    compute_start = time.perf_counter()
    q1 = compute_q1_ema(dataframe)
    path_q1 = output_root / "Q1" / f"{day_tag}.csv"
    path_q2 = output_root / "Q2" / f"{day_tag}.csv"
    q1.to_csv(path_q1, index=False)
    compute_q2_crossover(q1).to_csv(path_q2, index=False)
    log_phase(
        "Q1Q2",
        day_tag,
        "done",
        out1=str(path_q1),
        out2=str(path_q2),
        seconds=f"{time.perf_counter() - compute_start:.1f}",
    )
