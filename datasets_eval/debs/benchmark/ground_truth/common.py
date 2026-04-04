from __future__ import annotations

import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

_BENCHMARK_ROOT = Path(__file__).resolve().parent.parent
if str(_BENCHMARK_ROOT) not in sys.path:
    sys.path.insert(0, str(_BENCHMARK_ROOT))

from common import DEBS_TZ, data_path, day_to_filename

WINDOW_5MIN_MS = 300_000
WINDOW_15MIN_MS = 900_000

ALPHA_EMA_38 = 2.0 / 39.0
ALPHA_EMA_100 = 2.0 / 101.0
ALPHA_EMA_12 = 2.0 / 13.0
ALPHA_EMA_26 = 2.0 / 27.0
ALPHA_MACD_SIGNAL = 2.0 / 10.0
RSI_PERIOD = 14


def split_symbol_exchange(ids: pd.Series) -> tuple[pd.Series, pd.Series]:
    string_ids = ids.astype("string").str.strip()
    parts = string_ids.str.rsplit(".", n=1, expand=True)
    if parts.shape[1] == 0:
        empty = pd.Series(pd.array([], dtype="string"))
        return empty, empty
    symbol = parts.iloc[:, 0]
    if parts.shape[1] > 1:
        exchange = parts.iloc[:, 1].fillna("")
    else:
        exchange = pd.Series(
            pd.array([""] * len(symbol), dtype="string"), index=symbol.index
        )
    return symbol, exchange


def window_start_ms_vectorized(
    timestamps_ms: np.ndarray, window_ms: int
) -> np.ndarray:
    delta = pd.Timedelta(milliseconds=window_ms)
    datetime_index = pd.to_datetime(timestamps_ms, unit="ms", utc=True).tz_convert(
        DEBS_TZ
    )
    floored = datetime_index.floor(delta)
    floored_utc = floored.tz_convert("UTC")
    return (floored_utc.asi8 // 1_000_000).astype(np.int64)


def load_filtered_day(
    csv_path: Path,
    chunksize: int,
    max_event_minutes: int | None = None,
) -> pd.DataFrame:
    columns = ["ID", "SecType", "Date", "Last", "Trading time"]
    parts: list[pd.DataFrame] = []
    cutoff_ms: int | None = None
    for chunk in pd.read_csv(
        csv_path,
        comment="#",
        usecols=lambda column: column in columns,
        chunksize=chunksize,
        dtype=object,
        low_memory=False,
    ):
        chunk = chunk.copy()
        chunk["Date"] = chunk["Date"].astype(str).str.strip()
        chunk["Trading time"] = chunk["Trading time"].astype(str).str.strip()
        raw = pd.to_datetime(
            chunk["Date"] + " " + chunk["Trading time"],
            dayfirst=True,
            errors="coerce",
            format="mixed",
        )
        localized = raw.dt.tz_localize(
            DEBS_TZ, ambiguous=True, nonexistent="shift_forward"
        )
        chunk["ts_ms"] = (localized.astype("int64") // 1_000_000).astype(np.int64)
        chunk["Last"] = pd.to_numeric(chunk["Last"], errors="coerce")
        chunk = chunk[chunk["ts_ms"] > 0].dropna(subset=["Last"])
        if chunk.empty:
            continue
        if max_event_minutes is not None:
            chunk_min = int(chunk["ts_ms"].min())
            if cutoff_ms is None:
                cutoff_ms = chunk_min + max_event_minutes * 60_000
            chunk = chunk[chunk["ts_ms"] < cutoff_ms]
            if chunk.empty:
                # Chunk may be entirely after cutoff in file order; later chunks
                # can still contain rows within the window (CSV is not sorted by time).
                continue
        symbol_series, exchange_series = split_symbol_exchange(chunk["ID"])
        chunk["symbol"] = symbol_series
        chunk["exchange"] = exchange_series
        chunk["sectype"] = chunk["SecType"].astype(str).str.strip()
        parts.append(chunk[["symbol", "exchange", "sectype", "ts_ms", "Last"]])
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True)


def load_full_feed_day(
    csv_path: Path,
    chunksize: int,
    max_event_minutes: int | None = None,
) -> pd.DataFrame:
    columns = ["ID", "SecType", "Date", "Time", "Last"]
    parts: list[pd.DataFrame] = []
    cutoff_ms: int | None = None
    for chunk in pd.read_csv(
        csv_path,
        comment="#",
        index_col=False,
        usecols=lambda column: column in columns,
        chunksize=chunksize,
        dtype=object,
        low_memory=False,
    ):
        chunk = chunk.copy()
        raw = pd.to_datetime(
            chunk["Date"].astype(str).str.strip()
            + " "
            + chunk["Time"].astype(str).str.strip(),
            dayfirst=True,
            errors="coerce",
            format="mixed",
        )
        localized = raw.dt.tz_localize(
            DEBS_TZ, ambiguous=True, nonexistent="shift_forward"
        )
        chunk["ts_ms"] = (localized.astype("int64") // 1_000_000).astype(np.int64)
        chunk["Last"] = pd.to_numeric(chunk["Last"], errors="coerce")
        chunk = chunk[chunk["ts_ms"] > 0]
        if chunk.empty:
            continue
        if max_event_minutes is not None:
            chunk_min = int(chunk["ts_ms"].min())
            if cutoff_ms is None:
                cutoff_ms = chunk_min + max_event_minutes * 60_000
            chunk = chunk[chunk["ts_ms"] < cutoff_ms]
            if chunk.empty:
                continue
        chunk["symbol"], _ = split_symbol_exchange(chunk["ID"])
        parts.append(chunk[["symbol", "ts_ms", "Last"]])
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True)


def day_tag_from_arg(day: str) -> str:
    tag = day.strip()
    if tag.endswith(".csv"):
        tag = tag[:-4]
    return tag.replace("debs2022-gc-trading-day-", "")


def log_phase(query: str, day_tag: str, message: str, **extra: object) -> None:
    parts = [f"ground_truth {query} {day_tag}", message]
    for key, value in extra.items():
        parts.append(f"{key}={value}")
    print(" ".join(parts), file=sys.stderr, flush=True)


def timed_load(
    label: str,
    day_tag: str,
    path: Path,
    loader,
    chunksize: int,
) -> tuple[pd.DataFrame, float]:
    log_phase(label, day_tag, "load", path=str(path))
    start = time.perf_counter()
    dataframe = loader(path, chunksize)
    elapsed = time.perf_counter() - start
    log_phase(label, day_tag, "loaded", rows=len(dataframe), seconds=f"{elapsed:.1f}")
    return dataframe, elapsed
