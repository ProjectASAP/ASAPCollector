from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable, Iterator, Sequence

import numpy as np
import pandas as pd

BASE_COLS = ("ID", "SecType", "Date", "Time")
BASE_USECOLS = (0, 1, 2, 3)
WINDOW_SIZES_MS = (60_000, 300_000, 900_000, 1_800_000, 3_600_000)
WINDOW_LABELS = ("1min", "5min", "15min", "30min", "1hour")


def data_dir() -> Path:
    return Path(__file__).resolve().parent.parent / "data"


def results_dir() -> Path:
    return Path(__file__).resolve().parent.parent / "results"


def ensure_dirs() -> None:
    r = results_dir()
    (r / "summaries").mkdir(parents=True, exist_ok=True)
    (r / "detailed_windows").mkdir(parents=True, exist_ok=True)


def list_csv_files() -> list[Path]:
    d = data_dir()
    return sorted(p for p in d.glob("*.csv") if p.is_file())


def iter_csv_chunks(
    path: Path, chunksize: int = 1_000_000
) -> Iterator[pd.DataFrame]:
    for chunk in pd.read_csv(
        path,
        comment="#",
        usecols=list(BASE_USECOLS),
        names=list(BASE_COLS),
        header=0,
        dtype={"ID": "string", "SecType": "string", "Date": "string", "Time": "string"},
        chunksize=chunksize,
        low_memory=False,
    ):
        yield chunk


def chunk_timestamps_ms(chunk: pd.DataFrame) -> np.ndarray:
    d = chunk["Date"].astype("string").str.strip()
    t = chunk["Time"].astype("string").str.strip()
    dt = pd.to_datetime(d + " " + t, dayfirst=True, errors="coerce")
    ns = dt.values.astype("int64")
    ok = ns > 0
    out = (ns[ok] // 1_000_000).astype(np.int64)
    return out


def load_timestamps_ms(path: Path, chunksize: int = 1_000_000) -> np.ndarray:
    parts: list[np.ndarray] = []
    for ch in iter_csv_chunks(path, chunksize):
        a = chunk_timestamps_ms(ch)
        if a.size:
            parts.append(a)
    if not parts:
        return np.array([], dtype=np.int64)
    return np.concatenate(parts)


def diffs_ms_sorted(ts_ms: np.ndarray) -> np.ndarray:
    if ts_ms.size < 2:
        return np.array([], dtype=np.int64)
    s = np.sort(ts_ms)
    return np.diff(s).astype(np.int64)


def diff_stats_ms(diffs: np.ndarray) -> dict[str, float]:
    if diffs.size == 0:
        return {
            "min_ms": float("nan"),
            "max_ms": float("nan"),
            "mean_ms": float("nan"),
            "median_ms": float("nan"),
            "p95_ms": float("nan"),
            "p99_ms": float("nan"),
        }
    return {
        "min_ms": float(np.min(diffs)),
        "max_ms": float(np.max(diffs)),
        "mean_ms": float(np.mean(diffs)),
        "median_ms": float(np.median(diffs)),
        "p95_ms": float(np.percentile(diffs, 95)),
        "p99_ms": float(np.percentile(diffs, 99)),
    }


def segment_by_window(ts_sorted: np.ndarray, window_ms: int) -> tuple[np.ndarray, list[np.ndarray]]:
    if ts_sorted.size == 0:
        return np.array([], dtype=np.int64), []
    keys = (ts_sorted // window_ms) * window_ms
    ch = np.flatnonzero(np.diff(keys) != 0) + 1
    segs = np.split(ts_sorted, ch)
    ustarts = np.array([int(seg[0]) // window_ms * window_ms for seg in segs], dtype=np.int64)
    return ustarts, segs


def window_mean_interarrival_ms(segments: list[np.ndarray]) -> np.ndarray:
    out = np.empty(len(segments), dtype=np.float64)
    for i, seg in enumerate(segments):
        if seg.size < 2:
            out[i] = float("nan")
        else:
            out[i] = float(np.mean(np.diff(seg)))
    return out


def window_sample_counts(segments: list[np.ndarray]) -> np.ndarray:
    return np.array([len(s) for s in segments], dtype=np.int64)


def summarize_counts(counts: np.ndarray) -> dict[str, float]:
    if counts.size == 0:
        return {
            "total_windows": 0.0,
            "avg_samples": float("nan"),
            "min_samples": float("nan"),
            "max_samples": float("nan"),
            "std_samples": float("nan"),
        }
    return {
        "total_windows": float(counts.size),
        "avg_samples": float(np.mean(counts)),
        "min_samples": float(np.min(counts)),
        "max_samples": float(np.max(counts)),
        "std_samples": float(np.std(counts)),
    }


def write_csv_rows(path: Path, fieldnames: Sequence[str], rows: Iterable[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(fieldnames))
        w.writeheader()
        for row in rows:
            w.writerow(row)
