from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Iterable, Iterator, Sequence

import numpy as np
import pandas as pd

DEBS_ROOT = Path(__file__).resolve().parent.parent
DEBS_TZ = "Europe/Berlin"
BASE_COLS = ("ID", "SecType", "Date", "Time")
BASE_USECOLS = (0, 1, 2, 3)
WINDOW_SIZES_MS = (60_000, 300_000, 900_000, 1_800_000, 3_600_000)
WINDOW_LABELS = ("1min", "5min", "15min", "30min", "1hour")

_dataset_subdir: str = "data"


def set_dataset(subdir: str) -> None:
    global _dataset_subdir
    if subdir not in ("data", "data_filtered"):
        raise ValueError(subdir)
    _dataset_subdir = subdir


def get_dataset() -> str:
    return _dataset_subdir


def data_dir() -> Path:
    return DEBS_ROOT / _dataset_subdir


def results_dir() -> Path:
    return DEBS_ROOT / "results" / _dataset_subdir


def add_dataset_arg(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--dataset",
        choices=("data", "data_filtered"),
        default="data",
        help="Input CSV directory under debs/ and matching results/<name>/",
    )


def parse_dataset_and_configure(argv: Sequence[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    add_dataset_arg(p)
    args = p.parse_args(argv)
    set_dataset(args.dataset)
    return args


def ensure_dirs() -> None:
    r = results_dir()
    (r / "summaries").mkdir(parents=True, exist_ok=True)
    (r / "detailed_windows").mkdir(parents=True, exist_ok=True)


def list_csv_files() -> list[Path]:
    d = data_dir()
    return sorted(p for p in d.glob("*.csv") if p.is_file())


def split_series_symbol_exchange(ids: pd.Series) -> tuple[pd.Series, pd.Series]:
    s = ids.astype("string").str.strip()
    sp = s.str.rsplit(".", n=1, expand=True)
    sym = sp[0]
    if sp.shape[1] > 1:
        exc = sp[1].fillna("")
    else:
        exc = pd.Series(pd.array([""] * len(sym), dtype="string"), index=sym.index)
    return sym, exc


def iter_csv_chunks(
    path: Path, chunksize: int = 1_000_000
) -> Iterator[pd.DataFrame]:
    for chunk in pd.read_csv(
        path,
        comment="#",
        usecols=list(BASE_USECOLS),
        names=list(BASE_COLS),
        header=0,
        index_col=False,
        dtype={"ID": "string", "SecType": "string", "Date": "string", "Time": "string"},
        chunksize=chunksize,
        low_memory=False,
    ):
        yield chunk


def chunk_event_timestamps_ms_utc(chunk: pd.DataFrame) -> np.ndarray:
    d = chunk["Date"].astype("string").str.strip()
    t = chunk["Time"].astype("string").str.strip()
    m = d.notna() & t.notna() & (d != "") & (t != "") & (t.str.lower() != "nan")
    if not m.any():
        return np.array([], dtype=np.int64)
    raw = pd.to_datetime(d[m] + " " + t[m], dayfirst=True, errors="coerce")
    mv = raw.notna()
    if not mv.any():
        return np.array([], dtype=np.int64)
    loc = raw[mv].dt.tz_localize(
        DEBS_TZ, ambiguous="infer", nonexistent="shift_forward"
    )
    ns = loc.astype("int64")
    ok = ns > 0
    return (ns[ok] // 1_000_000).astype(np.int64)


def load_event_timestamps_ms_utc(
    path: Path, chunksize: int = 1_000_000
) -> np.ndarray:
    parts: list[np.ndarray] = []
    for ch in iter_csv_chunks(path, chunksize):
        a = chunk_event_timestamps_ms_utc(ch)
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


def segment_by_window_cest(
    ts_sorted: np.ndarray, window_ms: int
) -> tuple[np.ndarray, np.ndarray, list[np.ndarray]]:
    if ts_sorted.size == 0:
        return np.array([], dtype=np.int64), np.array([], dtype=np.int64), []
    td = pd.Timedelta(milliseconds=window_ms)
    s = pd.to_datetime(ts_sorted, unit="ms", utc=True).tz_convert(DEBS_TZ)
    floored = s.floor(td)
    keys_ms = (
        np.asarray(floored.tz_convert("UTC").astype("int64"), dtype=np.int64)
        // 1_000_000
    )
    ch = np.flatnonzero(np.diff(keys_ms) != 0) + 1
    segs = np.split(ts_sorted, ch)
    mask = np.r_[True, keys_ms[1:] != keys_ms[:-1]]
    starts = keys_ms[mask].astype(np.int64)
    ends = np.empty(len(starts), dtype=np.int64)
    for i, st_ms in enumerate(starts):
        st_loc = pd.Timestamp(int(st_ms), unit="ms", tz="UTC").tz_convert(DEBS_TZ)
        ends[i] = int((st_loc + td).tz_convert("UTC").timestamp() * 1000)
    return starts, ends, segs


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


def window_summary_sorted_long(sum_rows: list[dict]) -> list[dict]:
    if not sum_rows:
        return []
    order = {w: i for i, w in enumerate(WINDOW_LABELS)}

    def key(r: dict) -> tuple[int, str]:
        return (order.get(str(r["window_size"]), 99), str(r["file"]))

    return sorted(sum_rows, key=key)
