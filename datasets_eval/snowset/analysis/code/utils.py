from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

import numpy as np
import pandas as pd

ANALYSIS_ROOT = Path(__file__).resolve().parent.parent
SNOWSET_ROOT = ANALYSIS_ROOT.parent
DATA_ROOT = SNOWSET_ROOT / "data"
WINDOW_SIZES_MS = (60_000, 300_000, 900_000, 1_800_000, 3_600_000)
WINDOW_LABELS = ("1min", "5min", "15min", "30min", "1hour")

_selected_dataset: str = "all"


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    path: Path
    time_column: str
    entity_columns: tuple[str, ...]


DATASETS: tuple[DatasetSpec, ...] = (
    DatasetSpec(
        name="snowset-main",
        path=DATA_ROOT / "snowset-main.parquet",
        time_column="createdTime",
        entity_columns=("queryId", "warehouseId", "databaseId"),
    ),
)


def set_dataset(name: str) -> None:
    global _selected_dataset
    valid = {"snowset-main", "all"}
    if name not in valid:
        raise ValueError(f"--dataset must be one of {sorted(valid)}, got {name!r}")
    _selected_dataset = name


def get_dataset() -> str:
    return _selected_dataset


def add_dataset_arg(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--dataset",
        choices=("snowset-main", "all"),
        default="all",
        help="Which dataset to analyse. Default: all.",
    )


def add_delay_arg(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--delay",
        type=float,
        default=0.0,
        metavar="SECONDS",
        help="Sleep this many seconds between window iterations to reduce CPU/memory pressure. Default: 0.",
    )


def parse_dataset_and_configure(argv: Sequence[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    add_dataset_arg(p)
    add_delay_arg(p)
    args = p.parse_args(argv)
    set_dataset(args.dataset)
    return args


def ensure_dirs() -> None:
    root = results_dir()
    (root / "summaries").mkdir(parents=True, exist_ok=True)
    (root / "detailed_windows").mkdir(parents=True, exist_ok=True)


def results_dir() -> Path:
    subdir = _selected_dataset if _selected_dataset != "all" else "all"
    return ANALYSIS_ROOT / "results" / subdir


def dataset_specs() -> tuple[DatasetSpec, ...]:
    if _selected_dataset == "all":
        return DATASETS
    return tuple(d for d in DATASETS if d.name == _selected_dataset)


def read_parquet(path: Path, columns: Sequence[str] | None = None) -> pd.DataFrame:
    return pd.read_parquet(path, columns=list(columns) if columns else None)


def timestamps_ms_utc(series: pd.Series) -> np.ndarray:
    ts = pd.to_datetime(series, errors="coerce", utc=True)
    ts = ts.dropna()
    if ts.empty:
        return np.array([], dtype=np.int64)
    return (ts.astype("int64") // 1_000_000).to_numpy(dtype=np.int64)


def diffs_ms_sorted(ts_ms: np.ndarray) -> np.ndarray:
    if ts_ms.size < 2:
        return np.array([], dtype=np.int64)
    return np.diff(np.sort(ts_ms)).astype(np.int64)


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


def segment_by_window_utc(
    ts_sorted: np.ndarray, window_ms: int
) -> tuple[np.ndarray, np.ndarray, list[np.ndarray]]:
    if ts_sorted.size == 0:
        return np.array([], dtype=np.int64), np.array([], dtype=np.int64), []
    td = pd.Timedelta(milliseconds=window_ms)
    s = pd.to_datetime(ts_sorted, unit="ms", utc=True)
    floored = s.floor(td)
    keys_ms = (floored.astype("int64") // 1_000_000).to_numpy(dtype=np.int64)
    change_idx = np.flatnonzero(np.diff(keys_ms) != 0) + 1
    segments = np.split(ts_sorted, change_idx)
    mask = np.r_[True, keys_ms[1:] != keys_ms[:-1]]
    starts = keys_ms[mask].astype(np.int64)
    ends = starts + window_ms
    return starts, ends, segments


def window_sample_counts(segments: list[np.ndarray]) -> np.ndarray:
    return np.array([len(segment) for segment in segments], dtype=np.int64)


def window_mean_interarrival_ms(segments: list[np.ndarray]) -> np.ndarray:
    means = np.empty(len(segments), dtype=np.float64)
    for idx, segment in enumerate(segments):
        if segment.size < 2:
            means[idx] = float("nan")
        else:
            means[idx] = float(np.mean(np.diff(segment)))
    return means


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
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
