import numpy as np
import pandas as pd
from pathlib import Path

WINDOW_SIZES = [60, 300, 900, 1800, 3600]  # seconds
WINDOW_LABELS = ["1min", "5min", "15min", "30min", "1hour"]


def load_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = df.sort_values("t")
    return df


def get_timestamps(df: pd.DataFrame) -> np.ndarray:
    return df["t"].to_numpy()


def compute_intervals(timestamps: np.ndarray) -> np.ndarray:
    if len(timestamps) < 2:
        return np.array([])
    return np.diff(timestamps)


def stats(arr: np.ndarray):
    if len(arr) == 0:
        return {}

    return {
        "min": float(np.min(arr)),
        "max": float(np.max(arr)),
        "mean": float(np.mean(arr)),
        "median": float(np.median(arr)),
        "p95": float(np.percentile(arr, 95)),
        "p99": float(np.percentile(arr, 99)),
    }


def segment_windows(timestamps: np.ndarray, window_size: int):
    if len(timestamps) == 0:
        return []

    start = timestamps[0]
    windows = []

    current = []
    current_start = start

    for t in timestamps:
        if t < current_start + window_size:
            current.append(t)
        else:
            windows.append(current)
            current = [t]
            current_start = t

    if current:
        windows.append(current)

    return windows


def window_counts(windows):
    return [len(w) for w in windows]


def window_mean_intervals(windows):
    means = []
    for w in windows:
        if len(w) < 2:
            means.append(np.nan)
        else:
            means.append(np.mean(np.diff(w)))
    return means