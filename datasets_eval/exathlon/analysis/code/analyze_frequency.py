from __future__ import annotations

import csv
import math
import statistics
from pathlib import Path

WINDOWS = [
    (60, "1min"),
    (300, "5min"),
    (900, "15min"),
    (1800, "30min"),
    (3600, "1hour"),
]


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def raw_dir() -> Path:
    return project_root() / "MIT_SUPERCLOUD" / "data" / "raw"


def out_dir() -> Path:
    return project_root() / "analysis" / "results"


def ensure_dirs() -> tuple[Path, Path]:
    summaries = out_dir() / "summaries"
    detailed = out_dir() / "detailed_windows"
    summaries.mkdir(parents=True, exist_ok=True)
    detailed.mkdir(parents=True, exist_ok=True)
    return summaries, detailed


def list_csv_files() -> list[Path]:
    return [p for p in sorted(raw_dir().glob("app*/*.csv")) if p.is_file()]


def load_timestamps(path: Path) -> list[int]:
    timestamps: list[int] = []
    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "t" not in reader.fieldnames:
            return timestamps
        for row in reader:
            raw_t = (row.get("t") or "").strip()
            if not raw_t:
                continue
            try:
                timestamps.append(int(float(raw_t)))
            except ValueError:
                continue
    timestamps.sort()
    return timestamps


def diffs(values: list[int]) -> list[int]:
    if len(values) < 2:
        return []
    return [values[i + 1] - values[i] for i in range(len(values) - 1)]


def percentile(values: list[int], p: float) -> float:
    if not values:
        return float("nan")
    if len(values) == 1:
        return float(values[0])
    ordered = sorted(values)
    rank = (len(ordered) - 1) * p
    low = int(math.floor(rank))
    high = int(math.ceil(rank))
    if low == high:
        return float(ordered[low])
    weight = rank - low
    return float(ordered[low] * (1.0 - weight) + ordered[high] * weight)


def window_frequency_stats(timestamps: list[int], window_sec: int) -> list[tuple[int, int, float, int]]:
    if not timestamps:
        return []
    rows: list[tuple[int, int, float, int]] = []
    start = timestamps[0]
    end = start + window_sec
    current: list[int] = []
    idx = 0
    n = len(timestamps)
    while idx < n:
        t = timestamps[idx]
        if t < end:
            current.append(t)
            idx += 1
            continue
        d = diffs(current)
        mean_interval = statistics.fmean(d) if d else float("nan")
        rows.append((start, end, mean_interval, len(current)))
        start = end
        end = start + window_sec
        current = []
    d = diffs(current)
    mean_interval = statistics.fmean(d) if d else float("nan")
    rows.append((start, end, mean_interval, len(current)))
    return rows


def fmt_float(value: float) -> str:
    if math.isnan(value):
        return ""
    return f"{value:.6f}"


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    summaries_dir, detailed_dir = ensure_dirs()
    summary_rows: list[dict[str, str]] = []
    detailed_rows: list[dict[str, str]] = []

    for file_path in list_csv_files():
        ts = load_timestamps(file_path)
        d = diffs(ts)

        min_v = min(d) if d else float("nan")
        max_v = max(d) if d else float("nan")
        mean_v = statistics.fmean(d) if d else float("nan")
        median_v = statistics.median(d) if d else float("nan")
        p95_v = percentile(d, 0.95)
        p99_v = percentile(d, 0.99)
        hz = (1.0 / mean_v) if (d and mean_v > 0.0) else float("nan")

        summary_rows.append(
            {
                "file": file_path.name,
                "samples": str(len(ts)),
                "min_interval_s": fmt_float(min_v),
                "max_interval_s": fmt_float(max_v),
                "mean_interval_s": fmt_float(mean_v),
                "median_interval_s": fmt_float(median_v),
                "p95_interval_s": fmt_float(p95_v),
                "p99_interval_s": fmt_float(p99_v),
                "mean_frequency_hz": fmt_float(hz),
            }
        )

        for window_sec, label in WINDOWS:
            w_rows = window_frequency_stats(ts, window_sec)
            for start, end, avg_interval, samples in w_rows:
                detailed_rows.append(
                    {
                        "file": file_path.name,
                        "window_size": label,
                        "window_seconds": str(window_sec),
                        "window_start_t": str(start),
                        "window_end_t": str(end),
                        "avg_interval_s": fmt_float(avg_interval),
                        "samples_in_window": str(samples),
                    }
                )

    write_csv(
        summaries_dir / "frequency_summary.csv",
        [
            "file",
            "samples",
            "min_interval_s",
            "max_interval_s",
            "mean_interval_s",
            "median_interval_s",
            "p95_interval_s",
            "p99_interval_s",
            "mean_frequency_hz",
        ],
        summary_rows,
    )
    write_csv(
        detailed_dir / "frequency_per_window.csv",
        [
            "file",
            "window_size",
            "window_seconds",
            "window_start_t",
            "window_end_t",
            "avg_interval_s",
            "samples_in_window",
        ],
        detailed_rows,
    )


if __name__ == "__main__":
    main()
