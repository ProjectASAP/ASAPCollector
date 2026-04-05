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
    return project_root() / "exathlon" / "data" / "raw"


def out_dir() -> Path:
    return project_root() / "analysis" / "results"


def ensure_dirs() -> tuple[Path, Path]:
    summaries = out_dir() / "summaries"
    detailed = out_dir() / "detailed_windows"
    summaries.mkdir(parents=True, exist_ok=True)
    detailed.mkdir(parents=True, exist_ok=True)
    return summaries, detailed


def list_csv_files() -> list[Path]:
    files = [p for p in sorted(raw_dir().glob("app*/*.csv")) if p.is_file()]
    return files


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


def window_counts(timestamps: list[int], window_sec: int) -> tuple[list[int], list[int], list[int]]:
    if not timestamps:
        return [], [], []
    starts: list[int] = []
    ends: list[int] = []
    counts: list[int] = []
    start = timestamps[0]
    end = start + window_sec
    cnt = 0
    idx = 0
    n = len(timestamps)
    while idx < n:
        t = timestamps[idx]
        if t < end:
            cnt += 1
            idx += 1
            continue
        starts.append(start)
        ends.append(end)
        counts.append(cnt)
        start = end
        end = start + window_sec
        cnt = 0
    starts.append(start)
    ends.append(end)
    counts.append(cnt)
    return starts, ends, counts


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
    detailed_rows: dict[str, list[dict[str, str]]] = {label: [] for _, label in WINDOWS}

    for file_path in list_csv_files():
        ts = load_timestamps(file_path)
        for window_sec, label in WINDOWS:
            starts, ends, counts = window_counts(ts, window_sec)
            if counts:
                avg_samples = statistics.fmean(counts)
                min_samples = min(counts)
                max_samples = max(counts)
                std_samples = statistics.pstdev(counts) if len(counts) > 1 else 0.0
                ge_2 = sum(1 for c in counts if c >= 2)
                ge_10 = sum(1 for c in counts if c >= 10)
                pct_ge_2 = (ge_2 / len(counts)) * 100.0
                pct_ge_10 = (ge_10 / len(counts)) * 100.0
            else:
                avg_samples = float("nan")
                min_samples = 0
                max_samples = 0
                std_samples = float("nan")
                pct_ge_2 = float("nan")
                pct_ge_10 = float("nan")

            summary_rows.append(
                {
                    "file": file_path.name,
                    "window_size": label,
                    "window_seconds": str(window_sec),
                    "total_windows": str(len(counts)),
                    "avg_samples": fmt_float(avg_samples),
                    "min_samples": str(min_samples),
                    "max_samples": str(max_samples),
                    "std_samples": fmt_float(std_samples),
                    "pct_windows_ge_2_samples": fmt_float(pct_ge_2),
                    "pct_windows_ge_10_samples": fmt_float(pct_ge_10),
                }
            )

            for i in range(len(counts)):
                detailed_rows[label].append(
                    {
                        "file": file_path.name,
                        "window_start_t": str(starts[i]),
                        "window_end_t": str(ends[i]),
                        "sample_count": str(counts[i]),
                    }
                )

    write_csv(
        summaries_dir / "window_summary.csv",
        [
            "file",
            "window_size",
            "window_seconds",
            "total_windows",
            "avg_samples",
            "min_samples",
            "max_samples",
            "std_samples",
            "pct_windows_ge_2_samples",
            "pct_windows_ge_10_samples",
        ],
        summary_rows,
    )
    for _, label in WINDOWS:
        write_csv(
            detailed_dir / f"window_details_{label}.csv",
            ["file", "window_start_t", "window_end_t", "sample_count"],
            detailed_rows[label],
        )


if __name__ == "__main__":
    main()
