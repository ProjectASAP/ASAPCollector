from __future__ import annotations

import csv
import gc
import time

import numpy as np

from utils import (
    WINDOW_LABELS,
    WINDOW_SIZES_MS,
    dataset_specs,
    ensure_dirs,
    parse_dataset_and_configure,
    read_parquet,
    results_dir,
    segment_by_window_utc,
    summarize_counts,
    timestamps_ms_utc,
    window_sample_counts,
    write_csv_rows,
)

_DETAIL_FIELDS = ("dataset", "window_start_utc_ms", "window_end_utc_ms", "sample_count")


def main() -> None:
    args = parse_dataset_and_configure()
    delay: float = args.delay
    ensure_dirs()

    summary_rows: list[dict] = []
    detail_paths = {
        label: results_dir() / "detailed_windows" / f"window_details_{label}.csv"
        for label in WINDOW_LABELS
    }

    # Open all per-label CSV writers upfront so rows are streamed, not accumulated.
    detail_fhs = {label: detail_paths[label].open("w", newline="", encoding="utf-8") for label in WINDOW_LABELS}
    detail_writers = {
        label: csv.DictWriter(fh, fieldnames=list(_DETAIL_FIELDS))
        for label, fh in detail_fhs.items()
    }
    for w in detail_writers.values():
        w.writeheader()

    try:
        for spec in dataset_specs():
            df = read_parquet(spec.path, columns=[spec.time_column])
            ts = timestamps_ms_utc(df[spec.time_column])
            del df
            gc.collect()

            ts_sorted = np.sort(ts)
            del ts
            gc.collect()

            for window_ms, window_label in zip(WINDOW_SIZES_MS, WINDOW_LABELS):
                starts, ends, segments = segment_by_window_utc(ts_sorted, window_ms)
                counts = window_sample_counts(segments)
                stats = summarize_counts(counts)

                summary_rows.append(
                    {
                        "dataset": spec.name,
                        "window_size": window_label,
                        "total_windows": int(stats["total_windows"]),
                        "avg_samples": stats["avg_samples"],
                        "min_samples": stats["min_samples"],
                        "max_samples": stats["max_samples"],
                        "std_samples": stats["std_samples"],
                    }
                )

                w = detail_writers[window_label]
                fh = detail_fhs[window_label]
                for idx in range(len(segments)):
                    w.writerow(
                        {
                            "dataset": spec.name,
                            "window_start_utc_ms": int(starts[idx]),
                            "window_end_utc_ms": int(ends[idx]),
                            "sample_count": int(counts[idx]),
                        }
                    )
                fh.flush()

                del starts, ends, segments, counts
                gc.collect()

                if delay > 0:
                    time.sleep(delay)

            del ts_sorted
            gc.collect()
    finally:
        for fh in detail_fhs.values():
            fh.close()

    write_csv_rows(
        results_dir() / "summaries" / "window_summary.csv",
        (
            "dataset",
            "window_size",
            "total_windows",
            "avg_samples",
            "min_samples",
            "max_samples",
            "std_samples",
        ),
        summary_rows,
    )


if __name__ == "__main__":
    main()
