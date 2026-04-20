from __future__ import annotations

import csv
import gc
import time

import numpy as np

from utils import (
    WINDOW_LABELS,
    WINDOW_SIZES_MS,
    dataset_specs,
    diff_stats_ms,
    diffs_ms_sorted,
    ensure_dirs,
    parse_dataset_and_configure,
    read_parquet,
    results_dir,
    segment_by_window_utc,
    timestamps_ms_utc,
    window_mean_interarrival_ms,
    write_csv_rows,
)

_DETAIL_FIELDS = (
    "dataset",
    "window_size",
    "window_start_utc_ms",
    "window_end_utc_ms",
    "avg_freq_ms",
    "samples_in_window",
)


def main() -> None:
    args = parse_dataset_and_configure()
    delay: float = args.delay
    ensure_dirs()

    summary_rows: list[dict] = []
    detail_path = results_dir() / "detailed_windows" / "frequency_per_window.csv"
    detail_path.parent.mkdir(parents=True, exist_ok=True)

    with detail_path.open("w", newline="", encoding="utf-8") as detail_fh:
        writer = csv.DictWriter(detail_fh, fieldnames=list(_DETAIL_FIELDS))
        writer.writeheader()

        for spec in dataset_specs():
            df = read_parquet(spec.path, columns=[spec.time_column])
            ts = timestamps_ms_utc(df[spec.time_column])
            del df
            gc.collect()

            diffs = diffs_ms_sorted(ts)
            stats = diff_stats_ms(diffs)
            del diffs
            gc.collect()
            summary_rows.append({"dataset": spec.name, **stats})

            if ts.size == 0:
                continue
            ts_sorted = np.sort(ts)
            del ts
            gc.collect()

            for window_ms, window_label in zip(WINDOW_SIZES_MS, WINDOW_LABELS):
                starts, ends, segments = segment_by_window_utc(ts_sorted, window_ms)
                means = window_mean_interarrival_ms(segments)
                counts = np.array([len(s) for s in segments], dtype=np.int64)

                for idx in range(len(segments)):
                    writer.writerow(
                        {
                            "dataset": spec.name,
                            "window_size": window_label,
                            "window_start_utc_ms": int(starts[idx]),
                            "window_end_utc_ms": int(ends[idx]),
                            "avg_freq_ms": means[idx],
                            "samples_in_window": int(counts[idx]),
                        }
                    )
                detail_fh.flush()

                del starts, ends, segments, means, counts
                gc.collect()

                if delay > 0:
                    time.sleep(delay)

            del ts_sorted
            gc.collect()

    write_csv_rows(
        results_dir() / "summaries" / "frequency_summary.csv",
        ("dataset", "min_ms", "max_ms", "mean_ms", "median_ms", "p95_ms", "p99_ms"),
        summary_rows,
    )


if __name__ == "__main__":
    main()
