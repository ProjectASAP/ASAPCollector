from __future__ import annotations

import numpy as np

from utils import (
    WINDOW_LABELS,
    WINDOW_SIZES_MS,
    ensure_dirs,
    group_window_summary_by_size,
    list_csv_files,
    load_trading_event_timestamps_ms_utc,
    parse_dataset_and_configure,
    pivot_window_summary_rows,
    results_dir,
    segment_by_window_cest,
    summarize_counts,
    window_sample_counts,
    window_summary_pivot_fieldnames,
    window_summary_sorted_long,
    write_csv_rows,
)


def main() -> None:
    parse_dataset_and_configure()
    ensure_dirs()
    summ_path = results_dir() / "summaries" / "window_summary.csv"
    pivot_path = results_dir() / "summaries" / "window_summary_pivoted.csv"
    by_size_path = results_dir() / "summaries" / "window_summary_by_window_size.csv"
    sum_rows: list[dict] = []
    detail_paths = {
        lbl: results_dir() / "detailed_windows" / f"window_details_{lbl}.csv"
        for lbl in WINDOW_LABELS
    }
    detail_rows: dict[str, list[dict]] = {lbl: [] for lbl in WINDOW_LABELS}
    for path in list_csv_files():
        name = path.name
        ts = load_trading_event_timestamps_ms_utc(path)
        if ts.size == 0:
            for w_lbl in WINDOW_LABELS:
                sum_rows.append(
                    {
                        "file": name,
                        "window_size": w_lbl,
                        "total_windows": 0,
                        "avg_samples": float("nan"),
                        "min_samples": float("nan"),
                        "max_samples": float("nan"),
                        "std_samples": float("nan"),
                    }
                )
            continue
        ts_s = np.sort(ts)
        for w_ms, w_lbl in zip(WINDOW_SIZES_MS, WINDOW_LABELS):
            starts, ends, segs = segment_by_window_cest(ts_s, w_ms)
            cnts = window_sample_counts(segs)
            sm = summarize_counts(cnts)
            sum_rows.append(
                {
                    "file": name,
                    "window_size": w_lbl,
                    "total_windows": int(sm["total_windows"]),
                    "avg_samples": sm["avg_samples"],
                    "min_samples": sm["min_samples"],
                    "max_samples": sm["max_samples"],
                    "std_samples": sm["std_samples"],
                }
            )
            for i in range(len(segs)):
                detail_rows[w_lbl].append(
                    {
                        "file": name,
                        "window_start_utc_ms": int(starts[i]),
                        "window_end_utc_ms": int(ends[i]),
                        "sample_count": int(cnts[i]),
                    }
                )
    sum_long = window_summary_sorted_long(sum_rows)
    write_csv_rows(
        summ_path,
        (
            "file",
            "window_size",
            "total_windows",
            "avg_samples",
            "min_samples",
            "max_samples",
            "std_samples",
        ),
        sum_long,
    )
    pivot_rows = pivot_window_summary_rows(sum_rows)
    write_csv_rows(pivot_path, window_summary_pivot_fieldnames(), pivot_rows)
    grouped_rows = group_window_summary_by_size(sum_rows)
    write_csv_rows(
        by_size_path,
        (
            "window_size",
            "file_count",
            "files_with_windows",
            "sum_total_windows",
            "mean_avg_samples",
            "min_min_samples",
            "max_max_samples",
            "mean_std_samples",
        ),
        grouped_rows,
    )
    for lbl in WINDOW_LABELS:
        write_csv_rows(
            detail_paths[lbl],
            ("file", "window_start_utc_ms", "window_end_utc_ms", "sample_count"),
            detail_rows[lbl],
        )


if __name__ == "__main__":
    main()
