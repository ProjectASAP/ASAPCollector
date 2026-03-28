from __future__ import annotations

import numpy as np

from utils import (
    WINDOW_LABELS,
    WINDOW_SIZES_MS,
    diff_stats_ms,
    diffs_ms_sorted,
    ensure_dirs,
    list_csv_files,
    load_event_timestamps_ms_utc,
    parse_dataset_and_configure,
    results_dir,
    segment_by_window_cest,
    window_mean_interarrival_ms,
    write_csv_rows,
)


def main() -> None:
    parse_dataset_and_configure()
    ensure_dirs()
    summ_path = results_dir() / "summaries" / "frequency_summary.csv"
    det_path = results_dir() / "detailed_windows" / "frequency_per_window.csv"
    sum_rows: list[dict] = []
    det_rows: list[dict] = []
    for path in list_csv_files():
        name = path.name
        ts = load_event_timestamps_ms_utc(path)
        if ts.size == 0:
            st = diff_stats_ms(np.array([], dtype=np.int64))
            sum_rows.append({"file": name, **st})
            continue
        diffs = diffs_ms_sorted(ts)
        st = diff_stats_ms(diffs)
        sum_rows.append({"file": name, **st})
        ts_s = np.sort(ts)
        for w_ms, w_lbl in zip(WINDOW_SIZES_MS, WINDOW_LABELS):
            starts, ends, segs = segment_by_window_cest(ts_s, w_ms)
            if not segs:
                continue
            means = window_mean_interarrival_ms(segs)
            cnts = np.array([len(s) for s in segs], dtype=np.int64)
            for i in range(len(segs)):
                det_rows.append(
                    {
                        "file": name,
                        "window_size": w_lbl,
                        "window_start_utc_ms": int(starts[i]),
                        "window_end_utc_ms": int(ends[i]),
                        "avg_freq_ms": means[i],
                        "samples_in_window": int(cnts[i]),
                    }
                )
    write_csv_rows(
        summ_path,
        (
            "file",
            "min_ms",
            "max_ms",
            "mean_ms",
            "median_ms",
            "p95_ms",
            "p99_ms",
        ),
        sum_rows,
    )
    write_csv_rows(
        det_path,
        (
            "file",
            "window_size",
            "window_start_utc_ms",
            "window_end_utc_ms",
            "avg_freq_ms",
            "samples_in_window",
        ),
        det_rows,
    )


if __name__ == "__main__":
    main()
