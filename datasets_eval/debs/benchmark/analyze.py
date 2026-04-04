from __future__ import annotations

import argparse
import csv
from pathlib import Path

import numpy as np
import pandas as pd


def load_send_times_csv(path: Path) -> pd.DataFrame:
    if not path.is_file():
        return pd.DataFrame()
    return pd.read_csv(path)


def append_latency_row(
    results_dir: Path,
    query_label: str,
    day_label: str,
    replay_mode: str,
    send_times: pd.DataFrame,
) -> None:
    latency_path = results_dir / "latency.csv"
    header = (
        "query,day,replay_mode,p50_inter_arrival_ms,p95_inter_arrival_ms,"
        "p99_inter_arrival_ms,p50_send_lag_ms,p99_send_lag_ms\n"
    )
    if send_times.empty or len(send_times) < 3:
        row = [query_label, day_label, replay_mode, "", "", "", "", ""]
    else:
        ordered = send_times.sort_values("event_time_ns")
        event_ns = ordered["event_time_ns"].astype(np.int64)
        deltas_ms = np.diff(event_ns) / 1e6
        emit_ns = ordered["emit_wall_ns"].astype(np.int64)
        wall_rel = emit_ns - int(emit_ns.iloc[0])
        event_rel = event_ns - int(event_ns.iloc[0])
        lag_ms = (wall_rel.to_numpy(dtype=np.float64) - event_rel.to_numpy(dtype=np.float64)) / 1e6
        row = [
            query_label,
            day_label,
            replay_mode,
            f"{float(np.percentile(deltas_ms, 50)):.6f}",
            f"{float(np.percentile(deltas_ms, 95)):.6f}",
            f"{float(np.percentile(deltas_ms, 99)):.6f}",
            f"{float(np.percentile(lag_ms, 50)):.6f}",
            f"{float(np.percentile(lag_ms, 99)):.6f}",
        ]
    if not latency_path.is_file():
        latency_path.write_text(header, encoding="utf-8")
    with open(latency_path, "a", newline="", encoding="utf-8") as latency_file:
        csv.writer(latency_file).writerow(row)


def main() -> None:
    parser = argparse.ArgumentParser(description="Aggregate benchmark results into report.md.")
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "results",
    )
    parser.add_argument("--query", default="", help="Tag for latency row (optional).")
    parser.add_argument("--day", default="", help="Tag for latency row (optional).")
    parser.add_argument(
        "--replay-mode",
        default="",
        dest="replay_mode",
        help="Replay mode tag for latency row (e.g. max, scaled, paced).",
    )
    args = parser.parse_args()

    comparison_dir = args.results_dir / "comparison"
    send_times_path = args.results_dir / "send_times.csv"
    throughput_path = args.results_dir / "throughput.csv"
    report_path = args.results_dir / "report.md"

    send_times = load_send_times_csv(send_times_path)
    query_tag = args.query or "unknown"
    day_tag = args.day or "unknown"
    mode_tag = args.replay_mode or "unknown"
    append_latency_row(args.results_dir, query_tag, day_tag, mode_tag, send_times)

    lines: list[str] = ["# DEBS benchmark report", ""]

    comparison_frames: list[pd.DataFrame] = []
    if comparison_dir.is_dir():
        for csv_file in sorted(comparison_dir.glob("*.csv")):
            try:
                comparison_frames.append(pd.read_csv(csv_file))
            except OSError:
                pass
    if comparison_frames:
        accuracy = pd.concat(comparison_frames, ignore_index=True)
        lines.append("## Accuracy (sketch vs ground truth)")
        lines.append("| query | day | metric | value | threshold | pass |")
        lines.append("| --- | --- | --- | --- | --- | --- |")
        for _, record in accuracy.iterrows():
            lines.append(
                f"| {record.get('query', '')} | {record.get('day', '')} | "
                f"{record.get('metric', '')} | {record.get('value', '')} | "
                f"{record.get('threshold', '')} | {record.get('pass', '')} |"
            )
        lines.append("")

    if throughput_path.is_file():
        throughput_df = pd.read_csv(throughput_path)
        lines.append("## Throughput (replay)")
        lines.append(throughput_df.to_csv(index=False))
        lines.append("")

    if send_times.empty or len(send_times) < 3:
        lines.append("## Latency (send_times.csv)")
        lines.append("_Insufficient rows in send_times.csv for latency stats._")
        lines.append("")
    else:
        ordered = send_times.sort_values("event_time_ns")
        event_ns = ordered["event_time_ns"].astype(np.int64)
        deltas_ms = np.diff(event_ns) / 1e6
        emit_ns = ordered["emit_wall_ns"].astype(np.int64)
        wall_rel = emit_ns - int(emit_ns.iloc[0])
        event_rel = event_ns - int(event_ns.iloc[0])
        lag_ms = (wall_rel.to_numpy(dtype=np.float64) - event_rel.to_numpy(dtype=np.float64)) / 1e6
        lines.append("## Latency (send_times.csv, this run)")
        lines.append("| stat | ms |")
        lines.append("| --- | --- |")
        lines.append(f"| p50_delta_event | {float(np.percentile(deltas_ms, 50)):.6f} |")
        lines.append(f"| p95_delta_event | {float(np.percentile(deltas_ms, 95)):.6f} |")
        lines.append(f"| p99_delta_event | {float(np.percentile(deltas_ms, 99)):.6f} |")
        lines.append(f"| p50_send_lag | {float(np.percentile(lag_ms, 50)):.6f} |")
        lines.append(f"| p99_send_lag | {float(np.percentile(lag_ms, 99)):.6f} |")
        lines.append("")

    latency_aggregate = args.results_dir / "latency.csv"
    if latency_aggregate.is_file():
        lines.append("## Latency history (latency.csv)")
        lines.append(pd.read_csv(latency_aggregate).to_csv(index=False))
        lines.append("")

    lines.append("## Notes")
    lines.append(
        "- For sketch-finance vs throughput-only runs, compare accuracy rows only for "
        "queries with ground truth (Q1, Q3–Q8)."
    )
    lines.append(
        "- End-to-end scrape timing is bounded by scrape interval; see collector logs "
        "for finer-grained export latency if needed."
    )

    report_path.write_text("\n".join(lines), encoding="utf-8")
    print(report_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    main()
