from __future__ import annotations

"""Aggregate replay send-times into latency metrics and produce report.md."""

import argparse
import csv
from pathlib import Path

import numpy as np
import pandas as pd


def accuracy_display(metric: str, value: object) -> str:
    """Return a human-friendly accuracy string for report tables."""
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return ""
    if np.isnan(numeric):
        return "nan"
    if metric.endswith("_err"):
        return f"{max(0.0, 1.0 - numeric) * 100:.2f}%"
    if metric.endswith("_pass") or metric == "overall_pass":
        return f"{numeric * 100:.0f}%"
    return ""


def load_send_times_csv(path: Path) -> pd.DataFrame:
    if not path.is_file():
        return pd.DataFrame()
    return pd.read_csv(path)


def load_export_diagnostics_csv(path: Path) -> pd.DataFrame:
    if not path.is_file():
        return pd.DataFrame()
    return pd.read_csv(path)


def compute_latency_stats(send_times: pd.DataFrame) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Return (inter-arrival-ms, send-lag-ms, send-lag-drift-ms) in emit order."""
    ordered = send_times.sort_values("emit_wall_ns")
    emit_ns = ordered["emit_wall_ns"].astype(np.int64).to_numpy(dtype=np.int64)
    event_ns = ordered["event_time_ns"].astype(np.int64).to_numpy(dtype=np.int64)

    inter_arrival_ms = np.diff(emit_ns) / 1e6

    offsets_ns = emit_ns - event_ns
    # Use minimum observed offset as baseline so lag is non-negative and
    # represents extra delay over the best observed path in this run.
    base_offset_ns = int(np.min(offsets_ns))
    send_lag_ms = (offsets_ns - base_offset_ns) / 1e6

    # Keep first-point anchored drift for debugging instrumentation behavior.
    first_offset_ns = int(offsets_ns[0])
    send_lag_drift_ms = (offsets_ns - first_offset_ns) / 1e6
    return inter_arrival_ms, send_lag_ms, send_lag_drift_ms


def append_latency_row(
    results_dir: Path,
    query_label: str,
    file_label: str,
    replay_mode: str,
    send_times: pd.DataFrame,
) -> None:
    latency_path = results_dir / "latency.csv"
    header = (
        "query,file,replay_mode,p50_inter_arrival_ms,p95_inter_arrival_ms,"
        "p99_inter_arrival_ms,p50_send_lag_ms,p99_send_lag_ms\n"
    )
    if send_times.empty or len(send_times) < 3:
        row = [query_label, file_label, replay_mode, "", "", "", "", ""]
    else:
        deltas_ms, lag_ms, _ = compute_latency_stats(send_times)
        row = [
            query_label,
            file_label,
            replay_mode,
            f"{float(np.percentile(deltas_ms, 50)):.6f}",
            f"{float(np.percentile(deltas_ms, 95)):.6f}",
            f"{float(np.percentile(deltas_ms, 99)):.6f}",
            f"{float(np.percentile(lag_ms, 50)):.6f}",
            f"{float(np.percentile(lag_ms, 99)):.6f}",
        ]
    if not latency_path.is_file():
        latency_path.write_text(header, encoding="utf-8")
    with open(latency_path, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(row)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Aggregate exathlon benchmark results into report.md."
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "results",
    )
    parser.add_argument("--query", default="", help="Tag for latency row.")
    parser.add_argument("--file", default="", help="File tag for latency row.")
    parser.add_argument("--replay-mode", default="", dest="replay_mode")
    args = parser.parse_args()

    comparison_dir = args.results_dir / "comparison"
    send_times_path = args.results_dir / "send_times.csv"
    export_diag_path = args.results_dir / "export_diagnostics.csv"
    throughput_path = args.results_dir / "throughput.csv"
    report_path = args.results_dir / "report.md"

    send_times = load_send_times_csv(send_times_path)
    export_diag = load_export_diagnostics_csv(export_diag_path)
    query_tag = args.query or "unknown"
    file_tag = args.file or "unknown"
    mode_tag = args.replay_mode or "unknown"
    append_latency_row(args.results_dir, query_tag, file_tag, mode_tag, send_times)

    lines: list[str] = ["# Exathlon benchmark report", ""]

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
        # Include per-series columns when present (Q1 per-series output).
        series_cols = [c for c in ("entity", "metric_base", "aggregation")
                       if c in accuracy.columns]
        all_cols = ["query", "file"] + series_cols + ["metric", "value", "accuracy", "threshold", "pass"]
        lines.append("| " + " | ".join(all_cols) + " |")
        lines.append("| " + " | ".join("---" for _ in all_cols) + " |")
        for _, record in accuracy.iterrows():
            cells: list[str] = []
            for c in all_cols:
                if c == "accuracy":
                    cells.append(accuracy_display(str(record.get("metric", "")), record.get("value")))
                else:
                    cells.append(str(record.get(c, "")))
            lines.append("| " + " | ".join(cells) + " |")
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
        deltas_ms, lag_ms, lag_drift_ms = compute_latency_stats(send_times)
        lines.append("## Latency (send_times.csv, this run)")
        lines.append("| stat | ms |")
        lines.append("| --- | --- |")
        lines.append(f"| p50_delta_event | {float(np.percentile(deltas_ms, 50)):.6f} |")
        lines.append(f"| p95_delta_event | {float(np.percentile(deltas_ms, 95)):.6f} |")
        lines.append(f"| p99_delta_event | {float(np.percentile(deltas_ms, 99)):.6f} |")
        lines.append(f"| p50_send_lag | {float(np.percentile(lag_ms, 50)):.6f} |")
        lines.append(f"| p99_send_lag | {float(np.percentile(lag_ms, 99)):.6f} |")
        lines.append(f"| p50_send_lag_drift | {float(np.percentile(lag_drift_ms, 50)):.6f} |")
        lines.append(f"| p99_send_lag_drift | {float(np.percentile(lag_drift_ms, 99)):.6f} |")
        lines.append("")
        lines.append(
            "_`send_lag` uses min-observed wall-event offset as baseline (non-negative, extra delay)._"
        )
        lines.append("")

    if export_diag.empty:
        lines.append("## Export diagnostics (export_diagnostics.csv)")
        lines.append("_No export_diagnostics.csv found._")
        lines.append("")
    else:
        queue_wait_ms = pd.to_numeric(export_diag.get("queue_wait_ms"), errors="coerce").dropna()
        event_span_ms = pd.to_numeric(export_diag.get("event_span_ms"), errors="coerce").dropna()
        event_reg = pd.to_numeric(export_diag.get("event_regressions"), errors="coerce").fillna(0)
        lines.append("## Export diagnostics (export_diagnostics.csv, this run)")
        lines.append("| stat | value |")
        lines.append("| --- | --- |")
        if not queue_wait_ms.empty:
            lines.append(f"| queue_wait_p50_ms | {float(np.percentile(queue_wait_ms, 50)):.6f} |")
            lines.append(f"| queue_wait_p95_ms | {float(np.percentile(queue_wait_ms, 95)):.6f} |")
            lines.append(f"| queue_wait_p99_ms | {float(np.percentile(queue_wait_ms, 99)):.6f} |")
        if not event_span_ms.empty:
            lines.append(f"| event_span_p50_ms | {float(np.percentile(event_span_ms, 50)):.6f} |")
            lines.append(f"| event_span_p95_ms | {float(np.percentile(event_span_ms, 95)):.6f} |")
        lines.append(f"| event_regressions_total | {int(event_reg.sum())} |")
        lines.append(f"| event_regressions_max_export | {int(event_reg.max())} |")
        lines.append("")

    latency_aggregate = args.results_dir / "latency.csv"
    if latency_aggregate.is_file():
        lines.append("## Latency history (latency.csv)")
        lines.append(pd.read_csv(latency_aggregate).to_csv(index=False))
        lines.append("")

    lines.append("## Notes")
    lines.append(
        "- Accuracy rows are only available for queries with ground truth (Q1, Q3–Q9)."
    )
    lines.append(
        "- Q2, Q10, Q11, Q12 are throughput/latency-only (NOP collector path)."
    )
    lines.append(
        "- Threshold per (entity, metric_base) = file-local p95 of non-sentinel values."
    )

    report_path.write_text("\n".join(lines), encoding="utf-8")
    print(report_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    main()
