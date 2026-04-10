from __future__ import annotations

"""Aggregate replay send-times into latency metrics and produce report.md."""

import argparse
import csv
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


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


def _fmt_num(v: float, decimals: int = 3) -> str:
    if pd.isna(v):
        return "NA"
    return f"{float(v):.{decimals}f}"


def _build_q3_analysis(df: pd.DataFrame) -> str:
    if df.empty:
        return "No per-window Q3 rows were generated."

    pass_count = int(pd.to_numeric(df["pass"], errors="coerce").fillna(0).astype(bool).sum())
    total = len(df)
    rho = pd.to_numeric(df["rank_correlation"], errors="coerce")
    overlap = pd.to_numeric(df["topk_overlap"], errors="coerce")
    q3_score = pd.to_numeric(df["q3_score"], errors="coerce")
    gt_range = pd.to_numeric(df["gt_topk_range"], errors="coerce")

    strong_rank = int((rho >= 0.95).fillna(False).sum())
    early_n = max(total // 2, 1)
    early_avg = float(q3_score.iloc[:early_n].mean()) if total else float("nan")
    late_avg = float(q3_score.iloc[early_n:].mean()) if total > early_n else float("nan")
    range_first = float(gt_range.iloc[0]) if total else float("nan")
    range_last = float(gt_range.iloc[-1]) if total else float("nan")

    parts = [
        f"The CountSketch estimates show strong rank agreement in {strong_rank}/{total} windows "
        f"(Spearman rho >= 0.95), with mean top-K overlap {_fmt_num(overlap.mean())}.",
        f"Using q3_score = overlap + 0.25 * max(rho, 0), the pass rate is {pass_count}/{total} "
        f"windows at the threshold q3_score >= 1.0.",
    ]
    if not np.isnan(early_avg) and not np.isnan(late_avg):
        direction = "declines" if late_avg < early_avg else "improves"
        parts.append(
            f"The average q3_score {direction} from {_fmt_num(early_avg)} in the earlier windows "
            f"to {_fmt_num(late_avg)} in the later windows."
        )
    if not np.isnan(range_first) and not np.isnan(range_last):
        parts.append(
            f"The exact top-10 exceedance spread narrows from {_fmt_num(range_first, 1)} to "
            f"{_fmt_num(range_last, 1)} counts across the run; smaller spreads make near-threshold "
            "top-10 membership more sensitive to one-count estimation error."
        )
    return " ".join(parts)


def write_q3_window_report(results_dir: Path, query_tag: str, file_tag: str) -> None:
    window_csv = results_dir / "window_comparison" / f"{query_tag}_{file_tag}.csv"
    if query_tag != "Q3" or not window_csv.is_file():
        return
    df = pd.read_csv(window_csv)
    if df.empty:
        return

    first_ws = pd.to_numeric(df["window_start_s"], errors="coerce").dropna()
    if first_ws.empty:
        day_label = file_tag
    else:
        dt = datetime.fromtimestamp(int(first_ws.min()), tz=timezone.utc)
        day_label = dt.strftime("%Y-%m-%d UTC")

    lines = [
        f"Results - {day_label}",
        "",
        "| Window | q3_score | Pass |",
        "| --- | --- | --- |",
    ]
    for _, row in df.iterrows():
        mark = "\u2713" if bool(row.get("pass", False)) else "\u2717"
        score = _fmt_num(pd.to_numeric(row.get('q3_score'), errors='coerce'))
        reason = str(row.get("reason", "")).strip()
        if score == "NA" and reason:
            score = f"NA ({reason})"
        lines.append(f"| {row.get('window', '')} | {score} | {mark} |")

    pass_count = int(pd.to_numeric(df["pass"], errors="coerce").fillna(0).astype(bool).sum())
    total = len(df)
    lines += [
        "",
        f"Pass rate: {pass_count}/{total} windows (threshold q3_score >= 1.0).",
        "",
        "Analysis",
        _build_q3_analysis(df),
        "",
        "Notes",
        "- q3_score = topk_overlap + 0.25 * max(rank_correlation, 0).",
        "- topk_overlap is the fraction of exact top-10 metrics also present in the sketch top-10.",
        "- rank_correlation is Spearman rho over the shared keys in the top-10 sets.",
        "",
    ]
    (results_dir / "q3_benchmarking.md").write_text("\n".join(lines), encoding="utf-8")


def _build_q4_analysis(df: pd.DataFrame) -> str:
    if df.empty:
        return "No per-window Q4 rows were generated."

    frac_both = pd.to_numeric(df["frac_minmax_both_lt_2pct"], errors="coerce")
    frac_min = pd.to_numeric(df["frac_min_lt_2pct"], errors="coerce")
    frac_max = pd.to_numeric(df["frac_max_lt_2pct"], errors="coerce")
    pass_count = int(pd.to_numeric(df["pass"], errors="coerce").fillna(0).astype(bool).sum())
    total = len(df)
    sketch_flavor = str(df.get("sketch_flavor", pd.Series(dtype=str)).dropna().iloc[0]) if "sketch_flavor" in df and not df["sketch_flavor"].dropna().empty else "Sketch"

    if pass_count == total and total > 0:
        return (
            f"{sketch_flavor.capitalize()} p0/p100 (min/max) are essentially exact on this data. "
            f"All {total} windows pass the threshold, with median joint pass fraction "
            f"{_fmt_num(frac_both.median())}."
        )

    return (
        f"{sketch_flavor.capitalize()} p0/p100 remain sensitive to endpoint error here. "
        f"The median joint pass fraction is {_fmt_num(frac_both.median())}, with "
        f"median p0-only {_fmt_num(frac_min.median())} and p100-only {_fmt_num(frac_max.median())}."
    )


def write_q4_window_report(results_dir: Path, query_tag: str, file_tag: str) -> None:
    window_csv = results_dir / "window_comparison" / f"{query_tag}_{file_tag}.csv"
    if query_tag != "Q4" or not window_csv.is_file():
        return
    df = pd.read_csv(window_csv)
    if df.empty:
        return

    first_ws = pd.to_numeric(df["window_start_s"], errors="coerce").dropna()
    if first_ws.empty:
        day_label = file_tag
    else:
        dt = datetime.fromtimestamp(int(first_ws.min()), tz=timezone.utc)
        day_label = dt.strftime("%Y-%m-%d UTC")

    total = len(df)
    pass_count = int(pd.to_numeric(df["pass"], errors="coerce").fillna(0).astype(bool).sum())
    window_labels = df["window"].astype(str).tolist()
    span_label = f"{window_labels[0]} to {window_labels[-1]}" if window_labels else "n/a"
    sketch_flavor = str(df["sketch_flavor"].dropna().iloc[0]).capitalize() if "sketch_flavor" in df and not df["sketch_flavor"].dropna().empty else "DDSketch"
    verdict = "Perfect - all windows pass" if pass_count == total and total > 0 else f"Needs work - {pass_count}/{total} windows pass"

    lines = [
        f"Results - {day_label}",
        "",
        f"Sketch: {sketch_flavor}, relative_accuracy=0.01, quantiles [0.0, 1.0]",
        "Metric: fraction of symbols where both sketch p0 and p100 have <= 2% relative error.",
        "",
        "| Windows | Pass rate | Verdict |",
        "| --- | --- | --- |",
        f"| {total} ({span_label}) | {pass_count} / {total} | {verdict} |",
        "",
        "| Window | p0 <= 2% | p100 <= 2% | both <= 2% | Pass |",
        "| --- | --- | --- | --- | --- |",
    ]
    for _, row in df.iterrows():
        mark = "yes" if bool(row.get("pass", False)) else "no"
        lines.append(
            f"| {row.get('window', '')} | {_fmt_num(pd.to_numeric(row.get('frac_min_lt_2pct'), errors='coerce'))} | "
            f"{_fmt_num(pd.to_numeric(row.get('frac_max_lt_2pct'), errors='coerce'))} | "
            f"{_fmt_num(pd.to_numeric(row.get('frac_minmax_both_lt_2pct'), errors='coerce'))} | {mark} |"
        )

    lines += [
        "",
        "Analysis",
        _build_q4_analysis(df),
        "",
    ]
    (results_dir / "q4_benchmarking.md").write_text("\n".join(lines), encoding="utf-8")


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
    parser.add_argument(
        "--send-times",
        type=Path,
        default=None,
        help="Optional per-run send_times.csv path. Defaults to results/send_times.csv.",
    )
    args = parser.parse_args()

    comparison_dir = args.results_dir / "comparison"
    send_times_path = args.send_times or (args.results_dir / "send_times.csv")
    export_diag_path = args.results_dir / "export_diagnostics.csv"
    throughput_path = args.results_dir / "throughput.csv"
    report_path = args.results_dir / "report.md"

    send_times = load_send_times_csv(send_times_path)
    export_diag = load_export_diagnostics_csv(export_diag_path)
    query_tag = args.query or "unknown"
    file_tag = args.file or "unknown"
    mode_tag = args.replay_mode or "unknown"
    append_latency_row(args.results_dir, query_tag, file_tag, mode_tag, send_times)
    write_q3_window_report(args.results_dir, query_tag, file_tag)
    write_q4_window_report(args.results_dir, query_tag, file_tag)

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
        detail_metrics = {"gt_topk_keys", "sketch_topk_keys", "topk_exact_match"}
        numeric_accuracy = accuracy[~accuracy["metric"].isin(detail_metrics)].copy()
        lines.append("## Accuracy (sketch vs ground truth)")
        lines.append("| query | file | metric | value | threshold | pass |")
        lines.append("| --- | --- | --- | --- | --- | --- |")
        for _, record in numeric_accuracy.iterrows():
            lines.append(
                f"| {record.get('query', '')} | {record.get('file', '')} | "
                f"{record.get('metric', '')} | {record.get('value', '')} | "
                f"{record.get('threshold', '')} | {record.get('pass', '')} |"
            )
        lines.append("")

        q3_details = accuracy[accuracy["metric"].isin(detail_metrics)].copy()
        if not q3_details.empty:
            lines.append("## Q3 Top-K Comparison")
            for (query, file_name), group in q3_details.groupby(["query", "file"], dropna=False):
                gt_keys = group.loc[group["metric"] == "gt_topk_keys", "value"]
                sk_keys = group.loc[group["metric"] == "sketch_topk_keys", "value"]
                exact_match = group.loc[group["metric"] == "topk_exact_match", "value"]
                lines.append(f"### {query} / {file_name}")
                lines.append("")
                lines.append(f"- Exact match: {exact_match.iloc[0] if not exact_match.empty else 'False'}")
                lines.append("- Ground truth Top-K:")
                gt_items = [s.strip() for s in str(gt_keys.iloc[0]).split("|")] if not gt_keys.empty else []
                for item in gt_items:
                    if item:
                        lines.append(f"`{item}`")
                lines.append("- Sketch Top-K:")
                sk_items = [s.strip() for s in str(sk_keys.iloc[0]).split("|")] if not sk_keys.empty else []
                for item in sk_items:
                    if item:
                        lines.append(f"`{item}`")
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
