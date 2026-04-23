from __future__ import annotations

import argparse
import csv
from pathlib import Path

import numpy as np
import pandas as pd


def _load_send_times(path: Path) -> pd.DataFrame:
    if not path.is_file():
        return pd.DataFrame()
    return pd.read_csv(path)


def append_latency_row(
    results_dir: Path,
    query: str,
    slice_tag: str,
    replay_mode: str,
    send_times: pd.DataFrame,
) -> None:
    path = results_dir / "latency.csv"
    header = (
        "query,slice,replay_mode,"
        "p50_inter_arrival_ms,p95_inter_arrival_ms,p99_inter_arrival_ms,"
        "p50_send_lag_ms,p99_send_lag_ms\n"
    )
    if send_times.empty or len(send_times) < 3:
        row = [query, slice_tag, replay_mode, "", "", "", "", ""]
    else:
        ordered = send_times.sort_values("event_time_ns")
        event_ns = ordered["event_time_ns"].astype(np.int64)
        deltas_ms = np.diff(event_ns) / 1e6
        emit_ns = ordered["emit_wall_ns"].astype(np.int64)
        wall_rel = emit_ns - int(emit_ns.iloc[0])
        event_rel = event_ns - int(event_ns.iloc[0])
        lag_ms = (wall_rel.to_numpy(np.float64) - event_rel.to_numpy(np.float64)) / 1e6
        row = [
            query, slice_tag, replay_mode,
            f"{float(np.percentile(deltas_ms, 50)):.6f}",
            f"{float(np.percentile(deltas_ms, 95)):.6f}",
            f"{float(np.percentile(deltas_ms, 99)):.6f}",
            f"{float(np.percentile(lag_ms, 50)):.6f}",
            f"{float(np.percentile(lag_ms, 99)):.6f}",
        ]
    if not path.is_file():
        path.write_text(header, encoding="utf-8")
    with open(path, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(row)


def main() -> None:
    parser = argparse.ArgumentParser(description="Aggregate Snowset benchmark results.")
    parser.add_argument("--results-dir", type=Path,
                        default=Path(__file__).resolve().parent / "results")
    parser.add_argument("--query", default="")
    parser.add_argument("--slice", default="")
    parser.add_argument("--replay-mode", default="", dest="replay_mode")
    args = parser.parse_args()

    send_times = _load_send_times(args.results_dir / "send_times.csv")
    append_latency_row(
        args.results_dir,
        args.query or "unknown",
        args.slice or "unknown",
        args.replay_mode or "unknown",
        send_times,
    )

    comp_dir = args.results_dir / "comparison"
    report_path = args.results_dir / "report.md"
    lines: list[str] = ["# Snowset benchmark report", ""]

    frames: list[pd.DataFrame] = []
    if comp_dir.is_dir():
        for f in sorted(comp_dir.glob("*.csv")):
            try:
                frames.append(pd.read_csv(f))
            except OSError:
                pass
    if frames:
        acc = pd.concat(frames, ignore_index=True)
        lines += [
            "## Accuracy (sketch vs ground truth)",
            "| query | slice | metric | value | threshold | pass |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
        for _, r in acc.iterrows():
            lines.append(
                f"| {r.get('query','')} | {r.get('slice','')} | "
                f"{r.get('metric','')} | {r.get('value','')} | "
                f"{r.get('threshold','')} | {r.get('pass','')} |"
            )
        lines.append("")

    tp_path = args.results_dir / "throughput.csv"
    if tp_path.is_file():
        lines += ["## Throughput", pd.read_csv(tp_path).to_csv(index=False), ""]

    lat_path = args.results_dir / "latency.csv"
    if lat_path.is_file():
        lines += ["## Latency history", pd.read_csv(lat_path).to_csv(index=False), ""]

    if not send_times.empty and len(send_times) >= 3:
        ordered = send_times.sort_values("event_time_ns")
        event_ns = ordered["event_time_ns"].astype(np.int64)
        deltas_ms = np.diff(event_ns) / 1e6
        emit_ns = ordered["emit_wall_ns"].astype(np.int64)
        lag_ms = ((emit_ns - int(emit_ns.iloc[0])).to_numpy(np.float64)
                  - (event_ns - int(event_ns.iloc[0])).to_numpy(np.float64)) / 1e6
        lines += [
            "## Send-time stats (this run)",
            "| stat | ms |", "| --- | --- |",
            f"| p50_delta_event | {float(np.percentile(deltas_ms,50)):.4f} |",
            f"| p95_delta_event | {float(np.percentile(deltas_ms,95)):.4f} |",
            f"| p99_delta_event | {float(np.percentile(deltas_ms,99)):.4f} |",
            f"| p50_send_lag | {float(np.percentile(lag_ms,50)):.4f} |",
            f"| p99_send_lag | {float(np.percentile(lag_ms,99)):.4f} |",
            "",
        ]

    report_path.write_text("\n".join(lines), encoding="utf-8")
    print(report_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    main()
