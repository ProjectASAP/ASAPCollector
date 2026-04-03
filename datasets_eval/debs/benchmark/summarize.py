"""Aggregate per-day comparison CSVs into a results markdown table, or summarise a single run.

Usage:
    # Cross-day summary (existing behaviour, unchanged):
    python3 summarize.py --results-dir results/ --out results/10min_test_results.md

    # Per-run error stats for one Q1 run:
    python3 summarize.py run --query Q1 --day 08-11-21
    python3 summarize.py run --query Q1 --day 08-11-21 --results-dir results/ --log results/run_log.md
"""
from __future__ import annotations

import argparse
import datetime
from pathlib import Path

import pandas as pd

METRIC_DESCRIPTIONS: dict[str, str] = {
    "frac_lt_1pct": (
        "Fraction of (symbol, window) pairs where sketch EMA38 is within 1% relative error "
        "of exact EMA38. `1.0` = 100% correct. All non-warmup windows evaluated."
    ),
    "q3_score": (
        "`min(overlap/0.8, Spearman_ρ/0.7)`. "
        "Overlap = fraction of exact top-10 symbols also in sketch top-10. "
        "`1.0` means both overlap ≥ 80% and ρ ≥ 0.7. Last window only."
    ),
    "frac_hilo_lt_2pct": (
        "Fraction of symbols where sketch p0 (min) and p100 (max) are each within 2% "
        "relative error of the exact min/max. `1.0` = 100%. Last window only."
    ),
    "frac_sigma_lt_10pct": (
        "Fraction of symbols where the sketch-derived volatility (IQR/1.349) is within "
        "10% relative error of exact volatility. `1.0` = 100%. Last window only."
    ),
    "hll_max_rel_err": (
        "`|HLL_estimate − exact_count| / exact_count`. `0.0` = perfect estimate. "
        "Last window only."
    ),
    "frac_mean_lt_2pct": (
        "Fraction of symbols where sketch p50 is within 2% relative error of exact mean "
        "price. `1.0` = 100%. Last window only."
    ),
    "frac_iqr_lt_10pct": (
        "Fraction of symbols where sketch IQR (p75 − p25) is within 10% relative error "
        "of exact IQR. `1.0` = 100%. Last 15-min window only."
    ),
}

METRIC_RENAME: dict[str, str] = {
    "frac_lt_1pct": "symbols_within_1pct_ema",
    "q3_score": "topk_accuracy_score",
    "frac_hilo_lt_2pct": "symbols_within_2pct_price_range",
    "frac_sigma_lt_10pct": "symbols_within_10pct_volatility",
    "hll_max_rel_err": "hll_cardinality_rel_error",
    "frac_mean_lt_2pct": "symbols_within_2pct_mean",
    "frac_iqr_lt_10pct": "symbols_within_10pct_iqr",
}

THROUGHPUT_QUERIES = ("Q2", "Q9", "Q10", "Q11", "Q12")


def summarize_comparison(comparison_dir: Path) -> tuple[pd.DataFrame, list[str]]:
    frames = [pd.read_csv(f) for f in sorted(comparison_dir.glob("*.csv"))]
    if not frames:
        return pd.DataFrame(columns=["query", "metric", "threshold", "avg", "min", "max", "all_pass"]), []
    df = pd.concat(frames, ignore_index=True)
    days = sorted(df["day"].dropna().unique().tolist()) if "day" in df.columns else []
    agg = (
        df.groupby(["query", "metric", "threshold"])
        .agg(
            avg=("value", "mean"),
            min=("value", "min"),
            max=("value", "max"),
            all_pass=("pass", lambda s: int(s.all())),
        )
        .reset_index()
    )
    return agg, days


def summarize_throughput(throughput_path: Path) -> pd.DataFrame:
    if not throughput_path.is_file():
        return pd.DataFrame()
    df = pd.read_csv(throughput_path)
    if df.empty:
        return df
    nop = df[df["query"].isin(THROUGHPUT_QUERIES)] if "query" in df.columns else df
    return nop


def fmt(v: float, decimals: int = 4) -> str:
    return f"{v:.{decimals}f}"


def build_markdown(agg: pd.DataFrame, throughput: pd.DataFrame, days: list[str] | None = None) -> str:
    if days:
        days_sorted = sorted(set(days))
        if len(days_sorted) == 1:
            days_str = f"`{days_sorted[0]}`"
        else:
            days_str = f"`{days_sorted[0]}` through `{days_sorted[-1]}`"
    else:
        days_str = "(none)"
    lines: list[str] = [
        "# 10-Minute Benchmark Test Results",
        "",
        "## Setup",
        "",
        f"- Days: {days_str}",
        "- Mode: `sketch-finance`, `--accuracy-minutes 10`",
        "- Evaluation: Q1 compares all non-warmup 5-min windows; Q3–Q8 compare last "
        "completed window only.",
        "",
        "---",
        "",
        "## Accuracy results — statistical queries",
        "",
        "| Query | Metric | Description | Avg | Min | Max | Threshold | All Pass |",
        "|---|---|---|---|---|---|---|---|",
    ]

    for _, row in agg.iterrows():
        query = row["query"]
        raw_metric = row["metric"]
        if raw_metric not in METRIC_DESCRIPTIONS:
            continue
        metric_label = METRIC_RENAME.get(raw_metric, raw_metric)
        desc = METRIC_DESCRIPTIONS.get(raw_metric, "")
        threshold = row["threshold"]
        avg_v = fmt(float(row["avg"]))
        min_v = fmt(float(row["min"]))
        max_v = fmt(float(row["max"]))
        all_pass = "✓" if row["all_pass"] else "✗"
        if raw_metric == "hll_max_rel_err":
            thr_str = f"≤ {threshold:.2f}"
        else:
            thr_str = f"≥ {threshold:.2f}"
        lines.append(
            f"| {query} | `{metric_label}` | {desc} | {avg_v} | {min_v} | {max_v} | {thr_str} | {all_pass} |"
        )

    lines += ["", "---", ""]

    if not throughput.empty:
        lines += [
            "## Throughput — NOP queries (full day, max speed, day `08-11-21`)",
            "",
            "| Query | Events / sec | Total events | Elapsed (s) |",
            "|---|---|---|---|",
        ]
        for _, row in throughput.iterrows():
            q = row.get("query", "")
            eps = row.get("events_per_sec", 0)
            total = row.get("total_events", 0)
            elapsed = row.get("elapsed_s", 0)
            lines.append(f"| {q} | {int(eps):,} | {int(total):,} | {int(elapsed)} |")
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Per-run summary (single Q1 comparison CSV)
# ---------------------------------------------------------------------------

_Q1_ERR_METRICS = ("per_pair_rel_err_mean", "per_pair_rel_err_min", "per_pair_rel_err_max")
_Q1_PASS_METRICS = ("frac_lt_1pct",)

_LOG_HEADER = (
    "| timestamp | query | day | windows_compared"
    " | frac_lt_1pct | mean_err | min_err | max_err | pass |\n"
    "|---|---|---|---|---|---|---|---|---|\n"
)


def _count_windows(df: pd.DataFrame) -> int:
    """Return number of distinct GT windows present in a comparison CSV."""
    # compare_q1 stores one row per (symbol, window) pair via the merged GT;
    # the comparison CSV has one row per metric. We store windows_compared in
    # metadata if available, otherwise fall back to 'n/a'.
    if "windows_compared" in df.columns:
        v = df["windows_compared"].dropna()
        if not v.empty:
            return int(v.iloc[0])
    return -1


def run_single(
    query: str,
    day: str,
    results_dir: Path,
    log_path: Path,
) -> None:
    day_tag = day.replace(".csv", "").replace("debs2022-gc-trading-day-", "")
    csv_path = results_dir / "comparison" / f"{query}_{day_tag}.csv"
    if not csv_path.is_file():
        print(f"Comparison CSV not found: {csv_path}")
        return

    df = pd.read_csv(csv_path)
    if df.empty:
        print(f"Comparison CSV is empty: {csv_path}")
        return

    def _get(metric: str) -> float | None:
        rows = df[df["metric"] == metric]
        if rows.empty:
            return None
        return float(rows["value"].iloc[0])

    def _pass(metric: str) -> int | None:
        rows = df[df["metric"] == metric]
        if rows.empty:
            return None
        return int(rows["pass"].iloc[0])

    frac = _get("frac_lt_1pct")
    mean_e = _get("per_pair_rel_err_mean")
    min_e = _get("per_pair_rel_err_min")
    max_e = _get("per_pair_rel_err_max")

    all_pass = all(
        _pass(m) == 1
        for m in ("frac_lt_1pct", "per_pair_rel_err_mean", "per_pair_rel_err_max")
        if _pass(m) is not None
    )

    windows_compared = _count_windows(df)
    windows_str = str(windows_compared) if windows_compared >= 0 else "n/a"

    # --- console output ---
    print(f"\nRun summary: {query} / {day_tag}")
    print(f"  windows_compared : {windows_str}")
    print(f"  frac_lt_1pct     : {frac:.4f}" if frac is not None else "  frac_lt_1pct     : n/a")
    print(f"  mean_err         : {mean_e:.6f}" if mean_e is not None else "  mean_err         : n/a")
    print(f"  min_err          : {min_e:.6e}" if min_e is not None else "  min_err          : n/a")
    print(f"  max_err          : {max_e:.6f}" if max_e is not None else "  max_err          : n/a")
    print(f"  pass             : {'yes' if all_pass else 'no'}")

    # --- append to run log ---
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    frac_str = f"{frac:.4f}" if frac is not None else "—"
    mean_str = f"{mean_e:.6f}" if mean_e is not None else "—"
    min_str = f"{min_e:.6e}" if min_e is not None else "—"
    max_str = f"{max_e:.6f}" if max_e is not None else "—"
    pass_str = "✓" if all_pass else "✗"

    row = (
        f"| {ts} | {query} | {day_tag} | {windows_str}"
        f" | {frac_str} | {mean_str} | {min_str} | {max_str} | {pass_str} |\n"
    )

    log_path.parent.mkdir(parents=True, exist_ok=True)
    if not log_path.is_file():
        log_path.write_text(f"# Run log\n\n{_LOG_HEADER}{row}", encoding="utf-8")
        print(f"Created: {log_path}")
    else:
        content = log_path.read_text(encoding="utf-8")
        # Append row; if header not present (e.g. file was empty), prepend it.
        if "| timestamp |" not in content:
            log_path.write_text(content + f"\n{_LOG_HEADER}{row}", encoding="utf-8")
        else:
            log_path.write_text(content + row, encoding="utf-8")
        print(f"Appended: {log_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Summarise benchmark results.")
    sub = parser.add_subparsers(dest="command")

    # --- 'run' subcommand: per-run single-day stats ---
    p_run = sub.add_parser("run", help="Print per-run error stats for one Q1 run and append to run_log.md.")
    p_run.add_argument("--query", default="Q1", help="Query ID (default: Q1).")
    p_run.add_argument("--day", default="08-11-21", help="Trading day tag (default: 08-11-21).")
    p_run.add_argument(
        "--results-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "results",
    )
    p_run.add_argument(
        "--log",
        type=Path,
        default=None,
        help="Run log markdown file (default: <results-dir>/run_log.md).",
    )

    # --- default (no subcommand): cross-day aggregate markdown ---
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "results",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output markdown file (default: <results-dir>/10min_test_results.md).",
    )

    args = parser.parse_args()

    if args.command == "run":
        log = args.log or args.results_dir / "run_log.md"
        run_single(args.query, args.day, args.results_dir, log)
        return

    # Default cross-day aggregate path
    comparison_dir = args.results_dir / "comparison"
    out_path = args.out or args.results_dir / "10min_test_results.md"

    agg, days = summarize_comparison(comparison_dir)
    throughput = summarize_throughput(args.results_dir / "throughput.csv")
    md = build_markdown(agg, throughput, days)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(md, encoding="utf-8")
    print(f"Written: {out_path}")


if __name__ == "__main__":
    main()
