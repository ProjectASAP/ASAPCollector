"""Aggregate per-day comparison CSVs into a 10-minute test results markdown table.

Usage:
    python3 summarize.py --results-dir results/ --out results/10min_test_results.md
"""
from __future__ import annotations

import argparse
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


def summarize_comparison(comparison_dir: Path) -> pd.DataFrame:
    frames = [pd.read_csv(f) for f in sorted(comparison_dir.glob("*.csv"))]
    if not frames:
        return pd.DataFrame(columns=["query", "metric", "threshold", "avg", "min", "max", "all_pass"])
    df = pd.concat(frames, ignore_index=True)
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
    return agg


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


def build_markdown(agg: pd.DataFrame, throughput: pd.DataFrame) -> str:
    lines: list[str] = [
        "# 10-Minute Benchmark Test Results",
        "",
        "## Setup",
        "",
        "- Days: `08-11-21` through `12-11-21`",
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
        avg_v = fmt(row["avg"])
        min_v = fmt(row["min"])
        max_v = fmt(row["max"])
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Aggregate comparison CSVs into a results markdown.")
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

    comparison_dir = args.results_dir / "comparison"
    out_path = args.out or args.results_dir / "10min_test_results.md"

    agg = summarize_comparison(comparison_dir)
    throughput = summarize_throughput(args.results_dir / "throughput.csv")
    md = build_markdown(agg, throughput)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(md, encoding="utf-8")
    print(f"Written: {out_path}")


if __name__ == "__main__":
    main()
