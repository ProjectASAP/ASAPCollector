"""Aggregate per-file comparison CSVs into a benchmark results markdown table.

Usage:
    python3 summarize.py --results-dir results/ --out results/test_results.md
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

METRIC_DESCRIPTIONS: dict[str, str] = {
    "frac_q50_lt_1pct": (
        "Fraction of (entity, metric_base) pairs where sketch p50 is within 1% relative "
        "error of exact p50. `1.0` = 100% correct. Last window evaluated."
    ),
    "frac_q95_lt_1pct": (
        "Fraction of (entity, metric_base) pairs where sketch p95 is within 1% relative "
        "error of exact p95. Last window evaluated."
    ),
    "frac_q99_lt_1pct": (
        "Fraction of (entity, metric_base) pairs where sketch p99 is within 1% relative "
        "error of exact p99. Last window evaluated."
    ),
    "topk_overlap": (
        "Fraction of ground-truth top-10 metrics also present in sketch top-10 (Jaccard "
        "overlap). `1.0` = perfect overlap. Last window only."
    ),
    "rank_correlation": (
        "Spearman rank correlation between ground-truth and sketch top-K rankings. "
        "`1.0` = identical ordering. Last window only."
    ),
    "frac_min_lt_2pct": (
        "Fraction of (entity, metric_base) pairs where sketch min (p0) is within 2% "
        "relative error of exact min. Last 5-min window."
    ),
    "frac_max_lt_2pct": (
        "Fraction of (entity, metric_base) pairs where sketch max (p100) is within 2% "
        "relative error of exact max. Last 5-min window."
    ),
    "frac_iqr_lt_10pct": (
        "Fraction of (entity, metric_base) pairs where sketch IQR (p75−p25) is within "
        "10% relative error of exact IQR. Last 15-min window."
    ),
    "hll_rel_err": (
        "|HLL_estimate − exact_distinct| / exact_distinct. `0.0` = perfect estimate. "
        "Last 5-min window."
    ),
    "entity_topk_overlap": (
        "Fraction of ground-truth top-3 entities (by anomaly volume) also in sketch "
        "top-3. Last 5-min window."
    ),
    "frac_drift_p95_lt_20pct": (
        "Fraction of (entity, metric_base) pairs where sketch p95 drift error is within "
        "20% of exact drift. Last window pair."
    ),
    "sat_ratio_mae": (
        "Mean absolute error of sketch saturation ratio vs exact ratio per entity. "
        "`0.0` = perfect. Last 5-min window."
    ),
    # Q1_KLL stream error and pass metrics  # Q1_KLL
    "stream_A_p50_err": "Stream A (jvmGCTime, right-skewed): KLL p50 relative error vs exact sort. Target < 5%.",  # Q1_KLL
    "stream_A_p95_err": "Stream A (jvmGCTime, right-skewed): KLL p95 relative error. Target < 5%.",  # Q1_KLL
    "stream_A_p99_err": "Stream A (jvmGCTime, right-skewed): KLL p99 relative error. Hardest — p99==max.",  # Q1_KLL
    "stream_B_p50_err": "Stream B (PS-MarkSweep, near-constant): KLL p50 absolute error. Target == 0.",  # Q1_KLL
    "stream_B_p95_err": "Stream B (PS-MarkSweep, near-constant): KLL p95 absolute error. Target == 0.",  # Q1_KLL
    "stream_B_p99_err": "Stream B (PS-MarkSweep, near-constant): KLL p99 absolute error. Target == 0.",  # Q1_KLL
    "stream_C_p50_err": "Stream C (cpuTime ns→s, wide-range): KLL p50 relative error. Target < 5%.",  # Q1_KLL
    "stream_C_p95_err": "Stream C (cpuTime ns→s, wide-range): KLL p95 relative error. Target < 5%.",  # Q1_KLL
    "stream_C_p99_err": "Stream C (cpuTime ns→s, wide-range): KLL p99 relative error. Target < 5%.",  # Q1_KLL
    "stream_A_pass":    "Stream A overall pass: all three quantiles within 5% error. 1.0=pass.",  # Q1_KLL
    "stream_B_pass":    "Stream B overall pass: all three quantiles exact (0 absolute error). 1.0=pass.",  # Q1_KLL
    "stream_C_pass":    "Stream C overall pass: all three quantiles within 5% error. 1.0=pass.",  # Q1_KLL
    "overall_pass":     "All three streams pass their accuracy criteria simultaneously. 1.0=pass.",  # Q1_KLL
}

METRIC_RENAME: dict[str, str] = {
    "frac_q50_lt_1pct": "series_within_1pct_p50",
    "frac_q95_lt_1pct": "series_within_1pct_p95",
    "frac_q99_lt_1pct": "series_within_1pct_p99",
    "topk_overlap": "topk_metrics_overlap",
    "rank_correlation": "topk_rank_spearman",
    "frac_min_lt_2pct": "series_within_2pct_min",
    "frac_max_lt_2pct": "series_within_2pct_max",
    "frac_iqr_lt_10pct": "series_within_10pct_iqr",
    "hll_rel_err": "hll_distinct_rel_error",
    "entity_topk_overlap": "entity_topk_overlap",
    "frac_drift_p95_lt_20pct": "series_within_20pct_drift_p95",
    "sat_ratio_mae": "saturation_ratio_mae",
    # Q1_KLL stream error renames  # Q1_KLL
    "stream_A_p50_err": "kll_stream_A_jvmGC_p50_rel_err",  # Q1_KLL
    "stream_A_p95_err": "kll_stream_A_jvmGC_p95_rel_err",  # Q1_KLL
    "stream_A_p99_err": "kll_stream_A_jvmGC_p99_rel_err",  # Q1_KLL
    "stream_B_p50_err": "kll_stream_B_markSweep_p50_abs_err",  # Q1_KLL
    "stream_B_p95_err": "kll_stream_B_markSweep_p95_abs_err",  # Q1_KLL
    "stream_B_p99_err": "kll_stream_B_markSweep_p99_abs_err",  # Q1_KLL
    "stream_C_p50_err": "kll_stream_C_cpuTime_p50_rel_err",  # Q1_KLL
    "stream_C_p95_err": "kll_stream_C_cpuTime_p95_rel_err",  # Q1_KLL
    "stream_C_p99_err": "kll_stream_C_cpuTime_p99_rel_err",  # Q1_KLL
    "stream_A_pass":    "kll_stream_A_pass",  # Q1_KLL
    "stream_B_pass":    "kll_stream_B_pass",  # Q1_KLL
    "stream_C_pass":    "kll_stream_C_pass",  # Q1_KLL
    "overall_pass":     "kll_q1_overall_pass",  # Q1_KLL
}

THROUGHPUT_QUERIES = frozenset({"Q2", "Q10", "Q11", "Q12"})


def summarize_comparison(comparison_dir: Path) -> pd.DataFrame:
    frames = [pd.read_csv(f) for f in sorted(comparison_dir.glob("*.csv"))]
    if not frames:
        return pd.DataFrame(
            columns=["query", "metric", "threshold", "avg", "min", "max", "all_pass"]
        )
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
    if pd.isna(v):
        return "—"
    return f"{v:.{decimals}f}"


def build_markdown(agg: pd.DataFrame, throughput: pd.DataFrame) -> str:
    lines: list[str] = [
        "# Exathlon Benchmark Results",
        "",
        "## Setup",
        "",
        "- Dataset: `exathlon/data/raw` (Spark + HPC telemetry at ~1 Hz, ~2 283 series/file)",
        "- Window: 5-min (primary); 15-min for Q5; 1/5/15/30/60-min for Q4/Q12",
        "- Evaluation: last completed window per run (all non-warmup windows for Q1).",
        "- Threshold (Q3/Q9): file-local p95 of non-sentinel values per (entity, metric_base).",
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
        desc = METRIC_DESCRIPTIONS[raw_metric]
        threshold = row["threshold"]
        avg_v = fmt(row["avg"])
        min_v = fmt(row["min"])
        max_v = fmt(row["max"])
        all_pass = "✓" if row["all_pass"] else "✗"
        if raw_metric in ("hll_rel_err", "sat_ratio_mae"):
            thr_str = f"≤ {threshold:.2f}"
        else:
            thr_str = f"≥ {threshold:.2f}"
        lines.append(
            f"| {query} | `{metric_label}` | {desc} | {avg_v} | {min_v} | {max_v} | {thr_str} | {all_pass} |"
        )

    lines += ["", "---", ""]

    if not throughput.empty:
        lines += [
            "## Throughput — NOP queries",
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
    parser = argparse.ArgumentParser(
        description="Aggregate comparison CSVs into a results markdown."
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "results",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output markdown file (default: <results-dir>/test_results.md).",
    )
    args = parser.parse_args()

    comparison_dir = args.results_dir / "comparison"
    out_path = args.out or args.results_dir / "test_results.md"

    agg = summarize_comparison(comparison_dir)
    throughput = summarize_throughput(args.results_dir / "throughput.csv")
    md = build_markdown(agg, throughput)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(md, encoding="utf-8")
    print(f"Written: {out_path}")


if __name__ == "__main__":
    main()
