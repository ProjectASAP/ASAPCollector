"""Aggregate comparison CSVs into a benchmark results markdown table."""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

METRIC_DESCRIPTIONS: dict[str, str] = {
    "cms_freq_rel_err": (
        "Q1: Mean relative frequency error |est−true|/N across all warehouses in the last "
        "5-min window. `0.0` = exact. CMS guarantee: ≤ ε=0.001 with probability ≥ 0.99."
    ),
    "kll_p99_rel_err": (
        "Q2: Mean relative p99 error |KLL_p99 − exact_p99| / exact_p99 across warehouses "
        "in the last 5-min window. KLL rank-error bound < 0.5%."
    ),
    "dds_p99_rel_err": (
        "Q3: Mean relative p99 error |DDSketch_p99 − exact_p99| / exact_p99 across "
        "warehouses (non-zero S3 reads). DDSketch relative-error guarantee: ≤ α=0.01."
    ),
    "cms_arch_share_err": (
        "Q4: Mean absolute archetype share error |ŝ − s| per archetype in the last window. "
        "`0.0` = exact share. Low cardinality (6 archetypes) means CMS operates near-exactly."
    ),
    "hll_cardinality_rel_err": (
        "Q5: |HLL_estimate − exact_distinct_warehouses| / exact_distinct_warehouses. "
        "HLL standard error ~0.81% at precision p=14."
    ),
    "kll_p95_concurrency_band_rel_err": (
        "Q6: Mean relative p95 error by `(warehouseSize, concurrency_band)` in the last "
        "5-min window of the joined dataset. Baseline query uses KLL over durationTotal "
        "with labels derived from per-tick concurrency bands."
    ),
    "kll_p99_concurrency_band_rel_err": (
        "Q6: Mean relative p99 error by `(warehouseSize, concurrency_band)` in the last "
        "5-min window of the joined dataset. Uses p99 when the sketch does not emit p95."
    ),
}

METRIC_RENAME: dict[str, str] = {
    "cms_freq_rel_err":      "cms_s3_frequency_rel_error",
    "kll_p99_rel_err":       "kll_duration_p99_rel_error",
    "dds_p99_rel_err":       "dds_s3bytes_p99_rel_error",
    "cms_arch_share_err":    "cms_archetype_share_error",
    "hll_cardinality_rel_err": "hll_warehouse_cardinality_rel_error",
    "kll_p95_concurrency_band_rel_err": "kll_duration_p95_concurrency_band_rel_error",
    "kll_p99_concurrency_band_rel_err": "kll_duration_p99_concurrency_band_rel_error",
}

THRESHOLD_DIRECTION: dict[str, str] = {
    "cms_freq_rel_err":       "≤",
    "kll_p99_rel_err":        "≤",
    "dds_p99_rel_err":        "≤",
    "cms_arch_share_err":     "≤",
    "hll_cardinality_rel_err": "≤",
    "kll_p95_concurrency_band_rel_err": "≤",
    "kll_p99_concurrency_band_rel_err": "≤",
}


def summarize_comparison(comp_dir: Path) -> pd.DataFrame:
    frames = [pd.read_csv(f) for f in sorted(comp_dir.glob("*.csv")) if f.is_file()]
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    return (
        df.groupby(["query", "metric", "threshold"])
        .agg(avg=("value", "mean"), min=("value", "min"),
             max=("value", "max"), all_pass=("pass", lambda s: int(s.all())))
        .reset_index()
    )


def build_markdown(agg: pd.DataFrame, throughput: pd.DataFrame) -> str:
    lines: list[str] = [
        "# Snowset Benchmark Results",
        "",
        "## Setup",
        "",
        "- Dataset: `snowset-main.parquet` for Q1-Q5; `analysis/results/joined/full_join.parquet` for Q6",
        "- Queries: Q1 (CMS frequency), Q2 (KLL quantile), Q3 (DDSketch quantile),",
        "           Q4 (CMS archetype), Q5 (HLL cardinality), Q6 (KLL latency by concurrency band)",
        "- Window: 5-minute tumbling (~4 windows in the dataset)",
        "- Evaluation: last completed window only",
        "",
        "---",
        "",
        "## Accuracy results",
        "",
        "| Query | Metric | Description | Avg | Min | Max | Threshold | All Pass |",
        "|---|---|---|---|---|---|---|---|",
    ]

    for _, row in agg.iterrows():
        raw = row["metric"]
        if raw not in METRIC_DESCRIPTIONS:
            continue
        label = METRIC_RENAME.get(raw, raw)
        desc = METRIC_DESCRIPTIONS[raw]
        thr = row["threshold"]
        dir_ = THRESHOLD_DIRECTION.get(raw, "≤")
        all_pass = "✓" if row["all_pass"] else "✗"
        lines.append(
            f"| {row['query']} | `{label}` | {desc} | "
            f"{row['avg']:.4f} | {row['min']:.4f} | {row['max']:.4f} | "
            f"{dir_} {thr:.4f} | {all_pass} |"
        )

    lines += ["", "---", ""]

    if not throughput.empty:
        lines += [
            "## Throughput",
            "",
            "| Query | Slice | Events / sec | Total events | Elapsed (s) |",
            "|---|---|---|---|---|",
        ]
        for _, row in throughput.iterrows():
            lines.append(
                f"| {row.get('query','')} | {row.get('slice','')} | "
                f"{int(row.get('events_per_sec',0)):,} | "
                f"{int(row.get('total_events',0)):,} | "
                f"{int(row.get('elapsed_s',0))} |"
            )
        lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-dir", type=Path,
                        default=Path(__file__).resolve().parent / "results")
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()

    comp_dir = args.results_dir / "comparison"
    out_path = args.out or args.results_dir / "benchmark_results.md"

    agg = summarize_comparison(comp_dir)
    tp_path = args.results_dir / "throughput.csv"
    throughput = pd.read_csv(tp_path) if tp_path.is_file() else pd.DataFrame()
    md = build_markdown(agg, throughput)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(md, encoding="utf-8")
    print(f"Written: {out_path}")


if __name__ == "__main__":
    main()
