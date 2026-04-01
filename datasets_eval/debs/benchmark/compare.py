from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Callable

_BENCH_ROOT = Path(__file__).resolve().parent
if str(_BENCH_ROOT) not in sys.path:
    sys.path.insert(0, str(_BENCH_ROOT))

from common import data_path, day_to_filename
from ground_truth.common import (
    WINDOW_15MIN_MS,
    load_filtered_day,
    window_start_ms_vectorized,
)

import numpy as np
import pandas as pd


def get_latest_scrape_snapshot(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe
    wall_ns = pd.to_numeric(dataframe["scrape_wall_ns"], errors="coerce")
    dataframe = dataframe[wall_ns.notna()].copy()
    wall_ns = wall_ns[wall_ns.notna()]
    if dataframe.empty:
        return dataframe
    max_wall_ns = wall_ns.max()
    return dataframe[wall_ns == max_wall_ns]


# Populated incrementally by each query PR.
_SKETCH_METRIC_PATTERN: dict[str, str] = {}


def get_best_snapshot_for_query(sketch_data: pd.DataFrame, query_id: str) -> pd.DataFrame:
    """Pick the scrape timestamp with the richest sketch rows (avoids empty post-replay snapshot)."""
    pattern = _SKETCH_METRIC_PATTERN.get(query_id)
    if pattern is None or sketch_data.empty:
        return get_latest_scrape_snapshot(sketch_data)
    metrics = sketch_data["metric"].astype(str)
    relevant = sketch_data[metrics.str.contains(pattern, regex=True, na=False)]
    if relevant.empty:
        return get_latest_scrape_snapshot(sketch_data)
    numeric = pd.to_numeric(relevant["value"], errors="coerce").fillna(0)
    non_zero = relevant[numeric > 0]
    pool = non_zero if not non_zero.empty else relevant
    best_ts = pool.groupby("scrape_wall_ns").size().idxmax()
    return sketch_data[sketch_data["scrape_wall_ns"] == best_ts]


def _detect_sketch_flavor(dataframe: pd.DataFrame) -> str:
    """Return 'ddsketch', 'kll', or '' based on which quantile sketch metrics are present."""
    if dataframe.empty:
        return ""
    metrics = dataframe["metric"].astype(str)
    if metrics.str.contains("_ddsketch", regex=False, na=False).any():
        return "ddsketch"
    if metrics.str.contains("_kll", regex=False, na=False).any():
        return "kll"
    return ""


def extract_ddsketch_median(dataframe: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    flavor = _detect_sketch_flavor(dataframe)
    if not flavor:
        return pd.DataFrame(columns=["symbol", "v"])
    metric_substr = f"_{flavor}"
    quantile_key = f"{flavor}_quantile"
    for _, record in dataframe.iterrows():
        metric_name = str(record.get("metric", ""))
        if metric_substr not in metric_name:
            continue
        try:
            labels = json.loads(record["labels"])
        except (json.JSONDecodeError, KeyError, TypeError):
            continue
        if labels.get(quantile_key) not in ("0.5", "0.50"):
            continue
        symbol = labels.get("symbol")
        if not symbol:
            continue
        try:
            value = float(record["value"])
        except (TypeError, ValueError):
            continue
        rows.append({"symbol": symbol, "v": value})
    if not rows:
        return pd.DataFrame(columns=["symbol", "v"])
    return pd.DataFrame(rows).groupby("symbol", as_index=False)["v"].mean()


# Populated incrementally by each query PR.
_COMPARE_DISPATCH: dict[str, Callable[..., dict]] = {}


def compare_scrape_nonempty(sketch_rows: pd.DataFrame) -> dict:
    return {
        "metric": "scrape_rows",
        "value": float(len(sketch_rows)),
        "threshold": 1.0,
        "pass": int(len(sketch_rows) > 0),
    }


def run_comparison(
    query_id: str,
    day: str,
    ground_truth_dir: Path,
    sketch_output_dir: Path,
    comparison_out_dir: Path,
) -> None:
    day_tag = day.replace(".csv", "").replace("debs2022-gc-trading-day-", "")
    ground_truth_path = ground_truth_dir / query_id / f"{day_tag}.csv"
    sketch_path = sketch_output_dir / query_id / f"{day_tag}.csv"
    if not ground_truth_path.is_file():
        return
    ground_truth = pd.read_csv(ground_truth_path)
    sketch_data = pd.read_csv(sketch_path, on_bad_lines="skip", low_memory=False) if sketch_path.is_file() else pd.DataFrame()
    comparison_out_dir.mkdir(parents=True, exist_ok=True)
    fn = _COMPARE_DISPATCH.get(query_id)
    if fn is None:
        return
    sketch_snapshot = get_best_snapshot_for_query(sketch_data, query_id)
    if query_id == "Q6":
        result = fn(ground_truth, sketch_data, day=day_tag)
    else:
        result = fn(ground_truth, sketch_snapshot, day=day_tag)
    pd.DataFrame([{"query": query_id, "day": day_tag, **result}]).to_csv(
        comparison_out_dir / f"{query_id}_{day_tag}.csv", index=False
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare sketch scrapes to ground truth.")
    parser.add_argument("--query", default="Q1")
    parser.add_argument("--day", default="08-11-21")
    parser.add_argument(
        "--gt-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "results" / "ground_truth",
    )
    parser.add_argument(
        "--sketch-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "results" / "sketch_output",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "results" / "comparison",
    )
    args = parser.parse_args()
    run_comparison(args.query, args.day, args.gt_dir, args.sketch_dir, args.out_dir)


if __name__ == "__main__":
    main()
