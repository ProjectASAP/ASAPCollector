from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Callable

_BENCH_ROOT = Path(__file__).resolve().parent
if str(_BENCH_ROOT) not in sys.path:
    sys.path.insert(0, str(_BENCH_ROOT))

from ground_truth.common import WINDOW_15MIN_MS, WINDOW_5MIN_MS

import numpy as np
import pandas as pd

# Maps query ID to a regex that matches its primary sketch metric name.
# Populated incrementally by each query branch.
_SKETCH_METRIC_PATTERN: dict[str, str] = {}

# Compare function registry. Populated incrementally by each query branch.
_COMPARE_DISPATCH: dict[str, Callable[..., dict]] = {}


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


def get_best_snapshot_for_query(sketch_data: pd.DataFrame, query_id: str) -> pd.DataFrame:
    """Pick the scrape timestamp with the most relevant sketch rows."""
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


def _select_evaluation_window(
    ground_truth: pd.DataFrame,
    window_ms: int,
    skip_warmup_windows: int,
) -> pd.DataFrame:
    """Select the GT window to compare against the sketch.

    Two steps:
    1. Skip the first `skip_warmup_windows` windows. For EMA-based queries
       (Q1/Q2) the first window has cold-start bias; set skip=1 there.
       For stateless queries (Q3-Q8) set skip=0.
    2. Keep only the last remaining window. The Prometheus scrape captures the
       collector's current state, which corresponds to the most recent closed
       window, so we match GT to that same window.
    """
    if "window_start_ms" not in ground_truth.columns or ground_truth.empty:
        return ground_truth
    min_w = int(ground_truth["window_start_ms"].min())
    cutoff = min_w + skip_warmup_windows * window_ms
    gt = ground_truth[ground_truth["window_start_ms"] >= cutoff]
    if gt.empty:
        return gt
    last_w = int(gt["window_start_ms"].max())
    return gt[gt["window_start_ms"] == last_w]


def _quantile_from_labels(labels: dict) -> float | None:
    for key in ("ddsketch.quantile", "ddsketch_quantile", "kll.quantile", "kll_quantile"):
        if key not in labels:
            continue
        try:
            return float(labels[key])
        except (TypeError, ValueError):
            continue
    return None


def _detect_sketch_flavor(dataframe: pd.DataFrame) -> str:
    if dataframe.empty:
        return ""
    metrics = dataframe["metric"].astype(str)
    if metrics.str.contains("_ddsketch", regex=False, na=False).any():
        return "ddsketch"
    if metrics.str.contains("_kll", regex=False, na=False).any():
        return "kll"
    return ""


def extract_sketch_quantile(
    dataframe: pd.DataFrame, q_target: float, tol: float = 1e-4
) -> pd.DataFrame:
    rows: list[dict] = []
    flavor = _detect_sketch_flavor(dataframe)
    if not flavor:
        return pd.DataFrame(columns=["symbol", "v"])
    metric_substr = f"_{flavor}"
    for _, record in dataframe.iterrows():
        metric_name = str(record.get("metric", ""))
        if metric_substr not in metric_name:
            continue
        try:
            labels = json.loads(record["labels"])
        except (json.JSONDecodeError, KeyError, TypeError):
            continue
        qq = _quantile_from_labels(labels)
        if qq is None or abs(qq - q_target) > tol:
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


def extract_ddsketch_median(dataframe: pd.DataFrame) -> pd.DataFrame:
    return extract_sketch_quantile(dataframe, 0.5)


def _symbol_from_partition_key(partition_key: str) -> str | None:
    for segment in str(partition_key).split(";"):
        segment = segment.strip()
        if segment.startswith("symbol="):
            return segment[len("symbol="):].strip()
    return None


def extract_countsketch_estimates(dataframe: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    for _, record in dataframe.iterrows():
        if "countsketch_partition" not in str(record.get("metric", "")):
            continue
        try:
            labels = json.loads(record["labels"])
        except (json.JSONDecodeError, KeyError, TypeError):
            continue
        pk = labels.get("partition_key", "")
        symbol = _symbol_from_partition_key(pk)
        if not symbol:
            continue
        try:
            est = float(record["value"])
        except (TypeError, ValueError):
            continue
        rows.append({"symbol": symbol, "est": est})
    if not rows:
        return pd.DataFrame(columns=["symbol", "est"])
    return pd.DataFrame(rows).groupby("symbol", as_index=False)["est"].max()


def extract_hll_cardinality(dataframe: pd.DataFrame) -> float:
    """Return the HLL distinct-symbol estimate (count of active hll_cardinality rows)."""
    count = 0
    for _, record in dataframe.iterrows():
        if "_hll_cardinality" not in str(record.get("metric", "")):
            continue
        try:
            v = float(record["value"])
        except (TypeError, ValueError):
            continue
        if v > 0:
            count += 1
    return float(count)


def run_comparison(
    query_id: str,
    day: str,
    ground_truth_dir: Path,
    sketch_output_dir: Path,
    comparison_out_dir: Path,
    skip_warmup_windows: int | None = None,
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
    kwargs: dict = {"day": day_tag}
    if skip_warmup_windows is not None:
        kwargs["skip_warmup_windows"] = skip_warmup_windows
    if query_id == "Q6":
        result = fn(ground_truth, sketch_data, **kwargs)
    else:
        result = fn(ground_truth, sketch_snapshot, **kwargs)
    pd.DataFrame([{"query": query_id, "day": day_tag, **result}]).to_csv(
        comparison_out_dir / f"{query_id}_{day_tag}.csv", index=False
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare sketch scrapes to ground truth.")
    parser.add_argument("--query", default="Q1")
    parser.add_argument("--day", default="08-11-21")
    parser.add_argument(
        "--skip-warmup-windows",
        type=int,
        default=None,
        help="Override number of warmup windows to skip (default: 1 for Q1, 0 for Q3-Q8).",
    )
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
    run_comparison(
        args.query,
        args.day,
        args.gt_dir,
        args.sketch_dir,
        args.out_dir,
        skip_warmup_windows=args.skip_warmup_windows,
    )


# --- Q1: EMA quantile accuracy ---

def compare_q1(
    ground_truth: pd.DataFrame,
    sketch_rows: pd.DataFrame,
    *,
    day: str = "",
    skip_warmup_windows: int = 1,
) -> dict:
    # Skip first window (EMA cold-start), compare sketch p50 vs EMA38 across all remaining windows.
    if skip_warmup_windows > 0 and "window_start_ms" in ground_truth.columns:
        min_window = ground_truth["window_start_ms"].min()
        cutoff = min_window + skip_warmup_windows * WINDOW_5MIN_MS
        ground_truth = ground_truth[ground_truth["window_start_ms"] >= cutoff]
    sketch_medians = extract_ddsketch_median(sketch_rows)
    merged = ground_truth.merge(sketch_medians, on="symbol", how="inner")
    if merged.empty:
        return {"metric": "frac_lt_1pct", "value": 0.0, "threshold": 0.95, "pass": 0}
    relative_error = (merged["ema38"] - merged["v"]).abs() / merged["ema38"].abs().clip(lower=1e-12)
    fraction = float((relative_error < 0.01).mean())
    return {
        "metric": "frac_lt_1pct",
        "value": fraction,
        "threshold": 0.95,
        "pass": int(fraction >= 0.95),
    }


_SKETCH_METRIC_PATTERN["Q1"] = r"ddsketch|kll"
_COMPARE_DISPATCH["Q1"] = compare_q1


# --- Q3: top-K frequency accuracy ---

def compare_q3(
    ground_truth: pd.DataFrame,
    sketch_rows: pd.DataFrame,
    *,
    day: str = "",
    k: int = 10,
    skip_warmup_windows: int = 0,
) -> dict:
    gt = _select_evaluation_window(ground_truth, WINDOW_5MIN_MS, skip_warmup_windows)
    if gt.empty or len(gt) < k:
        return {"metric": "q3_score", "value": 0.0, "threshold": 1.0, "pass": 0}
    top_gt = gt.nlargest(k, "count")
    sk = extract_countsketch_estimates(sketch_rows)
    if sk.empty:
        return {"metric": "q3_score", "value": 0.0, "threshold": 1.0, "pass": 0}
    overlap = len(set(top_gt["symbol"]) & set(sk.nlargest(k, "est")["symbol"])) / float(k)
    merged = top_gt.merge(sk, on="symbol", how="left")
    merged["est"] = merged["est"].fillna(0.0)
    rho = merged["count"].corr(merged["est"], method="spearman")
    try:
        rho_f = float(rho)
    except (TypeError, ValueError):
        rho_f = 0.0
    # When all GT counts are equal (zero variance), Spearman is undefined (NaN).
    # Fall back to overlap-only scoring since ranking is meaningless.
    gt_constant = float(merged["count"].std()) == 0.0
    if np.isnan(rho_f) or gt_constant:
        score = overlap / 0.8
        passed = overlap >= 0.8
    else:
        score = min(overlap / 0.8, rho_f / 0.7)
        passed = overlap >= 0.8 and rho_f > 0.7
    return {
        "metric": "q3_score",
        "value": float(score),
        "threshold": 1.0,
        "pass": int(passed),
    }


_SKETCH_METRIC_PATTERN["Q3"] = r"countsketch"
_COMPARE_DISPATCH["Q3"] = compare_q3



# --- Q4: price range (min/max) accuracy ---

def compare_q4(
    ground_truth: pd.DataFrame,
    sketch_rows: pd.DataFrame,
    *,
    day: str = "",
    skip_warmup_windows: int = 0,
) -> dict:
    gt = _select_evaluation_window(ground_truth, WINDOW_5MIN_MS, skip_warmup_windows)
    p0 = extract_sketch_quantile(sketch_rows, 0.0, tol=1e-6)
    p100 = extract_sketch_quantile(sketch_rows, 1.0, tol=1e-6)
    if p0.empty or p100.empty:
        return {"metric": "frac_hilo_lt_2pct", "value": 0.0, "threshold": 0.90, "pass": 0}
    sketch = p0.merge(p100, on="symbol", suffixes=("_low", "_high"))
    merged = gt.merge(sketch[["symbol", "v_low", "v_high"]], on="symbol", how="inner")
    if merged.empty:
        return {"metric": "frac_hilo_lt_2pct", "value": 0.0, "threshold": 0.90, "pass": 0}
    rel_high = (merged["v_high"] - merged["high"]).abs() / merged["high"].abs().clip(lower=1e-12)
    rel_low = (merged["v_low"] - merged["low"]).abs() / merged["low"].abs().clip(lower=1e-12)
    passes = (rel_high < 0.02) & (rel_low < 0.02)
    fraction = float(passes.mean())
    return {
        "metric": "frac_hilo_lt_2pct",
        "value": fraction,
        "threshold": 0.90,
        "pass": int(fraction >= 0.90),
    }


_SKETCH_METRIC_PATTERN["Q4"] = r"ddsketch|kll"
_COMPARE_DISPATCH["Q4"] = compare_q4



# --- Q5: volatility (IQR/1.349) accuracy ---

def compare_q5(
    ground_truth: pd.DataFrame,
    sketch_rows: pd.DataFrame,
    *,
    day: str = "",
    skip_warmup_windows: int = 0,
) -> dict:
    gt = _select_evaluation_window(ground_truth, WINDOW_5MIN_MS, skip_warmup_windows)
    p25 = extract_sketch_quantile(sketch_rows, 0.25)
    p75 = extract_sketch_quantile(sketch_rows, 0.75)
    if p25.empty or p75.empty:
        return {"metric": "frac_sigma_lt_10pct", "value": 0.0, "threshold": 0.90, "pass": 0}
    sketch = p25.merge(p75, on="symbol", suffixes=("_q25", "_q75"))
    sketch["sketch_sigma"] = (sketch["v_q75"] - sketch["v_q25"]) / 1.349
    merged = gt.merge(sketch[["symbol", "sketch_sigma"]], on="symbol", how="inner")
    if merged.empty:
        return {"metric": "frac_sigma_lt_10pct", "value": 0.0, "threshold": 0.90, "pass": 0}
    truth = merged["sigma_iqr"] if "sigma_iqr" in merged.columns else merged["sigma"]
    acc = 0.01
    if "ref_price" in merged.columns:
        floor = (merged["ref_price"].abs() * acc).clip(lower=1e-12)
    else:
        floor = 1e-12
    scale = np.maximum(np.maximum(np.abs(truth), np.abs(merged["sketch_sigma"])), floor)
    rel = (merged["sketch_sigma"] - truth).abs() / scale
    fraction = float((rel < 0.10).mean())
    return {
        "metric": "frac_sigma_lt_10pct",
        "value": fraction,
        "threshold": 0.90,
        "pass": int(fraction >= 0.90),
    }


_SKETCH_METRIC_PATTERN["Q5"] = r"ddsketch|kll"
_COMPARE_DISPATCH["Q5"] = compare_q5


# --- Q6: HLL distinct-symbol cardinality accuracy ---

def compare_q6(
    ground_truth: pd.DataFrame,
    sketch_data: pd.DataFrame,
    *,
    day: str = "",
    skip_warmup_windows: int = 0,
) -> dict:
    snap = get_best_snapshot_for_query(sketch_data, "Q6")
    gt = _select_evaluation_window(ground_truth, WINDOW_5MIN_MS, skip_warmup_windows)
    if gt.empty or "exact_count" not in gt.columns:
        return {"metric": "hll_max_rel_err", "value": 1.0, "threshold": 0.02, "pass": 0}
    exact = float(gt["exact_count"].iloc[0])
    if exact <= 0:
        return {"metric": "hll_max_rel_err", "value": 1.0, "threshold": 0.02, "pass": 0}
    est = extract_hll_cardinality(snap)
    rel_err = abs(est - exact) / exact
    return {
        "metric": "hll_max_rel_err",
        "value": float(rel_err),
        "threshold": 0.02,
        "pass": int(rel_err < 0.02),
    }


_SKETCH_METRIC_PATTERN["Q6"] = r"hll_cardinality"
_COMPARE_DISPATCH["Q6"] = compare_q6


if __name__ == "__main__":
    main()
