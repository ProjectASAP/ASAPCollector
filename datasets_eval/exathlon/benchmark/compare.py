from __future__ import annotations

"""Compare Prometheus sketch scrapes to offline ground truth (exathlon benchmark).

Each query has a registered ``_COMPARE_DISPATCH[qN]`` function that:
  1. Receives the ground-truth DataFrame and the best Prometheus scrape snapshot.
  2. Extracts sketch estimates via the ``extract_*`` helpers.
  3. Returns a flat dict of accuracy metrics that is written as a one-row CSV.
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd
import scipy.stats

_BENCH_ROOT = Path(__file__).resolve().parent
if str(_BENCH_ROOT) not in sys.path:
    sys.path.insert(0, str(_BENCH_ROOT))

from ground_truth.common import WINDOW_5MIN_S, WINDOW_15MIN_S, TOP_K_ENTITIES, TOP_K_METRICS
from common import METRIC_NAME, file_tag_safe

# Prometheus metric name prefix (dots become underscores).
_PROM_PREFIX = METRIC_NAME.replace(".", "_")  # "system_telemetry"

# Maps query ID → regex matching its primary sketch metric name in Prometheus.
_SKETCH_METRIC_PATTERN: dict[str, str] = {
    "Q1": rf"{_PROM_PREFIX}_(?:ddsketch|kll)",
    "Q3": rf"{_PROM_PREFIX}_countsketch_partition",
    "Q4": rf"{_PROM_PREFIX}_(?:ddsketch|kll)",
    "Q5": rf"{_PROM_PREFIX}_(?:ddsketch|kll)",
    "Q6": rf"{_PROM_PREFIX}_hll_cardinality",
    "Q7": rf"{_PROM_PREFIX}_countsketch_partition",
    "Q8": rf"{_PROM_PREFIX}_(?:ddsketch|kll)",
    "Q9": rf"{_PROM_PREFIX}_hll_cardinality",
}

# Compare function registry.
_COMPARE_DISPATCH: dict[str, Callable[..., dict]] = {}


# ---------------------------------------------------------------------------
# Snapshot selection
# ---------------------------------------------------------------------------

def get_latest_scrape_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    wall_ns = pd.to_numeric(df["scrape_wall_ns"], errors="coerce")
    df = df[wall_ns.notna()].copy()
    wall_ns = wall_ns[wall_ns.notna()]
    if df.empty:
        return df
    return df[wall_ns == wall_ns.max()]


def get_best_snapshot_for_query(sketch_data: pd.DataFrame, query_id: str) -> pd.DataFrame:
    pattern = _SKETCH_METRIC_PATTERN.get(query_id)
    if pattern is None or sketch_data.empty:
        return get_latest_scrape_snapshot(sketch_data)
    metrics = sketch_data["metric"].astype(str)
    relevant = sketch_data[metrics.str.contains(pattern, regex=True, na=False)]
    if relevant.empty:
        return get_latest_scrape_snapshot(sketch_data)
    # Prefer temporal alignment: pick the newest scrape that already contains
    # non-zero relevant sketch values (if any), otherwise the newest relevant
    # scrape, and finally the newest scrape overall.
    numeric = pd.to_numeric(relevant["value"], errors="coerce").fillna(0)
    non_zero = relevant[numeric > 0]
    if not non_zero.empty:
        wall_ns = pd.to_numeric(non_zero["scrape_wall_ns"], errors="coerce")
        wall_ns = wall_ns[wall_ns.notna()]
        if not wall_ns.empty:
            best_ts = wall_ns.max()
            return sketch_data[pd.to_numeric(sketch_data["scrape_wall_ns"], errors="coerce") == best_ts]

    rel_wall_ns = pd.to_numeric(relevant["scrape_wall_ns"], errors="coerce")
    rel_wall_ns = rel_wall_ns[rel_wall_ns.notna()]
    if not rel_wall_ns.empty:
        best_ts = rel_wall_ns.max()
        return sketch_data[pd.to_numeric(sketch_data["scrape_wall_ns"], errors="coerce") == best_ts]

    return get_latest_scrape_snapshot(sketch_data)


# ---------------------------------------------------------------------------
# Generic extraction helpers
# ---------------------------------------------------------------------------

def _detect_sketch_flavor(df: pd.DataFrame) -> str:
    if df.empty:
        return ""
    metrics = df["metric"].astype(str)
    if metrics.str.contains("_ddsketch", regex=False, na=False).any():
        return "ddsketch"
    if metrics.str.contains("_kll", regex=False, na=False).any():
        return "kll"
    return ""


def _quantile_from_labels(labels: dict) -> float | None:
    for key in ("ddsketch.quantile", "ddsketch_quantile", "kll.quantile", "kll_quantile"):
        if key not in labels:
            continue
        try:
            return float(labels[key])
        except (TypeError, ValueError):
            continue
    return None


def extract_sketch_quantile_by_group(
    df: pd.DataFrame,
    q_target: float,
    group_keys: tuple[str, ...] = ("entity", "metric_base"),
    tol: float = 1e-4,
) -> pd.DataFrame:
    """Extract sketch quantile estimates grouped by ``group_keys``.

    Returns DataFrame with columns = group_keys + ['v'].
    """
    flavor = _detect_sketch_flavor(df)
    if not flavor:
        return pd.DataFrame(columns=list(group_keys) + ["v"])
    metric_substr = f"_{flavor}"
    rows: list[dict] = []
    for _, record in df.iterrows():
        if metric_substr not in str(record.get("metric", "")):
            continue
        try:
            labels = json.loads(record["labels"])
        except (json.JSONDecodeError, KeyError, TypeError):
            continue
        qq = _quantile_from_labels(labels)
        if qq is None or abs(qq - q_target) > tol:
            continue
        row: dict = {}
        for k in group_keys:
            v = labels.get(k)
            if v is None:
                break
            row[k] = str(v)
        else:
            try:
                row["v"] = float(record["value"])
            except (TypeError, ValueError):
                continue
            rows.append(row)
    if not rows:
        return pd.DataFrame(columns=list(group_keys) + ["v"])
    result = pd.DataFrame(rows)
    return result.groupby(list(group_keys), as_index=False)["v"].mean()


def extract_countsketch_estimates(df: pd.DataFrame) -> pd.DataFrame:
    """Return DataFrame with columns ['key', 'est'] from CountSketch partition rows."""
    rows: list[dict] = []
    for _, record in df.iterrows():
        if "countsketch_partition" not in str(record.get("metric", "")):
            continue
        try:
            labels = json.loads(record["labels"])
        except (json.JSONDecodeError, KeyError, TypeError):
            continue
        pk = labels.get("partition_key", "")
        try:
            est = float(record["value"])
        except (TypeError, ValueError):
            continue
        rows.append({"key": pk, "est": est})
    if not rows:
        return pd.DataFrame(columns=["key", "est"])
    return pd.DataFrame(rows).groupby("key", as_index=False)["est"].max()


def extract_hll_cardinality_by_label(
    df: pd.DataFrame,
    label_key: str | None = None,
) -> dict[str, float]:
    """Return {label_value: hll_estimate_count} for HLL cardinality rows.

    If ``label_key`` is None, returns {'': total_non_zero_count}.
    """
    result: dict[str, float] = {}
    for _, record in df.iterrows():
        if "_hll_cardinality" not in str(record.get("metric", "")):
            continue
        try:
            v = float(record["value"])
        except (TypeError, ValueError):
            continue
        if v <= 0:
            continue
        if label_key is None:
            result[""] = result.get("", 0.0) + 1.0
        else:
            try:
                labels = json.loads(record["labels"])
            except (json.JSONDecodeError, KeyError, TypeError):
                continue
            lv = str(labels.get(label_key, ""))
            result[lv] = result.get(lv, 0.0) + v
    return result


# ---------------------------------------------------------------------------
# Q1 — Quantile accuracy per (entity, metric_base)
# ---------------------------------------------------------------------------

def _compare_q1(ground_truth: pd.DataFrame, sketch: pd.DataFrame, **_) -> dict:
    _nan3 = {"frac_q50_lt_1pct": float("nan"), "frac_q95_lt_1pct": float("nan"),
             "frac_q99_lt_1pct": float("nan")}
    if ground_truth.empty:
        return _nan3

    # Q1 benchmark target is 5-minute windows.  The ground_truth DataFrame has
    # already been filtered to the replayed time range by run_comparison, so
    # taking .max() here gives the last window within that range — not the last
    # window of the entire 60-minute file.
    gt_5m = ground_truth[ground_truth["window_size_s"] == WINDOW_5MIN_S].copy()
    if gt_5m.empty:
        return _nan3
    last_ws = int(gt_5m["window_start_s"].max())
    gt = gt_5m[gt_5m["window_start_s"] == last_ws].copy()

    def _frac_within(q_target: float, gt_col: str,
                     q_fallback: float | None = None,
                     gt_fallback_col: str | None = None) -> float:
        """Fraction of (entity, metric_base) pairs whose sketch estimate is
        within 1 % relative error of the exact ground-truth quantile.

        When the sketch has no data at ``q_target`` (e.g. the collector was
        not configured to emit p95) it retries with ``q_fallback`` and uses
        ``gt_fallback_col`` for the exact reference so the comparison stays
        apples-to-apples.  The GT must contain ``gt_fallback_col`` (add p90 to
        q1.py before using the p90 fallback).
        """
        sk = extract_sketch_quantile_by_group(sketch, q_target, ("entity", "metric_base"))
        col_used = gt_col
        if sk.empty and q_fallback is not None and gt_fallback_col is not None:
            if gt_fallback_col in gt.columns:
                sk = extract_sketch_quantile_by_group(
                    sketch, q_fallback, ("entity", "metric_base")
                )
                col_used = gt_fallback_col
        if sk.empty or gt.empty or col_used not in gt.columns:
            return float("nan")
        merged = gt.merge(sk, on=["entity", "metric_base"], how="inner")
        if merged.empty:
            return float("nan")
        exact = merged[col_used].to_numpy(dtype=np.float64)
        est = merged["v"].to_numpy(dtype=np.float64)
        nonzero = exact != 0
        rel_err = np.abs(est[nonzero] - exact[nonzero]) / np.abs(exact[nonzero])
        return float(np.mean(rel_err < 0.01)) if nonzero.any() else float("nan")

    return {
        "frac_q50_lt_1pct": _frac_within(0.50, "p50"),
        # Try sketch p95; fall back to sketch p90 vs GT p90 when p95 is absent
        # (some DDSketch collector builds omit 0.95 from their quantile grid).
        "frac_q95_lt_1pct": _frac_within(0.95, "p95",
                                          q_fallback=0.90, gt_fallback_col="p90"),
        "frac_q99_lt_1pct": _frac_within(0.99, "p99"),
    }


_COMPARE_DISPATCH["Q1"] = _compare_q1


# ---------------------------------------------------------------------------
# Q3 — Top-K overlap and rank correlation
# ---------------------------------------------------------------------------

def _compare_q3(ground_truth: pd.DataFrame, sketch: pd.DataFrame, **_) -> dict:
    if ground_truth.empty:
        return {"topk_overlap": float("nan"), "rank_correlation": float("nan")}

    # Ground truth: last window only.
    last_ws = int(ground_truth["window_start_s"].max())
    gt_win = ground_truth[ground_truth["window_start_s"] == last_ws].copy()
    gt_top = gt_win.nsmallest(TOP_K_METRICS, "rank")["key"].tolist()

    sk = extract_countsketch_estimates(sketch)
    if sk.empty:
        return {"topk_overlap": float("nan"), "rank_correlation": float("nan")}

    sk_sorted = sk.sort_values("est", ascending=False).head(TOP_K_METRICS)
    sk_top = sk_sorted["key"].tolist()

    overlap = len(set(gt_top) & set(sk_top)) / max(len(gt_top), 1)

    # Spearman rank correlation on common keys.
    common = [k for k in gt_top if k in set(sk_top)]
    if len(common) >= 2:
        gt_ranks = [gt_top.index(k) + 1 for k in common]
        sk_ranks = [sk_top.index(k) + 1 if k in sk_top else len(sk_top) + 1 for k in common]
        rho, _ = scipy.stats.spearmanr(gt_ranks, sk_ranks)
        rho = float(rho) if not np.isnan(rho) else 0.0
    else:
        rho = float("nan")

    return {"topk_overlap": float(overlap), "rank_correlation": float(rho)}


_COMPARE_DISPATCH["Q3"] = _compare_q3


# ---------------------------------------------------------------------------
# Q4 — Min / max relative error
# ---------------------------------------------------------------------------

def _compare_q4(ground_truth: pd.DataFrame, sketch: pd.DataFrame, **_) -> dict:
    if ground_truth.empty:
        return {"frac_min_lt_2pct": float("nan"), "frac_max_lt_2pct": float("nan")}

    gt = ground_truth[ground_truth["window_size_s"] == WINDOW_5MIN_S].copy()
    gt_last = gt[gt["window_start_s"] == gt["window_start_s"].max()]

    sk_min = extract_sketch_quantile_by_group(sketch, 0.0, ("entity", "metric_base"))
    sk_max = extract_sketch_quantile_by_group(sketch, 1.0, ("entity", "metric_base"))

    def _frac(sk_df: pd.DataFrame, gt_col: str, threshold: float) -> float:
        if sk_df.empty or gt_last.empty:
            return float("nan")
        merged = gt_last.merge(sk_df, on=["entity", "metric_base"], how="inner")
        if merged.empty:
            return float("nan")
        exact = merged[gt_col].to_numpy(dtype=np.float64)
        est = merged["v"].to_numpy(dtype=np.float64)
        nonzero = exact != 0
        rel_err = np.abs(est[nonzero] - exact[nonzero]) / np.abs(exact[nonzero])
        return float(np.mean(rel_err < threshold)) if nonzero.any() else float("nan")

    return {
        "frac_min_lt_2pct": _frac(sk_min, "exact_min", 0.02),
        "frac_max_lt_2pct": _frac(sk_max, "exact_max", 0.02),
    }


_COMPARE_DISPATCH["Q4"] = _compare_q4


# ---------------------------------------------------------------------------
# Q5 — IQR accuracy (fraction of series within 10% relative error)
# ---------------------------------------------------------------------------

def _compare_q5(ground_truth: pd.DataFrame, sketch: pd.DataFrame, **_) -> dict:
    if ground_truth.empty:
        return {"frac_iqr_lt_10pct": float("nan")}

    gt_last = ground_truth[ground_truth["window_start_s"] == ground_truth["window_start_s"].max()]

    sk_q1 = extract_sketch_quantile_by_group(sketch, 0.25, ("entity", "metric_base"))
    sk_q3 = extract_sketch_quantile_by_group(sketch, 0.75, ("entity", "metric_base"))

    if sk_q1.empty or sk_q3.empty or gt_last.empty:
        return {"frac_iqr_lt_10pct": float("nan")}

    sk_iqr = sk_q1.merge(sk_q3, on=["entity", "metric_base"], suffixes=("_q1", "_q3"))
    sk_iqr["sketch_iqr"] = sk_iqr["v_q3"] - sk_iqr["v_q1"]

    merged = gt_last.merge(sk_iqr, on=["entity", "metric_base"], how="inner")
    if merged.empty:
        return {"frac_iqr_lt_10pct": float("nan")}

    exact = merged["iqr"].to_numpy(dtype=np.float64)
    est = merged["sketch_iqr"].to_numpy(dtype=np.float64)
    nonzero = exact != 0
    rel_err = np.abs(est[nonzero] - exact[nonzero]) / np.abs(exact[nonzero])
    frac = float(np.mean(rel_err < 0.10)) if nonzero.any() else float("nan")

    return {"frac_iqr_lt_10pct": frac}


_COMPARE_DISPATCH["Q5"] = _compare_q5


# ---------------------------------------------------------------------------
# Q6 — HLL distinct-count relative error
# ---------------------------------------------------------------------------

def _compare_q6(ground_truth: pd.DataFrame, sketch_full: pd.DataFrame, **_) -> dict:
    """Q6 uses full sketch_full (all scrapes) to pick the best estimate."""
    if ground_truth.empty:
        return {"hll_rel_err": float("nan")}

    last_ws = int(ground_truth["window_start_s"].max())
    gt_count = float(
        ground_truth[ground_truth["window_start_s"] == last_ws]["exact_distinct_count"].iloc[0]
    )

    hll_vals = extract_hll_cardinality_by_label(sketch_full, label_key=None)
    if not hll_vals:
        return {"hll_rel_err": float("nan")}

    hll_est = hll_vals.get("", 0.0)
    rel_err = abs(hll_est - gt_count) / gt_count if gt_count > 0 else float("nan")
    return {"hll_rel_err": float(rel_err)}


_COMPARE_DISPATCH["Q6"] = _compare_q6


# ---------------------------------------------------------------------------
# Q7 — Top-K entity overlap
# ---------------------------------------------------------------------------

def _compare_q7(ground_truth: pd.DataFrame, sketch: pd.DataFrame, **_) -> dict:
    if ground_truth.empty:
        return {"entity_topk_overlap": float("nan")}

    last_ws = int(ground_truth["window_start_s"].max())
    gt_top = (
        ground_truth[ground_truth["window_start_s"] == last_ws]
        .nsmallest(TOP_K_ENTITIES, "rank")["entity"].tolist()
    )

    sk = extract_countsketch_estimates(sketch)
    if sk.empty:
        return {"entity_topk_overlap": float("nan")}

    # For Q7, the partition_key encodes entity only: "entity=N"
    def _extract_entity(key: str) -> str:
        for part in key.split(";"):
            part = part.strip()
            if part.startswith("entity="):
                return part[len("entity="):]
        return key

    sk["entity"] = sk["key"].map(_extract_entity)
    sk_top = sk.sort_values("est", ascending=False).head(TOP_K_ENTITIES)["entity"].tolist()

    overlap = len(set(gt_top) & set(sk_top)) / max(len(gt_top), 1)
    return {"entity_topk_overlap": float(overlap)}


_COMPARE_DISPATCH["Q7"] = _compare_q7


# ---------------------------------------------------------------------------
# Q8 — Quantile drift error
# ---------------------------------------------------------------------------

def _compare_q8(ground_truth: pd.DataFrame, sketch: pd.DataFrame, **_) -> dict:
    if ground_truth.empty:
        return {"frac_drift_p95_lt_20pct": float("nan")}

    # Use last window pair.
    last_ws = int(ground_truth["window_start_s"].max())
    gt_last = ground_truth[ground_truth["window_start_s"] == last_ws]

    sk_p95 = extract_sketch_quantile_by_group(sketch, 0.95, ("entity", "metric_base"))
    sk_p95_prev = extract_sketch_quantile_by_group(sketch, 0.95, ("entity", "metric_base"))

    if sk_p95.empty or gt_last.empty:
        return {"frac_drift_p95_lt_20pct": float("nan")}

    merged = gt_last.merge(sk_p95, on=["entity", "metric_base"], how="inner")
    if merged.empty:
        return {"frac_drift_p95_lt_20pct": float("nan")}

    # We compare the sketch p95 drift (prev vs curr) relative to exact drift.
    exact_drift = merged["drift_p95"].to_numpy(dtype=np.float64)
    # Use the absolute difference between sketch p95 and exact prev_p95 as proxy for drift.
    sketch_curr = merged["v"].to_numpy(dtype=np.float64)
    exact_prev = merged["prev_p95"].to_numpy(dtype=np.float64)
    sketch_drift = np.abs(sketch_curr - exact_prev)

    nonzero = exact_drift > 0
    if not nonzero.any():
        return {"frac_drift_p95_lt_20pct": float("nan")}
    rel_err = np.abs(sketch_drift[nonzero] - exact_drift[nonzero]) / exact_drift[nonzero]
    frac = float(np.mean(rel_err < 0.20))
    return {"frac_drift_p95_lt_20pct": frac}


_COMPARE_DISPATCH["Q8"] = _compare_q8


# ---------------------------------------------------------------------------
# Q9 — Saturation ratio accuracy (mean absolute error)
# ---------------------------------------------------------------------------

def _compare_q9(ground_truth: pd.DataFrame, sketch: pd.DataFrame, **_) -> dict:
    if ground_truth.empty:
        return {"sat_ratio_mae": float("nan")}

    last_ws = int(ground_truth["window_start_s"].max())
    gt_last = ground_truth[ground_truth["window_start_s"] == last_ws].copy()

    # HLL estimate per entity = distinct saturated metric count per entity.
    hll_by_entity = extract_hll_cardinality_by_label(sketch, label_key="entity")

    errors = []
    for _, row in gt_last.iterrows():
        entity = str(row["entity"])
        hll_est = hll_by_entity.get(entity, 0.0)
        total = float(row["total_active_metric_count"])
        exact_ratio = float(row["exact_saturation_ratio"])
        sketch_ratio = hll_est / total if total > 0 else 0.0
        errors.append(abs(sketch_ratio - exact_ratio))

    return {"sat_ratio_mae": float(np.mean(errors)) if errors else float("nan")}


_COMPARE_DISPATCH["Q9"] = _compare_q9


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def _filter_gt_to_replay_range(
    ground_truth: pd.DataFrame,
    accuracy_minutes: int,
    replay_cutoff_s: int | None = None,
) -> pd.DataFrame:
    """Restrict ground-truth rows to windows that fall within the replayed
    time range.

    ``ref_ts`` is the earliest ``window_start_s`` found in the finest-grained
    window-size group.  Using the global minimum would anchor on a large window
    bucket (e.g. 1-hour) that may pre-date the first actual data point by up to
    one window size, pushing the cutoff before all fine-grained windows.

    When ``accuracy_minutes`` is 0 (full-file replay) or the DataFrame has no
    ``window_start_s`` column the original DataFrame is returned unchanged.
    """
    if (
        (accuracy_minutes <= 0 and replay_cutoff_s is None)
        or "window_start_s" not in ground_truth.columns
    ):
        return ground_truth

    cutoff_ws: int | None = None
    if "window_size_s" in ground_truth.columns:
        if accuracy_minutes > 0:
            # Anchor on the finest (smallest) window size so that large windows
            # (e.g. 1-hour buckets) don't push the reference timestamp too far back.
            min_ws_size = int(ground_truth["window_size_s"].min())
            ref_ts = int(
                ground_truth.loc[
                    ground_truth["window_size_s"] == min_ws_size, "window_start_s"
                ].min()
            )
            cutoff_ws = ref_ts + accuracy_minutes * 60
        if replay_cutoff_s is not None:
            cutoff_ws = replay_cutoff_s if cutoff_ws is None else min(cutoff_ws, replay_cutoff_s)
        if cutoff_ws is None:
            return ground_truth

        # Require the ENTIRE window to fall within the replay range.  A window
        # whose end (window_start_s + window_size_s) exceeds the cutoff was only
        # partially ingested by the sketch, so the GT value (computed over the
        # full window from the raw file) would not match what the sketch saw.
        filtered = ground_truth[
            ground_truth["window_start_s"] + ground_truth["window_size_s"] <= cutoff_ws
        ]
    else:
        if accuracy_minutes > 0:
            ref_ts = int(ground_truth["window_start_s"].min())
            cutoff_ws = ref_ts + accuracy_minutes * 60
        if replay_cutoff_s is not None:
            cutoff_ws = replay_cutoff_s if cutoff_ws is None else min(cutoff_ws, replay_cutoff_s)
        if cutoff_ws is None:
            return ground_truth
        # For single-window-size queries, exclude the very last window in the
        # filtered range: it is likely partially covered by the replay cutoff, so
        # the GT (full-file) value differs from what the sketch ingested.
        candidate = ground_truth[ground_truth["window_start_s"] < cutoff_ws]
        if candidate["window_start_s"].nunique() > 1:
            last_start = candidate["window_start_s"].max()
            filtered = candidate[candidate["window_start_s"] < last_start]
        else:
            filtered = candidate

    return filtered if not filtered.empty else ground_truth


def _read_replay_cutoff_s(send_times_path: Path | None) -> int | None:
    """Return last emitted event time (seconds) from send_times.csv, or None."""
    if send_times_path is None or not send_times_path.is_file():
        return None
    try:
        send_times = pd.read_csv(send_times_path, usecols=["event_time_ns"])
    except (ValueError, FileNotFoundError, pd.errors.EmptyDataError):
        return None
    if send_times.empty:
        return None
    event_ns = pd.to_numeric(send_times["event_time_ns"], errors="coerce").dropna()
    if event_ns.empty:
        return None
    return int(event_ns.max() // 1_000_000_000)


def run_comparison(
    query_id: str,
    file_tag: str,
    ground_truth_dir: Path,
    sketch_output_dir: Path,
    comparison_out_dir: Path,
    accuracy_minutes: int = 0,
    send_times_path: Path | None = None,
) -> None:
    tag = file_tag_safe(file_tag)
    gt_path = ground_truth_dir / query_id / f"{tag}.csv"
    sketch_path = sketch_output_dir / query_id / f"{tag}.csv"

    if not gt_path.is_file():
        return

    ground_truth = pd.read_csv(gt_path)
    # Restrict to the portion of event time actually replayed so that the
    # "last window" selected by each compare function corresponds to the data
    # the sketch has ingested, not the end of the full 60-minute file.
    replay_cutoff_s = _read_replay_cutoff_s(send_times_path)
    ground_truth = _filter_gt_to_replay_range(
        ground_truth,
        accuracy_minutes,
        replay_cutoff_s=replay_cutoff_s,
    )

    sketch_data = (
        pd.read_csv(sketch_path, on_bad_lines="skip", low_memory=False)
        if sketch_path.is_file()
        else pd.DataFrame()
    )

    comparison_out_dir.mkdir(parents=True, exist_ok=True)
    fn = _COMPARE_DISPATCH.get(query_id)
    if fn is None:
        return

    sketch_snapshot = get_best_snapshot_for_query(sketch_data, query_id)
    if query_id == "Q6":
        result = fn(ground_truth, sketch_data, file=tag)
    else:
        result = fn(ground_truth, sketch_snapshot, file=tag)

    out_row = {"query": query_id, "file": tag}
    for metric_name, value in result.items():
        out_row["metric"] = metric_name
        out_row["value"] = value
        threshold, pass_val = _metric_threshold(query_id, metric_name, value)
        out_row["threshold"] = threshold
        out_row["pass"] = pass_val

    # Write one row per metric.
    rows = []
    for metric_name, value in result.items():
        threshold, pass_val = _metric_threshold(query_id, metric_name, value)
        rows.append({"query": query_id, "file": tag, "metric": metric_name,
                     "value": value, "threshold": threshold, "pass": pass_val})

    pd.DataFrame(rows).to_csv(
        comparison_out_dir / f"{query_id}_{tag}.csv", index=False
    )


def _metric_threshold(query_id: str, metric: str, value: float) -> tuple[float, bool]:
    """Return (threshold, pass) for a given metric."""
    # Lower-is-better metrics (errors): threshold is max acceptable.
    lower_better = {
        "hll_rel_err": 0.05,
        "sat_ratio_mae": 0.05,
    }
    # Higher-is-better metrics (fractions, overlaps): threshold is min acceptable.
    higher_better = {
        "frac_q50_lt_1pct": 0.90,
        "frac_q95_lt_1pct": 0.90,
        "frac_q99_lt_1pct": 0.85,
        "topk_overlap": 0.80,
        "rank_correlation": 0.70,
        "frac_min_lt_2pct": 0.90,
        "frac_max_lt_2pct": 0.90,
        "frac_iqr_lt_10pct": 0.85,
        "entity_topk_overlap": 0.80,
        "frac_drift_p95_lt_20pct": 0.80,
    }
    if metric in lower_better:
        thr = lower_better[metric]
        return thr, bool(not np.isnan(value) and value <= thr)
    if metric in higher_better:
        thr = higher_better[metric]
        return thr, bool(not np.isnan(value) and value >= thr)
    return float("nan"), False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare exathlon sketch scrapes to ground truth."
    )
    parser.add_argument("--query", default="Q1")
    parser.add_argument("--file", default="app1/1_0_10000_17")
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
    parser.add_argument(
        "--accuracy-minutes",
        type=int,
        default=0,
        help=(
            "Minutes of event time that were replayed (0 = full file). "
            "Ground-truth windows beyond this range are excluded from comparison "
            "so that the 'last window' aligns with the sketch's ingested data."
        ),
    )
    parser.add_argument(
        "--send-times",
        type=Path,
        default=Path(__file__).resolve().parent / "results" / "send_times.csv",
        help=(
            "Path to send_times.csv. When present, the observed max event time "
            "is used as an additional replay cutoff for GT window filtering."
        ),
    )
    args = parser.parse_args()
    run_comparison(
        args.query,
        args.file,
        args.gt_dir,
        args.sketch_dir,
        args.out_dir,
        accuracy_minutes=args.accuracy_minutes,
        send_times_path=args.send_times,
    )


if __name__ == "__main__":
    main()
