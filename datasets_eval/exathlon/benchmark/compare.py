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

from ground_truth.common import (
    WINDOW_5MIN_S,
    WINDOW_15MIN_S,
    TOP_K_ENTITIES,
    TOP_K_METRICS,
    _stream_long_chunks,
)
from common import METRIC_NAME, file_csv_path, file_tag_safe
from common import (  # Q1_KLL
    parse_metric_column,  # Q1_KLL
    Q1_KLL_STREAMS,  # Q1_KLL
    Q1_KLL_ERROR_TARGET,  # Q1_KLL
    Q1_KLL_RANK_GRID_STEP,  # Q1_KLL
    q1_kll_report_value,  # Q1_KLL
)  # Q1_KLL

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
    "Q1_KLL": rf"{_PROM_PREFIX}_kll",  # Q1_KLL — streaming KLL metrics with quantile in labels
}

# Compare function registry.
# Q1 returns list[dict] (one row per time series); all others return dict.
_COMPARE_DISPATCH: dict[str, Callable[..., dict | list[dict]]] = {}


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


def _sketch_labels_have_key(df: pd.DataFrame, key: str) -> bool:
    """Return True if any row in *df* has *key* present in its parsed labels."""
    for _, record in df.iterrows():
        try:
            labels = json.loads(record["labels"])
        except (json.JSONDecodeError, KeyError, TypeError):
            continue
        if key in labels:
            return True
    return False


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
# Q1 — Quantile accuracy per (entity, metric_base, aggregation)
# ---------------------------------------------------------------------------

# Grouping key for Q1: aggregation must be preserved so that e.g. the "mean"
# and "p99" series of the same metric are evaluated independently.  Mixing
# them would produce a distribution over heterogeneous aggregated values,
# making the quantile error estimate statistically meaningless.
_Q1_GROUP: tuple[str, ...] = ("entity", "metric_base", "aggregation")


def _compare_q1(ground_truth: pd.DataFrame, sketch: pd.DataFrame, **_) -> list[dict]:
    """Quantile accuracy per (entity, metric_base, aggregation) time series.

    Returns a list of dicts — one per (series, metric) combination — with keys:
        entity, metric_base, aggregation, metric, value, threshold, pass
    """
    if ground_truth.empty:
        return []

    # Q1 benchmark target is 5-minute windows.  The ground_truth DataFrame has
    # already been filtered to the replayed time range by run_comparison, so
    # taking .max() here gives the last window within that range — not the last
    # window of the entire 60-minute file.
    gt_5m = ground_truth[ground_truth["window_size_s"] == WINDOW_5MIN_S].copy()
    if gt_5m.empty:
        return []
    last_ws = int(gt_5m["window_start_s"].max())
    gt = gt_5m[gt_5m["window_start_s"] == last_ws].copy()

    # Detect whether the sketch data carries an 'aggregation' label.
    # Collectors configured with group_by=("entity","metric_base") omit it.
    # Fall back to the 2-key group so existing data produces non-NaN results;
    # the merge still fans out to all matching (entity, metric_base, aggregation)
    # rows from the ground truth.
    _eff_group: tuple[str, ...] = (
        _Q1_GROUP if _sketch_labels_have_key(sketch, "aggregation")
        else ("entity", "metric_base")
    )

    # Extract sketch estimates per effective group.
    sk50 = extract_sketch_quantile_by_group(sketch, 0.50, _eff_group)
    sk95 = extract_sketch_quantile_by_group(sketch, 0.95, _eff_group)
    gt_col95 = "p95"
    # Fall back to p90 vs GT p90 when the collector omits 0.95 from its grid.
    if sk95.empty and "p90" in gt.columns:
        sk95 = extract_sketch_quantile_by_group(sketch, 0.90, _eff_group)
        gt_col95 = "p90"
    sk99 = extract_sketch_quantile_by_group(sketch, 0.99, _eff_group)

    def _per_series(sk_df: pd.DataFrame, gt_col: str) -> dict[tuple, float]:
        """Return {(entity, metric_base, aggregation): 1.0|0.0} for rel-err < 1%."""
        if sk_df.empty or gt_col not in gt.columns:
            return {}
        # Merge on the effective group (may be 2- or 3-key).  When using the
        # 2-key fallback, each sketch row fans out to all matching aggregation
        # variants in the ground truth — a single sketch value is compared
        # against each sub-series independently.
        merged = gt.merge(sk_df, on=list(_eff_group), how="inner")
        out: dict[tuple, float] = {}
        for _, row in merged.iterrows():
            exact = float(row[gt_col])
            if exact == 0.0:
                continue
            rel_err = abs(float(row["v"]) - exact) / abs(exact)
            agg = str(row["aggregation"]) if "aggregation" in row.index else ""
            out[(str(row["entity"]), str(row["metric_base"]), agg)] = (
                1.0 if rel_err < 0.01 else 0.0
            )
        return out

    acc50 = _per_series(sk50, "p50")
    acc95 = _per_series(sk95, gt_col95)
    acc99 = _per_series(sk99, "p99")

    # The dynamic GT builders always include 'aggregation'; the canonical on-disk
    # CSV (group_by entity+metric_base only) may not.  Synthesise a placeholder
    # so the output schema is consistent.
    if "aggregation" not in gt.columns:
        gt = gt.copy()
        gt["aggregation"] = ""
    series_keys = (
        gt[["entity", "metric_base", "aggregation"]]
        .drop_duplicates()
        .itertuples(index=False)
    )
    rows: list[dict] = []
    for s in series_keys:
        key = (str(s.entity), str(s.metric_base), str(s.aggregation))
        for metric_name, acc_map in (
            ("frac_q50_lt_1pct", acc50),
            ("frac_q95_lt_1pct", acc95),
            ("frac_q99_lt_1pct", acc99),
        ):
            value = acc_map.get(key, float("nan"))
            thr, pass_val = _metric_threshold("Q1", metric_name, value)
            rows.append({
                "entity":      key[0],
                "metric_base": key[1],
                "aggregation": key[2],
                "metric":      metric_name,
                "value":       value,
                "threshold":   thr,
                "pass":        pass_val,
            })
    return rows


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
# Q1_KLL — KLL sketch accuracy for three GC/CPU streams  # Q1_KLL
# ---------------------------------------------------------------------------

# Suffix → quantile value for KLL per-quantile metric names.  # Q1_KLL
_KLL_SUFFIX_TO_Q: dict[str, float] = {"_p50": 0.50, "_p95": 0.95, "_p99": 0.99}  # Q1_KLL


def extract_kll_quantiles(  # Q1_KLL
    df: pd.DataFrame,
    group_keys: tuple[str, ...] = ("entity", "metric_base"),
) -> pd.DataFrame:
    """Extract KLL quantile estimates from KLL gauge rows.

    The current Q1_KLL collector emits a single ``*_kll`` gauge metric and
    encodes the requested quantile in labels (``kll.quantile`` or the
    Prometheus-sanitized ``kll_quantile``). Older runs encoded the quantile in
    the metric-name suffix (``_p50``, ``_p95``, ``_p99``). Support both forms
    so comparison works across old and new benchmark outputs.

    Returns DataFrame with columns = group_keys + ['quantile', 'v'].
    """  # Q1_KLL
    rows: list[dict] = []  # Q1_KLL
    for _, record in df.iterrows():  # Q1_KLL
        metric = str(record.get("metric", ""))  # Q1_KLL
        try:  # Q1_KLL
            labels = json.loads(record["labels"])  # Q1_KLL
        except (json.JSONDecodeError, KeyError, TypeError):  # Q1_KLL
            continue  # Q1_KLL
        q = _quantile_from_labels(labels)  # Q1_KLL
        if q is None:  # Q1_KLL
            for suffix, qval in _KLL_SUFFIX_TO_Q.items():  # Q1_KLL
                if metric.endswith(suffix):  # Q1_KLL
                    q = qval  # Q1_KLL
                    break  # Q1_KLL
        if q is None:  # Q1_KLL
            continue  # Q1_KLL
        row: dict = {"quantile": q}  # Q1_KLL
        for k in group_keys:  # Q1_KLL
            v = labels.get(k)  # Q1_KLL
            if v is None:  # Q1_KLL
                break  # Q1_KLL
            row[k] = str(v)  # Q1_KLL
        else:  # Q1_KLL
            try:  # Q1_KLL
                row["v"] = float(record["value"])  # Q1_KLL
            except (TypeError, ValueError):  # Q1_KLL
                continue  # Q1_KLL
            rows.append(row)  # Q1_KLL
    if not rows:  # Q1_KLL
        return pd.DataFrame(columns=list(group_keys) + ["quantile", "v"])  # Q1_KLL
    result = pd.DataFrame(rows)  # Q1_KLL
    return result.groupby(list(group_keys) + ["quantile"], as_index=False)["v"].mean()  # Q1_KLL


def _kll_rank_error(  # Q1_KLL
    sketch_val: float,
    qvals: np.ndarray,
    q_target: float,
) -> float:  # Q1_KLL
    """Compute the rank error for a KLL sketch estimate.

    KLL's guarantee is in rank space: for target quantile *q_target*, the
    sketch returns a value *v* such that the true rank of *v* is within 1/k
    of *q_target*.  For zero-inflated and otherwise non-strictly-monotone
    distributions the correct check is interval-based:

        rank_error = max(0,  F(v−) − q,  q − F(v))

    where F(v) = fraction of values ≤ v  and  F(v−) = fraction of values < v.
    If *q_target* falls in [F(v−), F(v)] the error is zero (correct region).

    *qvals* is the 1001-point quantile grid stored in the kll-exact CSV
    (values at phi = 0, 0.001, 0.002, …, 1.0).
    """  # Q1_KLL
    if len(qvals) == 0:  # Q1_KLL
        return float("nan")  # Q1_KLL
    step = Q1_KLL_RANK_GRID_STEP  # Q1_KLL  — 0.001
    n = len(qvals)  # Q1_KLL  — 1001

    # F(v): fraction of distribution ≤ sketch_val.  # Q1_KLL
    # = phi at the last grid point whose value ≤ sketch_val.  # Q1_KLL
    right_idx = int(np.searchsorted(qvals, sketch_val, side="right")) - 1  # Q1_KLL
    if right_idx < 0:  # Q1_KLL
        rank_above = 0.0  # Q1_KLL
    elif right_idx >= n - 1:  # Q1_KLL
        rank_above = 1.0  # Q1_KLL
    else:  # Q1_KLL  — interpolate between adjacent grid points
        lo, hi = right_idx, right_idx + 1  # Q1_KLL
        if qvals[hi] == qvals[lo]:  # Q1_KLL  — flat (tie) region: go to upper boundary
            rank_above = hi * step  # Q1_KLL
        else:  # Q1_KLL
            t = (sketch_val - qvals[lo]) / (qvals[hi] - qvals[lo])  # Q1_KLL
            rank_above = (lo + t) * step  # Q1_KLL

    # F(v−): fraction of distribution < sketch_val.  # Q1_KLL
    # = phi just before the first grid point whose value ≥ sketch_val.  # Q1_KLL
    left_idx = int(np.searchsorted(qvals, sketch_val, side="left"))  # Q1_KLL
    if left_idx == 0:  # Q1_KLL
        rank_below = 0.0  # Q1_KLL
    elif left_idx >= n:  # Q1_KLL
        rank_below = 1.0  # Q1_KLL
    else:  # Q1_KLL
        lo_b = left_idx - 1  # Q1_KLL
        if qvals[left_idx] > qvals[lo_b]:  # Q1_KLL  — continuous region: interpolate
            t = (sketch_val - qvals[lo_b]) / (qvals[left_idx] - qvals[lo_b])  # Q1_KLL
            rank_below = (lo_b + t) * step  # Q1_KLL
        else:  # Q1_KLL  — sketch_val is inside a tie block: rank_below is the phi before the block
            rank_below = lo_b * step  # Q1_KLL

    return float(max(0.0, rank_below - q_target, q_target - rank_above))  # Q1_KLL


# Map label letters to Q1_KLL_STREAMS indices for result key construction.  # Q1_KLL
_Q1_KLL_STREAM_LABELS: tuple[str, ...] = ("A", "B", "C")  # Q1_KLL


def _compare_q1_kll(  # Q1_KLL
    ground_truth: pd.DataFrame,
    sketch: pd.DataFrame,
    **_,
) -> dict:
    """Compare KLL sketch quantile estimates to exact sort for three GC/CPU streams.

    Evaluates p50, p95, and p99 accuracy for the three streams defined in
    :data:`common.Q1_KLL_STREAMS` (jvmGCTime, PS-MarkSweep, cpuTime).

    Returns a flat dict with keys::

        stream_A_p50_err, stream_A_p95_err, stream_A_p99_err, stream_A_pass,
        stream_B_p50_err, stream_B_p95_err, stream_B_p99_err, stream_B_pass,
        stream_C_p50_err, stream_C_p95_err, stream_C_p99_err, stream_C_pass,
        overall_pass, sentinel_count

    Error semantics:
      * Streams A and C: relative error = |sketch - exact| / exact
      * Stream B (near-constant, exact==75): absolute error = |sketch - exact|
        (relative error is technically valid here but spec mandates absolute)

    Values for cpuTime (Stream C) are converted ns→s before error computation
    via :func:`common.q1_kll_report_value`.

    See also: :func:`ground_truth.q1.compute_q1_kll_exact`
    """  # Q1_KLL
    result: dict = {}  # Q1_KLL

    # Read sentinel_count from ground_truth if available.  # Q1_KLL
    if not ground_truth.empty and "sentinel_count" in ground_truth.columns:  # Q1_KLL
        try:  # Q1_KLL
            result["sentinel_count"] = float(  # Q1_KLL
                ground_truth["sentinel_count"].iloc[0]  # Q1_KLL
            )  # Q1_KLL
        except (IndexError, TypeError, ValueError):  # Q1_KLL
            result["sentinel_count"] = float("nan")  # Q1_KLL
    else:  # Q1_KLL
        result["sentinel_count"] = float("nan")  # Q1_KLL

    # Extract all KLL quantile estimates at once; filter per-stream and per-quantile below.  # Q1_KLL
    # KLL emits one gauge metric per quantile (_p50, _p95, _p99); quantile is in the name.  # Q1_KLL
    kll_df = extract_kll_quantiles(sketch, ("entity", "metric_base", "aggregation"))  # Q1_KLL

    stream_passes: list[bool] = []  # Q1_KLL

    for label, stream in zip(_Q1_KLL_STREAM_LABELS, Q1_KLL_STREAMS):  # Q1_KLL
        col = stream["column"]  # Q1_KLL
        parsed = parse_metric_column(col)  # Q1_KLL
        if parsed is None:  # Q1_KLL
            # Cannot parse column — mark stream as failed.  # Q1_KLL
            for phi in ("p50", "p95", "p99"):  # Q1_KLL
                result[f"stream_{label}_{phi}_err"] = float("nan")  # Q1_KLL
            result[f"stream_{label}_pass"] = 0.0  # Q1_KLL
            stream_passes.append(False)  # Q1_KLL
            continue  # Q1_KLL

        entity, metric_base, agg = parsed  # Q1_KLL

        # Load the rank grid for this stream from the ground-truth CSV.  # Q1_KLL
        # Falls back to an empty array when the column is absent (old GT files).  # Q1_KLL
        qvals: np.ndarray = np.array([])  # Q1_KLL
        if not ground_truth.empty and "rank_grid_json" in ground_truth.columns:  # Q1_KLL
            gt_row = ground_truth[ground_truth["column"] == col]  # Q1_KLL
            if not gt_row.empty:  # Q1_KLL
                try:  # Q1_KLL
                    qvals = np.asarray(  # Q1_KLL
                        json.loads(gt_row["rank_grid_json"].iloc[0]), dtype=np.float64  # Q1_KLL
                    )  # Q1_KLL
                except (json.JSONDecodeError, TypeError, ValueError):  # Q1_KLL
                    pass  # Q1_KLL

        phi_err: dict[str, float] = {}  # Q1_KLL
        for phi, q_target in (("p50", 0.50), ("p95", 0.95), ("p99", 0.99)):  # Q1_KLL
            # Filter KLL estimates to this stream's entity, metric_base, and quantile.  # Q1_KLL
            if not kll_df.empty:  # Q1_KLL
                rows_match = kll_df[  # Q1_KLL
                    (kll_df["entity"] == entity)  # Q1_KLL
                    & (kll_df["metric_base"] == metric_base)  # Q1_KLL
                    & (kll_df["aggregation"] == agg)  # Q1_KLL
                    & (kll_df["quantile"] == q_target)  # Q1_KLL
                ]  # Q1_KLL
            else:  # Q1_KLL
                rows_match = pd.DataFrame()  # Q1_KLL

            if rows_match.empty:  # Q1_KLL
                phi_err[phi] = float("nan")  # Q1_KLL
                continue  # Q1_KLL

            sketch_raw = float(rows_match["v"].iloc[0])  # Q1_KLL
            phi_err[phi] = _kll_rank_error(sketch_raw, qvals, q_target)  # Q1_KLL

        result[f"stream_{label}_p50_err"] = phi_err.get("p50", float("nan"))  # Q1_KLL
        result[f"stream_{label}_p95_err"] = phi_err.get("p95", float("nan"))  # Q1_KLL
        result[f"stream_{label}_p99_err"] = phi_err.get("p99", float("nan"))  # Q1_KLL

        # Stream passes when all three quantile errors are within the target.  # Q1_KLL
        errs = [phi_err.get(p, float("nan")) for p in ("p50", "p95", "p99")]  # Q1_KLL
        stream_ok = all(  # Q1_KLL
            not np.isnan(e) and e <= Q1_KLL_ERROR_TARGET  # Q1_KLL
            for e in errs  # Q1_KLL
        )  # Q1_KLL
        result[f"stream_{label}_pass"] = 1.0 if stream_ok else 0.0  # Q1_KLL
        stream_passes.append(stream_ok)  # Q1_KLL

    result["overall_pass"] = 1.0 if (stream_passes and all(stream_passes)) else 0.0  # Q1_KLL
    return result  # Q1_KLL


_COMPARE_DISPATCH["Q1_KLL"] = _compare_q1_kll  # Q1_KLL


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


def _build_q1_replay_relative_ground_truth(
    file_tag: str,
    replay_cutoff_s: int | None,
    accuracy_minutes: int,
    chunksize: int = 200,
) -> pd.DataFrame:
    """Compute Q1 exact quantiles using replay-relative 5-minute windows.

    The canonical Q1 ground truth uses Unix-epoch tumbling windows.  The
    benchmark DDSketch window processor flushes by collector wall-clock ticker,
    which makes its windows replay-relative rather than epoch-aligned.  This
    helper rebuilds Q1 over raw samples with window 0 anchored at the first raw
    replay timestamp so compare.py can evaluate the collector on matching
    replay-relative windows without overwriting the canonical GT CSV.
    """
    csv_path = file_csv_path(file_tag)
    if not csv_path.is_file():
        return pd.DataFrame()

    acc: dict[tuple[str, str, str, int], list[float]] = {}
    origin_s: int | None = None
    cutoff_s: int | None = None
    max_seen_s: int | None = None

    for chunk in _stream_long_chunks(csv_path, chunksize):
        if chunk.empty:
            continue
        ts = chunk["ts_s"].to_numpy(dtype=np.int64)
        chunk_max_s = int(ts.max())
        max_seen_s = chunk_max_s if max_seen_s is None else max(max_seen_s, chunk_max_s)
        if origin_s is None:
            origin_s = int(ts.min())
            if accuracy_minutes > 0:
                cutoff_s = origin_s + accuracy_minutes * 60
            if replay_cutoff_s is not None:
                cutoff_s = replay_cutoff_s if cutoff_s is None else min(cutoff_s, replay_cutoff_s)
        assert origin_s is not None

        if cutoff_s is not None:
            if int(ts.min()) > cutoff_s:
                break
            chunk = chunk[chunk["ts_s"] <= cutoff_s]
            if chunk.empty:
                continue

        rel_window_idx = ((chunk["ts_s"].to_numpy(dtype=np.int64) - origin_s) // WINDOW_5MIN_S)
        chunk = chunk.copy()
        chunk["window_start_s"] = origin_s + rel_window_idx * WINDOW_5MIN_S
        for (entity, mb, agg, ws), group in chunk.groupby(
            ["entity", "metric_base", "aggregation", "window_start_s"]
        ):
            key = (str(entity), str(mb), str(agg), int(ws))
            acc.setdefault(key, []).extend(group["value"].astype(float).tolist())

    if origin_s is None or not acc:
        return pd.DataFrame()

    if cutoff_s is None:
        # Exclude the final partial replay-relative window when the file does
        # not end exactly on a 5-minute boundary.
        cutoff_s = max_seen_s

    rows = []
    for (entity, mb, agg, ws), values in acc.items():
        if ws + WINDOW_5MIN_S > cutoff_s:
            continue
        arr = np.asarray(values, dtype=np.float64)
        if arr.size == 0:
            continue
        p50 = float(np.percentile(arr, 50))
        p90 = float(np.percentile(arr, 90))
        p95 = float(np.percentile(arr, 95))
        p99 = float(np.percentile(arr, 99))
        rows.append({
            "entity":             entity,
            "metric_base":        mb,
            "aggregation":        agg,
            "window_start_s":     ws,
            "window_size_s":      WINDOW_5MIN_S,
            "window_label":       "5min_replay_relative",
            "p50":                p50,
            "p90":                p90,
            "p95":                p95,
            "p99":                p99,
            "tail_ratio_p99_p50": (p99 / p50) if p50 != 0.0 else float("nan"),
            "tail_ratio_p95_p50": (p95 / p50) if p50 != 0.0 else float("nan"),
            "count":              int(arr.size),
        })

    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    out.sort_values(["entity", "metric_base", "aggregation", "window_start_s"], inplace=True)
    out.reset_index(drop=True, inplace=True)
    return out


def _build_q1_single_window_ground_truth(
    file_tag: str,
    window_start_s: int,
    window_end_s: int,
    chunksize: int = 2000,
) -> pd.DataFrame:
    """Compute Q1 exact quantiles for one event-time window.

    DDSketch window mode flushes by collector wall-clock time.  The best
    comparator-side approximation for a selected scrape is therefore the raw
    event-time interval that had been emitted when that scrape was taken.
    """
    csv_path = file_csv_path(file_tag)
    if not csv_path.is_file() or window_end_s <= window_start_s:
        return pd.DataFrame()

    acc: dict[tuple[str, str, str], list[float]] = {}
    for chunk in _stream_long_chunks(csv_path, chunksize):
        if chunk.empty:
            continue
        chunk = chunk[
            (chunk["ts_s"] >= window_start_s) & (chunk["ts_s"] <= window_end_s)
        ]
        if chunk.empty:
            continue
        for (entity, mb, agg), group in chunk.groupby(["entity", "metric_base", "aggregation"]):
            key = (str(entity), str(mb), str(agg))
            acc.setdefault(key, []).extend(group["value"].astype(float).tolist())

    rows = []
    for (entity, mb, agg), values in acc.items():
        arr = np.asarray(values, dtype=np.float64)
        if arr.size == 0:
            continue
        p50 = float(np.percentile(arr, 50))
        p90 = float(np.percentile(arr, 90))
        p95 = float(np.percentile(arr, 95))
        p99 = float(np.percentile(arr, 99))
        rows.append({
            "entity":             entity,
            "metric_base":        mb,
            "aggregation":        agg,
            "window_start_s":     window_start_s,
            "window_size_s":      WINDOW_5MIN_S,
            "window_label":       "5min_flush_aligned",
            "p50":                p50,
            "p90":                p90,
            "p95":                p95,
            "p99":                p99,
            "tail_ratio_p99_p50": (p99 / p50) if p50 != 0.0 else float("nan"),
            "tail_ratio_p95_p50": (p95 / p50) if p50 != 0.0 else float("nan"),
            "count":              int(arr.size),
        })

    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    out.sort_values(["entity", "metric_base", "aggregation"], inplace=True)
    out.reset_index(drop=True, inplace=True)
    return out


def _event_time_at_or_before_scrape_s(
    send_times_path: Path | None,
    scrape_wall_ns: int,
) -> int | None:
    if send_times_path is None or not send_times_path.is_file():
        return None
    try:
        send_times = pd.read_csv(send_times_path, usecols=["emit_wall_ns", "event_time_ns"])
    except (ValueError, FileNotFoundError, pd.errors.EmptyDataError):
        return None
    if send_times.empty:
        return None

    emit_ns = pd.to_numeric(send_times["emit_wall_ns"], errors="coerce")
    event_ns = pd.to_numeric(send_times["event_time_ns"], errors="coerce")
    valid = emit_ns.notna() & event_ns.notna()
    if not valid.any():
        return None

    eligible = send_times.loc[valid & (emit_ns <= scrape_wall_ns), "event_time_ns"]
    if eligible.empty:
        return None
    return int(pd.to_numeric(eligible, errors="coerce").max() // 1_000_000_000)


def _get_time_aligned_snapshot_for_gt_window(
    sketch_data: pd.DataFrame,
    ground_truth: pd.DataFrame,
    send_times_path: Path | None,
) -> pd.DataFrame:
    """Return a scrape snapshot aligned to the last completed GT window.

    Mapping is done through send_times.csv:
      event_time_ns (replay event clock) -> emit_wall_ns (wall clock at OTLP export).
    We pick the newest export whose event time is within the completed GT window
    range, then select the nearest scrape at/before that wall timestamp.
    """
    if (
        sketch_data.empty
        or ground_truth.empty
        or send_times_path is None
        or not send_times_path.is_file()
        or "window_start_s" not in ground_truth.columns
    ):
        return pd.DataFrame()

    try:
        send_times = pd.read_csv(send_times_path, usecols=["emit_wall_ns", "event_time_ns"])
    except (ValueError, FileNotFoundError, pd.errors.EmptyDataError):
        return pd.DataFrame()
    if send_times.empty:
        return pd.DataFrame()

    gt_ws = pd.to_numeric(ground_truth["window_start_s"], errors="coerce")
    if gt_ws.dropna().empty:
        return pd.DataFrame()
    if "window_size_s" in ground_truth.columns:
        gt_size = pd.to_numeric(ground_truth["window_size_s"], errors="coerce")
        gt_end_s = (gt_ws + gt_size).dropna()
        if gt_end_s.empty:
            return pd.DataFrame()
        target_event_s = int(gt_end_s.max())
    else:
        target_event_s = int(gt_ws.max())

    event_ns = pd.to_numeric(send_times["event_time_ns"], errors="coerce")
    emit_ns = pd.to_numeric(send_times["emit_wall_ns"], errors="coerce")
    valid = event_ns.notna() & emit_ns.notna()
    if not valid.any():
        return pd.DataFrame()
    event_s = (event_ns[valid] // 1_000_000_000).astype(np.int64)
    emit_valid = emit_ns[valid].astype(np.int64)

    eligible = emit_valid[event_s <= target_event_s]
    if eligible.empty:
        return pd.DataFrame()
    target_emit_ns = int(eligible.max())

    scrape_ns = pd.to_numeric(sketch_data.get("scrape_wall_ns"), errors="coerce")
    scrape_valid = sketch_data[scrape_ns.notna()].copy()
    if scrape_valid.empty:
        return pd.DataFrame()
    scrape_ns_valid = pd.to_numeric(scrape_valid["scrape_wall_ns"], errors="coerce").astype(np.int64)

    before_or_equal = scrape_ns_valid[scrape_ns_valid <= target_emit_ns]
    if not before_or_equal.empty:
        chosen_ns = int(before_or_equal.max())
    else:
        # If scrape cadence is sparse, fall back to the closest scrape.
        idx = (scrape_ns_valid - target_emit_ns).abs().idxmin()
        chosen_ns = int(scrape_ns_valid.loc[idx])
    return scrape_valid[scrape_ns_valid == chosen_ns]


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
    if query_id == "Q1" and replay_cutoff_s is not None:
        replay_relative_gt = _build_q1_replay_relative_ground_truth(
            file_tag,
            replay_cutoff_s=replay_cutoff_s,
            accuracy_minutes=accuracy_minutes,
        )
        if not replay_relative_gt.empty:
            ground_truth = replay_relative_gt
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

    aligned_snapshot = _get_time_aligned_snapshot_for_gt_window(
        sketch_data,
        ground_truth,
        send_times_path,
    )
    if aligned_snapshot.empty:
        sketch_snapshot = get_best_snapshot_for_query(sketch_data, query_id)
    else:
        sketch_snapshot = get_best_snapshot_for_query(aligned_snapshot, query_id)

    if query_id == "Q1" and not sketch_snapshot.empty:
        scrape_wall = pd.to_numeric(
            sketch_snapshot["scrape_wall_ns"], errors="coerce"
        ).dropna()
        if not scrape_wall.empty:
            window_end_s = _event_time_at_or_before_scrape_s(
                send_times_path,
                int(scrape_wall.max()),
            )
            if window_end_s is not None:
                flush_aligned_gt = _build_q1_single_window_ground_truth(
                    file_tag,
                    window_start_s=window_end_s - WINDOW_5MIN_S,
                    window_end_s=window_end_s,
                )
                if not flush_aligned_gt.empty:
                    ground_truth = flush_aligned_gt

    if query_id == "Q6":
        result = fn(ground_truth, sketch_data, file=tag)
    else:
        result = fn(ground_truth, sketch_snapshot, file=tag)

    if isinstance(result, list):
        # Per-series results (Q1): each item already has entity, metric_base,
        # aggregation, metric, value, threshold, pass.
        rows = [{"query": query_id, "file": tag, **item} for item in result]

        # Append a summary row per metric: mean pass-rate across all series.
        if rows:
            summary_df = pd.DataFrame(rows)
            for metric_name, grp in summary_df.groupby("metric"):
                avg_pass = float(grp["pass"].mean())
                thr, pass_val = _metric_threshold(query_id, str(metric_name), avg_pass)
                rows.append({
                    "query":       query_id,
                    "file":        tag,
                    "entity":      "__summary__",
                    "metric_base": "__all__",
                    "aggregation": "__all__",
                    "metric":      metric_name,
                    "value":       avg_pass,
                    "threshold":   thr,
                    "pass":        pass_val,
                })
    else:
        # Scalar-dict results (all other queries): one row per metric.
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
    # Q1_KLL: sentinel_count is informational — always pass.  # Q1_KLL
    if query_id == "Q1_KLL" and metric == "sentinel_count":  # Q1_KLL
        return float("nan"), True  # Q1_KLL
    # Q1_KLL: stream error metrics are lower-is-better, threshold = 1%.  # Q1_KLL
    if (  # Q1_KLL
        metric.startswith("stream_")  # Q1_KLL
        and metric.endswith(("_p50_err", "_p95_err", "_p99_err"))  # Q1_KLL
    ):  # Q1_KLL
        thr = Q1_KLL_ERROR_TARGET  # Q1_KLL
        return thr, bool(not np.isnan(value) and value <= thr)  # Q1_KLL
    # Q1_KLL: pass flags are higher-is-better, must equal 1.0.  # Q1_KLL
    if (  # Q1_KLL
        (metric.startswith("stream_") and metric.endswith("_pass"))  # Q1_KLL
        or metric == "overall_pass"  # Q1_KLL
    ):  # Q1_KLL
        return 1.0, bool(not np.isnan(value) and value >= 1.0)  # Q1_KLL

    # Lower-is-better metrics (errors): threshold is max acceptable.
    lower_better = {
        "hll_rel_err": 0.05,
        "sat_ratio_mae": 0.05,
    }
    # Higher-is-better metrics (fractions, overlaps): threshold is min acceptable.
    higher_better = {
        "frac_q50_lt_1pct": 0.95,
        "frac_q95_lt_1pct": 0.95,
        "frac_q99_lt_1pct": 0.95,
        "topk_overlap": 0.95,
        "rank_correlation": 0.95,
        "frac_min_lt_2pct": 0.95,
        "frac_max_lt_2pct": 0.95,
        "frac_iqr_lt_10pct": 0.95,
        "entity_topk_overlap": 0.95,
        "frac_drift_p95_lt_20pct": 0.95,
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
