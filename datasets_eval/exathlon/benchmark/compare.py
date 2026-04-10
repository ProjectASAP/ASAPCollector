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
from datetime import datetime, timezone
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
    compute_per_metric_thresholds,
)
from common import METRIC_NAME, file_csv_path, file_tag_safe

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


def _q3_gt_key(row: pd.Series) -> str:
    return (
        f"aggregation={row['aggregation']};"
        f"entity={row['entity']};"
        f"metric_base={row['metric_base']};"
    )


def _filter_frequency_sketch_rows(sketch_data: pd.DataFrame, query_id: str) -> pd.DataFrame:
    """Drop raw metric rows when a scrape mixes originals and CountSketch flushes."""
    if query_id not in {"Q3", "Q7"} or sketch_data.empty or "metric" not in sketch_data.columns:
        return sketch_data
    metrics = sketch_data["metric"].astype(str)
    has_partition = metrics.str.contains("countsketch_partition", regex=False, na=False).any()
    has_non_partition = (~metrics.str.contains("countsketch_partition", regex=False, na=False)).any()
    if has_partition and has_non_partition:
        return sketch_data[metrics.str.contains("countsketch_partition", regex=False, na=False)].copy()
    return sketch_data


def _spearman_for_topk(gt_top: list[str], sk_top: list[str]) -> float:
    common = [k for k in gt_top if k in set(sk_top)]
    if len(common) < 2:
        return float("nan")
    gt_ranks = [gt_top.index(k) + 1 for k in common]
    sk_ranks = [sk_top.index(k) + 1 for k in common]
    rho, _ = scipy.stats.spearmanr(gt_ranks, sk_ranks)
    return float(rho) if not np.isnan(rho) else 0.0


def compute_q3_score(topk_overlap: float, rank_correlation: float) -> float:
    """Weighted Q3 score used by the detailed per-window report.

    Overlap is the primary term; rank agreement adds a smaller bonus and is
    clipped at zero so anti-correlation does not invert the score.
    """
    rho = 0.0 if np.isnan(rank_correlation) else max(float(rank_correlation), 0.0)
    return float(topk_overlap + 0.25 * rho)


def _format_window_label(window_start_s: int, window_size_s: int) -> str:
    start = datetime.fromtimestamp(window_start_s, tz=timezone.utc)
    end = datetime.fromtimestamp(window_start_s + window_size_s, tz=timezone.utc)
    return f"{start:%H:%M}-{end:%H:%M} UTC"


def _get_snapshot_for_event_time_s(
    sketch_data: pd.DataFrame,
    target_event_s: int,
    query_id: str,
    send_times_path: Path | None,
) -> pd.DataFrame:
    if (
        sketch_data.empty
        or send_times_path is None
        or not send_times_path.is_file()
    ):
        return pd.DataFrame()
    try:
        send_times = pd.read_csv(send_times_path, usecols=["emit_wall_ns", "event_time_ns"])
    except (ValueError, FileNotFoundError, pd.errors.EmptyDataError):
        return pd.DataFrame()
    if send_times.empty:
        return pd.DataFrame()

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
        idx = (scrape_ns_valid - target_emit_ns).abs().idxmin()
        chosen_ns = int(scrape_ns_valid.loc[idx])
    snapshot = scrape_valid[scrape_ns_valid == chosen_ns]
    return get_best_snapshot_for_query(snapshot, query_id)


def compare_q3_per_window(
    ground_truth: pd.DataFrame,
    sketch_data: pd.DataFrame,
    send_times_path: Path | None,
    score_threshold: float = 1.0,
) -> pd.DataFrame:
    """Return one comparison row per GT window for Q3."""
    cols = [
        "query", "file", "window_start_s", "window_size_s", "window",
        "topk_overlap", "rank_correlation", "q3_score", "threshold", "pass",
        "gt_rank1_count", "gt_rank10_count", "gt_topk_range",
        "snapshot_rows", "partition_rows", "partition_nonzero_rows", "status", "reason",
        "gt_topk_keys", "sketch_topk_keys", "topk_exact_match",
    ]
    if ground_truth.empty:
        return pd.DataFrame(columns=cols)

    windows: list[dict] = []
    gt_sorted = ground_truth.sort_values(["window_start_s", "rank"]).copy()
    for ws, gt_win in gt_sorted.groupby("window_start_s", sort=True):
        gt_top = [
            _q3_gt_key(row)
            for _, row in gt_win.nsmallest(TOP_K_METRICS, "rank").iterrows()
        ]
        if not gt_top:
            continue
        window_size_s = int(pd.to_numeric(gt_win["window_size_s"], errors="coerce").dropna().iloc[0])
        window_end_s = int(ws) + window_size_s
        snapshot = _get_snapshot_for_event_time_s(sketch_data, window_end_s, "Q3", send_times_path)
        snapshot_rows = int(len(snapshot))
        partition_rows = 0
        partition_nonzero_rows = 0
        status = "ok"
        reason = ""
        if snapshot.empty:
            status = "missing"
            reason = "no_aligned_snapshot"
        else:
            metric_series = snapshot["metric"].astype(str) if "metric" in snapshot.columns else pd.Series(dtype=str)
            partition_mask = metric_series.str.contains("countsketch_partition", regex=False, na=False)
            partition_rows = int(partition_mask.sum())
            if partition_rows == 0:
                status = "missing"
                reason = "no_countsketch_partition_rows"
            else:
                values = pd.to_numeric(snapshot.loc[partition_mask, "value"], errors="coerce").fillna(0.0)
                partition_nonzero_rows = int((values > 0).sum())
                if partition_nonzero_rows == 0:
                    status = "missing"
                    reason = "countsketch_rows_all_zero"
        sk = extract_countsketch_estimates(snapshot)
        if sk.empty:
            overlap = float("nan")
            rho = float("nan")
            sk_top: list[str] = []
            if status == "ok":
                status = "missing"
                reason = "extract_countsketch_estimates_empty"
        else:
            sk_top = sk.sort_values("est", ascending=False).head(TOP_K_METRICS)["key"].tolist()
            overlap = len(set(gt_top) & set(sk_top)) / max(len(gt_top), 1)
            rho = _spearman_for_topk(gt_top, sk_top)
            if len(sk_top) == 0 and status == "ok":
                status = "missing"
                reason = "sketch_topk_empty"
        q3_score = compute_q3_score(overlap, rho) if not np.isnan(overlap) else float("nan")
        sorted_counts = gt_win.nsmallest(TOP_K_METRICS, "rank")["exceedance_count"].astype(float).tolist()
        gt_rank1_count = sorted_counts[0] if sorted_counts else float("nan")
        gt_rank10_count = sorted_counts[-1] if sorted_counts else float("nan")
        windows.append({
            "query": "Q3",
            "file": "",
            "window_start_s": int(ws),
            "window_size_s": window_size_s,
            "window": _format_window_label(int(ws), window_size_s),
            "topk_overlap": float(overlap),
            "rank_correlation": float(rho),
            "q3_score": float(q3_score),
            "threshold": float(score_threshold),
            "pass": bool(not np.isnan(q3_score) and q3_score >= score_threshold),
            "gt_rank1_count": float(gt_rank1_count),
            "gt_rank10_count": float(gt_rank10_count),
            "gt_topk_range": float(gt_rank1_count - gt_rank10_count),
            "snapshot_rows": snapshot_rows,
            "partition_rows": partition_rows,
            "partition_nonzero_rows": partition_nonzero_rows,
            "status": status,
            "reason": reason,
            "gt_topk_keys": " | ".join(gt_top),
            "sketch_topk_keys": " | ".join(sk_top),
            "topk_exact_match": gt_top == sk_top,
        })
    return pd.DataFrame(windows, columns=cols)


def _build_q4_single_window_ground_truth(
    file_tag: str,
    window_start_s: int,
    window_end_s: int,
    chunksize: int = 2000,
) -> pd.DataFrame:
    """Compute Q4 exact min / max / range for one event-time window."""
    csv_path = file_csv_path(file_tag)
    if not csv_path.is_file() or window_end_s <= window_start_s:
        return pd.DataFrame()

    acc: dict[tuple[str, str], list[float]] = {}
    for chunk in _stream_long_chunks(csv_path, chunksize):
        if chunk.empty:
            continue
        chunk = chunk[
            (chunk["ts_s"] >= window_start_s) & (chunk["ts_s"] <= window_end_s)
        ]
        if chunk.empty:
            continue
        for (entity, mb), group in chunk.groupby(["entity", "metric_base"]):
            key = (str(entity), str(mb))
            acc.setdefault(key, []).extend(group["value"].astype(float).tolist())

    rows = []
    for (entity, mb), values in acc.items():
        arr = np.asarray(values, dtype=np.float64)
        if arr.size == 0:
            continue
        exact_min = float(arr.min())
        exact_max = float(arr.max())
        rows.append({
            "entity": entity,
            "metric_base": mb,
            "window_start_s": window_start_s,
            "window_size_s": WINDOW_5MIN_S,
            "window_label": "5min_flush_aligned",
            "exact_min": exact_min,
            "exact_max": exact_max,
            "exact_range": exact_max - exact_min,
            "count": int(arr.size),
        })

    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    out.sort_values(["entity", "metric_base"], inplace=True)
    out.reset_index(drop=True, inplace=True)
    return out


def compare_q4_per_window(
    file_tag: str,
    ground_truth: pd.DataFrame,
    sketch_data: pd.DataFrame,
    send_times_path: Path | None,
    threshold: float = 0.90,
    rel_err_threshold: float = 0.02,
) -> pd.DataFrame:
    """Return one comparison row per GT window for Q4.

    The window-level metric is the fraction of (entity, metric_base) pairs for
    which both p0 and p100 are within ``rel_err_threshold`` relative error.
    """
    cols = [
        "query", "file", "window_start_s", "window_size_s", "window",
        "frac_min_lt_2pct", "frac_max_lt_2pct", "frac_minmax_both_lt_2pct",
        "threshold", "pass", "pairs_compared",
        "snapshot_rows", "status", "reason", "sketch_flavor",
    ]
    if ground_truth.empty:
        return pd.DataFrame(columns=cols)

    gt_5m = ground_truth[ground_truth["window_size_s"] == WINDOW_5MIN_S].copy()
    if gt_5m.empty:
        return pd.DataFrame(columns=cols)

    rows: list[dict[str, object]] = []
    for ws in sorted(pd.to_numeric(gt_5m["window_start_s"], errors="coerce").dropna().astype(int).unique()):
        gt_win = gt_5m[gt_5m["window_start_s"] == ws].copy()
        if gt_win.empty:
            continue
        window_size_s = int(pd.to_numeric(gt_win["window_size_s"], errors="coerce").dropna().iloc[0])
        window_end_s = int(ws) + window_size_s
        snapshot = _get_snapshot_for_event_time_s(sketch_data, window_end_s, "Q4", send_times_path)
        snapshot_rows = int(len(snapshot))
        status = "ok"
        reason = ""
        sketch_flavor = _detect_sketch_flavor(snapshot)
        if snapshot.empty:
            status = "missing"
            reason = "no_aligned_snapshot"

        aligned_gt = _build_q4_single_window_ground_truth(
            file_tag,
            window_start_s=int(ws),
            window_end_s=window_end_s,
        )
        if aligned_gt.empty:
            status = "missing"
            reason = "no_aligned_ground_truth"

        sk_min = extract_sketch_quantile_by_group(snapshot, 0.0, ("entity", "metric_base"))
        sk_max = extract_sketch_quantile_by_group(snapshot, 1.0, ("entity", "metric_base"))

        frac_min = float("nan")
        frac_max = float("nan")
        frac_both = float("nan")
        pairs_compared = 0

        if not aligned_gt.empty and not sk_min.empty and not sk_max.empty:
            merged = (
                aligned_gt
                .merge(sk_min, on=["entity", "metric_base"], how="inner")
                .rename(columns={"v": "sk_min"})
                .merge(sk_max, on=["entity", "metric_base"], how="inner")
                .rename(columns={"v": "sk_max"})
            )
            if not merged.empty:
                pairs_compared = int(len(merged))
                exact_min = merged["exact_min"].to_numpy(dtype=np.float64)
                exact_max = merged["exact_max"].to_numpy(dtype=np.float64)
                est_min = merged["sk_min"].to_numpy(dtype=np.float64)
                est_max = merged["sk_max"].to_numpy(dtype=np.float64)

                min_ok = np.zeros(len(merged), dtype=bool)
                max_ok = np.zeros(len(merged), dtype=bool)

                min_nonzero = exact_min != 0
                max_nonzero = exact_max != 0
                min_ok[min_nonzero] = (
                    np.abs(est_min[min_nonzero] - exact_min[min_nonzero]) / np.abs(exact_min[min_nonzero])
                ) < rel_err_threshold
                max_ok[max_nonzero] = (
                    np.abs(est_max[max_nonzero] - exact_max[max_nonzero]) / np.abs(exact_max[max_nonzero])
                ) < rel_err_threshold

                frac_min = float(np.mean(min_ok[min_nonzero])) if min_nonzero.any() else float("nan")
                frac_max = float(np.mean(max_ok[max_nonzero])) if max_nonzero.any() else float("nan")

                both_mask = min_nonzero & max_nonzero
                both_ok = min_ok & max_ok
                frac_both = float(np.mean(both_ok[both_mask])) if both_mask.any() else float("nan")
            elif status == "ok":
                status = "missing"
                reason = "no_common_pairs"
        elif status == "ok":
            status = "missing"
            reason = "missing_p0_or_p100_rows"

        rows.append({
            "query": "Q4",
            "file": "",
            "window_start_s": int(ws),
            "window_size_s": window_size_s,
            "window": _format_window_label(int(ws), window_size_s),
            "frac_min_lt_2pct": float(frac_min),
            "frac_max_lt_2pct": float(frac_max),
            "frac_minmax_both_lt_2pct": float(frac_both),
            "threshold": float(threshold),
            "pass": bool(not np.isnan(frac_both) and frac_both >= threshold),
            "pairs_compared": int(pairs_compared),
            "snapshot_rows": snapshot_rows,
            "status": status,
            "reason": reason,
            "sketch_flavor": sketch_flavor,
        })

    return pd.DataFrame(rows, columns=cols)


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
        return {
            "topk_overlap": float("nan"),
            "rank_correlation": float("nan"),
            "gt_topk_keys": "",
            "sketch_topk_keys": "",
            "topk_exact_match": False,
        }

    # Ground truth: last window only.
    last_ws = int(ground_truth["window_start_s"].max())
    gt_win = ground_truth[ground_truth["window_start_s"] == last_ws].copy()
    gt_top = [
        _q3_gt_key(row)
        for _, row in gt_win.nsmallest(TOP_K_METRICS, "rank").iterrows()
    ]

    sk = extract_countsketch_estimates(sketch)
    if sk.empty:
        return {
            "topk_overlap": float("nan"),
            "rank_correlation": float("nan"),
            "gt_topk_keys": " | ".join(gt_top),
            "sketch_topk_keys": "",
            "topk_exact_match": False,
        }

    sk_sorted = sk.sort_values("est", ascending=False).head(TOP_K_METRICS)
    sk_top = sk_sorted["key"].tolist()

    overlap = len(set(gt_top) & set(sk_top)) / max(len(gt_top), 1)
    rho = _spearman_for_topk(gt_top, sk_top)

    return {
        "topk_overlap": float(overlap),
        "rank_correlation": float(rho),
        "gt_topk_keys": " | ".join(gt_top),
        "sketch_topk_keys": " | ".join(sk_top),
        "topk_exact_match": gt_top == sk_top,
    }


_COMPARE_DISPATCH["Q3"] = _compare_q3


# ---------------------------------------------------------------------------
# Q4 — Min / max relative error
# ---------------------------------------------------------------------------

def _compare_q4(ground_truth: pd.DataFrame, sketch: pd.DataFrame, **_) -> dict:
    if ground_truth.empty:
        return {
            "frac_min_lt_2pct": float("nan"),
            "frac_max_lt_2pct": float("nan"),
            "frac_minmax_both_lt_2pct": float("nan"),
        }

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

    frac_min = _frac(sk_min, "exact_min", 0.02)
    frac_max = _frac(sk_max, "exact_max", 0.02)

    frac_both = float("nan")
    if not sk_min.empty and not sk_max.empty and not gt_last.empty:
        merged = (
            gt_last
            .merge(sk_min, on=["entity", "metric_base"], how="inner")
            .rename(columns={"v": "sk_min"})
            .merge(sk_max, on=["entity", "metric_base"], how="inner")
            .rename(columns={"v": "sk_max"})
        )
        if not merged.empty:
            exact_min = merged["exact_min"].to_numpy(dtype=np.float64)
            exact_max = merged["exact_max"].to_numpy(dtype=np.float64)
            est_min = merged["sk_min"].to_numpy(dtype=np.float64)
            est_max = merged["sk_max"].to_numpy(dtype=np.float64)
            both_mask = (exact_min != 0) & (exact_max != 0)
            if both_mask.any():
                min_ok = (
                    np.abs(est_min[both_mask] - exact_min[both_mask]) / np.abs(exact_min[both_mask])
                ) < 0.02
                max_ok = (
                    np.abs(est_max[both_mask] - exact_max[both_mask]) / np.abs(exact_max[both_mask])
                ) < 0.02
                frac_both = float(np.mean(min_ok & max_ok))

    return {
        "frac_min_lt_2pct": frac_min,
        "frac_max_lt_2pct": frac_max,
        "frac_minmax_both_lt_2pct": frac_both,
    }


_COMPARE_DISPATCH["Q4"] = _compare_q4


# ---------------------------------------------------------------------------
# Q5 — IQR accuracy and anomaly-flag validation
# ---------------------------------------------------------------------------

def _build_q5_window_event_flags(
    file_tag: str,
    window_start_s: int,
    window_size_s: int,
    bounds: pd.DataFrame,
    chunksize: int = 2000,
) -> pd.DataFrame:
    """Return per-point anomaly flags for one Q5 window using supplied bounds.

    ``bounds`` must contain:
        entity, metric_base, lower_fence, upper_fence
    """
    csv_path = file_csv_path(file_tag)
    if not csv_path.is_file() or window_size_s <= 0 or bounds.empty:
        return pd.DataFrame()

    window_end_s = window_start_s + window_size_s
    bounds_map = {
        (str(row["entity"]), str(row["metric_base"])): (
            float(row["lower_fence"]),
            float(row["upper_fence"]),
        )
        for _, row in bounds.iterrows()
    }

    rows: list[dict[str, object]] = []
    for chunk in _stream_long_chunks(csv_path, chunksize):
        if chunk.empty:
            continue
        chunk = chunk[
            (chunk["ts_s"] >= window_start_s) & (chunk["ts_s"] < window_end_s)
        ]
        if chunk.empty:
            continue
        for _, row in chunk.iterrows():
            key = (str(row["entity"]), str(row["metric_base"]))
            fence = bounds_map.get(key)
            if fence is None:
                continue
            lower_fence, upper_fence = fence
            value = float(row["value"])
            rows.append({
                "entity": key[0],
                "metric_base": key[1],
                "ts_s": int(row["ts_s"]),
                "value": value,
                "is_anomaly": bool(value < lower_fence or value > upper_fence),
            })

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _compare_q5(ground_truth: pd.DataFrame, sketch: pd.DataFrame, **_) -> dict:
    file_tag = str(_.get("file", ""))
    nan_result = {
        "frac_iqr_lt_10pct": float("nan"),
        "anomaly_precision": float("nan"),
        "anomaly_recall": float("nan"),
        "anomaly_f1": float("nan"),
    }
    if ground_truth.empty:
        return nan_result

    gt_last = ground_truth[ground_truth["window_start_s"] == ground_truth["window_start_s"].max()]

    sk_q1 = extract_sketch_quantile_by_group(sketch, 0.25, ("entity", "metric_base"))
    sk_q3 = extract_sketch_quantile_by_group(sketch, 0.75, ("entity", "metric_base"))

    if sk_q1.empty or sk_q3.empty or gt_last.empty:
        return nan_result

    sk_iqr = sk_q1.merge(sk_q3, on=["entity", "metric_base"], suffixes=("_q1", "_q3"))
    sk_iqr["sketch_iqr"] = sk_iqr["v_q3"] - sk_iqr["v_q1"]
    sk_iqr["sketch_lower_fence"] = sk_iqr["v_q1"] - 1.5 * sk_iqr["sketch_iqr"]
    sk_iqr["sketch_upper_fence"] = sk_iqr["v_q3"] + 1.5 * sk_iqr["sketch_iqr"]

    merged = gt_last.merge(sk_iqr, on=["entity", "metric_base"], how="inner")
    if merged.empty:
        return nan_result

    exact = merged["iqr"].to_numpy(dtype=np.float64)
    est = merged["sketch_iqr"].to_numpy(dtype=np.float64)
    nonzero = exact != 0
    rel_err = np.abs(est[nonzero] - exact[nonzero]) / np.abs(exact[nonzero])
    frac = float(np.mean(rel_err < 0.10)) if nonzero.any() else float("nan")

    precision = float("nan")
    recall = float("nan")
    f1 = float("nan")

    if file_tag and "window_size_s" in gt_last.columns:
        window_start_s = int(pd.to_numeric(gt_last["window_start_s"], errors="coerce").max())
        window_size_s = int(pd.to_numeric(gt_last["window_size_s"], errors="coerce").dropna().iloc[0])

        exact_flags = _build_q5_window_event_flags(
            file_tag,
            window_start_s,
            window_size_s,
            merged[["entity", "metric_base", "lower_fence", "upper_fence"]],
        )
        sketch_flags = _build_q5_window_event_flags(
            file_tag,
            window_start_s,
            window_size_s,
            merged[["entity", "metric_base", "sketch_lower_fence", "sketch_upper_fence"]].rename(
                columns={
                    "sketch_lower_fence": "lower_fence",
                    "sketch_upper_fence": "upper_fence",
                }
            ),
        )

        if not exact_flags.empty and not sketch_flags.empty:
            flags = exact_flags.merge(
                sketch_flags[["entity", "metric_base", "ts_s", "value", "is_anomaly"]].rename(
                    columns={"is_anomaly": "sketch_is_anomaly"}
                ),
                on=["entity", "metric_base", "ts_s", "value"],
                how="inner",
            )
            if not flags.empty:
                y_true = flags["is_anomaly"].to_numpy(dtype=bool)
                y_pred = flags["sketch_is_anomaly"].to_numpy(dtype=bool)
                tp = int(np.sum(y_true & y_pred))
                fp = int(np.sum((~y_true) & y_pred))
                fn = int(np.sum(y_true & (~y_pred)))
                precision = float(tp / (tp + fp)) if (tp + fp) > 0 else float("nan")
                recall = float(tp / (tp + fn)) if (tp + fn) > 0 else float("nan")
                if not np.isnan(precision) and not np.isnan(recall) and (precision + recall) > 0:
                    f1 = float(2.0 * precision * recall / (precision + recall))

    return {
        "frac_iqr_lt_10pct": frac,
        "anomaly_precision": precision,
        "anomaly_recall": recall,
        "anomaly_f1": f1,
    }


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

    acc: dict[tuple[str, str, int], list[float]] = {}
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
        for (entity, mb, ws), group in chunk.groupby(["entity", "metric_base", "window_start_s"]):
            key = (str(entity), str(mb), int(ws))
            acc.setdefault(key, []).extend(group["value"].astype(float).tolist())

    if origin_s is None or not acc:
        return pd.DataFrame()

    if cutoff_s is None:
        # Exclude the final partial replay-relative window when the file does
        # not end exactly on a 5-minute boundary.
        cutoff_s = max_seen_s

    rows = []
    for (entity, mb, ws), values in acc.items():
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
            "entity": entity,
            "metric_base": mb,
            "window_start_s": ws,
            "window_size_s": WINDOW_5MIN_S,
            "window_label": "5min_replay_relative",
            "p50": p50,
            "p90": p90,
            "p95": p95,
            "p99": p99,
            "tail_ratio_p99_p50": (p99 / p50) if p50 != 0.0 else float("nan"),
            "tail_ratio_p95_p50": (p95 / p50) if p50 != 0.0 else float("nan"),
            "count": int(arr.size),
        })

    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    out.sort_values(["entity", "metric_base", "window_start_s"], inplace=True)
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

    acc: dict[tuple[str, str], list[float]] = {}
    for chunk in _stream_long_chunks(csv_path, chunksize):
        if chunk.empty:
            continue
        chunk = chunk[
            (chunk["ts_s"] >= window_start_s) & (chunk["ts_s"] <= window_end_s)
        ]
        if chunk.empty:
            continue
        for (entity, mb), group in chunk.groupby(["entity", "metric_base"]):
            key = (str(entity), str(mb))
            acc.setdefault(key, []).extend(group["value"].astype(float).tolist())

    rows = []
    for (entity, mb), values in acc.items():
        arr = np.asarray(values, dtype=np.float64)
        if arr.size == 0:
            continue
        p50 = float(np.percentile(arr, 50))
        p90 = float(np.percentile(arr, 90))
        p95 = float(np.percentile(arr, 95))
        p99 = float(np.percentile(arr, 99))
        rows.append({
            "entity": entity,
            "metric_base": mb,
            "window_start_s": window_start_s,
            "window_size_s": WINDOW_5MIN_S,
            "window_label": "5min_flush_aligned",
            "p50": p50,
            "p90": p90,
            "p95": p95,
            "p99": p99,
            "tail_ratio_p99_p50": (p99 / p50) if p50 != 0.0 else float("nan"),
            "tail_ratio_p95_p50": (p95 / p50) if p50 != 0.0 else float("nan"),
            "count": int(arr.size),
        })

    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    out.sort_values(["entity", "metric_base"], inplace=True)
    out.reset_index(drop=True, inplace=True)
    return out


def _build_q3_single_window_ground_truth(
    file_tag: str,
    window_start_s: int,
    window_end_s: int,
    chunksize: int = 2000,
) -> pd.DataFrame:
    """Compute exact Q3 top-K for one event-time window.

    Thresholds remain file-local p95 per (entity, metric_base), matching the
    canonical Q3 definition. The window itself is aligned to the replay event
    interval covered by the selected scrape.
    """
    csv_path = file_csv_path(file_tag)
    if not csv_path.is_file() or window_end_s <= window_start_s:
        return pd.DataFrame()

    thresholds = compute_per_metric_thresholds(csv_path, chunksize=chunksize)
    exc_counts: dict[tuple[str, str, str], int] = {}

    for chunk in _stream_long_chunks(csv_path, chunksize):
        if chunk.empty:
            continue
        chunk = chunk[
            (chunk["ts_s"] >= window_start_s) & (chunk["ts_s"] <= window_end_s)
        ]
        if chunk.empty:
            continue
        entities = chunk["entity"].to_numpy()
        metric_bases = chunk["metric_base"].to_numpy()
        aggregations = chunk["aggregation"].to_numpy()
        values = chunk["value"].to_numpy(dtype=np.float64)
        for i in range(len(values)):
            thr = thresholds.get((entities[i], metric_bases[i]))
            if thr is not None and values[i] > thr:
                key = (str(entities[i]), str(metric_bases[i]), str(aggregations[i]))
                exc_counts[key] = exc_counts.get(key, 0) + 1

    if not exc_counts:
        return pd.DataFrame(
            columns=[
                "entity", "metric_base", "aggregation", "window_start_s",
                "window_size_s", "window_label", "exceedance_count", "rank",
            ]
        )

    ranked = sorted(
        exc_counts.items(),
        key=lambda kv: (-kv[1], kv[0][0], kv[0][1], kv[0][2]),
    )[:TOP_K_METRICS]
    rows: list[dict[str, object]] = []
    for rank, ((entity, metric_base, aggregation), cnt) in enumerate(ranked, start=1):
        rows.append({
            "entity": entity,
            "metric_base": metric_base,
            "aggregation": aggregation,
            "window_start_s": window_start_s,
            "window_size_s": WINDOW_5MIN_S,
            "window_label": "5min_flush_aligned",
            "exceedance_count": int(cnt),
            "rank": int(rank),
        })
    out = pd.DataFrame(rows)
    out.sort_values("rank", inplace=True)
    out.reset_index(drop=True, inplace=True)
    return out


def _find_last_sketch_flush_wall_ns(
    sketch_data: pd.DataFrame,
    snapshot_wall_ns: int,
) -> int | None:
    """Return the wall-clock ns of the most recent sketch window flush.

    The DDSketch processor fires a fixed-interval wall-clock ticker
    (``window_duration = 5 min``).  Flush boundaries form an arithmetic
    sequence starting at the wall time of the first non-zero scrape.  We
    compute the last flush before ``snapshot_wall_ns`` using integer
    division so the GT window aligns with the sketch's *actual* accumulated
    interval rather than the last OTLP emit time (which may belong to the
    *next* window that has not yet flushed).

    Returns ``None`` if the flush time cannot be determined (e.g. no non-zero
    sketch rows exist in ``sketch_data``).
    """
    if sketch_data.empty:
        return None
    flavor_mask = sketch_data["metric"].astype(str).str.contains(
        "_ddsketch|_kll", regex=True, na=False
    )
    ddsk = sketch_data[flavor_mask]
    if ddsk.empty:
        return None
    numeric = pd.to_numeric(ddsk["value"], errors="coerce").fillna(0)
    non_zero = ddsk[numeric > 0]
    if non_zero.empty:
        return None
    wall_ns_series = pd.to_numeric(non_zero["scrape_wall_ns"], errors="coerce").dropna()
    if wall_ns_series.empty:
        return None
    first_flush_ns = int(wall_ns_series.min())
    window_ns = WINDOW_5MIN_S * 1_000_000_000
    if snapshot_wall_ns < first_flush_ns:
        return None
    n_flushes = (snapshot_wall_ns - first_flush_ns) // window_ns
    last_flush_ns = first_flush_ns + n_flushes * window_ns
    # Sanity: the computed flush must be strictly before the snapshot.
    if last_flush_ns >= snapshot_wall_ns:
        return None
    return last_flush_ns


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
    if query_id == "Q3" and not gt_path.is_file():
        fallback = ground_truth_dir / query_id / f"{tag}_5min.csv"
        if fallback.is_file():
            gt_path = fallback
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
    sketch_data = _filter_frequency_sketch_rows(sketch_data, query_id)

    comparison_out_dir.mkdir(parents=True, exist_ok=True)
    fn = _COMPARE_DISPATCH.get(query_id)
    if fn is None:
        return

    if query_id == "Q3":
        # Q3 uses Window mode (5-min wall-clock flushes). The time-aligned periodic
        # scrape may predate the last flush, causing a snapshot/GT mismatch.
        # Always use the latest snapshot (which captures the most recent complete
        # window flush); the GT is rebuilt dynamically from that snapshot's
        # event-time boundary below.
        sketch_snapshot = get_best_snapshot_for_query(sketch_data, query_id)
    else:
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

    if query_id == "Q3" and not sketch_snapshot.empty:
        scrape_wall = pd.to_numeric(
            sketch_snapshot["scrape_wall_ns"], errors="coerce"
        ).dropna()
        if not scrape_wall.empty:
            window_end_s = _event_time_at_or_before_scrape_s(
                send_times_path,
                int(scrape_wall.max()),
            )
            if window_end_s is not None:
                flush_aligned_gt = _build_q3_single_window_ground_truth(
                    file_tag,
                    window_start_s=window_end_s - WINDOW_5MIN_S,
                    window_end_s=window_end_s,
                )
                if not flush_aligned_gt.empty:
                    ground_truth = flush_aligned_gt

    if query_id == "Q4" and not sketch_snapshot.empty:
        scrape_wall = pd.to_numeric(
            sketch_snapshot["scrape_wall_ns"], errors="coerce"
        ).dropna()
        if not scrape_wall.empty:
            # Use the wall-clock time of the last sketch *flush* rather than
            # the scrape time.  In Window mode the DDSketch ticker fires every
            # WINDOW_5MIN_S seconds; data keeps arriving between the flush and
            # the next scrape, so "last OTLP emit before scrape" overshoots by
            # one full window and makes the GT miss the sketch's actual window.
            scrape_wall_ns = int(scrape_wall.max())
            flush_wall_ns = _find_last_sketch_flush_wall_ns(sketch_data, scrape_wall_ns)
            lookup_wall_ns = flush_wall_ns if flush_wall_ns is not None else scrape_wall_ns
            window_end_s = _event_time_at_or_before_scrape_s(
                send_times_path,
                lookup_wall_ns,
            )
            if window_end_s is not None:
                flush_aligned_gt = _build_q4_single_window_ground_truth(
                    file_tag,
                    window_start_s=window_end_s - WINDOW_5MIN_S,
                    window_end_s=window_end_s,
                )
                if not flush_aligned_gt.empty:
                    ground_truth = flush_aligned_gt

    if query_id == "Q6":
        result = fn(ground_truth, sketch_data, file=file_tag)
    else:
        result = fn(ground_truth, sketch_snapshot, file=file_tag)

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

    if query_id == "Q3":
        q3_windows = compare_q3_per_window(ground_truth, sketch_data, send_times_path)
        if not q3_windows.empty:
            q3_windows["file"] = tag
            window_out_dir = comparison_out_dir.parent / "window_comparison"
            window_out_dir.mkdir(parents=True, exist_ok=True)
            q3_windows.to_csv(window_out_dir / f"{query_id}_{tag}.csv", index=False)
    elif query_id == "Q4":
        q4_windows = compare_q4_per_window(file_tag, ground_truth, sketch_data, send_times_path)
        if not q4_windows.empty:
            q4_windows["file"] = tag
            window_out_dir = comparison_out_dir.parent / "window_comparison"
            window_out_dir.mkdir(parents=True, exist_ok=True)
            q4_windows.to_csv(window_out_dir / f"{query_id}_{tag}.csv", index=False)


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
        "frac_minmax_both_lt_2pct": 0.90,
        "frac_iqr_lt_10pct": 0.85,
        "anomaly_precision": 0.80,
        "anomaly_recall": 0.80,
        "anomaly_f1": 0.80,
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
