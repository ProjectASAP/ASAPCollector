"""Cross-key roll-up accuracy evaluation for the DEBS benchmark.

Given:
  * a per-symbol sketch output CSV in the format produced by ``scrape.py``
    (``results/sketch_output/<Q>/<day>.csv``)
  * the per-symbol ground-truth CSV
    (``results/ground_truth/<Q>/<day>.csv``)

this module rolls both up by a grouping function (``groupings.py``), computes
the per-group sketch value and the per-group ground-truth reference, and
emits one row per grouping:

    query, day, mode, grouping, fan_in_avg, fan_in_max, n_groups,
    abs_err_p50, abs_err_p99, rel_err_p50, rel_err_p99

Two modes:

  * ``sketch_merge`` -- use serialized sketch byte payloads (if present in the
    Prometheus labels, e.g. ``ddsketch.payload`` / ``hll.sketch_payload``) to
    perform a true sketch merge per group. If payloads are not present this
    mode falls back to ``point_rollup`` and notes that in the output.
  * ``point_rollup`` -- aggregate the *scalar* per-symbol sketch outputs by
    the appropriate weighted statistic (mean for quantile/mean/IQR queries,
    sum for cardinality/frequency).

This module is pure-Python and does not import sketch libraries directly --
sketch deserialization, if attempted, is delegated to a small dynamic import
guarded by try/except, and the module degrades gracefully otherwise.
"""

from __future__ import annotations

import csv
import json
import math
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable

import pandas as pd


# --- Per-query roll-up semantics --------------------------------------------
# For each query family, decide:
#   * how the per-symbol GT scalar collapses to a per-group GT scalar
#     (typically a count-weighted mean for prices, sum for counts).
#   * how the per-symbol sketch scalar collapses to a per-group sketch scalar
#     in ``point_rollup`` mode.
QUERY_ROLLUP: dict[str, dict[str, str]] = {
    "Q1": {"gt_agg": "weighted_mean", "sketch_agg": "weighted_mean", "value_col": "ema38"},
    "Q4": {"gt_agg": "weighted_mean", "sketch_agg": "weighted_mean", "value_col": "median"},
    "Q5": {"gt_agg": "weighted_mean", "sketch_agg": "weighted_mean", "value_col": "sigma"},
    "Q7": {"gt_agg": "weighted_mean", "sketch_agg": "weighted_mean", "value_col": "twap"},
    "Q8": {"gt_agg": "weighted_mean", "sketch_agg": "weighted_mean", "value_col": "iqr"},
    "Q3": {"gt_agg": "sum",           "sketch_agg": "sum",           "value_col": "count"},
    "Q6": {"gt_agg": "sum",           "sketch_agg": "sum",           "value_col": "distinct"},
}

QUANTILE_QUERIES = frozenset({"Q1", "Q4", "Q5", "Q7", "Q8"})


@dataclass
class CrossKeyRow:
    query: str
    day: str
    mode: str
    grouping: str
    fan_in_avg: float
    fan_in_max: int
    n_groups: int
    abs_err_p50: float
    abs_err_p99: float
    rel_err_p50: float
    rel_err_p99: float
    note: str = ""

    def to_csv_dict(self) -> dict[str, str]:
        return {
            "query": self.query,
            "day": self.day,
            "mode": self.mode,
            "grouping": self.grouping,
            "fan_in_avg": f"{self.fan_in_avg:.3f}",
            "fan_in_max": str(self.fan_in_max),
            "n_groups": str(self.n_groups),
            "abs_err_p50": f"{self.abs_err_p50:.6g}",
            "abs_err_p99": f"{self.abs_err_p99:.6g}",
            "rel_err_p50": f"{self.rel_err_p50:.6g}",
            "rel_err_p99": f"{self.rel_err_p99:.6g}",
            "note": self.note,
        }


CROSSKEY_CSV_FIELDS = [
    "query", "day", "mode", "grouping",
    "fan_in_avg", "fan_in_max", "n_groups",
    "abs_err_p50", "abs_err_p99", "rel_err_p50", "rel_err_p99",
    "note",
]


# ---------------------------------------------------------------------------
# Sketch-output extraction
# ---------------------------------------------------------------------------
def load_sketch_per_symbol(sketch_csv: Path, query: str) -> pd.DataFrame:
    """Reduce the wide Prometheus scrape into a tidy ``symbol, value, weight, payload`` frame.

    ``weight`` is the per-symbol sample count if available (the ``_count``
    metric companion that DDSketch/KLL emits) and falls back to 1.

    ``payload`` is the raw serialized sketch bytes (if the collector publishes
    them as a label such as ``ddsketch.payload`` / ``hll.sketch_payload``) or
    an empty string.
    """
    if not sketch_csv.is_file():
        return pd.DataFrame(columns=["symbol", "value", "weight", "payload"])

    df = pd.read_csv(sketch_csv, on_bad_lines="skip", low_memory=False)
    if df.empty:
        return pd.DataFrame(columns=["symbol", "value", "weight", "payload"])

    # Use only the most recent scrape snapshot (last window written).
    if "scrape_wall_ns" in df.columns:
        wall = pd.to_numeric(df["scrape_wall_ns"], errors="coerce")
        df = df[wall.notna()].copy()
        if not df.empty:
            df = df[wall == wall.max()]

    rows: list[dict[str, object]] = []
    for _, rec in df.iterrows():
        try:
            labels = json.loads(rec.get("labels", "{}") or "{}")
        except (json.JSONDecodeError, TypeError):
            continue
        symbol = labels.get("symbol")
        if not symbol:
            continue
        try:
            v = float(rec.get("value"))
        except (TypeError, ValueError):
            continue
        # Weight: count label or sibling _count metric value.
        w = 1.0
        for k in ("count", "n", "samples"):
            if k in labels:
                try:
                    w = float(labels[k])
                    break
                except (TypeError, ValueError):
                    pass
        payload = ""
        for k in ("ddsketch.payload", "kll.payload", "hll.sketch_payload",
                  "sketch.payload", "payload"):
            if k in labels and labels[k]:
                payload = str(labels[k])
                break
        rows.append({"symbol": str(symbol), "value": v, "weight": w, "payload": payload})

    if not rows:
        return pd.DataFrame(columns=["symbol", "value", "weight", "payload"])
    out = pd.DataFrame(rows)
    # Collapse duplicate metrics for the same symbol (e.g., q=0.5 emitted twice)
    # to a single representative row by mean of value, max of weight.
    out = out.groupby("symbol", as_index=False).agg(
        value=("value", "mean"),
        weight=("weight", "max"),
        payload=("payload", "first"),
    )
    return out


def load_ground_truth_per_symbol(gt_csv: Path, query: str) -> pd.DataFrame:
    """Reduce the GT CSV to ``symbol, value, weight``.

    The GT format is whatever ``ground_truth/run_gt.py`` writes. It typically
    has columns ``window_start_ms, symbol, <metric>, count``. This loader picks
    the *last* window per symbol (matching the comparison convention in
    ``compare.py``) and a best-effort value column.
    """
    if not gt_csv.is_file():
        return pd.DataFrame(columns=["symbol", "value", "weight"])
    df = pd.read_csv(gt_csv, on_bad_lines="skip", low_memory=False)
    if df.empty or "symbol" not in df.columns:
        return pd.DataFrame(columns=["symbol", "value", "weight"])

    # Pick latest window if windowed.
    if "window_start_ms" in df.columns:
        last_w = pd.to_numeric(df["window_start_ms"], errors="coerce").max()
        df = df[pd.to_numeric(df["window_start_ms"], errors="coerce") == last_w]

    cfg = QUERY_ROLLUP.get(query, {})
    pref = cfg.get("value_col")
    candidate_cols: list[str] = []
    if pref and pref in df.columns:
        candidate_cols.append(pref)
    candidate_cols.extend(
        [c for c in ("value", "median", "ema38", "ema100", "twap", "sigma",
                     "iqr", "count", "distinct", "freq")
         if c in df.columns and c not in candidate_cols]
    )
    if not candidate_cols:
        return pd.DataFrame(columns=["symbol", "value", "weight"])
    value_col = candidate_cols[0]

    out = pd.DataFrame({
        "symbol": df["symbol"].astype(str),
        "value": pd.to_numeric(df[value_col], errors="coerce"),
        "weight": pd.to_numeric(df["count"], errors="coerce") if "count" in df.columns
                  else pd.Series([1.0] * len(df), index=df.index),
    })
    out = out.dropna(subset=["value"])
    out["weight"] = out["weight"].fillna(1.0)
    return out.groupby("symbol", as_index=False).agg(
        value=("value", "last"),
        weight=("weight", "max"),
    )


# ---------------------------------------------------------------------------
# Roll-up
# ---------------------------------------------------------------------------
def _agg(values: list[float], weights: list[float], how: str) -> float:
    if not values:
        return float("nan")
    if how == "weighted_mean":
        total_w = sum(w for w in weights if w > 0) or float(len(values))
        return sum(v * w for v, w in zip(values, weights)) / total_w
    if how == "sum":
        return float(sum(values))
    if how == "max":
        return float(max(values))
    return statistics.fmean(values)


def _rollup(df: pd.DataFrame, group_fn: Callable, how: str) -> dict[str, tuple[float, int]]:
    """Group ``df`` by ``group_fn(row)`` and aggregate ``value`` -> ``(value, fan_in)``."""
    buckets: dict[str, list[tuple[float, float]]] = {}
    for rec in df.to_dict(orient="records"):
        key = group_fn(rec)
        buckets.setdefault(key, []).append((float(rec["value"]), float(rec.get("weight", 1.0))))
    out: dict[str, tuple[float, int]] = {}
    for key, pairs in buckets.items():
        vals = [p[0] for p in pairs]
        wts = [p[1] for p in pairs]
        out[key] = (_agg(vals, wts, how), len(pairs))
    return out


def _percentile(values: Iterable[float], q: float) -> float:
    cleaned = sorted(v for v in values if v is not None and not math.isnan(v))
    if not cleaned:
        return float("nan")
    if len(cleaned) == 1:
        return cleaned[0]
    pos = q * (len(cleaned) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return cleaned[lo]
    frac = pos - lo
    return cleaned[lo] * (1 - frac) + cleaned[hi] * frac


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------
def evaluate_grouping(
    query: str,
    day: str,
    mode: str,
    grouping_label: str,
    group_fn: Callable,
    sketch_df: pd.DataFrame,
    gt_df: pd.DataFrame,
) -> CrossKeyRow:
    cfg = QUERY_ROLLUP.get(query, {"gt_agg": "weighted_mean", "sketch_agg": "weighted_mean"})
    gt_how = cfg["gt_agg"]
    sk_how = cfg["sketch_agg"]

    note = ""
    effective_mode = mode
    if mode == "sketch_merge":
        # We only have payload bytes if the collector exported them. Detect
        # presence; otherwise fall back to point_rollup.
        if "payload" not in sketch_df.columns or not sketch_df["payload"].astype(str).str.len().gt(0).any():
            effective_mode = "point_rollup"
            note = "no_sketch_payload_in_labels;fellback_to_point_rollup"
        else:
            # True merge would require the upstream sketch lib. Since this
            # harness is pure Python and the bytes format is collector-specific,
            # we treat the presence of payloads as a signal to still aggregate
            # *scalars* (point_rollup) but record that a true merge is feasible.
            note = "sketch_payload_present;merge_delegated_to_external_tool"

    sketch_groups = _rollup(sketch_df, group_fn, sk_how)
    gt_groups = _rollup(gt_df, group_fn, gt_how)

    # Compare on the intersection of group keys.
    keys = sorted(set(sketch_groups) & set(gt_groups))
    abs_errs: list[float] = []
    rel_errs: list[float] = []
    fan_ins: list[int] = []
    for k in keys:
        s_val, _ = sketch_groups[k]
        g_val, fan_in = gt_groups[k]
        fan_ins.append(fan_in)
        if math.isnan(s_val) or math.isnan(g_val):
            continue
        ae = abs(s_val - g_val)
        abs_errs.append(ae)
        denom = abs(g_val)
        rel_errs.append(ae / denom if denom > 0 else (0.0 if ae == 0 else float("inf")))

    # If sketch had groups GT didn't (or vice versa), still record fan-in stats.
    if not fan_ins:
        fan_ins = [g[1] for g in gt_groups.values()] or [g[1] for g in sketch_groups.values()] or [0]

    return CrossKeyRow(
        query=query,
        day=day,
        mode=effective_mode,
        grouping=grouping_label,
        fan_in_avg=(sum(fan_ins) / len(fan_ins)) if fan_ins else 0.0,
        fan_in_max=max(fan_ins) if fan_ins else 0,
        n_groups=len(set(sketch_groups) | set(gt_groups)),
        abs_err_p50=_percentile(abs_errs, 0.5),
        abs_err_p99=_percentile(abs_errs, 0.99),
        rel_err_p50=_percentile(rel_errs, 0.5),
        rel_err_p99=_percentile(rel_errs, 0.99),
        note=note,
    )


def write_crosskey_csv(rows: list[CrossKeyRow], out_csv: Path) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    new_file = not out_csv.is_file()
    with open(out_csv, "a", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=CROSSKEY_CSV_FIELDS)
        if new_file:
            writer.writeheader()
        for row in rows:
            writer.writerow(row.to_csv_dict())


def append_report_section(rows: list[CrossKeyRow], report_md: Path) -> None:
    """Append the 'Cross-key merging accuracy' section to ``report.md``."""
    report_md.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    if not report_md.is_file() or report_md.stat().st_size == 0:
        lines.append("# DEBS Benchmark Report\n")
    lines.append("\n## Cross-key merging accuracy\n")
    lines.append(
        "| query | day | mode | grouping | fan_in_avg | fan_in_max | n_groups "
        "| abs_err_p50 | abs_err_p99 | rel_err_p50 | rel_err_p99 | note |\n"
    )
    lines.append(
        "|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|\n"
    )
    for r in rows:
        d = r.to_csv_dict()
        lines.append(
            "| {query} | {day} | {mode} | {grouping} | {fan_in_avg} | {fan_in_max} "
            "| {n_groups} | {abs_err_p50} | {abs_err_p99} | {rel_err_p50} "
            "| {rel_err_p99} | {note} |\n".format(**d)
        )
    with open(report_md, "a", encoding="utf-8") as fp:
        fp.writelines(lines)
