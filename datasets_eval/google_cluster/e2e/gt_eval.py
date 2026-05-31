#!/usr/bin/env python3
"""Exact offline ground truth for the google_cluster E2E harness.

Computes the *exact* answer to each query directly from the mapped
OTLP JSONL (the same bytes replayed into the fused asap_edge agent),
so the warm backend query result can be compared against a real
oracle — not against the archive tier.

This is deliberately dependency-free (stdlib only): the mapped rows
are small (a bounded subsample) and the aggregates are simple, so we
avoid a pandas/numpy requirement that would not be present on every
eval host.

Each queries.json entry carries a structured ``gt`` spec (added by
this harness) describing how to compute the exact answer:

    "gt": {
      "op":         "quantile" | "sum" | "count_distinct"
                    | "topk_sum" | "topk_count" | "frequency",
      "metric":     "google_cluster_2019_cpu_rate",
      "q":          0.99,                 # quantile only
      "by":         ["zone"],             # group-by labels ([] = global)
      "k":          10,                   # topk only
      "key_label":  "host",              # topk grouping / count_distinct dim
      "item_label": "service",           # frequency: which attr is the item
      "item_value": "svc-svc-000123"      # frequency: the specific item value
    }

Using a structured spec rather than parsing ``expected_ground_truth_query``
keeps the oracle unambiguous and testable.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable


# ---------------------------------------------------------------------------
# Row loading + window selection
# ---------------------------------------------------------------------------


def load_rows(jsonl_path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with open(jsonl_path, "r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def select_window(
    rows: list[dict[str, Any]],
    window_start_ms: int | None,
    window_end_ms: int | None,
) -> list[dict[str, Any]]:
    """Filter rows to ``[window_start_ms, window_end_ms)``.

    With both bounds ``None`` (the recommended deterministic mode) the
    GT is computed over *all* replayed rows — pair this with the
    "replay all -> one warm window -> query that window" replay mode so
    the warm answer and the GT see the identical sample set.
    """
    if window_start_ms is None and window_end_ms is None:
        return rows
    lo = window_start_ms if window_start_ms is not None else -(1 << 62)
    hi = window_end_ms if window_end_ms is not None else (1 << 62)
    return [r for r in rows if lo <= int(r["timestamp_ms"]) < hi]


# ---------------------------------------------------------------------------
# Aggregate primitives
# ---------------------------------------------------------------------------


def _quantile_linear(values: list[float], q: float) -> float:
    """Exact φ-quantile with linear interpolation (numpy 'linear' / PromQL).

    index = q * (n - 1); interpolate between the two neighbouring
    order statistics. Matches how quantile_over_time computes the
    reference value, so a sketch's approximation is measured against
    the same definition.
    """
    if not values:
        return float("nan")
    s = sorted(values)
    n = len(s)
    if n == 1:
        return s[0]
    pos = q * (n - 1)
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return s[int(pos)]
    frac = pos - lo
    return s[lo] * (1.0 - frac) + s[hi] * frac


def _group_key(attrs: dict[str, str], by: list[str]) -> tuple:
    return tuple(attrs.get(k, "") for k in by)


def _rows_for_metric(rows: Iterable[dict[str, Any]], metric: str) -> Iterable[dict[str, Any]]:
    return (r for r in rows if r["metric"] == metric)


# ---------------------------------------------------------------------------
# Per-op evaluators
# ---------------------------------------------------------------------------


def gt_quantile(rows, spec) -> dict[str, float] | float:
    metric = spec["metric"]
    q = float(spec["q"])
    by = spec.get("by", []) or []
    if not by:
        vals = [float(r["value"]) for r in _rows_for_metric(rows, metric)]
        return _quantile_linear(vals, q)
    groups: dict[tuple, list[float]] = defaultdict(list)
    for r in _rows_for_metric(rows, metric):
        groups[_group_key(r["attributes"], by)].append(float(r["value"]))
    return {":".join(k): _quantile_linear(v, q) for k, v in groups.items()}


def gt_sum(rows, spec) -> dict[str, float] | float:
    metric = spec["metric"]
    by = spec.get("by", []) or []
    if not by:
        return sum(float(r["value"]) for r in _rows_for_metric(rows, metric))
    groups: dict[tuple, float] = defaultdict(float)
    for r in _rows_for_metric(rows, metric):
        groups[_group_key(r["attributes"], by)] += float(r["value"])
    return {":".join(k): v for k, v in groups.items()}


def gt_count_distinct(rows, spec) -> float:
    """Exact distinct count of ``key_label`` (or a tuple of labels).

    Under the mapper's --cardinality-cap the distinct alphabet is
    closed at N; if ``cap_rescale`` is set the harness recovers the
    true cardinality by U/N. Here we report the *observed* distinct
    count (what the HLL sees), which is what the warm result is
    compared against.
    """
    metric = spec["metric"]
    dim = spec["key_label"]
    dims = dim if isinstance(dim, list) else [dim]
    seen: set[tuple] = set()
    for r in _rows_for_metric(rows, metric):
        seen.add(tuple(r["attributes"].get(d, "") for d in dims))
    return float(len(seen))


def gt_topk(rows, spec, inner: str) -> dict[str, float]:
    """Top-k groups by an inner aggregate (sum or count) of ``key_label``."""
    metric = spec["metric"]
    k = int(spec["k"])
    key = spec["key_label"]
    agg: dict[str, float] = defaultdict(float)
    if inner == "sum":
        for r in _rows_for_metric(rows, metric):
            agg[r["attributes"].get(key, "")] += float(r["value"])
    else:  # count
        c: Counter = Counter(r["attributes"].get(key, "") for r in _rows_for_metric(rows, metric))
        agg = {kk: float(vv) for kk, vv in c.items()}
    top = sorted(agg.items(), key=lambda kv: (-kv[1], kv[0]))[:k]
    return dict(top)


def gt_frequency(rows, spec) -> float:
    """Exact per-item frequency: count of rows whose ``item_label`` equals
    ``item_value`` for ``metric`` (optionally within a ``by`` group).

    This is the oracle for the new CMS per-item ``estimate(key)``.
    A CMS is a one-sided over-estimator, so the warm answer should be
    ``>= `` this value within the relative-error band.
    """
    metric = spec["metric"]
    item_label = spec["item_label"]
    item_value = spec["item_value"]
    by = spec.get("by", []) or []
    by_val = spec.get("by_value")
    n = 0
    for r in _rows_for_metric(rows, metric):
        if r["attributes"].get(item_label, "") != item_value:
            continue
        if by and by_val is not None:
            if _group_key(r["attributes"], by) != tuple(by_val):
                continue
        n += 1
    return float(n)


_OPS = {
    "quantile": lambda rows, spec: gt_quantile(rows, spec),
    "sum": lambda rows, spec: gt_sum(rows, spec),
    "count_distinct": lambda rows, spec: gt_count_distinct(rows, spec),
    "topk_sum": lambda rows, spec: gt_topk(rows, spec, "sum"),
    "topk_count": lambda rows, spec: gt_topk(rows, spec, "count"),
    "frequency": lambda rows, spec: gt_frequency(rows, spec),
}


def evaluate_gt(rows: list[dict[str, Any]], spec: dict[str, Any]) -> Any:
    op = spec.get("op")
    if op not in _OPS:
        raise ValueError(f"gt_eval: unknown op {op!r}; supported: {sorted(_OPS)}")
    return _OPS[op](rows, spec)


def evaluate_queries(
    queries: list[dict[str, Any]],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """Return {query_id_or_index: gt_result} for every query carrying a gt spec."""
    out: dict[str, Any] = {}
    for i, q in enumerate(queries):
        spec = q.get("gt")
        if not spec:
            continue
        qid = q.get("id") or q.get("promql") or f"query[{i}]"
        out[qid] = evaluate_gt(rows, spec)
    return out


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Exact offline GT for google_cluster queries.")
    ap.add_argument("--jsonl", type=Path, required=True, help="Mapped OTLP JSONL.")
    ap.add_argument("--queries", type=Path,
                    default=Path(__file__).resolve().parent.parent / "queries.json")
    ap.add_argument("--out", type=Path, default=None, help="Write GT JSON here (default stdout).")
    ap.add_argument("--window-start-ms", type=int, default=None)
    ap.add_argument("--window-end-ms", type=int, default=None)
    args = ap.parse_args(argv)

    rows = load_rows(args.jsonl)
    rows = select_window(rows, args.window_start_ms, args.window_end_ms)
    queries = json.loads(args.queries.read_text())
    gt = evaluate_queries(queries, rows)

    payload = json.dumps(gt, indent=2, sort_keys=True)
    if args.out:
        args.out.write_text(payload + "\n")
        print(f"gt_eval: wrote {len(gt)} GT results -> {args.out} "
              f"({len(rows)} rows)", file=sys.stderr)
    else:
        print(payload)
    return 0


if __name__ == "__main__":
    sys.exit(main())
