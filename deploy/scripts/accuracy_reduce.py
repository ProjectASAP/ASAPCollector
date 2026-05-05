#!/usr/bin/env python3
"""Accuracy reducer (P8).

Joins replay.jsonl (sketch's answer, from P5) with the
hour-bucketed JSONL ground truth produced by the raw_tee (P4),
and emits per-query accuracy rows. One CSV per sweep, one row per
query attempt.

Computes:

  - quantile     → relative error vs exact P-th quantile
                   ε = |answer - truth| / max(truth, 1)
  - topk         → recall vs exact top-K by sum(value)
                   recall = |sketch ∩ truth| / K
  - count_unique → relative error vs exact distinct cardinality
                   ε = |answer - truth| / max(truth, 1)
  - sum          → relative error vs exact sum (identity check —
                   any non-zero ε flags a bug)

Output CSV columns:

  cell,kind,query,t_ms,duration_ms,plan_id,truth,answer,error,recall,n_truth_samples

Usage (per cell):

  python3 accuracy_reduce.py \\
      --cell-dir /tmp/sweep/ddsketch_N1_w100ms_c10000 \\
      --out      /tmp/sweep/ddsketch_N1_w100ms_c10000/accuracy.csv

Or in batch mode over a sweep root:

  python3 accuracy_reduce.py --sweep-root /tmp/sweep --out /tmp/sweep/all.csv
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import glob
import json
import os
import re
import sys
from collections import Counter, defaultdict
from typing import Iterable


# --- ground-truth loader -------------------------------------------


def iter_truth_samples(cold_truth_dir: str, metric: str) -> Iterable[dict]:
    """Yield {ts_ms, labels, value} from every part-*.jsonl under
    `<cold_truth_dir>/<metric>/...`. Tolerates a torn last line
    (the writer might still be flushing when the snapshot was
    taken)."""
    pat = os.path.join(cold_truth_dir, metric, "*", "*", "*", "*", "part-*.jsonl")
    files = sorted(glob.glob(pat))
    if not files:
        return
    for path in files:
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    # Torn last line tolerated; everything else is
                    # caller's problem.
                    continue


# --- query parsing -------------------------------------------------


# Quantile shape #1 (legacy): `histogram_quantile(0.5, sum by (le) (metric))`
_QUANTILE_HIST_RE = re.compile(
    r"histogram_quantile\(\s*([0-9.]+)\s*,\s*sum\s+by\s+\(\s*le\s*\)\s*\(\s*([\w_]+)\s*\)\s*\)",
    re.IGNORECASE,
)
# Quantile shape #2 (post-#266 e2e queries): `quantile_over_time(0.5, metric_quantile[1m])`.
# We treat the "_quantile" suffix as a sketch-projection naming convention; the
# underlying ground truth is the raw metric without the suffix. Many of our
# replay queries are warm-tier `quantile_over_time(φ, *_quantile[1m])` which
# the engine routes to the DDSketch / KLL precompute output. The cold-truth
# metric directory is the unsuffixed name (raw_tee writes one dir per
# top-level metric). Handle both: reduce against the canonical
# unsuffixed metric.
_QUANTILE_OVER_TIME_RE = re.compile(
    r"quantile_over_time\(\s*([0-9.]+)\s*,\s*([\w_]+?)(?:_quantile)?\s*\[\s*[0-9smhd]+\s*\]\s*\)",
    re.IGNORECASE,
)
_TOPK_RE = re.compile(r"topk\(\s*(\d+)\s*,\s*([\w_]+)\s*\)", re.IGNORECASE)
# count_unique shape #1 (legacy): `count(count by (X)(metric))` — distinct
# values of X across the series set.
_COUNT_UNIQUE_GROUP_RE = re.compile(
    r"count\(\s*count\s+by\s+\(\s*([\w_]+)\s*\)\s*\(\s*([\w_]+)\s*\)\s*\)",
    re.IGNORECASE,
)
# count_unique shape #2 (post-#266 e2e queries): `count(metric)` — distinct
# series count, equivalent to "how many time series exist for this metric".
# `truth_count_unique(samples, by="series")` re-uses the labels-set as the
# distinguishing key (computed in truth_count_distinct_series).
_COUNT_SERIES_RE = re.compile(r"^count\(\s*([\w_]+)\s*\)$", re.IGNORECASE)
# sum shape #1 (legacy): `sum(metric)` — sum across all series.
_SUM_INSTANT_RE = re.compile(r"^sum\(\s*([\w_]+)\s*\)$", re.IGNORECASE)
# sum shape #2 (post-#266 e2e queries): `sum_over_time(metric[1m])` — sum
# across the trailing 1m window for each series. Truth maps to total sum
# across all samples in the cold-window since the replay queries an instant
# at end-of-soak — the cold-truth set covers the full soak.
_SUM_OVER_TIME_RE = re.compile(
    r"sum_over_time\(\s*([\w_]+)\s*\[\s*[0-9smhd]+\s*\]\s*\)",
    re.IGNORECASE,
)


def parse_query(promql: str) -> tuple[str, dict] | None:
    """Returns (kind, params). None if the shape isn't one we
    recognise; the row gets skipped with a logged warning."""
    s = promql.strip()
    if (m := _QUANTILE_HIST_RE.match(s)):
        return "quantile", {"q": float(m.group(1)), "metric": m.group(2)}
    if (m := _QUANTILE_OVER_TIME_RE.match(s)):
        return "quantile", {"q": float(m.group(1)), "metric": m.group(2)}
    if (m := _TOPK_RE.match(s)):
        return "topk", {"k": int(m.group(1)), "metric": m.group(2)}
    if (m := _COUNT_UNIQUE_GROUP_RE.match(s)):
        return "count_unique", {"by": m.group(1), "metric": m.group(2)}
    if (m := _COUNT_SERIES_RE.match(s)):
        return "count_unique", {"by": "__series__", "metric": m.group(1)}
    if (m := _SUM_INSTANT_RE.match(s)):
        return "sum", {"metric": m.group(1)}
    if (m := _SUM_OVER_TIME_RE.match(s)):
        return "sum", {"metric": m.group(1)}
    return None


# --- ground-truth computers ----------------------------------------


def truth_quantile(samples: list[dict], q: float) -> float:
    if not samples:
        return float("nan")
    vals = sorted(s["value"] for s in samples)
    if not vals:
        return float("nan")
    # Nearest-rank quantile. Matches what most sketches target,
    # within ε tolerance.
    n = len(vals)
    idx = max(0, min(n - 1, int(q * n)))
    return vals[idx]


def truth_topk(samples: list[dict], k: int) -> list[tuple[str, float]]:
    """Top-K by sum(value) with the full attribute set as the
    grouping key. Returns sorted descending."""
    bucket: Counter[str] = Counter()
    for s in samples:
        key = json.dumps(s.get("labels", {}), sort_keys=True)
        bucket[key] += s["value"]
    return bucket.most_common(k)


def truth_count_unique(samples: list[dict], by: str) -> int:
    if by == "__series__":
        # Distinct series count: full labels-set as key. Mirrors what
        # `count(metric)` returns in PromQL — number of distinct
        # time series for the metric.
        return len({json.dumps(s.get("labels", {}), sort_keys=True) for s in samples})
    return len({s.get("labels", {}).get(by) for s in samples})


def truth_sum(samples: list[dict]) -> float:
    return sum(s["value"] for s in samples)


# --- result extraction (PromQL → scalar / list) --------------------


def extract_scalar(result) -> float | None:
    if not result:
        return None
    if isinstance(result, list) and result:
        first = result[0]
        if isinstance(first, dict) and "value" in first:
            v = first["value"]
            if isinstance(v, list) and len(v) >= 2:
                try:
                    return float(v[1])
                except (TypeError, ValueError):
                    return None
    if isinstance(result, dict) and "value" in result:
        v = result["value"]
        if isinstance(v, list) and len(v) >= 2:
            try:
                return float(v[1])
            except (TypeError, ValueError):
                return None
    return None


def extract_topk_keys(result, k: int) -> list[str]:
    if not isinstance(result, list):
        return []
    keys: list[str] = []
    for el in result[:k]:
        if not isinstance(el, dict):
            continue
        m = el.get("metric") or {}
        keys.append(json.dumps(m, sort_keys=True))
    return keys


# --- per-cell reducer ----------------------------------------------


def reduce_cell(cell_dir: str, writer: csv.DictWriter, cell_label: str) -> int:
    replay_path = os.path.join(cell_dir, "replay.jsonl")
    cold_root = os.path.join(cell_dir, "cold-truth")
    if not os.path.exists(replay_path):
        print(f"[skip] no replay.jsonl in {cell_dir}", file=sys.stderr)
        return 0
    if not os.path.isdir(cold_root):
        print(f"[skip] no cold-truth/ in {cell_dir}", file=sys.stderr)
        return 0

    # Group truth samples by metric name. We don't ts-bucket
    # because the replay queries are instant queries against the
    # whole cold window — match that scope.
    truth_by_metric: dict[str, list[dict]] = defaultdict(list)
    metric_dirs = [
        d for d in os.listdir(cold_root) if os.path.isdir(os.path.join(cold_root, d))
    ]
    for metric in metric_dirs:
        for s in iter_truth_samples(cold_root, metric):
            truth_by_metric[metric].append(s)

    n_rows = 0
    with open(replay_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            parsed = parse_query(rec["query"])
            if parsed is None:
                continue
            kind, params = parsed
            metric = params.get("metric", "")
            samples = truth_by_metric.get(metric, [])

            row = {
                "cell": cell_label,
                "kind": kind,
                "query": rec["query"],
                "t": rec["ts"],
                "duration_ms": rec.get("duration_ms"),
                "plan_id": rec.get("plan_id"),
                "n_truth_samples": len(samples),
                "truth": "",
                "answer": "",
                "error": "",
                "recall": "",
            }

            if kind == "quantile":
                t = truth_quantile(samples, params["q"])
                a = extract_scalar(rec.get("result"))
                row["truth"] = f"{t:.6f}" if t == t else ""  # nan check
                if a is not None and t == t:
                    row["answer"] = f"{a:.6f}"
                    row["error"] = f"{abs(a - t) / max(abs(t), 1.0):.6f}"
            elif kind == "topk":
                truth_pairs = truth_topk(samples, params["k"])
                truth_keys = [k for k, _ in truth_pairs]
                sketch_keys = extract_topk_keys(rec.get("result"), params["k"])
                if truth_keys:
                    overlap = len(set(truth_keys) & set(sketch_keys))
                    row["recall"] = f"{overlap / len(truth_keys):.4f}"
                row["truth"] = json.dumps(truth_keys)[:120]
                row["answer"] = json.dumps(sketch_keys)[:120]
            elif kind == "count_unique":
                t = truth_count_unique(samples, params["by"])
                a = extract_scalar(rec.get("result"))
                row["truth"] = str(t)
                if a is not None:
                    row["answer"] = f"{a:.0f}"
                    row["error"] = f"{abs(a - t) / max(t, 1):.6f}"
            elif kind == "sum":
                t = truth_sum(samples)
                a = extract_scalar(rec.get("result"))
                row["truth"] = f"{t:.6f}"
                if a is not None:
                    row["answer"] = f"{a:.6f}"
                    row["error"] = f"{abs(a - t) / max(abs(t), 1.0):.6f}"

            writer.writerow(row)
            n_rows += 1
    return n_rows


def main() -> int:
    ap = argparse.ArgumentParser(description="Accuracy reducer (P8)")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--cell-dir", help="single cell directory to reduce")
    g.add_argument("--sweep-root", help="sweep root containing many cell dirs")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    fields = [
        "cell", "kind", "query", "t", "duration_ms", "plan_id",
        "truth", "answer", "error", "recall", "n_truth_samples",
    ]

    cells: list[tuple[str, str]] = []
    if args.cell_dir:
        cells.append((args.cell_dir, os.path.basename(args.cell_dir.rstrip("/"))))
    else:
        for entry in sorted(os.listdir(args.sweep_root)):
            full = os.path.join(args.sweep_root, entry)
            if os.path.isdir(full) and os.path.exists(os.path.join(full, "replay.jsonl")):
                cells.append((full, entry))

    total = 0
    with open(args.out, "w", newline="") as fout:
        w = csv.DictWriter(fout, fieldnames=fields)
        w.writeheader()
        for cell_dir, label in cells:
            rows = reduce_cell(cell_dir, w, label)
            print(f"[{label}] {rows} rows")
            total += rows

    print(f"reduce: total {total} rows → {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
