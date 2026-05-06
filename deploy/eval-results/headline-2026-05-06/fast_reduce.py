#!/usr/bin/env python3
"""Faster per-cell accuracy reducer.

Drop-in replacement for accuracy_reduce.py --cell-dir.

Optimization vs the original:
  - For each metric, walk the cold-truth ONCE and compute a small
    set of summary statistics keyed by (kind, params). All replay
    queries against the same metric reuse the precomputed truth
    instead of re-iterating the sample set per query.
  - Quantiles share a single sorted value array per metric.
  - Top-K shares a single Counter per metric.
  - count_unique{by=__series__} shares one set per metric.

Schema-compatible with accuracy_reduce.py output.
"""
from __future__ import annotations

import argparse
import csv
import glob
import json
import os
import re
import sys
from collections import Counter, defaultdict


_QUANTILE_HIST_RE = re.compile(
    r"histogram_quantile\(\s*([0-9.]+)\s*,\s*sum\s+by\s+\(\s*le\s*\)\s*\(\s*([\w_]+)\s*\)\s*\)",
    re.IGNORECASE,
)
_QUANTILE_OVER_TIME_RE = re.compile(
    r"quantile_over_time\(\s*([0-9.]+)\s*,\s*([\w_]+?)(?:_quantile)?\s*\[\s*[0-9smhd]+\s*\]\s*\)",
    re.IGNORECASE,
)
_TOPK_RE = re.compile(r"topk\(\s*(\d+)\s*,\s*([\w_]+)\s*\)", re.IGNORECASE)
_COUNT_UNIQUE_GROUP_RE = re.compile(
    r"count\(\s*count\s+by\s+\(\s*([\w_]+)\s*\)\s*\(\s*([\w_]+)\s*\)\s*\)",
    re.IGNORECASE,
)
_COUNT_SERIES_RE = re.compile(r"^count\(\s*([\w_]+)\s*\)$", re.IGNORECASE)
_SUM_INSTANT_RE = re.compile(r"^sum\(\s*([\w_]+)\s*\)$", re.IGNORECASE)
_SUM_OVER_TIME_RE = re.compile(
    r"sum_over_time\(\s*([\w_]+)\s*\[\s*[0-9smhd]+\s*\]\s*\)",
    re.IGNORECASE,
)


def parse_query(promql):
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


def extract_scalar(result):
    if not result:
        return None
    if isinstance(result, list) and result:
        first = result[0]
        if isinstance(first, dict) and "value" in first:
            v = first["value"]
            if isinstance(v, list) and len(v) >= 2:
                try:
                    return float(v[1])
                except Exception:
                    return None
    if isinstance(result, dict) and "value" in result:
        v = result["value"]
        if isinstance(v, list) and len(v) >= 2:
            try:
                return float(v[1])
            except Exception:
                return None
    return None


def extract_topk_keys(result, k):
    if not isinstance(result, list):
        return []
    out = []
    for el in result[:k]:
        if not isinstance(el, dict):
            continue
        m = el.get("metric") or {}
        out.append(json.dumps(m, sort_keys=True))
    return out


def precompute_truth(cell_dir, metric):
    """Single-pass over a metric's truth files. Returns:

      {
        "values_sorted": [...],          # for quantiles
        "topk_counter":  Counter(),      # full series→sum
        "series_keys":   set(),          # for count_unique by=__series__
        "by_label":      {label: set()}, # populated lazily on demand
        "total_sum":     float,          # for sum
        "n":             int,
      }
    """
    pat = os.path.join(cell_dir, "cold-truth", metric, "*", "*", "*", "*", "part-*.jsonl")
    files = sorted(glob.glob(pat))
    values = []
    topk = Counter()
    series_keys = set()
    by_label = defaultdict(set)
    total_sum = 0.0
    n = 0
    for path in files:
        with open(path, "r") as f:
            for line in f:
                try:
                    s = json.loads(line)
                except Exception:
                    continue
                v = s.get("value")
                if v is None:
                    continue
                values.append(v)
                lbls = s.get("labels", {})
                key = json.dumps(lbls, sort_keys=True)
                topk[key] += v
                series_keys.add(key)
                # Cache select label sets cheaply.
                for lk, lv in lbls.items():
                    by_label[lk].add(lv)
                total_sum += v
                n += 1
    values.sort()
    return {
        "values_sorted": values,
        "topk_counter": topk,
        "series_keys": series_keys,
        "by_label": by_label,
        "total_sum": total_sum,
        "n": n,
    }


def truth_quantile(values_sorted, q):
    n = len(values_sorted)
    if n == 0:
        return float("nan")
    idx = max(0, min(n - 1, int(q * n)))
    return values_sorted[idx]


def reduce_cell(cell_dir, out_path, label):
    replay_path = os.path.join(cell_dir, "replay.jsonl")
    cold_root = os.path.join(cell_dir, "cold-truth")
    if not os.path.exists(replay_path):
        print(f"[skip] no replay.jsonl in {cell_dir}", file=sys.stderr)
        return 0
    if not os.path.isdir(cold_root):
        print(f"[skip] no cold-truth/ in {cell_dir}", file=sys.stderr)
        return 0

    # Discover metrics that the replay actually queries; only
    # precompute those (avoids loading both metric directories
    # when one would suffice). Falls back to ALL directories if
    # we can't parse some queries — same behaviour as upstream.
    metrics_needed = set()
    seen_replay_lines = []
    with open(replay_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            seen_replay_lines.append(rec)
            parsed = parse_query(rec.get("query", ""))
            if parsed is None:
                continue
            metrics_needed.add(parsed[1].get("metric", ""))
    if not metrics_needed:
        metrics_needed = {
            d for d in os.listdir(cold_root)
            if os.path.isdir(os.path.join(cold_root, d))
        }

    truth_cache = {
        m: precompute_truth(cell_dir, m)
        for m in sorted(metrics_needed)
        if os.path.isdir(os.path.join(cold_root, m))
    }
    print(
        f"[{label}] truth precomputed for {len(truth_cache)} metrics; "
        f"sizes={[t['n'] for t in truth_cache.values()]}",
        file=sys.stderr,
    )

    fields = [
        "cell", "kind", "query", "t", "duration_ms", "plan_id",
        "truth", "answer", "error", "recall", "n_truth_samples",
    ]
    n_rows = 0
    with open(out_path, "w", newline="") as fout:
        w = csv.DictWriter(fout, fieldnames=fields)
        w.writeheader()
        for rec in seen_replay_lines:
            parsed = parse_query(rec.get("query", ""))
            if parsed is None:
                continue
            kind, params = parsed
            metric = params.get("metric", "")
            T = truth_cache.get(metric)
            row = {
                "cell": label,
                "kind": kind,
                "query": rec.get("query", ""),
                "t": rec.get("ts", ""),
                "duration_ms": rec.get("duration_ms"),
                "plan_id": rec.get("plan_id"),
                "n_truth_samples": (T["n"] if T else 0),
                "truth": "",
                "answer": "",
                "error": "",
                "recall": "",
            }
            if T is None:
                w.writerow(row)
                n_rows += 1
                continue

            if kind == "quantile":
                t = truth_quantile(T["values_sorted"], params["q"])
                a = extract_scalar(rec.get("result"))
                if t == t:
                    row["truth"] = f"{t:.6f}"
                if a is not None and t == t:
                    row["answer"] = f"{a:.6f}"
                    row["error"] = f"{abs(a - t) / max(abs(t), 1.0):.6f}"
            elif kind == "topk":
                k = params["k"]
                truth_keys = [kk for kk, _ in T["topk_counter"].most_common(k)]
                sketch_keys = extract_topk_keys(rec.get("result"), k)
                if truth_keys:
                    overlap = len(set(truth_keys) & set(sketch_keys))
                    row["recall"] = f"{overlap / len(truth_keys):.4f}"
                row["truth"] = json.dumps(truth_keys)[:120]
                row["answer"] = json.dumps(sketch_keys)[:120]
            elif kind == "count_unique":
                if params["by"] == "__series__":
                    t = len(T["series_keys"])
                else:
                    t = len(T["by_label"].get(params["by"], set()))
                a = extract_scalar(rec.get("result"))
                row["truth"] = str(t)
                if a is not None:
                    row["answer"] = f"{a:.0f}"
                    row["error"] = f"{abs(a - t) / max(t, 1):.6f}"
            elif kind == "sum":
                t = T["total_sum"]
                a = extract_scalar(rec.get("result"))
                row["truth"] = f"{t:.6f}"
                if a is not None:
                    row["answer"] = f"{a:.6f}"
                    row["error"] = f"{abs(a - t) / max(abs(t), 1.0):.6f}"
            w.writerow(row)
            n_rows += 1
    return n_rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cell-dir", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    label = os.path.basename(args.cell_dir.rstrip("/"))
    n = reduce_cell(args.cell_dir, args.out, label)
    print(f"[{label}] {n} rows -> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
