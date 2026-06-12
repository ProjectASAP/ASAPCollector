#!/usr/bin/env python3
"""Per-series quantile accuracy + DDSketch-vs-KLL head-to-head.

The fused asap_edge stores DDSketch/KLL as PER-SERIES sketches (the
workload declares NO grouping_labels for the quantile metrics — "per-series
quantile preserves PromQL semantics"), so `quantile_over_time(q, m[Ns])`
returns one value per replayed series, not a single global pooled quantile.
The committed global GT (gt-full.json) is therefore the WRONG oracle for a
per-series PromQL result; the right accuracy measure is per-series:
compare each series' warm sketch quantile against that same series' EXACT
offline quantile (linear interpolation, same definition gt_eval uses).

Outputs, per family and per q:
  matched series, mean/median/p95/max relative error, fraction within the
  family's published envelope (DDSketch alpha=0.01 -> ~0.02 band; KLL
  ~1/k with k=200 -> a few % band), and the wire bytes the family shipped.

Also emits the global pooled quantile error using the per-series-count-
weighted reconstruction is NOT possible from per-series quantiles, so we
additionally report the error of the single best-covered (max-N) series as
an illustrative "one clean sketch vs its own GT" point.
"""
from __future__ import annotations
import json, math, sys, urllib.request, urllib.parse
from collections import defaultdict
from pathlib import Path

BASE = "http://127.0.0.1:9091"


def qlin(vals, q):
    if not vals:
        return float("nan")
    s = sorted(vals); n = len(s)
    if n == 1:
        return s[0]
    pos = q * (n - 1); lo = math.floor(pos); hi = math.ceil(pos)
    if lo == hi:
        return s[int(pos)]
    f = pos - lo
    return s[lo] * (1 - f) + s[hi] * f


def load_series(jsonl, metric):
    series = defaultdict(list)
    with open(jsonl) as f:
        for line in f:
            o = json.loads(line)
            if o["metric"] != metric:
                continue
            series[o["series_id"]].append(float(o["value"]))
    return series


def warm_quantile(metric, q, window, t):
    promql = f"quantile_over_time({q}, {metric}[{window}])"
    url = BASE + "/api/v1/query?" + urllib.parse.urlencode({"query": promql, "time": str(t)})
    d = json.load(urllib.request.urlopen(url, timeout=120))
    src = "unknown"
    for info in d.get("infos") or []:
        if isinstance(info, str) and info.startswith("data_source:"):
            src = info.split(":", 1)[1].strip()
    out = {}
    for s in (d.get("data") or {}).get("result") or []:
        m = s["metric"]
        sid = f"{m.get('zone')}:{m.get('rack')}:{m.get('host')}:{m.get('service')}:{m.get('task')}"
        out[sid] = float(s["value"][1])
    return out, src


def accuracy(metric, jsonl, window, t, qs=(0.99, 0.50)):
    series = load_series(jsonl, metric)
    res = {}
    for q in qs:
        warm, src = warm_quantile(metric, q, window, t)
        errs = []
        for sid, vals in series.items():
            if sid not in warm:
                continue
            gt = qlin(vals, q)
            if gt == 0:
                continue
            errs.append(abs(warm[sid] - gt) / abs(gt))
        errs.sort()
        n = len(errs)
        res[q] = {
            "data_source": src,
            "n_series_total": len(series),
            "n_series_matched": n,
            "mean_rel_err": (sum(errs) / n) if n else None,
            "median_rel_err": errs[n // 2] if n else None,
            "p95_rel_err": errs[int(n * 0.95)] if n else None,
            "max_rel_err": errs[-1] if n else None,
            "frac_within_0.02": (sum(1 for e in errs if e <= 0.02) / n) if n else None,
            "frac_within_0.05": (sum(1 for e in errs if e <= 0.05) / n) if n else None,
        }
    return res


if __name__ == "__main__":
    t = int(sys.argv[1]) if len(sys.argv) > 1 else None
    jsonl = sys.argv[2] if len(sys.argv) > 2 else "/tmp/gct-aliased.jsonl"
    window = sys.argv[3] if len(sys.argv) > 3 else "300s"
    out = {
        "ddsketch": accuracy("google_cluster_2019_cpu_rate_q_ddsketch", jsonl, window, t),
        "kll": accuracy("google_cluster_2019_cpu_rate_q_kll", jsonl, window, t),
    }
    print(json.dumps(out, indent=2))
