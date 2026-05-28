#!/usr/bin/env python3
"""Compare warm backend query results to exact offline ground truth.

Consumes (a) the GT map from gt_eval.evaluate_queries and (b) a
normalized warm-result map produced by run_e2e.py from the data-plane
query responses, and applies per-family success thresholds:

  quantile      rel_err < 0.02            (DDSketch / KLL)
  sum           rel_err <= 1e-6           (Sum envelope — lossless)
  count_unique  rel_err < 0.02            (HLL)
  topk          overlap@k >= 0.80 AND spearman > 0.70   (CountSketch)
  frequency     warm >= gt (one-sided) AND rel_err < 0.05  (CMS estimate)

A warm result that fell through to the archive tier is a FAIL even if
the number matches — the harness records `data_source` per query and
flags any non-warm source.

Normalized warm-result shapes (per query id):
  scalar family (global quantile/sum/count/frequency):  float
  grouped family (by-zone quantile/sum):                {group_key: float}
  topk family:                                          {member_key: float}
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any


# Per-family thresholds (rel-err unless noted).
THRESHOLDS: dict[str, dict[str, float]] = {
    "quantile": {"rel_err": 0.02},
    "sum": {"rel_err": 1e-6},
    "count_unique": {"rel_err": 0.02},
    "topk": {"overlap": 0.80, "spearman": 0.70},
    "frequency": {"rel_err": 0.05},
}


def _rel_err(warm: float, gt: float) -> float:
    denom = max(abs(gt), 1e-12)
    return abs(warm - gt) / denom


def _spearman(common: list[str], warm: dict[str, float], gt: dict[str, float]) -> float:
    """Spearman rank correlation over the keys present in BOTH maps."""
    if len(common) < 2:
        return 1.0 if common else 0.0
    def ranks(d: dict[str, float]) -> dict[str, float]:
        ordered = sorted(common, key=lambda k: d[k])
        # average ranks for ties
        rk: dict[str, float] = {}
        i = 0
        while i < len(ordered):
            j = i
            while j + 1 < len(ordered) and d[ordered[j + 1]] == d[ordered[i]]:
                j += 1
            avg = (i + j) / 2.0 + 1.0
            for t in range(i, j + 1):
                rk[ordered[t]] = avg
            i = j + 1
        return rk
    rw, rg = ranks(warm), ranks(gt)
    n = len(common)
    d2 = sum((rw[k] - rg[k]) ** 2 for k in common)
    return 1.0 - (6.0 * d2) / (n * (n * n - 1))


def compare_one(kind: str, warm: Any, gt: Any) -> dict[str, Any]:
    """Return a result dict {pass, metric, detail} for one query."""
    th = THRESHOLDS.get(kind, {})

    if kind in ("quantile", "sum", "count_unique"):
        if isinstance(gt, dict):  # grouped
            errs = {}
            for g, gv in gt.items():
                wv = warm.get(g) if isinstance(warm, dict) else None
                errs[g] = _rel_err(float(wv), float(gv)) if wv is not None else float("inf")
            worst = max(errs.values()) if errs else float("inf")
            ok = worst < th["rel_err"]
            return {"pass": ok, "metric": "max_rel_err", "value": worst,
                    "threshold": th["rel_err"], "per_group": errs}
        err = _rel_err(float(warm), float(gt)) if warm is not None else float("inf")
        return {"pass": err < th["rel_err"], "metric": "rel_err",
                "value": err, "threshold": th["rel_err"]}

    if kind == "topk":
        if not isinstance(warm, dict):
            return {"pass": False, "metric": "overlap", "value": 0.0,
                    "detail": "warm result not a top-k map"}
        gset, wset = set(gt.keys()), set(warm.keys())
        k = max(len(gset), 1)
        overlap = len(gset & wset) / k
        common = sorted(gset & wset)
        rho = _spearman(common, warm, gt)
        ok = overlap >= th["overlap"] and rho > th["spearman"]
        return {"pass": ok, "metric": "overlap/spearman",
                "overlap": overlap, "spearman": rho,
                "overlap_threshold": th["overlap"], "spearman_threshold": th["spearman"],
                "missing": sorted(gset - wset)}

    if kind == "frequency":
        if warm is None:
            return {"pass": False, "metric": "rel_err", "value": float("inf"),
                    "detail": "no warm result (capability miss / archive fallthrough?)"}
        w, g = float(warm), float(gt)
        one_sided = w >= g - 1e-9  # CMS over-estimates
        err = _rel_err(w, g)
        return {"pass": one_sided and err < th["rel_err"], "metric": "rel_err",
                "value": err, "threshold": th["rel_err"], "one_sided_ok": one_sided}

    return {"pass": False, "metric": "unknown_kind", "detail": kind}


def compare_all(
    queries: list[dict[str, Any]],
    gt: dict[str, Any],
    warm: dict[str, Any],
    data_source: dict[str, str] | None = None,
) -> dict[str, Any]:
    data_source = data_source or {}
    results: dict[str, Any] = {}
    n_pass = 0
    for q in queries:
        qid = q.get("id") or q.get("promql")
        if qid not in gt:
            continue
        r = compare_one(q["kind"], warm.get(qid), gt[qid])
        src = data_source.get(qid, "unknown")
        # A non-warm data source is a fail regardless of numeric match.
        if src not in ("", "unknown") and "archive" in src.lower():
            r["pass"] = False
            r["data_source_fail"] = src
        r["kind"] = q["kind"]
        r["data_source"] = src
        results[qid] = r
        n_pass += 1 if r["pass"] else 0
    return {"n_queries": len(results), "n_pass": n_pass,
            "n_fail": len(results) - n_pass, "results": results}


def render_report(summary: dict[str, Any]) -> str:
    lines = ["# google_cluster E2E accuracy report", ""]
    lines.append(f"**{summary['n_pass']}/{summary['n_queries']} queries passed** "
                 f"({summary['n_fail']} failed)\n")
    lines.append("| query | kind | metric | value | threshold | source | pass |")
    lines.append("|---|---|---|---|---|---|---|")
    for qid, r in sorted(summary["results"].items()):
        if r["metric"] == "overlap/spearman":
            val = f"ov={r['overlap']:.2f} ρ={r['spearman']:.2f}"
            thr = f"ov≥{r['overlap_threshold']} ρ>{r['spearman_threshold']}"
        else:
            v = r.get("value")
            val = f"{v:.3g}" if isinstance(v, (int, float)) and math.isfinite(v) else str(v)
            thr = str(r.get("threshold", ""))
        lines.append(f"| {qid} | {r['kind']} | {r['metric']} | {val} | {thr} "
                     f"| {r.get('data_source','?')} | {'✅' if r['pass'] else '❌'} |")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Compare warm results to GT.")
    ap.add_argument("--queries", type=Path,
                    default=Path(__file__).resolve().parent.parent / "queries.json")
    ap.add_argument("--gt", type=Path, required=True, help="GT JSON from gt_eval.")
    ap.add_argument("--warm", type=Path, required=True, help="Normalized warm-result JSON.")
    ap.add_argument("--data-source", type=Path, default=None,
                    help="Optional {query_id: data_source} JSON.")
    ap.add_argument("--report", type=Path, default=None)
    args = ap.parse_args(argv)

    queries = json.loads(args.queries.read_text())
    gt = json.loads(args.gt.read_text())
    warm = json.loads(args.warm.read_text())
    ds = json.loads(args.data_source.read_text()) if args.data_source else {}
    summary = compare_all(queries, gt, warm, ds)
    report = render_report(summary)
    if args.report:
        args.report.write_text(report)
    print(report)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if summary["n_fail"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
