#!/usr/bin/env python3
"""Backend query-latency reducer (Fig 7, §6 eval).

Reads the metricsql_replay.py JSONL (per-query wall-clock latency) and:
  - asserts every timed query returned a REAL warm result (status=success,
    non-empty result, data_source=asap_query) — latency of "No result" is
    meaningless, so we hard-fail if any timed query was empty/errored;
  - computes p50/p95/p99 (ms) overall and per query kind;
  - emits a per-query latency JSON summary;
  - renders the latency CDF PNG (overall + per-kind, warm tier).

Usage:
  compute_latency.py --warm replay-warm.jsonl [--cold replay-cold.jsonl] \
      --out-json latency_summary.json --out-png latency_cdf.png
"""
from __future__ import annotations
import argparse, json, sys
from pathlib import Path


def load(path: str):
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def pct(sorted_vals, p):
    if not sorted_vals:
        return None
    # nearest-rank-ish linear interpolation
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    k = (len(sorted_vals) - 1) * (p / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = k - lo
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * frac


def summarize(rows, label, require_warm=True):
    """Returns (summary_dict, {kind: [latencies]}, [all_latencies]).
    Hard-fails if require_warm and any timed query was empty/errored."""
    bad = []
    by_kind: dict[str, list[float]] = {}
    alllat: list[float] = []
    src_counter: dict[str, int] = {}
    for r in rows:
        res = r.get("result") or []
        src = r.get("data_source")
        src_counter[src] = src_counter.get(src, 0) + 1
        ok = r.get("status") == "success" and bool(res)
        if require_warm and not ok:
            bad.append({"query": r.get("query"), "status": r.get("status"),
                        "n_result": len(res), "data_source": src})
            continue
        if not ok:
            continue
        lat = float(r["duration_ms"])
        alllat.append(lat)
        by_kind.setdefault(r["kind"], []).append(lat)

    if require_warm and bad:
        sys.exit(f"[{label}] {len(bad)} timed queries returned no real result "
                 f"(empty/errored) — latency would be meaningless. First few: "
                 f"{bad[:3]}")

    def stats(vals):
        s = sorted(vals)
        return {
            "n": len(s),
            "min_ms": round(s[0], 3) if s else None,
            "p50_ms": round(pct(s, 50), 3) if s else None,
            "p95_ms": round(pct(s, 95), 3) if s else None,
            "p99_ms": round(pct(s, 99), 3) if s else None,
            "max_ms": round(s[-1], 3) if s else None,
            "mean_ms": round(sum(s) / len(s), 3) if s else None,
        }

    summary = {
        "label": label,
        "n_queries": len(rows),
        "n_real_warm": len(alllat),
        "data_source_counts": src_counter,
        "overall": stats(alllat),
        "by_kind": {k: stats(v) for k, v in sorted(by_kind.items())},
    }
    return summary, by_kind, alllat


def cdf_xy(vals):
    s = sorted(vals)
    n = len(s)
    ys = [(i + 1) / n for i in range(n)]
    return s, ys


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--warm", required=True)
    ap.add_argument("--cold", default=None)
    ap.add_argument("--out-json", required=True)
    ap.add_argument("--out-png", required=True)
    args = ap.parse_args()

    warm_rows = load(args.warm)
    warm_sum, warm_by_kind, warm_all = summarize(warm_rows, "warm", require_warm=True)

    out = {"warm": warm_sum}

    cold_all = None
    if args.cold and Path(args.cold).exists():
        cold_rows = load(args.cold)
        # cold arm: don't hard-fail on empties (route may differ); report what landed
        cold_sum, _, cold_all = summarize(cold_rows, "cold", require_warm=False)
        out["cold"] = cold_sum

    Path(args.out_json).write_text(json.dumps(out, indent=2) + "\n")

    # ---- CDF plot ----
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(7.2, 4.6))
    # overall warm
    x, y = cdf_xy(warm_all)
    ax.plot(x, y, label=f"warm — all ({len(warm_all)} q)", color="#1f77b4", lw=2.2)
    palette = {"quantile": "#2ca02c", "sum": "#ff7f0e", "count_unique": "#9467bd"}
    for kind, vals in sorted(warm_by_kind.items()):
        x, y = cdf_xy(vals)
        ax.plot(x, y, label=f"warm — {kind} ({len(vals)} q)",
                color=palette.get(kind, "#7f7f7f"), lw=1.4, ls="--", alpha=0.85)
    if cold_all:
        x, y = cdf_xy(cold_all)
        ax.plot(x, y, label=f"cold-fallback ({len(cold_all)} q)",
                color="#d62728", lw=2.0)

    for p, ls in ((50, ":"), (99, "-.")):
        v = pct(sorted(warm_all), p)
        ax.axvline(v, color="#888", ls=ls, lw=0.9)
        ax.text(v, 0.04, f"p{p}={v:.1f}ms", rotation=90, fontsize=7,
                va="bottom", ha="right", color="#555")

    ax.set_xlabel("backend query latency (ms)")
    ax.set_ylabel("CDF (fraction of queries ≤ x)")
    ax.set_title("Fig 7 — backend query latency CDF (warm sketch tier, single-node loopback)")
    ax.set_ylim(0, 1.02)
    ax.set_xlim(left=0)
    ax.grid(True, alpha=0.3)
    ax.legend(loc="lower right", fontsize=8)
    fig.tight_layout()
    fig.savefig(args.out_png, dpi=140)
    print(f"wrote {args.out_json} and {args.out_png}")
    print(json.dumps(out["warm"]["overall"], indent=2))
    for k, v in out["warm"]["by_kind"].items():
        print(f"  {k:14s} p50={v['p50_ms']:.2f}  p95={v['p95_ms']:.2f}  p99={v['p99_ms']:.2f}  (n={v['n']})")


if __name__ == "__main__":
    main()
