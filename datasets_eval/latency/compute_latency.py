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


def _n_result(r) -> int:
    """Result count from either the full replay schema (`result` vector) or
    the slim per-query schema (`n_result_series`)."""
    res = r.get("result")
    if res is not None:
        return len(res)
    return int(r.get("n_result_series") or 0)


def summarize(rows, label, require_real=True, require_source=None):
    """Returns (summary_dict, {kind: [latencies]}, [all_latencies]).
    Hard-fails if require_real and any timed query was empty/errored, or if
    require_source is set and any timed query was served by a different
    data_source (guards that the arm's answers came from the intended tier)."""
    bad = []
    wrong_src = []
    by_kind: dict[str, list[float]] = {}
    alllat: list[float] = []
    src_counter: dict[str, int] = {}
    for r in rows:
        n = _n_result(r)
        src = r.get("data_source")
        src_counter[src] = src_counter.get(src, 0) + 1
        ok = r.get("status") == "success" and n > 0
        if require_real and not ok:
            bad.append({"query": r.get("query"), "status": r.get("status"),
                        "n_result": n, "data_source": src})
            continue
        if require_source is not None and src != require_source:
            wrong_src.append({"query": r.get("query"), "data_source": src})
            continue
        if not ok:
            continue
        lat = float(r["duration_ms"])
        alllat.append(lat)
        by_kind.setdefault(r["kind"], []).append(lat)

    if require_real and bad:
        sys.exit(f"[{label}] {len(bad)} timed queries returned no real result "
                 f"(empty/errored) — latency would be meaningless. First few: "
                 f"{bad[:3]}")
    if require_source is not None and wrong_src:
        sys.exit(f"[{label}] {len(wrong_src)} timed queries were NOT served by "
                 f"data_source={require_source} (wrong tier — this arm must "
                 f"measure that tier only). First few: {wrong_src[:3]}")

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


def load_any(path: str):
    """Load either a JSONL replay log (one object per line) or a JSON array
    (the committed slim per_query_latency*.json)."""
    text = Path(path).read_text().lstrip()
    if text.startswith("["):
        return json.loads(text)
    return load(path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--warm", required=True,
                    help="warm replay JSONL or committed per_query_latency.json")
    ap.add_argument("--cold", default=None,
                    help="cold replay JSONL or per_query_latency_cold.json")
    ap.add_argument("--out-json", required=True)
    ap.add_argument("--out-png", required=True)
    args = ap.parse_args()

    warm_rows = load_any(args.warm)
    # Warm arm: every timed query must be a real warm answer (data_source=asap_query).
    warm_sum, warm_by_kind, warm_all = summarize(
        warm_rows, "warm", require_real=True, require_source="asap_query")

    out = {"warm": warm_sum}

    cold_all = None
    cold_by_kind = None
    if args.cold and Path(args.cold).exists():
        cold_rows = load_any(args.cold)
        # Cold arm: GUARD that every timed query landed real AND was served by
        # the cold/archive engine (data_source=thanos_query) — we are measuring
        # the cold-fallback tier, so a warm shortcut would invalidate the arm.
        cold_sum, cold_by_kind, cold_all = summarize(
            cold_rows, "cold", require_real=True, require_source="thanos_query")
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
        ax.plot(x, y, label=f"cold-fallback — all ({len(cold_all)} q)",
                color="#d62728", lw=2.2)
        cold_palette = {"quantile": "#8c564b", "sum": "#e377c2"}
        for kind, vals in sorted((cold_by_kind or {}).items()):
            x, y = cdf_xy(vals)
            ax.plot(x, y, label=f"cold — {kind} ({len(vals)} q)",
                    color=cold_palette.get(kind, "#d62728"), lw=1.4, ls="--", alpha=0.85)

    # percentile guide lines: warm (solid grey) + cold (red) overall p50/p99
    for p, ls in ((50, ":"), (99, "-.")):
        v = pct(sorted(warm_all), p)
        ax.axvline(v, color="#888", ls=ls, lw=0.9)
        ax.text(v, 0.04, f"warm p{p}={v:.1f}ms", rotation=90, fontsize=6,
                va="bottom", ha="right", color="#555")
        if cold_all:
            cv = pct(sorted(cold_all), p)
            ax.axvline(cv, color="#d62728", ls=ls, lw=0.8, alpha=0.6)
            ax.text(cv, 0.04, f"cold p{p}={cv:.1f}ms", rotation=90, fontsize=6,
                    va="bottom", ha="right", color="#d62728")

    ax.set_xlabel("backend query latency (ms)")
    ax.set_ylabel("CDF (fraction of queries ≤ x)")
    title = "Fig 7 — backend query latency CDF (single-node loopback)"
    if cold_all:
        title = ("Fig 7 — backend query latency CDF: warm sketch tier vs "
                 "cold-fallback archive\n(single-node loopback)")
    ax.set_title(title)
    ax.set_ylim(0, 1.02)
    ax.set_xlim(left=0)
    ax.grid(True, alpha=0.3)
    ax.legend(loc="lower right", fontsize=7)
    fig.tight_layout()
    fig.savefig(args.out_png, dpi=140)
    print(f"wrote {args.out_json} and {args.out_png}")
    print("WARM:", json.dumps(out["warm"]["overall"]))
    for k, v in out["warm"]["by_kind"].items():
        print(f"  warm {k:10s} p50={v['p50_ms']:.2f}  p95={v['p95_ms']:.2f}  p99={v['p99_ms']:.2f}  (n={v['n']})")
    if cold_all:
        print("COLD:", json.dumps(out["cold"]["overall"]))
        for k, v in out["cold"]["by_kind"].items():
            print(f"  cold {k:10s} p50={v['p50_ms']:.2f}  p95={v['p95_ms']:.2f}  p99={v['p99_ms']:.2f}  (n={v['n']})")


if __name__ == "__main__":
    main()
