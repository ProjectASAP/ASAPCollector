#!/usr/bin/env python3
"""Build the four §5 plots from the merged 60-cell sweep.

This is a slim re-implementation of e2e_plots.py that handles two sweep
roots and degrades gracefully when accuracy.csv is partial.

Inputs:
  --accuracy        merged accuracy.csv (concat of per-cell)
  --out-dir         output dir for PNGs + CSVs
  --sweep-old       first sweep root (DD/KLL)
  --sweep-new       second sweep root (CS/CMS/HLL)

Outputs (4 PNGs):
  pareto_acc_vs_thru.png
  bandwidth_vs_n.png
  query_latency_cdf.png
  transition_timeline.png
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

PALETTE = {
    "ddsketch": "tab:blue",
    "kll": "tab:orange",
    "cs": "tab:green",
    "cms": "tab:red",
    "hll": "tab:purple",
}
CELL_RE = re.compile(r"(?P<fam>[a-z]+)_N(?P<n>\d+)_w(?P<w>\d+)ms_c(?P<card>\d+)")


def cell_meta(cell):
    m = CELL_RE.match(cell)
    if not m:
        return {"fam": cell, "n": None, "w_ms": None, "card": None}
    g = m.groupdict()
    return {"fam": g["fam"], "n": int(g["n"]), "w_ms": int(g["w"]), "card": int(g["card"])}


def parse_iso(s):
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return dt.datetime.fromisoformat(s)


def cell_dirs(roots):
    """Yield (cell_name, cell_dir) across both sweep roots."""
    seen = set()
    for root in roots:
        if not os.path.isdir(root):
            continue
        for ent in sorted(os.listdir(root)):
            full = os.path.join(root, ent)
            if not os.path.isdir(full):
                continue
            if not os.path.isfile(os.path.join(full, "measurement.csv")):
                continue
            if ent in seen:
                continue
            seen.add(ent)
            yield ent, full


# --- figure 1: pareto accuracy vs throughput
def fig_pareto(accuracy_csv, roots, out_dir):
    if not os.path.isfile(accuracy_csv):
        print("[fig1] accuracy.csv missing, skipping")
        return
    acc = pd.read_csv(accuracy_csv)
    if acc.empty:
        print("[fig1] empty accuracy CSV")
        return
    rows = []
    for cellname, d in cell_dirs(roots):
        meta = cell_meta(cellname)
        sub = acc[acc["cell"] == cellname]
        # Per-cell error: median over QUANTILE rows only — quantile is
        # the kind §5 makes a relative-error claim about. count_unique
        # collapses to 0 (HLL/CMS are near-exact on these workloads),
        # so mixing it would dominate the per-cell median and hide the
        # quantile signal we're plotting.
        sub_q = sub[sub["kind"] == "quantile"]
        errs = pd.to_numeric(sub_q["error"], errors="coerce").dropna()
        med_err = float(errs.median()) if len(errs) else float("nan")
        sub_t = sub[sub["kind"] == "topk"]
        rec = pd.to_numeric(sub_t["recall"], errors="coerce").dropna()
        med_recall_loss = (1.0 - float(rec.median())) if len(rec) else float("nan")

        # producer cpu from sample.jsonl (median of fake-exporter cpu_pct)
        thru = float("nan")
        sample_path = os.path.join(d, "sample.jsonl")
        if os.path.exists(sample_path):
            cpus = []
            for ln in open(sample_path):
                try:
                    r = json.loads(ln)
                except Exception:
                    continue
                if r.get("container") == "fake-exporter":
                    cpus.append(r.get("cpu_pct", 0.0))
            if cpus:
                thru = float(pd.Series(cpus).median())

        rows.append({
            "cell": cellname, **meta,
            "median_error": med_err,
            "median_recall_loss": med_recall_loss,
            "median_producer_cpu_pct": thru,
        })
    df = pd.DataFrame(rows)
    df.to_csv(os.path.join(out_dir, "pareto_acc_vs_thru.csv"), index=False)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    for fam, sub in df.groupby("fam"):
        ax1.scatter(sub["median_error"], sub["median_producer_cpu_pct"],
                    label=fam, color=PALETTE.get(fam, "gray"), s=70, alpha=0.7)
    ax1.set_xscale("symlog", linthresh=1e-4)
    ax1.set_xlabel("median relative error (symlog)")
    ax1.set_ylabel("producer cpu %  (throughput proxy)")
    ax1.set_title("Pareto: error vs throughput\n(quantile / cardinality / sum)")
    ax1.legend(fontsize=8)
    ax1.grid(True, alpha=0.3)

    plotted_recall = False
    for fam, sub in df.groupby("fam"):
        if sub["median_recall_loss"].notna().any():
            ax2.scatter(sub["median_recall_loss"], sub["median_producer_cpu_pct"],
                        label=fam, color=PALETTE.get(fam, "gray"), s=70, alpha=0.7)
            plotted_recall = True
    ax2.set_xlabel("1 - top-K recall  (lower is better)")
    ax2.set_ylabel("producer cpu %")
    ax2.set_title("Pareto: top-K recall loss vs throughput")
    if plotted_recall:
        ax2.legend(fontsize=8)
    ax2.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "pareto_acc_vs_thru.png"), dpi=150)
    plt.close(fig)
    print("[fig1] pareto_acc_vs_thru.png")


# --- figure 2: bandwidth vs N (from sample.jsonl agent net_tx_mb)
def fig_bandwidth(roots, out_dir):
    rows = []
    for cellname, d in cell_dirs(roots):
        meta = cell_meta(cellname)
        path = os.path.join(d, "sample.jsonl")
        if not os.path.exists(path):
            continue
        agent_tx = []
        for ln in open(path):
            try:
                r = json.loads(ln)
            except Exception:
                continue
            if r.get("container", "").startswith("agent"):
                agent_tx.append(r.get("net_tx_mb", 0.0))
        if not agent_tx:
            continue
        s = pd.Series(agent_tx)
        rows.append({**meta, "cell": cellname,
                     "agent_tx_p50_mb": float(s.median()),
                     "agent_tx_p99_mb": float(s.quantile(0.99))})
    if not rows:
        print("[fig2] no agent samples")
        return
    df = pd.DataFrame(rows)
    df.to_csv(os.path.join(out_dir, "bandwidth_vs_n.csv"), index=False)

    fig, ax = plt.subplots(figsize=(8, 5))
    for fam, sub in df.groupby("fam"):
        agg = sub.groupby("n")["agent_tx_p99_mb"].median().sort_index()
        if not len(agg):
            continue
        ax.plot(agg.index, agg.values, marker="o",
                label=fam, color=PALETTE.get(fam, "gray"))
    ax.set_xlabel("number of agents (N)")
    ax.set_ylabel("p99 agent tx bytes (MiB, total since start)")
    ax.set_title("Bandwidth vs N")
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "bandwidth_vs_n.png"), dpi=150)
    plt.close(fig)
    print("[fig2] bandwidth_vs_n.png")


# --- figure 3: plan-transition timeline
def fig_transition(roots, out_dir):
    rows = []
    for cellname, d in cell_dirs(roots):
        meta = cell_meta(cellname)
        path = os.path.join(d, "transition.jsonl")
        if not os.path.exists(path):
            continue
        try:
            tr = json.loads(open(path).read())
        except Exception:
            continue
        if not tr.get("t_query_in"):
            continue
        t0 = parse_iso(tr["t_query_in"])

        def offs(key):
            v = tr.get(key)
            if v is None:
                return None
            return (parse_iso(v) - t0).total_seconds()

        rows.append({
            **meta, "cell": cellname,
            "t_plan_ready_s": offs("t_plan_ready"),
            "t_first_hit_s": offs("t_first_hit"),
            "t_steady_s": offs("t_steady"),
            "before_plan": tr.get("before_plan"),
            "after_plan": tr.get("after_plan"),
            "transition_query": tr.get("transition_query"),
        })
    if not rows:
        print("[fig3] no transition records")
        return
    df = pd.DataFrame(rows).sort_values(["fam", "n", "card"]).reset_index(drop=True)
    df.to_csv(os.path.join(out_dir, "transition_timeline.csv"), index=False)

    plot_df = df.dropna(subset=["t_plan_ready_s", "t_first_hit_s", "t_steady_s"])
    if plot_df.empty:
        # All transitions failed to complete in the observation
        # window — surface that fact in the plot rather than skip,
        # because §5 needs a figure that documents the outcome.
        fig, ax = plt.subplots(figsize=(10, 6))
        # Bar of who DID get a query_in but no plan_ready
        groups = df["fam"].value_counts().sort_index()
        bars = ax.bar(groups.index, groups.values,
                      color=[PALETTE.get(f, "gray") for f in groups.index])
        for b, n in zip(bars, groups.values):
            ax.text(b.get_x() + b.get_width()/2, b.get_height() + 0.2, str(n),
                    ha="center", va="bottom", fontsize=10)
        ax.set_ylabel("# cells with transition_query but no t_plan_ready")
        ax.set_title("Plan-transition timeline\n(no cell reached t_steady within the 60s soak)")
        ax.grid(True, axis="y", alpha=0.3)
        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, "transition_timeline.png"), dpi=150)
        plt.close(fig)
        print(
            f"[fig3] transition_timeline.png (degenerate: 0/{len(df)} cells "
            "reached t_steady; rendered as failure-mode bar)"
        )
        return

    df = plot_df.reset_index(drop=True)
    fig, ax = plt.subplots(figsize=(10, max(4, 0.3 * len(df))))
    y = range(len(df))
    ax.barh(list(y), df["t_plan_ready_s"], height=0.7,
            color="tab:blue", label="t_plan_ready")
    ax.barh(list(y), df["t_first_hit_s"] - df["t_plan_ready_s"],
            left=df["t_plan_ready_s"], height=0.7,
            color="tab:green", label="→ t_first_hit")
    ax.barh(list(y), df["t_steady_s"] - df["t_first_hit_s"],
            left=df["t_first_hit_s"], height=0.7,
            color="tab:orange", label="→ t_steady")
    ax.set_yticks(list(y))
    ax.set_yticklabels(df["cell"], fontsize=7)
    ax.set_xlabel("seconds since query_in")
    ax.set_title("Plan-transition timeline")
    ax.legend(fontsize=9, loc="lower right")
    ax.grid(True, axis="x", alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "transition_timeline.png"), dpi=150)
    plt.close(fig)
    print("[fig3] transition_timeline.png")


# --- figure 4: query-latency CDF (read replay.jsonl directly so it
# works without accuracy.csv)
def fig_latency_cdf(roots, out_dir):
    rows_raw = []
    for cellname, d in cell_dirs(roots):
        meta = cell_meta(cellname)
        path = os.path.join(d, "replay.jsonl")
        if not os.path.exists(path):
            continue
        for ln in open(path):
            try:
                r = json.loads(ln)
            except Exception:
                continue
            if r.get("status") != "success":
                continue
            dur = r.get("duration_ms")
            if not isinstance(dur, (int, float)):
                continue
            rows_raw.append({
                "cell": cellname,
                "fam": meta["fam"],
                "n": meta["n"],
                "w_ms": meta["w_ms"],
                "card": meta["card"],
                "query": r.get("query", ""),
                "duration_ms": float(dur),
                "plan_id": r.get("plan_id", ""),
            })
    if not rows_raw:
        print("[fig4] no replay durations")
        return
    df = pd.DataFrame(rows_raw)
    df.to_csv(os.path.join(out_dir, "query_latency_cdf.csv"), index=False)

    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    # left: per-family CDF
    ax = axes[0]
    for fam, sub in df.groupby("fam"):
        v = sorted(sub["duration_ms"].values)
        if not v:
            continue
        n = len(v)
        ys = [(i + 1) / n for i in range(n)]
        ax.plot(v, ys, label=fam, color=PALETTE.get(fam, "gray"))
    ax.set_xscale("log")
    ax.set_xlabel("query duration (ms, log)")
    ax.set_ylabel("CDF")
    ax.set_title("Query-latency CDF, all queries (per family)")
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)
    # right: only WARM-tier queries (everything except count(metric)
    # which is a known cold-fallback path). Lets the reader see the
    # warm-tier envelope §5 actually claims.
    ax = axes[1]
    warm = df[~df["query"].str.startswith("count(http_requests_total)")]
    for fam, sub in warm.groupby("fam"):
        v = sorted(sub["duration_ms"].values)
        if not v:
            continue
        n = len(v)
        ys = [(i + 1) / n for i in range(n)]
        ax.plot(v, ys, label=fam, color=PALETTE.get(fam, "gray"))
    ax.set_xscale("log")
    ax.set_xlabel("query duration (ms, log)")
    ax.set_ylabel("CDF")
    ax.set_title("Warm-tier only\n(quantile / topk / sum_over_time; count(·) excluded)")
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "query_latency_cdf.png"), dpi=150)
    plt.close(fig)
    print("[fig4] query_latency_cdf.png")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--accuracy", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--sweep-old", required=True)
    ap.add_argument("--sweep-new", required=True)
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    roots = [args.sweep_old, args.sweep_new]
    fig_pareto(args.accuracy, roots, args.out_dir)
    fig_bandwidth(roots, args.out_dir)
    fig_transition(roots, args.out_dir)
    fig_latency_cdf(roots, args.out_dir)
    print(f"plots: written to {args.out_dir}")


if __name__ == "__main__":
    sys.exit(main())
