#!/usr/bin/env python3
"""E2E plots (P9) — produces four figures + their underlying CSVs.

Inputs:

  --sweep-root  directory containing per-cell subdirs with
                replay.jsonl, transition.jsonl, sample.jsonl and
                an accuracy CSV (`accuracy_reduce.py --sweep-root
                ... --out <root>/all-accuracy.csv` should run
                first)
  --accuracy    path to the per-sweep accuracy CSV produced by
                accuracy_reduce.py
  --out-dir     output directory; gets <fig>.png + <fig>.csv per
                figure

Figures:

  1. pareto_acc_vs_thru.png — accuracy (median error / 1-recall)
     on x, throughput (median fake-exporter cpu_pct) on y, one
     point per cell, coloured by sketch family.
  2. bandwidth_vs_n.png — agent net_tx_mb p50/p99 vs N, faceted
     by sketch family + scrape window.
  3. transition_timeline.png — horizontal bar per cell showing
     t_query_in → t_plan_ready → t_first_hit → t_steady offsets.
  4. query_latency_cdf.png — empirical CDF of duration_ms per
     sketch family.

Usage:

  python3 accuracy_reduce.py --sweep-root /tmp/sweep \\
      --out /tmp/sweep/all-accuracy.csv
  python3 e2e_plots.py \\
      --sweep-root /tmp/sweep \\
      --accuracy /tmp/sweep/all-accuracy.csv \\
      --out-dir /tmp/sweep/plots
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd


CELL_RE = re.compile(
    r"(?P<fam>[a-z]+)_N(?P<n>\d+)_w(?P<w>\d+)ms_c(?P<card>\d+)"
)


def cell_meta(cell: str) -> dict:
    m = CELL_RE.match(cell)
    if not m:
        return {"fam": cell, "n": None, "w_ms": None, "card": None}
    g = m.groupdict()
    return {"fam": g["fam"], "n": int(g["n"]), "w_ms": int(g["w"]), "card": int(g["card"])}


def parse_iso(s: str) -> dt.datetime:
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return dt.datetime.fromisoformat(s)


# --- figure 1: accuracy vs throughput ------------------------------


def fig_pareto(accuracy_csv: str, sweep_root: str, out_dir: str) -> None:
    acc = pd.read_csv(accuracy_csv)
    if acc.empty:
        print("[fig1] empty accuracy CSV; skipping")
        return

    # Per-cell summary stats.
    rows = []
    for cell, df in acc.groupby("cell"):
        meta = cell_meta(cell)
        # Quantile / count_unique / sum: median relative error.
        errs = pd.to_numeric(df["error"], errors="coerce").dropna()
        med_err = float(errs.median()) if len(errs) else float("nan")
        # topk: 1 - median recall.
        rec = pd.to_numeric(df["recall"], errors="coerce").dropna()
        med_recall_loss = (1.0 - float(rec.median())) if len(rec) else float("nan")

        # Throughput proxy: median fake-exporter cpu_pct from
        # sample.jsonl (high cpu = high produce rate sustained).
        thru = float("nan")
        sample_path = os.path.join(sweep_root, cell, "sample.jsonl")
        if os.path.exists(sample_path):
            samples = []
            with open(sample_path) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec_ = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if rec_.get("container") == "fake-exporter":
                        samples.append(rec_.get("cpu_pct", 0.0))
            if samples:
                thru = float(pd.Series(samples).median())

        rows.append({
            "cell": cell,
            "fam": meta["fam"],
            "n": meta["n"],
            "w_ms": meta["w_ms"],
            "card": meta["card"],
            "median_error": med_err,
            "median_recall_loss": med_recall_loss,
            "median_producer_cpu_pct": thru,
        })

    df = pd.DataFrame(rows)
    df.to_csv(os.path.join(out_dir, "pareto_acc_vs_thru.csv"), index=False)

    # Plot quantile/sum/cardinality cells (median_error) as one
    # axis; topk cells (recall_loss) as a second subplot since
    # the metric semantics differ.
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    palette = {
        "ddsketch": "tab:blue", "kll": "tab:orange",
        "cs": "tab:green", "cms": "tab:red", "hll": "tab:purple",
    }
    for fam, sub in df.groupby("fam"):
        ax1.scatter(
            sub["median_error"], sub["median_producer_cpu_pct"],
            label=fam, color=palette.get(fam, "gray"), s=70, alpha=0.7,
        )
    ax1.set_xscale("log")
    ax1.set_xlabel("median relative error (log)")
    ax1.set_ylabel("producer cpu %  (throughput proxy)")
    ax1.set_title("Pareto: error vs throughput\n(quantile / cardinality / sum)")
    ax1.legend(fontsize=8)
    ax1.grid(True, alpha=0.3)

    for fam, sub in df.groupby("fam"):
        if sub["median_recall_loss"].notna().any():
            ax2.scatter(
                sub["median_recall_loss"], sub["median_producer_cpu_pct"],
                label=fam, color=palette.get(fam, "gray"), s=70, alpha=0.7,
            )
    ax2.set_xlabel("1 - top-K recall  (lower is better)")
    ax2.set_ylabel("producer cpu %")
    ax2.set_title("Pareto: top-K recall loss vs throughput")
    ax2.legend(fontsize=8)
    ax2.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "pareto_acc_vs_thru.png"), dpi=150)
    plt.close(fig)
    print("[fig1] pareto_acc_vs_thru.png")


# --- figure 2: bandwidth vs N --------------------------------------


def fig_bandwidth(sweep_root: str, out_dir: str) -> None:
    rows = []
    for cell in sorted(os.listdir(sweep_root)):
        path = os.path.join(sweep_root, cell, "sample.jsonl")
        if not os.path.exists(path):
            continue
        meta = cell_meta(cell)
        agent_tx: list[float] = []
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if r.get("container", "").startswith("agent"):
                    agent_tx.append(r.get("net_tx_mb", 0.0))
        if not agent_tx:
            continue
        s = pd.Series(agent_tx)
        rows.append({**meta, "cell": cell,
                     "agent_tx_p50_mb": float(s.median()),
                     "agent_tx_p99_mb": float(s.quantile(0.99))})
    if not rows:
        print("[fig2] no agent samples")
        return

    df = pd.DataFrame(rows)
    df.to_csv(os.path.join(out_dir, "bandwidth_vs_n.csv"), index=False)

    fig, ax = plt.subplots(figsize=(8, 5))
    palette = {
        "ddsketch": "tab:blue", "kll": "tab:orange",
        "cs": "tab:green", "cms": "tab:red", "hll": "tab:purple",
    }
    for fam, sub in df.groupby("fam"):
        agg = sub.groupby("n")["agent_tx_p99_mb"].median().sort_index()
        ax.plot(agg.index, agg.values, marker="o",
                label=fam, color=palette.get(fam, "gray"))
    ax.set_xlabel("number of agents (N)")
    ax.set_ylabel("p99 agent tx bytes (MiB, total since start)")
    ax.set_title("Bandwidth vs N")
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "bandwidth_vs_n.png"), dpi=150)
    plt.close(fig)
    print("[fig2] bandwidth_vs_n.png")


# --- figure 3: plan-transition timeline ----------------------------


def fig_transition(sweep_root: str, out_dir: str) -> None:
    rows = []
    for cell in sorted(os.listdir(sweep_root)):
        path = os.path.join(sweep_root, cell, "transition.jsonl")
        if not os.path.exists(path):
            continue
        try:
            tr = json.loads(open(path).read())
        except json.JSONDecodeError:
            continue
        if not tr.get("t_query_in"):
            continue
        meta = cell_meta(cell)
        t0 = parse_iso(tr["t_query_in"])

        def offs(key: str) -> float | None:
            v = tr.get(key)
            if v is None:
                return None
            return (parse_iso(v) - t0).total_seconds()

        rows.append({
            **meta,
            "cell": cell,
            "t_plan_ready_s": offs("t_plan_ready"),
            "t_first_hit_s": offs("t_first_hit"),
            "t_steady_s": offs("t_steady"),
        })
    if not rows:
        print("[fig3] no transition records")
        return

    df = pd.DataFrame(rows).sort_values(["fam", "n", "card"]).reset_index(drop=True)
    df.to_csv(os.path.join(out_dir, "transition_timeline.csv"), index=False)

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


# --- figure 4: query-latency CDF -----------------------------------


def fig_latency_cdf(accuracy_csv: str, out_dir: str) -> None:
    acc = pd.read_csv(accuracy_csv)
    if acc.empty:
        print("[fig4] no accuracy rows; skipping")
        return
    durs = pd.to_numeric(acc["duration_ms"], errors="coerce").dropna()
    if durs.empty:
        print("[fig4] no duration_ms values; skipping")
        return

    # CDF per family (extracted from cell name).
    acc = acc.copy()
    acc["fam"] = acc["cell"].astype(str).str.extract(r"^([a-z]+)_")
    acc["duration_ms_num"] = pd.to_numeric(acc["duration_ms"], errors="coerce")
    acc = acc.dropna(subset=["duration_ms_num"])

    fig, ax = plt.subplots(figsize=(8, 5))
    palette = {
        "ddsketch": "tab:blue", "kll": "tab:orange",
        "cs": "tab:green", "cms": "tab:red", "hll": "tab:purple",
    }
    for fam, sub in acc.groupby("fam"):
        v = sorted(sub["duration_ms_num"].values)
        if not v:
            continue
        n = len(v)
        ys = [(i + 1) / n for i in range(n)]
        ax.plot(v, ys, label=fam, color=palette.get(fam, "gray"))

    ax.set_xscale("log")
    ax.set_xlabel("query duration (ms, log)")
    ax.set_ylabel("CDF")
    ax.set_title("Query-latency CDF, per sketch family")
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "query_latency_cdf.png"), dpi=150)
    plt.close(fig)

    # Also dump the raw points as CSV for downstream inspection.
    acc[["cell", "fam", "kind", "duration_ms_num", "plan_id"]].to_csv(
        os.path.join(out_dir, "query_latency_cdf.csv"), index=False
    )
    print("[fig4] query_latency_cdf.png")


def main() -> int:
    ap = argparse.ArgumentParser(description="E2E sweep plots (P9)")
    ap.add_argument("--sweep-root", required=True)
    ap.add_argument("--accuracy", required=True)
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    fig_pareto(args.accuracy, args.sweep_root, args.out_dir)
    fig_bandwidth(args.sweep_root, args.out_dir)
    fig_transition(args.sweep_root, args.out_dir)
    fig_latency_cdf(args.accuracy, args.out_dir)
    print(f"plots: outputs under {args.out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
