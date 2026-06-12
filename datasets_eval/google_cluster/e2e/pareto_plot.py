#!/usr/bin/env python3
"""Render the headline accuracy-vs-cost Pareto scatter (Fig 1) from the
sweep-results.json produced by pareto_sweep.py.

x = total cost (× raw),  y = accuracy (1 - p99 rel-err).
Raw anchor at (1.0, 1.0). Pareto frontier (lower-left envelope of the
non-raw sketch points) highlighted; arrows annotate the p↓ and delta
directions.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def pareto_frontier(points):
    """Lower-cost, higher-accuracy envelope. A point is on the frontier if
    no other point has cost <= and accuracy >= (strictly better on one)."""
    front = []
    for p in points:
        dominated = False
        for q in points:
            if q is p:
                continue
            if (q["cost_total"] <= p["cost_total"]
                    and q["accuracy"] >= p["accuracy"]
                    and (q["cost_total"] < p["cost_total"]
                         or q["accuracy"] > p["accuracy"])):
                dominated = True
                break
        if not dominated:
            front.append(p)
    return sorted(front, key=lambda r: r["cost_total"])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", type=Path,
                    default=Path("/tmp/pareto/results/sweep-results.json"))
    ap.add_argument("--out", type=Path,
                    default=Path("/tmp/pareto/results/pareto.png"))
    args = ap.parse_args()

    data = json.loads(args.results.read_text())
    res = [r for r in data["results"] if r.get("accuracy") is not None]

    raw = next(r for r in res if r["name"] == "raw")
    full = [r for r in res if r["agg"] == "dd-full"]
    delta = [r for r in res if r["agg"] == "dd-delta"]

    front = pareto_frontier([r for r in res])

    fig, ax = plt.subplots(figsize=(8.2, 6.0))

    # frontier line
    if len(front) >= 2:
        ax.plot([r["cost_total"] for r in front],
                [r["accuracy"] for r in front],
                "-", color="#bbbbbb", lw=2.2, zorder=1,
                label="Pareto frontier")

    # raw anchor
    ax.scatter([raw["cost_total"]], [raw["accuracy"]], s=190, marker="*",
               color="#d62728", zorder=5, label="raw forwarding (anchor)")
    ax.annotate("raw (1.0, 1.0)", (raw["cost_total"], raw["accuracy"]),
                textcoords="offset points", xytext=(10, 6), fontsize=9)

    def plot_series(pts, color, marker, label, dy):
        pts = sorted(pts, key=lambda r: -r["p"])
        ax.plot([r["cost_total"] for r in pts], [r["accuracy"] for r in pts],
                marker=marker, color=color, ms=10, lw=1.3, zorder=4,
                label=label)
        for r in pts:
            ax.annotate(f"p={r['p']:g}",
                        (r["cost_total"], r["accuracy"]),
                        textcoords="offset points", xytext=(8, dy),
                        fontsize=8, color=color)

    plot_series(full, "#1f77b4", "o", "DDSketch full (ε_cdm=0)", dy=8)
    plot_series(delta, "#2ca02c", "D", "DDSketch delta (ε_cdm>0)", dy=-14)

    # p-down arrow (full series, highest-p -> lowest-p)
    fs = sorted(full, key=lambda r: -r["p"])
    if len(fs) >= 2:
        ax.annotate("", xy=(fs[-1]["cost_total"], fs[-1]["accuracy"]),
                    xytext=(fs[0]["cost_total"], fs[0]["accuracy"]),
                    arrowprops=dict(arrowstyle="->", color="#1f77b4",
                                    lw=1.6, alpha=0.7))
        ax.text(min(r["cost_total"] for r in fs) - 0.005,
                0.5 * (fs[0]["accuracy"] + fs[-1]["accuracy"]),
                "p↓\n(cheaper,\nε_s tail)", color="#1f77b4", fontsize=8.5,
                ha="right", va="center")

    ax.axvline(1.0, color="#d62728", ls=":", lw=1, alpha=0.4)
    ax.set_xlabel("total cost  (× raw;  0.5·[edge CPU + wire egress], normalized)")
    ax.set_ylabel("accuracy  (1 − p99 rel-err vs true pooled p99)")
    ax.set_title("Accuracy-vs-cost Pareto on the real ASAP stack\n"
                 "quantile_over_time(0.99, cpu_rate[30s]) — 2019 Google cluster trace",
                 pad=14)
    ax.set_xlim(0.30, 1.08)
    accs = [r["accuracy"] for r in res]
    ax.set_ylim(min(accs) - 0.008, 1.004)
    ax.grid(True, alpha=0.25)
    ax.legend(loc="center right", fontsize=9)
    note = ("sketch cuts wire ~44× (wire_norm≈0.02) — total cost is CPU-"
            "dominated here;\ndelta sits ~right of full (tumbling windows are "
            "independent → no cross-window redundancy).")
    fig.text(0.12, 0.015, note, fontsize=7.5, color="#555555")
    fig.tight_layout(rect=(0, 0.05, 1, 1))
    fig.savefig(args.out, dpi=130)
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
