#!/usr/bin/env python3
"""plot_fig8_fig6b.py — Fig 8 (cross-layer placement CPU) and Fig 6b (soak RSS).

  python3 plot_fig8_fig6b.py --fig8 <fig8.csv> --soak <soak_rss.csv> --out <dir>
"""
from __future__ import annotations
import argparse, csv, os
from collections import defaultdict
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


def fig8(path, out):
    if not os.path.isfile(path): print("[skip] fig8: no csv"); return
    data = defaultdict(dict)  # placement -> layer -> cpu
    for r in csv.DictReader(open(path)):
        data[r["placement"]][r["layer"]] = float(r["cpu_perc"])
    layers = ["producer", "agent", "backend"]
    placements = list(data)
    x = np.arange(len(layers)); w = 0.8/max(1, len(placements))
    fig, ax = plt.subplots(figsize=(6.2, 3.8))
    for i, pl in enumerate(placements):
        vals = [data[pl].get(l, 0) for l in layers]
        ax.bar(x + i*w, vals, w, label=f"{pl} placement")
        for xi, v in zip(x + i*w, vals):
            ax.text(xi, v, f"{v:.0f}", ha="center", va="bottom", fontsize=8)
    ax.set_xticks(x + w*(len(placements)-1)/2); ax.set_xticklabels(layers)
    ax.set_ylabel("CPU (%)"); ax.set_title("Fig 8 — same DDSketch agg, CPU by layer × placement")
    ax.legend(fontsize=8); ax.grid(axis="y", alpha=.3)
    p = os.path.join(out, "fig8_placement.png"); plt.tight_layout(); plt.savefig(p, dpi=140); plt.close()
    print(f"[ok] {p}")


def fig6b(path, out):
    if not os.path.isfile(path): print("[skip] fig6b: no csv"); return
    series = defaultdict(list)
    for r in csv.DictReader(open(path)):
        try: series[r["container"]].append((float(r["elapsed_s"]), float(r["rss_mib"])))
        except: pass
    fig, ax = plt.subplots(figsize=(6.4, 3.8))
    for c, pts in sorted(series.items()):
        pts.sort(); xs=[p[0]/60 for p in pts]; ys=[p[1] for p in pts]
        if len(xs) < 3: continue
        # slope MiB/hr
        n=len(xs); mx=sum(xs)/n; my=sum(ys)/n
        den=sum((x-mx)**2 for x in xs) or 1
        slope=sum((x-mx)*(y-my) for x,y in zip(xs,ys))/den*60  # MiB/hr
        ax.plot(xs, ys, "o-", ms=3, label=f"{c} ({slope:+.0f} MiB/hr)")
    ax.set_xlabel("elapsed (min)"); ax.set_ylabel("agent RSS (MiB)")
    ax.set_title("Fig 6b — agent RSS over soak (leak check)")
    ax.legend(fontsize=8); ax.grid(alpha=.3)
    p = os.path.join(out, "fig6b_soak_rss.png"); plt.tight_layout(); plt.savefig(p, dpi=140); plt.close()
    print(f"[ok] {p}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--fig8"); ap.add_argument("--soak"); ap.add_argument("--out", default=".")
    a = ap.parse_args(); os.makedirs(a.out, exist_ok=True)
    if a.fig8: fig8(a.fig8, a.out)
    if a.soak: fig6b(a.soak, a.out)
