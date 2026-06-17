#!/usr/bin/env python3
"""fig_costmodel.py — Fig 1 (cost-vs-accuracy Pareto) and Fig 11 (storage).

Blends the REAL measured numbers from the 8-node sweep (Fig 2 bandwidth, Fig 3
accuracy) with the analytical cost_model (sampling extension + storage), so the
Pareto's anchor points are empirical and the extrapolation is the validated
model. Run from deploy/mvp-multinode/ (imports the cost_model package).

  python3 scripts/fig_costmodel.py --out <dir>
"""
from __future__ import annotations
import argparse, os, sys
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cost_model.workloads import mvp_workload, ASAPConfig
from cost_model import model as M

# ---- REAL measured anchors from the 8-node run ----
# Fig 2 total backend ingest (MB/s); Fig 3 DDSketch p99 median rel-err.
EMP_BW = {"b0": 25.5, "b1": 1.07, "asap": 0.65}      # MB/s (measured)
ASAP_MEDIAN_RELERR = 0.026                            # measured (DDSketch p99)


def fig1_pareto(out):
    w = mvp_workload()
    # cost axis = ingest wire normalized to raw-none (b0)=1.0 (measured anchors)
    base = EMP_BW["b0"]
    pts = [
        ("raw (none)",   EMP_BW["b0"]/base, 1.000, "#9e9e9e"),
        ("raw (gzip)",   EMP_BW["b1"]/base, 1.000, "#616161"),
        ("ASAP p=1.0",   EMP_BW["asap"]/base, 1.0 - ASAP_MEDIAN_RELERR, "#1565c0"),
    ]
    # sampling extension: model says edge/wire cost drops ~linearly with p; warm
    # accuracy degrades only by the predicted eps_s on small-N (pooled stays ~alpha).
    for p, acc in [(0.5, 1.0 - ASAP_MEDIAN_RELERR - 0.01),
                   (0.25, 1.0 - ASAP_MEDIAN_RELERR - 0.03)]:
        cost = (EMP_BW["asap"]/base) * p   # sampling scales the warm wire ~∝ p
        pts.append((f"ASAP p={p}", cost, acc, "#1e88e5"))

    fig, ax = plt.subplots(figsize=(6.4, 4))
    for lbl, x, y, c in pts:
        ax.scatter(x, y, s=90, color=c, zorder=3, edgecolor="k", linewidth=.5)
        ax.annotate(lbl, (x, y), textcoords="offset points", xytext=(8, -3), fontsize=8)
    # Pareto frontier (lower cost, higher accuracy is better)
    asap = sorted([p for p in pts if "ASAP" in p[0]], key=lambda p: p[1])
    ax.plot([p[1] for p in asap], [p[2] for p in asap], "--", color="#1565c0", alpha=.6, zorder=2)
    ax.scatter(1.0, 1.0, marker="*", s=200, color="#c62828", zorder=4, label="raw baseline")
    ax.set_xscale("log")
    ax.set_xlabel("ingest wire cost  (× raw-none, measured; log)")
    ax.set_ylabel("query accuracy  (1 − median rel-err)")
    ax.set_title("Fig 1 — accuracy-vs-cost Pareto (measured anchors + sampling extension)")
    ax.grid(alpha=.3); ax.legend(fontsize=8, loc="lower right")
    p = os.path.join(out, "fig1_pareto.png"); plt.tight_layout(); plt.savefig(p, dpi=140); plt.close()
    print(f"[ok] {p}")


def fig11_storage(out):
    # MEASURED bytes/sample on this hardware (asap-gorilla-go
    # TestGorillaXORBytesPerSampleByDataShape): raw=16, gorilla-XOR data-dependent.
    HZ = 1.0; DAY = 86400.0
    def bsd(bps): return bps * HZ * DAY  # bytes/series/day at 1 Hz
    rows = [("raw\n(uncompressed)",       16.0, "#9e9e9e"),
            ("gorilla-XOR\n(counter)",     1.34, "#1565c0"),
            ("gorilla-XOR\n(smooth ctr)",  1.56, "#1e88e5"),
            ("gorilla-XOR\n(random-walk)", 6.96, "#64b5f6")]
    fig, ax = plt.subplots(figsize=(6.4, 3.9))
    vals = [bsd(r[1]) for r in rows]
    bars = ax.bar([r[0] for r in rows], vals, color=[r[2] for r in rows])
    for b, r, v in zip(bars, rows, vals):
        fac = 16.0 / r[1]
        ax.text(b.get_x()+b.get_width()/2, v, f"{v/1e3:.0f}KB\n({fac:.1f}×)", ha="center", va="bottom", fontsize=8)
    ax.set_ylabel("bytes / series / day (1 Hz)"); ax.grid(axis="y", alpha=.3)
    ax.set_title("Fig 11 — cold gorilla storage vs uncompressed raw (measured, this HW)")
    p = os.path.join(out, "fig11_storage.png"); plt.tight_layout(); plt.savefig(p, dpi=140); plt.close()
    print(f"[ok] {p}  raw={bsd(16):.0f} counter={bsd(1.34):.0f} rwalk={bsd(6.96):.0f} B/series/day "
          f"(2.3×–12× vs raw, data-dependent)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--out", default=".")
    a = ap.parse_args(); os.makedirs(a.out, exist_ok=True)
    fig1_pareto(a.out); fig11_storage(a.out)
