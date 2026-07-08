#!/usr/bin/env python3
"""analyze.py — reduce soak samples to Fig-6 stats + RSS-over-time PNG.

Inputs:
  --b0 samples_b0.jsonl       (measurement (a) raw-forward arm /proc samples)
  --b3 samples_b3_soak.jsonl  (measurement (a)+(b) sketch arm /proc samples)
  --memdiag dp_memdiag_b3.log (data_plane [MEMORY_DIAG] SketchStore lines)

Outputs:
  - prints the arm CPU/RSS table + slope verdicts (stdout)
  - writes rss_over_time.png
  - writes summary.json
"""
from __future__ import annotations

import argparse
import json
import re
import sys

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

MEMDIAG_RE = re.compile(
    r"SketchStore: (\d+) instance.* (\d+) sid\(s\) with state, "
    r"payload=([\d.]+) KB.*process RSS=([\d.]+) MB")


def load_samples(path):
    edge_t, edge_rss, edge_cpu = [], [], []
    dp_t, dp_rss, dp_cpu = [], [], []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            t = r["t_rel"]
            e = r.get("edge", {})
            d = r.get("dp", {})
            if e.get("alive") and e.get("rss_mb") is not None:
                edge_t.append(t); edge_rss.append(e["rss_mb"])
                if e.get("cpu_pct") is not None:
                    edge_cpu.append(e["cpu_pct"])
            if d.get("alive") and d.get("rss_mb") is not None:
                dp_t.append(t); dp_rss.append(d["rss_mb"])
                if d.get("cpu_pct") is not None:
                    dp_cpu.append(d["cpu_pct"])
    return {"edge_t": edge_t, "edge_rss": edge_rss, "edge_cpu": edge_cpu,
            "dp_t": dp_t, "dp_rss": dp_rss, "dp_cpu": dp_cpu}


def load_memdiag(path):
    t, sids, payload_kb, rss_mb = [], [], [], []
    with open(path) as f:
        for line in f:
            m = MEMDIAG_RE.search(line)
            if not m:
                continue
            sids.append(int(m.group(2)))
            payload_kb.append(float(m.group(3)))
            rss_mb.append(float(m.group(4)))
    # synthesize a 30s-cadence time axis (MEMORY_DIAG logs every 30s)
    t = [30.0 * i for i in range(len(sids))]
    return {"t": t, "sids": sids, "payload_kb": payload_kb, "rss_mb": rss_mb}


def stats(cpu, rss, t):
    if not cpu:
        cpu = [0.0]
    return {
        "n": len(rss),
        "mean_cpu_pct": round(float(np.mean(cpu)), 2),
        "p99_cpu_pct": round(float(np.percentile(cpu, 99)), 2),
        "steady_rss_mb": round(float(np.median(rss[len(rss)//2:])), 1) if rss else None,
        "rss_min_mb": round(min(rss), 1) if rss else None,
        "rss_max_mb": round(max(rss), 1) if rss else None,
    }


def slope_mb_per_h(t, rss, tail_frac=0.5):
    """Linear fit of RSS(MB) vs t(s) over the tail (post warm-up); MB/hour."""
    if len(t) < 4:
        return None
    i0 = int(len(t) * (1 - tail_frac))
    tt = np.array(t[i0:]); rr = np.array(rss[i0:])
    if tt.max() - tt.min() < 1:
        return None
    a, b = np.polyfit(tt, rr, 1)  # a = MB/s
    return round(a * 3600.0, 2)


def verdict(slope, span_h):
    if slope is None:
        return "indeterminate (too few samples)"
    proj_24h = slope * 24
    if abs(slope) < 5:
        return f"BOUNDED (slope {slope:+.2f} MB/h ~= 0; 24h extrap {proj_24h:+.1f} MB)"
    if slope > 0:
        return f"CLIMBING (slope {slope:+.2f} MB/h; 24h extrap {proj_24h:+.1f} MB)"
    return f"DECLINING (slope {slope:+.2f} MB/h)"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--b0")
    ap.add_argument("--b3", required=True)
    ap.add_argument("--memdiag")
    ap.add_argument("--png", default="rss_over_time.png")
    ap.add_argument("--out", default="summary.json")
    args = ap.parse_args()

    b3 = load_samples(args.b3)
    summary = {"arms": {}}

    # measurement (a): first 180s window of b3 = the steady CPU/RSS comparator
    def window(d, key_t, key_rss, key_cpu, tmax):
        ts = d[key_t]
        idx = [i for i, t in enumerate(ts) if t <= tmax]
        return ([d[key_cpu][i] for i in idx if i < len(d[key_cpu])],
                [d[key_rss][i] for i in idx], [ts[i] for i in idx])

    if args.b0:
        b0 = load_samples(args.b0)
        c, r, t = window(b0, "edge_t", "edge_rss", "edge_cpu", 1e9)
        summary["arms"]["b0_raw_forward_edge"] = stats(c, r, t)
        c, r, t = window(b0, "dp_t", "dp_rss", "dp_cpu", 1e9)
        summary["arms"]["b0_data_plane"] = stats(c, r, t)

    # b3 measurement (a): use first 180s for the comparable steady CPU/RSS
    c, r, t = window(b3, "edge_t", "edge_rss", "edge_cpu", 180)
    summary["arms"]["b3_sketch_edge_first180s"] = stats(c, r, t)
    c, r, t = window(b3, "dp_t", "dp_rss", "dp_cpu", 180)
    summary["arms"]["b3_data_plane_first180s"] = stats(c, r, t)

    # full-soak stats
    summary["arms"]["b3_sketch_edge_fullsoak"] = stats(
        b3["edge_cpu"], b3["edge_rss"], b3["edge_t"])
    summary["arms"]["b3_data_plane_fullsoak"] = stats(
        b3["dp_cpu"], b3["dp_rss"], b3["dp_t"])

    # leak slopes (tail half, after warm-up)
    span_h = (b3["edge_t"][-1] - b3["edge_t"][0]) / 3600.0 if b3["edge_t"] else 0
    edge_slope = slope_mb_per_h(b3["edge_t"], b3["edge_rss"])
    dp_slope = slope_mb_per_h(b3["dp_t"], b3["dp_rss"])
    summary["soak"] = {
        "duration_s": round(b3["edge_t"][-1], 1) if b3["edge_t"] else 0,
        "duration_h": round(span_h, 3),
        "edge_rss_slope_mb_per_h": edge_slope,
        "edge_verdict": verdict(edge_slope, span_h),
        "dp_rss_slope_mb_per_h": dp_slope,
        "dp_verdict": verdict(dp_slope, span_h),
    }

    md = None
    if args.memdiag:
        md = load_memdiag(args.memdiag)
        if md["sids"]:
            md_slope = slope_mb_per_h(md["t"], md["rss_mb"])
            summary["data_plane_memdiag"] = {
                "n": len(md["sids"]),
                "sids_min": min(md["sids"]), "sids_max": max(md["sids"]),
                "sids_final": md["sids"][-1],
                "payload_kb_final": md["payload_kb"][-1],
                "rss_mb_min": min(md["rss_mb"]), "rss_mb_max": max(md["rss_mb"]),
                "rss_mb_final": md["rss_mb"][-1],
                "rss_slope_mb_per_h": md_slope,
                "rss_verdict": verdict(md_slope, span_h),
            }

    # ---- plot ----
    fig, axes = plt.subplots(2, 1, figsize=(10, 9), sharex=False)
    ax = axes[0]
    ax.plot(b3["edge_t"], b3["edge_rss"], label="b3 sketch edge (asap-otel) RSS",
            color="C0", lw=1.4)
    ax.plot(b3["dp_t"], b3["dp_rss"], label="b3 data_plane RSS (/proc)",
            color="C1", lw=1.4)
    if args.b0:
        ax.plot(b0["edge_t"], b0["edge_rss"],
                label="b0 raw-forward edge RSS", color="C2", lw=1.0, ls="--")
    ax.set_ylabel("process RSS (MB)")
    ax.set_xlabel("soak time (s)")
    ax.set_title("Fig 6 — edge / data_plane RSS over time "
                 f"(b3 soak {span_h:.2f} h @ 5000 pts/s, single-node loopback)")
    ax.grid(alpha=0.3); ax.legend(loc="best", fontsize=8)
    txt = (f"edge slope {edge_slope:+.2f} MB/h\n"
           f"dp(/proc) slope {dp_slope:+.2f} MB/h")
    ax.text(0.02, 0.97, txt, transform=ax.transAxes, va="top", fontsize=8,
            bbox=dict(boxstyle="round", fc="white", alpha=0.7))

    ax2 = axes[1]
    if md and md["sids"]:
        ax2.plot(md["t"], md["rss_mb"], color="C1", lw=1.4,
                 label="data_plane RSS (MEMORY_DIAG)")
        ax2b = ax2.twinx()
        ax2b.plot(md["t"], md["sids"], color="C3", lw=1.2, ls=":",
                  label="SketchStore sids")
        ax2b.set_ylabel("SketchStore sids", color="C3")
        ax2.set_ylabel("data_plane RSS (MB)", color="C1")
        ax2.set_xlabel("soak time (s)")
        ax2.set_title("data_plane SketchStore sid count + RSS "
                      "(stale-sid retention check)")
        ax2.grid(alpha=0.3)
        ax2.legend(loc="upper left", fontsize=8)
        ax2b.legend(loc="lower right", fontsize=8)
    fig.tight_layout()
    fig.savefig(args.png, dpi=110)

    with open(args.out, "w") as f:
        json.dump(summary, f, indent=2)
    print(json.dumps(summary, indent=2))
    print(f"\nwrote {args.png} and {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
