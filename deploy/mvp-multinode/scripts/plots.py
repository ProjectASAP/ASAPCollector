#!/usr/bin/env python3
"""plots.py — turn mvp-multinode sweep artifacts into the §6 paper figures.

Reads the CSV/JSONL a run_demo_sweep.sh (and scale_fleet.sh) run drops into a
RUN_DIR and renders PNGs into RUN_DIR/figs (or --out). Each figure is guarded:
if its inputs are missing it is skipped with a printed note rather than crashing,
so a partial run still yields whatever figures it can.

Inputs (as produced by snapshot_resources.sh / run_demo_sweep.sh):
  nic-<arm>.csv               arm,node,window_s,rx_bytes_total,tx_bytes_total,rx_bytes_per_s,tx_bytes_per_s
  container-summary-<arm>.csv arm,host,container,cpu_mean_perc,cpu_max_perc,mem_mean_mib,mem_max_mib,n_samples
  <arm>/replay.jsonl          one JSON object per query with at least {"latency_ms": float, "query"|"name": str}
  scale.csv (scale_fleet.sh)  n_agents,arm,sink_rx_MB_s,per_agent_rx_MB_s,agent_cpu_mean_perc,agent_mem_mean_mib

Usage:  python3 plots.py <run_dir> [--out <dir>]
"""
from __future__ import annotations
import argparse, csv, glob, json, os, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

ARM_LABEL = {"b0": "raw OTLP (none)", "b1": "raw OTLP (gzip)",
             "b2": "raw PRW (snappy)", "b3": "serf wire",
             "asap": "ASAP sketch", "asap-gzip": "ASAP sketch (gzip)"}
ARM_ORDER = ["b0", "b1", "b2", "b3", "asap", "asap-gzip"]
COLOR = {"b0": "#9e9e9e", "b1": "#616161", "b2": "#bdbdbd", "b3": "#757575",
         "asap": "#1565c0", "asap-gzip": "#1e88e5"}


def _arms_present(run_dir, pattern):
    arms = []
    for f in glob.glob(os.path.join(run_dir, pattern)):
        a = os.path.basename(f).split("-", 1)[1].rsplit(".", 1)[0]
        # nic-<arm>.csv / container-summary-<arm>.csv → strip leading "summary-"
        a = a.replace("summary-", "")
        arms.append(a)
    return sorted(set(arms), key=lambda x: ARM_ORDER.index(x) if x in ARM_ORDER else 99)


def fig_bandwidth(run_dir, out, backend_nodes=("node1", "node2")):
    """Fig 2 — total backend ingest wire bandwidth per arm.

    Sums RX bytes/s over the backend sink nodes (cold=node1 VM/gorilla,
    warm=node2 data-plane) so the bar is the true wire the backend receives,
    counting BOTH the warm sketch tier and the cold gorilla backup for ASAP."""
    rows = {}
    for f in glob.glob(os.path.join(run_dir, "nic-*.csv")):
        arm = os.path.basename(f)[len("nic-"):-len(".csv")]
        tot = 0.0
        with open(f) as fh:
            for r in csv.DictReader(fh):
                if r["node"] in backend_nodes:
                    tot += float(r["rx_bytes_per_s"])
        rows[arm] = tot / 1e6  # MB/s
    if not rows:
        print("[skip] Fig2 bandwidth: no nic-*.csv"); return
    arms = [a for a in ARM_ORDER if a in rows] + [a for a in rows if a not in ARM_ORDER]
    vals = [rows[a] for a in arms]
    fig, ax = plt.subplots(figsize=(6, 3.6))
    bars = ax.bar([ARM_LABEL.get(a, a) for a in arms], vals,
                  color=[COLOR.get(a, "#1565c0") for a in arms])
    for b, v in zip(bars, vals):
        ax.text(b.get_x() + b.get_width() / 2, v, f"{v:.1f}", ha="center", va="bottom", fontsize=8)
    # annotate the asap-vs-raw reduction factor (matched gzip pair if available)
    if "b1" in rows and "asap-gzip" in rows and rows["asap-gzip"] > 0:
        fac = rows["b1"] / rows["asap-gzip"]
        ax.set_title(f"Ingest wire bandwidth — ASAP {fac:.0f}× below raw (matched gzip)")
    elif "b0" in rows and "asap" in rows and rows["asap"] > 0:
        fac = rows["b0"] / rows["asap"]
        ax.set_title(f"Ingest wire bandwidth — ASAP {fac:.0f}× below raw (matched none)")
    else:
        ax.set_title("Ingest wire bandwidth at the sink node")
    ax.set_ylabel("sink RX (MB/s)"); ax.grid(axis="y", alpha=0.3)
    plt.xticks(rotation=20, ha="right"); plt.tight_layout()
    p = os.path.join(out, "fig2_bandwidth.png"); plt.savefig(p, dpi=140); plt.close()
    print(f"[ok] {p}  ({', '.join(f'{a}={rows[a]:.1f}' for a in arms)} MB/s)")


def fig_resource(run_dir, out):
    """Fig 6 — agent edge CPU and memory per arm."""
    agg = {}  # arm -> (cpu_list, mem_list) for agent containers
    for f in glob.glob(os.path.join(run_dir, "container-summary-*.csv")):
        with open(f) as fh:
            for r in csv.DictReader(fh):
                if "agent" not in r["container"]:
                    continue
                a = r["arm"]
                agg.setdefault(a, ([], []))
                agg[a][0].append(float(r["cpu_mean_perc"]))
                agg[a][1].append(float(r["mem_mean_mib"]))
    if not agg:
        print("[skip] Fig6 resource: no container-summary-*.csv"); return
    arms = [a for a in ARM_ORDER if a in agg] + [a for a in agg if a not in ARM_ORDER]
    cpu = [np.mean(agg[a][0]) for a in arms]
    mem = [np.mean(agg[a][1]) for a in arms]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(8, 3.4))
    labels = [ARM_LABEL.get(a, a) for a in arms]
    cols = [COLOR.get(a, "#1565c0") for a in arms]
    a1.bar(labels, cpu, color=cols); a1.set_ylabel("agent CPU (%)"); a1.set_title("Edge CPU")
    a2.bar(labels, mem, color=cols); a2.set_ylabel("agent RSS (MiB)"); a2.set_title("Edge memory")
    for ax in (a1, a2):
        ax.grid(axis="y", alpha=0.3)
        for t in ax.get_xticklabels():
            t.set_rotation(20); t.set_ha("right")
    plt.tight_layout()
    p = os.path.join(out, "fig6_edge_resource.png"); plt.savefig(p, dpi=140); plt.close()
    print(f"[ok] {p}  (cpu%={dict(zip(arms,[round(c,1) for c in cpu]))})")


def _read_latencies(path):
    out = []
    try:
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                d = json.loads(line)
                for k in ("duration_ms", "latency_ms", "latency", "ms", "elapsed_ms"):
                    if k in d:
                        out.append(float(d[k])); break
    except FileNotFoundError:
        pass
    return out


def fig_latency_cdf(run_dir, out):
    """Fig 7 — query latency CDF per arm (warm vs raw)."""
    series = {}
    for arm in ARM_ORDER:
        lat = _read_latencies(os.path.join(run_dir, arm, "replay.jsonl"))
        if lat:
            series[arm] = sorted(lat)
    if not series:
        print("[skip] Fig7 latency: no */replay.jsonl"); return
    fig, ax = plt.subplots(figsize=(6, 3.6))
    for arm, lat in series.items():
        y = np.arange(1, len(lat) + 1) / len(lat)
        p50 = lat[int(.5 * len(lat))]; p99 = lat[min(len(lat) - 1, int(.99 * len(lat)))]
        ax.plot(lat, y, label=f"{ARM_LABEL.get(arm, arm)} (p50={p50:.1f} p99={p99:.1f}ms)",
                color=COLOR.get(arm, "#1565c0"), lw=1.8)
    ax.set_xlabel("query latency (ms)"); ax.set_ylabel("CDF"); ax.set_xscale("log")
    ax.set_title("PromQL query latency CDF"); ax.grid(alpha=0.3); ax.legend(fontsize=7)
    plt.tight_layout()
    p = os.path.join(out, "fig7_latency_cdf.png"); plt.savefig(p, dpi=140); plt.close()
    print(f"[ok] {p}  (arms={list(series)})")


def fig_scaling(run_dir, out):
    """Fig 10 — per-agent bandwidth & CPU vs fleet size N (from scale.csv)."""
    f = os.path.join(run_dir, "scale.csv")
    if not os.path.isfile(f):
        print("[skip] Fig10 scaling: no scale.csv"); return
    by_arm = {}
    with open(f) as fh:
        for r in csv.DictReader(fh):
            by_arm.setdefault(r["arm"], []).append(r)
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(8.4, 3.4))
    for arm, rows in by_arm.items():
        rows = sorted(rows, key=lambda r: int(r["n_agents"]))
        n = [int(r["n_agents"]) for r in rows]
        pa = [float(r["per_agent_rx_MB_s"]) for r in rows]
        cpu = [float(r["agent_cpu_mean_perc"]) for r in rows]
        c = COLOR.get(arm, "#1565c0")
        a1.plot(n, pa, "o-", color=c, label=ARM_LABEL.get(arm, arm))
        a2.plot(n, cpu, "o-", color=c, label=ARM_LABEL.get(arm, arm))
        if "sim" in arm:
            a1.lines[-1].set_linestyle("--"); a2.lines[-1].set_linestyle("--")
    a1.set_xlabel("fleet size N (agents)"); a1.set_ylabel("per-agent wire (MB/s)")
    a1.set_title("Per-agent bandwidth stays flat"); a1.grid(alpha=0.3); a1.legend(fontsize=7)
    a2.set_xlabel("fleet size N (agents)"); a2.set_ylabel("agent CPU (%)")
    a2.set_title("Per-agent CPU stays flat"); a2.grid(alpha=0.3); a2.legend(fontsize=7)
    plt.tight_layout()
    p = os.path.join(out, "fig10_scaling.png"); plt.savefig(p, dpi=140); plt.close()
    print(f"[ok] {p}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("run_dir")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    out = args.out or os.path.join(args.run_dir, "figs")
    os.makedirs(out, exist_ok=True)
    print(f"[plots] run_dir={args.run_dir} out={out}")
    fig_bandwidth(args.run_dir, out)
    fig_resource(args.run_dir, out)
    fig_latency_cdf(args.run_dir, out)
    fig_scaling(args.run_dir, out)


if __name__ == "__main__":
    main()
