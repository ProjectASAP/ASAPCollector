#!/usr/bin/env python3
# Aggregate epsilon_cluster_sweep.sh per-arm output into one integrated table.
#   cols: p | implied ε | freshness warm/archive p50 | warm latency p50/p99 |
#         edge CPU | data-plane CPU/mem | NIC tx kbps | (accuracy layered later)
import csv, glob, os, sys, math
BASE = sys.argv[1] if len(sys.argv) > 1 else "/mydata/eval/phase2/sweep"
# rate per window for implied-ε: light workload ~ card*freq*producers*nodes admitted
# we read admitted rate from data-plane if available; else use nominal.
NOMINAL_RATE = 300 * 20 * 2 * 2   # card*freq*prod*nodes ~ updates/s (upper bound)

def fresh(arm_dir):
    f = os.path.join(arm_dir, "freshness", "freshness-asap.csv")
    out = {}
    if not os.path.exists(f): return out
    by = {}
    for r in csv.DictReader(open(f)):
        try: ov, pt = float(r["observed_value_ms"]), float(r["poll_ts_ms"])
        except: continue
        if ov > 1e12:                      # valid emit epoch-ms
            by.setdefault(r["tier"], []).append(pt - ov)
    for t, v in by.items():
        v = sorted(x for x in v if x >= 0)
        if v: out[t] = (v[len(v)//2], v[min(len(v)-1, int(.99*len(v)))], len(v))
    return out

def latency(arm_dir):
    f = os.path.join(arm_dir, "latency.csv")
    if not os.path.exists(f): return (None, None)
    rows = list(csv.DictReader(open(f)))
    if not rows: return (None, None)
    return (float(rows[0]["p50_ms"]), float(rows[0]["p99_ms"]))

def resources(arm_dir):
    f = glob.glob(os.path.join(arm_dir, "resources", "container-summary-*.csv"))
    edge_cpu = dp_cpu = dp_mem = cold_cpu = 0.0
    if f:
        for r in csv.DictReader(open(f[0])):
            c = r["container"]; cpu = float(r["cpu_mean_perc"]); mem = float(r["mem_mean_mib"])
            if "agent" in c or "producer" in c: edge_cpu += cpu
            elif "data-plane" in c: dp_cpu, dp_mem = cpu, mem
            elif any(k in c for k in ("gorilla","minio","thanos","prometheus")): cold_cpu += cpu
    return edge_cpu, dp_cpu, dp_mem, cold_cpu

def nic(arm_dir):
    f = glob.glob(os.path.join(arm_dir, "resources", "nic-*.csv"))
    tx = 0.0
    if f:
        for r in csv.DictReader(open(f[0])):
            try: tx += float(r["tx_bytes_per_s"])
            except: pass
    return tx * 8 / 1000.0  # kbps aggregate

print(f"{'p':>5} {'ε~':>7} | {'fresh_warm':>10} {'fresh_arch':>10} | {'lat50':>6} {'lat99':>6} | "
      f"{'edgeCPU%':>8} {'dpCPU%':>6} {'dpMEM':>6} {'coldCPU%':>8} | {'NIC_kbps':>9}")
for arm_dir in sorted(glob.glob(os.path.join(BASE, "p*")),
                      key=lambda d: -float(os.path.basename(d)[1:])):
    p = float(os.path.basename(arm_dir)[1:])
    eps = math.sqrt((1/p - 1)/NOMINAL_RATE) if p < 1 else 0.0
    fr = fresh(arm_dir); l50, l99 = latency(arm_dir)
    ecpu, dcpu, dmem, ccpu = resources(arm_dir); ntx = nic(arm_dir)
    fw = f"{fr['warm'][0]:.0f}ms" if 'warm' in fr else "—"
    fa = f"{fr['archive'][0]:.0f}ms" if 'archive' in fr else "—"
    print(f"{p:>5} {eps:>7.4f} | {fw:>10} {fa:>10} | "
          f"{(f'{l50:.1f}' if l50 else '—'):>6} {(f'{l99:.1f}' if l99 else '—'):>6} | "
          f"{ecpu:>8.0f} {dcpu:>6.1f} {dmem:>5.0f}M {ccpu:>8.1f} | {ntx:>9.1f}")
