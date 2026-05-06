#!/usr/bin/env python3
"""Compute per-claim headline numbers from the merged sweep dataset.

Reads:
  - /home/zeying/repos/sweep-eval/headline-2026-05-06/accuracy.csv
    (concatenated per-cell accuracy CSVs)
  - 60 measurement.csv files (one per cell) in
    {SWEEP_OLD,SWEEP_NEW}/<cell>/measurement.csv
  - replay.jsonl per cell for client-side latency
  - transition.jsonl per cell for plan-transition timing

Emits a markdown verdict table to stdout.
"""
from __future__ import annotations

import csv
import json
import os
import re
import statistics
import sys
from collections import defaultdict
from glob import glob

OUT_DIR = "/home/zeying/repos/sweep-eval/headline-2026-05-06"
SWEEP_OLD = "/home/zeying/repos/sweep-eval/sketchcol-sweep-20260505-204002"
SWEEP_NEW = "/home/zeying/repos/sweep-eval/sketchcol-sweep-cont-20260505-230219"

CELL_RE = re.compile(r"(?P<fam>[a-z]+)_N(?P<n>\d+)_w(?P<w>\d+)ms_c(?P<card>\d+)")


def cell_meta(cell):
    m = CELL_RE.match(cell)
    if not m:
        return None
    g = m.groupdict()
    return {"fam": g["fam"], "n": int(g["n"]), "w_ms": int(g["w"]), "card": int(g["card"])}


def cell_dir(cell):
    for root in (SWEEP_OLD, SWEEP_NEW):
        cand = os.path.join(root, cell)
        if os.path.isdir(cand) and os.path.isfile(os.path.join(cand, "measurement.csv")):
            return cand
    return None


def fnum(x):
    try:
        v = float(x)
        if v != v:  # NaN
            return None
        return v
    except Exception:
        return None


# --- collect all measurement.csv rows
meas_rows = []
for root in (SWEEP_OLD, SWEEP_NEW):
    for cellname in sorted(os.listdir(root)):
        m = os.path.join(root, cellname, "measurement.csv")
        if not os.path.isfile(m):
            continue
        with open(m) as f:
            r = csv.DictReader(f)
            for row in r:
                meta = cell_meta(cellname)
                if not meta:
                    continue
                row["cell"] = cellname
                row.update(meta)
                meas_rows.append(row)
                break

print(f"# loaded {len(meas_rows)} cells\n", file=sys.stderr)

# --- accuracy CSV (concatenated)
acc_rows = []
for p in sorted(glob(os.path.join(OUT_DIR, "per-cell", "accuracy-*.csv"))):
    with open(p) as f:
        for row in csv.DictReader(f):
            acc_rows.append(row)
print(f"# loaded {len(acc_rows)} accuracy rows\n", file=sys.stderr)

# Per-sketch accuracy summary
fam_err = defaultdict(list)
fam_recall = defaultdict(list)
for row in acc_rows:
    cell = row.get("cell", "")
    meta = cell_meta(cell)
    if not meta:
        continue
    fam = meta["fam"]
    e = fnum(row.get("error"))
    if e is not None:
        fam_err[(fam, row["kind"])].append(e)
    rec = fnum(row.get("recall"))
    if rec is not None:
        fam_recall[(fam, row["kind"])].append(rec)


def med(xs):
    if not xs:
        return float("nan")
    return statistics.median(xs)


# --- claim 1: bandwidth (producer_bytes_out_per_s, agent_out_kib_per_s) by family
fam_prod_bytes = defaultdict(list)
fam_agent_out_kib = defaultdict(list)
for r in meas_rows:
    pb = fnum(r.get("producer_bytes_out_per_s"))
    ao = fnum(r.get("agent_out_kib_per_s"))
    if pb is not None:
        fam_prod_bytes[r["fam"]].append(pb)
    if ao is not None:
        fam_agent_out_kib[r["fam"]].append(ao)

# --- claim 2: producer CPU bound + agent footprint
fam_prod_cpu = defaultdict(list)
fam_agent_rss = defaultdict(list)
for r in meas_rows:
    pc = fnum(r.get("producer_cpu_cores"))
    ar = fnum(r.get("agent_rss_mib"))
    if pc is not None:
        fam_prod_cpu[r["fam"]].append(pc)
    if ar is not None:
        fam_agent_rss[r["fam"]].append(ar)

# --- claim 3: latency (client p50/p99 from replay.jsonl)
fam_durs = defaultdict(list)
fam_durs_excl_count = defaultdict(list)
for r in meas_rows:
    d = cell_dir(r["cell"])
    if not d:
        continue
    rj = os.path.join(d, "replay.jsonl")
    if not os.path.isfile(rj):
        continue
    for ln in open(rj):
        try:
            rec = json.loads(ln)
        except Exception:
            continue
        if rec.get("status") != "success":
            continue
        dur = rec.get("duration_ms")
        if not isinstance(dur, (int, float)):
            continue
        fam_durs[r["fam"]].append(dur)
        q = rec.get("query", "")
        if not q.startswith("count(http_requests_total)"):
            fam_durs_excl_count[r["fam"]].append(dur)


def p50(xs):
    if not xs:
        return float("nan")
    xs = sorted(xs)
    return xs[len(xs) // 2]


def p99(xs):
    if not xs:
        return float("nan")
    xs = sorted(xs)
    return xs[max(0, int(0.99 * (len(xs) - 1)))]


# --- claim 4: transition timing
fam_t_query_only = defaultdict(int)
fam_t_complete = defaultdict(int)
fam_count = defaultdict(int)
for r in meas_rows:
    d = cell_dir(r["cell"])
    if not d:
        continue
    tj = os.path.join(d, "transition.jsonl")
    if not os.path.isfile(tj):
        continue
    try:
        tr = json.loads(open(tj).read())
    except Exception:
        continue
    fam_count[r["fam"]] += 1
    if tr.get("t_steady"):
        fam_t_complete[r["fam"]] += 1
    elif tr.get("t_query_in"):
        fam_t_query_only[r["fam"]] += 1

# --- claim 5: warm-tier sketch error envelopes
# DDSketch eps=0.01, KLL eps=0.16, HLL eps=0.008, CS eps=0.03, CMS eps≈0.0027
ENVELOPES = {
    "ddsketch": ("relative_quantile", 0.01),
    "kll": ("rank_quantile", 0.16),
    "hll": ("relative_cardinality", 0.008),
    "cs": ("additive_frequency", 0.03),
    "cms": ("additive_frequency", 0.0027),
}

# --- print summary
print("\n## CLAIM 1 — Bandwidth (producer wire bytes/s and agent_out_kib/s) by sketch family")
print(f"{'family':10s} {'cells':5s} {'prod_p50_Bps':>14s} {'prod_p99_Bps':>14s} {'agentout_p50_KiB/s':>20s} {'agentout_p99_KiB/s':>20s}")
for fam in ["ddsketch", "kll", "cs", "cms", "hll"]:
    pb = fam_prod_bytes[fam]
    ao = fam_agent_out_kib[fam]
    print(f"{fam:10s} {len(pb):5d} {p50(pb):14.1f} {p99(pb):14.1f} {p50(ao):20.3f} {p99(ao):20.3f}")

print("\n## CLAIM 2 — Producer CPU + agent RSS")
print(f"{'family':10s} {'p50_cpu_cores':>14s} {'p99_cpu_cores':>14s} {'p50_agent_rss':>14s} {'p99_agent_rss':>14s}")
for fam in ["ddsketch", "kll", "cs", "cms", "hll"]:
    pc = fam_prod_cpu[fam]
    ar = fam_agent_rss[fam]
    print(f"{fam:10s} {p50(pc):14.3f} {p99(pc):14.3f} {p50(ar):14.1f} {p99(ar):14.1f}")

print("\n## CLAIM 3 — Query latency (client-side p50/p99 ms, all queries vs excluding count(metric))")
print(f"{'family':10s} {'all_p50':>10s} {'all_p99':>10s} {'excl_count_p50':>16s} {'excl_count_p99':>16s}")
for fam in ["ddsketch", "kll", "cs", "cms", "hll"]:
    a = fam_durs[fam]
    e = fam_durs_excl_count[fam]
    print(f"{fam:10s} {p50(a):10.1f} {p99(a):10.1f} {p50(e):16.1f} {p99(e):16.1f}")

print("\n## CLAIM 4 — Plan transitions captured")
print(f"{'family':10s} {'cells':>6s} {'has_t_query_in_only':>22s} {'has_t_steady':>14s}")
for fam in ["ddsketch", "kll", "cs", "cms", "hll"]:
    print(f"{fam:10s} {fam_count[fam]:>6d} {fam_t_query_only[fam]:>22d} {fam_t_complete[fam]:>14d}")

print("\n## CLAIM 5 — Accuracy by family + kind (median relative error / median recall, # samples)")
print(f"{'family':10s} {'kind':14s} {'med_err':>10s} {'med_recall':>12s} {'n':>6s}")
for fam in ["ddsketch", "kll", "cs", "cms", "hll"]:
    for k in ("quantile", "count_unique", "sum", "topk"):
        e = fam_err.get((fam, k), [])
        r = fam_recall.get((fam, k), [])
        n = max(len(e), len(r))
        print(f"{fam:10s} {k:14s} {med(e):10.4f} {med(r):12.4f} {n:>6d}")

# Accuracy headline: count(metric) error
all_count_err = [fnum(r["error"]) for r in acc_rows if r["kind"] == "count_unique"]
all_count_err = [x for x in all_count_err if x is not None]
print(f"\n# all count_unique queries: median relative error = {med(all_count_err):.4g}, n={len(all_count_err)}")
