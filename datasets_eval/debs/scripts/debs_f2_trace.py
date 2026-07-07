#!/usr/bin/env python3
"""Build an F2-monitoring trace from the real DEBS 2022 trading-day CSV.

Each market event is one (symbol) key. We bin the first N events into STEPS
sub-windows (in event order = time order), assign each symbol to one of EDGES
edges by hash, and emit the INCREMENTAL per-(edge, symbol) event counts per
step. The f2driver replays this: each edge accumulates its symbols into a
Count-Sketch and calls OnWindow per step, so the global F2 = Σ_symbol (cumulative
count)² grows over the day — a real ramp. We also print the exact F2 trajectory
so τ can be set to cross partway (a real "trading got concentrated" alert).

Output line: `step edge symbol count`  (aggregated; symbol is a short int id).
Usage: debs_f2_trace.py <csv> <n_events> <steps> <edges> <out_trace>
"""
import sys, hashlib

csv, n_events, steps, edges, out = (
    sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), sys.argv[5])

# Pass 1: read n_events data rows, record (symbol, step). Bin by event index.
events = []  # (symbol, step)
symbols = {}  # symbol -> compact int id
read = 0
with open(csv, "r", errors="replace") as f:
    for line in f:
        if line.startswith("#") or line.startswith("ID,"):
            continue
        sym = line.split(",", 1)[0]
        if not sym:
            continue
        step = min(read * steps // n_events, steps - 1)
        sid = symbols.setdefault(sym, len(symbols))
        events.append((sid, step))
        read += 1
        if read >= n_events:
            break

H = len(symbols)
edge_of = lambda sid: int(hashlib.md5(str(sid).encode()).hexdigest(), 16) % edges

# Per-(step, edge, symbol) incremental counts.
from collections import defaultdict
inc = defaultdict(int)  # (step, edge, sid) -> count
for sid, step in events:
    inc[(step, edge_of(sid), sid)] += 1

with open(out, "w") as w:
    for (step, edge, sid), c in sorted(inc.items()):
        w.write(f"{step} {edge} {sid} {c}\n")

# Exact global F2 trajectory (cumulative per-symbol counts).
cum = defaultdict(int)
print(f"# DEBS F2 trace: events={read} distinct_symbols(H)={H} steps={steps} edges={edges}", file=sys.stderr)
print(f"# step  exact_global_F2  (F2 = sum_symbol cumulative_count^2)", file=sys.stderr)
step_events = defaultdict(list)
for sid, step in events:
    step_events[step].append(sid)
for step in range(steps):
    for sid in step_events[step]:
        cum[sid] += 1
    f2 = sum(c * c for c in cum.values())
    print(f"{step:4d}  {f2:,}", file=sys.stderr)
