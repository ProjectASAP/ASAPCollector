#!/usr/bin/env python3
"""sampler.py — sample edge + data_plane process RSS and CPU% from /proc.

Used by the §6 edge soak (Fig 6) measurement on the real ASAP stack. Both
the asap-otel (edge) and data_plane containers run with `--network host`,
so their PIDs are visible on the host and /proc/<pid>/{stat,status} give
exact RSS + CPU jiffies without any container overhead.

CPU% is computed from the delta of (utime+stime) jiffies between two
consecutive samples divided by the wall-clock delta, normalised to a
single core (so 100% == one fully-busy core; can exceed 100% multi-core).

Output: one JSON object per sample to the --out JSONL file, plus a human
line to stderr. Run for --duration seconds at --interval seconds.

Usage:
  sampler.py --pids edge=1521738 dp=1521599 \
             --interval 3 --duration 1800 --out samples.jsonl --tag b3
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

CLK_TCK = os.sysconf("SC_CLK_TCK")
PAGE = os.sysconf("SC_PAGE_SIZE")


def read_stat(pid: int):
    """Return (utime+stime jiffies, rss_bytes) or None if pid is gone."""
    try:
        with open(f"/proc/{pid}/stat", "rb") as f:
            data = f.read()
        # comm may contain spaces/parens; split on last ')'
        rparen = data.rindex(b")")
        fields = data[rparen + 2 :].split()
        # after comm, field index 0 == state; utime=13,stime=14 (1-based 14,15)
        utime = int(fields[11])
        stime = int(fields[12])
        with open(f"/proc/{pid}/status", "r") as f:
            rss_kb = 0
            for line in f:
                if line.startswith("VmRSS:"):
                    rss_kb = int(line.split()[1])
                    break
        return utime + stime, rss_kb * 1024
    except (FileNotFoundError, ProcessLookupError, ValueError):
        return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pids", nargs="+", required=True,
                    help="name=pid pairs, e.g. edge=123 dp=456")
    ap.add_argument("--interval", type=float, default=3.0)
    ap.add_argument("--duration", type=float, default=1800.0)
    ap.add_argument("--out", required=True)
    ap.add_argument("--tag", default="")
    args = ap.parse_args()

    pids = {}
    for kv in args.pids:
        name, pid = kv.split("=")
        pids[name] = int(pid)

    prev = {name: read_stat(pid) for name, pid in pids.items()}
    prev_t = time.monotonic()
    start = prev_t

    out = open(args.out, "a", buffering=1)
    n = 0
    while True:
        now = time.monotonic()
        if now - start >= args.duration:
            break
        time.sleep(args.interval)
        t = time.monotonic()
        dt = t - prev_t
        rec = {"t_wall": time.time(), "t_rel": round(t - start, 2),
               "tag": args.tag}
        for name, pid in pids.items():
            cur = read_stat(pid)
            if cur is None:
                rec[name] = {"rss_mb": None, "cpu_pct": None, "alive": False}
                continue
            cpu_pct = None
            if prev[name] is not None and dt > 0:
                djiff = cur[0] - prev[name][0]
                cpu_pct = 100.0 * (djiff / CLK_TCK) / dt
            rec[name] = {"rss_mb": round(cur[1] / 1e6, 2),
                         "cpu_pct": round(cpu_pct, 2) if cpu_pct is not None else None,
                         "alive": True}
            prev[name] = cur
        prev_t = t
        out.write(json.dumps(rec) + "\n")
        n += 1
        parts = " ".join(
            f"{k}:rss={v['rss_mb']}MB cpu={v['cpu_pct']}%"
            for k, v in rec.items() if isinstance(v, dict))
        print(f"[{rec['t_rel']:7.1f}s] {parts}", file=sys.stderr)
    out.close()
    print(f"sampler: wrote {n} samples to {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
