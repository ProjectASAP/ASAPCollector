#!/usr/bin/env python3
"""Plan-transition driver + 1 Hz CPU/bandwidth sampler (P6).

Mid-soak, fires a query that doesn't match the active plan; logs
the transition timeline:

  t_query_in       wall clock when the missing-plan query is sent
  t_plan_ready     wall clock when controller's /metrics first
                   shows a new plan_id (different from the
                   pre-transition value)
  t_first_hit      wall clock of the first replay query that
                   returned data with the new plan_id, indicating
                   the new aggregation has begun producing
                   answerable buckets
  t_steady         wall clock when the rolling p50 query latency
                   over the last 10 s has dropped below
                   `--steady-threshold-ms` (default 50 ms)

In parallel, samples docker stats at 1 Hz for every container
matched by `--sample-prefix` (default 'docker-compose-') and
appends to `--sample-out`.

Output schemas:

  transition.jsonl (single line):
    {
      "t_query_in":  "...",
      "t_plan_ready": "...",
      "t_first_hit":  "...",
      "t_steady":     "...",
      "before_plan":  "...",
      "after_plan":   "...",
      "transition_query": "..."
    }

  sample.jsonl (one line per container per second):
    {"ts": "...", "container": "fake-exporter", "cpu_pct": 12.3,
     "mem_mb": 220.4, "net_rx_bytes": 1234567, "net_tx_bytes": ...}

Usage (during a live e2e run):

  python3 plan_transition.py \\
      --target http://localhost:19091 \\
      --controller http://localhost:18080 \\
      --transition-query 'histogram_quantile(0.999, sum by (le) (http_requests_total_latency_ms))' \\
      --transition-out /tmp/transition.jsonl \\
      --sample-out /tmp/sample.jsonl \\
      --soak-secs 120 \\
      --pre-transition-secs 30
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import shutil
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request

from promql_replay import PlanIdTracker, run_query


def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


# --- 1 Hz docker stats sampler -------------------------------------


class DockerStatsSampler(threading.Thread):
    """One thread, samples `docker stats --no-stream` every second
    for containers matching prefix. Cheap enough not to need
    persistent connections; the stream form requires more parsing
    babysitting and we don't need sub-second resolution."""

    def __init__(self, prefix: str, out_path: str, interval_s: float = 1.0):
        super().__init__(daemon=True)
        self.prefix = prefix
        self.out_path = out_path
        self.interval_s = interval_s
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        with open(self.out_path, "w") as f:
            while not self._stop.is_set():
                self._sample_once(f)
                self._stop.wait(self.interval_s)

    def _sample_once(self, f) -> None:
        cmd = [
            "docker", "stats", "--no-stream",
            "--format",
            "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}|{{.NetIO}}",
        ]
        try:
            out = subprocess.run(
                cmd, capture_output=True, text=True, timeout=5
            ).stdout
        except subprocess.TimeoutExpired:
            return

        ts = now_iso()
        for line in out.splitlines():
            parts = line.split("|")
            if len(parts) != 4:
                continue
            name, cpu, mem, net = parts
            if not name.startswith(self.prefix):
                continue
            rec = {
                "ts": ts,
                "container": name[len(self.prefix):].rstrip("-0123456789"),
                "container_full": name,
                "cpu_pct": _parse_pct(cpu),
                "mem_mb": _parse_mb(mem.split(" / ")[0]),
                "net_rx_mb": _parse_mb(net.split(" / ")[0]),
                "net_tx_mb": _parse_mb(net.split(" / ")[1] if " / " in net else "0B"),
            }
            f.write(json.dumps(rec) + "\n")
        f.flush()


def _parse_pct(s: str) -> float:
    s = s.strip().rstrip("%")
    try:
        return float(s)
    except ValueError:
        return 0.0


def _parse_mb(s: str) -> float:
    """Parse docker stats memory/network values: '220.4MiB',
    '1.5GB', '512KB', etc. Returns megabytes."""
    s = s.strip()
    if not s:
        return 0.0
    units = {
        "B": 1 / (1024 * 1024),
        "kB": 1 / 1024, "KB": 1 / 1024, "KiB": 1 / 1024,
        "MB": 1.0, "MiB": 1.0,
        "GB": 1024.0, "GiB": 1024.0,
        "TB": 1024.0 * 1024.0, "TiB": 1024.0 * 1024.0,
    }
    for u, mul in sorted(units.items(), key=lambda kv: -len(kv[0])):
        if s.endswith(u):
            try:
                return float(s[: -len(u)]) * mul
            except ValueError:
                return 0.0
    try:
        return float(s) / (1024 * 1024)
    except ValueError:
        return 0.0


# --- transition timeline -------------------------------------------


def main() -> int:
    ap = argparse.ArgumentParser(description="Plan-transition driver (P6)")
    ap.add_argument("--target", default="http://localhost:19091")
    ap.add_argument("--controller", default="http://localhost:18080")
    ap.add_argument("--transition-query", required=True,
                    help="PromQL whose answer requires a plan the controller hasn't pushed yet")
    ap.add_argument("--transition-out", required=True)
    ap.add_argument("--sample-out", required=True)
    ap.add_argument("--sample-prefix", default="docker-compose-")
    ap.add_argument("--soak-secs", type=float, default=120.0)
    ap.add_argument("--pre-transition-secs", type=float, default=30.0)
    ap.add_argument("--steady-threshold-ms", type=float, default=50.0)
    ap.add_argument("--probe-qps", type=float, default=2.0,
                    help="how often to re-probe with the transition query while waiting for steady")
    ap.add_argument("--max-wait-secs", type=float, default=120.0,
                    help="bail out if t_steady hasn't fired by this many seconds after t_first_hit")
    args = ap.parse_args()

    if shutil.which("docker") is None:
        sys.exit("docker is not on PATH — cannot sample with `docker stats`")

    tracker = PlanIdTracker(args.controller)
    tracker.start()

    sampler = DockerStatsSampler(args.sample_prefix, args.sample_out)
    sampler.start()

    print(f"plan-transition: pre-transition soak {args.pre_transition_secs}s")
    time.sleep(args.pre_transition_secs)
    before_plan = tracker.latest()
    print(f"plan-transition: before_plan={before_plan!r}")

    t_query_in = now_iso()
    print(f"plan-transition: firing transition query at {t_query_in}")
    _, _ = run_query(args.target, args.transition_query, timeout_s=10.0)

    # Watch for plan change.
    t_plan_ready = None
    deadline = time.monotonic() + args.max_wait_secs
    while time.monotonic() < deadline:
        cur = tracker.latest()
        if cur is not None and cur != before_plan:
            t_plan_ready = now_iso()
            print(f"plan-transition: plan_ready at {t_plan_ready} (plan_id={cur!r})")
            break
        time.sleep(0.05)

    after_plan = tracker.latest()

    # Probe for first-hit (non-empty result with new plan_id).
    t_first_hit = None
    durations: list[float] = []
    if t_plan_ready is not None:
        deadline = time.monotonic() + args.max_wait_secs
        period = 1.0 / args.probe_qps if args.probe_qps > 0 else 0.0
        while time.monotonic() < deadline:
            t_start = time.perf_counter()
            dur, res = run_query(args.target, args.transition_query, timeout_s=10.0)
            durations.append(dur)
            if res.get("status") == "success" and res.get("result"):
                t_first_hit = now_iso()
                print(f"plan-transition: first_hit at {t_first_hit} (dur={dur:.2f}ms)")
                break
            elapsed = time.perf_counter() - t_start
            if period > 0 and elapsed < period:
                time.sleep(period - elapsed)

    # Now wait for steady (rolling p50 over 10 s < threshold).
    t_steady = None
    if t_first_hit is not None:
        deadline = time.monotonic() + args.max_wait_secs
        period = 1.0 / args.probe_qps if args.probe_qps > 0 else 0.0
        recent: list[float] = []
        while time.monotonic() < deadline:
            t_start = time.perf_counter()
            dur, res = run_query(args.target, args.transition_query, timeout_s=10.0)
            durations.append(dur)
            recent.append(dur)
            # Trim to last 10 s of probes.
            if len(recent) > int(10.0 * args.probe_qps):
                recent = recent[-int(10.0 * args.probe_qps):]
            if len(recent) >= 5:
                p50 = sorted(recent)[len(recent) // 2]
                if p50 < args.steady_threshold_ms:
                    t_steady = now_iso()
                    print(f"plan-transition: steady at {t_steady} (rolling p50={p50:.2f}ms)")
                    break
            elapsed = time.perf_counter() - t_start
            if period > 0 and elapsed < period:
                time.sleep(period - elapsed)

    # Continue soaking out the rest of `soak_secs` so the sampler
    # captures the post-steady window.
    soak_remaining = args.soak_secs - args.pre_transition_secs
    if soak_remaining > 0:
        print(f"plan-transition: post-soak {soak_remaining:.0f}s")
        time.sleep(soak_remaining)

    record = {
        "t_query_in": t_query_in,
        "t_plan_ready": t_plan_ready,
        "t_first_hit": t_first_hit,
        "t_steady": t_steady,
        "before_plan": before_plan,
        "after_plan": after_plan,
        "transition_query": args.transition_query,
    }
    with open(args.transition_out, "w") as f:
        f.write(json.dumps(record, indent=2) + "\n")
    print(f"plan-transition: wrote {args.transition_out}")

    sampler.stop()
    sampler.join(timeout=2.0)
    tracker.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
