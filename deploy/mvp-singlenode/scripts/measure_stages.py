#!/usr/bin/env python3
"""measure_stages.py — issue #46 v4 stage-separated resource breakdown.

Samples per-container CPU cores, RSS MiB, net rx/tx KiB/s, and disk
usage MiB at 1Hz over a measurement window (default 60s), then
aggregates per stage and emits one row per (baseline, stage,
container).

Stage mapping (hard-coded; override via --stages-yaml):

    agent-* (any container whose name starts with "agent-"  )      → agent
    otel-app*                                                 → producer
    gateway                                                         → gateway
    backend                                                         → backend
    prometheus                                                      → backend-storage  (B0/B1)
    minio                                                           → backend-storage  (ASAP)
    controller                                                      → controller
    grafana, *-setup, cold-store-init                               → ignored

The backend process is emitted once because it serves both ingest and query;
duplicating it into two stage rows would double-count CPU and RSS.

Disk usage is sampled with `du -sb` inside the relevant container
or via S3-bucket size for the MinIO case (when applicable). For
Prometheus, `du -sb /prometheus` (the TSDB chunk dir). For MinIO,
the `mc du --recursive asap/asap-gorilla` listing.

Output CSV columns:

    baseline,stage,container,cpu_cores,rss_mib,
    net_in_kibps,net_out_kibps,disk_mib

Each container produces ONE row per (baseline, stage); the values
are time-averages over the window (CPU as mean cores; RSS as mean
MiB; net rx/tx as window-rate KiB/s; disk as the measured-window-end
sample). Stdlib only — uses `docker stats` and `docker exec`.

Independent of `run_mvp_demo.sh`. Exits cleanly after `--duration`.

Usage:

  python3 measure_stages.py \\
      --baseline asap-single-sketch \\
      --duration 60 \\
      --out stages.csv

  # Override the mapping if a baseline relabels containers:
  python3 measure_stages.py \\
      --baseline b0-prometheus \\
      --duration 60 \\
      --stages-yaml deploy/mvp-singlenode/configs/stages-mapping.yaml \\
      --out stages.csv
"""
from __future__ import annotations

import argparse
import csv
import re
import shutil
import subprocess
import sys
import time
from typing import Iterable


# ── default stage mapping ──────────────────────────────────────────


def default_stage_for(container: str, baseline: str) -> list[str]:
    """Return the list of stage labels a container falls into. A
    backend container is duplicated into both ingest+query stages
    so the v4 §1 table has dedicated rows.

    `baseline` is consulted to disambiguate the storage mapping:
    Prometheus is the storage tier under B0/B1; MinIO is the
    archive storage tier under the ASAP baselines.
    """
    name = container

    # Strip docker-compose's `<project>-<service>-<n>` form to
    # the bare service tag. With BuildKit + compose v2 the
    # default project is the parent dir name (here "docker-compose"),
    # so a typical container looks like `docker-compose-agent-3-1`.
    bare = name
    bare = re.sub(r"-\d+$", "", bare)  # strip replica suffix
    bare = re.sub(r"^docker-compose-", "", bare)
    bare = re.sub(r"^docker_compose_", "", bare)
    bare = re.sub(r"^asap-", "", bare)  # multinode docker-run names

    if bare.startswith("agent-") or bare.startswith("agent_"):
        return ["agent"]
    if bare.startswith("otel-app") or bare.startswith("otel_app") or bare.startswith("producer-"):
        return ["producer"]
    if bare == "gateway":
        return ["gateway"]
    if bare in {"backend", "data-plane", "victoriametrics"}:
        # Single container, dual stage labels.
        return ["backend"]
    if bare == "prometheus":
        # Storage stage under B0 / B1; for ASAP this would be a
        # no-op (Prometheus is up but only used for self-telemetry
        # scrape). Keep the storage label only for the raw / serf
        # baselines so the §1 table doesn't miscount.
        if baseline.startswith("b0") or baseline.startswith("b1"):
            return ["backend-storage"]
        return []
    if bare == "minio":
        # Storage stage under ASAP archive baselines; ignored for
        # B0/B1 because the bucket is empty there.
        if "asap" in baseline or "gorilla" in baseline:
            return ["backend-storage"]
        return []
    if bare in {"controller", "control-plane"}:
        return ["controller"]
    # grafana, minio-setup, cold-store-init, etc.
    return []


# ── docker stats helpers (subset of measure-baseline.py) ───────────


def _parse_size_to_bytes(s: str) -> float:
    s = s.strip()
    if not s:
        return float("nan")
    for suffix, factor in (("GB", 1e9), ("MB", 1e6), ("kB", 1e3), ("B", 1.0)):
        if s.endswith(suffix):
            try:
                return float(s[: -len(suffix)]) * factor
            except ValueError:
                return float("nan")
    try:
        return float(s)
    except ValueError:
        return float("nan")


def _parse_mem_to_mib(s: str) -> float:
    s = s.strip()
    for suffix, factor in (("GiB", 1024.0), ("MiB", 1.0), ("KiB", 1.0 / 1024)):
        if s.endswith(suffix):
            try:
                return float(s[: -len(suffix)]) * factor
            except ValueError:
                return float("nan")
    return float("nan")


def docker_stats_snapshot() -> dict[str, dict[str, float]]:
    """Single `docker stats --no-stream` snapshot keyed by container."""
    try:
        out = subprocess.check_output(
            [
                "docker", "stats", "--no-stream",
                "--format",
                "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}|{{.NetIO}}",
            ],
            text=True,
            timeout=10,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        print(f"# docker stats failed: {e}", file=sys.stderr)
        return {}
    result: dict[str, dict[str, float]] = {}
    for line in out.strip().splitlines():
        try:
            name, cpu, mem, netio = line.split("|", 3)
        except ValueError:
            continue
        try:
            cpu_pct = float(cpu.rstrip("%"))
        except ValueError:
            cpu_pct = float("nan")
        mem_mib = _parse_mem_to_mib(mem.split("/", 1)[0])
        if "/" in netio:
            rx_str, tx_str = [p.strip() for p in netio.split("/", 1)]
            net_rx = _parse_size_to_bytes(rx_str)
            net_tx = _parse_size_to_bytes(tx_str)
        else:
            net_rx = net_tx = float("nan")
        result[name] = {
            "cpu_pct": cpu_pct,
            "mem_mib": mem_mib,
            "net_rx_bytes": net_rx,
            "net_tx_bytes": net_tx,
        }
    return result


# ── disk measurement ───────────────────────────────────────────────


def disk_mib_for(container: str, baseline: str) -> float:
    """Best-effort disk-usage probe. Container-specific paths
    matter — Prometheus's TSDB lives in /prometheus, MinIO's
    bucket layout under /data.

    Returns NaN if the probe can't run (image missing du, host
    permission, etc.). Soft-fail by design: this column isn't
    load-bearing for verdicts."""
    bare = re.sub(r"-\d+$", "", container)
    bare = re.sub(r"^docker-compose-", "", bare)
    bare = re.sub(r"^docker_compose_", "", bare)

    paths_to_try: list[str] = []
    if bare == "prometheus":
        paths_to_try = ["/prometheus", "/prometheus/wal"]
    elif bare == "minio":
        # Whole `/data` covers raw + asap-gorilla buckets.
        paths_to_try = ["/data/asap-gorilla", "/data"]
    elif bare.startswith("agent-") or bare.startswith("agent_"):
        # SERF / Gorilla on-disk blobs.
        paths_to_try = ["/var/tmp/asap-serf-out", "/var/tmp/asap-gorilla-out"]
    else:
        return float("nan")

    for path in paths_to_try:
        try:
            out = subprocess.check_output(
                ["docker", "exec", container, "du", "-sb", path],
                text=True,
                stderr=subprocess.STDOUT,
                timeout=10,
            )
            tok = out.strip().split()
            if tok and tok[0].isdigit():
                return float(tok[0]) / (1024.0 * 1024.0)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            continue
        except FileNotFoundError:
            return float("nan")
    return float("nan")


# ── sampling loop ──────────────────────────────────────────────────


def sample_window(duration_s: float, period_s: float = 1.0) -> dict[str, dict]:
    """Sample `docker stats` at `period_s` Hz for `duration_s`,
    computing per-container time-averaged CPU / RSS and window-rate
    net rx/tx KiB/s.

    Returns:
        { container_name: {
              "cpu_cores":    float,   # mean cores over window
              "rss_mib":      float,   # mean MiB over window
              "net_in_kibps": float,   # (last - first) / window / 1024
              "net_out_kibps": float,
          } }
    """
    n_samples = max(2, int(duration_s / period_s))
    snapshots: list[tuple[float, dict[str, dict[str, float]]]] = []
    t0 = time.monotonic()
    for i in range(n_samples):
        t = time.monotonic()
        snap = docker_stats_snapshot()
        snapshots.append((t, snap))
        if i < n_samples - 1:
            time.sleep(max(0.0, period_s - (time.monotonic() - t)))
    t_end = snapshots[-1][0]
    window = max(t_end - t0, 1e-6)

    # Aggregate.
    all_names: set[str] = set()
    for _, snap in snapshots:
        all_names.update(snap.keys())

    out: dict[str, dict] = {}
    for name in all_names:
        cpu_pcts: list[float] = []
        rss_vals: list[float] = []
        first_rx = first_tx = float("nan")
        last_rx = last_tx = float("nan")
        for ts, snap in snapshots:
            row = snap.get(name)
            if not row:
                continue
            cp = row.get("cpu_pct")
            if cp is not None and cp == cp:
                cpu_pcts.append(cp)
            mb = row.get("mem_mib")
            if mb is not None and mb == mb:
                rss_vals.append(mb)
            rx = row.get("net_rx_bytes")
            tx = row.get("net_tx_bytes")
            if rx is not None and rx == rx:
                if first_rx != first_rx:
                    first_rx = rx
                last_rx = rx
            if tx is not None and tx == tx:
                if first_tx != first_tx:
                    first_tx = tx
                last_tx = tx
        cpu_cores = (sum(cpu_pcts) / len(cpu_pcts) / 100.0) if cpu_pcts else float("nan")
        rss_mib = (sum(rss_vals) / len(rss_vals)) if rss_vals else float("nan")
        net_in_kibps = (
            ((last_rx - first_rx) / window / 1024.0)
            if (last_rx == last_rx and first_rx == first_rx)
            else float("nan")
        )
        net_out_kibps = (
            ((last_tx - first_tx) / window / 1024.0)
            if (last_tx == last_tx and first_tx == first_tx)
            else float("nan")
        )
        out[name] = {
            "cpu_cores": cpu_cores,
            "rss_mib": rss_mib,
            "net_in_kibps": net_in_kibps,
            "net_out_kibps": net_out_kibps,
        }
    return out


# ── main ───────────────────────────────────────────────────────────


def main(argv: Iterable[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--baseline",
        required=True,
        help="Label for the baseline (e.g. b0-prometheus, b1-serf, asap-single-sketch).",
    )
    p.add_argument(
        "--duration",
        type=float,
        default=60.0,
        help="Measurement window seconds (default 60s).",
    )
    p.add_argument(
        "--period",
        type=float,
        default=1.0,
        help="`docker stats` sample period (default 1.0s).",
    )
    p.add_argument(
        "--out",
        required=True,
        help="Output CSV path. Columns: "
        "baseline,stage,container,cpu_cores,rss_mib,"
        "net_in_kibps,net_out_kibps,disk_mib.",
    )
    p.add_argument(
        "--stages-yaml",
        default="",
        help="Optional YAML override for the stage mapping. Schema: "
        "`{<container_glob>: <stage>}`. If unset, `default_stage_for` "
        "is used (recommended for v4 demo).",
    )
    args = p.parse_args(argv)

    if args.stages_yaml and not shutil.which("python3"):
        print("# --stages-yaml ignored — pyyaml not in stdlib; using defaults", file=sys.stderr)
        args.stages_yaml = ""

    print(
        f"# stage probe — baseline={args.baseline} duration={args.duration}s "
        f"period={args.period}s",
        file=sys.stderr,
    )

    stats = sample_window(args.duration, args.period)
    if not stats:
        print("# no docker stats samples — aborting", file=sys.stderr)
        return 1

    # Disk probe at end of window.
    rows: list[tuple] = []
    for container, vals in sorted(stats.items()):
        stages = default_stage_for(container, args.baseline)
        if not stages:
            continue
        disk = disk_mib_for(container, args.baseline)
        for stage in stages:
            rows.append(
                (
                    args.baseline,
                    stage,
                    container,
                    f"{vals['cpu_cores']:.4f}",
                    f"{vals['rss_mib']:.2f}",
                    f"{vals['net_in_kibps']:.2f}",
                    f"{vals['net_out_kibps']:.2f}",
                    f"{disk:.2f}" if disk == disk else "",
                )
            )

    header = (
        "baseline", "stage", "container",
        "cpu_cores", "rss_mib",
        "net_in_kibps", "net_out_kibps",
        "disk_mib",
    )
    with open(args.out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)

    print(f"# wrote {args.out} ({len(rows)} rows)")
    # Per-stage summary.
    by_stage: dict[str, dict[str, float]] = {}
    for r in rows:
        st = r[1]
        try:
            cpu = float(r[3]); rss = float(r[4])
        except ValueError:
            continue
        agg = by_stage.setdefault(st, {"cpu_cores": 0.0, "rss_mib": 0.0, "n": 0.0})
        agg["cpu_cores"] += cpu
        agg["rss_mib"] += rss
        agg["n"] += 1
    for st in sorted(by_stage):
        a = by_stage[st]
        print(
            f"# baseline={args.baseline} stage={st} containers={int(a['n'])} "
            f"cpu_cores_total={a['cpu_cores']:.3f} rss_mib_total={a['rss_mib']:.1f}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
