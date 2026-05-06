#!/usr/bin/env python3
"""measure_per_edge_bandwidth.py — MVP v6 per-edge bandwidth probe.

Extends `measure_stages.py`'s `docker stats` net-rx/tx capture with
per-edge labelling for the v6 multi-stage topology:

    sdk → agent       (×10 producers → 2 agents)
    agent → gateway   (2 agents → 1 gateway)
    gateway → backend (1 gateway → 1 backend)
    gateway → s3      (Gorilla archive PUTs / MinIO traffic)

Why this lives in its own script (vs. a flag on measure_stages.py):

    measure_stages.py emits ONE row per (baseline, stage, container)
    summarising the whole window. The v6 report wants per-edge
    bytes/sec time-series so the headline can show "per-edge
    bandwidth", not just per-stage. Two outputs, two scripts.

Per-edge bytes/sec is computed as the sum of relevant containers'
NetIO TX (or MinIO's RX for the gateway-→s3 edge) deltas across each
1Hz sample, divided by the wall-clock interval.

Hard fact: docker stats reports CUMULATIVE rx + tx since container
start, so a delta over the window measures wire-bytes during the
window (modulo TCP retransmits etc., which we accept as noise).

Output CSV columns:

    edge,sample_ts_ms,window_s,bytes_total,bytes_per_s

`edge` is one of:
    edge_sdk_to_agent
    edge_agent_to_gateway
    edge_gateway_to_backend
    edge_gateway_to_s3

One row per (edge, sample) — i.e. ~60 rows per edge for a 60s run
at 1Hz. Reduce to per-edge averages downstream (mvp_report_v6.py
handles this).

Stdlib only. Calls `docker stats --no-stream` once per sample — same
mechanism measure_stages.py uses, so two probes can run in parallel
without a docker rate-limit issue (the daemon is happy to serve
multiple `docker stats --no-stream` calls per second).

Usage:

    python3 measure_per_edge_bandwidth.py \\
        --duration 60 \\
        --period 1.0 \\
        --out per_edge_bandwidth.csv
"""
from __future__ import annotations

import argparse
import csv
import re
import subprocess
import sys
import time
from typing import Iterable


# Edge container-set definitions for the v6 topology. The values
# are role tags consumed by `_role_for_container` below.
#
# Trade-off: keeping the edge → container mapping in this file
# rather than parsing a YAML keeps the script stdlib-only. The
# mapping mirrors mvp-v6-multi-stage.yml; if that overlay's
# service names change, this script must follow.

# Container role classifier. We match on the bare service name
# stripped of compose's `<project>-<service>-<n>` suffix. For
# producer-{a,b}-N we collapse to "producer".
def _bare_service(name: str) -> str:
    bare = name
    bare = re.sub(r"-\d+$", "", bare)            # strip replica index
    bare = re.sub(r"^docker-compose-", "", bare)  # strip project prefix
    bare = re.sub(r"^docker_compose_", "", bare)
    bare = re.sub(r"^deploy-docker-compose-", "", bare)
    return bare


def _role_for_container(name: str) -> str:
    """One of {producer, agent, gateway, backend, minio, other}."""
    bare = _bare_service(name)
    if bare.startswith("producer-"):
        return "producer"
    if bare.startswith("agent-"):
        return "agent"
    if bare == "gateway":
        return "gateway"
    if bare == "backend":
        return "backend"
    if bare == "minio":
        return "minio"
    if bare.startswith("fake-exporter") or bare.startswith("fake_exporter"):
        # base.yml's fake-exporter; under v6 overlay it's a stub
        # alpine that does nothing, so its bytes-on-wire is ~0. Tag
        # it as producer so a misconfiguration (overlay not active)
        # still attributes to the right edge — better than silently
        # under-counting.
        return "producer"
    return "other"


# ── docker stats helpers (subset of measure_stages.py) ────────────


def _parse_size_to_bytes(s: str) -> float:
    s = s.strip()
    if not s:
        return float("nan")
    # docker stats can report e.g. "1.23GB", "456MB", "12.3kB", "789B".
    for suffix, factor in (
        ("GB", 1e9), ("MB", 1e6), ("kB", 1e3), ("B", 1.0),
    ):
        if s.endswith(suffix):
            try:
                return float(s[: -len(suffix)]) * factor
            except ValueError:
                return float("nan")
    try:
        return float(s)
    except ValueError:
        return float("nan")


def docker_stats_snapshot() -> dict[str, tuple[float, float]]:
    """Returns {container_name: (cumulative_rx_bytes, cumulative_tx_bytes)}."""
    try:
        out = subprocess.check_output(
            [
                "docker", "stats", "--no-stream",
                "--format", "{{.Name}}|{{.NetIO}}",
            ],
            text=True,
            timeout=10,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        print(f"# docker stats failed: {e}", file=sys.stderr)
        return {}
    out_map: dict[str, tuple[float, float]] = {}
    for line in out.strip().splitlines():
        try:
            name, netio = line.split("|", 1)
        except ValueError:
            continue
        if "/" not in netio:
            continue
        rx_str, tx_str = (p.strip() for p in netio.split("/", 1))
        rx = _parse_size_to_bytes(rx_str)
        tx = _parse_size_to_bytes(tx_str)
        if rx == rx and tx == tx:  # NaN guard
            out_map[name] = (rx, tx)
    return out_map


# ── per-edge classifier ───────────────────────────────────────────

# Per-edge attribution rules. Each edge is a `(metric, role)`
# tuple — we use that container's TX (or RX) bytes as the edge's
# wire bytes during the window.
#
# Why we attribute to one side of the wire only:
#   - Producers have one outbound stream → agent. Sum their TX.
#   - Agents fan out to gateway. Sum their TX (this overcounts
#     by including the agent's own /metrics scrape from
#     Prometheus, but that's negligible vs. OTLP traffic in the
#     v6 topology).
#   - Gateway fans out to backend AND s3. We can't distinguish
#     the two from a single-NIC TX counter — so:
#       edge_gateway_to_backend = backend.RX
#       edge_gateway_to_s3      = minio.RX
#     This gets us per-destination accounting at the cost of
#     trusting the receiver's RX counter.
#
# The fan-in vs. fan-out asymmetry is intentional: we want the
# tightest counter per edge. For the producer→agent edge we trust
# producer.TX (a single source per row). For the gateway split we
# trust the destination's RX so the two sub-edges are
# distinguishable.

EDGES: list[tuple[str, str]] = [
    # (edge_label, classifier_token)
    ("edge_sdk_to_agent",        "PRODUCER_TX"),
    ("edge_agent_to_gateway",    "AGENT_TX"),
    ("edge_gateway_to_backend",  "BACKEND_RX"),
    ("edge_gateway_to_s3",       "MINIO_RX"),
]


def _bytes_for_edge(
    snap: dict[str, tuple[float, float]], edge_token: str
) -> float:
    """Sum the relevant cumulative counter across all containers
    matching this edge's classifier token."""
    total = 0.0
    found = False
    for name, (rx, tx) in snap.items():
        role = _role_for_container(name)
        if edge_token == "PRODUCER_TX" and role == "producer":
            total += tx
            found = True
        elif edge_token == "AGENT_TX" and role == "agent":
            total += tx
            found = True
        elif edge_token == "BACKEND_RX" and role == "backend":
            total += rx
            found = True
        elif edge_token == "MINIO_RX" and role == "minio":
            total += rx
            found = True
    return total if found else float("nan")


# ── sampling loop ─────────────────────────────────────────────────


def sample_edges(
    duration_s: float, period_s: float = 1.0
) -> list[tuple[str, int, float, float, float]]:
    """Sample per-edge cumulative bytes at `period_s` Hz for
    `duration_s`. Returns a list of (edge, sample_ts_ms, window_s,
    bytes_total, bytes_per_s) rows.

    The first sample is the BASELINE — its bytes_total/bytes_per_s
    are NaN (we have no prior sample to diff against). Subsequent
    samples diff against the immediately previous snapshot. The
    final row per edge is also a "tail" diff that covers
    [t_n-1 → t_n], same as every row in between.
    """
    n_samples = max(2, int(duration_s / period_s) + 1)
    rows: list[tuple[str, int, float, float, float]] = []
    prev_snap: dict[str, tuple[float, float]] | None = None
    prev_t: float | None = None

    for i in range(n_samples):
        t = time.monotonic()
        snap = docker_stats_snapshot()
        wall_ms = int(time.time() * 1000)
        if prev_snap is None:
            # Baseline row per edge.
            for label, _ in EDGES:
                rows.append((label, wall_ms, 0.0, float("nan"), float("nan")))
        else:
            window = max(t - (prev_t or t), 1e-6)
            for label, token in EDGES:
                cur = _bytes_for_edge(snap, token)
                prev = _bytes_for_edge(prev_snap, token)
                if cur == cur and prev == prev:  # both non-NaN
                    bytes_delta = max(0.0, cur - prev)
                    bytes_per_s = bytes_delta / window
                else:
                    bytes_delta = float("nan")
                    bytes_per_s = float("nan")
                rows.append((label, wall_ms, window, bytes_delta, bytes_per_s))
        prev_snap = snap
        prev_t = t
        if i < n_samples - 1:
            time.sleep(max(0.0, period_s - (time.monotonic() - t)))
    return rows


# ── main ──────────────────────────────────────────────────────────


def main(argv: Iterable[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--duration",
        type=float,
        default=60.0,
        help="Total sampling window in seconds (default 60).",
    )
    p.add_argument(
        "--period",
        type=float,
        default=1.0,
        help="Sample period in seconds (default 1.0).",
    )
    p.add_argument(
        "--out",
        required=True,
        help="Output CSV path. Columns: edge,sample_ts_ms,window_s,"
             "bytes_total,bytes_per_s.",
    )
    args = p.parse_args(list(argv) if argv is not None else None)

    print(
        f"# per-edge bandwidth probe — duration={args.duration}s "
        f"period={args.period}s out={args.out}",
        file=sys.stderr,
    )

    rows = sample_edges(args.duration, args.period)
    if not rows:
        print("# no docker stats samples — aborting", file=sys.stderr)
        return 1

    with open(args.out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(("edge", "sample_ts_ms", "window_s", "bytes_total", "bytes_per_s"))
        for r in rows:
            label, ts, window, bt, bps = r
            w.writerow(
                (
                    label,
                    ts,
                    f"{window:.3f}",
                    f"{bt:.1f}" if bt == bt else "",
                    f"{bps:.3f}" if bps == bps else "",
                )
            )
    print(f"# wrote {args.out} ({len(rows)} rows)")

    # Per-edge summary: count of valid samples + mean bytes/s.
    by_edge: dict[str, list[float]] = {}
    for label, _, _, _, bps in rows:
        if bps == bps:  # not NaN
            by_edge.setdefault(label, []).append(bps)
    for label in sorted(by_edge):
        vals = by_edge[label]
        mean = sum(vals) / len(vals) if vals else float("nan")
        print(
            f"# edge={label} samples={len(vals)} mean_bytes_per_s={mean:.1f}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
