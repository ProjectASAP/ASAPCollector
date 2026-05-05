#!/usr/bin/env python3
"""
measure-baseline.py — pull per-baseline CPU, memory, bandwidth
figures from the running compose stack.

Queries Prometheus for agent / gateway / backend metrics, plus
supplements with `docker stats` for containers that don't
self-report (backend + the producer / fake-exporter). Producer-side
columns were added 2026-04-23 to support the three-axis SDK
aggregation sweep — see
docs/sdk-cost-evaluation.md and the encoding-axis
ablation in particular, which measures the cost the SDK pays to
emit raw / full-sketch / delta-sketch per tick.

Output is CSV to stdout. The intended flow is:

    ./run-baseline-sweep.sh > sweep-YYYYMMDD.csv

which iterates baseline × (rate, cardinality) and concatenates
measure-baseline.py output from each configuration.

This is a pure stdlib Python script — no extra deps.

Usage:
    python3 measure-baseline.py [--prom URL] [--window 60s]
        [--baseline b0-raw|b1-serf|b2-full|b3-delta|b4-tunable|b5-gorilla]
        [--bytes-sample-window 5s]

The `--baseline` tag is just pass-through labelling; the script
doesn't know or care which baseline is active.
"""
import argparse
import csv
import json
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from typing import Any


def prom_query(url: str, q: str) -> list[dict[str, Any]]:
    """Instant query; returns the `result` list."""
    params = urllib.parse.urlencode({"query": q})
    with urllib.request.urlopen(f"{url}/api/v1/query?{params}", timeout=5) as resp:
        body = json.load(resp)
    if body.get("status") != "success":
        raise RuntimeError(f"Prometheus error: {body}")
    return body["data"]["result"]


def scalar(url: str, q: str) -> float:
    """Single-valued query → float, or NaN if no result."""
    r = prom_query(url, q)
    if not r:
        return float("nan")
    return float(r[0]["value"][1])


def _parse_size_to_bytes(s: str) -> float:
    """'1.23kB' / '4.56MB' / '1.0GB' → bytes. NaN on parse error."""
    s = s.strip()
    if not s:
        return float("nan")
    # Units docker uses for NetIO: B, kB, MB, GB (decimal SI).
    for suffix, factor in (("GB", 1e9), ("MB", 1e6), ("kB", 1e3), ("B", 1.0)):
        if s.endswith(suffix):
            try:
                return float(s[: -len(suffix)]) * factor
            except ValueError:
                return float("nan")
    # Fallback: raw number
    try:
        return float(s)
    except ValueError:
        return float("nan")


def _parse_mem_to_mib(s: str) -> float:
    """'335.8MiB' / '1.0GiB' / '512KiB' → MiB. NaN on parse error."""
    s = s.strip()
    for suffix, factor in (("GiB", 1024.0), ("MiB", 1.0), ("KiB", 1.0 / 1024)):
        if s.endswith(suffix):
            try:
                return float(s[: -len(suffix)]) * factor
            except ValueError:
                return float("nan")
    return float("nan")


def docker_stats() -> dict[str, dict[str, float]]:
    """
    `docker stats --no-stream` keyed by container name.

    Returns a dict per container:
        {
            "cpu_pct":     float,
            "mem_mib":     float,
            "net_rx_bytes": float,   # cumulative since container start
            "net_tx_bytes": float,   # cumulative since container start
        }

    NetIO is cumulative — to get a rate, call this twice and divide
    the delta by the wall-clock gap (see docker_bytes_rate).
    """
    out = subprocess.check_output(
        [
            "docker",
            "stats",
            "--no-stream",
            "--format",
            "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}|{{.NetIO}}",
        ],
        text=True,
    )
    result: dict[str, dict[str, float]] = {}
    for line in out.strip().splitlines():
        name, cpu, mem, netio = line.split("|", 3)
        cpu_pct = float(cpu.rstrip("%"))
        mem_mib = _parse_mem_to_mib(mem.split("/", 1)[0])
        # NetIO looks like "1.23kB / 4.56MB" = rx / tx
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


def docker_stats_with_bytes_rate(
    window_s: float,
) -> dict[str, dict[str, float]]:
    """
    Sample `docker stats` twice separated by `window_s` seconds.
    Returns per-container:
        {
            "cpu_pct":         float,  # from the second sample
            "mem_mib":         float,  # from the second sample
            "net_tx_per_s":    float,  # delta / window
            "net_rx_per_s":    float,
        }

    Adds `window_s` of wall-clock overhead to the caller; worth it
    because cAdvisor isn't in the compose stack (see
    deploy/configs/prometheus.yml — only OTel targets configured).
    """
    t0 = time.monotonic()
    s0 = docker_stats()
    time.sleep(window_s)
    t1 = time.monotonic()
    s1 = docker_stats()
    dt = max(t1 - t0, 1e-6)

    result: dict[str, dict[str, float]] = {}
    for name, cur in s1.items():
        prev = s0.get(name, {})
        result[name] = {
            "cpu_pct": cur["cpu_pct"],
            "mem_mib": cur["mem_mib"],
            "net_tx_per_s": (
                (cur["net_tx_bytes"] - prev.get("net_tx_bytes", cur["net_tx_bytes"]))
                / dt
            ),
            "net_rx_per_s": (
                (cur["net_rx_bytes"] - prev.get("net_rx_bytes", cur["net_rx_bytes"]))
                / dt
            ),
        }
    return result


# Prometheus query templates. `w` is the rate window (e.g. "1m").
#
# Both agent and gateway are on otelcol v0.141 today (the gateway was
# bumped along with the agent during the Phase 2 shim PRs); both use
# the `_total` suffix on counters and the `_bytes` suffix on the
# process-RSS gauge. The original measure-baseline.py was written
# against a v0.108 gateway and v0.141 agent — that asymmetry no
# longer holds, so the gateway queries now mirror the agent shape.
# Closes Phase 2.11B gap #1.
#
# Each gateway query is wrapped in `or` against the v0.108 (no-suffix)
# variant so this script keeps producing rows when run against a
# legacy gateway image (e.g. someone replaying an old worktree).
# When neither variant exists the result is NaN, same as before.
QUERIES: dict[str, str] = {
    # ── Agent tier (source) ─────────────────────────────────────
    "agent_cpu_cores": (
        "avg by (agent_id) ("
        "  rate(otelcol_process_cpu_seconds_total{{job=\"agents\"}}[{w}])"
        ")"
    ),
    "agent_rss_mib": (
        "avg by (agent_id) ("
        "  otelcol_process_memory_rss_bytes{{job=\"agents\"}} / 1024 / 1024"
        ")"
    ),
    # Bytes entering the agent's sketch pipeline. Only populated
    # for the ASAP-patched processors (DDSketch/HLL/KLL/etc.);
    # B0/B1/B5 leave this NaN and report bandwidth via the
    # agent_points_per_s receiver count instead.
    "agent_in_kib_per_s": (
        "avg by (agent_id) ("
        "  sum by (agent_id) ("
        "    rate(otelcol_datacollector_processor_input_bytes_total"
        "         {{job=\"agents\"}}[{w}])"
        "  )"
        ") / 1024"
    ),
    "agent_out_kib_per_s": (
        "avg by (agent_id) ("
        "  sum by (agent_id) ("
        "    rate(otelcol_datacollector_processor_output_bytes_total"
        "         {{job=\"agents\"}}[{w}])"
        "  )"
        ") / 1024"
    ),
    # Receiver-level point rate is universal across baselines.
    # Useful as an input-side bandwidth proxy for B0/B1/B5.
    "agent_points_per_s": (
        "avg by (agent_id) ("
        "  rate(otelcol_receiver_accepted_metric_points_total"
        "       {{job=\"agents\"}}[{w}])"
        ")"
    ),
    # ── Gateway tier (destination-1) ────────────────────────────
    # v0.141 names with `_total` suffix; legacy v0.108 names appended
    # via PromQL `or` so old worktrees keep producing data.
    "gateway_cpu_cores": (
        "rate(otelcol_process_cpu_seconds_total{{job=\"gateway\"}}[{w}])"
        " or rate(otelcol_process_cpu_seconds{{job=\"gateway\"}}[{w}])"
    ),
    "gateway_rss_mib": (
        "otelcol_process_memory_rss_bytes{{job=\"gateway\"}} / 1024 / 1024"
        " or otelcol_process_memory_rss{{job=\"gateway\"}} / 1024 / 1024"
    ),
    "gateway_points_per_s": (
        "rate(otelcol_receiver_accepted_metric_points_total"
        "     {{job=\"gateway\"}}[{w}])"
        " or rate(otelcol_receiver_accepted_metric_points"
        "        {{job=\"gateway\"}}[{w}])"
    ),
    "gateway_out_series_per_s": (
        "rate(otelcol_exporter_sent_metric_points_total"
        "     {{job=\"gateway\"}}[{w}])"
        " or rate(otelcol_exporter_sent_metric_points"
        "        {{job=\"gateway\"}}[{w}])"
    ),
    # ── Backend tier (destination-2) ────────────────────────────
    # NOTE: these only populate under an ingest+query soak. An ingest-
    # only soak (like the Phase 2.11B audit) leaves them at NaN. This
    # is a design-level gap, not a query bug — see
    # docs/phase-2-perf-deployment.md "Gaps" section #2.
    "backend_samples_per_s": "rate(asap_ingest_samples_total[{w}])",
    "backend_query_p99_ms": (
        "1000 * histogram_quantile(0.99, "
        "sum by (le) (rate(asap_query_duration_seconds_bucket[{w}])))"
    ),
}


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--prom", default="http://localhost:9090")
    p.add_argument(
        "--window",
        default="2m",
        help="rate() lookback window. ≥2m needed when baselines emit on "
        "60s windows — otherwise rate() sees a single sample and returns NaN.",
    )
    p.add_argument("--baseline", default="unset", help="label for the output row")
    p.add_argument(
        "--scale", default="N1", help="label for the scale (e.g. N1 / N10)"
    )
    p.add_argument("--rate", default="", help="workload rate label")
    p.add_argument("--cardinality", default="", help="workload cardinality label")
    p.add_argument(
        "--bytes-sample-window",
        type=float,
        default=5.0,
        help="seconds between the two docker-stats samples used to compute "
        "producer_bytes_out_per_s. Too short and the tx counter barely moves; "
        "too long and the sweep gets expensive per baseline.",
    )
    p.add_argument(
        "--producer-container",
        default="docker-compose-fake-exporter-1",
        help="docker container name of the producer to scrape for SDK-side "
        "CPU / RSS / bytes-out.",
    )
    args = p.parse_args()

    # Prom-sourced metrics. `.format(w=…)` fills the rate window.
    rows: dict[str, float] = {}
    for key, tmpl in QUERIES.items():
        q = tmpl.format(w=args.window)
        try:
            # Per-agent metrics produce multiple rows — average them
            # for the single-row CSV output.
            vals = [float(r["value"][1]) for r in prom_query(args.prom, q)]
            rows[key] = sum(vals) / len(vals) if vals else float("nan")
        except Exception as e:
            print(f"# query {key} failed: {e}", file=sys.stderr)
            rows[key] = float("nan")

    # docker stats — backend CPU/RSS + producer CPU / RSS / tx-bytes.
    # Single two-sample pass (separated by --bytes-sample-window)
    # avoids three trips through `docker stats`.
    try:
        stats = docker_stats_with_bytes_rate(args.bytes_sample_window)

        backend = stats.get("docker-compose-backend-1") or {}
        rows["backend_cpu_pct"] = backend.get("cpu_pct", float("nan"))
        rows["backend_rss_mib"] = backend.get("mem_mib", float("nan"))

        producer = stats.get(args.producer_container) or {}
        # docker reports CPU as a percentage of one core; scale to
        # cores so the column matches agent_cpu_cores.
        rows["producer_cpu_cores"] = producer.get("cpu_pct", float("nan")) / 100.0
        rows["producer_rss_mib"] = producer.get("mem_mib", float("nan"))
        rows["producer_bytes_out_per_s"] = producer.get(
            "net_tx_per_s", float("nan")
        )
    except Exception as e:
        print(f"# docker stats failed: {e}", file=sys.stderr)
        for k in (
            "backend_cpu_pct",
            "backend_rss_mib",
            "producer_cpu_cores",
            "producer_rss_mib",
            "producer_bytes_out_per_s",
        ):
            rows[k] = float("nan")

    # Emit CSV with a stable column order. Producer columns sit
    # between the label columns and the agent columns so they show
    # up first in spreadsheets — the three-axis sweep's headline
    # metric is producer_bytes_out_per_s.
    header = [
        "baseline",
        "scale",
        "rate",
        "cardinality",
        "producer_cpu_cores",
        "producer_rss_mib",
        "producer_bytes_out_per_s",
        "agent_cpu_cores",
        "agent_rss_mib",
        "agent_in_kib_per_s",
        "agent_out_kib_per_s",
        "agent_points_per_s",
        "gateway_cpu_cores",
        "gateway_rss_mib",
        "gateway_points_per_s",
        "gateway_out_series_per_s",
        "backend_cpu_pct",
        "backend_rss_mib",
        "backend_samples_per_s",
        "backend_query_p99_ms",
    ]
    w = csv.writer(sys.stdout)
    w.writerow(header)
    w.writerow(
        [
            args.baseline,
            args.scale,
            args.rate,
            args.cardinality,
            *[f"{rows.get(c, float('nan')):.3f}" for c in header[4:]],
        ]
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
