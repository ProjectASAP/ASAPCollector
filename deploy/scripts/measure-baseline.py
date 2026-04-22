#!/usr/bin/env python3
"""
measure-baseline.py — pull per-baseline CPU, memory, bandwidth
figures from the running compose stack.

Queries Prometheus for every metric the paper §6.2 bandwidth +
§6.3 CPU tables need, plus supplements with `docker stats` for
backend resource usage (the backend doesn't self-report CPU/RSS
the way the patched otel processors do).

Output is CSV to stdout. The intended flow is:

    ./run-baseline-sweep.sh > sweep-YYYYMMDD.csv

which iterates baseline × (rate, cardinality) and concatenates
measure-baseline.py output from each configuration.

This is a pure stdlib Python script — no extra deps.

Usage:
    python3 measure-baseline.py [--prom URL] [--window 60s]
        [--baseline b0-raw|b1-serf|b2-full|b3-delta|b4-tunable|b5-gorilla]

The `--baseline` tag is just pass-through labelling; the script
doesn't know or care which baseline is active.
"""
import argparse
import csv
import json
import subprocess
import sys
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


def docker_stats() -> dict[str, tuple[float, float]]:
    """
    `docker stats --no-stream` for {CPU%, MemMB} per container.
    Matches docker-compose-* names so callers can index by service.
    """
    out = subprocess.check_output(
        [
            "docker",
            "stats",
            "--no-stream",
            "--format",
            "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}",
        ],
        text=True,
    )
    result: dict[str, tuple[float, float]] = {}
    for line in out.strip().splitlines():
        name, cpu, mem = line.split("|", 2)
        # "12.34%" → 12.34
        cpu_pct = float(cpu.rstrip("%"))
        # "335.8MiB / 1GiB" → 335.8
        mem_mib_str = mem.split("/", 1)[0].strip()
        if mem_mib_str.endswith("MiB"):
            mem_mib = float(mem_mib_str[:-3])
        elif mem_mib_str.endswith("GiB"):
            mem_mib = float(mem_mib_str[:-3]) * 1024
        elif mem_mib_str.endswith("KiB"):
            mem_mib = float(mem_mib_str[:-3]) / 1024
        else:
            mem_mib = float("nan")
        result[name] = (cpu_pct, mem_mib)
    return result


# Prometheus query templates. `w` is the rate window (e.g. "1m").
# Agent (v0.141) uses `_total` suffix on counters; gateway
# (v0.108) doesn't — hence the duplicated-looking queries.
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
    # Gateway is v0.108, no `_total` suffix on process counters.
    "gateway_cpu_cores": "rate(otelcol_process_cpu_seconds{{job=\"gateway\"}}[{w}])",
    "gateway_rss_mib": "otelcol_process_memory_rss{{job=\"gateway\"}} / 1024 / 1024",
    # Gateway v0.108 metric names drop the `_total` suffix that
    # v0.141 adds — use the no-suffix variant here.
    "gateway_points_per_s": (
        "rate(otelcol_receiver_accepted_metric_points"
        "     {{job=\"gateway\"}}[{w}])"
    ),
    "gateway_out_series_per_s": (
        "rate(otelcol_exporter_sent_metric_points"
        "     {{job=\"gateway\"}}[{w}])"
    ),
    # ── Backend tier (destination-2) ────────────────────────────
    "backend_samples_per_s": "rate(asap_ingest_samples_total[{w}])",
    "backend_query_p99_ms": (
        "1000 * histogram_quantile(0.99, "
        "sum by (le) (rate(asap_query_duration_seconds_bucket[{w}])))"
    ),
}


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--prom", default="http://localhost:9090")
    p.add_argument("--window", default="1m", help="rate() lookback window")
    p.add_argument("--baseline", default="unset", help="label for the output row")
    p.add_argument(
        "--scale", default="N1", help="label for the scale (e.g. N1 / N10)"
    )
    p.add_argument("--rate", default="", help="workload rate label")
    p.add_argument("--cardinality", default="", help="workload cardinality label")
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

    # Backend CPU/RSS via docker stats.
    try:
        stats = docker_stats()
        backend_stat = stats.get("docker-compose-backend-1")
        rows["backend_cpu_pct"] = backend_stat[0] if backend_stat else float("nan")
        rows["backend_rss_mib"] = backend_stat[1] if backend_stat else float("nan")
    except Exception as e:
        print(f"# docker stats failed: {e}", file=sys.stderr)
        rows["backend_cpu_pct"] = float("nan")
        rows["backend_rss_mib"] = float("nan")

    # Emit CSV with a stable column order.
    header = [
        "baseline",
        "scale",
        "rate",
        "cardinality",
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
