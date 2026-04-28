#!/usr/bin/env python3
"""Summarize one Prometheus client profiling cell as a CSV row."""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from pathlib import Path


def parse_size_to_bytes(value: str) -> float:
    value = value.strip()
    if not value:
        return math.nan
    for suffix, factor in (
        ("GiB", 1024**3),
        ("MiB", 1024**2),
        ("KiB", 1024),
        ("GB", 1e9),
        ("MB", 1e6),
        ("kB", 1e3),
        ("B", 1.0),
    ):
        if value.endswith(suffix):
            try:
                return float(value[: -len(suffix)]) * factor
            except ValueError:
                return math.nan
    try:
        return float(value)
    except ValueError:
        return math.nan


def parse_mem_to_mib(mem_usage: str) -> float:
    current = mem_usage.split("/", 1)[0].strip()
    bytes_value = parse_size_to_bytes(current)
    if math.isnan(bytes_value):
        return math.nan
    return bytes_value / 1024 / 1024


def parse_stats(path: str) -> dict[str, float]:
    try:
        data = json.loads(Path(path).read_text())
    except Exception:
        return {
            "cpu_cores": math.nan,
            "rss_mib": math.nan,
            "net_rx_bytes": math.nan,
            "net_tx_bytes": math.nan,
        }

    try:
        cpu_cores = float(data.get("CPUPerc", "").rstrip("%")) / 100.0
    except ValueError:
        cpu_cores = math.nan

    rss_mib = parse_mem_to_mib(data.get("MemUsage", ""))
    netio = data.get("NetIO", "")
    if "/" in netio:
        rx_raw, tx_raw = [part.strip() for part in netio.split("/", 1)]
        rx = parse_size_to_bytes(rx_raw)
        tx = parse_size_to_bytes(tx_raw)
    else:
        rx = tx = math.nan

    return {
        "cpu_cores": cpu_cores,
        "rss_mib": rss_mib,
        "net_rx_bytes": rx,
        "net_tx_bytes": tx,
    }


def read_scrapes(path: str) -> dict[str, float]:
    rows: list[tuple[float, float, int]] = []
    p = Path(path)
    if p.exists():
        for line in p.read_text().splitlines():
            if not line.strip():
                continue
            parts = line.split(",")
            if len(parts) != 3:
                continue
            try:
                rows.append((float(parts[0]), float(parts[1]), int(parts[2])))
            except ValueError:
                continue

    successes = [row for row in rows if row[2] == 0]
    durations = sorted(row[1] for row in successes)
    total_bytes = sum(row[0] for row in successes)
    avg_ms = sum(durations) / len(durations) if durations else math.nan
    if durations:
        idx = min(len(durations) - 1, int(math.ceil(len(durations) * 0.95)) - 1)
        p95_ms = durations[idx]
    else:
        p95_ms = math.nan

    return {
        "scrape_count": float(len(rows)),
        "scrape_success": float(len(successes)),
        "scrape_bytes": total_bytes,
        "scrape_avg_ms": avg_ms,
        "scrape_p95_ms": p95_ms,
    }


HEADER = [
    "phase",
    "update_mode",
    "instruments",
    "cardinality",
    "freq_hz",
    "scrape_interval",
    "profile_seconds",
    "soak_s",
    "producer_cpu_cores",
    "producer_rss_mib",
    "producer_net_rx_per_s",
    "producer_net_tx_per_s",
    "scrape_count",
    "scrape_success",
    "scrape_bytes_per_s",
    "scrape_avg_ms",
    "scrape_p95_ms",
    "cpu_profile",
    "heap_profile",
    "cpu_top",
    "heap_top",
]


def fmt(value: float) -> str:
    if math.isnan(value):
        return "nan"
    return f"{value:.3f}"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--header", action="store_true")
    parser.add_argument("--phase", required=True)
    parser.add_argument("--update-mode", required=True)
    parser.add_argument("--instruments", required=True)
    parser.add_argument("--cardinality", required=True)
    parser.add_argument("--freq-hz", required=True)
    parser.add_argument("--scrape-interval", required=True)
    parser.add_argument("--profile-seconds", type=float, required=True)
    parser.add_argument("--soak-s", required=True)
    parser.add_argument("--stats-before", required=True)
    parser.add_argument("--stats-after", required=True)
    parser.add_argument("--scrape-log", required=True)
    parser.add_argument("--cpu-profile", required=True)
    parser.add_argument("--heap-profile", required=True)
    parser.add_argument("--cpu-top", required=True)
    parser.add_argument("--heap-top", required=True)
    args = parser.parse_args()

    before = parse_stats(args.stats_before)
    after = parse_stats(args.stats_after)
    scrapes = read_scrapes(args.scrape_log)
    elapsed = max(args.profile_seconds, 1e-6)

    rx_rate = (
        (after["net_rx_bytes"] - before["net_rx_bytes"]) / elapsed
        if not math.isnan(after["net_rx_bytes"]) and not math.isnan(before["net_rx_bytes"])
        else math.nan
    )
    tx_rate = (
        (after["net_tx_bytes"] - before["net_tx_bytes"]) / elapsed
        if not math.isnan(after["net_tx_bytes"]) and not math.isnan(before["net_tx_bytes"])
        else math.nan
    )

    row = [
        args.phase,
        args.update_mode,
        args.instruments,
        args.cardinality,
        args.freq_hz,
        args.scrape_interval,
        fmt(args.profile_seconds),
        args.soak_s,
        fmt(after["cpu_cores"]),
        fmt(after["rss_mib"]),
        fmt(rx_rate),
        fmt(tx_rate),
        fmt(scrapes["scrape_count"]),
        fmt(scrapes["scrape_success"]),
        fmt(scrapes["scrape_bytes"] / elapsed),
        fmt(scrapes["scrape_avg_ms"]),
        fmt(scrapes["scrape_p95_ms"]),
        args.cpu_profile,
        args.heap_profile,
        args.cpu_top,
        args.heap_top,
    ]

    writer = csv.writer(sys.stdout, lineterminator="\n")
    if args.header:
        writer.writerow(HEADER)
    writer.writerow(row)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
