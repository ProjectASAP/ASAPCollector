#!/usr/bin/env python3
"""Measure Collector ingest/export counters over an E2E load window."""

from __future__ import annotations

import argparse
import json
import math
import re
import time
import urllib.request


SAMPLE_RE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{[^}]*\})?\s+([^\s]+)")


def parse_prometheus(text: str) -> dict[str, float]:
    totals: dict[str, float] = {}
    for line in text.splitlines():
        match = SAMPLE_RE.match(line)
        if not match or line.startswith("#"):
            continue
        try:
            value = float(match.group(2))
        except ValueError:
            continue
        if math.isfinite(value):
            totals[match.group(1)] = totals.get(match.group(1), 0.0) + value
    return totals


def scrape(url: str) -> dict[str, float]:
    with urllib.request.urlopen(url, timeout=10) as response:
        return parse_prometheus(response.read().decode("utf-8"))


def counter_total(values: dict[str, float], fragment: str) -> tuple[float, list[str]]:
    names = sorted(name for name in values if name.endswith(fragment) or name.endswith(fragment + "_total"))
    return sum(values[name] for name in names), names


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agent", action="append", required=True, metavar="NAME=URL")
    parser.add_argument("--duration", type=float, required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    endpoints = dict(item.split("=", 1) for item in args.agent)
    started_at = time.time()
    start = {name: scrape(url) for name, url in endpoints.items()}
    time.sleep(max(args.duration, 0.0))
    end = {name: scrape(url) for name, url in endpoints.items()}
    duration = max(time.time() - started_at, 1e-9)

    agents = {}
    fragments = {
        "accepted_metric_points": "receiver_accepted_metric_points",
        "refused_metric_points": "receiver_refused_metric_points",
        "sent_metric_points": "exporter_sent_metric_points",
        "send_failed_metric_points": "exporter_send_failed_metric_points",
    }
    for agent in sorted(endpoints):
        counters = {}
        metric_names = {}
        for key, fragment in fragments.items():
            before, before_names = counter_total(start[agent], fragment)
            after, after_names = counter_total(end[agent], fragment)
            counters[key] = after - before
            metric_names[key] = sorted(set(before_names) | set(after_names))
        agents[agent] = {"endpoint": endpoints[agent], "counter_deltas": counters,
                         "metric_names": metric_names}

    result = {
        "schema_version": 1,
        "duration_s": duration,
        "agents": agents,
    }
    with open(args.out, "w", encoding="utf-8") as handle:
        json.dump(result, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
