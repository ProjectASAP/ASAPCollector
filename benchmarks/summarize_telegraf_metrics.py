#!/usr/bin/env python3
"""
Summarize Telegraf internal throughput and latency metrics from *.lp files.

Usage:
    python summarize_telegraf_metrics.py [--results-dir benchmarks/results]

The script scans all *.lp files beneath the results directory (non-recursive
by default), parses Influx line protocol entries that start with `internal_*`,
and prints a concise summary of:
  * Total metrics gathered/written/dropped and their per-second throughput
  * Average gather/write latency across all internal_gather/internal_write samples
"""

from __future__ import annotations

import argparse
import glob
import math
import os
from dataclasses import dataclass, field
from typing import Dict, Iterable, Optional, Tuple


@dataclass
class AgentSnapshot:
    timestamp: int
    gathered: int
    written: int
    dropped: int


@dataclass
class LatencyStats:
    total_ns: int = 0
    count: int = 0
    max_ns: int = 0

    def add(self, value_ns: int) -> None:
        self.total_ns += value_ns
        self.count += 1
        if value_ns > self.max_ns:
            self.max_ns = value_ns

    def average_ms(self) -> Optional[float]:
        if self.count == 0:
            return None
        return self.total_ns / self.count / 1e6

    def max_ms(self) -> Optional[float]:
        if self.count == 0:
            return None
        return self.max_ns / 1e6


@dataclass
class FileSummary:
    path: str
    agent_start: Optional[AgentSnapshot] = None
    agent_end: Optional[AgentSnapshot] = None
    gather_latency: LatencyStats = field(default_factory=LatencyStats)
    write_latency: LatencyStats = field(default_factory=LatencyStats)

    def duration_seconds(self) -> Optional[float]:
        if not (self.agent_start and self.agent_end):
            return None
        dt = self.agent_end.timestamp - self.agent_start.timestamp
        if dt <= 0:
            return None
        return dt / 1e9

    def throughput(self) -> Dict[str, float]:
        """Return per-second throughput for gathered/written/dropped metrics."""
        duration = self.duration_seconds()
        if duration is None or duration == 0:
            return {}
        gathered = (
            self.agent_end.gathered - self.agent_start.gathered
            if self.agent_start and self.agent_end
            else 0
        )
        written = (
            self.agent_end.written - self.agent_start.written
            if self.agent_start and self.agent_end
            else 0
        )
        dropped = (
            self.agent_end.dropped - self.agent_start.dropped
            if self.agent_start and self.agent_end
            else 0
        )
        return {
            "gathered_per_s": gathered / duration,
            "written_per_s": written / duration,
            "dropped_per_s": dropped / duration,
            "dropped_pct": (dropped / max(gathered, 1)) * 100.0 if gathered > 0 else 0.0,
            "total_gathered": gathered,
            "total_written": written,
            "total_dropped": dropped,
            "duration_s": duration,
        }


def parse_fields(part: str) -> Dict[str, float]:
    fields: Dict[str, float] = {}
    for token in part.split(","):
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        fields[key] = parse_value(value)
    return fields


def parse_value(raw: str) -> float:
    if not raw:
        return math.nan
    if raw.endswith("i"):
        raw = raw[:-1]
        if raw.startswith("0x") or raw.startswith("-0x"):
            return float(int(raw, 16))
        return float(int(raw))
    if raw.lower() in {"true", "t"}:
        return 1.0
    if raw.lower() in {"false", "f"}:
        return 0.0
    try:
        return float(raw)
    except ValueError:
        return math.nan


def parse_line(line: str) -> Optional[Tuple[str, Dict[str, float], int]]:
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    parts = line.split(" ", 2)
    if len(parts) != 3:
        return None
    measurement = parts[0].split(",", 1)[0]
    fields = parse_fields(parts[1])
    try:
        timestamp = int(parts[2])
    except ValueError:
        return None
    return measurement, fields, timestamp


def summarize_file(path: str) -> FileSummary:
    summary = FileSummary(path=path)
    with open(path, "r", encoding="utf-8") as handle:
        for raw_line in handle:
            parsed = parse_line(raw_line)
            if not parsed:
                continue
            measurement, fields, timestamp = parsed
            if measurement == "internal_agent":
                snapshot = AgentSnapshot(
                    timestamp=timestamp,
                    gathered=int(fields.get("metrics_gathered", 0)),
                    written=int(fields.get("metrics_written", 0)),
                    dropped=int(fields.get("metrics_dropped", 0)),
                )
                if summary.agent_start is None:
                    summary.agent_start = snapshot
                summary.agent_end = snapshot
            elif measurement == "internal_gather":
                gather_time = fields.get("gather_time_ns")
                if gather_time is not None:
                    summary.gather_latency.add(int(gather_time))
            elif measurement == "internal_write":
                write_time = fields.get("write_time_ns")
                if write_time is not None:
                    summary.write_latency.add(int(write_time))
    return summary


def collect_lp_files(results_dir: str) -> Iterable[str]:
    pattern = os.path.join(results_dir, "*.lp")
    return sorted(glob.glob(pattern))


def format_optional(value: Optional[float], unit: str = "ms") -> str:
    return f"{value:.2f} {unit}" if value is not None else "n/a"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Summarize Telegraf throughput/latency from *.lp files"
    )
    parser.add_argument(
        "--results-dir",
        default="benchmarks/results",
        help="Directory holding *.lp files (default: benchmarks/results)",
    )
    args = parser.parse_args()

    files = list(collect_lp_files(args.results_dir))
    if not files:
        print(f"No .lp files found in {args.results_dir}")
        return

    for path in files:
        summary = summarize_file(path)
        throughput = summary.throughput()
        print(f"\n=== {os.path.relpath(path)} ===")
        if not throughput:
            print("Insufficient internal_agent data to compute throughput.")
            continue
        print(
            f"Window: {throughput['duration_s']:.2f}s  "
            f"Total gathered: {throughput['total_gathered']:,}  "
            f"written: {throughput['total_written']:,}  "
            f"dropped: {throughput['total_dropped']:,} "
            f"({throughput['dropped_pct']:.2f}% drop)"
        )
        print(
            "Throughput: "
            f"{throughput['gathered_per_s']:.0f} gathered/s, "
            f"{throughput['written_per_s']:.0f} written/s, "
            f"{throughput['dropped_per_s']:.0f} dropped/s"
        )
        print(
            "Latency: "
            f"gather avg={format_optional(summary.gather_latency.average_ms())}, "
            f"max={format_optional(summary.gather_latency.max_ms())}; "
            f"write avg={format_optional(summary.write_latency.average_ms())}, "
            f"max={format_optional(summary.write_latency.max_ms())}"
        )
        print(
            f"samples: gather={summary.gather_latency.count}, "
            f"write={summary.write_latency.count}"
        )


if __name__ == "__main__":
    main()
