from __future__ import annotations

"""Poll the Prometheus metrics endpoint and write scraped rows to CSV.

Each row has: scrape_wall_ns, metric, labels (JSON), value.
Identical in structure to the DEBS benchmark scrape.py; adapted to use
exathlon file tags for output naming.
"""

import argparse
import csv
import json
import re
import sys
import time
from pathlib import Path

import requests

from common import file_tag_safe

PROMETHEUS_LINE_PATTERN = re.compile(
    r"^([a-zA-Z_:][a-zA-Z0-9_:]*)\s*(\{[^}]*\})?\s+([+-eE0-9.]+|nan)"
)


def parse_prometheus_label_blob(label_blob: str) -> dict[str, str]:
    labels: dict[str, str] = {}
    if not label_blob:
        return labels
    for segment in label_blob.strip("{}").split(","):
        segment = segment.strip()
        if not segment or "=" not in segment:
            continue
        key, value = segment.split("=", 1)
        labels[key.strip()] = value.strip().strip('"')
    return labels


def fetch_prometheus_metrics_text(url: str) -> list[dict[str, str]]:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    rows: list[dict[str, str]] = []
    for line in response.text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = PROMETHEUS_LINE_PATTERN.match(line)
        if not match:
            continue
        metric_name = match.group(1)
        labels_raw = match.group(2) or ""
        metric_value = match.group(3)
        rows.append({
            "metric": metric_name,
            "labels": json.dumps(
                parse_prometheus_label_blob(labels_raw), sort_keys=True
            ),
            "value": metric_value,
        })
    return rows


def probe_prometheus(url: str, timeout_s: float = 10.0) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            fetch_prometheus_metrics_text(url)
            return True
        except Exception:
            time.sleep(0.25)
    return False


def append_metrics_snapshot(url: str, output_path: Path) -> int:
    """Append one scrape to CSV (creates file with header if missing).  Returns rows written."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    scrape_wall_ns = time.time_ns()
    rows = fetch_prometheus_metrics_text(url)
    fieldnames = ["scrape_wall_ns", "metric", "labels", "value"]
    new_file = not output_path.is_file()
    with open(output_path, "a", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        if new_file:
            writer.writeheader()
        for row in rows:
            row["scrape_wall_ns"] = str(scrape_wall_ns)
            writer.writerow({key: row.get(key, "") for key in fieldnames})
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Poll Prometheus text metrics into CSV (exathlon benchmark)."
    )
    parser.add_argument("--query", default="Q1")
    parser.add_argument(
        "--file",
        default="app1/1_0_10000_17",
        help="File tag (e.g. app1/1_0_10000_17) used for output path naming.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "results" / "sketch_output",
    )
    parser.add_argument("--url", default="http://localhost:8889/metrics")
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--duration", type=float, default=7200.0)
    parser.add_argument(
        "--probe-timeout",
        type=float,
        default=10.0,
        help="Seconds to wait for first successful GET before polling.",
    )
    parser.add_argument(
        "--no-probe",
        action="store_true",
        help="Skip startup probe (e.g. NOP collector without Prometheus).",
    )
    args = parser.parse_args()

    if not args.no_probe and not probe_prometheus(args.url, timeout_s=args.probe_timeout):
        print(
            f"ERROR: Prometheus metrics not reachable at {args.url!r} within {args.probe_timeout}s.",
            file=sys.stderr,
        )
        sys.exit(1)

    tag = file_tag_safe(args.file)
    output_subdir = args.out_dir / args.query
    output_subdir.mkdir(parents=True, exist_ok=True)
    output_path = output_subdir / f"{tag}.csv"

    start_wall = time.time()
    fieldnames = ["scrape_wall_ns", "metric", "labels", "value"]
    fail_count = 0
    first_error: str | None = None

    with open(output_path, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        while time.time() - start_wall < args.duration:
            scrape_wall_ns = time.time_ns()
            try:
                for row in fetch_prometheus_metrics_text(args.url):
                    row["scrape_wall_ns"] = str(scrape_wall_ns)
                    writer.writerow({key: row.get(key, "") for key in fieldnames})
            except Exception as exc:
                fail_count += 1
                if first_error is None:
                    first_error = f"{type(exc).__name__}: {exc}"
                    print(f"scrape warning: {first_error}", file=sys.stderr)
                elif fail_count == 2:
                    print("scrape: further failures suppressed", file=sys.stderr)
            outfile.flush()
            time.sleep(args.interval)

    if fail_count:
        print(f"scrape: {fail_count} failed poll(s)", file=sys.stderr)


if __name__ == "__main__":
    main()
