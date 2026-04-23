from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
from pathlib import Path

import requests

PROMETHEUS_LINE_RE = re.compile(
    r"^([a-zA-Z_:][a-zA-Z0-9_:]*)\s*(\{[^}]*\})?\s+([+-eE0-9.]+|nan)"
)


def _parse_labels(blob: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for seg in blob.strip("{}").split(","):
        seg = seg.strip()
        if "=" not in seg:
            continue
        k, v = seg.split("=", 1)
        out[k.strip()] = v.strip().strip('"')
    return out


def fetch_metrics(url: str) -> list[dict[str, str]]:
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    rows: list[dict[str, str]] = []
    for line in resp.text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = PROMETHEUS_LINE_RE.match(line)
        if not m:
            continue
        rows.append({
            "metric": m.group(1),
            "labels": json.dumps(_parse_labels(m.group(2) or ""), sort_keys=True),
            "value": m.group(3),
        })
    return rows


def probe(url: str, timeout_s: float = 10.0) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            fetch_metrics(url)
            return True
        except Exception:
            time.sleep(0.25)
    return False


def append_snapshot(url: str, out_path: Path) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    wall_ns = time.time_ns()
    rows = fetch_metrics(url)
    fields = ["scrape_wall_ns", "metric", "labels", "value"]
    new = not out_path.is_file()
    with open(out_path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if new:
            w.writeheader()
        for r in rows:
            r["scrape_wall_ns"] = str(wall_ns)
            w.writerow({k: r.get(k, "") for k in fields})
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Poll Prometheus metrics into CSV.")
    parser.add_argument("--query", default="Q1")
    parser.add_argument("--slice", default="full")
    parser.add_argument("--out-dir", type=Path,
                        default=Path(__file__).resolve().parent / "results" / "sketch_output")
    parser.add_argument("--url", default="http://localhost:8889/metrics")
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--duration", type=float, default=7200.0)
    parser.add_argument("--probe-timeout", type=float, default=10.0)
    parser.add_argument("--no-probe", action="store_true")
    args = parser.parse_args()

    if not args.no_probe and not probe(args.url, args.probe_timeout):
        print(f"ERROR: {args.url!r} not reachable within {args.probe_timeout}s.",
              file=sys.stderr)
        sys.exit(1)

    out_dir = args.out_dir / args.query
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{args.slice}.csv"

    fields = ["scrape_wall_ns", "metric", "labels", "value"]
    start = time.time()
    fail_count = 0
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        while time.time() - start < args.duration:
            wall_ns = time.time_ns()
            try:
                for row in fetch_metrics(args.url):
                    row["scrape_wall_ns"] = str(wall_ns)
                    w.writerow({k: row.get(k, "") for k in fields})
            except Exception as exc:
                fail_count += 1
                if fail_count == 1:
                    print(f"scrape warning: {exc}", file=sys.stderr)
            f.flush()
            time.sleep(args.interval)

    if fail_count:
        print(f"scrape: {fail_count} failed polls", file=sys.stderr)


if __name__ == "__main__":
    main()
