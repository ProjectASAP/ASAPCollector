#!/usr/bin/env python3
"""Google cluster-trace fetcher (2011 + 2019).

Downloads, verifies, and caches a documented subsample of the public
Google cluster traces so the rest of the pipeline (otlp_mapper.py +
run.py) can replay against ASAP without paying the full
~2 TB download cost on every run.

## Subsample selection

Both traces are huge (2011: ~40 GB compressed; 2019: ~2 TB across the
8 cells a-h). We do NOT pretend to fetch the full corpus. Instead we
fetch a documented "head" of the relevant first part-file:

  2011 trace:
    table        : task_usage (per-task 5-minute resource samples)
    transport    : gzipped CSV part files (no header) under
                   https://storage.googleapis.com/clusterdata-2011-2/task_usage/
    part         : part-00000-of-00500.csv.gz (~91 MB compressed,
                   ~600 MB decompressed)
    sample size  : --max-rows rows from the streaming-decoded part
                   (default 100 000)
    rationale    : task_usage gives per-(machine, job, task) CPU/mem
                   timeseries — directly maps to OTLP gauges keyed by
                   {machine_id, job_id, task_index}, the natural
                   workload-resource shape for ASAP claims #1–#5.

  2019 trace:
    table        : instance_usage (cell a)
    transport    : gzipped JSON-Lines (newline-delimited JSON) under
                   https://storage.googleapis.com/clusterdata_2019_a/
    part         : instance_usage-000000000000.json.gz (~498 MB
                   compressed, ~3 GB decompressed)
    sample size  : --max-rows rows from the streaming-decoded part
                   (default 100 000)
    rationale    : 2019 instance_usage rows carry start_time/end_time
                   plus average_usage / maximum_usage CPU+memory
                   measurements per Borg instance. Same shape concern
                   as 2011 — per-instance resource gauges over time.

Both samples are deterministic: same `--year` + `--max-rows` produces
byte-identical output (we sort the resulting rows on a stable key
before writing the cache).

## Streaming decode + early stop

We DO NOT download the full part-file. The fetcher uses a streaming
HTTP response + streaming gunzip and stops reading the upstream as
soon as `max_rows` records have been buffered. This keeps the smoke
test (`--max-rows 1000`) at <50 MB of network traffic even though
the underlying files are hundreds of MB compressed.

## Disk + wall-time budget

  --year 2011 --max-rows 1_000     :   <2  MB,    ~5 s   (smoke)
  --year 2011 --max-rows 100_000   :  ~50 MB,   ~30 s   (default; pulls
                                                          most/all of the
                                                          first part)
  --year 2011 (full task_usage)    :  ~40 GB,    ~6 h   (paper experiment)

  --year 2019 --max-rows 1_000     :   ~2 MB,    ~10 s  (smoke)
  --year 2019 --max-rows 100_000   :  ~80 MB,   ~60 s   (default)
  --year 2019 (full cell-a usage)  : ~250 GB,   ~24 h   (paper experiment)
  --year 2019 (full all 8 cells)   :   ~2 TB,    ~1 wk   (not recommended)

The default --max-rows = 100_000 is what the paper experiments use
unless otherwise stated; it fits on a laptop and produces enough
unique (machine, task) tuples to hit the 1k/10k/100k cardinality
sweep.

## Idempotence

The fetcher writes:

  <out-dir>/<year>/<table>.csv          — the parsed sample (header + rows)
  <out-dir>/<year>/<table>.sha256       — checksum of the above
  <out-dir>/<year>/manifest.json        — fetch parameters + source URLs

If <table>.csv already exists and its sha256 matches the recorded
checksum, the fetcher returns immediately. To force a re-fetch:
delete the manifest.

## No auth required

Both buckets are world-readable; no GCP credentials needed. We use
plain HTTPS so the fetcher works on machines without `gsutil`.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import os
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

# ---------------------------------------------------------------------------
# Source registry — what we know about each trace's public layout.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TraceSource:
    year: str
    table: str
    # The first part-file URL. Used as the "head" subsample source.
    part_url: str
    # Canonical column ordering. The fetcher writes a CSV with this
    # exact header regardless of the upstream encoding, so the
    # otlp_mapper can index by position.
    header: tuple[str, ...]
    # Upstream encoding. "csv-no-header" (2011) or "jsonl" (2019).
    fmt: str


# 2011 schema, per ClusterData2011_2.md.
TRACE_2011 = TraceSource(
    year="2011",
    table="task_usage",
    part_url=(
        "https://storage.googleapis.com/clusterdata-2011-2/"
        "task_usage/part-00000-of-00500.csv.gz"
    ),
    header=(
        "start_time", "end_time", "job_id", "task_index", "machine_id",
        "cpu_rate", "canonical_memory_usage", "assigned_memory_usage",
        "unmapped_page_cache", "total_page_cache", "maximum_memory_usage",
        "disk_io_time", "local_disk_space_usage", "maximum_cpu_rate",
        "maximum_disk_io_time", "cycles_per_instruction",
        "memory_accesses_per_instruction", "sample_portion",
        "aggregation_type", "sampled_cpu_usage",
    ),
    fmt="csv-no-header",
)

# 2019 schema, per ClusterData2019.md. The published JSON has more
# nested fields (cpu_usage_distribution, etc.) which we flatten to
# stable scalars when present and drop when nested.
TRACE_2019 = TraceSource(
    year="2019",
    table="instance_usage",
    part_url=(
        "https://storage.googleapis.com/clusterdata_2019_a/"
        "instance_usage-000000000000.json.gz"
    ),
    header=(
        "start_time", "end_time", "collection_id", "instance_index",
        "machine_id", "alloc_collection_id",
        "average_usage_cpus", "average_usage_memory",
        "maximum_usage_cpus", "maximum_usage_memory",
        "random_sample_usage_cpus", "random_sample_usage_memory",
        "assigned_memory", "page_cache_memory", "cycles_per_instruction",
        "memory_accesses_per_instruction", "sample_rate",
        "cpu_usage_distribution", "tail_cpu_usage_distribution",
    ),
    fmt="jsonl",
)

SOURCES: dict[str, TraceSource] = {
    "2011": TRACE_2011,
    "2019": TRACE_2019,
}


# ---------------------------------------------------------------------------
# Streaming HTTP + decode
# ---------------------------------------------------------------------------


def _open_streaming_gzip(url: str, timeout_s: float = 60.0) -> io.BufferedReader:
    """Open `url` and return a buffered reader that yields decompressed bytes.

    The returned object can be wrapped in `io.TextIOWrapper` for line
    iteration. The HTTP response stays open until the caller closes
    the reader (e.g. via early-stop after max_rows).
    """
    req = urllib.request.Request(
        url, headers={"User-Agent": "asap-google-cluster-fetcher/1.0"},
    )
    resp = urllib.request.urlopen(req, timeout=timeout_s)
    # gzip.GzipFile reads incrementally; pair with a TextIOWrapper for line iter.
    gz = gzip.GzipFile(fileobj=resp)
    return gz  # type: ignore[return-value]


def _stream_csv_rows(url: str, max_rows: int) -> Iterable[list[str]]:
    """Yield up to `max_rows` rows from a gzipped CSV (no header)."""
    raw = _open_streaming_gzip(url)
    text = io.TextIOWrapper(raw, encoding="utf-8", errors="replace", newline="")
    reader = csv.reader(text)
    n = 0
    try:
        for row in reader:
            if not row:
                continue
            yield row
            n += 1
            if max_rows > 0 and n >= max_rows:
                return
    finally:
        text.close()


def _stream_jsonl_rows(
    url: str, max_rows: int, header: tuple[str, ...],
) -> Iterable[list[str]]:
    """Yield up to `max_rows` rows from a gzipped JSON-Lines stream.

    Each input record is projected onto `header`'s column order:
    missing keys become "", complex values (dict / list) become "".
    """
    raw = _open_streaming_gzip(url)
    text = io.TextIOWrapper(raw, encoding="utf-8", errors="replace")
    n = 0
    try:
        for line in text:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            row = [_flatten_2019_field(obj, k) for k in header]
            yield row
            n += 1
            if max_rows > 0 and n >= max_rows:
                return
    finally:
        text.close()


def _flatten_2019_field(obj: dict, key: str) -> str:
    """Project a 2019 JSON record onto a single string column.

    The 2019 schema nests average_usage / maximum_usage as
    {cpus, memory} sub-objects. We flatten those to
    `average_usage_cpus`, `average_usage_memory`, etc. — same
    canonical column names the OTLP mapper expects.
    """
    # Direct key.
    if key in obj:
        v = obj[key]
        return _scalar_or_empty(v)

    # Flattened forms: average_usage_cpus, average_usage_memory, etc.
    parents = (
        "average_usage", "maximum_usage", "random_sample_usage",
    )
    for parent in parents:
        for child in ("cpus", "memory"):
            flat = f"{parent}_{child}"
            if key == flat and isinstance(obj.get(parent), dict):
                return _scalar_or_empty(obj[parent].get(child))
    return ""


def _scalar_or_empty(v) -> str:
    if v is None:
        return ""
    if isinstance(v, (str, int, float, bool)):
        return str(v)
    # dict / list — drop (we've already pre-flattened the cases we care about).
    return ""


# ---------------------------------------------------------------------------
# Fetcher core
# ---------------------------------------------------------------------------


def _file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fp:
        for chunk in iter(lambda: fp.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _normalize_to_schema(rows: list[list[str]], n_cols: int) -> list[list[str]]:
    out: list[list[str]] = []
    for row in rows:
        if not row:
            continue
        if len(row) >= n_cols:
            out.append([(x or "").strip() for x in row[:n_cols]])
        else:
            padded = [(x or "").strip() for x in row] + [""] * (n_cols - len(row))
            out.append(padded)
    return out


def _stable_sort_key(row: list[str], src: TraceSource) -> tuple:
    """Sort by (start_time, machine_id, identity_key) for deterministic order.

    2011 identity = (job_id, task_index)
    2019 identity = (collection_id, instance_index)
    """
    h = src.header
    def _col(name: str) -> str:
        return row[h.index(name)] if name in h else ""

    def _i(name: str) -> int:
        v = _col(name)
        try:
            return int(float(v)) if v else 0
        except ValueError:
            return 0

    if src.year == "2011":
        return (_i("start_time"), _col("machine_id"), _i("job_id"), _i("task_index"))
    return (_i("start_time"), _col("machine_id"), _col("collection_id"),
            _i("instance_index"))


def _fetch_streaming(src: TraceSource, max_rows: int) -> list[list[str]]:
    """Return up to `max_rows` raw rows from the upstream, in source order.

    `max_rows == 0` means "all rows", which on the 2019 trace can be
    multiple GB; the caller is responsible for not asking for that
    on a smoke run.
    """
    if src.fmt == "csv-no-header":
        rows = list(_stream_csv_rows(src.part_url, max_rows))
    elif src.fmt == "jsonl":
        rows = list(_stream_jsonl_rows(src.part_url, max_rows, src.header))
    else:
        raise RuntimeError(f"unknown source format: {src.fmt!r}")
    return rows


def fetch(year: str, out_dir: Path, max_rows: int) -> Path:
    """Fetch + cache + verify the documented subsample.

    Returns the path to the cached CSV. Idempotent: if the cache is
    valid (manifest matches + sha256 matches), no network IO is done.
    """
    if year not in SOURCES:
        raise ValueError(f"unknown year {year!r}; supported: {sorted(SOURCES)}")
    src = SOURCES[year]

    target_dir = out_dir / year
    target_dir.mkdir(parents=True, exist_ok=True)
    csv_path = target_dir / f"{src.table}.csv"
    sha_path = target_dir / f"{src.table}.sha256"
    manifest_path = target_dir / "manifest.json"

    # Idempotence check.
    if csv_path.is_file() and sha_path.is_file() and manifest_path.is_file():
        try:
            manifest = json.loads(manifest_path.read_text())
            recorded_sha = sha_path.read_text().strip().split()[0]
            if (
                manifest.get("year") == year
                and manifest.get("max_rows") == max_rows
                and manifest.get("source_url") == src.part_url
                and _file_sha256(csv_path) == recorded_sha
            ):
                print(
                    f"fetch({year}): cache hit — {csv_path} "
                    f"({manifest.get('row_count')} rows, sha {recorded_sha[:12]}...)",
                    file=sys.stderr,
                )
                return csv_path
        except (json.JSONDecodeError, OSError, IndexError):
            pass

    print(
        f"fetch({year}): streaming up to {max_rows or 'ALL'} rows from "
        f"{src.part_url}",
        file=sys.stderr,
    )
    raw_rows = _fetch_streaming(src, max_rows)
    if not raw_rows:
        raise RuntimeError(f"empty stream from {src.part_url}")

    rows = _normalize_to_schema(raw_rows, len(src.header))
    rows.sort(key=lambda r: _stable_sort_key(r, src))

    if max_rows > 0:
        rows = rows[:max_rows]

    with open(csv_path, "w", newline="", encoding="utf-8") as fp:
        w = csv.writer(fp)
        w.writerow(src.header)
        w.writerows(rows)

    digest = _file_sha256(csv_path)
    sha_path.write_text(f"{digest}  {csv_path.name}\n")

    manifest = {
        "year": year,
        "table": src.table,
        "source_url": src.part_url,
        "source_format": src.fmt,
        "max_rows": max_rows,
        "row_count": len(rows),
        "schema_version": "asap-google-cluster-1",
        "header": list(src.header),
        "sha256": digest,
        "fetched_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")

    print(
        f"fetch({year}): wrote {len(rows)} rows -> {csv_path}\n"
        f"  sha256: {digest}\n"
        f"  manifest: {manifest_path}",
        file=sys.stderr,
    )
    return csv_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Fetch a deterministic subsample of the Google cluster trace.",
    )
    parser.add_argument(
        "--year", choices=sorted(SOURCES), required=True,
        help="Trace year. 2011 = task_usage, 2019 = instance_usage.",
    )
    parser.add_argument(
        "--out-dir", type=Path,
        default=Path(os.environ.get("GCT_OUT_DIR", "/tmp/gct")),
        help="Cache root (default /tmp/gct).",
    )
    parser.add_argument(
        "--max-rows", type=int,
        default=int(os.environ.get("GCT_MAX_ROWS", "100000")),
        help=(
            "Max rows to keep from the first part file. "
            "Default 100000 = paper-experiment subsample. "
            "Use 0 for the full part file (this is many GB; do not "
            "use for smoke runs)."
        ),
    )
    args = parser.parse_args(argv)
    try:
        fetch(args.year, args.out_dir, args.max_rows)
    except Exception as exc:  # noqa: BLE001
        print(f"fetch failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
