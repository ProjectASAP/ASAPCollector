#!/usr/bin/env python3
"""Google cluster trace -> OTLP-shaped metric stream.

Reads the cached CSV produced by `fetcher.py` and emits a stream of
OTLP-shaped metric events that match the wire shape produced by
`otel-app/`. The output is JSON-Lines so it can be
consumed by `run.py replay` (which then drives an OTLP/gRPC receiver
on the agent), and so unit tests can assert against it as plain text.

## Output shape

Each line is one JSON object:

  {
    "timestamp_ms": 1234567890123,
    "metric": "google_cluster_cpu_rate",          // or *_memory_usage
    "value": 0.0123,
    "attributes": {
        "zone": "z3", "rack": "r07", "host": "n01-024",
        "service": "job-1234567890", "task": "0"
    },
    "series_id": "z3:r07:n01-024:job-1234567890:0"
  }

Attribute keys ({zone, rack, host, service, task}) are chosen to match
otel-app's existing 4-dim `{zone, rack, node, pod}` schema as
closely as the trace allows (see "Mapping" below); `series_id` is the
deterministic concatenation used by otel-app's trace-replay
mode (timestamp_ms, series_id, value).

The mapper produces TWO metric families per row:

  - {metric}_cpu_rate          : the CPU usage rate sample (gauge)
  - {metric}_memory_usage      : canonical memory usage sample (gauge)

This mirrors otel-app's pattern of emitting two correlated
families per event (counter + latency gauge); having two families
lets the same trace exercise both quantile-flavoured and
sum/topk-flavoured queries.

## Mapping (trace row -> attributes)

  2011 task_usage row -> {
      zone     = hash(machine_id) % zone_vals     # synthetic, no zone field
      rack     = hash(machine_id) % rack_vals     # ditto
      host     = "host-{machine_id}"
      service  = "job-{job_id}"
      task     = "{task_index}"
  }

  2019 instance_usage row -> {
      zone     = hash(machine_id) % zone_vals
      rack     = hash(machine_id) % rack_vals
      host     = "host-{machine_id}"
      service  = "coll-{collection_id}"
      task     = "{instance_index}"
  }

The 2011/2019 schemas don't carry zone/rack columns directly, so we
synthesize them deterministically from machine_id. This is honest:
the original trace does cluster machines into platform IDs (2011
`machine_events`) and clusters/cells (2019), but those joins require
extra tables. For the cardinality-cap experiment, what matters is
that the projection is reproducible and that hashing produces a
roughly uniform spread across the synthetic zone/rack values — which
matches how otel-app generates labels.

## Cardinality cap (the projection bias)

`--cardinality-cap N` projects the {(machine_id, service, task)}
identity tuples onto a hashed N-element subset. The hash is
`hashlib.blake2b(identity, digest_size=8)` mod N. This is a
fingerprinting projection: collisions between distinct identities
are possible and deliberately so — that's what lets us drive the
SDK aggregator at a fixed cardinality regardless of how big the
underlying trace is.

### Quantitative bias

For a trace with U distinct identities and a cap of N:

  - If U <= N: no collisions, projection is identity. Bias = 0.
  - If U  > N: each cell receives ~U/N collisions in expectation;
    Var(cell-load) ≈ U/N (Poisson approx). The bias on per-cell
    sample counts is bounded by O(sqrt(U/N) / (U/N)) = O(sqrt(N/U))
    relative standard deviation. For U=1e7, N=1e5 -> ~1% RSD.
  - Distinct-count queries (`count_unique` over the projection)
    saturate at N. To recover the true cardinality the test harness
    multiplies by U/N — this scaling is documented in queries.json.

We do NOT shuffle by trace time, only by identity tuple, so the
*temporal* distribution within each (capped) series is preserved.
That keeps quantile/topk queries meaningful at the cost of mixing
samples from formerly-distinct underlying tasks.

## Determinism

For a fixed (input file, cardinality-cap, schema_dims), the output
is byte-stable. We:

  1. Sort the input rows on (start_time, machine_id, identity_key)
     before emitting.
  2. Use a deterministic blake2b hash with a fixed key.
  3. Iterate dictionaries via `sorted(...)` where order matters
     for output bytes.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

# ---------------------------------------------------------------------------
# Schema (matches otel-app's default zone/rack/node/pod widths)
# ---------------------------------------------------------------------------

DEFAULT_ZONE_VALS = 4
DEFAULT_RACK_VALS = 10

# Used to derive the zone/rack synthesizer; cap on the per-cell
# fanout. For ASAP cardinality sweeps we only really vary --cardinality-cap.
HASH_KEY = b"asap-google-cluster-mapper-v1\x00"


# ---------------------------------------------------------------------------
# Per-year parsing rules
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class YearShape:
    year: str
    # Column name -> position in the canonical fetcher header.
    cols: dict[str, int]
    identity_keys: tuple[str, ...]
    service_col: str
    task_col: str
    machine_col: str
    start_col: str
    cpu_col: str
    mem_col: str
    metric_prefix: str


# Mirrors fetcher.TRACE_2011.header indices.
SHAPE_2011 = YearShape(
    year="2011",
    cols={
        "start_time": 0, "end_time": 1, "job_id": 2, "task_index": 3,
        "machine_id": 4, "cpu_rate": 5, "canonical_memory_usage": 6,
    },
    identity_keys=("job_id", "task_index"),
    service_col="job_id",
    task_col="task_index",
    machine_col="machine_id",
    start_col="start_time",
    cpu_col="cpu_rate",
    mem_col="canonical_memory_usage",
    metric_prefix="google_cluster_2011",
)

# Mirrors fetcher.TRACE_2019.header indices.
SHAPE_2019 = YearShape(
    year="2019",
    cols={
        "start_time": 0, "end_time": 1, "collection_id": 2,
        "instance_index": 3, "machine_id": 4,
        "average_usage_cpus": 6, "average_usage_memory": 7,
    },
    identity_keys=("collection_id", "instance_index"),
    service_col="collection_id",
    task_col="instance_index",
    machine_col="machine_id",
    start_col="start_time",
    cpu_col="average_usage_cpus",
    mem_col="average_usage_memory",
    metric_prefix="google_cluster_2019",
)

SHAPES: dict[str, YearShape] = {"2011": SHAPE_2011, "2019": SHAPE_2019}


# ---------------------------------------------------------------------------
# Hashing helpers
# ---------------------------------------------------------------------------


def _hash_int(s: str, mod: int) -> int:
    """Stable, salted blake2b hash modulo `mod`."""
    if mod <= 0:
        return 0
    h = hashlib.blake2b(s.encode("utf-8"), digest_size=8, key=HASH_KEY)
    return int.from_bytes(h.digest(), "big") % mod


# ---------------------------------------------------------------------------
# Mapper core
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OTLPRow:
    timestamp_ms: int
    metric: str
    value: float
    attributes: dict[str, str]
    series_id: str

    def to_json(self) -> str:
        # Sort keys for byte-stable output.
        return json.dumps(
            {
                "timestamp_ms": self.timestamp_ms,
                "metric": self.metric,
                "value": self.value,
                "attributes": dict(sorted(self.attributes.items())),
                "series_id": self.series_id,
            },
            sort_keys=False,  # explicit ordering above
            separators=(",", ":"),
        )


def _to_float(s: str) -> float | None:
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _to_ms(start_time: str) -> int | None:
    """Convert the trace's start_time to milliseconds since epoch.

    2011 timestamps are in microseconds since the trace start
    (relative); we leave them as-is interpreted as milliseconds since
    a synthetic epoch — what matters for the harness is monotone
    ordering. 2019 timestamps are likewise microseconds-since-trace-
    start; same treatment. Note this means absolute wall times in the
    OTLP stream are NOT real-world dates, but the inter-arrival
    structure that ASAP's quantile/topk evaluation depends on IS
    preserved.
    """
    v = _to_float(start_time)
    if v is None:
        return None
    # Trace timestamps are us-since-start; / 1000 -> ms-since-start.
    return int(v / 1000.0)


def map_rows(
    csv_path: Path,
    year: str,
    cardinality_cap: int,
    zone_vals: int,
    rack_vals: int,
) -> Iterator[OTLPRow]:
    """Stream OTLP rows out of the cached CSV. Deterministic order."""
    if year not in SHAPES:
        raise ValueError(f"unknown year {year!r}; supported: {sorted(SHAPES)}")
    shape = SHAPES[year]

    with open(csv_path, "r", encoding="utf-8") as fp:
        reader = csv.reader(fp)
        try:
            header = next(reader)
        except StopIteration:
            return
        # Validate header matches what the fetcher writes; otherwise
        # column indices wouldn't be correct.
        for col, idx in shape.cols.items():
            if idx >= len(header) or header[idx] != col:
                raise RuntimeError(
                    f"mapper: header mismatch for {year}: expected "
                    f"col {col!r} at index {idx}, got header={header}"
                )
        rows = list(reader)

    # Re-sort defensively — fetcher already sorts, but we don't trust
    # callers (e.g. tests) not to hand us hand-edited CSVs.
    def _sort_key(row: list[str]) -> tuple:
        sc = shape.cols[shape.start_col]
        mc = shape.cols[shape.machine_col]
        ident = tuple(
            row[shape.cols[k]] if shape.cols[k] < len(row) else ""
            for k in shape.identity_keys
        )
        try:
            t = int(float(row[sc])) if sc < len(row) and row[sc] else 0
        except ValueError:
            t = 0
        return (t, row[mc] if mc < len(row) else "", *ident)

    rows.sort(key=_sort_key)

    for row in rows:
        ts = _to_ms(row[shape.cols[shape.start_col]] if shape.cols[shape.start_col] < len(row) else "")
        if ts is None:
            continue
        machine = row[shape.cols[shape.machine_col]] if shape.cols[shape.machine_col] < len(row) else ""
        service = row[shape.cols[shape.service_col]] if shape.cols[shape.service_col] < len(row) else ""
        task = row[shape.cols[shape.task_col]] if shape.cols[shape.task_col] < len(row) else ""
        cpu = _to_float(row[shape.cols[shape.cpu_col]] if shape.cols[shape.cpu_col] < len(row) else "")
        mem = _to_float(row[shape.cols[shape.mem_col]] if shape.cols[shape.mem_col] < len(row) else "")

        identity = f"{machine}|{service}|{task}"
        if cardinality_cap > 0:
            cell = _hash_int(identity, cardinality_cap)
            # The capped identity replaces the raw service/task with
            # a synthetic cell label so downstream queries see a
            # closed alphabet of N values regardless of trace size.
            service = f"svc-{cell:06d}"
            task = "0"
            machine = f"m-{cell:06d}"

        zone = f"z{_hash_int(machine, zone_vals)}"
        rack = f"r{_hash_int(machine, rack_vals):02d}"
        host = f"host-{machine}"
        attrs = {
            "zone": zone,
            "rack": rack,
            "host": host,
            "service": f"svc-{service}" if not service.startswith("svc-") else service,
            "task": str(task),
        }
        series_id = ":".join(
            (zone, rack, host, attrs["service"], attrs["task"])
        )

        if cpu is not None:
            yield OTLPRow(
                timestamp_ms=ts,
                metric=f"{shape.metric_prefix}_cpu_rate",
                value=cpu,
                attributes=attrs,
                series_id=series_id,
            )
        if mem is not None:
            yield OTLPRow(
                timestamp_ms=ts,
                metric=f"{shape.metric_prefix}_memory_usage",
                value=mem,
                attributes=attrs,
                series_id=series_id,
            )


# ---------------------------------------------------------------------------
# Detection helper used by run.py / tests
# ---------------------------------------------------------------------------


def detect_year_from_dir(in_dir: Path) -> str | None:
    """Look at <in-dir>/<year>/manifest.json to figure out the cached year.

    If both years are cached, returns the most recent (lexicographic
    max — 2019 > 2011 by string compare).
    """
    candidates = []
    for child in sorted(in_dir.glob("*/manifest.json")):
        try:
            m = json.loads(child.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        if "year" in m:
            candidates.append(m["year"])
    if not candidates:
        return None
    return max(candidates)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Map cached Google cluster trace rows to OTLP-shaped JSONL.",
    )
    parser.add_argument(
        "--year", choices=sorted(SHAPES),
        help="Trace year. If omitted, auto-detect from --in-dir.",
    )
    parser.add_argument(
        "--in-dir",
        type=Path,
        default=Path(os.environ.get("GCT_OUT_DIR", "/tmp/gct")),
        help="Directory written by fetcher.py (default /tmp/gct).",
    )
    parser.add_argument(
        "--out", type=Path, required=True,
        help="Output JSONL path.",
    )
    parser.add_argument(
        "--cardinality-cap", type=int, default=1000,
        help=(
            "Project (machine, service, task) tuples onto an N-element "
            "hashed subset. 0 disables capping (use raw identities). "
            "Default 1000 matches the smallest cell of the synthetic "
            "harness sweep matrix."
        ),
    )
    parser.add_argument(
        "--zone-vals", type=int, default=DEFAULT_ZONE_VALS,
        help="Synthetic zone fanout (default 4 — matches otel-app).",
    )
    parser.add_argument(
        "--rack-vals", type=int, default=DEFAULT_RACK_VALS,
        help="Synthetic rack fanout (default 10 — matches otel-app).",
    )
    parser.add_argument(
        "--max-rows", type=int, default=0,
        help="Cap output rows (0 = all). Useful for tests.",
    )
    args = parser.parse_args(argv)

    year = args.year or detect_year_from_dir(args.in_dir)
    if year is None:
        print(
            f"otlp_mapper: cannot detect year from {args.in_dir}; "
            "pass --year explicitly or run fetcher.py first.",
            file=sys.stderr,
        )
        return 2

    shape = SHAPES[year]
    csv_path = args.in_dir / year / f"{shape.year}_{shape.metric_prefix.split('_')[-1]}.csv"
    # The fetcher writes <year>/<table>.csv; figure that out.
    # For 2011 it's task_usage.csv, for 2019 instance_usage.csv.
    table_filename = "task_usage.csv" if year == "2011" else "instance_usage.csv"
    csv_path = args.in_dir / year / table_filename

    if not csv_path.is_file():
        print(
            f"otlp_mapper: input CSV missing: {csv_path}\n"
            f"  Run: python3 fetcher.py --year {year} --out-dir {args.in_dir}",
            file=sys.stderr,
        )
        return 2

    args.out.parent.mkdir(parents=True, exist_ok=True)
    n_written = 0
    with open(args.out, "w", encoding="utf-8") as fp:
        for i, row in enumerate(map_rows(
            csv_path,
            year=year,
            cardinality_cap=args.cardinality_cap,
            zone_vals=args.zone_vals,
            rack_vals=args.rack_vals,
        )):
            if args.max_rows > 0 and n_written >= args.max_rows:
                break
            fp.write(row.to_json())
            fp.write("\n")
            n_written += 1

    print(
        f"otlp_mapper: wrote {n_written} OTLP rows -> {args.out}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
