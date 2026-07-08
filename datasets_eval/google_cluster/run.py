#!/usr/bin/env python3
"""Google cluster trace orchestrator.

Mirrors `datasets_eval/debs/benchmark/run.py` in spirit: a single
script that runs the dataset's full lifecycle of fetch -> map -> replay
-> validate. The replay path drives an OTLP/gRPC receiver on the
running ASAP agent; we do NOT modify otel-app/ source —
the JSONL produced by `otlp_mapper.py` plus a thin OTLP/gRPC sender
in `replay()` is sufficient.

Subcommands:

  fetch     Download + cache the trace subsample (delegates to fetcher.py).
  map       Run the OTLP mapper (delegates to otlp_mapper.py).
  replay    Stream the JSONL into an OTLP/gRPC receiver. Pure-stdlib
            fallback prints a 'send-only-mode' summary if the
            opentelemetry-proto python package isn't available, so
            CI smoke tests work without that dep.
  validate  Verify queries.json schema matches deploy/scripts/queries-e2e.json
            and confirm the cached JSONL conforms to the OTLP shape
            this dataset claims to produce.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
REPO_ROOT = ROOT.parent.parent

# Stable OTLP attribute keys this dataset emits — used by `validate`
# to confirm map output matches what queries.json references.
EXPECTED_ATTR_KEYS = {"zone", "rack", "host", "service", "task"}

# Required keys in each queries.json entry. `kind` and `promql` are
# the schema defined by deploy/scripts/queries-e2e.json; the
# google-cluster log adds `expected_ground_truth_query` so the
# accuracy reducer can compare against ground truth.
QUERY_REQUIRED_KEYS = {"kind", "promql"}
# `metricsql` is the warm-tier query string the E2E harness sends to the
# data-plane query engine; `gt` is the structured ground-truth spec consumed
# by e2e/gt_eval.py; `id` is a stable handle for reports.
QUERY_OPTIONAL_KEYS = {"expected_ground_truth_query", "rationale", "id", "metricsql", "gt"}
# `frequency` covers the CountMinSketch per-item estimate(key) path.
QUERY_ALLOWED_KINDS = {"quantile", "topk", "sum", "count_unique", "frequency"}


def _run_module(module_path: Path, argv: list[str]) -> int:
    """Run a sibling .py file as a subprocess. Returns its exit code."""
    cmd = [sys.executable, str(module_path), *argv]
    return subprocess.call(cmd)


# ---------------------------------------------------------------------------
# Subcommand: fetch
# ---------------------------------------------------------------------------


def cmd_fetch(args: argparse.Namespace) -> int:
    return _run_module(
        ROOT / "fetcher.py",
        [
            "--year", args.year,
            "--out-dir", str(args.out_dir),
            "--max-rows", str(args.max_rows),
        ],
    )


# ---------------------------------------------------------------------------
# Subcommand: map
# ---------------------------------------------------------------------------


def cmd_map(args: argparse.Namespace) -> int:
    argv = [
        "--in-dir", str(args.in_dir),
        "--out", str(args.out),
        "--cardinality-cap", str(args.cardinality_cap),
    ]
    if args.year:
        argv += ["--year", args.year]
    if args.max_rows:
        argv += ["--max-rows", str(args.max_rows)]
    return _run_module(ROOT / "otlp_mapper.py", argv)


# ---------------------------------------------------------------------------
# Subcommand: replay
# ---------------------------------------------------------------------------


def _replay_dry_run(jsonl_path: Path, max_lines: int) -> int:
    """Pure-stdlib replay: read the JSONL and pretend to send.

    Reports how many rows would be emitted, the first/last timestamp,
    and the unique series count. This is what the smoke test calls.
    """
    if not jsonl_path.is_file():
        print(f"replay: input JSONL missing: {jsonl_path}", file=sys.stderr)
        return 2
    n = 0
    series: set[str] = set()
    first_ts: int | None = None
    last_ts: int | None = None
    with open(jsonl_path, "r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                print(f"replay: bad JSON line {n}", file=sys.stderr)
                return 3
            ts = int(obj["timestamp_ms"])
            if first_ts is None or ts < first_ts:
                first_ts = ts
            if last_ts is None or ts > last_ts:
                last_ts = ts
            series.add(obj["series_id"])
            n += 1
            if max_lines > 0 and n >= max_lines:
                break
    span_ms = (last_ts - first_ts) if (first_ts is not None and last_ts is not None) else 0
    print(
        f"replay: dry-run summary\n"
        f"  rows={n}\n"
        f"  unique_series={len(series)}\n"
        f"  first_ts={first_ts}\n"
        f"  last_ts={last_ts}\n"
        f"  span_ms={span_ms}",
        file=sys.stderr,
    )
    return 0


def _replay_otlp_grpc(
    jsonl_path: Path,
    endpoint: str,
    pace_factor: float,
    max_lines: int,
    wall_clock_anchor: bool = False,
    anchor_span_s: float = 0.0,
) -> int:
    """OTLP/gRPC sender. Lazy-imports opentelemetry-proto deps.

    Returns 0 on success, 4 if optional dependency missing (caller
    prints a documented fallback message), 1 on transport error.
    """
    try:
        import grpc  # type: ignore
        from opentelemetry.proto.collector.metrics.v1 import metrics_service_pb2  # type: ignore
        from opentelemetry.proto.collector.metrics.v1 import metrics_service_pb2_grpc  # type: ignore
        from opentelemetry.proto.common.v1 import common_pb2  # type: ignore
    except ImportError as exc:
        print(
            f"replay: opentelemetry-proto/grpc not available ({exc}); "
            "falling back to dry-run summary. To send for real, "
            "`pip install opentelemetry-proto grpcio` in your replay env.",
            file=sys.stderr,
        )
        return 4

    if not jsonl_path.is_file():
        print(f"replay: input JSONL missing: {jsonl_path}", file=sys.stderr)
        return 2

    channel = grpc.insecure_channel(endpoint)
    stub = metrics_service_pb2_grpc.MetricsServiceStub(channel)

    BATCH_SIZE = 500
    batch: list[dict[str, Any]] = []
    n = 0
    walk_start_ns = time.time_ns()
    trace_start_ms: int | None = None
    # When wall_clock_anchor is set we re-stamp EVERY datapoint at a SINGLE
    # wall-clock instant (captured once at replay start), discarding the
    # trace's epoch-relative timestamps. Rationale:
    #   - The trace spans ~31 days of trace-relative time; preserving that
    #     spacing scatters rows across thousands of (epoch-1970) windows so no
    #     recent-range [Ns] selector intersects the stored warm windows — the
    #     original "quantile_over_time returns empty" defect was this timing
    #     mismatch, NOT a reducer bug.
    #   - Collapsing to ONE instant folds the entire replay into ONE agent
    #     window, so every replayed series carries its full value multiset into
    #     a single per-series sketch in one warm window. A quantile_over_time
    #     /sum_over_time/count_over_time at now (range >= one window) then reads
    #     that one window and reconstructs the full-replay aggregate for ALL
    #     series (vs a send-time spread that splits a series across windows and
    #     leaves only a subset queryable from any single window).
    #   - GT is defined over ALL rows (window-bounds None) from VALUES only, so
    #     a value-preserving timestamp rewrite leaves the offline GT identical.
    # PRECONDITION: the replay must finish within one window_duration (the
    # asap_edge seals on WALL-CLOCK passing the window end, so a replay slower
    # than the window would have its instant's window seal mid-replay and drop
    # late rows). The per-family arms (~200k rows) replay in << 60s; for a
    # multi-minute all-families replay, slice per family or widen the window.
    anchor_now_ns = time.time_ns()
    # anchor_span_s > 0: instead of collapsing EVERY point onto a single
    # instant, spread the points across the last `anchor_span_s` seconds with a
    # distinct nanosecond each (still one window when span < window_duration and
    # the send is boundary-aligned). Single-instant anchoring gives every event
    # of a key an IDENTICAL (series, ts, value) tuple, so count-type sketches
    # (CountSketch/CountMin: value==1.0 per event) see the duplicates collapse
    # and lose all per-key multiplicity — topk/frequency then read ~1 per key.
    # A quantile workload is unaffected (its values differ), but a spread is
    # strictly safer for it too. Requires a pre-count to size the step.
    span_ns = int(anchor_span_s * 1_000_000_000)
    total_pts = 0
    if wall_clock_anchor and span_ns > 0:
        with open(jsonl_path, "r", encoding="utf-8") as _fp:
            total_pts = sum(1 for _l in _fp if _l.strip())
    step_ns = span_ns // max(1, total_pts - 1) if total_pts > 1 else 0
    # Spread FORWARD from now: [now, now+span]. now is boundary-aligned to a
    # fresh window start, so the whole span lands in the OPEN window. A backward
    # spread [now-span, now] would backdate points into the PREVIOUS window,
    # which has already sealed on wall-clock — the agent then drops them as
    # late-arriving (only the points nearest `now` survive). Keep span <
    # window_duration so it doesn't spill into the next window.
    anchor_start_ns = anchor_now_ns

    def flush(rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        req = metrics_service_pb2.ExportMetricsServiceRequest()
        rm = req.resource_metrics.add()
        attr = rm.resource.attributes.add()
        attr.key = "service.name"
        attr.value.string_value = "google-cluster-replay"
        sm = rm.scope_metrics.add()
        # One scope; group by metric name.
        per_metric: dict[str, list[dict[str, Any]]] = {}
        for r in rows:
            per_metric.setdefault(r["metric"], []).append(r)
        for mname, mrows in sorted(per_metric.items()):
            metric = sm.metrics.add()
            metric.name = mname
            for r in mrows:
                dp = metric.gauge.data_points.add()
                dp.as_double = float(r["value"])
                if wall_clock_anchor:
                    dp.time_unix_nano = r.get("_ts_ns", anchor_now_ns)
                else:
                    dp.time_unix_nano = int(r["timestamp_ms"]) * 1_000_000
                for k, v in sorted(r["attributes"].items()):
                    a = common_pb2.AnyValue()
                    a.string_value = str(v)
                    dp.attributes.add(key=k, value=a)
        try:
            stub.Export(req, timeout=10.0)
        except grpc.RpcError as exc:
            print(f"replay: OTLP/gRPC export failed: {exc}", file=sys.stderr)
            raise

    try:
        with open(jsonl_path, "r", encoding="utf-8") as fp:
            for line in fp:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                if trace_start_ms is None:
                    trace_start_ms = int(obj["timestamp_ms"])
                    if wall_clock_anchor:
                        print(
                            f"replay: wall-clock anchor on — re-stamping ALL rows at a "
                            f"single instant now={anchor_now_ns // 1_000_000} ms (collapse "
                            f"31-day trace span into ONE window; trace epoch-relative ts "
                            f"discarded, trace_start_ms={trace_start_ms}); a [Ns] query at "
                            f"now intersects that window",
                            file=sys.stderr,
                        )
                if pace_factor > 0:
                    target_offset_ms = (int(obj["timestamp_ms"]) - trace_start_ms) / pace_factor
                    target_ns = walk_start_ns + int(target_offset_ms * 1_000_000)
                    sleep_ns = target_ns - time.time_ns()
                    if sleep_ns > 0:
                        time.sleep(sleep_ns / 1e9)
                if wall_clock_anchor and step_ns > 0:
                    obj["_ts_ns"] = anchor_start_ns + n * step_ns
                batch.append(obj)
                if len(batch) >= BATCH_SIZE:
                    flush(batch)
                    batch.clear()
                n += 1
                if max_lines > 0 and n >= max_lines:
                    break
            if batch:
                flush(batch)
    except grpc.RpcError:
        return 1

    print(f"replay: sent {n} OTLP gauge points to {endpoint}", file=sys.stderr)
    return 0


def cmd_replay(args: argparse.Namespace) -> int:
    if args.dry_run or not args.endpoint:
        return _replay_dry_run(args.jsonl, args.max_lines)
    rc = _replay_otlp_grpc(args.jsonl, args.endpoint, args.pace_factor, args.max_lines,
                           wall_clock_anchor=getattr(args, "wall_clock_anchor", False),
                           anchor_span_s=getattr(args, "anchor_span_s", 0.0))
    if rc == 4:
        return _replay_dry_run(args.jsonl, args.max_lines)
    return rc


# ---------------------------------------------------------------------------
# Subcommand: validate
# ---------------------------------------------------------------------------


def _validate_query_entry(idx: int, q: dict[str, Any]) -> list[str]:
    errs: list[str] = []
    extra = set(q.keys()) - QUERY_REQUIRED_KEYS - QUERY_OPTIONAL_KEYS
    if extra:
        errs.append(f"query[{idx}]: unknown keys {sorted(extra)}")
    for k in QUERY_REQUIRED_KEYS:
        if k not in q:
            errs.append(f"query[{idx}]: missing required key {k!r}")
    if "kind" in q and q["kind"] not in QUERY_ALLOWED_KINDS:
        errs.append(
            f"query[{idx}]: kind={q['kind']!r} not in {sorted(QUERY_ALLOWED_KINDS)}"
        )
    if "promql" in q and not isinstance(q["promql"], str):
        errs.append(f"query[{idx}]: promql must be string, got {type(q['promql']).__name__}")
    if "promql" in q and isinstance(q["promql"], str) and not q["promql"].strip():
        errs.append(f"query[{idx}]: promql is empty")
    return errs


def _validate_queries_file(path: Path, reference: Path | None) -> int:
    if not path.is_file():
        print(f"validate: queries file missing: {path}", file=sys.stderr)
        return 2
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        print(f"validate: queries.json is not valid JSON: {exc}", file=sys.stderr)
        return 3
    if not isinstance(data, list) or not data:
        print("validate: queries.json must be a non-empty JSON array", file=sys.stderr)
        return 3

    errs: list[str] = []
    for i, q in enumerate(data):
        if not isinstance(q, dict):
            errs.append(f"query[{i}]: must be object, got {type(q).__name__}")
            continue
        errs.extend(_validate_query_entry(i, q))

    # Reference-schema check against deploy/scripts/queries-e2e.json
    if reference is not None and reference.is_file():
        try:
            ref = json.loads(reference.read_text())
        except json.JSONDecodeError:
            ref = None
        if isinstance(ref, list) and ref:
            ref_required = QUERY_REQUIRED_KEYS
            for i, q in enumerate(data):
                if not isinstance(q, dict):
                    continue
                missing = ref_required - set(q.keys())
                if missing:
                    errs.append(
                        f"query[{i}]: missing keys {sorted(missing)} required "
                        f"by reference schema {reference}"
                    )
            ref_kinds = {q.get("kind") for q in ref if isinstance(q, dict)}
            for i, q in enumerate(data):
                k = q.get("kind") if isinstance(q, dict) else None
                if k not in ref_kinds and k not in QUERY_ALLOWED_KINDS:
                    errs.append(
                        f"query[{i}]: kind={k!r} not in reference {sorted(ref_kinds)}"
                    )

    if errs:
        print("validate: FAIL", file=sys.stderr)
        for e in errs:
            print(f"  {e}", file=sys.stderr)
        return 1

    print(
        f"validate: OK — {len(data)} queries match the OTLP-shape "
        f"schema ({sorted(QUERY_ALLOWED_KINDS)}).",
        file=sys.stderr,
    )
    return 0


def _validate_jsonl_shape(path: Path, max_lines: int = 1000) -> int:
    if not path.is_file():
        # Not an error — validate is queries-only when no JSONL given.
        return 0
    bad: list[str] = []
    n = 0
    with open(path, "r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                bad.append(f"line {n+1}: not JSON")
                continue
            for req in ("timestamp_ms", "metric", "value", "attributes", "series_id"):
                if req not in obj:
                    bad.append(f"line {n+1}: missing {req!r}")
            if isinstance(obj.get("attributes"), dict):
                missing_attrs = EXPECTED_ATTR_KEYS - set(obj["attributes"].keys())
                if missing_attrs:
                    bad.append(
                        f"line {n+1}: attributes missing keys "
                        f"{sorted(missing_attrs)}"
                    )
            n += 1
            if n >= max_lines:
                break
    if bad:
        print(f"validate-jsonl: FAIL ({len(bad)} issues, first 5):", file=sys.stderr)
        for b in bad[:5]:
            print(f"  {b}", file=sys.stderr)
        return 1
    print(f"validate-jsonl: OK ({n} rows checked)", file=sys.stderr)
    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    rc1 = _validate_queries_file(args.queries, args.reference_queries)
    rc2 = _validate_jsonl_shape(args.jsonl) if args.jsonl else 0
    return rc1 or rc2


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Google cluster dataset orchestrator.")
    sub = parser.add_subparsers(dest="command", required=True)

    pf = sub.add_parser("fetch", help="Download + cache trace subsample.")
    pf.add_argument("--year", choices=("2011", "2019"), required=True)
    pf.add_argument("--out-dir", type=Path, default=Path("/tmp/gct"))
    pf.add_argument("--max-rows", type=int, default=100_000)
    pf.set_defaults(func=cmd_fetch)

    pm = sub.add_parser("map", help="Map cached CSV -> OTLP-shaped JSONL.")
    pm.add_argument("--year", choices=("2011", "2019"))
    pm.add_argument("--in-dir", type=Path, default=Path("/tmp/gct"))
    pm.add_argument("--out", type=Path, required=True)
    pm.add_argument("--cardinality-cap", type=int, default=1000)
    pm.add_argument("--max-rows", type=int, default=0)
    pm.set_defaults(func=cmd_map)

    pr = sub.add_parser("replay", help="Stream JSONL into an OTLP/gRPC receiver.")
    pr.add_argument("--jsonl", type=Path, required=True)
    pr.add_argument(
        "--endpoint", default=os.environ.get("OTLP_ENDPOINT", ""),
        help="OTLP/gRPC endpoint, e.g. localhost:4317. "
             "If empty -> dry-run (count + summary only).",
    )
    pr.add_argument(
        "--pace-factor", type=float, default=0.0,
        help="Replay pace multiplier. 0 = as-fast-as-possible (default), "
             "1.0 = trace's natural pace, 10.0 = 10x faster.",
    )
    pr.add_argument("--max-lines", type=int, default=0,
                    help="Cap rows sent (0 = all).")
    pr.add_argument("--wall-clock-anchor", action="store_true",
                    help="Re-stamp EVERY datapoint at one wall-clock instant "
                         "(captured at replay start), collapsing the trace's "
                         "epoch-relative span into ONE warm window at now. "
                         "Required for recent-range PromQL ([Ns]) to intersect "
                         "the warm sketch windows. Timestamp-only; GT unchanged. "
                         "Replay must finish within one window_duration.")
    pr.add_argument("--anchor-span-s", type=float, default=0.0,
                    help="With --wall-clock-anchor, spread points across the last "
                         "N seconds (distinct ns each) instead of one instant, so "
                         "count-type sketches (value==1.0 per event) keep per-key "
                         "multiplicity. Keep N < window_duration so it stays one "
                         "window (e.g. 50 for a 60s window).")
    pr.add_argument("--dry-run", action="store_true",
                    help="Force dry-run even with --endpoint set.")
    pr.set_defaults(func=cmd_replay)

    pv = sub.add_parser("validate", help="Verify queries.json + (optionally) JSONL shape.")
    pv.add_argument("--queries", type=Path, default=ROOT / "queries.json")
    pv.add_argument(
        "--reference-queries", type=Path,
        default=REPO_ROOT / "deploy" / "scripts" / "queries-e2e.json",
        help="Reference queries.json from deploy/scripts/ to cross-check schema.",
    )
    pv.add_argument(
        "--jsonl", type=Path, default=None,
        help="Optional: JSONL produced by `map` to validate row shape.",
    )
    pv.set_defaults(func=cmd_validate)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
