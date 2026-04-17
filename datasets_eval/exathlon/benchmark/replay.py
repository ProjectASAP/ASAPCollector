from __future__ import annotations

"""Replay Exathlon telemetry CSVs as OTLP gauge metrics.

Each exathlon CSV row contains a Unix-epoch timestamp column ``t`` and ~2 283
wide metric columns named ``{entity}_{metric_base}_{aggregation}``.  The
replay performs a wide-to-narrow pivot: every non-sentinel, non-NaN column
value in a row becomes one OTLP gauge data point whose attributes are
``entity``, ``metric_base``, and ``aggregation``.

Sentinel value (-1.0) rows represent inactive / not-yet-initialised metrics
and are skipped.
"""

import argparse
import csv
import queue
import threading
import time
from pathlib import Path
from typing import List, Tuple

import grpc
import numpy as np
import pandas as pd
from opentelemetry.proto.collector.metrics.v1 import metrics_service_pb2_grpc
from opentelemetry.proto.collector.metrics.v1 import metrics_service_pb2
from opentelemetry.proto.common.v1 import common_pb2

from common import (
    METRIC_NAME,
    SENTINEL_VALUE,
    build_column_metadata,
    file_csv_path,
    file_tag_safe,
)

# Each element: (value, time_unix_nano, entity, metric_base, aggregation)
_Point = Tuple[float, int, str, str, str]
_Batch = List[_Point]
_QueuedBatch = tuple[_Batch, int]


# ---------------------------------------------------------------------------
# OTLP request builder
# ---------------------------------------------------------------------------

def build_otlp_export_request(
    batch: _Batch,
) -> metrics_service_pb2.ExportMetricsServiceRequest:
    request = metrics_service_pb2.ExportMetricsServiceRequest()
    resource_metrics = request.resource_metrics.add()
    svc_attr = resource_metrics.resource.attributes.add()
    svc_attr.key = "service.name"
    svc_attr.value.string_value = "exathlon-replay"
    scope_metrics = resource_metrics.scope_metrics.add()
    metric = scope_metrics.metrics.add()
    metric.name = METRIC_NAME

    for value, time_unix_nano, entity, metric_base, aggregation in batch:
        dp = metric.gauge.data_points.add()
        dp.as_double = float(value)
        dp.time_unix_nano = int(time_unix_nano)

        av = common_pb2.AnyValue()
        av.string_value = entity
        dp.attributes.add(key="entity", value=av)

        bv = common_pb2.AnyValue()
        bv.string_value = metric_base
        dp.attributes.add(key="metric_base", value=bv)

        cv = common_pb2.AnyValue()
        cv.string_value = aggregation
        dp.attributes.add(key="aggregation", value=cv)

    return request


# ---------------------------------------------------------------------------
# CSV iteration
# ---------------------------------------------------------------------------

def iter_batches(
    csv_path: Path,
    chunksize: int,
    batch_size: int,
    increment_cols: set[str] | None = None,
):
    """Yield batches of (value, time_unix_nano, entity, metric_base, aggregation).

    Performs the wide-to-narrow pivot inline: each non-sentinel, non-NaN
    column value in a row becomes one tuple in the batch.

    For columns listed in *increment_cols*, the emitted value is the
    tick-to-tick delta rather than the raw accumulator total.  Negative deltas
    (JVM restarts) and the first valid observation after a sentinel gap are
    silently dropped — they do not represent real events.
    """
    inc_cols: set[str] = set(increment_cols or [])

    # Pre-parse all column names from the header.
    header_df = pd.read_csv(csv_path, nrows=0)
    col_meta = build_column_metadata(list(header_df.columns))
    metric_cols = list(col_meta.keys())

    if not metric_cols:
        return

    pending: _Batch = []
    metric_info = [(col, *col_meta[col]) for col in metric_cols]
    # Per-column last non-sentinel value for increment computation.
    prev_val: dict[str, float] = {}

    for chunk in pd.read_csv(
        csv_path,
        usecols=["t"] + metric_cols,
        chunksize=chunksize,
        dtype=object,
        low_memory=False,
    ):
        ts_raw = pd.to_numeric(chunk["t"], errors="coerce")
        ts_ns = (ts_raw * 1_000_000_000).astype("Int64")

        ts_arr = ts_ns.to_numpy(dtype=np.float64)
        col_values = {
            col: pd.to_numeric(chunk[col], errors="coerce").to_numpy(dtype=np.float64)
            for col in metric_cols
        }

        # Keep event-time order stable inside each batch. This avoids mixing far
        # apart CSV timestamps in a single export, which corrupts lag diagnostics.
        for i in range(len(ts_arr)):
            t = ts_arr[i]
            if np.isnan(t):
                continue
            t_ns = int(t)
            for col, entity, mb, agg in metric_info:
                v = col_values[col][i]
                if np.isnan(v) or v == SENTINEL_VALUE:
                    if col in inc_cols:
                        prev_val.pop(col, None)  # reset boundary on sentinel
                    continue

                if col in inc_cols:
                    prev = prev_val.get(col)
                    prev_val[col] = v
                    if prev is None:
                        continue  # first valid tick — no predecessor to diff against
                    delta = v - prev
                    if delta < 0:
                        continue  # JVM restart reset — not a real event
                    emit_v = delta
                else:
                    emit_v = v

                pending.append((float(emit_v), t_ns, entity, mb, agg))
                if len(pending) >= batch_size:
                    yield pending
                    pending = []

    if pending:
        yield pending


# ---------------------------------------------------------------------------
# Pacing helper
# ---------------------------------------------------------------------------

def sleep_until_deadline(deadline_perf: float) -> None:
    while True:
        now = time.perf_counter()
        if now >= deadline_perf:
            return
        remaining = deadline_perf - now
        time.sleep(min(0.05, remaining))


# ---------------------------------------------------------------------------
# Throughput accounting
# ---------------------------------------------------------------------------

def append_throughput_row(
    results_dir: Path,
    query_label: str,
    file_label: str,
    replay_mode: str,
    speed_factor: float,
    total_events: int,
    elapsed_seconds: float,
    export_count: int,
) -> None:
    events_per_second = total_events / elapsed_seconds if elapsed_seconds > 0 else 0.0
    throughput_path = results_dir / "throughput.csv"
    header = (
        "query,file,replay_mode,speed_factor,total_events,elapsed_s,"
        "events_per_sec,export_count\n"
    )
    if not throughput_path.is_file():
        throughput_path.write_text(header, encoding="utf-8")
    with open(throughput_path, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([
            query_label,
            file_label,
            replay_mode,
            f"{speed_factor:g}",
            total_events,
            f"{elapsed_seconds:.6f}",
            f"{events_per_second:.6f}",
            export_count,
        ])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replay Exathlon CSV files as OTLP gauge metrics (wide-to-narrow pivot)."
    )
    parser.add_argument(
        "--files",
        default="app1/1_0_10000_17,app5/5_0_50000_38,app9/9_0_100000_1",
        help="Comma-separated file tags (e.g. app1/1_0_10000_17,app5/5_0_50000_38).",
    )
    parser.add_argument("--mode", choices=("max", "paced", "scaled"), default="max")
    parser.add_argument("--speed-factor", type=float, default=10000.0)
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50_000,
        help="OTLP export batch size in data points (default 50 000; ~22 CSV rows).",
    )
    parser.add_argument("--endpoint", default="localhost:4317")
    parser.add_argument("--chunksize", type=int, default=200,
                        help="Pandas CSV read chunksize (rows per chunk).")
    parser.add_argument(
        "--max-event-minutes",
        type=int,
        default=0,
        help="Stop after this many minutes of event time from the first sample (0 = no cutoff).",
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "results",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=-1,
        help="Log progress every N exports (-1 = auto, 0 = disabled).",
    )
    parser.add_argument(
        "--queue-depth",
        type=int,
        default=32,
    )
    parser.add_argument("--query", default="", help="Query tag for throughput.csv (e.g. Q1).")
    parser.add_argument("--file", default="", help="Single file tag for throughput.csv (e.g. app1/1_0_10000_17).")
    parser.add_argument(
        "--increment-cols",
        default="",
        help="Comma-separated column names to emit as tick-to-tick deltas instead of raw values.",
    )
    args = parser.parse_args()

    pe = args.progress_every
    if pe < 0:
        args.progress_every = max(5, min(50, 200_000 // max(1, args.batch_size)))
    else:
        args.progress_every = max(1, pe) if pe else 0

    wall_start = time.perf_counter()
    args.results_dir.mkdir(parents=True, exist_ok=True)
    send_times_path = args.results_dir / "send_times.csv"
    export_diag_path = args.results_dir / "export_diagnostics.csv"

    files_list = [f.strip() for f in args.files.split(",") if f.strip()]
    channel = grpc.insecure_channel(
        args.endpoint,
        options=[
            ("grpc.max_send_message_length", 64 * 1024 * 1024),
            ("grpc.max_receive_message_length", 64 * 1024 * 1024),
        ],
    )
    stub = metrics_service_pb2_grpc.MetricsServiceStub(channel)

    first_event_ns: int | None = None
    replay_start_perf: float | None = None
    cutoff_ns: int | None = None

    send_times_file = open(send_times_path, "w", newline="", encoding="utf-8")
    send_times_writer = csv.writer(send_times_file)
    send_times_writer.writerow(["emit_wall_ns", "event_time_ns"])
    send_times_file.flush()
    export_diag_file = open(export_diag_path, "w", newline="", encoding="utf-8")
    export_diag_writer = csv.writer(export_diag_file)
    export_diag_writer.writerow([
        "export_n",
        "enqueue_wall_ns",
        "emit_wall_ns",
        "queue_wait_ms",
        "batch_points",
        "event_min_ns",
        "event_max_ns",
        "event_span_ms",
        "event_regressions",
    ])
    export_diag_file.flush()

    total_events = 0
    export_count = 0

    print(
        "replay start",
        f"files={files_list}",
        f"mode={args.mode}",
        f"speed_factor={args.speed_factor}",
        f"endpoint={args.endpoint}",
        f"metric={METRIC_NAME}",
        f"batch_size={args.batch_size}",
        flush=True,
    )

    send_queue: queue.Queue[_QueuedBatch | None] = queue.Queue(maxsize=args.queue_depth)
    sender_errors: list[Exception] = []

    def sender_worker() -> None:
        nonlocal total_events, export_count
        while True:
            queued = send_queue.get()
            if queued is None:
                send_queue.task_done()
                break
            try:
                pending: list[_QueuedBatch] = [queued]
                while pending:
                    current, enqueue_wall_ns = pending.pop()
                    wall_ns = time.time_ns()
                    request = build_otlp_export_request(current)
                    batch_len = len(current)
                    try:
                        stub.Export(request)
                    except grpc.RpcError as rpc_error:
                        code = rpc_error.code()
                        if code == grpc.StatusCode.RESOURCE_EXHAUSTED and batch_len > 1:
                            mid = batch_len // 2
                            left = current[:mid]
                            right = current[mid:]
                            # Depth-first retry with smaller payloads.
                            pending.append((right, enqueue_wall_ns))
                            pending.append((left, enqueue_wall_ns))
                            print(
                                "export split",
                                f"orig_points={batch_len}",
                                f"left={len(left)}",
                                f"right={len(right)}",
                                flush=True,
                            )
                            continue
                        print("export failed", code, rpc_error.details(), flush=True)
                        raise
                    event_min_ns = min(row[1] for row in current)
                    event_max_ns = max(row[1] for row in current)
                    # One row per export: wall time of the Export() call vs. the
                    # last event timestamp in this batch.  Writing per-point was
                    # wrong: every point in a batch shares the same emit_wall_ns,
                    # making np.diff(emit_ns) = 0 inside each batch and producing
                    # spurious zero inter-arrival times and a negative lag drift.
                    send_times_writer.writerow([wall_ns, event_max_ns])
                    regressions = 0
                    prev_event = current[0][1]
                    for row in current[1:]:
                        if row[1] < prev_event:
                            regressions += 1
                        prev_event = row[1]

                    total_events += batch_len
                    export_count += 1
                    export_diag_writer.writerow([
                        export_count,
                        enqueue_wall_ns,
                        wall_ns,
                        f"{(wall_ns - enqueue_wall_ns) / 1e6:.6f}",
                        batch_len,
                        event_min_ns,
                        event_max_ns,
                        f"{(event_max_ns - event_min_ns) / 1e6:.6f}",
                        regressions,
                    ])
                    if export_count <= 3:
                        print(
                            "export ok",
                            f"batch_points={batch_len}",
                            f"total_points={total_events}",
                            f"export_n={export_count}",
                            flush=True,
                        )
                    elif args.progress_every and export_count % args.progress_every == 0:
                        elapsed = time.perf_counter() - wall_start
                        rate = total_events / elapsed if elapsed > 0 else 0.0
                        print(
                            "progress",
                            f"total_points={total_events}",
                            f"exports={export_count}",
                            f"elapsed_s={elapsed:.1f}",
                            f"points_per_s={rate:.0f}",
                            flush=True,
                        )
            except Exception as exc:
                sender_errors.append(exc)
                send_queue.task_done()
                break
            send_queue.task_done()

    sender_thread = threading.Thread(target=sender_worker, daemon=True, name="otlp-sender")
    sender_thread.start()

    try:
        for file_tag in files_list:
            csv_path = file_csv_path(file_tag)
            if not csv_path.is_file():
                print(f"skip missing {csv_path}", flush=True)
                continue
            print(f"file {csv_path}", flush=True)

            inc_cols = {c.strip() for c in args.increment_cols.split(",") if c.strip()}
            for batch in iter_batches(csv_path, args.chunksize, args.batch_size, inc_cols):
                if sender_errors:
                    raise sender_errors[0]

                if args.max_event_minutes > 0:
                    if cutoff_ns is None:
                        cutoff_ns = batch[0][1] + args.max_event_minutes * 60 * 1_000_000_000
                    batch = [r for r in batch if r[1] <= cutoff_ns]
                    if not batch:
                        continue

                last_event_ns = batch[-1][1]
                if first_event_ns is None:
                    first_event_ns = batch[0][1]
                    replay_start_perf = time.perf_counter()
                assert replay_start_perf is not None

                if args.mode == "paced":
                    sleep_until_deadline(
                        replay_start_perf
                        + (last_event_ns - first_event_ns) / 1e9
                    )
                elif args.mode == "scaled":
                    sleep_until_deadline(
                        replay_start_perf
                        + (last_event_ns - first_event_ns) / 1e9 / args.speed_factor
                    )

                while True:
                    if sender_errors:
                        raise sender_errors[0]
                    try:
                        send_queue.put((batch, time.time_ns()), timeout=1.0)
                        break
                    except queue.Full:
                        continue
    finally:
        send_queue.put(None)
        sender_thread.join()

    if sender_errors:
        raise sender_errors[0]

    send_times_file.close()
    export_diag_file.close()
    elapsed_total = time.perf_counter() - wall_start
    overall_rate = total_events / elapsed_total if elapsed_total > 0 else 0.0

    query_tag = args.query or "unknown"
    if len(files_list) == 1:
        file_label = args.file or file_tag_safe(files_list[0])
    else:
        file_label = args.file or "multi"

    append_throughput_row(
        args.results_dir,
        query_tag,
        file_label,
        args.mode,
        args.speed_factor,
        total_events,
        elapsed_total,
        export_count,
    )

    print(
        "replay done",
        f"total_points={total_events}",
        f"exports={export_count}",
        f"send_times={send_times_path}",
        f"export_diagnostics={export_diag_path}",
        f"elapsed_s={elapsed_total:.2f}",
        f"points_per_s={overall_rate:.0f}",
        flush=True,
    )


if __name__ == "__main__":
    main()
