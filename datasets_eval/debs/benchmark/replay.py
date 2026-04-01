from __future__ import annotations

import argparse
import csv
import queue
import threading
import time
from pathlib import Path

import grpc
import numpy as np
import pandas as pd
from opentelemetry.proto.collector.metrics.v1 import metrics_service_pb2_grpc
from opentelemetry.proto.collector.metrics.v1 import metrics_service_pb2
from opentelemetry.proto.common.v1 import common_pb2
from typing import List, Tuple

from common import METRIC_NAME, data_path, day_to_filename

_DEBS_TZ = "Europe/Berlin"

# Type alias for a batch of (price, time_unix_nano, symbol, exchange, sectype)
# Uses typing.List/Tuple for Python 3.7/3.8 compatibility (list[...] syntax
# is only subscriptable at runtime from Python 3.9+).
_Batch = List[Tuple[float, int, str, str, str]]


def build_otlp_export_request(
    batch_rows: _Batch,
) -> metrics_service_pb2.ExportMetricsServiceRequest:
    request = metrics_service_pb2.ExportMetricsServiceRequest()
    resource_metrics = request.resource_metrics.add()
    service_attr = resource_metrics.resource.attributes.add()
    service_attr.key = "service.name"
    service_attr.value.string_value = "debs-replay"
    scope_metrics = resource_metrics.scope_metrics.add()
    metric = scope_metrics.metrics.add()
    metric.name = METRIC_NAME
    for value, time_unix_nano, symbol, exchange, sectype in batch_rows:
        data_point = metric.gauge.data_points.add()
        data_point.as_double = float(value)
        data_point.time_unix_nano = int(time_unix_nano)
        symbol_value = common_pb2.AnyValue()
        symbol_value.string_value = symbol
        data_point.attributes.add(key="symbol", value=symbol_value)
        exchange_value = common_pb2.AnyValue()
        exchange_value.string_value = exchange
        data_point.attributes.add(key="exchange", value=exchange_value)
        sectype_value = common_pb2.AnyValue()
        sectype_value.string_value = sectype
        data_point.attributes.add(key="sectype", value=sectype_value)
    return request


def _parse_chunk_filtered(chunk: pd.DataFrame) -> pd.DataFrame:
    """Vectorized parse of a data_filtered CSV chunk.

    Processes the entire chunk in bulk using pandas/numpy operations — no
    per-row Python loops or per-row pd.to_datetime calls.  Returns a
    DataFrame with columns [Last, ts_ns, symbol, exchange, sectype].
    """
    chunk = chunk.copy()
    chunk["Date"] = chunk["Date"].astype(str).str.strip()
    chunk["Trading time"] = chunk["Trading time"].astype(str).str.strip()
    raw = pd.to_datetime(
        chunk["Date"] + " " + chunk["Trading time"],
        dayfirst=True,
        errors="coerce",
        format="mixed",
    )
    localized = raw.dt.tz_localize(_DEBS_TZ, ambiguous=True, nonexistent="shift_forward")
    chunk["ts_ns"] = (localized.astype("int64") // 1_000_000).astype(np.int64) * 1_000_000
    chunk["Last"] = pd.to_numeric(chunk["Last"], errors="coerce")
    chunk = chunk[chunk["ts_ns"] > 0].dropna(subset=["Last"])
    if chunk.empty:
        return chunk
    ids = chunk["ID"].astype(str).str.strip()
    parts = ids.str.rsplit(".", n=1, expand=True)
    chunk["symbol"] = parts.iloc[:, 0].fillna("").astype(str)
    chunk["exchange"] = (
        parts.iloc[:, 1].fillna("").astype(str) if parts.shape[1] > 1 else ""
    )
    chunk["sectype"] = chunk["SecType"].astype(str).str.strip()
    return chunk[["Last", "ts_ns", "symbol", "exchange", "sectype"]]


def _parse_chunk_full(chunk: pd.DataFrame) -> pd.DataFrame:
    """Vectorized parse of a full-feed CSV chunk.

    Same bulk-operation approach as _parse_chunk_filtered.  Missing Last
    values are filled with 0.0 to match the original full-feed semantics.
    Returns a DataFrame with columns [Last, ts_ns, symbol, exchange, sectype].
    """
    chunk = chunk.copy()
    raw = pd.to_datetime(
        chunk["Date"].astype(str).str.strip() + " " + chunk["Time"].astype(str).str.strip(),
        dayfirst=True,
        errors="coerce",
        format="mixed",
    )
    localized = raw.dt.tz_localize(_DEBS_TZ, ambiguous=True, nonexistent="shift_forward")
    chunk["ts_ns"] = (localized.astype("int64") // 1_000_000).astype(np.int64) * 1_000_000
    chunk["Last"] = pd.to_numeric(chunk["Last"], errors="coerce").fillna(0.0)
    chunk = chunk[chunk["ts_ns"] > 0]
    if chunk.empty:
        return chunk
    ids = chunk["ID"].astype(str).str.strip()
    parts = ids.str.rsplit(".", n=1, expand=True)
    chunk["symbol"] = parts.iloc[:, 0].fillna("").astype(str)
    chunk["exchange"] = (
        parts.iloc[:, 1].fillna("").astype(str) if parts.shape[1] > 1 else ""
    )
    chunk["sectype"] = chunk["SecType"].astype(str).str.strip()
    return chunk[["Last", "ts_ns", "symbol", "exchange", "sectype"]]


def iter_batches(
    csv_path: Path,
    dataset: str,
    chunksize: int,
    batch_size: int,
):
    """Yield complete batches of (price, time_unix_nano, symbol, exchange, sectype).

    Reads the CSV in chunks and processes each chunk entirely with vectorized
    pandas/numpy operations before yielding rows.  This avoids per-row Python
    overhead (iterrows, per-row pd.to_datetime) that was the main throughput
    bottleneck in the previous implementation.
    """
    if dataset == "data_filtered":
        columns = ["ID", "SecType", "Date", "Last", "Trading time"]
        parse_fn = _parse_chunk_filtered
    else:
        columns = ["ID", "SecType", "Date", "Time", "Last"]
        parse_fn = _parse_chunk_full

    pending: _Batch = []

    for chunk in pd.read_csv(
        csv_path,
        comment="#",
        index_col=False,
        usecols=lambda c: c in columns,
        chunksize=chunksize,
        dtype=object,
        low_memory=False,
    ):
        parsed = parse_fn(chunk)
        if parsed.empty:
            continue

        # Convert to numpy arrays once per chunk — plain Python loop over
        # numpy scalars is ~20–50× faster than iterrows over a DataFrame.
        prices = parsed["Last"].to_numpy(dtype=np.float64)
        ts_ns_arr = parsed["ts_ns"].to_numpy(dtype=np.int64)
        symbols = parsed["symbol"].to_numpy(dtype=object)
        exchanges = parsed["exchange"].to_numpy(dtype=object)
        sectypes = parsed["sectype"].to_numpy(dtype=object)
        n = len(parsed)

        for i in range(n):
            pending.append((
                float(prices[i]),
                int(ts_ns_arr[i]),
                str(symbols[i]),
                str(exchanges[i]),
                str(sectypes[i]),
            ))
            if len(pending) >= batch_size:
                yield pending
                pending = []

    if pending:
        yield pending


def sleep_until_deadline(deadline_perf: float) -> None:
    while True:
        now = time.perf_counter()
        if now >= deadline_perf:
            return
        remaining = deadline_perf - now
        time.sleep(min(0.05, remaining))


def append_throughput_row(
    results_dir: Path,
    query_label: str,
    day_label: str,
    replay_mode: str,
    speed_factor: float,
    dataset_name: str,
    total_events: int,
    elapsed_seconds: float,
    export_count: int,
) -> None:
    events_per_second = total_events / elapsed_seconds if elapsed_seconds > 0 else 0.0
    throughput_path = results_dir / "throughput.csv"
    header = (
        "query,day,replay_mode,speed_factor,dataset,total_events,elapsed_s,"
        "events_per_sec,export_count\n"
    )
    if not throughput_path.is_file():
        throughput_path.write_text(header, encoding="utf-8")
    with open(throughput_path, "a", newline="", encoding="utf-8") as throughput_file:
        writer = csv.writer(throughput_file)
        writer.writerow(
            [
                query_label,
                day_label,
                replay_mode,
                f"{speed_factor:g}",
                dataset_name,
                total_events,
                f"{elapsed_seconds:.6f}",
                f"{events_per_second:.6f}",
                export_count,
            ]
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay DEBS CSV as OTLP gauge metrics.")
    parser.add_argument("--dataset", choices=("data", "data_filtered"), required=True)
    parser.add_argument(
        "--days",
        default="08-11-21,09-11-21,10-11-21,11-11-21,12-11-21",
    )
    parser.add_argument("--mode", choices=("max", "paced", "scaled"), default="max")
    parser.add_argument("--speed-factor", type=float, default=10000.0)
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--endpoint", default="localhost:4317")
    parser.add_argument("--chunksize", type=int, default=200_000)
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "results",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=-1,
        help="Log every N successful Export calls after the first 3; 0 disables; "
        "-1 (default) picks N from batch_size so lines appear about every ~50k points.",
    )
    parser.add_argument(
        "--queue-depth",
        type=int,
        default=32,
        help="Max number of pre-built batches buffered between the reader and sender "
        "threads. Higher values trade memory for smoother throughput under bursty gRPC "
        "latency. Default: 32.",
    )
    parser.add_argument(
        "--query",
        default="",
        help="Query id for throughput.csv tagging (e.g. Q1)",
    )
    parser.add_argument(
        "--day",
        default="",
        help="Trading day tag for throughput.csv (e.g. 08-11-21)",
    )
    args = parser.parse_args()

    pe = args.progress_every
    if pe == 0:
        pass  # disabled
    elif pe < 0:
        args.progress_every = max(5, min(50, 50_000 // max(1, args.batch_size)))
    else:
        args.progress_every = max(1, pe)

    wall_start_time = time.perf_counter()
    args.results_dir.mkdir(parents=True, exist_ok=True)
    send_times_path = args.results_dir / "send_times.csv"
    data_directory = data_path(args.dataset)
    days_list = [part.strip() for part in args.days.split(",") if part.strip()]
    channel = grpc.insecure_channel(
        args.endpoint,
        options=[("grpc.max_send_message_length", 64 * 1024 * 1024)],
    )
    stub = metrics_service_pb2_grpc.MetricsServiceStub(channel)

    first_event_time_ns: int | None = None
    replay_start_perf: float | None = None

    send_times_file = open(send_times_path, "w", newline="", encoding="utf-8")
    send_times_writer = csv.writer(send_times_file)
    send_times_writer.writerow(["emit_wall_ns", "event_time_ns"])

    # These are mutated only by the sender thread (after join, read by main thread).
    total_events = 0
    export_count = 0

    print(
        "replay start",
        f"dataset={args.dataset}",
        f"mode={args.mode}",
        f"speed_factor={args.speed_factor}",
        f"endpoint={args.endpoint}",
        f"metric={METRIC_NAME}",
        f"batch_size={args.batch_size}",
        f"days={days_list}",
        flush=True,
    )
    if args.progress_every > 0:
        approx_pts = args.progress_every * args.batch_size
        print(
            "replay",
            f"progress lines every {args.progress_every} exports (~{approx_pts} points); "
            "silence here does not mean OTLP is idle.",
            flush=True,
        )

    # ------------------------------------------------------------------
    # Producer-consumer pipeline
    #
    # Main thread  : reads CSV chunks, vectorizes, enforces replay timing,
    #                enqueues complete batches.
    # Sender thread: dequeues batches, builds OTLP requests, calls gRPC.
    #
    # This overlaps CSV I/O + pandas parsing with gRPC network time so
    # neither side is idle waiting for the other.
    # ------------------------------------------------------------------
    send_queue: queue.Queue[_Batch | None] = queue.Queue(maxsize=args.queue_depth)
    sender_errors: list[Exception] = []

    def sender_worker() -> None:
        nonlocal total_events, export_count
        while True:
            batch = send_queue.get()
            if batch is None:  # sentinel — clean shutdown
                send_queue.task_done()
                break
            try:
                wall_ns = time.time_ns()
                request = build_otlp_export_request(batch)
                batch_len = len(batch)
                try:
                    stub.Export(request)
                except grpc.RpcError as rpc_error:
                    print("export failed", rpc_error.code(), rpc_error.details(), flush=True)
                    raise
                for row in batch:
                    send_times_writer.writerow([wall_ns, row[1]])
                total_events += batch_len
                export_count += 1
                if export_count <= 3:
                    print(
                        "export ok",
                        f"batch_points={batch_len}",
                        f"total_points={total_events}",
                        f"export_n={export_count}",
                        flush=True,
                    )
                elif args.progress_every > 0 and export_count % args.progress_every == 0:
                    elapsed = time.perf_counter() - wall_start_time
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
        for day in days_list:
            file_path = data_directory / day_to_filename(day)
            if not file_path.is_file():
                print("skip missing", file_path, flush=True)
                continue
            print("file", file_path, flush=True)

            for batch in iter_batches(file_path, args.dataset, args.chunksize, args.batch_size):
                # Bail early if the sender thread failed.
                if sender_errors:
                    raise sender_errors[0]

                # Timing: sleep to the deadline of the LAST event in this batch.
                # Semantically identical to per-row sleeping but much cheaper
                # because we avoid one sleep call per row; early events in the
                # batch are sent at most (batch_size / event_rate) seconds early,
                # which is negligible at speed_factor=10000.
                last_event_time_ns = batch[-1][1]
                if first_event_time_ns is None:
                    first_event_time_ns = batch[0][1]
                    replay_start_perf = time.perf_counter()
                assert replay_start_perf is not None

                if args.mode == "paced":
                    sleep_until_deadline(
                        replay_start_perf
                        + (last_event_time_ns - first_event_time_ns) / 1e9
                    )
                elif args.mode == "scaled":
                    sleep_until_deadline(
                        replay_start_perf
                        + (last_event_time_ns - first_event_time_ns) / 1e9 / args.speed_factor
                    )

                # Put with a short timeout loop so a crashed sender thread
                # doesn't leave the main thread blocked on a full queue.
                while True:
                    if sender_errors:
                        raise sender_errors[0]
                    try:
                        send_queue.put(batch, timeout=1.0)
                        break
                    except queue.Full:
                        continue
    finally:
        # Always send the sentinel so the sender thread can exit cleanly.
        send_queue.put(None)
        sender_thread.join()

    if sender_errors:
        raise sender_errors[0]

    send_times_file.close()
    elapsed_total = time.perf_counter() - wall_start_time
    overall_rate = total_events / elapsed_total if elapsed_total > 0 else 0.0

    query_tag = args.query or "unknown"
    if len(days_list) == 1:
        day_tag = args.day or days_list[0]
    else:
        day_tag = args.day or "multi"

    append_throughput_row(
        args.results_dir,
        query_tag,
        day_tag,
        args.mode,
        args.speed_factor,
        args.dataset,
        total_events,
        elapsed_total,
        export_count,
    )

    print(
        "replay done",
        f"total_points={total_events}",
        f"exports={export_count}",
        f"send_times={send_times_path}",
        f"elapsed_s={elapsed_total:.2f}",
        f"points_per_s={overall_rate:.0f}",
        flush=True,
    )


if __name__ == "__main__":
    main()
