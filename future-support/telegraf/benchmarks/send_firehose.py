#!/usr/bin/env python3
"""
Firehose generator for the max-throughput Prometheus client benchmark.

Streams Influx line protocol over TCP to the Telegraf socket listener
configured at tcp://localhost:8094.
"""

from __future__ import annotations

import argparse
import multiprocessing as mp
import socket
import sys
import time

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8094
DEFAULT_BATCH_SIZE = 5000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream Influx line protocol to a socket_listener firehose"
    )
    parser.add_argument("--host", default=DEFAULT_HOST, help="Telegraf host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Telegraf port (default: 8094)")
    parser.add_argument(
        "--measurement",
        default="firehose",
        help="Measurement name to emit (default: firehose)",
    )
    parser.add_argument(
        "--tags",
        default="source=python_firehose",
        help='Comma-separated tag assignments (default: "source=python_firehose")',
    )
    parser.add_argument(
        "--fields",
        default="value=1i",
        help='Comma-separated field assignments, e.g. "value=1i,temp=42". Defaults to "value=1i".',
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=0.0,
        help="Optional target lines/sec per process. Omit or set 0 for best-effort firehose.",
    )
    parser.add_argument(
        "--processes",
        type=int,
        default=1,
        help="Number of worker processes to spawn (default: 1)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Number of identical lines to send per socket write (default: {DEFAULT_BATCH_SIZE})",
    )
    return parser.parse_args()


def build_line_template(
    measurement: str, tags: str, field_assignments: list[str], timestamp: int
) -> bytes:
    field_pairs = ",".join(field_assignments)
    line = f"{measurement},{tags} {field_pairs} {timestamp}\n"
    return line.encode("utf-8")


def run_worker(args: argparse.Namespace, worker_id: int) -> None:
    if args.batch_size <= 0:
        print("Batch size must be a positive integer.", file=sys.stderr)
        return

    field_assignments = [field.strip() for field in args.fields.split(",") if field.strip()]
    if not field_assignments:
        print("At least one field assignment is required.", file=sys.stderr)
        return
    if any("=" not in assignment for assignment in field_assignments):
        print(
            "Field assignments must include '=' (e.g. value=1i,temp=42).",
            file=sys.stderr,
        )
        return

    addr = (args.host, args.port)
    prefix = f"[worker {worker_id}]"
    print(f"{prefix} Connecting to {addr[0]}:{addr[1]}...", file=sys.stderr)
    sock = socket.create_connection(addr)
    sent = 0
    start_time = time.time()
    next_report = start_time + 5
    throttle = args.rate > 0
    lines_per_send = args.batch_size
    interval = lines_per_send / args.rate if throttle else 0.0

    timestamp = time.time_ns()
    template = build_line_template(args.measurement, args.tags, field_assignments, timestamp)
    batch = template * args.batch_size

    try:
        while True:
            sock.sendall(batch)
            sent += lines_per_send

            now = time.time()
            if now >= next_report:
                elapsed = now - start_time
                rate = sent / max(elapsed, 1e-6)
                print(
                    f"{prefix} Sent {sent:,} lines (~{rate:.0f} lines/sec avg over {elapsed:.1f}s)",
                    file=sys.stderr,
                )
                next_report = now + 5
            if throttle:
                time.sleep(interval)
    except KeyboardInterrupt:
        print(f"\n{prefix} Stopped by user.", file=sys.stderr)
    finally:
        sock.close()
        total_elapsed = time.time() - start_time
        avg_rate = sent / max(total_elapsed, 1e-6)
        print(
            f"{prefix} Total lines sent: {sent:,} in {total_elapsed:.1f}s "
            f"(avg {avg_rate:.0f} lines/sec)",
            file=sys.stderr,
        )


def main() -> None:
    args = parse_args()
    if args.processes <= 1:
        run_worker(args, worker_id=0)
        return

    processes = []
    try:
        for i in range(args.processes):
            p = mp.Process(target=run_worker, args=(args, i))
            p.start()
            processes.append(p)

        for p in processes:
            p.join()
    except KeyboardInterrupt:
        print("\nStopping all workers...", file=sys.stderr)
        for p in processes:
            p.terminate()
    finally:
        for p in processes:
            if p.is_alive():
                p.join()


if __name__ == "__main__":
    main()
