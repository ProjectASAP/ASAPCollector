#!/usr/bin/env python3
"""
Firehose generator for the max-throughput Prometheus client benchmark.

Streams Influx line protocol over TCP to the Telegraf socket listener
configured at tcp://localhost:8094.
"""

from __future__ import annotations

import argparse
import multiprocessing as mp
import random
import socket
import sys
import time

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8094


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
        default="value",
        help='Comma-separated field names to emit random floats for (default: "value")',
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
    return parser.parse_args()


def build_line(measurement: str, tags: str, field_names: list[str]) -> str:
    ts = time.time_ns()
    field_pairs = ",".join(f"{name}={random.random():.6f}" for name in field_names)
    return f"{measurement},{tags} {field_pairs} {ts}\n"


def run_worker(args: argparse.Namespace, worker_id: int) -> None:
    field_names = [name.strip() for name in args.fields.split(",") if name.strip()]
    if not field_names:
        print("At least one field name is required.", file=sys.stderr)
        return

    addr = (args.host, args.port)
    prefix = f"[worker {worker_id}]"
    print(f"{prefix} Connecting to {addr[0]}:{addr[1]}...", file=sys.stderr)
    sock = socket.create_connection(addr)
    sent = 0
    start_time = time.time()
    next_report = start_time + 5
    throttle = args.rate > 0
    interval = 1.0 / args.rate if throttle else 0.0

    try:
        while True:
            line = build_line(args.measurement, args.tags, field_names)
            sock.sendall(line.encode("utf-8"))
            sent += 1

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
