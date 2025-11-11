#!/usr/bin/env python3
"""
Measure the firehose generator's maximum rate without involving Telegraf.

Spins up a lightweight Python TCP server that discards everything, then reuses
`send_firehose.py` to blast data into it.
"""

from __future__ import annotations

import argparse
import socket
import threading

from send_firehose import parse_args as firehose_parse_args, main as firehose_main  # type: ignore


def start_null_server(host: str, port: int) -> threading.Thread:
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((host, port))
    server_sock.listen()

    def serve() -> None:
        with server_sock:
            while True:
                conn, _addr = server_sock.accept()
                threading.Thread(target=drain_conn, args=(conn,), daemon=True).start()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    return thread


def drain_conn(conn: socket.socket) -> None:
    with conn:
        while True:
            data = conn.recv(65536)
            if not data:
                break


def main() -> None:
    args = firehose_parse_args()
    print(f"Starting null sink on {args.host}:{args.port} ...")
    start_null_server(args.host, args.port)
    firehose_main()


if __name__ == "__main__":
    main()
