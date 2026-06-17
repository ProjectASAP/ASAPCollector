#!/usr/bin/env python3
"""loadgen.py — constant-rate OTLP/gRPC load generator for the edge soak.

Reuses the OTLP/gRPC encoding from datasets_eval/google_cluster/run.py but
drives a SUSTAINED CONSTANT rate (points/sec) by looping over the real
Google-2019 mapped trace JSONL indefinitely, rather than pacing to the
trace's own (very sparse) timestamps. Each loop re-bases timestamps to
"now" so the edge keeps closing windows on fresh data and the warm store
keeps turning over — exactly the steady-ingest condition a leak-slope
soak needs.

Usage:
  loadgen.py --jsonl /tmp/gct-otlp.jsonl --endpoint 127.0.0.1:4317 \
             --rate 5000 --duration 1800
"""
from __future__ import annotations

import argparse
import json
import sys
import time

import grpc
from opentelemetry.proto.collector.metrics.v1 import metrics_service_pb2
from opentelemetry.proto.collector.metrics.v1 import metrics_service_pb2_grpc
from opentelemetry.proto.common.v1 import common_pb2


def load_rows(path: str, cap: int):
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
            if cap and len(rows) >= cap:
                break
    return rows


def build_req(rows):
    req = metrics_service_pb2.ExportMetricsServiceRequest()
    rm = req.resource_metrics.add()
    a = rm.resource.attributes.add()
    a.key = "service.name"
    a.value.string_value = "google-cluster-soak"
    sm = rm.scope_metrics.add()
    per_metric = {}
    for r in rows:
        per_metric.setdefault(r["metric"], []).append(r)
    now_ns = time.time_ns()
    for mname, mrows in sorted(per_metric.items()):
        metric = sm.metrics.add()
        metric.name = mname
        for r in mrows:
            dp = metric.gauge.data_points.add()
            dp.as_double = float(r["value"])
            dp.time_unix_nano = now_ns
            for k, v in sorted(r["attributes"].items()):
                av = common_pb2.AnyValue()
                av.string_value = str(v)
                dp.attributes.add(key=k, value=av)
    return req


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--jsonl", required=True)
    ap.add_argument("--endpoint", default="127.0.0.1:4317")
    ap.add_argument("--rate", type=float, default=5000.0,
                    help="target data points / second")
    ap.add_argument("--duration", type=float, default=1800.0)
    ap.add_argument("--batch", type=int, default=500)
    ap.add_argument("--cap", type=int, default=200000,
                    help="max rows loaded from trace into the loop buffer")
    args = ap.parse_args()

    rows = load_rows(args.jsonl, args.cap)
    if not rows:
        print("loadgen: no rows", file=sys.stderr)
        return 2
    print(f"loadgen: loaded {len(rows)} trace rows; target rate "
          f"{args.rate} pts/s for {args.duration}s", file=sys.stderr)

    ch = grpc.insecure_channel(
        args.endpoint,
        options=[("grpc.max_send_message_length", 1 << 30)])
    stub = metrics_service_pb2_grpc.MetricsServiceStub(ch)

    start = time.monotonic()
    sent = 0
    idx = 0
    batch_period = args.batch / args.rate  # seconds per batch to hit rate
    next_send = start
    last_report = start
    errors = 0
    while True:
        now = time.monotonic()
        if now - start >= args.duration:
            break
        # take the next `batch` rows, wrapping
        chunk = []
        for _ in range(args.batch):
            chunk.append(rows[idx])
            idx = (idx + 1) % len(rows)
        req = build_req(chunk)
        try:
            stub.Export(req, timeout=15.0)
            sent += len(chunk)
        except grpc.RpcError as exc:
            errors += 1
            if errors <= 5:
                print(f"loadgen: export error: {exc.code()}", file=sys.stderr)
        next_send += batch_period
        sleep = next_send - time.monotonic()
        if sleep > 0:
            time.sleep(sleep)
        if now - last_report >= 30:
            el = now - start
            print(f"loadgen: t={el:6.0f}s sent={sent} "
                  f"({sent/el:.0f} pts/s) errors={errors}", file=sys.stderr)
            last_report = now
    el = time.monotonic() - start
    print(f"loadgen: done. sent={sent} in {el:.0f}s "
          f"({sent/el:.0f} pts/s) errors={errors}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
