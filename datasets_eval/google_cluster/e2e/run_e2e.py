#!/usr/bin/env python3
"""End-to-end validation orchestrator for the google_cluster trace.

Drives the FULL backend query path:

    mapped OTLP JSONL  --replay-->  fused asap_edge agent (:4317)
                       --windows close + ship-->  data plane
    each query.metricsql  --query-->  data plane asap_query (:9091)
                       --vs-->  exact offline pandas-free GT (gt_eval)

and emits a per-family pass/fail report (compare).

Assumes the stack is already up (e.g. via
`deploy/mvp-multinode/scripts/run_demo.sh up gctrace`); use `--bring-up`
to shell that out first. The replay reuses run.py's OTLP/gRPC sender.

Subcommands:
  all     replay -> wait -> query -> gt -> compare -> report
  query   query-only (stack already fed): query -> gt -> compare

Determinism: default replay mode pushes ALL rows as fast as possible,
then we query the single sealed warm window and compute GT over all
replayed rows (so warm and GT see the identical sample set). The
agent/window timing gotchas from the plan are encoded in WARMUP_S.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
DATASET_ROOT = ROOT.parent
sys.path.insert(0, str(ROOT))

import gt_eval          # noqa: E402
import compare as cmp   # noqa: E402
import query_client     # noqa: E402


def _normalize_warm(qspec: dict[str, Any], qr: "query_client.QueryResult") -> Any:
    """Map a Prometheus-style response to the shape compare.py expects.

    scalar family -> float; grouped/topk family -> {key: float} keyed by
    the gt spec's grouping label (`by[0]` or `key_label`).
    """
    gt = qspec.get("gt", {})
    op = gt.get("op", "")
    if not qr.ok or not qr.series:
        return None
    if op in ("quantile", "sum") and (gt.get("by")):
        label = gt["by"][0]
        return {s["labels"].get(label, ""): s["value"] for s in qr.series}
    if op in ("topk_sum", "topk_count"):
        label = gt["key_label"]
        return {s["labels"].get(label, ""): s["value"] for s in qr.series}
    # scalar families: global quantile/sum, count_distinct, frequency
    return qr.series[0]["value"]


def _run_queries(base_url: str, queries: list[dict[str, Any]],
                 archive_cross_check: bool) -> tuple[dict, dict]:
    warm: dict[str, Any] = {}
    data_source: dict[str, str] = {}
    for q in queries:
        if "gt" not in q:
            continue
        qid = q.get("id") or q.get("promql")
        promql = q.get("metricsql") or q["promql"]
        qr = query_client.query_instant(base_url, promql)
        warm[qid] = _normalize_warm(q, qr)
        data_source[qid] = qr.data_source if qr.ok else f"error:{qr.error[:60]}"
        status = "ok" if qr.ok else "ERR"
        print(f"  query {qid:24s} [{status}] src={data_source[qid]} -> {warm[qid]}",
              file=sys.stderr)
    return warm, data_source


def cmd_all(args: argparse.Namespace) -> int:
    if args.bring_up:
        print("run_e2e: bringing up stack (run_demo.sh up gctrace)...", file=sys.stderr)
        subprocess.check_call([str(args.run_demo), "up", "gctrace"])

    print(f"run_e2e: awaiting data plane at {args.backend} ...", file=sys.stderr)
    if not query_client.await_ready(args.backend):
        print("run_e2e: data plane not ready; aborting.", file=sys.stderr)
        return 2

    # Replay the mapped JSONL into the agent via run.py's OTLP/gRPC sender.
    print(f"run_e2e: replaying {args.jsonl} -> {args.otlp_endpoint} ...", file=sys.stderr)
    rc = subprocess.call([
        sys.executable, str(DATASET_ROOT / "run.py"), "replay",
        "--jsonl", str(args.jsonl),
        "--endpoint", args.otlp_endpoint,
        "--pace-factor", str(args.pace_factor),
    ])
    if rc != 0:
        print(f"run_e2e: replay failed (rc={rc})", file=sys.stderr)
        return rc

    print(f"run_e2e: waiting {args.warmup_secs}s for window close + ship ...", file=sys.stderr)
    time.sleep(args.warmup_secs)
    return _query_gt_compare(args)


def cmd_query(args: argparse.Namespace) -> int:
    return _query_gt_compare(args)


def _query_gt_compare(args: argparse.Namespace) -> int:
    queries = json.loads(args.queries.read_text())
    rows = gt_eval.load_rows(args.jsonl)
    rows = gt_eval.select_window(rows, args.window_start_ms, args.window_end_ms)

    gt = gt_eval.evaluate_queries(queries, rows)
    warm, data_source = _run_queries(args.backend, queries, args.archive_cross_check)

    summary = cmp.compare_all(queries, gt, warm, data_source)
    report = cmp.render_report(summary)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    (args.out_dir / "gt.json").write_text(json.dumps(gt, indent=2, sort_keys=True) + "\n")
    (args.out_dir / "warm.json").write_text(json.dumps(warm, indent=2, sort_keys=True) + "\n")
    (args.out_dir / "data_source.json").write_text(json.dumps(data_source, indent=2, sort_keys=True) + "\n")
    (args.out_dir / "report.md").write_text(report)
    (args.out_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(report)
    print(f"run_e2e: {summary['n_pass']}/{summary['n_queries']} passed -> {args.out_dir}",
          file=sys.stderr)
    return 0 if summary["n_fail"] == 0 else 1


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="google_cluster E2E validation orchestrator.")
    ap.add_argument("command", choices=("all", "query"))
    ap.add_argument("--jsonl", type=Path, required=True)
    ap.add_argument("--queries", type=Path, default=DATASET_ROOT / "queries.json")
    ap.add_argument("--backend", default="http://127.0.0.1:9091",
                    help="Data-plane query base URL.")
    ap.add_argument("--otlp-endpoint", default="127.0.0.1:4317",
                    help="Fused agent OTLP/gRPC endpoint.")
    ap.add_argument("--out-dir", type=Path,
                    default=Path("/tmp/gct-e2e-" + time.strftime("%Y%m%d-%H%M%S")))
    ap.add_argument("--pace-factor", type=float, default=0.0)
    ap.add_argument("--warmup-secs", type=float, default=70.0)
    ap.add_argument("--window-start-ms", type=int, default=None)
    ap.add_argument("--window-end-ms", type=int, default=None)
    ap.add_argument("--archive-cross-check", action="store_true")
    ap.add_argument("--bring-up", action="store_true")
    ap.add_argument("--run-demo", type=Path,
                    default=DATASET_ROOT.parent.parent / "deploy" / "mvp-multinode" / "scripts" / "run_demo.sh")
    args = ap.parse_args(argv)
    return cmd_all(args) if args.command == "all" else cmd_query(args)


if __name__ == "__main__":
    sys.exit(main())
