#!/usr/bin/env python3
"""aggregate_report.py — multinode MVP demo report generator.

Renders one Markdown report from the artefacts captured by
`deploy/mvp-multinode/scripts/run_demo.sh all`. The multinode dir
shape is flatter than the singlenode `mvp_report.py` layout:

    <run-dir>/
        b0/
            replay.jsonl        # one JSON record per query attempt
            replay.log
            edge-node0.csv      # measure_per_edge_bandwidth.py output
            edge-node1.csv
            edge-node2.csv
            edge-node3.csv
            stages-node0.csv    # measure_stages.py output
            stages-node1.csv
            stages-node2.csv
            stages-node3.csv
        b1/
            ...
        asap/
            ...

For each arm we emit:
  - replay summary: total attempts, success counts, error counts,
    per-query success/total breakdown
  - edge bandwidth summary: total + per-node mean bytes/sec
  - stages summary: per-stage cpu_cores + rss_mib totals

Pure stdlib. Idempotent. Non-fatal on missing files — a missing
artefact just shows up as `n/a` in the corresponding column. The
parent run_demo.sh treats a non-zero exit as non-fatal.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from collections import defaultdict
from typing import Any


ARMS = ("asap", "b0", "b1")
NODES = ("node0", "node1", "node2", "node3")


# ── small helpers ────────────────────────────────────────────────


def _fmt_int(n: int) -> str:
    return f"{n:,}"


def _fmt_bytes_per_s(bps: float) -> str:
    """Human-readable bytes/s with KiB/MiB/GiB units."""
    if bps != bps:  # NaN
        return "n/a"
    units = (("GiB/s", 1024 ** 3), ("MiB/s", 1024 ** 2), ("KiB/s", 1024), ("B/s", 1))
    for label, scale in units:
        if bps >= scale:
            return f"{bps / scale:.2f} {label}"
    return f"{bps:.2f} B/s"


def _safe_float(s: str) -> float:
    try:
        return float(s)
    except (TypeError, ValueError):
        return float("nan")


# ── replay aggregation ───────────────────────────────────────────


def _load_replay(path: str) -> list[dict[str, Any]]:
    """Read a JSONL replay log into a list of dicts. Skip blank/
    malformed lines silently — the harness can produce a partial
    final line if the soak ends mid-write."""
    if not os.path.isfile(path):
        return []
    out = []
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


def _summarise_replay(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Total / success / per-query breakdown.

    A record counts as `success` when:
      - status == "success" AND
      - http_code == 200 AND
      - result is a non-empty list (PromQL returns [] for no-data;
        the report should distinguish "query ran, no data" from
        "query returned a non-empty vector").

    Treat empty-result responses as `empty` (separate column from
    `error`) so the operator can tell whether the wiring is broken
    or whether the metric simply hasn't landed yet.
    """
    by_query: dict[str, dict[str, int]] = defaultdict(
        lambda: {"total": 0, "success": 0, "empty": 0, "error": 0}
    )
    totals = {"total": 0, "success": 0, "empty": 0, "error": 0}
    error_codes: dict[str, int] = defaultdict(int)

    for rec in records:
        q = rec.get("query") or "<unknown>"
        status = rec.get("status")
        http_code = rec.get("http_code")
        result = rec.get("result")
        bucket = by_query[q]
        bucket["total"] += 1
        totals["total"] += 1
        if status == "success" and http_code == 200:
            if isinstance(result, list) and len(result) > 0:
                bucket["success"] += 1
                totals["success"] += 1
            else:
                bucket["empty"] += 1
                totals["empty"] += 1
        else:
            bucket["error"] += 1
            totals["error"] += 1
            tag = status or "unknown"
            if http_code is not None:
                tag = f"{tag}/{http_code}"
            error_codes[tag] += 1

    return {
        "totals": totals,
        "by_query": dict(by_query),
        "error_codes": dict(error_codes),
    }


# ── edge bandwidth aggregation ───────────────────────────────────


def _load_edge_csv(path: str) -> dict[str, list[float]]:
    """Read measure_per_edge_bandwidth.py output. Returns
    {edge_label: [bytes_per_s, ...]}."""
    if not os.path.isfile(path):
        return {}
    by_edge: dict[str, list[float]] = defaultdict(list)
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            edge = row.get("edge")
            if not edge:
                continue
            bps = _safe_float(row.get("bytes_per_s", ""))
            if bps != bps:  # NaN
                continue
            by_edge[edge].append(bps)
    return dict(by_edge)


def _summarise_edge_per_arm(arm_dir: str) -> dict[str, Any]:
    """Aggregate edge bandwidth across all node CSVs for one arm."""
    per_node: dict[str, dict[str, list[float]]] = {}
    for n in NODES:
        per_node[n] = _load_edge_csv(os.path.join(arm_dir, f"edge-{n}.csv"))

    # Per-node summary: total mean bytes/s across all that node's edges.
    node_totals: dict[str, float] = {}
    for n, edges in per_node.items():
        total = 0.0
        for bps_list in edges.values():
            if bps_list:
                total += sum(bps_list) / len(bps_list)
        node_totals[n] = total

    # Per-edge-label summary, summed across nodes' means.
    edge_totals: dict[str, float] = defaultdict(float)
    for edges in per_node.values():
        for edge, bps_list in edges.items():
            if bps_list:
                edge_totals[edge] += sum(bps_list) / len(bps_list)

    return {
        "per_node_total_bps": node_totals,
        "per_edge_total_bps": dict(edge_totals),
        "grand_total_bps": sum(node_totals.values()),
    }


# ── stages aggregation ───────────────────────────────────────────


def _load_stages_csv(path: str) -> list[dict[str, str]]:
    if not os.path.isfile(path):
        return []
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return list(csv.DictReader(f))


def _summarise_stages_per_arm(arm_dir: str) -> dict[str, Any]:
    """Aggregate stages.csv rows across all node CSVs for one arm.

    Each row is (baseline, stage, container, cpu_cores, rss_mib,
    net_in_kibps, net_out_kibps, disk_mib). Sum cpu_cores + rss_mib
    per stage across all nodes; mean net rates per stage.
    """
    per_stage: dict[str, dict[str, float]] = defaultdict(
        lambda: {"cpu_cores": 0.0, "rss_mib": 0.0, "container_count": 0.0}
    )
    for n in NODES:
        rows = _load_stages_csv(os.path.join(arm_dir, f"stages-{n}.csv"))
        for row in rows:
            stage = row.get("stage") or "<unknown>"
            cpu = _safe_float(row.get("cpu_cores", ""))
            rss = _safe_float(row.get("rss_mib", ""))
            if cpu != cpu or rss != rss:
                continue
            agg = per_stage[stage]
            agg["cpu_cores"] += cpu
            agg["rss_mib"] += rss
            agg["container_count"] += 1
    return {"per_stage": dict(per_stage)}


# ── markdown rendering ───────────────────────────────────────────


def _render_replay_section(arm: str, summary: dict[str, Any]) -> list[str]:
    out: list[str] = []
    totals = summary["totals"]
    out.append(f"#### Replay — arm `{arm}`")
    out.append("")
    out.append(
        f"- total attempts: **{_fmt_int(totals['total'])}**"
        f" · success: **{_fmt_int(totals['success'])}**"
        f" · empty: **{_fmt_int(totals['empty'])}**"
        f" · error: **{_fmt_int(totals['error'])}**"
    )
    if summary["error_codes"]:
        codes = ", ".join(f"`{k}`={v}" for k, v in sorted(summary["error_codes"].items()))
        out.append(f"- error codes: {codes}")
    out.append("")

    out.append("| Query | Total | Success | Empty | Error |")
    out.append("|---|---:|---:|---:|---:|")
    # Sort by total desc, then query text, for stability.
    for q, b in sorted(
        summary["by_query"].items(),
        key=lambda kv: (-kv[1]["total"], kv[0]),
    ):
        out.append(
            f"| `{q}` | {_fmt_int(b['total'])} | {_fmt_int(b['success'])} "
            f"| {_fmt_int(b['empty'])} | {_fmt_int(b['error'])} |"
        )
    out.append("")
    return out


def _render_edge_section(arm: str, summary: dict[str, Any]) -> list[str]:
    out: list[str] = []
    out.append(f"#### Edge bandwidth — arm `{arm}`")
    out.append("")
    out.append(
        f"- grand total (sum of per-node means): "
        f"**{_fmt_bytes_per_s(summary['grand_total_bps'])}**"
    )
    out.append("")
    if summary["per_node_total_bps"]:
        out.append("| Node | Mean total bytes/s |")
        out.append("|---|---:|")
        for n in NODES:
            bps = summary["per_node_total_bps"].get(n, 0.0)
            out.append(f"| `{n}` | {_fmt_bytes_per_s(bps)} |")
        out.append("")
    if summary["per_edge_total_bps"]:
        out.append("| Edge label | Sum of per-node means |")
        out.append("|---|---:|")
        for edge, bps in sorted(summary["per_edge_total_bps"].items()):
            out.append(f"| `{edge}` | {_fmt_bytes_per_s(bps)} |")
        out.append("")
    return out


def _render_stages_section(arm: str, summary: dict[str, Any]) -> list[str]:
    out: list[str] = []
    out.append(f"#### Stages — arm `{arm}`")
    out.append("")
    if not summary["per_stage"]:
        out.append("_no stages data_")
        out.append("")
        return out
    out.append("| Stage | Containers | Sum CPU (cores) | Sum RSS (MiB) |")
    out.append("|---|---:|---:|---:|")
    for stage in sorted(summary["per_stage"].keys()):
        agg = summary["per_stage"][stage]
        out.append(
            f"| `{stage}` | {int(agg['container_count'])} "
            f"| {agg['cpu_cores']:.3f} | {agg['rss_mib']:.1f} |"
        )
    out.append("")
    return out


def _render_arm(arm: str, run_dir: str) -> list[str]:
    arm_dir = os.path.join(run_dir, arm)
    out: list[str] = [f"### Arm `{arm}`", ""]
    if not os.path.isdir(arm_dir):
        out.append(f"_arm directory `{arm_dir}` not found_")
        out.append("")
        return out

    replay = _load_replay(os.path.join(arm_dir, "replay.jsonl"))
    if replay:
        out.extend(_render_replay_section(arm, _summarise_replay(replay)))
    else:
        out.append("_no replay.jsonl_")
        out.append("")

    out.extend(_render_edge_section(arm, _summarise_edge_per_arm(arm_dir)))
    out.extend(_render_stages_section(arm, _summarise_stages_per_arm(arm_dir)))
    return out


def _render_top_summary(run_dir: str) -> list[str]:
    """Headline table: one row per arm with total/success/error."""
    out: list[str] = []
    out.append("## Summary")
    out.append("")
    out.append("| Arm | Total | Success | Empty | Error |")
    out.append("|---|---:|---:|---:|---:|")
    for arm in ARMS:
        records = _load_replay(os.path.join(run_dir, arm, "replay.jsonl"))
        if not records:
            out.append(f"| `{arm}` | n/a | n/a | n/a | n/a |")
            continue
        s = _summarise_replay(records)["totals"]
        out.append(
            f"| `{arm}` | {_fmt_int(s['total'])} | {_fmt_int(s['success'])} "
            f"| {_fmt_int(s['empty'])} | {_fmt_int(s['error'])} |"
        )
    out.append("")
    return out


def render_report(run_dir: str) -> str:
    lines: list[str] = []
    lines.append(f"# MVP multinode demo report")
    lines.append("")
    lines.append(f"Run dir: `{run_dir}`")
    lines.append("")
    lines.extend(_render_top_summary(run_dir))
    lines.append("## Per-arm details")
    lines.append("")
    for arm in ARMS:
        lines.extend(_render_arm(arm, run_dir))
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--run-dir", required=True,
                    help="run output directory (contains b0/, b1/, asap/)")
    ap.add_argument("--out", required=True, help="output Markdown path")
    args = ap.parse_args()

    if not os.path.isdir(args.run_dir):
        print(f"run-dir does not exist: {args.run_dir}", file=sys.stderr)
        return 2

    md = render_report(args.run_dir)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(md)
    print(f"wrote {args.out} ({len(md)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
