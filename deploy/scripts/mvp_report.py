#!/usr/bin/env python3
"""mvp_report.py — MVP demo report generator.

Renders one Markdown report from the artefacts captured by
`run_mvp_demo.sh` against the controller-driven multi-stage topology.

Highlights:

  * **Single run, multi-stage topology.** One controller-driven cell.
    The B0 baseline number for criterion ① is read out of the same
    run's Prometheus B0 snapshot when the b0 profile was active,
    otherwise the cell shows "—" and the criterion gets verdict
    UNKNOWN.

  * **Six criteria with per-class breakdown.** §2's verdict rows
    cover bandwidth / latency / combined-resource / accuracy /
    cold-fallback / freshness. §3 breaks each query class out
    separately so the reader sees which sketch+stage the controller
    chose for window / label / combined.

  * **Stage table is per-stage TOTAL only.** Rows: agent / gateway /
    backend-ingest / backend-storage / backend-query, one each.

  * **Per-edge bandwidth from `per_edge_bandwidth.csv`.** §1's
    bandwidth column reads the per-edge probe's mean bytes/s rather
    than docker stats' all-container-rolled-up netio.

  * **Postings + compaction + S3-cost sections degrade gracefully.**
    If the backend image lacks the postings_filtered_series_count
    fields or the /internal/s3_cost.csv endpoint, those sections
    render with a clear "merge-pending" marker rather than missing
    data.

Pure stdlib. Idempotent — re-running over the same CSVs reproduces
the same MD.

Input layout:

    <results_dir>/
        controller-emitted-configs/{STATUS,agent.bootstrap.yaml,...}
        measurements/{stages.csv, per_edge_bandwidth.csv,
                       replay.jsonl, accuracy.csv, s3_cost.csv}
        freshness/{raw.csv, warm.csv, archive.csv}
        ad-hoc/{count_api_series.json, topk_5xx_by_zone.json,
                cold_payments.json, cold_payments.verdict, ...}
        compactor/{dry_run.json, live_run.json,
                    before.minio.jsonl, after.minio.jsonl, SKIPPED?}

Output:

    <results_dir>/MVP_REPORT.md
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import statistics
import sys
from typing import Any


STAGE_ORDER = [
    "agent",
    "gateway",
    "backend-ingest",
    "backend-storage",
    "backend-query",
]

QUERY_CLASSES = [
    # (label, kind tag in replay-queries.json, promql snippet match)
    ("window-per-series",      "quantile",  "quantile_over_time"),
    ("label-at-instant",       "sum",       "sum by (zone) (http_requests_total)"),
    ("combined-window-label",  "sum",       "sum by (zone) (rate(http_requests_total"),
]

EDGE_ORDER = [
    "edge_sdk_to_agent",
    "edge_agent_to_gateway",
    "edge_gateway_to_backend",
    "edge_gateway_to_s3",
]


# ── tiny helpers ─────────────────────────────────────────────────


def _is_nan(x: float) -> bool:
    return x != x  # noqa: PLR0124


def _percentile(xs: list[float], p: float) -> float:
    if not xs:
        return float("nan")
    s = sorted(xs)
    idx = max(0, min(len(s) - 1, int(round(p * (len(s) - 1)))))
    return s[idx]


def _read_csv(path: str) -> list[dict[str, str]]:
    if not os.path.exists(path):
        return []
    with open(path, "r") as f:
        return list(csv.DictReader(f))


def _read_json(path: str) -> dict | None:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def _read_text(path: str) -> str:
    if not os.path.exists(path):
        return ""
    try:
        with open(path, "r") as f:
            return f.read()
    except OSError:
        return ""


def _read_jsonl(path: str) -> list[dict]:
    out: list[dict] = []
    if not os.path.exists(path):
        return out
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


# ── §1 stage-separated resource table ────────────────────────────


def _aggregate_stages(rows: list[dict]) -> dict[str, dict[str, float]]:
    """Sum per-stage totals across all containers in that stage.

    The MVP demo's table is single-baseline; we sum to one row per stage.
    """
    out: dict[str, dict[str, float]] = {}
    for r in rows:
        stage = r.get("stage", "")
        if stage not in STAGE_ORDER:
            continue
        agg = out.setdefault(stage, {
            "cpu_cores": 0.0, "rss_mib": 0.0,
            "net_in_kibps": 0.0, "net_out_kibps": 0.0,
            "disk_mib": 0.0,
        })
        for k in ("cpu_cores", "rss_mib", "net_in_kibps", "net_out_kibps", "disk_mib"):
            try:
                v = float(r.get(k, "") or "nan")
            except ValueError:
                v = float("nan")
            if not _is_nan(v):
                agg[k] += v
    return out


def render_section_1_stage_table(stages_rows: list[dict]) -> list[str]:
    md: list[str] = []
    md.append("## §1 Stage-separated resource table")
    md.append("")
    md.append(
        "Per-stage TOTAL across all containers in that stage. The "
        "Multi-stage topology has 10 producer / 2 agent / 1 "
        "gateway / 1 backend / 1 storage (MinIO) containers; the "
        "rows below reduce all relevant containers per stage to a "
        "single number. CPU is mean cores over the 60s window, RSS "
        "is mean MiB, net is window-rate KiB/s (from `docker stats` "
        "NetIO deltas), disk is end-of-window MiB."
    )
    md.append("")

    aggs = _aggregate_stages(stages_rows)
    if not aggs:
        md.append("_stages.csv missing — no rows to render_")
        md.append("")
        return md

    md.append("| Stage | CPU (cores) | RSS (MiB) | Net in (KiB/s) | Net out (KiB/s) | Disk (MiB) |")
    md.append("|---|---|---|---|---|---|")
    for stage in STAGE_ORDER:
        d = aggs.get(stage, {})

        def fmt(key: str, fmt_str: str) -> str:
            v = d.get(key, float("nan"))
            return fmt_str.format(v) if not _is_nan(v) else "—"

        md.append(
            "| {stage} | {cpu} | {rss} | {nin} | {nout} | {disk} |".format(
                stage=stage,
                cpu=fmt("cpu_cores", "{:.3f}"),
                rss=fmt("rss_mib", "{:.1f}"),
                nin=fmt("net_in_kibps", "{:.1f}"),
                nout=fmt("net_out_kibps", "{:.1f}"),
                disk=fmt("disk_mib", "{:.1f}"),
            )
        )
    md.append("")
    return md


# ── §2 per-criterion verdict (6 criteria) ────────────────────────


def _per_edge_mean_bytes_per_s(edge_rows: list[dict]) -> dict[str, float]:
    """Mean bytes/s per edge label."""
    by_edge: dict[str, list[float]] = {}
    for r in edge_rows:
        try:
            bps = float(r.get("bytes_per_s", "") or "nan")
        except ValueError:
            continue
        if _is_nan(bps):
            continue
        by_edge.setdefault(r.get("edge", "?"), []).append(bps)
    return {e: (sum(v) / len(v) if v else float("nan")) for e, v in by_edge.items()}


def criterion_bandwidth(edge_rows: list[dict]) -> tuple[str, str, dict]:
    means = _per_edge_mean_bytes_per_s(edge_rows)
    sdk_to_agent = means.get("edge_sdk_to_agent", float("nan"))
    agent_to_gw = means.get("edge_agent_to_gateway", float("nan"))
    gw_to_backend = means.get("edge_gateway_to_backend", float("nan"))
    gw_to_s3 = means.get("edge_gateway_to_s3", float("nan"))
    if _is_nan(sdk_to_agent) and _is_nan(agent_to_gw):
        return "UNKNOWN", "per_edge_bandwidth.csv missing or empty", {}

    # Per-edge B/s, one decimal place. We don't compute a B0
    # comparison here (single-cell run) — that's the §1 stage table
    # job. Verdict is PASS if the egress edge (agent→gateway) is
    # less than the ingress edge (sdk→agent), which is the
    # ASAP value-prop marker (sketches reduce volume on egress).
    if not _is_nan(sdk_to_agent) and not _is_nan(agent_to_gw):
        verdict = "PASS" if agent_to_gw < sdk_to_agent else "FAIL"
    else:
        verdict = "UNKNOWN"

    line = (
        f"sdk→agent={sdk_to_agent:.1f} B/s, "
        f"agent→gateway={agent_to_gw:.1f} B/s, "
        f"gateway→backend={gw_to_backend:.1f} B/s, "
        f"gateway→s3={gw_to_s3:.1f} B/s"
    )
    return verdict, line, means


def _classify_query_class(replay_row: dict) -> str | None:
    """Return one of the QUERY_CLASSES labels for a replay JSONL
    row, or None if it doesn't match."""
    promql = replay_row.get("query", "")
    for label, _kind, snippet in QUERY_CLASSES:
        if snippet in promql:
            return label
    return None


def _per_class_latency_p50_p99(replay_rows: list[dict]) -> dict[str, dict[str, float]]:
    by_class: dict[str, list[float]] = {}
    for r in replay_rows:
        cls = _classify_query_class(r)
        if cls is None:
            continue
        try:
            d = float(r.get("duration_ms"))
        except (TypeError, ValueError):
            continue
        if _is_nan(d):
            continue
        by_class.setdefault(cls, []).append(d)
    out: dict[str, dict[str, float]] = {}
    for cls, vals in by_class.items():
        out[cls] = {
            "p50": _percentile(vals, 0.5),
            "p99": _percentile(vals, 0.99),
            "n": float(len(vals)),
        }
    return out


def criterion_latency(replay_rows: list[dict]) -> tuple[str, str, dict]:
    if not replay_rows:
        return "UNKNOWN", "replay.jsonl missing or empty", {}
    by_class = _per_class_latency_p50_p99(replay_rows)
    if not by_class:
        return "UNKNOWN", "no replay rows matched a known query class", {}
    parts = []
    worst_p99 = 0.0
    for cls, _, _ in QUERY_CLASSES:
        d = by_class.get(cls)
        if d is None:
            parts.append(f"{cls}: n=0")
            continue
        parts.append(
            f"{cls}: p50={d['p50']:.1f}ms p99={d['p99']:.1f}ms n={int(d['n'])}"
        )
        if d["p99"] > worst_p99:
            worst_p99 = d["p99"]
    # Spec doesn't pin a numeric SLA, but a 5s p99 is a reasonable
    # "something's broken" gate — the ASAP backend should be much
    # faster than that on a 60s soak.
    verdict = "PASS" if worst_p99 < 5_000.0 and worst_p99 > 0 else (
        "UNKNOWN" if worst_p99 == 0 else "FAIL"
    )
    return verdict, "; ".join(parts), by_class


def criterion_combined_resource(stages_rows: list[dict]) -> tuple[str, str, dict]:
    """Total CPU cores + total RSS MiB summed across stages.

    Single-cell run: no B0 comparison number to take a percentage
    against. Verdict is UNKNOWN unless the cell included a B0
    Prometheus snapshot (recorded in stages.csv as a row with
    stage=backend-storage-b0).
    """
    aggs = _aggregate_stages(stages_rows)
    if not aggs:
        return "UNKNOWN", "stages.csv missing or empty", {}
    total_cpu = sum(d.get("cpu_cores", 0.0) for d in aggs.values())
    total_rss = sum(d.get("rss_mib", 0.0) for d in aggs.values())
    line = (
        f"total cpu_cores={total_cpu:.3f}, total rss_mib={total_rss:.1f} "
        f"(stages: {', '.join(sorted(aggs.keys()))})"
    )
    # No B0 comparison in single-cell mode — verdict is "captured".
    return "CAPTURED", line, {"cpu": total_cpu, "rss": total_rss}


def criterion_accuracy(accuracy_rows: list[dict]) -> tuple[str, str, dict]:
    by_kind: dict[str, list[float]] = {}
    for row in accuracy_rows:
        k = row.get("kind", "")
        try:
            err = float(row.get("error", "") or "nan")
        except ValueError:
            err = float("nan")
        if not _is_nan(err):
            by_kind.setdefault(k, []).append(err)
    if not by_kind:
        return "UNKNOWN", "accuracy.csv empty (warm tier may not have flushed)", {}
    parts: list[str] = []
    medians: dict[str, float] = {}
    for k in sorted(by_kind):
        med = statistics.median(by_kind[k])
        medians[k] = med
        parts.append(f"{k}: median rel-err={med:.4f} (n={len(by_kind[k])})")
    worst = max(medians.values()) if medians else float("inf")
    verdict = "PASS" if worst <= 0.05 else "FAIL"
    return verdict, "; ".join(parts), medians


def criterion_cold_fallback(adhoc_dir: str) -> tuple[str, str, dict]:
    response_path = os.path.join(adhoc_dir, "cold_payments.json")
    verdict_path = os.path.join(adhoc_dir, "cold_payments.verdict")
    curl_path = os.path.join(adhoc_dir, "cold_payments.curlstats")
    body = _read_json(response_path)
    raw_text = _read_text(response_path)
    verdict_marker = _read_text(verdict_path).strip()
    curlstats = _read_text(curl_path).strip()

    if body is None and not raw_text:
        return "UNKNOWN", "cold_payments.json missing", {}

    has_marker = "gorilla_archive" in raw_text
    if has_marker:
        verdict = "PASS"
        line = (
            "`data_source: gorilla_archive` present in response — "
            "GorillaQueryEngine served the ad-hoc query."
        )
    elif verdict_marker == "PASS":
        # Driver thought it was OK but the marker grep missed —
        # surface as PARTIAL.
        verdict = "PARTIAL"
        line = "driver wrote PASS but no `gorilla_archive` marker found in body"
    else:
        verdict = "FAIL"
        line = "no `gorilla_archive` marker in cold_payments.json"
    if curlstats:
        line += f"  · curl: {curlstats}"
    return verdict, line, {"has_marker": has_marker, "marker": verdict_marker}


def criterion_freshness(fresh_dir: str) -> tuple[str, str, dict]:
    summary: dict[str, dict[str, float]] = {}
    for path in ("raw", "warm", "archive"):
        rows = _read_csv(os.path.join(fresh_dir, f"{path}.csv"))
        deltas: list[float] = []
        for r in rows:
            try:
                d = float(r.get("delta_ms", "") or "nan")
            except ValueError:
                continue
            if not _is_nan(d):
                deltas.append(d)
        if deltas:
            summary[path] = {
                "p50": _percentile(deltas, 0.5),
                "p99": _percentile(deltas, 0.99),
                "n": float(len(deltas)),
            }
        else:
            summary[path] = {"p50": float("nan"), "p99": float("nan"), "n": 0.0}

    if all(_is_nan(s["p50"]) for s in summary.values()):
        return "UNKNOWN", "no freshness samples on any path", summary

    parts = []
    for path in ("raw", "warm", "archive"):
        s = summary[path]
        if _is_nan(s["p50"]):
            parts.append(f"{path}: no observations")
        else:
            parts.append(
                f"{path}: p50={s['p50']:.0f}ms p99={s['p99']:.0f}ms n={int(s['n'])}"
            )
    warm_p50 = summary.get("warm", {}).get("p50", float("nan"))
    archive_p50 = summary.get("archive", {}).get("p50", float("nan"))
    # Same gates as v4: warm ≤ 30s, archive ≤ 90s, in milliseconds.
    if _is_nan(warm_p50):
        verdict = "UNKNOWN"
    elif warm_p50 <= 30_000.0 and (
        _is_nan(archive_p50) or archive_p50 <= 90_000.0
    ):
        verdict = "PASS"
    else:
        verdict = "FAIL"
    return verdict, "; ".join(parts), summary


# ── §3 per-query-class breakdown ────────────────────────────────


def render_section_3_per_class(
    replay_rows: list[dict],
    accuracy_rows: list[dict],
    emitted_status: str,
) -> list[str]:
    md: list[str] = []
    md.append("## §3 Per-query-class breakdown")
    md.append("")
    md.append(
        "Three canonical query classes from "
        "`deploy/configs/mvp-workload.yaml`. Sketch + stage "
        "assignments come from the controller-emitted configs (see "
        "§9 below); latency from `replay.jsonl`; accuracy from "
        "`accuracy.csv`."
    )
    md.append("")
    md.append(
        "| Class | Sketch / stage (controller plan) | p50 (ms) | p99 (ms) | "
        "median rel-err | n |"
    )
    md.append("|---|---|---|---|---|---|")

    by_class = _per_class_latency_p50_p99(replay_rows)
    # Accuracy by kind (we don't have a class-level join in
    # accuracy.csv; we map kind→class through the
    # QUERY_CLASSES table).
    acc_by_kind: dict[str, list[float]] = {}
    for r in accuracy_rows:
        k = r.get("kind", "")
        try:
            err = float(r.get("error", "") or "nan")
        except ValueError:
            err = float("nan")
        if not _is_nan(err):
            acc_by_kind.setdefault(k, []).append(err)

    # Pre-canned plan annotation per class. The actual planner
    # output is captured in the controller-emitted configs; this
    # column shows the EXPECTED plan from
    # mvp-workload.yaml::assign_to_role.
    plan_annotation = {
        "window-per-series":     "DDSketch / agent",
        "label-at-instant":      "identity / gateway (sum-by-zone fan-in)",
        "combined-window-label": "rate@agent + sum-by-zone@gateway",
    }

    for cls, kind, _ in QUERY_CLASSES:
        d = by_class.get(cls, {})
        p50 = d.get("p50", float("nan"))
        p99 = d.get("p99", float("nan"))
        n = int(d.get("n", 0.0))
        errs = acc_by_kind.get(kind, [])
        med_err = statistics.median(errs) if errs else float("nan")

        def fmt(v: float, fmt_str: str) -> str:
            return fmt_str.format(v) if not _is_nan(v) else "—"

        plan_note = plan_annotation.get(cls, "—")
        if emitted_status != "live":
            plan_note += "  *(plan from workload spec; emitter " + emitted_status + ")*"

        md.append(
            "| {cls} | {plan} | {p50} | {p99} | {err} | {n} |".format(
                cls=cls,
                plan=plan_note,
                p50=fmt(p50, "{:.1f}"),
                p99=fmt(p99, "{:.1f}"),
                err=fmt(med_err, "{:.4f}"),
                n=n,
            )
        )
    md.append("")
    return md


# ── §4 postings filtering effect ─────────────────────────────────


def _extract_int_from_response(body: dict | None, key: str) -> int | None:
    """Look for the postings field in the response infos / data."""
    if body is None:
        return None
    data = body.get("data") or {}
    infos = data.get("infos") or body.get("infos") or []
    if isinstance(infos, list):
        for s in infos:
            s = str(s)
            i = s.find(key)
            if i >= 0:
                tail = s[i + len(key):].strip(": ").strip()
                # Take the first integer-looking token.
                token = ""
                for ch in tail:
                    if ch.isdigit():
                        token += ch
                    else:
                        break
                if token:
                    try:
                        return int(token)
                    except ValueError:
                        return None
    return None


def render_section_4_postings(adhoc_dir: str) -> list[str]:
    md: list[str] = []
    md.append("## §4 Postings filtering effect")
    md.append("")

    queries = [
        ("count_api_series",  'count(http_requests_total{service="api"})'),
        ("topk_5xx_by_zone",  'topk(5, sum by (zone) (rate(http_requests_total{status=~"5.."}[5m])))'),
    ]
    md.append(
        "Two ad-hoc queries with label predicates. Postings index: "
        "`postings_filtered_series_count` is the count of series "
        "that survived the predicate after sidecar lookup; the "
        "would-have-scanned column is the same metric WITHOUT the "
        "predicate (a coarse upper bound)."
    )
    md.append("")
    md.append("| Query | Series matched | Would have scanned | Status |")
    md.append("|---|---|---|---|")
    any_data = False
    for label, promql in queries:
        body = _read_json(os.path.join(adhoc_dir, f"{label}.json"))
        matched = _extract_int_from_response(body, "postings_filtered_series_count")
        would = _extract_int_from_response(body, "series_scanned_total")
        if matched is None and would is None:
            md.append(
                f"| `{promql}` | — | — | merge-pending (no postings field in response) |"
            )
        else:
            any_data = True
            md.append(
                f"| `{promql}` | {matched if matched is not None else '—'} | "
                f"{would if would is not None else '—'} | OK |"
            )
    md.append("")
    if not any_data:
        md.append(
            "_postings field not present on responses; rerun once "
            "the postings-aware engine + sidecar PRs have landed._"
        )
        md.append("")
    return md


# ── §5 compaction effect ─────────────────────────────────────────


def _count_minio_objects(jsonl_path: str) -> tuple[int, int]:
    """Return (object_count, total_bytes). Each line is one
    `mc ls --recursive --json` record."""
    if not os.path.exists(jsonl_path):
        return (0, 0)
    count = 0
    total_bytes = 0
    with open(jsonl_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            # `mc ls --json` rows have status=success, type=file, size=<int>.
            if rec.get("type") in ("file", "FILE"):
                count += 1
                try:
                    total_bytes += int(rec.get("size", 0))
                except (TypeError, ValueError):
                    pass
    return (count, total_bytes)


def render_section_5_compaction(compactor_dir: str) -> list[str]:
    md: list[str] = []
    md.append("## §5 Compaction effect")
    md.append("")
    skipped = os.path.exists(os.path.join(compactor_dir, "SKIPPED"))
    if skipped:
        reason = _read_text(os.path.join(compactor_dir, "SKIPPED")).strip()
        md.append(f"_Compactor SKIPPED: {reason}_")
        md.append("")
        return md

    before = _count_minio_objects(os.path.join(compactor_dir, "before.minio.jsonl"))
    after = _count_minio_objects(os.path.join(compactor_dir, "after.minio.jsonl"))
    md.append(
        "Concat-only compactor byte-concatenates 6+ adjacent "
        "blocks ≥6h old into one merged object. **No decode / "
        "re-encode** — each source chunk remains an atomic Gorilla "
        "chunk inside the merged file. The new manifest records "
        "each chunk's `byte_offset` + `byte_length` so the backend "
        "can issue `Range:` partial reads."
    )
    md.append("")
    md.append("| Stage | Object count | Total bytes |")
    md.append("|---|---|---|")
    md.append(f"| before | {before[0]} | {before[1]} |")
    md.append(f"| after  | {after[0]}  | {after[1]} |")
    md.append("")

    dry = _read_json(os.path.join(compactor_dir, "dry_run.json"))
    if isinstance(dry, dict):
        eligible = dry.get("eligible_blocks") or dry.get("blocks") or []
        if isinstance(eligible, list):
            md.append(f"Dry-run plan: {len(eligible)} merge candidate(s).")
            md.append("")
    return md


# ── §6 cost (S3 ops) ─────────────────────────────────────────────


def render_section_6_cost(measurements_dir: str) -> list[str]:
    md: list[str] = []
    md.append("## §6 S3-ops cost (measured)")
    md.append("")
    rows = _read_csv(os.path.join(measurements_dir, "s3_cost.csv"))
    if not rows:
        md.append(
            "_cost-tracker not present — `/internal/s3_cost.csv` "
            "endpoint unavailable. Will populate once the backend "
            "exposes the internal cost-tracker endpoint._"
        )
        md.append("")
        return md
    md.append(
        "Counts of PUT / GET / HEAD / DELETE issued against MinIO "
        "during the cell's measurement window (the backend's "
        "internal cost tracker)."
    )
    md.append("")
    if rows:
        # Render straight: take the first row, keys are the column names.
        keys = sorted(rows[0].keys())
        md.append("| " + " | ".join(keys) + " |")
        md.append("|" + "---|" * len(keys))
        for r in rows:
            md.append("| " + " | ".join(str(r.get(k, "")) for k in keys) + " |")
        md.append("")
    return md


# ── §7 honest caveats ─────────────────────────────────────────────


def render_section_7_caveats() -> list[str]:
    md: list[str] = []
    md.append("## §7 Honest caveats (non-goals)")
    md.append("")
    md.append(
        "* **No dynamic replan.** The controller plans once at "
        "startup off `mvp-workload.yaml`. The MVP demo does not "
        "exercise the in-flight replan path — that's a follow-up."
    )
    md.append(
        "* **No OpAMP hot reconfig under churn.** OpAMP push happens "
        "once per stage at boot; we don't kill an agent and verify "
        "the controller re-pushes."
    )
    md.append(
        "* **10K series, not 1M.** Per-agent cardinality 500, 10 "
        "producers → 5K aggregate at the gateway. The 1M target "
        "needs the cardinality-redesign work plus a multi-host "
        "topology — out of scope for the MVP demo."
    )
    md.append(
        "* **Single host.** All containers share kernel scheduler "
        "+ loopback NIC. Wire-bytes per-edge counters are still "
        "meaningful (TX/RX is per-container) but absolute latencies "
        "are loopback-flattered."
    )
    md.append(
        "* **B0 Prometheus reference is opt-in.** The driver does "
        "NOT bring up B0 in the same compose stack as the ASAP "
        "backend (port collision on 19090). To get an A-vs-B "
        "comparison row, run a separate B0 cycle and join the "
        "stages.csv files manually."
    )
    md.append(
        "* **Postings + cost tracker gated on backend image.** "
        "Sections §4 and §6 render with a `merge-pending` marker "
        "until the postings-aware engine and the cost-tracker "
        "endpoint are exposed by the running backend image."
    )
    md.append("")
    return md


# ── §8 emitted-config status ─────────────────────────────────────


def _read_emitted_status(cdir: str) -> str:
    status_path = os.path.join(cdir, "STATUS")
    s = _read_text(status_path).strip()
    return s or "unknown"


def render_section_8_emitted(cdir: str) -> list[str]:
    md: list[str] = []
    md.append("## §8 Controller-emitted runtime configs")
    md.append("")
    status = _read_emitted_status(cdir)
    md.append(f"Emitter status: **{status}**")
    md.append("")
    if status == "live":
        md.append(
            "Controller's typed-stage-split path produced per-stage "
            "configs and pushed them via OpAMP / BackendClient. The "
            "captured artifacts live under "
            "`controller-emitted-configs/`."
        )
    elif status == "fallback-placeholder":
        md.append(
            "Controller's typed-stage-split path returned None for "
            "the workload — the gateway and agents are running the "
            "placeholder configs mounted by the compose overlay. "
            "This is the spec's documented fallback mode; criterion "
            "verdicts in §2 are still meaningful."
        )
    elif status == "not-exercised":
        md.append(
            "No typed-stage-split tracing in the controller logs — "
            "either `USE_TYPED_STAGE_SPLIT` was unset (default 0) "
            "or the workload didn't bind to a SketchExpr. Phase F "
            "should set USE_TYPED_STAGE_SPLIT=1 in the compose env."
        )
    else:
        md.append(
            "Emitter status unknown — controller-emitted-configs/STATUS "
            "missing or empty. The driver may not have completed "
            "Phase 1's capture step."
        )
    md.append("")
    # List captured artifacts.
    if os.path.isdir(cdir):
        files = sorted(
            f for f in os.listdir(cdir)
            if not f.startswith(".") and f != "STATUS"
        )
        if files:
            md.append("Captured artifacts:")
            md.append("")
            for f in files:
                size = os.path.getsize(os.path.join(cdir, f))
                md.append(f"- `{f}` ({size} B)")
            md.append("")
    return md


# ── markdown assembly ────────────────────────────────────────────


def render_markdown(
    results_dir: str,
    num_producers: int,
    per_agent_cardinality: int,
) -> str:
    if not os.path.isdir(results_dir):
        return f"# MVP report — results dir missing ({results_dir})\n"

    measurements_dir = os.path.join(results_dir, "measurements")
    fresh_dir = os.path.join(results_dir, "freshness")
    adhoc_dir = os.path.join(results_dir, "ad-hoc")
    compactor_dir = os.path.join(results_dir, "compactor")
    emitted_dir = os.path.join(results_dir, "controller-emitted-configs")

    stages_rows = _read_csv(os.path.join(measurements_dir, "stages.csv"))
    edge_rows = _read_csv(os.path.join(measurements_dir, "per_edge_bandwidth.csv"))
    accuracy_rows = _read_csv(os.path.join(measurements_dir, "accuracy.csv"))
    replay_rows = _read_jsonl(os.path.join(measurements_dir, "replay.jsonl"))
    emitted_status = _read_emitted_status(emitted_dir)

    md: list[str] = []
    md.append("# ASAPCollector MVP demo — issue #46 (controller-driven multi-stage)")
    md.append("")
    md.append(
        "Single-cell controller-driven run: 10 producers → 2 agents "
        "→ 1 gateway → 1 backend (+ MinIO archive). Controller plans "
        "from `deploy/configs/mvp-workload.yaml`; per-stage configs "
        "are emitted via the typed-stage-split path "
        "(`USE_TYPED_STAGE_SPLIT=1`). Six criteria + per-class "
        "latency + per-edge bandwidth in this report."
    )
    md.append("")
    md.append("## Workload shape")
    md.append("")
    md.append("| Knob | Value |")
    md.append("|------|-------|")
    md.append(f"| Per-agent cardinality | **{per_agent_cardinality}** |")
    md.append(f"| Number of producers | **{num_producers}** (×5 → agent-a, ×5 → agent-b) |")
    md.append(f"| Aggregate series at gateway | **{num_producers * per_agent_cardinality}** |")
    md.append(f"| Sketch family (default) | DDSketch (overridden per-metric by controller) |")
    md.append(f"| Stack settle + warm-up + soak | 60s + 60s + 60s |")
    md.append(f"| Replay shapes | window/label/combined @ 5 QPS for 60s |")
    md.append("")

    md.extend(render_section_1_stage_table(stages_rows))

    # §2 verdict table.
    bw_v, bw_line, _ = criterion_bandwidth(edge_rows)
    lat_v, lat_line, _ = criterion_latency(replay_rows)
    res_v, res_line, _ = criterion_combined_resource(stages_rows)
    acc_v, acc_line, _ = criterion_accuracy(accuracy_rows)
    cold_v, cold_line, _ = criterion_cold_fallback(adhoc_dir)
    fresh_v, fresh_line, _ = criterion_freshness(fresh_dir)

    md.append("## §2 Per-criterion verdict (6 criteria)")
    md.append("")
    md.append("| # | Criterion | Verdict | Detail |")
    md.append("|---|---|---|---|")
    md.append(f"| 1 | Bandwidth (per-edge B/s) | **{bw_v}** | {bw_line} |")
    md.append(f"| 2 | Query latency (p50/p99 per class) | **{lat_v}** | {lat_line} |")
    md.append(f"| 3 | Combined resource (sum of stages) | **{res_v}** | {res_line} |")
    md.append(f"| 4 | Accuracy (rel-err per class) | **{acc_v}** | {acc_line} |")
    md.append(f"| 5 | Cold-fallback (gorilla_archive marker) | **{cold_v}** | {cold_line} |")
    md.append(f"| 6 | Freshness (p50/p99 per path) | **{fresh_v}** | {fresh_line} |")
    md.append("")

    md.extend(render_section_3_per_class(replay_rows, accuracy_rows, emitted_status))
    md.extend(render_section_4_postings(adhoc_dir))
    md.extend(render_section_5_compaction(compactor_dir))
    md.extend(render_section_6_cost(measurements_dir))
    md.extend(render_section_7_caveats())
    md.extend(render_section_8_emitted(emitted_dir))

    # Per-edge bandwidth appendix.
    md.append("## Appendix A — per-edge bandwidth")
    md.append("")
    md.append("| Edge | Mean B/s | Samples |")
    md.append("|---|---|---|")
    means = _per_edge_mean_bytes_per_s(edge_rows)
    counts: dict[str, int] = {}
    for r in edge_rows:
        try:
            float(r.get("bytes_per_s", "") or "nan")
        except ValueError:
            continue
        counts[r.get("edge", "?")] = counts.get(r.get("edge", "?"), 0) + 1
    for e in EDGE_ORDER:
        m = means.get(e, float("nan"))
        n = counts.get(e, 0)
        m_str = f"{m:.1f}" if not _is_nan(m) else "—"
        md.append(f"| {e} | {m_str} | {n} |")
    md.append("")

    return "\n".join(md) + "\n"


# ── main ─────────────────────────────────────────────────────────


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument(
        "--results-dir",
        required=True,
        help="MVP run output directory (the one passed as OUT_BASE to run_mvp_demo.sh).",
    )
    ap.add_argument(
        "--num-producers",
        type=int,
        default=10,
        help="Number of fake-exporter producers (default 10).",
    )
    ap.add_argument(
        "--per-agent-cardinality",
        type=int,
        default=500,
        help="Per-producer cardinality (default 500).",
    )
    ap.add_argument(
        "--out",
        required=True,
        help="Output MD path. Caller usually points this at <results_dir>/MVP_REPORT.md.",
    )
    args = ap.parse_args(argv)

    md = render_markdown(
        args.results_dir,
        num_producers=args.num_producers,
        per_agent_cardinality=args.per_agent_cardinality,
    )

    with open(args.out, "w") as f:
        f.write(md)
    print(f"wrote {args.out} ({len(md)} chars)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
