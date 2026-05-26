#!/usr/bin/env python3
"""mvp_report.py — MVP demo report generator.

Renders one Markdown report from the artefacts captured by
`run_mvp_demo.sh`. Supports two layouts:

  * **Dual-mode (preferred for issue #46 verdict matrix):** the
    results dir contains both `baseline/` and `asap/` subdirectories
    (`run_mvp_demo.sh --mode both`). The report renders side-by-side
    rows for each criterion + a reduction column (X/Y/Z numbers per
    the runbook table).

  * **Single-mode (back-compat):** the results dir contains a single
    pipeline's artefacts at the top level (legacy layout) OR exactly
    one of `baseline/` / `asap/` is populated. The report degrades
    gracefully to the original single-pipeline rendering.

In both layouts the per-pipeline subdir shape is:

    <pipeline-dir>/
        controller-emitted-configs/{STATUS,agent.bootstrap.yaml,...}
        measurements/{stages.csv, per_edge_bandwidth.csv,
                       replay.jsonl, accuracy.csv, s3_cost.csv}
        freshness/{raw.csv, warm.csv, archive.csv}
        ad-hoc/{count_api_series.json, topk_5xx_by_zone.json,
                cold_payments.json, cold_payments.verdict, ...}
        thanos-compact/{before.minio.jsonl, after.minio.jsonl,
                         metrics.before.txt, metrics.after.txt,
                         health.txt, SKIPPED?}
            (Phase δ.1: replaced the legacy `compactor/` subdir which
             held the deleted gorilla-compactor binary's artefacts;
             thanos-compact now drives archive-tier compaction.)

Pure stdlib. Idempotent — re-running over the same CSVs reproduces
the same MD.

Output: `<results_dir>/MVP_REPORT.md`.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
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


# ── ④ accuracy: per-sketch-family contract ──────────────────────
#
# Shared across the parallel agents working on the MVP demo (see issue
# #46). Each metric in `mvp-workload.yaml` binds to exactly one sketch
# family with a known per-family accuracy bound. The ④ accuracy
# verdict aggregates across all 6 (5 sketches + raw passthrough) — the
# overall verdict is PASS only if EVERY family is within bound.
#
# Columns in `accuracy.csv` we read:
#
#   - kind  (quantile | topk | count_unique | sum | "")
#   - query (the PromQL string — used to back-resolve which metric, and
#            therefore which sketch family, the row belongs to)
#   - rel_err (relative error — populated for quantile/count_unique/sum)
#   - recall (top-K recall — populated for topk)
#
# Family bounds:
#
#   raw               — exact equality (rel_err == 0)
#   DDSketch          — relative-error quantile, ε ≤ 0.01
#   KLL               — rank-error quantile, ε ≤ 0.005 (1/k where
#                       k=200 default). NB: today's reducer reports a
#                       *value* relative-error for KLL queries; the
#                       rank-error column is not surfaced. We document
#                       the gap inline and check rel_err against the
#                       same 0.005 threshold as a proxy.
#   HLL               — relative-error cardinality, ε ≤ 0.0325 (p=12)
#   CountSketch       — top-K recall, ε ≥ 0.85 (recall is a HIGHER-is-
#                       better metric — so the bound is an inequality
#                       in the opposite direction). The CMS-Heap
#                       pattern (Cormode & Muthukrishnan 2005) means a
#                       workload may legitimately bind `top_endpoint_qps`
#                       to CountMinSketch instead — the planner accepts
#                       either family for a TopK metric. CountSketch is
#                       the canonical (unbiased) pick; the report row's
#                       family label notes the alternative without
#                       changing the recall bound (both families clear
#                       the same recall threshold for a Zipfian heavy-
#                       hitter workload).
#   CountMinSketch    — additive-error frequency, ε / total ≤ 1/w. We
#                       don't have a per-row `additive_err` column; we
#                       reuse `rel_err` as a proxy and compare against
#                       a hardcoded ceiling (0.02 = 1/w with w=64) so
#                       the cell renders without surfacing a stale
#                       UNKNOWN. This is a documented approximation —
#                       the reducer does not yet report w or total.
#
# `metric_name` keys match the workload contract; the renderer
# extracts the metric out of the PromQL string in `query` because the
# accuracy CSV has no dedicated metric column.

SKETCH_FAMILIES = [
    # (family, metric_name, query_class_label, bound_text, bound_value, error_column)
    #
    # The top-K row's family label is intentionally "CountSketch" —
    # the canonical (unbiased) pick and the contract-row default. A
    # workload may instead bind `top_endpoint_qps` to CountMinSketch
    # via the CMS-Heap pattern (Cormode & Muthukrishnan 2005) — the
    # planner's capability matrix accepts either family for a TopK
    # statistic. The recall bound (≥0.85) is identical for both
    # families on the demo's Zipfian heavy-hitter workload, so the
    # verdict logic is family-agnostic. `top_k_family_label()`
    # detects the actual chosen family at render time when a
    # workload spec is available.
    ("DDSketch",       "http_requests_total_latency_ms", "quantile",     "≤0.01",    0.01,   "rel_err"),
    ("KLL",            "request_size_bytes",     "quantile",     "≤0.005",   0.005,  "rel_err"),
    ("HLL",            "unique_users_per_min",   "cardinality",  "≤0.0325",  0.0325, "rel_err"),
    ("CountSketch",    "top_endpoint_qps",       "top-K",        "≥0.85",    0.85,   "recall"),
    ("CountMinSketch", "endpoint_request_freq",  "frequency",    "≤theoretical (1/w)", 0.02,  "rel_err"),
    ("raw",            "http_requests_total",    "sum_rate",     "exact (=0)", 0.0,  "rel_err"),
]

# Family labels accepted for the top-K row. Both clear the same recall
# bound on the demo's Zipfian workload; which one a given run actually
# uses is determined by the workload spec's `sketch_family_override`.
_TOPK_ACCEPTED_FAMILIES: tuple[str, ...] = ("CountSketch", "CountMinSketch")


def top_k_family_label(workload_yaml_path: str | None = None) -> str:
    """Return the rendered family label for the top-K row.

    When `workload_yaml_path` is supplied AND the file declares a
    `sketch_family_override` for `top_endpoint_qps`, the label reflects
    the chosen family. Otherwise the canonical "CountSketch" label is
    used (with a "(or CountMin via CMS-Heap)" annotation so the reader
    knows the alternative is valid).
    """
    chosen = _detect_top_k_family(workload_yaml_path)
    if chosen == "CountMinSketch":
        return "CountMinSketch (CMS-Heap)"
    if chosen == "CountSketch":
        return "CountSketch"
    # No spec available, or no override declared. Show the canonical
    # pick + the alternative inline.
    return "CountSketch (or CountMin via CMS-Heap)"


def _detect_top_k_family(workload_yaml_path: str | None) -> str | None:
    """Best-effort parse of `mvp-workload.yaml` to find which family
    the workload pinned for `top_endpoint_qps`. Returns the family
    string ("CountSketch" / "CountMinSketch") or None if no spec
    available or the metric isn't pinned.
    """
    if not workload_yaml_path or not os.path.exists(workload_yaml_path):
        return None
    try:
        with open(workload_yaml_path, "r") as f:
            text = f.read()
    except OSError:
        return None
    # Tiny, dependency-free YAML probe — we only need to find the
    # `sketch_family_override` line under the `top_endpoint_qps` block.
    in_block = False
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("- metric_name:"):
            in_block = "top_endpoint_qps" in stripped
            continue
        if in_block and stripped.startswith("sketch_family_override:"):
            value = stripped.split(":", 1)[1].strip()
            if value in _TOPK_ACCEPTED_FAMILIES:
                return value
            return None
    return None

# Reverse map: metric → family entry. Used by the accuracy renderer to
# group accuracy.csv rows by family.
_METRIC_TO_FAMILY: dict[str, tuple[str, str, str, str, float, str]] = {
    metric: (family, metric, klass, bound, bv, col)
    for (family, metric, klass, bound, bv, col) in SKETCH_FAMILIES
}


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


def _fmt_num(v: float, fmt: str, missing: str = "—") -> str:
    if _is_nan(v):
        return missing
    return fmt.format(v)


def _reduction_pct(baseline: float, target: float) -> float:
    """Return (baseline - target) / baseline as a fraction.

    Sign convention: positive = target REDUCES the metric vs. baseline
    (lower is better, e.g. bandwidth, latency, resource). NaN if
    baseline missing or zero.
    """
    if _is_nan(baseline) or _is_nan(target) or baseline == 0:
        return float("nan")
    return (baseline - target) / baseline


# ── per-pipeline data bag ───────────────────────────────────────


class PipelineData:
    """All the artefacts loaded for a single pipeline run.

    Constructed once per pipeline (baseline + asap) in dual-mode and
    once in single-mode. Keeps the renderers free of file-system
    accesses.
    """

    def __init__(self, root: str, label: str):
        self.label = label
        self.root = root
        self.measurements_dir = os.path.join(root, "measurements")
        self.fresh_dir = os.path.join(root, "freshness")
        self.adhoc_dir = os.path.join(root, "ad-hoc")
        # Phase δ.1: subdir renamed compactor/ → thanos-compact/. The
        # `compactor_dir` attribute name is kept for renderer
        # back-compat; only the on-disk path moved.
        self.compactor_dir = os.path.join(root, "thanos-compact")
        self.emitted_dir = os.path.join(root, "controller-emitted-configs")

        self.stages_rows = _read_csv(os.path.join(self.measurements_dir, "stages.csv"))
        self.edge_rows = _read_csv(os.path.join(self.measurements_dir, "per_edge_bandwidth.csv"))
        self.accuracy_rows = _read_csv(os.path.join(self.measurements_dir, "accuracy.csv"))
        self.replay_rows = _read_jsonl(os.path.join(self.measurements_dir, "replay.jsonl"))
        self.emitted_status = _read_text(os.path.join(self.emitted_dir, "STATUS")).strip() or "unknown"

    @property
    def is_populated(self) -> bool:
        """True if there's any per-pipeline data on disk."""
        return any((self.stages_rows, self.edge_rows, self.replay_rows))


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
        md.append(
            "| {stage} | {cpu} | {rss} | {nin} | {nout} | {disk} |".format(
                stage=stage,
                cpu=_fmt_num(d.get("cpu_cores", float("nan")), "{:.3f}"),
                rss=_fmt_num(d.get("rss_mib", float("nan")), "{:.1f}"),
                nin=_fmt_num(d.get("net_in_kibps", float("nan")), "{:.1f}"),
                nout=_fmt_num(d.get("net_out_kibps", float("nan")), "{:.1f}"),
                disk=_fmt_num(d.get("disk_mib", float("nan")), "{:.1f}"),
            )
        )
    md.append("")
    return md


def render_section_1_stage_table_dual(
    base: PipelineData,
    asap: PipelineData,
) -> list[str]:
    """Dual-mode §1: TWO rows per stage (baseline / asap) + reduction."""
    md: list[str] = []
    md.append("## §1 Stage-separated resource table (baseline vs ASAP)")
    md.append("")
    md.append(
        "Per-stage TOTAL across all containers in that stage, side-by-"
        "side for baseline (OTel→Prometheus) vs ASAP (controller-"
        "driven sketches + Gorilla-S3). Reduction column is "
        "(baseline − asap) / baseline. Positive = ASAP uses fewer "
        "resources at that stage."
    )
    md.append("")

    base_aggs = _aggregate_stages(base.stages_rows)
    asap_aggs = _aggregate_stages(asap.stages_rows)
    if not base_aggs and not asap_aggs:
        md.append("_stages.csv missing for both pipelines — no rows to render_")
        md.append("")
        return md

    md.append(
        "| Stage | Pipeline | CPU (cores) | RSS (MiB) | "
        "Net in (KiB/s) | Net out (KiB/s) | Disk (MiB) |"
    )
    md.append("|---|---|---|---|---|---|---|")
    for stage in STAGE_ORDER:
        b = base_aggs.get(stage, {})
        a = asap_aggs.get(stage, {})
        md.append(
            "| {s} | baseline | {cpu} | {rss} | {nin} | {nout} | {disk} |".format(
                s=stage,
                cpu=_fmt_num(b.get("cpu_cores", float("nan")), "{:.3f}"),
                rss=_fmt_num(b.get("rss_mib", float("nan")), "{:.1f}"),
                nin=_fmt_num(b.get("net_in_kibps", float("nan")), "{:.1f}"),
                nout=_fmt_num(b.get("net_out_kibps", float("nan")), "{:.1f}"),
                disk=_fmt_num(b.get("disk_mib", float("nan")), "{:.1f}"),
            )
        )
        md.append(
            "| {s} | asap     | {cpu} | {rss} | {nin} | {nout} | {disk} |".format(
                s=stage,
                cpu=_fmt_num(a.get("cpu_cores", float("nan")), "{:.3f}"),
                rss=_fmt_num(a.get("rss_mib", float("nan")), "{:.1f}"),
                nin=_fmt_num(a.get("net_in_kibps", float("nan")), "{:.1f}"),
                nout=_fmt_num(a.get("net_out_kibps", float("nan")), "{:.1f}"),
                disk=_fmt_num(a.get("disk_mib", float("nan")), "{:.1f}"),
            )
        )
        # Reduction row.
        reductions = []
        for k in ("cpu_cores", "rss_mib", "net_in_kibps", "net_out_kibps", "disk_mib"):
            r = _reduction_pct(b.get(k, float("nan")), a.get(k, float("nan")))
            reductions.append(_fmt_num(r * 100.0, "{:+.1f}%") if not _is_nan(r) else "—")
        md.append(
            "| {s} | _reduction_ | {0} | {1} | {2} | {3} | {4} |".format(
                *reductions, s=stage
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


# ── ④ per-sketch-family accuracy ─────────────────────────────────


def _row_metric_name(row: dict) -> str:
    """Best-effort metric-name extraction from an accuracy.csv row.

    The reducer doesn't emit a dedicated `metric` column, but the
    PromQL `query` string contains the metric. We do a substring scan
    for each metric in our contract — first hit wins. The contract's
    metric names are distinct so substring collisions aren't a concern
    in practice (the longest match would also work; substring scan is
    enough for the 6 names we care about).
    """
    q = row.get("query", "") or ""
    for metric in _METRIC_TO_FAMILY:
        if metric in q:
            return metric
    return ""


def _aggregate_family_rows(
    accuracy_rows: list[dict],
) -> dict[str, dict[str, Any]]:
    """Group accuracy.csv rows by sketch family.

    Returns a dict keyed by family name; values are
    `{n: int, errors: [floats], recalls: [floats], metric: str}`.
    """
    by_family: dict[str, dict[str, Any]] = {}
    for row in accuracy_rows:
        metric = _row_metric_name(row)
        if not metric:
            continue
        family, _metric, _kind, _bound, _bv, _col = _METRIC_TO_FAMILY[metric]
        bag = by_family.setdefault(family, {
            "metric": metric, "errors": [], "recalls": [], "n": 0,
        })
        bag["n"] += 1
        for col, dest in (("rel_err", "errors"), ("recall", "recalls")):
            raw = row.get(col, "")
            if raw == "" or raw is None:
                continue
            try:
                v = float(raw)
            except (TypeError, ValueError):
                continue
            if not _is_nan(v):
                bag[dest].append(v)
    return by_family


def _family_verdict(
    family: str,
    bound_value: float,
    error_column: str,
    bag: dict[str, Any],
) -> tuple[str, float, str]:
    """Compute (verdict, observed_metric, formatted_observed) for a
    single sketch family.

    `error_column` selects which list in `bag` we reduce ("errors" for
    rel_err / additive proxy, "recalls" for top-K).
    """
    if error_column == "recall":
        recalls = bag.get("recalls") or []
        if not recalls:
            return "UNKNOWN", float("nan"), "—"
        observed = statistics.median(recalls)
        verdict = "PASS" if observed >= bound_value else "FAIL"
        return verdict, observed, f"recall={observed:.3f}"
    # rel_err / additive proxy paths.
    errs = bag.get("errors") or []
    if not errs:
        return "UNKNOWN", float("nan"), "—"
    observed = statistics.median(errs)
    if family == "raw":
        verdict = "PASS" if observed == 0.0 else "FAIL"
        return verdict, observed, f"{observed:.4f}"
    verdict = "PASS" if observed <= bound_value else "FAIL"
    suffix = ""
    if family == "KLL":
        suffix = " (rank-err proxy: reducer reports value rel-err)"
    elif family == "CountMinSketch":
        suffix = " (additive proxy vs 1/w ≈ 0.02 ceiling)"
    return verdict, observed, f"{observed:.4f}{suffix}"


def criterion_accuracy_per_sketch(
    accuracy_rows: list[dict],
) -> tuple[str, list[dict], str]:
    """Per-sketch-family ④ accuracy verdict.

    Returns `(aggregate_verdict, per_family_rows, summary_line)`.

    `per_family_rows` is one dict per family (in SKETCH_FAMILIES
    order) with keys: `family`, `metric`, `query_class`, `n`,
    `bound`, `observed`, `verdict`. The aggregate verdict bubbles
    UP: any UNKNOWN family → UNKNOWN aggregate; any FAIL family →
    FAIL aggregate; otherwise PASS.
    """
    if not accuracy_rows:
        return "UNKNOWN", [], "accuracy.csv empty (warm tier may not have flushed)"

    by_family = _aggregate_family_rows(accuracy_rows)
    per_family_rows: list[dict] = []
    n_pass = 0
    n_fail = 0
    n_unknown = 0
    for family, metric, klass, bound, bound_value, error_column in SKETCH_FAMILIES:
        bag = by_family.get(family) or {"metric": metric, "errors": [], "recalls": [], "n": 0}
        verdict, _observed, observed_str = _family_verdict(
            family, bound_value, error_column, bag,
        )
        if verdict == "PASS":
            n_pass += 1
        elif verdict == "FAIL":
            n_fail += 1
        else:
            n_unknown += 1
        per_family_rows.append({
            "family": family,
            "metric": metric,
            "query_class": klass,
            "n": int(bag.get("n", 0)),
            "bound": bound,
            "observed": observed_str,
            "verdict": verdict,
        })

    if n_unknown > 0:
        aggregate = "UNKNOWN"
    elif n_fail > 0:
        aggregate = "FAIL"
    else:
        aggregate = "PASS"
    summary = (
        f"{n_pass}/{len(SKETCH_FAMILIES)} families within bound; "
        f"FAIL={n_fail} UNKNOWN={n_unknown}"
    )
    return aggregate, per_family_rows, summary


def render_accuracy_per_sketch_table(
    per_family_rows: list[dict],
) -> list[str]:
    """Render the §④ per-sketch-family table — 6 rows (5 sketches +
    raw). Used by both single-mode and dual-mode renderers."""
    md: list[str] = []
    md.append(
        "| Sketch family | Metric | Query class | n | "
        "rel-err / recall | within ε bound? | Verdict |"
    )
    md.append("|---|---|---|---|---|---|---|")
    for r in per_family_rows:
        within = "—"
        if r["verdict"] == "PASS":
            within = f"yes ({r['bound']})"
        elif r["verdict"] == "FAIL":
            within = f"no ({r['bound']})"
        else:
            within = f"unknown ({r['bound']})"
        md.append(
            "| {family} | {metric} | {klass} | {n} | {obs} | {within} | {v} |".format(
                family=r["family"],
                metric=r["metric"],
                klass=r["query_class"],
                n=r["n"],
                obs=r["observed"],
                within=within,
                v=r["verdict"],
            )
        )
    return md


def criterion_cold_fallback(adhoc_dir: str) -> tuple[str, str, dict]:
    response_path = os.path.join(adhoc_dir, "cold_payments.json")
    verdict_path = os.path.join(adhoc_dir, "cold_payments.verdict")
    curl_path = os.path.join(adhoc_dir, "cold_payments.curlstats")
    body = _read_json(response_path)
    raw_text = _read_text(response_path)
    verdict_marker = _read_text(verdict_path).strip()
    curlstats = _read_text(curl_path).strip()
    http_code = ""
    if curlstats:
        match = re.search(r'"http_code"\s*:\s*"?([0-9]{3})"?', curlstats)
        if match:
            http_code = match.group(1)

    if body is None and not raw_text:
        return "UNKNOWN", "cold_payments.json missing", {}

    has_marker = "thanos_archive" in raw_text or "gorilla_archive" in raw_text
    http_ok = http_code == "200"
    if has_marker and http_ok:
        verdict = "PASS"
        line = (
            "archive `data_source` marker present with HTTP 200 — "
            "archive engine served the ad-hoc query."
        )
    elif has_marker and http_code:
        verdict = "FAIL"
        line = f"archive marker present but HTTP status was {http_code}"
    elif verdict_marker == "PASS" and not http_ok:
        verdict = "FAIL"
        line = f"driver wrote PASS but cold-fallback HTTP status was {http_code or 'unknown'}"
    elif verdict_marker == "PASS":
        # Driver thought it was OK but the marker grep missed —
        # surface as PARTIAL.
        verdict = "PARTIAL"
        line = "driver wrote PASS but no archive marker found in body"
    else:
        verdict = "FAIL"
        line = "no archive `data_source` marker in cold_payments.json"
    if curlstats:
        line += f"  · curl: {curlstats}"
    return verdict, line, {
        "has_marker": has_marker,
        "marker": verdict_marker,
        "http_code": http_code,
    }


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


# ── §2 dual-mode ─────────────────────────────────────────────────


def render_section_2_verdict_dual(
    base: PipelineData,
    asap: PipelineData,
) -> list[str]:
    """Dual-mode §2: per-criterion baseline-vs-asap + reduction column.

    Reduction sign convention: positive = ASAP wins (lower is
    better). For accuracy we don't compute a reduction — both
    pipelines have separate semantics (baseline = exact-by-
    construction, ASAP = bounded sketch error).
    """
    md: list[str] = []
    md.append("## §2 Per-criterion verdict (baseline vs ASAP)")
    md.append("")
    md.append(
        "Five empirical claims from issue #46. Reduction is "
        "(baseline − asap) / baseline; positive = ASAP wins."
    )
    md.append("")

    # ① Bandwidth — per cut edge.
    base_means = _per_edge_mean_bytes_per_s(base.edge_rows)
    asap_means = _per_edge_mean_bytes_per_s(asap.edge_rows)

    md.append("### ① Bandwidth (mean B/s per cut edge)")
    md.append("")
    md.append("| Edge | Baseline B/s | ASAP B/s | Reduction |")
    md.append("|---|---|---|---|")
    any_bw = False
    for e in EDGE_ORDER:
        b = base_means.get(e, float("nan"))
        a = asap_means.get(e, float("nan"))
        r = _reduction_pct(b, a)
        if not _is_nan(b) or not _is_nan(a):
            any_bw = True
        md.append(
            "| {edge} | {b} | {a} | {r} |".format(
                edge=e,
                b=_fmt_num(b, "{:.1f}"),
                a=_fmt_num(a, "{:.1f}"),
                r=_fmt_num(r * 100.0, "{:+.1f}%") if not _is_nan(r) else "—",
            )
        )
    md.append("")
    bw_verdict = (
        "UNKNOWN" if not any_bw else
        "PASS" if any(
            _reduction_pct(base_means.get(e, float("nan")),
                           asap_means.get(e, float("nan"))) > 0
            for e in EDGE_ORDER
        ) else "FAIL"
    )
    md.append(f"**Verdict ①:** {bw_verdict}")
    md.append("")

    # ② Query latency — per query class.
    md.append("### ② Query latency (p99 per class)")
    md.append("")
    md.append("| Class | Baseline p99 (ms) | ASAP p99 (ms) | Reduction |")
    md.append("|---|---|---|---|")
    base_lat = _per_class_latency_p50_p99(base.replay_rows)
    asap_lat = _per_class_latency_p50_p99(asap.replay_rows)
    any_lat = False
    for cls, _, _ in QUERY_CLASSES:
        b_p99 = base_lat.get(cls, {}).get("p99", float("nan"))
        a_p99 = asap_lat.get(cls, {}).get("p99", float("nan"))
        r = _reduction_pct(b_p99, a_p99)
        if not _is_nan(b_p99) or not _is_nan(a_p99):
            any_lat = True
        md.append(
            "| {cls} | {b} | {a} | {r} |".format(
                cls=cls,
                b=_fmt_num(b_p99, "{:.1f}"),
                a=_fmt_num(a_p99, "{:.1f}"),
                r=_fmt_num(r * 100.0, "{:+.1f}%") if not _is_nan(r) else "—",
            )
        )
    md.append("")
    lat_verdict = "UNKNOWN" if not any_lat else "CAPTURED"
    md.append(f"**Verdict ②:** {lat_verdict}")
    md.append("")

    # ③ Combined resource — Σ stages CPU + RSS.
    base_aggs = _aggregate_stages(base.stages_rows)
    asap_aggs = _aggregate_stages(asap.stages_rows)
    base_cpu = sum(d.get("cpu_cores", 0.0) for d in base_aggs.values())
    base_rss = sum(d.get("rss_mib", 0.0) for d in base_aggs.values())
    asap_cpu = sum(d.get("cpu_cores", 0.0) for d in asap_aggs.values())
    asap_rss = sum(d.get("rss_mib", 0.0) for d in asap_aggs.values())
    md.append("### ③ Combined e2e resource (Σ stages)")
    md.append("")
    md.append("| Metric | Baseline | ASAP | Δ (asap − baseline) | Reduction |")
    md.append("|---|---|---|---|---|")
    cpu_delta = asap_cpu - base_cpu
    rss_delta = asap_rss - base_rss
    cpu_red = _reduction_pct(base_cpu, asap_cpu)
    rss_red = _reduction_pct(base_rss, asap_rss)
    md.append(
        f"| total CPU cores | {base_cpu:.3f} | {asap_cpu:.3f} | {cpu_delta:+.3f} | "
        f"{_fmt_num(cpu_red * 100.0, '{:+.1f}%') if not _is_nan(cpu_red) else '—'} |"
    )
    md.append(
        f"| total RSS MiB | {base_rss:.1f} | {asap_rss:.1f} | {rss_delta:+.1f} | "
        f"{_fmt_num(rss_red * 100.0, '{:+.1f}%') if not _is_nan(rss_red) else '—'} |"
    )
    md.append("")
    res_verdict = "UNKNOWN" if (not base_aggs and not asap_aggs) else "CAPTURED"
    md.append(f"**Verdict ③:** {res_verdict}  (sign convention: ASAP `Δ` rendered with sign)")
    md.append("")

    # ④ Accuracy — ASAP-only (baseline = exact by construction).
    # Per-sketch-family breakout: 5 sketch rows + 1 raw row. Aggregate
    # verdict is PASS only if every family is within its bound; any
    # UNKNOWN bubbles up to UNKNOWN, any FAIL bubbles up to FAIL.
    acc_v, per_family_rows, acc_summary = criterion_accuracy_per_sketch(
        asap.accuracy_rows,
    )
    md.append("### ④ Accuracy (per sketch family)")
    md.append("")
    md.append(
        "ASAP-only — baseline is exact by construction. The table "
        "breaks out one row per sketch family (DDSketch / KLL / HLL "
        "/ CountSketch / CountMinSketch) plus a 6th `raw` row for "
        "the passthrough metric. The aggregate verdict ④ is PASS "
        "only if all 5 sketch families are within their per-family "
        "ε bound AND raw is exactly equal."
    )
    md.append("")
    md.extend(render_accuracy_per_sketch_table(per_family_rows))
    md.append("")
    md.append(f"**Verdict ④:** {acc_v}  · {acc_summary}")
    md.append("")

    # ⑤ Cold-fallback — ASAP only.
    cold_v, cold_line, _ = criterion_cold_fallback(asap.adhoc_dir)
    md.append("### ⑤ Cold-fallback (archive marker + HTTP 200 — ASAP only)")
    md.append("")
    md.append(f"**Verdict ⑤:** {cold_v}  · {cold_line}")
    md.append("")

    # ⑥ Freshness — per-path Δ. Show baseline + asap side by side.
    md.append("### ⑥ Freshness (p50 per path)")
    md.append("")
    md.append("| Path | Baseline p50 (ms) | ASAP p50 (ms) | Δ (asap − baseline) |")
    md.append("|---|---|---|---|")
    base_fr_summary: dict[str, dict[str, float]] = {}
    asap_fr_summary: dict[str, dict[str, float]] = {}
    _, _, base_fr_summary = criterion_freshness(base.fresh_dir)  # type: ignore
    _, _, asap_fr_summary = criterion_freshness(asap.fresh_dir)  # type: ignore
    if not isinstance(base_fr_summary, dict):
        base_fr_summary = {}
    if not isinstance(asap_fr_summary, dict):
        asap_fr_summary = {}
    any_fr = False
    for path in ("raw", "warm", "archive"):
        b = base_fr_summary.get(path, {}).get("p50", float("nan"))
        a = asap_fr_summary.get(path, {}).get("p50", float("nan"))
        d = (a - b) if (not _is_nan(a) and not _is_nan(b)) else float("nan")
        if not _is_nan(b) or not _is_nan(a):
            any_fr = True
        md.append(
            "| {p} | {b} | {a} | {d} |".format(
                p=path,
                b=_fmt_num(b, "{:.0f}"),
                a=_fmt_num(a, "{:.0f}"),
                d=_fmt_num(d, "{:+.0f}"),
            )
        )
    md.append("")
    # Use the criterion's underlying PASS/FAIL/UNKNOWN evaluation
    # (warm p50 ≤ 30s, archive optional ≤ 90s). The original
    # CAPTURED/UNKNOWN rollup was a placeholder while ⑥ was
    # blocked on probe routing — once the data lands we want the
    # honest pass/fail signal to surface here too. ASAP-side gates
    # the verdict because the warm-tier sketch is the under-test
    # path; baseline raw is just the comparator.
    asap_verdict, _, _ = criterion_freshness(asap.fresh_dir)  # type: ignore
    if not any_fr:
        asap_verdict = "UNKNOWN"
    md.append(f"**Verdict ⑥:** {asap_verdict}")
    md.append("")

    return md


# ── §3 per-query-class breakdown ────────────────────────────────


def render_section_3_per_class(
    replay_rows: list[dict],
    accuracy_rows: list[dict],
    emitted_status: str,
) -> list[str]:
    md: list[str] = []
    md.append("### §3.1 Per-query-class breakdown")
    md.append("")
    md.append(
        "Three canonical query classes from "
        "`deploy/mvp-singlenode/configs/mvp-workload.yaml`. Sketch + stage "
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

        plan_note = plan_annotation.get(cls, "—")
        if emitted_status != "live":
            plan_note += "  *(plan from workload spec; emitter " + emitted_status + ")*"

        md.append(
            "| {cls} | {plan} | {p50} | {p99} | {err} | {n} |".format(
                cls=cls,
                plan=plan_note,
                p50=_fmt_num(p50, "{:.1f}"),
                p99=_fmt_num(p99, "{:.1f}"),
                err=_fmt_num(med_err, "{:.4f}"),
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


def _extract_compact_iter_count(metrics_path: str) -> float:
    """Parse `thanos_compact_iterations_total` out of a Prometheus
    text-format /metrics dump. Returns NaN if the line is missing.

    Phase δ.1: replaces the legacy gorilla-compactor `dry_run.json`
    structured output. thanos-compact exposes its sweep counter via
    /metrics in the standard Prometheus exposition format."""
    if not os.path.exists(metrics_path):
        return float("nan")
    try:
        with open(metrics_path, "r") as f:
            for line in f:
                line = line.rstrip("\n")
                if not line or line.startswith("#"):
                    continue
                # `thanos_compact_iterations_total <count>` (no labels).
                if line.startswith("thanos_compact_iterations_total "):
                    parts = line.split()
                    if len(parts) >= 2:
                        try:
                            return float(parts[1])
                        except ValueError:
                            return float("nan")
    except OSError:
        return float("nan")
    return float("nan")


def render_section_5_compaction(compactor_dir: str) -> list[str]:
    """§5 Compaction effect — Phase δ.1 thanos-compact replacement.

    The directory layout is `thanos-compact/` (renamed from
    `compactor/`). thanos-compact runs continuously in `--wait` mode;
    Phase 6 of the driver captures one before/after snapshot pair plus
    the iteration counter."""
    md: list[str] = []
    md.append("## §5 Compaction effect")
    md.append("")
    skipped = os.path.exists(os.path.join(compactor_dir, "SKIPPED"))
    if skipped:
        reason = _read_text(os.path.join(compactor_dir, "SKIPPED")).strip()
        md.append(f"_thanos-compact SKIPPED: {reason}_")
        md.append("")
        return md

    before = _count_minio_objects(os.path.join(compactor_dir, "before.minio.jsonl"))
    after = _count_minio_objects(os.path.join(compactor_dir, "after.minio.jsonl"))

    iter_before = _extract_compact_iter_count(
        os.path.join(compactor_dir, "metrics.before.txt"))
    iter_after = _extract_compact_iter_count(
        os.path.join(compactor_dir, "metrics.after.txt"))

    md.append(
        "Phase δ.1: archive-tier compaction is now performed by the "
        "stock **`thanos-compact`** sidecar (replacing the deleted "
        "`gorilla-compactor` Rust binary). Unlike the previous "
        "concat-only design, thanos-compact does **decode + "
        "re-encode** — that's a real CPU cost during compaction "
        "sweeps, but it gives better compression on top of block "
        "consolidation, plus downsampled tiers (raw / 5m / 1h) "
        "for free. Storage savings reported below therefore include "
        "both block-count consolidation AND re-encoded compression."
    )
    md.append("")
    md.append("| Stage | Object count | Total bytes |")
    md.append("|---|---|---|")
    md.append(f"| before | {before[0]} | {before[1]} |")
    md.append(f"| after  | {after[0]}  | {after[1]} |")
    md.append("")

    if not _is_nan(iter_before) or not _is_nan(iter_after):
        md.append(
            f"`thanos_compact_iterations_total`: "
            f"{_fmt_num(iter_before, '{:.0f}')} → "
            f"{_fmt_num(iter_after, '{:.0f}')} "
            f"(at least one sweep observed during the demo window if "
            f"the `after` value > `before`)."
        )
        md.append("")
    else:
        md.append(
            "_thanos-compact `/metrics` snapshot not captured — see "
            "`thanos-compact/metrics.before.err` and "
            "`thanos-compact/metrics.after.err`._"
        )
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


def render_section_7_caveats(dual_mode: bool = False) -> list[str]:
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
    if dual_mode:
        md.append(
            "* **Sequential pipelines, not side-by-side.** The "
            "baseline and ASAP cycles run back-to-back with a full "
            "`docker compose down -v` + 10s settle between them. "
            "Side-by-side execution would contaminate per-pipeline "
            "resource numbers (both stacks consume host CPU + RAM at "
            "the same time), so the driver explicitly serialises."
        )
    else:
        md.append(
            "* **B0 Prometheus reference is opt-in.** The driver does "
            "NOT bring up B0 in the same compose stack as the ASAP "
            "backend (port collision on 19090). To get an A-vs-B "
            "comparison row, run `--mode both` so the driver does "
            "two cycles with full teardown between."
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
    elif status == "n/a-baseline":
        md.append(
            "Baseline pipeline has no controller — agents load "
            "`asap-otel-agent-b0-victoriametrics.yaml` directly "
            "(was `…-b0-prometheus.yaml` pre Step 2g 2026-05). "
            "This section is not applicable to the baseline run."
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


def _detect_layout(results_dir: str) -> str:
    """Return one of: 'dual', 'baseline-only', 'asap-only', 'legacy'.

    'dual'         — both `baseline/` and `asap/` subdirs populated
    'baseline-only'— only `baseline/` subdir populated
    'asap-only'    — only `asap/` subdir populated
    'legacy'       — neither subdir present; results_dir IS the
                      pipeline root (current single-mode behaviour)
    """
    base_dir = os.path.join(results_dir, "baseline")
    asap_dir = os.path.join(results_dir, "asap")
    base_pop = os.path.isdir(base_dir) and os.path.isdir(
        os.path.join(base_dir, "measurements"))
    asap_pop = os.path.isdir(asap_dir) and os.path.isdir(
        os.path.join(asap_dir, "measurements"))
    if base_pop and asap_pop:
        return "dual"
    if base_pop:
        return "baseline-only"
    if asap_pop:
        return "asap-only"
    return "legacy"


def render_markdown_single(
    pipeline_root: str,
    num_producers: int,
    per_agent_cardinality: int,
    label_for_header: str = "single-cell",
) -> str:
    """Render the original single-pipeline report. Used both for
    legacy layout (results_dir == pipeline_root) and for the single-
    populated-subdir fallback."""
    if not os.path.isdir(pipeline_root):
        return f"# MVP report — results dir missing ({pipeline_root})\n"

    p = PipelineData(pipeline_root, label_for_header)

    md: list[str] = []
    md.append("# ASAPCollector MVP demo — issue #46 (controller-driven multi-stage)")
    md.append("")
    md.append(
        f"Single-pipeline run ({label_for_header}): 10 producers → 2 agents "
        "→ 1 gateway → 1 backend (+ MinIO archive). Controller plans "
        "from `deploy/mvp-singlenode/configs/mvp-workload.yaml`; per-stage configs "
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

    md.extend(render_section_1_stage_table(p.stages_rows))

    # §2 verdict table.
    bw_v, bw_line, _ = criterion_bandwidth(p.edge_rows)
    lat_v, lat_line, _ = criterion_latency(p.replay_rows)
    res_v, res_line, _ = criterion_combined_resource(p.stages_rows)
    # ④ Aggregate verdict comes from the per-sketch-family rollup
    # (PASS only if all 5 sketches within bound + raw exact). The
    # per-family detail renders in §3 below.
    acc_v, per_family_rows, acc_summary = criterion_accuracy_per_sketch(
        p.accuracy_rows,
    )
    cold_v, cold_line, _ = criterion_cold_fallback(p.adhoc_dir)
    fresh_v, fresh_line, _ = criterion_freshness(p.fresh_dir)

    md.append("## §2 Per-criterion verdict (6 criteria)")
    md.append("")
    md.append("| # | Criterion | Verdict | Detail |")
    md.append("|---|---|---|---|")
    md.append(f"| 1 | Bandwidth (per-edge B/s) | **{bw_v}** | {bw_line} |")
    md.append(f"| 2 | Query latency (p50/p99 per class) | **{lat_v}** | {lat_line} |")
    md.append(f"| 3 | Combined resource (sum of stages) | **{res_v}** | {res_line} |")
    md.append(
        f"| 4 | Accuracy (per sketch family, see §3) | **{acc_v}** | "
        f"{acc_summary} |"
    )
    md.append(f"| 5 | Cold-fallback (archive marker + HTTP 200) | **{cold_v}** | {cold_line} |")
    md.append(f"| 6 | Freshness (p50/p99 per path) | **{fresh_v}** | {fresh_line} |")
    md.append("")

    # §3 per-sketch accuracy detail (5 sketch rows + raw) +
    # per-query-class breakdown.
    md.append("## §3 Accuracy (per sketch family)")
    md.append("")
    md.append(
        "Per-sketch ④ accuracy detail. The aggregate ④ verdict in "
        "§2 PASS-iff-all-pass; rows below show each family's "
        "observed error / recall against its hardcoded bound. The "
        "6th `raw` row is the exact-passthrough metric."
    )
    md.append("")
    md.extend(render_accuracy_per_sketch_table(per_family_rows))
    md.append("")
    md.append(f"**Verdict ④:** {acc_v}  · {acc_summary}")
    md.append("")

    # §3 (cont.) — per-query-class breakdown (the original §3).
    md.extend(render_section_3_per_class(p.replay_rows, p.accuracy_rows, p.emitted_status))
    md.extend(render_section_4_postings(p.adhoc_dir))
    md.extend(render_section_5_compaction(p.compactor_dir))
    md.extend(render_section_6_cost(p.measurements_dir))
    md.extend(render_section_7_caveats(dual_mode=False))
    md.extend(render_section_8_emitted(p.emitted_dir))

    # Per-edge bandwidth appendix.
    md.append("## Appendix A — per-edge bandwidth")
    md.append("")
    md.append("| Edge | Mean B/s | Samples |")
    md.append("|---|---|---|")
    means = _per_edge_mean_bytes_per_s(p.edge_rows)
    counts: dict[str, int] = {}
    for r in p.edge_rows:
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


def render_markdown_dual(
    results_dir: str,
    num_producers: int,
    per_agent_cardinality: int,
) -> str:
    """Render the dual-mode comparison report (baseline + asap)."""
    base = PipelineData(os.path.join(results_dir, "baseline"), "baseline")
    asap = PipelineData(os.path.join(results_dir, "asap"), "asap")

    md: list[str] = []
    md.append("# ASAPCollector MVP demo — issue #46 (baseline vs ASAP)")
    md.append("")
    md.append(
        "Dual-pipeline run: same workload, same otel-app "
        "producers, same per-agent cardinality, same query classes, "
        "same soak duration — only the pipeline differs. Pipelines "
        "run **sequentially** with a full `docker compose down -v` "
        "between them so per-pipeline resource numbers don't "
        "contaminate each other."
    )
    md.append("")
    md.append("Pipelines compared:")
    md.append("")
    md.append(
        "* **baseline** — `mvp-multi-stage.yml` + `--profile b0`; "
        "agents load `asap-otel-agent-b0-victoriametrics.yaml` "
        "(was `…-b0-prometheus.yaml` pre Step 2g 2026-05); "
        "storage = VictoriaMetrics (Prometheus-wire compatible); "
        "queries hit VM's PromQL HTTP surface."
    )
    md.append(
        "* **asap** — `mvp-multi-stage.yml` default profile; "
        "controller-driven sketch + Gorilla-S3; queries hit the "
        "ASAPQuery-backend's BackendStorageRouting."
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
    md.append(f"| Stack settle + warm-up + soak | 60s + 60s + 60s (each pipeline) |")
    md.append(f"| Replay shapes | window/label/combined @ 5 QPS for 60s |")
    md.append("")

    # §1 dual-mode stage table.
    md.extend(render_section_1_stage_table_dual(base, asap))
    # §2 dual-mode verdict (5 empirical claims + accuracy + cold +
    # freshness).
    md.extend(render_section_2_verdict_dual(base, asap))
    # §3 ASAP-only per-class breakdown (the 3 query classes are an
    # ASAP-side concept; baseline answers all 3 from Prometheus
    # natively).
    md.append("## §3 Per-query-class breakdown (ASAP)")
    md.append("")
    # The single-mode renderer prefixes its row with `### §3.1` (so
    # it doesn't collide with the per-sketch §3 above); strip that
    # subheader here in dual-mode where the parent ## already labels
    # this whole section.
    sub = render_section_3_per_class(
        asap.replay_rows, asap.accuracy_rows, asap.emitted_status,
    )
    md.extend(sub[2:] if sub and sub[0].startswith("### §3.1") else sub)
    # §4..§6 are ASAP-only by construction.
    md.append("_§4..§6 below cover the ASAP pipeline only — postings filtering, "
              "concat-only compaction, and S3-ops cost are ASAP architectural "
              "concepts that have no baseline counterpart._")
    md.append("")
    md.extend(render_section_4_postings(asap.adhoc_dir))
    md.extend(render_section_5_compaction(asap.compactor_dir))
    md.extend(render_section_6_cost(asap.measurements_dir))
    # §7 caveats (dual-mode wording).
    md.extend(render_section_7_caveats(dual_mode=True))
    # §8 emitter status — only meaningful for ASAP.
    md.append("## §8 Controller-emitted runtime configs (ASAP pipeline)")
    md.append("")
    md.extend(render_section_8_emitted(asap.emitted_dir)[2:])  # strip duplicate header

    # Appendix A — per-edge bandwidth, side-by-side.
    md.append("## Appendix A — per-edge bandwidth (baseline vs ASAP)")
    md.append("")
    md.append("| Edge | Baseline mean B/s | ASAP mean B/s | Baseline samples | ASAP samples |")
    md.append("|---|---|---|---|---|")
    base_means = _per_edge_mean_bytes_per_s(base.edge_rows)
    asap_means = _per_edge_mean_bytes_per_s(asap.edge_rows)
    base_counts: dict[str, int] = {}
    asap_counts: dict[str, int] = {}
    for r in base.edge_rows:
        base_counts[r.get("edge", "?")] = base_counts.get(r.get("edge", "?"), 0) + 1
    for r in asap.edge_rows:
        asap_counts[r.get("edge", "?")] = asap_counts.get(r.get("edge", "?"), 0) + 1
    for e in EDGE_ORDER:
        bm = base_means.get(e, float("nan"))
        am = asap_means.get(e, float("nan"))
        md.append(
            f"| {e} | {_fmt_num(bm, '{:.1f}')} | {_fmt_num(am, '{:.1f}')} | "
            f"{base_counts.get(e, 0)} | {asap_counts.get(e, 0)} |"
        )
    md.append("")

    return "\n".join(md) + "\n"


def render_markdown(
    results_dir: str,
    num_producers: int,
    per_agent_cardinality: int,
) -> str:
    """Top-level dispatch — selects single vs dual rendering based
    on the on-disk layout."""
    if not os.path.isdir(results_dir):
        return f"# MVP report — results dir missing ({results_dir})\n"

    layout = _detect_layout(results_dir)
    if layout == "dual":
        return render_markdown_dual(results_dir, num_producers, per_agent_cardinality)
    if layout == "baseline-only":
        return render_markdown_single(
            os.path.join(results_dir, "baseline"),
            num_producers, per_agent_cardinality,
            label_for_header="baseline",
        )
    if layout == "asap-only":
        return render_markdown_single(
            os.path.join(results_dir, "asap"),
            num_producers, per_agent_cardinality,
            label_for_header="asap",
        )
    # Legacy: results_dir IS the pipeline root.
    return render_markdown_single(
        results_dir, num_producers, per_agent_cardinality,
        label_for_header="single-cell",
    )


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
        help="Number of otel-app producers (default 10).",
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
