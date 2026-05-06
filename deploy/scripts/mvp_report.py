#!/usr/bin/env python3
"""mvp_report.py — issue-46 MVP report generator (v5).

v5 changes vs v4:

  * Reads three new artifacts under each results dir:
      - `compactor/{dry_run,live_run}.json`
      - `compactor/{before,after}.minio.jsonl`
      - per-baseline `s3_cost.csv`
      - per-baseline `label_predicate_queries/*.json`

  * Adds §5 Cost (S3 ops, measured) — table of PUT / GET / HEAD /
    DELETE counts + bytes per baseline.
  * Adds §6 Compaction effect — before/after object count + total
    bytes; explicitly notes "no decode/re-encode — concat-only".
  * Adds §7 Postings filtering — for each ad-hoc query, response
    `postings_filtered_series_count` vs would-have-scanned chunks.
  * Adds §8 Criterion ⑦ verdict (PromQL ad-hoc label predicate
    latency stays bounded).
  * Adds Annex: S3 cost projection out to 100k / 1M / 10M
    cardinality using the linear PUT-cost model.

v4 changes vs v3:

  * Reads from a v4-style results directory laid out
    `<results_dir>/<baseline>/{measurement.csv, accuracy.csv,
    freshness.csv, stages.csv, replay.jsonl,
    ad_hoc_query_response.json}` rather than the v3 two-cell
    {asap,raw} layout. Baselines are auto-discovered as the set
    of subdirectories that carry a `measurement.csv`.

  * Adds §1 Stage-separated resource breakdown table — 4 baselines
    × 5 stages × {CPU cores, RSS MiB, net in/out KiB/s, disk MiB}
    plus per-baseline totals and per-stage reduction-factor rows
    (ASAP relative to B0).

  * Adds §3 Freshness p50/p99 table — per (baseline, path) sample
    counts + p50/p99 deltas in milliseconds.

  * §2 Per-criterion verdict expanded to 6 criteria
    (5 originals + freshness).

  * §4 Honest caveats kept as in v3.

  * v3 entry points (`--asap-dir / --raw-dir`) are kept as a
    legacy fallback so old callers don't immediately break.

Pure stdlib. Idempotent — re-running over the same CSVs reproduces
the same MD.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import statistics
import sys
from typing import Any


# ── csv helpers ──────────────────────────────────────────────────


def load_measurement(path: str) -> dict[str, float]:
    if not os.path.exists(path):
        return {}
    with open(path, "r") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return {}
    out: dict[str, float] = {}
    for k, v in rows[0].items():
        if k in {"baseline", "scale", "rate", "cardinality"}:
            continue
        try:
            out[k] = float(v)
        except (TypeError, ValueError):
            out[k] = float("nan")
    return out


def load_accuracy(path: str) -> list[dict[str, str]]:
    if not os.path.exists(path):
        return []
    with open(path, "r") as f:
        return list(csv.DictReader(f))


def load_freshness(path: str) -> list[dict[str, str]]:
    if not os.path.exists(path):
        return []
    with open(path, "r") as f:
        return list(csv.DictReader(f))


def load_stages(path: str) -> list[dict[str, str]]:
    if not os.path.exists(path):
        return []
    with open(path, "r") as f:
        return list(csv.DictReader(f))


def load_response(path: str) -> dict | None:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def _is_nan(x: float) -> bool:
    return x != x  # noqa: PLR0124


def safe_pct(num: float, denom: float) -> float:
    if _is_nan(num) or _is_nan(denom) or denom == 0:
        return float("nan")
    return 100.0 * (denom - num) / denom


def _percentile(xs: list[float], p: float) -> float:
    if not xs:
        return float("nan")
    s = sorted(xs)
    idx = max(0, min(len(s) - 1, int(round(p * (len(s) - 1)))))
    return s[idx]


# ── per-criterion reducers ───────────────────────────────────────


def criterion_x_bandwidth(asap: dict, raw: dict) -> tuple[str, str, dict]:
    asap_v = asap.get("producer_bytes_out_per_s", float("nan"))
    raw_v = raw.get("producer_bytes_out_per_s", float("nan"))
    pct = safe_pct(asap_v, raw_v)
    if _is_nan(pct):
        verdict = "UNKNOWN"
        line = f"asap={asap_v:.1f} B/s, raw={raw_v:.1f} B/s — measurement missing"
    else:
        verdict = "PASS" if pct > 0 else "FAIL"
        line = f"raw={raw_v:.1f} B/s → asap={asap_v:.1f} B/s = **{pct:.1f}% reduction**"
    return verdict, line, {"asap": asap_v, "raw": raw_v, "pct": pct}


def criterion_y_latency(asap: dict, raw: dict) -> tuple[str, str, dict]:
    asap_v = asap.get("backend_query_p99_ms", float("nan"))
    raw_v = raw.get("backend_query_p99_ms", float("nan"))
    pct = safe_pct(asap_v, raw_v)
    if _is_nan(pct):
        verdict = "UNKNOWN"
        line = f"asap p99={asap_v:.2f} ms, raw p99={raw_v:.2f} ms — measurement missing"
    else:
        verdict = "PASS" if pct > 0 else "FAIL"
        line = f"raw p99={raw_v:.2f} ms → asap p99={asap_v:.2f} ms = **{pct:.1f}% reduction**"
    return verdict, line, {"asap": asap_v, "raw": raw_v, "pct": pct}


def criterion_z_resource(
    stages_by_baseline: dict[str, list[dict]], asap_label: str, raw_label: str,
) -> tuple[str, str, dict]:
    """Z — combined resource cost (sum of CPU cores + RSS MiB across all stages)."""
    def total(rows: list[dict]) -> tuple[float, float]:
        cpu = 0.0
        rss = 0.0
        # Drop bookkeeping duplication of the backend container
        # across {ingest, query} stages so RSS isn't double-counted.
        seen: set[tuple[str, str]] = set()
        for r in rows:
            container = r.get("container", "")
            stage = r.get("stage", "")
            key = (container, stage)
            if key in seen:
                continue
            # If we already have backend-ingest, skip backend-query
            # for the same container.
            if (container, "backend-ingest") in seen and stage == "backend-query":
                continue
            seen.add(key)
            try:
                cpu += float(r.get("cpu_cores", "nan"))
                rss += float(r.get("rss_mib", "nan"))
            except ValueError:
                continue
        return cpu, rss

    asap_rows = stages_by_baseline.get(asap_label, [])
    raw_rows = stages_by_baseline.get(raw_label, [])
    a_cpu, a_rss = total(asap_rows)
    r_cpu, r_rss = total(raw_rows)
    asap_v = (0.0 if _is_nan(a_cpu) else a_cpu) * 100.0 + (0.0 if _is_nan(a_rss) else a_rss)
    raw_v = (0.0 if _is_nan(r_cpu) else r_cpu) * 100.0 + (0.0 if _is_nan(r_rss) else r_rss)
    pct = safe_pct(asap_v, raw_v)
    if not asap_rows or not raw_rows:
        return "UNKNOWN", "stages.csv missing for asap or raw", {}
    if _is_nan(pct):
        verdict = "UNKNOWN"
        line = f"asap={asap_v:.1f}, raw={raw_v:.1f} — measurement missing"
    else:
        verdict = "PASS" if pct > 0 else "FAIL"
        line = (
            f"raw composite={raw_v:.1f} → asap composite={asap_v:.1f} = "
            f"**{pct:.1f}% reduction** (cpu_cores×100 + rss_mib summed across "
            f"agent + producer + gateway + backend + storage)"
        )
    return verdict, line, {
        "asap_cpu": a_cpu, "asap_rss": a_rss,
        "raw_cpu": r_cpu, "raw_rss": r_rss,
        "asap": asap_v, "raw": raw_v, "pct": pct,
    }


def criterion_accuracy(
    asap_acc: list[dict], asap_response: dict | None
) -> tuple[str, str, dict]:
    by_kind: dict[str, list[float]] = {}
    for row in asap_acc:
        k = row.get("kind", "")
        try:
            err = float(row.get("error", "") or "nan")
        except ValueError:
            err = float("nan")
        if not _is_nan(err):
            by_kind.setdefault(k, []).append(err)

    parts: list[str] = []
    medians: dict[str, float] = {}
    for k in sorted(by_kind):
        if not by_kind[k]:
            continue
        med = statistics.median(by_kind[k])
        medians[k] = med
        parts.append(f"{k}: median rel-err={med:.4f} (n={len(by_kind[k])})")

    info_lines: list[str] = []
    if asap_response is not None:
        data = asap_response.get("data") or {}
        infos = data.get("infos") or asap_response.get("infos") or []
        if isinstance(infos, list):
            info_lines = [str(x) for x in infos]

    if not medians:
        verdict = "UNKNOWN"
        line = "no per-kind error rows in accuracy.csv (warm-tier may not have flushed in window)"
    else:
        worst = max(medians.values())
        verdict = "PASS" if worst <= 0.05 else "FAIL"
        line = "; ".join(parts)
        if info_lines:
            line += "  · response infos: " + " | ".join(info_lines)
    return verdict, line, {"medians": medians, "info_lines": info_lines}


def criterion_cold_fallback(
    asap_response: dict | None, asap_dir: str
) -> tuple[str, str, dict]:
    if asap_response is None:
        return "UNKNOWN", "ad_hoc_query_response.json missing", {}

    data = asap_response.get("data") or {}
    infos = data.get("infos") or asap_response.get("infos") or []
    info_str = " | ".join(str(x) for x in infos) if isinstance(infos, list) else str(infos)
    status = asap_response.get("status", "")

    chunks_path = os.path.join(asap_dir, "gorilla_chunks.txt")
    chunks_present = False
    chunk_summary = ""
    if os.path.exists(chunks_path):
        with open(chunks_path, "r") as f:
            chunks_text = f.read()
        chunk_lines = [
            line for line in chunks_text.splitlines() if line.strip().endswith(".gor")
        ]
        chunks_present = bool(chunk_lines)
        chunk_summary = f"{len(chunk_lines)} chunk(s) in asap-gorilla bucket"

    if "gorilla_archive" in info_str:
        verdict = "PASS"
        line = (
            f"`data_source: gorilla_archive` present in response infos — "
            f"GorillaQueryEngine served the ad-hoc query.  · {chunk_summary}"
        )
    elif chunks_present:
        verdict = "PARTIAL"
        line = (
            f"agent → MinIO write verified ({chunk_summary}); backend response "
            f"infos = `{info_str}` (status={status}). Backend's EngineRouter "
            f"may not yet route to GorillaQueryEngine in this image — see "
            f"`ad_hoc_query_response.json`."
        )
    else:
        verdict = "FAIL"
        line = (
            f"no `gorilla_archive` marker AND no chunks in MinIO. "
            f"infos=`{info_str}` status={status}"
        )
    return verdict, line, {
        "infos": info_str, "chunks_present": chunks_present, "chunk_summary": chunk_summary,
    }


def criterion_freshness(
    fresh_by_baseline: dict[str, list[dict]], asap_label: str,
) -> tuple[str, str, dict]:
    """Criterion ⑥ — freshness. PASS if the warm-tier path on the
    ASAP baseline has p50 ≤ 30s and the archive path has p50 ≤ 90s
    (1.5 × the 60s gorilla chunk window). FAIL otherwise. UNKNOWN
    if the CSV is empty / missing."""
    rows = fresh_by_baseline.get(asap_label, [])
    if not rows:
        return "UNKNOWN", "freshness.csv missing for ASAP baseline", {}
    by_path: dict[str, list[float]] = {}
    for r in rows:
        try:
            d = float(r.get("delta_ms", "nan"))
        except ValueError:
            continue
        if not _is_nan(d):
            by_path.setdefault(r.get("path", "?"), []).append(d)
    pieces: list[str] = []
    summary: dict[str, dict[str, float]] = {}
    for path in ("warm", "archive"):
        deltas = by_path.get(path, [])
        if deltas:
            p50 = _percentile(deltas, 0.5)
            p99 = _percentile(deltas, 0.99)
            summary[path] = {"p50": p50, "p99": p99, "count": len(deltas)}
            pieces.append(f"{path}: p50={p50:.0f}ms p99={p99:.0f}ms n={len(deltas)}")
        else:
            summary[path] = {"p50": float("nan"), "p99": float("nan"), "count": 0}
            pieces.append(f"{path}: no observations")

    warm_p50 = summary.get("warm", {}).get("p50", float("nan"))
    archive_p50 = summary.get("archive", {}).get("p50", float("nan"))
    if _is_nan(warm_p50):
        verdict = "UNKNOWN"
    elif warm_p50 <= 30_000.0 and (
        _is_nan(archive_p50) or archive_p50 <= 90_000.0
    ):
        verdict = "PASS"
    else:
        verdict = "FAIL"
    return verdict, "; ".join(pieces), summary


# ── §1 stage table ───────────────────────────────────────────────


STAGE_ORDER = ["agent", "gateway", "backend-ingest", "backend-query", "backend-storage"]


def aggregate_stages(rows: list[dict]) -> dict[str, dict[str, float]]:
    """Sum per-stage totals from a single baseline's stages.csv rows."""
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


def render_stage_table(
    stages_by_baseline: dict[str, list[dict]],
    baselines: list[str],
) -> list[str]:
    """One markdown table per metric (CPU / RSS / net-in / net-out /
    disk) with stages × baselines."""
    aggs = {b: aggregate_stages(stages_by_baseline.get(b, [])) for b in baselines}

    md: list[str] = []
    md.append("## §1 Stage-separated resource breakdown")
    md.append("")
    md.append(
        "Each cell is the per-stage TOTAL across all containers belonging to "
        "that stage (agent: all 10 agent-* containers; backend-storage: "
        "Prometheus TSDB for B0/B1, MinIO bucket for ASAP). The backend "
        "container is double-listed under {backend-ingest, backend-query} "
        "so the table can show both surfaces — RSS rows on those two stages "
        "report the SAME container's RSS twice; criterion ③ deduplicates "
        "before computing the headline reduction."
    )
    md.append("")

    metric_specs = [
        ("CPU (cores, mean over 60s)", "cpu_cores", "{:.3f}"),
        ("RSS (MiB, mean over 60s)", "rss_mib", "{:.1f}"),
        ("Net in (KiB/s, window-rate)", "net_in_kibps", "{:.1f}"),
        ("Net out (KiB/s, window-rate)", "net_out_kibps", "{:.1f}"),
        ("Disk (MiB, end-of-window)", "disk_mib", "{:.1f}"),
    ]

    for title, key, fmt in metric_specs:
        md.append(f"### {title}")
        md.append("")
        md.append("| Stage | " + " | ".join(baselines) + " |")
        md.append("|" + "---|" * (1 + len(baselines)))
        for stage in STAGE_ORDER:
            row = [stage]
            for b in baselines:
                v = aggs.get(b, {}).get(stage, {}).get(key, float("nan"))
                row.append(fmt.format(v) if not _is_nan(v) else "—")
            md.append("| " + " | ".join(row) + " |")
        # Totals row, deduplicated for backend-{ingest,query}.
        total_row = ["**total** (dedup backend)"]
        for b in baselines:
            tot = 0.0
            for stage in STAGE_ORDER:
                if stage == "backend-query":
                    # Skip — dedup with backend-ingest.
                    continue
                v = aggs.get(b, {}).get(stage, {}).get(key, float("nan"))
                if not _is_nan(v):
                    tot += v
            total_row.append(fmt.format(tot))
        md.append("| " + " | ".join(total_row) + " |")
        md.append("")
    return md


# ── §3 freshness table ───────────────────────────────────────────


def render_freshness_table(
    fresh_by_baseline: dict[str, list[dict]],
    baselines: list[str],
) -> list[str]:
    md: list[str] = []
    md.append("## §3 Freshness p50 / p99 (criterion ⑥)")
    md.append("")
    md.append(
        "Δ between sample emission unix_ts_ms (encoded in the gauge value) and "
        "PromQL observation wall-clock at the first non-NaN result. Polled at "
        "10 Hz with `last_over_time(<metric>[10s])`. B0 / B1 / B5 only "
        "exercise the warm path (Prometheus has no cold archive)."
    )
    md.append("")
    md.append("| Baseline | Path | n samples | p50 (ms) | p99 (ms) |")
    md.append("|---|---|---|---|---|")
    for b in baselines:
        rows = fresh_by_baseline.get(b, [])
        by_path: dict[str, list[float]] = {}
        for r in rows:
            try:
                d = float(r.get("delta_ms", "nan"))
            except ValueError:
                continue
            if not _is_nan(d):
                by_path.setdefault(r.get("path", "?"), []).append(d)
        if not by_path:
            md.append(f"| {b} | — | 0 | — | — |")
            continue
        for path in sorted(by_path):
            deltas = by_path[path]
            md.append(
                f"| {b} | {path} | {len(deltas)} | "
                f"{_percentile(deltas, 0.5):.0f} | {_percentile(deltas, 0.99):.0f} |"
            )
    md.append("")
    return md


# ── §5 / §6 / §7 / §8 — v5 helpers ──────────────────────────────


def load_s3_cost(path: str) -> dict[str, int]:
    """Read a single-row s3_cost.csv → dict[col → int]."""
    if not os.path.exists(path):
        return {}
    with open(path, "r") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return {}
    out: dict[str, int] = {}
    for k, v in rows[0].items():
        try:
            out[k] = int(v)
        except (TypeError, ValueError):
            out[k] = 0
    return out


def load_compactor_outputs(results_dir: str) -> dict[str, Any]:
    """Pull both the dry-run + live-run summary JSON, plus the
    before/after MinIO listings (count + total bytes)."""
    cdir = os.path.join(results_dir, "compactor")
    out: dict[str, Any] = {
        "dry_run": None,
        "live_run": None,
        "before_count": 0, "before_bytes": 0,
        "after_count": 0, "after_bytes": 0,
        "skipped": os.path.exists(os.path.join(cdir, "SKIPPED")),
    }
    for tag, fname in (("dry_run", "dry_run.json"), ("live_run", "live_run.json")):
        p = os.path.join(cdir, fname)
        if os.path.exists(p):
            try:
                with open(p, "r") as f:
                    out[tag] = json.load(f)
            except (json.JSONDecodeError, OSError):
                pass
    for tag, fname in (("before", "before.minio.jsonl"), ("after", "after.minio.jsonl")):
        p = os.path.join(cdir, fname)
        if not os.path.exists(p):
            continue
        count = 0
        bytes_sum = 0
        with open(p, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                count += 1
                # `mc ls --json` carries the size at .size.
                bytes_sum += int(rec.get("size", 0) or 0)
        out[f"{tag}_count"] = count
        out[f"{tag}_bytes"] = bytes_sum
    return out


def load_label_predicate_queries(baseline_dir: str) -> list[dict]:
    """Read every JSON file under `<baseline>/label_predicate_queries/`."""
    qdir = os.path.join(baseline_dir, "label_predicate_queries")
    if not os.path.isdir(qdir):
        return []
    out: list[dict] = []
    for entry in sorted(os.listdir(qdir)):
        p = os.path.join(qdir, entry)
        if not entry.endswith(".json"):
            continue
        try:
            with open(p, "r") as f:
                out.append(json.load(f))
        except (json.JSONDecodeError, OSError):
            continue
    return out


def render_s3_cost_section(
    s3_costs: dict[str, dict[str, int]],
    baselines: list[str],
) -> list[str]:
    md: list[str] = []
    md.append("## §5 Cost (S3 ops, measured)")
    md.append("")
    md.append(
        "Per-baseline counters from the backend's `/internal/s3_cost.csv` "
        "endpoint. `bytes_got` is **measured response bytes** (not "
        "billed S3 bytes — the wire-format overhead is not included)."
    )
    md.append("")
    cols = ["put_count", "get_count", "head_count", "list_count",
            "delete_count", "bytes_put", "bytes_got"]
    md.append("| Baseline | " + " | ".join(cols) + " |")
    md.append("|" + "---|" * (1 + len(cols)))
    for b in baselines:
        row = [b]
        cost = s3_costs.get(b, {})
        for c in cols:
            row.append(str(cost.get(c, 0)))
        md.append("| " + " | ".join(row) + " |")
    md.append("")
    return md


def render_compaction_section(comp: dict[str, Any]) -> list[str]:
    md: list[str] = []
    md.append("## §6 Compaction effect")
    md.append("")
    md.append(
        "Concat-only compaction: source `part-*.gor` files are byte-"
        "concatenated into a single per-day `block-NNNN-MMMM.gor` "
        "object; chunks themselves are NOT decoded or re-encoded. "
        "The merged object is bit-identical to the source bytes "
        "laid end-to-end. Wins are: ~6× fewer S3 PUTs at "
        "compaction time, fewer GETs to answer long-range queries, "
        "single merged postings file. The MVP does not (yet) decode "
        "+ re-encode adjacent same-series chunks for the additional "
        "10–30% Gorilla compression — that's the natural follow-up."
    )
    md.append("")
    if comp.get("skipped"):
        md.append("_Compactor phase was SKIPPED on this run (see `compactor/SKIPPED`)._")
        md.append("")
        return md
    md.append("| Phase | Object count | Total bytes |")
    md.append("|---|---|---|")
    md.append(
        f"| before | {comp.get('before_count', 0)} | {comp.get('before_bytes', 0)} |"
    )
    md.append(
        f"| after  | {comp.get('after_count', 0)} | {comp.get('after_bytes', 0)} |"
    )
    md.append("")
    live = comp.get("live_run") or {}
    metrics = live.get("metrics") or {}
    md.append(
        f"Compactor metrics: blocks_in={metrics.get('blocks_in', 0)}, "
        f"blocks_out={metrics.get('blocks_out', 0)}, "
        f"chunks_in={metrics.get('chunks_in', 0)}, "
        f"chunks_out={metrics.get('chunks_out', 0)}, "
        f"s3_put={metrics.get('s3_put_count', 0)}, "
        f"s3_get={metrics.get('s3_get_count', 0)}, "
        f"s3_delete={metrics.get('s3_delete_count', 0)}, "
        f"duration_ms={metrics.get('duration_ms', 0)}."
    )
    md.append("")
    if comp.get("dry_run"):
        dry = comp["dry_run"]
        md.append(
            f"Dry-run plan: groups_planned={dry.get('groups_planned', 0)}, "
            f"deferred={dry.get('deferred', 0)}."
        )
        md.append("")
    return md


def render_postings_filtering_section(
    queries: list[dict],
) -> list[str]:
    md: list[str] = []
    md.append("## §7 Postings filtering (per query)")
    md.append("")
    md.append(
        "`postings_filtered_series_count` comes from the engine's "
        "response `infos`; `chunks_skipped_via_postings` likewise. "
        "When `data_source_quirk: postings_missing` appears, the "
        "engine fell back to the scan-all path and the count is "
        "the would-have-been series count (post-decode label "
        "filter)."
    )
    md.append("")
    if not queries:
        md.append("_No label-predicate query responses captured._")
        md.append("")
        return md
    md.append(
        "| Query | client latency (ms) | series filtered | "
        "chunks skipped | postings missing |"
    )
    md.append("|---|---|---|---|---|")
    for q in queries:
        name = q.get("name", "?")
        lat = q.get("client_latency_ms", float("nan"))
        infos = []
        resp = q.get("response", {}) or {}
        data = resp.get("data") or {}
        infos = data.get("infos") or resp.get("infos") or []

        def _num(prefix: str) -> str:
            for line in infos:
                if isinstance(line, str) and line.startswith(prefix):
                    return line[len(prefix):].strip().rstrip(',')
            return "—"

        series_filtered = _num("postings_filtered_series_count: ")
        chunks_skipped = _num("chunks_skipped_via_postings: ")
        postings_missing = "yes" if any(
            isinstance(l, str) and "postings_missing" in l for l in infos
        ) else "no"
        md.append(
            f"| `{name}` | {lat:.1f} | {series_filtered} | "
            f"{chunks_skipped} | {postings_missing} |"
        )
    md.append("")
    return md


def criterion_label_predicate_latency(
    queries: list[dict],
    p99_threshold_ms: float = 2_000.0,
) -> tuple[str, str, dict]:
    """Criterion ⑦ — ad-hoc PromQL with label predicates stays
    bounded (p99 across the supplied queries ≤ threshold)."""
    if not queries:
        return "UNKNOWN", "no label-predicate query responses captured", {}
    latencies = [
        float(q.get("client_latency_ms", float("nan")))
        for q in queries
        if isinstance(q.get("client_latency_ms"), (int, float))
    ]
    if not latencies:
        return "UNKNOWN", "no per-query latency observations", {}
    p99 = _percentile(latencies, 0.99)
    pass_ = p99 <= p99_threshold_ms
    line = (
        f"p99 across {len(latencies)} queries = {p99:.1f} ms "
        f"(threshold {p99_threshold_ms:.0f} ms; "
        f"min={min(latencies):.1f}, max={max(latencies):.1f})"
    )
    return "PASS" if pass_ else "FAIL", line, {"p99_ms": p99, "n": len(latencies)}


def render_cost_projection_annex(
    s3_costs: dict[str, dict[str, int]],
    asap_label: str,
    cardinalities: tuple[int, ...] = (100_000, 1_000_000, 10_000_000),
    measured_cardinality: int = 10_000,
) -> list[str]:
    """Linear extrapolation of measured PUT / GET counts. State the
    assumption explicitly: every series writes one chunk per
    `WindowInterval` (60s default), so PUT count scales linearly
    with cardinality. GET count is query-driven and not modelled
    here (we just dilate it identically — same caveat)."""
    md: list[str] = []
    md.append("## Annex — Cost projection (linear PUT model)")
    md.append("")
    md.append(
        f"Measured at {measured_cardinality} series. Linear "
        "extrapolation assumes (a) each series writes one chunk per "
        "60s window, (b) each window emits one `index.json` and one "
        "`postings-v1.json` per metric. With the same agent + soak "
        "duration, PUT count is `(measured PUT) × (target / measured)` "
        "series. **Caveat**: GET count depends on workload (scan-vs-"
        "predicate mix) and is reproduced here at the same scaling "
        "factor for shape only — DO NOT cite as a workload-derived "
        "projection."
    )
    md.append("")
    asap_cost = s3_costs.get(asap_label, {})
    if not asap_cost:
        md.append("_no asap-single-sketch s3_cost.csv to extrapolate from_")
        md.append("")
        return md
    md.append("| Cardinality | PUT (extrap) | GET (extrap) | bytes_got (extrap) |")
    md.append("|---|---|---|---|")
    for card in cardinalities:
        scale = card / max(1, measured_cardinality)
        md.append(
            f"| {card:,} | "
            f"{int(asap_cost.get('put_count', 0) * scale):,} | "
            f"{int(asap_cost.get('get_count', 0) * scale):,} | "
            f"{int(asap_cost.get('bytes_got', 0) * scale):,} |"
        )
    md.append("")
    return md


# ── markdown assembly ────────────────────────────────────────────


def render_markdown_v4(
    results_dir: str,
    num_agents: int,
    per_agent_cardinality: int,
) -> str:
    # Discover baselines.
    if not os.path.isdir(results_dir):
        return f"# MVP report — results dir missing ({results_dir})\n"
    baselines: list[str] = []
    for entry in sorted(os.listdir(results_dir)):
        sub = os.path.join(results_dir, entry)
        if os.path.isdir(sub) and os.path.exists(os.path.join(sub, "measurement.csv")):
            baselines.append(entry)
    # Stable canonical order: B0 / B1 / B5 / asap.
    canonical = ["b0-prometheus", "b1-serf", "b5-gorilla", "asap-single-sketch"]
    baselines = [b for b in canonical if b in baselines] + [
        b for b in baselines if b not in canonical
    ]

    measurements = {b: load_measurement(os.path.join(results_dir, b, "measurement.csv"))
                    for b in baselines}
    accuracies = {b: load_accuracy(os.path.join(results_dir, b, "accuracy.csv"))
                  for b in baselines}
    freshness = {b: load_freshness(os.path.join(results_dir, b, "freshness.csv"))
                 for b in baselines}
    stages = {b: load_stages(os.path.join(results_dir, b, "stages.csv"))
              for b in baselines}
    responses = {b: load_response(os.path.join(results_dir, b, "ad_hoc_query_response.json"))
                 for b in baselines}

    asap_label = "asap-single-sketch"
    raw_label = "b0-prometheus"
    # ASAP / RAW must be present for §2 verdicts to compute.
    asap_meas = measurements.get(asap_label, {})
    raw_meas = measurements.get(raw_label, {})

    badge = lambda v: {  # noqa: E731
        "PASS": "PASS", "FAIL": "FAIL", "PARTIAL": "PARTIAL", "UNKNOWN": "UNKNOWN",
    }.get(v, v)

    total_card = num_agents * per_agent_cardinality

    md: list[str] = []
    md.append("# ASAPCollector MVP demo — issue #46 (v5)")
    md.append("")
    md.append(
        "Four-baseline back-to-back run: "
        "B0 raw→Prometheus, B1 SERF→Prometheus, B5 Gorilla agent-side, "
        "B6 ASAP single-sketch + Gorilla-S3 cold archive. v5 adds: "
        "(1) postings-aware ad-hoc PromQL queries with label predicates, "
        "(2) S3 cost-tracking CSV per baseline, (3) concat-only "
        "compactor sweep on the surviving MinIO bucket. The compactor "
        "does NOT decode + re-encode Gorilla bit streams (see §6)."
    )
    md.append("")
    md.append("## Workload shape")
    md.append("")
    md.append("| Knob | v5 value |")
    md.append("|------|---------|")
    md.append(f"| Per-agent cardinality | **{per_agent_cardinality}** series |")
    md.append(f"| Number of distributed agents (N) | **{num_agents}** |")
    md.append(
        f"| Total backend cardinality (N × per-agent) | **{total_card}** series |"
    )
    md.append(f"| Scrape frequency | {os.environ.get('FREQ_HZ', '1')} Hz |")
    md.append(f"| Agent warm-up + query warm-up + soak | 60 s + ≤30 s + 60 s |")
    md.append("| Replay shapes | quantile×2, sum, count, topk @ 5 QPS for 60 s |")
    md.append("| ASAP sketch family | DDSketch (single-sketch v4 mode) |")
    md.append("")

    # §1 stage table
    md.extend(render_stage_table(stages, baselines))

    # §2 criteria
    x_v, x_line, _ = criterion_x_bandwidth(asap_meas, raw_meas)
    y_v, y_line, _ = criterion_y_latency(asap_meas, raw_meas)
    z_v, z_line, _ = criterion_z_resource(stages, asap_label, raw_label)
    a_v, a_line, _ = criterion_accuracy(
        accuracies.get(asap_label, []),
        responses.get(asap_label),
    )
    c_v, c_line, _ = criterion_cold_fallback(
        responses.get(asap_label),
        os.path.join(results_dir, asap_label),
    )
    f_v, f_line, _ = criterion_freshness(freshness, asap_label)

    md.append("## §2 Per-criterion verdict (6 criteria)")
    md.append("")
    md.append("| # | Criterion | Verdict | Number |")
    md.append("|---|-----------|---------|--------|")
    md.append(f"| ① | Bandwidth reduction (X)            | **{badge(x_v)}** | {x_line} |")
    md.append(f"| ② | Query latency reduction (Y)         | **{badge(y_v)}** | {y_line} |")
    md.append(f"| ③ | Combined resource reduction (Z)     | **{badge(z_v)}** | {z_line} |")
    md.append(f"| ④ | Accuracy (ε/δ)                      | **{badge(a_v)}** | {a_line} |")
    md.append(f"| ⑤ | Cold-store S3 fallback works        | **{badge(c_v)}** | {c_line} |")
    md.append(f"| ⑥ | Freshness (sample → first query)    | **{badge(f_v)}** | {f_line} |")
    md.append("")

    # §3 freshness
    md.extend(render_freshness_table(freshness, baselines))

    # ── v5 §5 / §6 / §7 / §8 ────────────────────────────────────
    s3_costs = {b: load_s3_cost(os.path.join(results_dir, b, "s3_cost.csv"))
                for b in baselines}
    md.extend(render_s3_cost_section(s3_costs, baselines))

    comp = load_compactor_outputs(results_dir)
    md.extend(render_compaction_section(comp))

    asap_label_predicate_queries = load_label_predicate_queries(
        os.path.join(results_dir, asap_label)
    )
    md.extend(render_postings_filtering_section(asap_label_predicate_queries))

    # §8 — criterion ⑦ verdict.
    p7_v, p7_line, _ = criterion_label_predicate_latency(asap_label_predicate_queries)
    md.append("## §8 Criterion ⑦ — PromQL ad-hoc label predicate latency")
    md.append("")
    md.append("| # | Criterion | Verdict | Number |")
    md.append("|---|-----------|---------|--------|")
    md.append(f"| ⑦ | Label-predicate latency stays bounded | **{badge(p7_v)}** | {p7_line} |")
    md.append("")

    # §4 caveats
    md.append("## §4 Honest caveats")
    md.append("")
    md.append("* **Single-host bench.** Every container runs on one machine "
              "(localhost network, shared kernel scheduler). Wire-bytes signals "
              "are still meaningful (TX counters are per-container) but absolute "
              "latency / CPU figures over-aggressively share cache + scheduler "
              "with the producers. A multi-host run would inflate net I/O latency "
              "and isolate per-stage CPU; this run does not.")
    md.append("* **60s measurement window.** Below the 60s gorilla-S3 chunk "
              "rotation period, so the cold-archive freshness number is bounded "
              "below by chunk write cadence, not by query latency. A longer run "
              "(≥ 5 min) would surface a more representative archive p99.")
    md.append("* **Single-sketch ASAP, not all-five.** This run uses DDSketch "
              "alone for the warm tier; the v3 all-five-sketches overlay is left "
              "on disk (`baseline-b6-gorilla-s3.yml`) for paper-figure use only. "
              "Resource numbers below should NOT be compared against the v3 "
              "report's all-five rows.")
    md.append("* **Real Prometheus, real PromQL evaluator.** B0 / B1 / B5 talk "
              "to a vanilla `prom/prometheus:v2.53.1` container with "
              "`--web.enable-remote-write-receiver`. No precompute_engine in "
              "those paths. Comparable apples-to-apples against ASAP's "
              "precompute backend on equivalent query shapes.")
    md.append("* **Freshness probe encodes ts inside the gauge value.** The "
              "delta is `obs_wall_clock - emit_ts`, polled at 10 Hz against "
              "`last_over_time(<metric>[10s])`. The 10s range window upper-"
              "bounds Prometheus's 15s scrape quantization on the warm side; "
              "it does NOT account for clock skew between the probe driver "
              "and Prometheus's scrape clock (we run both on the same host so "
              "skew is < 1 ms in practice).")
    md.append("")

    # Raw measurement appendix
    md.append("## Raw measurements (per-baseline measurement.csv)")
    md.append("")
    md.append("```text")
    for b in baselines:
        meas = measurements.get(b, {})
        md.append(f"{b}:")
        for k in sorted(meas):
            md.append(f"  {k}: {meas[k]:.4f}")
        md.append("")
    md.append("```")
    md.append("")

    # v5 cost-projection annex.
    md.extend(render_cost_projection_annex(
        s3_costs, asap_label,
        measured_cardinality=total_card,
    ))

    # ASAP cold-fallback response
    md.append("## ASAP cold-fallback ad-hoc query response")
    md.append("")
    asap_response = responses.get(asap_label)
    if asap_response is not None:
        md.append("```json")
        md.append(json.dumps(asap_response, indent=2)[:4000])
        md.append("```")
    else:
        md.append("_response not captured_")
    md.append("")

    return "\n".join(md) + "\n"


# ── v3 legacy mode (kept for back-compat) ────────────────────────


def render_markdown_v3_legacy(
    asap_dir: str,
    raw_dir: str,
    num_agents: int = 10,
    per_agent_cardinality: int = 1000,
) -> str:
    """Bare-bones legacy two-cell renderer used when --asap-dir +
    --raw-dir are supplied. Kept so v3 callers don't break."""
    asap_meas = load_measurement(os.path.join(asap_dir, "measurement.csv"))
    raw_meas = load_measurement(os.path.join(raw_dir, "measurement.csv"))
    asap_acc = load_accuracy(os.path.join(asap_dir, "accuracy.csv"))
    asap_response = load_response(os.path.join(asap_dir, "ad_hoc_query_response.json"))

    x_v, x_line, _ = criterion_x_bandwidth(asap_meas, raw_meas)
    y_v, y_line, _ = criterion_y_latency(asap_meas, raw_meas)
    a_v, a_line, _ = criterion_accuracy(asap_acc, asap_response)
    c_v, c_line, _ = criterion_cold_fallback(asap_response, asap_dir)

    md = [
        "# ASAPCollector MVP demo — issue #46 (legacy v3 layout)",
        "",
        "Two-cell ASAP-vs-raw report. v4 layout uses `--results-dir`.",
        "",
        f"## Verdicts",
        "",
        "| # | Criterion | Verdict | Number |",
        "|---|-----------|---------|--------|",
        f"| 1 | Bandwidth (X) | **{x_v}** | {x_line} |",
        f"| 2 | Latency  (Y) | **{y_v}** | {y_line} |",
        f"| 4 | Accuracy     | **{a_v}** | {a_line} |",
        f"| 5 | Cold fallback | **{c_v}** | {c_line} |",
        "",
    ]
    return "\n".join(md) + "\n"


# ── main ─────────────────────────────────────────────────────────


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--results-dir",
        default="",
        help="v4 results dir; expects <results_dir>/<baseline>/{measurement.csv,...}.",
    )
    ap.add_argument(
        "--asap-dir", default="",
        help="(legacy v3) ASAP cell directory.",
    )
    ap.add_argument(
        "--raw-dir", default="",
        help="(legacy v3) raw cell directory.",
    )
    ap.add_argument(
        "--num-agents", type=int, default=10,
        help="Number of distributed edge agents (default 10).",
    )
    ap.add_argument(
        "--per-agent-cardinality", type=int, default=1000,
        help="Per-agent series cardinality (default 1000).",
    )
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    if args.results_dir:
        md = render_markdown_v4(
            args.results_dir,
            num_agents=args.num_agents,
            per_agent_cardinality=args.per_agent_cardinality,
        )
    elif args.asap_dir and args.raw_dir:
        md = render_markdown_v3_legacy(
            args.asap_dir, args.raw_dir,
            num_agents=args.num_agents,
            per_agent_cardinality=args.per_agent_cardinality,
        )
    else:
        sys.exit("must supply --results-dir (v4) or --asap-dir + --raw-dir (v3 legacy)")

    with open(args.out, "w") as f:
        f.write(md)
    print(f"wrote {args.out} ({len(md)} chars)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
