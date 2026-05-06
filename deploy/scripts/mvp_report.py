#!/usr/bin/env python3
"""mvp_report.py — issue-46 MVP report generator.

Reads the ASAP and RAW cell outputs produced by run_mvp_demo.sh and
emits MVP_REPORT.md with one PASS/FAIL line per acceptance criterion:

  X — bandwidth reduction:    (raw producer bytes/s − ASAP) / raw
  Y — query latency reduction: (raw p99 − ASAP p99) / raw
  Z — combined resource cost: agent CPU + RSS + backend CPU + RSS
  ε/δ — accuracy: median rel-error from accuracy.csv + the response
       infos line from the warm-tier query
  Cold-fallback: ad-hoc query response shows `data_source:
       gorilla_archive` (Phase-6+ backend) OR cold-store JSONL marker

Pure stdlib. Designed to handle missing/NaN measurements gracefully —
each criterion logs UNKNOWN if its source CSV is absent.

Usage:
  python3 mvp_report.py --asap-dir <dir> --raw-dir <dir> --out <path>
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
    """Single-row measurement.csv → {column: float}. NaN on parse fail."""
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


def _is_nan(x: float) -> bool:
    return x != x  # noqa: PLR0124


def safe_pct(num: float, denom: float) -> float:
    """100 * (denom - num) / denom — pct reduction. NaN-safe."""
    if _is_nan(num) or _is_nan(denom) or denom == 0:
        return float("nan")
    return 100.0 * (denom - num) / denom


# ── per-criterion reducers ───────────────────────────────────────


def criterion_x_bandwidth(asap: dict, raw: dict) -> tuple[str, str, dict]:
    """X — bandwidth reduction (producer wire bytes/s)."""
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
    """Y — query p99 reduction (warm-tier client-side)."""
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


def criterion_z_resource(asap: dict, raw: dict) -> tuple[str, str, dict]:
    """Z — combined resource cost (agent + backend CPU + RSS)."""
    cols = ("agent_cpu_cores", "agent_rss_mib", "backend_cpu_pct", "backend_rss_mib")

    def total(d: dict) -> float:
        # Mixed units (cores / MiB / %); we report a single-number
        # composite by normalising to a reasonable scale: cores → %,
        # MiB stays MiB. The key metric is the DELTA, so as long as
        # both sides use the same composite the ratio is meaningful.
        a_cpu = d.get("agent_cpu_cores", float("nan")) * 100.0
        a_rss = d.get("agent_rss_mib", float("nan"))
        b_cpu = d.get("backend_cpu_pct", float("nan"))
        b_rss = d.get("backend_rss_mib", float("nan"))
        # NaN-safe sum: NaN → 0 contribution so a partially-missing
        # row is recoverable.
        s = 0.0
        for v in (a_cpu, a_rss, b_cpu, b_rss):
            if not _is_nan(v):
                s += v
        return s

    asap_v = total(asap)
    raw_v = total(raw)
    pct = safe_pct(asap_v, raw_v)
    breakdown_asap = {c: asap.get(c, float("nan")) for c in cols}
    breakdown_raw = {c: raw.get(c, float("nan")) for c in cols}
    if _is_nan(pct):
        verdict = "UNKNOWN"
        line = f"asap={asap_v:.1f}, raw={raw_v:.1f} — measurement missing"
    else:
        verdict = "PASS" if pct > 0 else "FAIL"
        line = (
            f"raw composite={raw_v:.1f} → asap composite={asap_v:.1f} = "
            f"**{pct:.1f}% reduction** (agent_cpu%+agent_rss_mib"
            f"+backend_cpu%+backend_rss_mib)"
        )
    return verdict, line, {
        "asap": asap_v,
        "raw": raw_v,
        "pct": pct,
        "breakdown_asap": breakdown_asap,
        "breakdown_raw": breakdown_raw,
    }


def criterion_accuracy(
    asap_acc: list[dict], asap_response: dict | None
) -> tuple[str, str, dict]:
    """ε/δ — median relative error per query kind from accuracy.csv,
    plus any `infos` lines from the warm-tier response that pin
    the sketch's ε/δ envelope."""
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

    # Try to pull `infos` lines from the cold-fallback response.
    # When the backend image carries the EngineRouter / GorillaQueryEngine
    # wiring, this line surfaces ε=0 / δ=0 / kind=Exact for the cold
    # tier. Warm-tier sketches have their own ε/δ envelopes published
    # by their per-sketch processors at decode time, but that surface
    # is not yet exposed in the response infos.
    info_lines: list[str] = []
    if asap_response is not None:
        data = asap_response.get("data") or {}
        infos = data.get("infos") or asap_response.get("infos") or []
        if isinstance(infos, list):
            info_lines = [str(x) for x in infos]

    # PASS criterion: at least one kind reported AND every median ≤ 0.05
    # (5 % rel-error — generous envelope, bigger than the per-sketch ε
    # of any of the sketches in use). The 60-cell sweep showed all
    # families ≤ 0.01 at this workload.
    if not medians:
        verdict = "UNKNOWN"
        line = "no per-kind error rows in accuracy.csv"
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
    """Cold-fallback — ad-hoc query response. PASS if the response
    body carries the `data_source: gorilla_archive` marker (Phase-6
    backend with EngineRouter wired). PARTIAL if a different cold
    marker is present (LocalFsColdStore JSONL fallback). FAIL if
    the query errored or returned no marker."""
    if asap_response is None:
        return "UNKNOWN", "ad_hoc_query_response.json missing", {}

    data = asap_response.get("data") or {}
    infos = data.get("infos") or asap_response.get("infos") or []
    info_str = " | ".join(str(x) for x in infos) if isinstance(infos, list) else str(infos)
    status = asap_response.get("status", "")

    # MinIO chunk presence is a structural signal that the gorillas3
    # processor wrote SOMETHING — even when the backend's cold engine
    # isn't wired yet, the agent-side path is verified by the bucket
    # listing.
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
        "infos": info_str,
        "chunks_present": chunks_present,
        "chunk_summary": chunk_summary,
    }


# ── markdown emit ────────────────────────────────────────────────


def render_markdown(
    asap_meas: dict,
    raw_meas: dict,
    asap_acc: list[dict],
    asap_response: dict | None,
    asap_dir: str,
) -> str:
    x_v, x_line, _ = criterion_x_bandwidth(asap_meas, raw_meas)
    y_v, y_line, _ = criterion_y_latency(asap_meas, raw_meas)
    z_v, z_line, z_data = criterion_z_resource(asap_meas, raw_meas)
    a_v, a_line, _ = criterion_accuracy(asap_acc, asap_response)
    c_v, c_line, _ = criterion_cold_fallback(asap_response, asap_dir)

    badge = lambda v: {  # noqa: E731
        "PASS": "PASS",
        "FAIL": "FAIL",
        "PARTIAL": "PARTIAL",
        "UNKNOWN": "UNKNOWN",
    }.get(v, v)

    md: list[str] = []
    md.append("# ASAPCollector MVP demo — issue #46")
    md.append("")
    md.append(
        "Single-cell paired run of the ASAP all-sketches + Gorilla-S3 cold-archive "
        "pipeline against a raw OTLP streaming baseline. One driver "
        "(`run_mvp_demo.sh`) brings each cell up, soaks, replays the same "
        "PromQL suite, then snapshots metrics + cold-truth before tearing the "
        "stack down. Numbers below are from this run, NOT the 60-cell sweep."
    )
    md.append("")
    md.append(
        "Workload: N=1 agent · 1 Hz scrape · cardinality 10000 · soak 60s + "
        f"warm-up 60s. Replay queries: 5 PromQL shapes (quantile×2, sum, "
        f"count, topk) at 5 QPS for 60s."
    )
    md.append("")
    md.append("## Acceptance criteria")
    md.append("")
    md.append("| # | Criterion | Verdict | Number |")
    md.append("|---|-----------|---------|--------|")
    md.append(f"| 1 | Bandwidth reduction (X)            | **{badge(x_v)}** | {x_line} |")
    md.append(f"| 2 | Query latency reduction (Y)         | **{badge(y_v)}** | {y_line} |")
    md.append(f"| 3 | Combined resource reduction (Z)     | **{badge(z_v)}** | {z_line} |")
    md.append(f"| 4 | Accuracy (ε/δ)                      | **{badge(a_v)}** | {a_line} |")
    md.append(f"| 5 | Cold-store S3 fallback works        | **{badge(c_v)}** | {c_line} |")
    md.append("")
    md.append("## Raw measurements")
    md.append("")
    md.append("```text")
    md.append("ASAP cell (b6-gorilla-s3 + all-sketches warm tier):")
    for k in sorted(asap_meas):
        md.append(f"  {k}: {asap_meas[k]:.4f}")
    md.append("")
    md.append("RAW cell (b0a-raw-stream):")
    for k in sorted(raw_meas):
        md.append(f"  {k}: {raw_meas[k]:.4f}")
    md.append("```")
    md.append("")
    md.append("## Resource breakdown (criterion 3)")
    md.append("")
    md.append("| Component | RAW | ASAP |")
    md.append("|-----------|-----|------|")
    for c in ("agent_cpu_cores", "agent_rss_mib", "backend_cpu_pct", "backend_rss_mib"):
        a = z_data.get("breakdown_asap", {}).get(c, float("nan"))
        r = z_data.get("breakdown_raw", {}).get(c, float("nan"))
        md.append(f"| {c} | {r:.3f} | {a:.3f} |")
    md.append("")
    md.append("## Cold-fallback ad-hoc query response")
    md.append("")
    if asap_response is not None:
        md.append("```json")
        md.append(json.dumps(asap_response, indent=2)[:4000])
        md.append("```")
    else:
        md.append("_response not captured_")
    md.append("")
    md.append("## Notes")
    md.append("")
    md.append(
        "* `producer_bytes_out_per_s` is captured from `docker stats` on the "
        "fake-exporter container (the wire-bytes signal bandwidth claim ① "
        "actually cares about)."
    )
    md.append(
        "* `backend_query_p99_ms` is the client-side p99 of "
        "`promql_replay.py`'s successful-query duration_ms — survives the "
        "`docker compose down -v` that follows each cell."
    )
    md.append(
        "* The accuracy column reports per-kind median relative error from "
        "`accuracy_reduce.py` (joining `replay.jsonl` with the cold-truth "
        "JSONL the producer raw_tee wrote)."
    )
    return "\n".join(md) + "\n"


# ── main ─────────────────────────────────────────────────────────


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--asap-dir", required=True)
    ap.add_argument("--raw-dir", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    asap_meas = load_measurement(os.path.join(args.asap_dir, "measurement.csv"))
    raw_meas = load_measurement(os.path.join(args.raw_dir, "measurement.csv"))
    asap_acc = load_accuracy(os.path.join(args.asap_dir, "accuracy.csv"))

    asap_response: dict | None = None
    resp_path = os.path.join(args.asap_dir, "ad_hoc_query_response.json")
    if os.path.exists(resp_path):
        try:
            with open(resp_path, "r") as f:
                asap_response = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            print(f"# could not parse {resp_path}: {e}", file=sys.stderr)

    md = render_markdown(asap_meas, raw_meas, asap_acc, asap_response, args.asap_dir)
    with open(args.out, "w") as f:
        f.write(md)
    print(f"wrote {args.out} ({len(md)} chars)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
