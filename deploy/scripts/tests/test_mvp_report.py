"""Unit tests for mvp_report.py.

Hermetic — no compose stack, no docker. We synthesise a minimal
results directory layout and assert the generator produces the
expected MD shape (per-section presence, per-criterion verdict
formatting, idempotency).

Synthesis policy: every numeric value in these fixtures is clearly
SYNTHETIC (small round numbers, "MOCK" markers in comments). Real
numbers come from the live run — these tests just exercise the
renderer.
"""
from __future__ import annotations

import csv
import importlib.util
import json
import os
import sys
from pathlib import Path

import pytest

# Load mvp_report.py without requiring an installed package.
HERE = Path(__file__).resolve().parent
SCRIPT = HERE.parent / "mvp_report.py"
spec = importlib.util.spec_from_file_location("mvp_report", SCRIPT)
assert spec is not None and spec.loader is not None
mvp_report = importlib.util.module_from_spec(spec)
sys.modules["mvp_report"] = mvp_report
spec.loader.exec_module(mvp_report)


# ── fixture builder ──────────────────────────────────────────────


def _write_csv(path: Path, header: list[str], rows: list[list]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows:
            w.writerow(r)


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


def _build_results_dir(root: Path, *, kind: str = "happy") -> Path:
    """Create an MVP-demo results dir with synthetic CSVs (legacy
    single-pipeline layout).

    `kind` selects which fixture variant:
      - "happy"   — all sections populated, all criteria PASS
      - "sparse"  — only stages.csv present; everything else missing
      - "fallback"— emitted-config STATUS = fallback-placeholder
    """
    return _build_pipeline_dir(root, kind=kind)


def _build_pipeline_dir(
    root: Path,
    *,
    kind: str = "happy",
    cpu_scale: float = 1.0,
    bandwidth_scale: float = 1.0,
    latency_scale: float = 1.0,
    emit_status: str | None = None,
    include_compactor: bool = True,
) -> Path:
    """Build a single-pipeline results dir (the per-pipeline subdir
    shape used by both legacy and dual layouts).

    Knobs let the dual-mode test seed baseline > asap on resource
    metrics so the reduction columns produce non-trivial numbers.
    """
    root.mkdir(parents=True, exist_ok=True)
    measurements = root / "measurements"
    fresh = root / "freshness"
    adhoc = root / "ad-hoc"
    compactor = root / "compactor"
    emitted = root / "controller-emitted-configs"

    for d in (measurements, fresh, adhoc, compactor, emitted):
        d.mkdir(parents=True, exist_ok=True)

    if kind == "sparse":
        # Only stages.csv. Verdicts that need other CSVs go UNKNOWN.
        # MOCK numbers — single agent + gateway row, that's it.
        _write_csv(
            measurements / "stages.csv",
            ["baseline", "stage", "container",
             "cpu_cores", "rss_mib",
             "net_in_kibps", "net_out_kibps", "disk_mib"],
            [
                ["mvp", "agent",   "agent-a",  "0.10", "100.0", "10.0", "5.0",  ""],
                ["mvp", "agent",   "agent-b",  "0.10", "100.0", "10.0", "5.0",  ""],
                ["mvp", "gateway", "gateway",  "0.20", "200.0", "20.0", "15.0", ""],
            ],
        )
        return root

    # ── happy + fallback share the same data; only STATUS differs.

    # MOCK stages: one row per stage, round numbers.
    s = cpu_scale  # CPU + RSS multiplier (lets dual-mode seed baseline > asap).
    _write_csv(
        measurements / "stages.csv",
        ["baseline", "stage", "container",
         "cpu_cores", "rss_mib",
         "net_in_kibps", "net_out_kibps", "disk_mib"],
        [
            # 2 agents
            ["mvp", "agent",            "agent-a",  f"{0.10 * s:.4f}", f"{100.0 * s:.2f}", "10.0", "5.0",  "0"],
            ["mvp", "agent",            "agent-b",  f"{0.10 * s:.4f}", f"{100.0 * s:.2f}", "10.0", "5.0",  "0"],
            ["mvp", "gateway",          "gateway",  f"{0.20 * s:.4f}", f"{200.0 * s:.2f}", "20.0", "15.0", "0"],
            ["mvp", "backend-ingest",   "backend",  f"{0.30 * s:.4f}", f"{300.0 * s:.2f}", "30.0", "0.0",  "0"],
            ["mvp", "backend-query",    "backend",  f"{0.10 * s:.4f}", f"{300.0 * s:.2f}", "0.0",  "5.0",  "0"],
            ["mvp", "backend-storage",  "minio",    f"{0.05 * s:.4f}", f"{50.0 * s:.2f}",  "5.0",  "5.0",  "100.0"],
        ],
    )

    # MOCK per-edge bandwidth: 3 samples per edge.
    # Note: producer→agent should be larger than agent→gateway so
    # the bandwidth verdict is PASS. Numbers below are bytes/sec
    # (intentionally tiny — these are synthetic).
    edge_rows: list[list] = []
    base_ts = 1_700_000_000_000
    edges = [
        ("edge_sdk_to_agent",        1000.0 * bandwidth_scale),
        ("edge_agent_to_gateway",     500.0 * bandwidth_scale),
        ("edge_gateway_to_backend",   400.0 * bandwidth_scale),
        ("edge_gateway_to_s3",        100.0 * bandwidth_scale),
    ]
    for edge_label, bps in edges:
        for i in range(3):
            edge_rows.append([
                edge_label, base_ts + i * 1000, "1.000",
                f"{bps:.1f}", f"{bps:.3f}",
            ])
    _write_csv(
        measurements / "per_edge_bandwidth.csv",
        ["edge", "sample_ts_ms", "window_s", "bytes_total", "bytes_per_s"],
        edge_rows,
    )

    # MOCK accuracy: one row per (kind, query) — all under 5%.
    _write_csv(
        measurements / "accuracy.csv",
        ["cell", "kind", "query", "t", "duration_ms", "plan_id",
         "truth", "answer", "error", "recall", "n_truth_samples"],
        [
            ["mvp", "quantile", "quantile_over_time(0.99, http_requests_total_latency_ms[1m])",
             "0", "5.0", "p1", "100", "101", "0.01", "", "1000"],
            ["mvp", "sum", "sum by (zone) (http_requests_total)",
             "0", "3.0", "p1", "5000", "5005", "0.001", "", "1000"],
            ["mvp", "sum", "sum by (zone) (rate(http_requests_total[5m]))",
             "0", "4.0", "p1", "10", "10.05", "0.005", "", "1000"],
        ],
    )

    # MOCK replay JSONL: 5 attempts per query class.
    replay_rows = []
    queries = [
        ("quantile", "quantile_over_time(0.99, http_requests_total_latency_ms[1m])", 12.0 * latency_scale),
        ("sum",      "sum by (zone) (http_requests_total)",                            8.0 * latency_scale),
        ("sum",      "sum by (zone) (rate(http_requests_total[5m]))",                  9.0 * latency_scale),
    ]
    for q_kind, q, base_lat in queries:
        for i in range(5):
            replay_rows.append({
                "ts": "2026-05-06T00:00:00Z",
                "query": q,
                "kind": q_kind,
                "duration_ms": base_lat + i,
                "status": "success",
                "http_code": 200,
                "result_type": "vector",
                "result": [],
                "plan_id": "p1",
                "fallback_used": None,
            })
    _write_jsonl(measurements / "replay.jsonl", replay_rows)

    # MOCK freshness: 3 paths × 5 samples.
    for path_label, p50_anchor in (("raw", 200), ("warm", 1500), ("archive", 8000)):
        rows = []
        for i in range(5):
            sample_ts = base_ts + i * 100
            observed = sample_ts - p50_anchor + i  # delta_ms ≈ p50_anchor
            rows.append([
                path_label, sample_ts, observed, sample_ts - observed,
            ])
        _write_csv(
            fresh / f"{path_label}.csv",
            ["path", "sample_ts_ms", "observed_ts_ms", "delta_ms"],
            rows,
        )

    # MOCK ad-hoc: cold fallback marker present.
    cold_resp = {
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [{"metric": {}, "value": [0, "42"]}],
            "infos": ["data_source: gorilla_archive", "chunks_scanned: 3"],
        },
    }
    (adhoc / "cold_payments.json").write_text(json.dumps(cold_resp))
    (adhoc / "cold_payments.verdict").write_text("PASS\n")
    (adhoc / "cold_payments.curlstats").write_text(
        '{"http_code":200,"time_total":0.012}'
    )

    # MOCK postings exercise responses without the postings index
    # fields (most realistic state when the backend image lacks the
    # postings-aware engine).
    bare_resp = {
        "status": "success",
        "data": {"resultType": "vector", "result": [{"metric": {}, "value": [0, "1"]}]},
    }
    (adhoc / "count_api_series.json").write_text(json.dumps(bare_resp))
    (adhoc / "topk_5xx_by_zone.json").write_text(json.dumps(bare_resp))

    # MOCK compactor: minio listing before and after, +1 object then
    # -5 objects (simulating concat of 6→1).
    if include_compactor:
        before_lines = []
        for i in range(6):
            before_lines.append(json.dumps({
                "type": "file",
                "key": f"raw/part-{i:04d}.gor",
                "size": 1_000_000,
            }))
        after_lines = [
            json.dumps({
                "type": "file",
                "key": "raw/merged-0000.gor",
                "size": 6_000_000,
            }),
        ]
        (compactor / "before.minio.jsonl").write_text("\n".join(before_lines))
        (compactor / "after.minio.jsonl").write_text("\n".join(after_lines))
        (compactor / "dry_run.json").write_text(json.dumps({
            "eligible_blocks": [{"key": f"raw/part-{i:04d}.gor"} for i in range(6)],
            "dry_run": True,
        }))
        (compactor / "live_run.json").write_text(json.dumps({
            "merged_blocks": 1,
            "source_count": 6,
        }))
    else:
        (compactor / "SKIPPED").write_text("compactor not run for this pipeline\n")

    # MOCK emitted-config status.
    if emit_status is not None:
        (emitted / "STATUS").write_text(emit_status + "\n")
    elif kind == "fallback":
        (emitted / "STATUS").write_text("fallback-placeholder\n")
    else:
        (emitted / "STATUS").write_text("live\n")
    (emitted / "agent.bootstrap.yaml").write_text(
        "# MOCK bootstrap agent config\nreceivers: {}\n"
    )

    return root


# ── tests ────────────────────────────────────────────────────────


def test_renders_all_sections_for_happy_fixture(tmp_path):
    results = _build_results_dir(tmp_path, kind="happy")
    out = tmp_path / "MVP_REPORT.md"

    rc = mvp_report.main([
        "--results-dir", str(results),
        "--num-producers", "10",
        "--per-agent-cardinality", "500",
        "--out", str(out),
    ])
    assert rc == 0
    md = out.read_text()

    # Section headers — all 8 must be present. §3 is now the
    # per-sketch-family accuracy section; per-query-class breakdown
    # lives under it as `### §3.1`.
    for header in (
        "## §1 Stage-separated resource table",
        "## §2 Per-criterion verdict (6 criteria)",
        "## §3 Accuracy (per sketch family)",
        "### §3.1 Per-query-class breakdown",
        "## §4 Postings filtering effect",
        "## §5 Compaction effect",
        "## §6 S3-ops cost (measured)",
        "## §7 Honest caveats (non-goals)",
        "## §8 Controller-emitted runtime configs",
    ):
        assert header in md, f"missing section header {header!r}"

    # All 6 criteria rows present in §2. ④ is now the per-sketch
    # rollup ("see §3" for the per-family detail).
    for marker in (
        "Bandwidth (per-edge B/s)",
        "Query latency (p50/p99 per class)",
        "Combined resource (sum of stages)",
        "Accuracy (per sketch family, see §3)",
        "Cold-fallback (gorilla_archive marker)",
        "Freshness (p50/p99 per path)",
    ):
        assert marker in md, f"missing criterion {marker!r}"

    # Verdict tokens — happy fixture should show PASS for at least
    # bandwidth (egress < ingress), accuracy (<5%), cold-fallback
    # (marker present), and freshness (warm p50 ~1.5s ≤ 30s).
    assert "PASS" in md

    # §1 stage rows.
    for stage in ("agent", "gateway", "backend-ingest",
                  "backend-storage", "backend-query"):
        assert f"| {stage} |" in md

    # §3 per-class rows.
    for cls in ("window-per-series", "label-at-instant", "combined-window-label"):
        assert cls in md

    # §8 status.
    assert "Emitter status: **live**" in md


def test_idempotent_rerun_produces_identical_md(tmp_path):
    """Running the renderer twice over the same CSVs must give
    byte-identical MD output."""
    results = _build_results_dir(tmp_path, kind="happy")
    out1 = tmp_path / "first.md"
    out2 = tmp_path / "second.md"

    for out in (out1, out2):
        rc = mvp_report.main([
            "--results-dir", str(results),
            "--num-producers", "10",
            "--per-agent-cardinality", "500",
            "--out", str(out),
        ])
        assert rc == 0

    assert out1.read_text() == out2.read_text()


def test_sparse_fixture_renders_with_unknown_verdicts(tmp_path):
    """When most CSVs are missing, the renderer must still produce
    a complete MD — verdicts go UNKNOWN rather than raising."""
    results = _build_results_dir(tmp_path, kind="sparse")
    out = tmp_path / "MVP_REPORT.md"

    rc = mvp_report.main([
        "--results-dir", str(results),
        "--num-producers", "10",
        "--per-agent-cardinality", "500",
        "--out", str(out),
    ])
    assert rc == 0
    md = out.read_text()

    # Should still have all 8 section headers.
    assert "## §1 Stage-separated resource table" in md
    assert "## §2 Per-criterion verdict (6 criteria)" in md
    assert "## §7 Honest caveats (non-goals)" in md

    # Most criteria UNKNOWN due to missing CSVs.
    assert "UNKNOWN" in md
    # merge-pending markers present in §4 / §6.
    assert "merge-pending" in md or "cost-tracker not present" in md


def test_fallback_status_renders_correctly(tmp_path):
    results = _build_results_dir(tmp_path, kind="fallback")
    out = tmp_path / "MVP_REPORT.md"

    rc = mvp_report.main([
        "--results-dir", str(results),
        "--num-producers", "10",
        "--per-agent-cardinality", "500",
        "--out", str(out),
    ])
    assert rc == 0
    md = out.read_text()

    assert "Emitter status: **fallback-placeholder**" in md
    # The §3 plan annotations should mention the fallback state.
    assert "emitter fallback-placeholder" in md


# ── dual-mode (baseline + asap subdirs) tests ────────────────────


def _build_dual_results_dir(root: Path) -> Path:
    """Build a results dir with BOTH baseline/ and asap/ subdirs.

    Synthesised so reductions are non-trivial: baseline uses ~2x the
    CPU + RSS, ~2x the bandwidth on egress edges, and ~3x the latency
    of asap. Numbers are still deliberately tiny / round.
    """
    root.mkdir(parents=True, exist_ok=True)
    base = root / "baseline"
    asap = root / "asap"
    _build_pipeline_dir(
        base, kind="happy",
        cpu_scale=2.0,            # baseline: 2x cpu + rss
        bandwidth_scale=2.0,      # baseline: 2x bandwidth on every edge
        latency_scale=3.0,        # baseline: 3x latency
        emit_status="n/a-baseline",
        include_compactor=False,
    )
    _build_pipeline_dir(
        asap, kind="happy",
        cpu_scale=1.0,
        bandwidth_scale=1.0,
        latency_scale=1.0,
        emit_status="live",
        include_compactor=True,
    )
    return root


def test_dual_mode_renders_comparison_report(tmp_path):
    """When both baseline/ and asap/ subdirs are populated, the
    report must render the comparison layout: §1 has both rows per
    stage + reduction, §2 lists the 6 criteria with ASAP-vs-baseline
    columns + reduction percentages."""
    results = _build_dual_results_dir(tmp_path)
    out = tmp_path / "MVP_REPORT.md"

    rc = mvp_report.main([
        "--results-dir", str(results),
        "--num-producers", "10",
        "--per-agent-cardinality", "500",
        "--out", str(out),
    ])
    assert rc == 0
    md = out.read_text()

    # Header reflects dual-pipeline framing.
    assert "baseline vs ASAP" in md
    assert "Sequential" in md or "sequentially" in md  # caveat text

    # §1 dual-mode markers.
    assert "## §1 Stage-separated resource table (baseline vs ASAP)" in md
    # Baseline + asap rows + reduction row appear for at least one stage.
    assert "| baseline |" in md
    assert "| asap     |" in md
    assert "_reduction_" in md

    # §2 dual-mode subsections — all five empirical claims + accuracy
    # + cold-fallback + freshness are present.
    for header in (
        "## §2 Per-criterion verdict (baseline vs ASAP)",
        "### ① Bandwidth (mean B/s per cut edge)",
        "### ② Query latency (p99 per class)",
        "### ③ Combined e2e resource (Σ stages)",
        "### ④ Accuracy",
        "### ⑤ Cold-fallback",
        "### ⑥ Freshness",
    ):
        assert header in md, f"missing dual-mode section: {header!r}"

    # Reduction columns must produce concrete numbers — baseline 2x
    # cpu, asap 1x cpu → reduction ~+50%.
    assert "+50.0%" in md or "+49." in md or "+50." in md

    # All edges render in the bandwidth subsection.
    for e in (
        "edge_sdk_to_agent",
        "edge_agent_to_gateway",
        "edge_gateway_to_backend",
        "edge_gateway_to_s3",
    ):
        assert e in md

    # ASAP-only sections still render below the comparison.
    assert "## §3 Per-query-class breakdown (ASAP)" in md
    assert "## §4 Postings filtering effect" in md
    assert "## §5 Compaction effect" in md
    assert "## §6 S3-ops cost (measured)" in md

    # §8 status — ASAP pipeline is live.
    assert "Emitter status: **live**" in md

    # Appendix A: dual-mode side-by-side per-edge bandwidth table.
    assert "Appendix A — per-edge bandwidth (baseline vs ASAP)" in md


def test_dual_mode_idempotent(tmp_path):
    results = _build_dual_results_dir(tmp_path)
    out1 = tmp_path / "first.md"
    out2 = tmp_path / "second.md"
    for out in (out1, out2):
        rc = mvp_report.main([
            "--results-dir", str(results),
            "--num-producers", "10",
            "--per-agent-cardinality", "500",
            "--out", str(out),
        ])
        assert rc == 0
    assert out1.read_text() == out2.read_text()


def test_only_asap_subdir_falls_back_to_single_mode(tmp_path):
    """If only asap/ is populated (e.g. user passed --mode asap), the
    report must fall back to single-mode rendering — not crash, not
    render the dual-mode comparison."""
    asap = tmp_path / "asap"
    _build_pipeline_dir(asap, kind="happy", emit_status="live")
    out = tmp_path / "MVP_REPORT.md"

    rc = mvp_report.main([
        "--results-dir", str(tmp_path),
        "--num-producers", "10",
        "--per-agent-cardinality", "500",
        "--out", str(out),
    ])
    assert rc == 0
    md = out.read_text()

    # Single-mode header (no "baseline vs ASAP" framing).
    assert "controller-driven multi-stage" in md
    assert "Single-pipeline run (asap)" in md
    # No dual-mode comparison subsection headers.
    assert "## §2 Per-criterion verdict (baseline vs ASAP)" not in md


def test_only_baseline_subdir_falls_back_to_single_mode(tmp_path):
    base = tmp_path / "baseline"
    _build_pipeline_dir(base, kind="happy", emit_status="n/a-baseline",
                         include_compactor=False)
    out = tmp_path / "MVP_REPORT.md"
    rc = mvp_report.main([
        "--results-dir", str(tmp_path),
        "--num-producers", "10",
        "--per-agent-cardinality", "500",
        "--out", str(out),
    ])
    assert rc == 0
    md = out.read_text()
    assert "Single-pipeline run (baseline)" in md
    # baseline emit-status branch in §8.
    assert "Baseline pipeline has no controller" in md


def test_layout_detector():
    # Empty dir → legacy.
    import tempfile
    with tempfile.TemporaryDirectory() as d:
        assert mvp_report._detect_layout(d) == "legacy"
        os.makedirs(os.path.join(d, "baseline", "measurements"))
        assert mvp_report._detect_layout(d) == "baseline-only"
        os.makedirs(os.path.join(d, "asap", "measurements"))
        assert mvp_report._detect_layout(d) == "dual"


def test_missing_results_dir_returns_error_md(tmp_path):
    """If --results-dir doesn't exist, the renderer emits a
    self-explanatory MD rather than crashing."""
    out = tmp_path / "out.md"
    rc = mvp_report.main([
        "--results-dir", str(tmp_path / "does-not-exist"),
        "--num-producers", "10",
        "--per-agent-cardinality", "500",
        "--out", str(out),
    ])
    assert rc == 0
    md = out.read_text()
    assert "results dir missing" in md


# ── unit tests for individual helpers ────────────────────────────


def test_per_edge_mean_bytes_per_s():
    rows = [
        {"edge": "edge_sdk_to_agent",     "bytes_per_s": "100"},
        {"edge": "edge_sdk_to_agent",     "bytes_per_s": "200"},
        {"edge": "edge_agent_to_gateway", "bytes_per_s": "50"},
        # malformed row (NaN-like) is dropped:
        {"edge": "edge_agent_to_gateway", "bytes_per_s": ""},
    ]
    means = mvp_report._per_edge_mean_bytes_per_s(rows)
    assert means["edge_sdk_to_agent"] == 150.0
    assert means["edge_agent_to_gateway"] == 50.0


def test_classify_query_class():
    assert mvp_report._classify_query_class(
        {"query": "quantile_over_time(0.99, http_requests_total_latency_ms[1m])"}
    ) == "window-per-series"
    assert mvp_report._classify_query_class(
        {"query": "sum by (zone) (http_requests_total)"}
    ) == "label-at-instant"
    assert mvp_report._classify_query_class(
        {"query": "sum by (zone) (rate(http_requests_total[5m]))"}
    ) == "combined-window-label"
    # Unknown query — None.
    assert mvp_report._classify_query_class({"query": "vector(1)"}) is None


def test_count_minio_objects(tmp_path):
    p = tmp_path / "before.minio.jsonl"
    p.write_text("\n".join([
        json.dumps({"type": "file", "size": 100}),
        json.dumps({"type": "file", "size": 200}),
        # Non-file rows skipped.
        json.dumps({"type": "directory"}),
        # Malformed line skipped.
        "not-json",
    ]))
    count, total = mvp_report._count_minio_objects(str(p))
    assert count == 2
    assert total == 300


def test_extract_int_from_response_handles_missing_field():
    body = {
        "status": "success",
        "data": {"infos": ["chunks_scanned: 5", "data_source: warm"]},
    }
    assert mvp_report._extract_int_from_response(body, "chunks_scanned") == 5
    assert mvp_report._extract_int_from_response(body, "postings_filtered_series_count") is None
    assert mvp_report._extract_int_from_response(None, "anything") is None


# ── ④ per-sketch-family accuracy tests ──────────────────────────


def _all_5_sketches_accuracy_rows() -> list[dict]:
    """Synthetic accuracy.csv rows covering all 5 sketch families +
    raw passthrough — every family within its bound (PASS aggregate).

    Numbers are deliberately small / round: each rel-err sits below
    its family's epsilon ceiling, top-K recall sits above 0.85, raw
    is exactly equal.
    """
    return [
        # DDSketch — http_latency_ms, ε ≤ 0.01
        {"kind": "quantile", "query": "quantile_over_time(0.99, http_latency_ms[1m])",
         "rel_err": "0.0042", "recall": ""},
        {"kind": "quantile", "query": "quantile_over_time(0.99, http_latency_ms[1m])",
         "rel_err": "0.0050", "recall": ""},
        # KLL — request_size_bytes, ε ≤ 0.005 (rank-err proxy)
        {"kind": "quantile", "query": "quantile_over_time(0.5, request_size_bytes[1m])",
         "rel_err": "0.0030", "recall": ""},
        {"kind": "quantile", "query": "quantile_over_time(0.5, request_size_bytes[1m])",
         "rel_err": "0.0035", "recall": ""},
        # HLL — unique_users_per_min, ε ≤ 0.0325
        {"kind": "count_unique", "query": "count(unique_users_per_min)",
         "rel_err": "0.018", "recall": ""},
        # CountSketch — top_endpoint_qps, recall ≥ 0.85
        {"kind": "topk", "query": "topk(5, top_endpoint_qps)",
         "rel_err": "", "recall": "0.92"},
        # CountMinSketch — endpoint_request_freq, additive proxy ≤ 0.02
        {"kind": "sum", "query": "sum(endpoint_request_freq)",
         "rel_err": "0.014", "recall": ""},
        # raw — http_requests_total, exact
        {"kind": "sum", "query": "sum(http_requests_total)",
         "rel_err": "0.0", "recall": ""},
    ]


def test_criterion_accuracy_per_sketch_all_pass():
    rows = _all_5_sketches_accuracy_rows()
    aggregate, per_family, summary = mvp_report.criterion_accuracy_per_sketch(rows)
    assert aggregate == "PASS"
    assert "6/6" in summary
    # 6 rows in the per-family table: 5 sketches + raw, in
    # contract order.
    families = [r["family"] for r in per_family]
    assert families == [
        "DDSketch", "KLL", "HLL", "CountSketch", "CountMinSketch", "raw",
    ]
    for r in per_family:
        assert r["verdict"] == "PASS"


def test_criterion_accuracy_per_sketch_one_family_fails():
    """Push DDSketch over its 0.01 bound — aggregate must FAIL."""
    rows = _all_5_sketches_accuracy_rows()
    rows[0]["rel_err"] = "0.05"  # blow past ε = 0.01
    rows[1]["rel_err"] = "0.06"
    aggregate, per_family, _ = mvp_report.criterion_accuracy_per_sketch(rows)
    assert aggregate == "FAIL"
    by_family = {r["family"]: r["verdict"] for r in per_family}
    assert by_family["DDSketch"] == "FAIL"
    assert by_family["KLL"] == "PASS"
    assert by_family["raw"] == "PASS"


def test_criterion_accuracy_per_sketch_missing_family_unknown():
    """No rows for HLL → that family is UNKNOWN → aggregate UNKNOWN."""
    rows = [r for r in _all_5_sketches_accuracy_rows()
            if "unique_users_per_min" not in r["query"]]
    aggregate, per_family, _ = mvp_report.criterion_accuracy_per_sketch(rows)
    assert aggregate == "UNKNOWN"
    by_family = {r["family"]: r["verdict"] for r in per_family}
    assert by_family["HLL"] == "UNKNOWN"
    assert by_family["DDSketch"] == "PASS"


def test_criterion_accuracy_per_sketch_raw_must_be_exact():
    """A non-zero error on the raw row makes raw FAIL → aggregate FAIL."""
    rows = _all_5_sketches_accuracy_rows()
    # Replace raw row with non-zero error.
    rows[-1] = {"kind": "sum", "query": "sum(http_requests_total)",
                "rel_err": "0.001", "recall": ""}
    aggregate, per_family, _ = mvp_report.criterion_accuracy_per_sketch(rows)
    assert aggregate == "FAIL"
    by_family = {r["family"]: r["verdict"] for r in per_family}
    assert by_family["raw"] == "FAIL"


def test_criterion_accuracy_per_sketch_empty_csv():
    aggregate, per_family, summary = mvp_report.criterion_accuracy_per_sketch([])
    assert aggregate == "UNKNOWN"
    assert per_family == []
    assert "empty" in summary


def test_render_accuracy_per_sketch_table_shape():
    """Verify the rendered ④ table has the expected 5+1 row shape."""
    rows = _all_5_sketches_accuracy_rows()
    _, per_family, _ = mvp_report.criterion_accuracy_per_sketch(rows)
    md = mvp_report.render_accuracy_per_sketch_table(per_family)
    # Header + separator + 6 family rows = 8 lines.
    assert len(md) == 8
    # Header columns.
    assert "Sketch family" in md[0]
    assert "Metric" in md[0]
    assert "Query class" in md[0]
    assert "ε bound" in md[0]
    assert "Verdict" in md[0]
    # Each family appears in its own row, in contract order.
    assert "| DDSketch |" in md[2]
    assert "| KLL |" in md[3]
    assert "| HLL |" in md[4]
    assert "| CountSketch |" in md[5]
    assert "| CountMinSketch |" in md[6]
    assert "| raw |" in md[7]


def test_per_sketch_table_renders_in_single_mode_report(tmp_path):
    """End-to-end: build a results dir whose accuracy.csv covers all
    5 sketch families + raw, render the report, assert the §3
    per-family table includes all 6 rows + an aggregate ④ verdict."""
    results = _build_results_dir(tmp_path, kind="happy")
    # Replace the synthetic accuracy.csv with one that has all 5
    # sketch family metrics + raw — the 6-row contract.
    rows = _all_5_sketches_accuracy_rows()
    csv_rows = []
    for r in rows:
        csv_rows.append([
            "mvp", r["kind"], r["query"], "0", "5.0", "p1",
            "", "", r.get("rel_err", ""), r.get("recall", ""),
            "", "ok", "", "", "", "", "",
        ])
    _write_csv(
        results / "measurements" / "accuracy.csv",
        ["cell", "kind", "query", "t", "duration_ms", "plan_id",
         "warm_answer", "archive_answer", "rel_err", "recall",
         "archive_query_latency_ms", "archive_status", "n_chunks_read",
         "truth", "answer", "error", "n_truth_samples"],
        csv_rows,
    )

    out = tmp_path / "MVP_REPORT.md"
    rc = mvp_report.main([
        "--results-dir", str(results),
        "--num-producers", "10",
        "--per-agent-cardinality", "500",
        "--out", str(out),
    ])
    assert rc == 0
    md = out.read_text()

    # §3 header for the per-sketch family table.
    assert "## §3 Accuracy (per sketch family)" in md
    # All 6 family rows present.
    for fam in ("DDSketch", "KLL", "HLL", "CountSketch",
                "CountMinSketch", "raw"):
        assert f"| {fam} |" in md
    # All 6 metric names present.
    for metric in ("http_latency_ms", "request_size_bytes",
                   "unique_users_per_min", "top_endpoint_qps",
                   "endpoint_request_freq", "http_requests_total"):
        assert metric in md
    # Aggregate ④ verdict in the §2 table — PASS for this fixture
    # (all families within bound).
    assert "Accuracy (per sketch family, see §3)" in md
    # 6/6 summary text in §2 row.
    assert "6/6" in md


def test_per_sketch_table_renders_in_dual_mode_report(tmp_path):
    """Dual-mode: ④ section renders the per-family table inline."""
    results = _build_dual_results_dir(tmp_path)
    # Patch the asap-side accuracy.csv with all-5-sketch rows.
    rows = _all_5_sketches_accuracy_rows()
    csv_rows = []
    for r in rows:
        csv_rows.append([
            "mvp", r["kind"], r["query"], "0", "5.0", "p1",
            "", "", r.get("rel_err", ""), r.get("recall", ""),
            "", "ok", "", "", "", "", "",
        ])
    _write_csv(
        results / "asap" / "measurements" / "accuracy.csv",
        ["cell", "kind", "query", "t", "duration_ms", "plan_id",
         "warm_answer", "archive_answer", "rel_err", "recall",
         "archive_query_latency_ms", "archive_status", "n_chunks_read",
         "truth", "answer", "error", "n_truth_samples"],
        csv_rows,
    )

    out = tmp_path / "MVP_REPORT.md"
    rc = mvp_report.main([
        "--results-dir", str(results),
        "--num-producers", "10",
        "--per-agent-cardinality", "500",
        "--out", str(out),
    ])
    assert rc == 0
    md = out.read_text()

    assert "### ④ Accuracy (per sketch family)" in md
    for fam in ("DDSketch", "KLL", "HLL", "CountSketch",
                "CountMinSketch", "raw"):
        assert f"| {fam} |" in md
    # Aggregate ④ verdict line.
    assert "**Verdict ④:**" in md
    assert "6/6 families within bound" in md


# ── top_k_family_label helper (CMS-Heap acceptance for top-K) ────


def test_top_k_family_label_default_is_countsketch_alt():
    """No workload spec: label should advertise both families (the
    canonical CountSketch + the CMS-Heap alternative)."""
    label = mvp_report.top_k_family_label(None)
    assert "CountSketch" in label
    assert "CountMin" in label, (
        "default label should mention CMS-Heap alternative"
    )


def test_top_k_family_label_pinned_to_countsketch(tmp_path):
    """Workload pins CountSketch — label is the canonical
    CountSketch only."""
    spec = tmp_path / "mvp-workload.yaml"
    spec.write_text(
        "- metric_name: top_endpoint_qps\n"
        "  query_string: \"topk(5, top_endpoint_qps)\"\n"
        "  sketch_family_override: CountSketch\n"
    )
    assert mvp_report.top_k_family_label(str(spec)) == "CountSketch"


def test_top_k_family_label_pinned_to_countmin(tmp_path):
    """Workload pins CountMinSketch — label reflects the CMS-Heap
    pattern (CountMinSketch)."""
    spec = tmp_path / "mvp-workload.yaml"
    spec.write_text(
        "- metric_name: top_endpoint_qps\n"
        "  query_string: \"topk(5, top_endpoint_qps)\"\n"
        "  sketch_family_override: CountMinSketch\n"
    )
    label = mvp_report.top_k_family_label(str(spec))
    assert label.startswith("CountMinSketch")
    assert "CMS-Heap" in label, "CMS pin should annotate CMS-Heap"


def test_top_k_family_label_no_override_falls_back(tmp_path):
    """Workload has the metric but no sketch_family_override —
    falls back to the dual-family label."""
    spec = tmp_path / "mvp-workload.yaml"
    spec.write_text(
        "- metric_name: top_endpoint_qps\n"
        "  query_string: \"topk(5, top_endpoint_qps)\"\n"
    )
    label = mvp_report.top_k_family_label(str(spec))
    assert "CountSketch" in label and "CountMin" in label
