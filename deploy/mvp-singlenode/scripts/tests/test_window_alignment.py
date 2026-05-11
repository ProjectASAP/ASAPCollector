"""Tests pinning replay-query window alignment with warm-tier
pre-compute window.

This file guards against the regression fixed in
`fix/quantile-window-alignment` (issue #46 ε-bound bug):

  - The warm-tier ASAPQuery streaming pre-compute is configured with
    `windowSize: 30` in `deploy/mvp-singlenode/configs/backend-streaming.yaml` and
    its variants. The warm tier therefore returns quantiles over a
    30-second tumbling window.
  - The DDSketch / KLL replay queries in `mvp-workload.yaml` and the
    `replay-queries.json` heredoc inside `run_mvp_demo.sh` MUST ask
    for the same range (`[30s]`); any mismatch (e.g. asking `[1m]`)
    means the warm tier returns a quantile over different data than
    the archive ground truth. The result is a rel-err that violates
    the DDSketch ε bound (the headline-2026-05-06 measurement
    captured rel_err mean=0.126 with a `[1m]` replay range against
    a 30s warm window — well above ε=0.01).

The tests below do TWO things:

  1. Static config check: parse `mvp-workload.yaml`, `run_mvp_demo.sh`'s
     embedded JSON, and `backend-streaming.yaml`, and assert that
     every quantile-class replay range matches the warm-tier
     `windowSize`. This is a string-level pin: any future edit that
     desyncs the two surfaces will fail this test.

  2. End-to-end rel_err synthesis: pipe a synthetic replay row plus a
     stub archive response through `accuracy_reduce.py` (the same
     reducer the demo runs) and assert that, when the warm and
     archive answers come from the SAME window, rel_err is at or
     below the DDSketch ε bound. This protects against a different
     class of regression where the reducer formula itself drifts.
"""
from __future__ import annotations

import csv
import importlib.util
import json
import re
import sys
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

# Repo paths — resolve from this file's location so the tests are
# hermetic against the caller's CWD.
TESTS_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = TESTS_DIR.parent
DEPLOY_DIR = SCRIPTS_DIR.parent
CONFIGS_DIR = DEPLOY_DIR / "configs"
REPO_ROOT = DEPLOY_DIR.parent

MVP_WORKLOAD = CONFIGS_DIR / "mvp-workload.yaml"
RUN_MVP_DEMO = SCRIPTS_DIR / "run_mvp_demo.sh"
BACKEND_STREAMING = CONFIGS_DIR / "backend-streaming.yaml"

# Load accuracy_reduce.py without requiring an installed package —
# mirrors test_accuracy_reduce.py's loader.
ACCURACY_REDUCE_PATH = SCRIPTS_DIR / "accuracy_reduce.py"
spec = importlib.util.spec_from_file_location("accuracy_reduce", ACCURACY_REDUCE_PATH)
assert spec is not None and spec.loader is not None
accuracy_reduce = importlib.util.module_from_spec(spec)
sys.modules["accuracy_reduce"] = accuracy_reduce
spec.loader.exec_module(accuracy_reduce)


# ── helpers to extract window strings ────────────────────────────────


_RANGE_RE = re.compile(r"\[(\d+)([smhd])\]")


def _to_seconds(amount: int, unit: str) -> int:
    return amount * {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]


def _quantile_ranges_in_text(text: str) -> list[int]:
    """Return the list of `[N<unit>]` ranges (in seconds) found
    immediately after a `quantile_over_time` token in `text`. The
    five-sketch workload's quantile metrics (DDSketch, KLL) are the
    families whose ε-bound we are pinning."""
    out: list[int] = []
    # Walk every `quantile_over_time(...)` invocation and grab the
    # `[N<unit>]` token after the metric name. The regex below is
    # intentionally permissive (any chars between `quantile_over_time`
    # and `]`) so it survives whitespace / argument variations.
    for m in re.finditer(r"quantile_over_time\([^)]*?\[(\d+)([smhd])\]", text):
        out.append(_to_seconds(int(m.group(1)), m.group(2)))
    return out


def _warm_window_seconds() -> int:
    """Parse `backend-streaming.yaml` for the DDSketch aggregation's
    `windowSize` (in seconds). The MVP DDSketch entry is the
    aggregation we are aligning against."""
    text = BACKEND_STREAMING.read_text()
    # `windowSize: 30` — capture the integer. There may be more than
    # one aggregation in the file; the MVP demo's DDSketch entries
    # all share the same value, so we assert that they're consistent.
    matches = re.findall(r"^\s*windowSize:\s*(\d+)\s*$", text, flags=re.M)
    assert matches, f"no windowSize in {BACKEND_STREAMING}"
    secs = {int(m) for m in matches}
    assert len(secs) == 1, (
        f"backend-streaming.yaml has inconsistent windowSize values: "
        f"{secs}; replay-window alignment requires a single canonical value"
    )
    return next(iter(secs))


# ── (1) static config alignment ──────────────────────────────────────


def test_warm_window_is_30s_canonical():
    """Pin the warm-tier pre-compute window. If this changes, every
    quantile replay range must change in lock-step (the next two
    tests will catch the desync)."""
    assert _warm_window_seconds() == 30


def test_mvp_workload_quantile_ranges_match_warm_window():
    """Every `quantile_over_time(...)` range in `mvp-workload.yaml`
    MUST equal the warm tier's pre-compute window. The controller's
    analyzer parses these ranges to set the planner's `time_window`,
    which in turn drives the agent's `window_duration`. Mismatch =>
    warm tier computes over a different range than the replay asks
    for => rel-err blows the ε bound."""
    text = MVP_WORKLOAD.read_text()
    ranges = _quantile_ranges_in_text(text)
    assert ranges, f"no quantile_over_time entries in {MVP_WORKLOAD}"
    warm = _warm_window_seconds()
    for r in ranges:
        assert r == warm, (
            f"mvp-workload.yaml has a quantile range of {r}s but warm "
            f"pre-compute window is {warm}s — replay would compute over "
            f"different data than warm. Update both surfaces together."
        )


def test_run_mvp_demo_replay_quantile_ranges_match_warm_window():
    """The runtime replay JSON is generated by a heredoc inside
    `run_mvp_demo.sh`. Its quantile ranges MUST match the warm
    window for the same reason as above."""
    text = RUN_MVP_DEMO.read_text()
    ranges = _quantile_ranges_in_text(text)
    assert ranges, f"no quantile_over_time entries in {RUN_MVP_DEMO}"
    warm = _warm_window_seconds()
    for r in ranges:
        assert r == warm, (
            f"run_mvp_demo.sh has a quantile replay range of {r}s but "
            f"warm pre-compute window is {warm}s. Update both surfaces "
            f"together (mvp-workload.yaml and run_mvp_demo.sh)."
        )


def test_replay_quantile_ranges_consistent_across_workload_and_demo():
    """Even if both surfaces drift away from the warm window, they
    MUST agree with each other — the controller is fed the workload
    YAML, the replay client is fed the heredoc, and a desync would
    silently bind the planner to one window and the verification
    rig to another."""
    workload_ranges = set(_quantile_ranges_in_text(MVP_WORKLOAD.read_text()))
    demo_ranges = set(_quantile_ranges_in_text(RUN_MVP_DEMO.read_text()))
    assert workload_ranges == demo_ranges, (
        f"quantile ranges drifted: mvp-workload.yaml={workload_ranges}, "
        f"run_mvp_demo.sh={demo_ranges}"
    )


# ── (2) end-to-end rel_err ≤ ε when windows are aligned ──────────────
#
# Reuse the stub-backend pattern from test_accuracy_reduce.py: a
# small in-process HTTP server pretends to be the archive engine and
# returns a canned ground-truth answer for the quantile query. We
# synthesize a warm answer that is INSIDE the DDSketch ε bound and
# assert that the reducer reports rel_err ≤ ε.


def _ok_vector(value: float, data_source: str = "thanos_archive") -> bytes:
    body = {
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [{"metric": {}, "value": [1700000000, str(value)]}],
        },
        "infos": [f"data_source: {data_source}"],
    }
    return json.dumps(body).encode("utf-8")


class _StubBackend(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        query = (qs.get("query") or [""])[0]
        engine = self.headers.get(accuracy_reduce.ENGINE_OVERRIDE_HEADER)
        if engine != accuracy_reduce.DEFAULT_ARCHIVE_ENGINE_ID:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b"missing override header")
            return
        canned = self.server.responses.get(query)  # type: ignore[attr-defined]
        if canned is None:
            self.send_response(404)
            self.end_headers()
            return
        status, body = canned
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):  # silence test output
        return


@contextmanager
def _stub_backend(responses: dict[str, tuple[int, bytes]]):
    server = HTTPServer(("127.0.0.1", 0), _StubBackend)
    server.responses = responses  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        yield f"http://{host}:{port}"
    finally:
        server.shutdown()
        thread.join(timeout=2)


# DDSketch ε bound from the controller's emitted `relative_accuracy`
# (see PROGRESS.md §244 quoting the warm response's accuracy info-
# line: `accuracy: ε=0.01, δ=0, kind=relative_quantile`).
DDSKETCH_EPSILON = 0.01


def test_e2e_rel_err_within_epsilon_when_windows_aligned(tmp_path):
    """Synthesize a replay row with a warm-tier quantile answer that
    is `(1 + ε) * truth` — i.e. exactly at the DDSketch ε bound — and
    confirm `accuracy_reduce.py` computes rel_err ≤ ε. This is the
    end-to-end shape of the post-fix accuracy.csv: with both warm
    and archive answering over the same 30s window, the only error
    left is the sketch's own approximation, which IS bounded by ε."""
    cell_dir = tmp_path / "aligned"
    cell_dir.mkdir()

    # Use the same quantile shape the demo issues post-fix.
    query = "quantile_over_time(0.99, http_requests_total_latency_ms[30s])"
    truth = 100.0
    # Warm answer: at the ε boundary. With aligned windows the
    # observed error is the sketch's own ε; with mis-aligned windows
    # we observed mean=0.126 which is 12x too large.
    warm_ans = truth * (1.0 + DDSKETCH_EPSILON)

    (cell_dir / "replay.jsonl").write_text(
        json.dumps(
            {
                "ts": "2026-05-08T12:00:00Z",
                "query": query,
                "kind": "quantile",
                "duration_ms": 5.0,
                "plan_id": "plan-aligned",
                "result": [{"metric": {}, "value": [1700000000, str(warm_ans)]}],
                "result_type": "vector",
                "status": "success",
            }
        )
        + "\n"
    )

    out_path = tmp_path / "accuracy.csv"
    with _stub_backend({query: (200, _ok_vector(truth))}) as base_url:
        rc = accuracy_reduce.main(
            [
                "--cell-dir", str(cell_dir),
                "--backend", base_url,
                "--out", str(out_path),
            ]
        )
    assert rc == 0

    rows = list(csv.DictReader(out_path.open()))
    assert len(rows) == 1
    r = rows[0]
    assert r["kind"] == "quantile"
    rel_err = float(r["rel_err"])
    # rel_err = |warm - truth| / max(|truth|, 1) = ε * truth / truth = ε.
    # Allow a tiny float tolerance — equality up to FP rounding.
    assert rel_err <= DDSKETCH_EPSILON + 1e-9, (
        f"rel_err {rel_err} exceeds DDSketch ε={DDSKETCH_EPSILON} when "
        f"windows are aligned — reducer formula or sketch bound has "
        f"regressed"
    )


def test_e2e_rel_err_blows_epsilon_when_windows_misaligned(tmp_path):
    """The negative twin of the test above: when warm and archive
    answer over different windows, the warm answer can be far from
    truth even if the sketch itself is within ε. We synthesize a
    warm answer that's 12% off (matching the headline-2026-05-06
    observed mean rel_err of 0.126 with `[1m]` vs warm's 30s) and
    confirm the reducer reports rel_err > ε. This documents the
    failure mode the alignment fix prevents."""
    cell_dir = tmp_path / "misaligned"
    cell_dir.mkdir()

    query = "quantile_over_time(0.99, http_requests_total_latency_ms[1m])"
    truth = 100.0
    warm_ans = truth * 1.126  # Mirrors the observed pre-fix mean.

    (cell_dir / "replay.jsonl").write_text(
        json.dumps(
            {
                "ts": "2026-05-08T12:00:00Z",
                "query": query,
                "kind": "quantile",
                "duration_ms": 5.0,
                "plan_id": "plan-misaligned",
                "result": [{"metric": {}, "value": [1700000000, str(warm_ans)]}],
                "result_type": "vector",
                "status": "success",
            }
        )
        + "\n"
    )

    out_path = tmp_path / "accuracy.csv"
    with _stub_backend({query: (200, _ok_vector(truth))}) as base_url:
        rc = accuracy_reduce.main(
            [
                "--cell-dir", str(cell_dir),
                "--backend", base_url,
                "--out", str(out_path),
            ]
        )
    assert rc == 0

    rows = list(csv.DictReader(out_path.open()))
    assert len(rows) == 1
    rel_err = float(rows[0]["rel_err"])
    # Confirm we modeled the bug correctly: rel_err well above ε.
    assert rel_err > DDSKETCH_EPSILON, (
        f"rel_err {rel_err} did NOT exceed ε={DDSKETCH_EPSILON} — "
        f"misalignment fixture is wrong (or reducer is averaging it away)"
    )
    # And matches the observed pre-fix magnitude (0.126 ± float dust).
    assert rel_err == pytest.approx(0.126, rel=1e-3)
