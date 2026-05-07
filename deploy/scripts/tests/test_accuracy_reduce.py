"""Unit tests for accuracy_reduce.py (Phase-6 Fix 1).

Hermetic — we spin up a stub HTTP server that pretends to be the
backend's Gorilla-archive engine (i.e., honours the
`X-ASAP-Engine: gorilla_archive` header by returning a fixed exact
answer for the queried PromQL).

Synthesis policy: every numeric value is a small round number that's
clearly synthetic. We do NOT exercise the real backend — these tests
just prove the reducer issues the right HTTP requests and emits the
right CSV rows.
"""
from __future__ import annotations

import csv
import importlib.util
import json
import sys
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

# Load accuracy_reduce.py without requiring an installed package.
HERE = Path(__file__).resolve().parent
SCRIPT = HERE.parent / "accuracy_reduce.py"
spec = importlib.util.spec_from_file_location("accuracy_reduce", SCRIPT)
assert spec is not None and spec.loader is not None
accuracy_reduce = importlib.util.module_from_spec(spec)
sys.modules["accuracy_reduce"] = accuracy_reduce
spec.loader.exec_module(accuracy_reduce)


# ── stub backend that pretends to be the archive engine ─────────────


def _ok_vector(value: float, data_source: str = "gorilla_archive") -> bytes:
    """Prometheus instant-vector envelope with `data_source: <id>`
    info-line. Mirrors what the backend's query handler returns
    after `process_via_named_engine`."""
    body = {
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [
                {"metric": {}, "value": [1700000000, str(value)]}
            ],
        },
        "infos": [f"data_source: {data_source}"],
    }
    return json.dumps(body).encode("utf-8")


def _empty_vector(data_source: str = "gorilla_archive") -> bytes:
    body = {
        "status": "success",
        "data": {"resultType": "vector", "result": []},
        "infos": [f"data_source: {data_source}"],
    }
    return json.dumps(body).encode("utf-8")


def _error_404(error_type: str = "bad_data") -> bytes:
    body = {"status": "error", "errorType": error_type, "error": "capability miss"}
    return json.dumps(body).encode("utf-8")


class _StubBackend(BaseHTTPRequestHandler):
    """Replays a configured response for each query, asserting that
    the override header was set. Stamps the server instance with:

      - `responses`: dict[str query → bytes JSON body]
      - `received`: list[(query, headers)] for assertion
      - `default_status`: int (HTTP status for queries not in
        `responses`)
      - `default_body`: bytes (body for queries not in `responses`)
    """

    def do_GET(self):  # noqa: N802
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        query = (qs.get("query") or [""])[0]
        # `self.headers` is case-insensitive (http.client.HTTPMessage)
        engine = self.headers.get(accuracy_reduce.ENGINE_OVERRIDE_HEADER)

        # Record the request so the test can assert. Snapshot keys
        # exactly as received so the assertion can do its own
        # case-insensitive lookup.
        self.server.received.append((query, dict(self.headers)))  # type: ignore[attr-defined]

        if engine != accuracy_reduce.DEFAULT_ARCHIVE_ENGINE_ID:
            # Override header missing — should never happen if reducer
            # is correct; treat as test failure surface.
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b"missing override header")
            return

        canned = self.server.responses.get(query)  # type: ignore[attr-defined]
        if canned is not None:
            status, body = canned
        else:
            status = self.server.default_status  # type: ignore[attr-defined]
            body = self.server.default_body  # type: ignore[attr-defined]

        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):  # silence test output
        return


@contextmanager
def _stub_backend(
    responses: dict[str, tuple[int, bytes]] | None = None,
    default_status: int = 200,
    default_body: bytes | None = None,
):
    server = HTTPServer(("127.0.0.1", 0), _StubBackend)
    server.responses = responses or {}  # type: ignore[attr-defined]
    server.received = []  # type: ignore[attr-defined]
    server.default_status = default_status  # type: ignore[attr-defined]
    server.default_body = default_body or _empty_vector()  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        yield server, f"http://{host}:{port}"
    finally:
        server.shutdown()
        thread.join(timeout=2)


def _write_replay(cell_dir: Path, rows: list[dict]) -> None:
    cell_dir.mkdir(parents=True, exist_ok=True)
    p = cell_dir / "replay.jsonl"
    with p.open("w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


# ── ArchiveTruthClient ───────────────────────────────────────────────


def _header_lookup(headers: dict, name: str) -> str | None:
    """`http.server` lower-cases header keys via `BaseHTTPRequestHandler`.
    Look up case-insensitively so the assertion stays robust."""
    target = name.lower()
    for k, v in headers.items():
        if k.lower() == target:
            return v
    return None


def test_archive_client_sends_override_header():
    """The client MUST set `X-ASAP-Engine: gorilla_archive` on every
    request — that's the whole point of Fix 1."""
    with _stub_backend(default_body=_ok_vector(42.0)) as (server, base_url):
        client = accuracy_reduce.ArchiveTruthClient(base_url)
        out = client.query("sum(foo)")
        assert out["status"] == "ok"
        # Server recorded exactly one request and the override
        # header was present.
        assert len(server.received) == 1
        _, headers = server.received[0]
        got = _header_lookup(headers, accuracy_reduce.ENGINE_OVERRIDE_HEADER)
        assert got == accuracy_reduce.DEFAULT_ARCHIVE_ENGINE_ID


def test_archive_client_extracts_scalar_from_vector():
    with _stub_backend(default_body=_ok_vector(123.5)) as (_, base_url):
        client = accuracy_reduce.ArchiveTruthClient(base_url)
        out = client.query("sum(foo)")
        assert out["status"] == "ok"
        assert accuracy_reduce.extract_scalar(out["result"]) == pytest.approx(123.5)
        # latency is a non-negative float
        assert isinstance(out["latency_ms"], float)
        assert out["latency_ms"] >= 0.0


def test_archive_client_treats_empty_result_as_archive_miss():
    """Empty instant vector → archive_miss (chunk hasn't landed
    yet). NOT an error — the reducer must still emit a row."""
    with _stub_backend(default_body=_empty_vector()) as (_, base_url):
        client = accuracy_reduce.ArchiveTruthClient(base_url)
        out = client.query("sum(foo)")
        assert out["status"] == "archive_miss"
        assert out["error"] is None


def test_archive_client_treats_404_as_archive_miss():
    """404 = capability miss = archive_miss (the engine couldn't
    serve this query shape; not a plumbing bug)."""
    with _stub_backend(default_status=404, default_body=_error_404()) as (_, base_url):
        client = accuracy_reduce.ArchiveTruthClient(base_url)
        out = client.query("sum(foo)")
        assert out["status"] == "archive_miss"


def test_archive_client_treats_500_as_archive_error():
    with _stub_backend(default_status=500, default_body=b"backend exploded") as (_, base_url):
        client = accuracy_reduce.ArchiveTruthClient(base_url)
        out = client.query("sum(foo)")
        assert out["status"] == "archive_error"
        assert "500" in (out["error"] or "")


def test_archive_client_flags_unexpected_data_source():
    """If the wire response lacks the expected `data_source:
    gorilla_archive` info-line, the override didn't take effect
    (e.g., backend predates Fix 1). Flagged as archive_error."""
    body = _ok_vector(1.0, data_source="sketch_warm")  # WRONG engine
    with _stub_backend(default_body=body) as (_, base_url):
        client = accuracy_reduce.ArchiveTruthClient(base_url)
        out = client.query("sum(foo)")
        assert out["status"] == "archive_error"
        assert "data_source" in (out["error"] or "")


# ── reduce_cell_via_archive ──────────────────────────────────────────


def test_reduce_cell_writes_csv_with_archive_truth_and_rel_err(tmp_path):
    """End-to-end: replay row carries warm answer; archive returns
    a different exact answer; the CSV row carries both + computed
    rel-err."""
    cell_dir = tmp_path / "cell_a"
    _write_replay(
        cell_dir,
        [
            {
                "ts": "2026-05-06T12:00:00Z",
                "query": "sum_over_time(foo[1m])",
                "kind": "sum",
                "duration_ms": 12.5,
                "plan_id": "plan-7",
                # Warm-tier (under-test) answer = 100
                "result": [{"metric": {}, "value": [1700000000, "100"]}],
                "result_type": "vector",
                "status": "success",
            },
        ],
    )
    # Archive (ground truth) returns 110 → rel_err = 10 / 110 ≈ 0.0909...
    responses = {
        "sum_over_time(foo[1m])": (200, _ok_vector(110.0)),
    }
    out_path = tmp_path / "accuracy.csv"
    with _stub_backend(responses=responses) as (_, base_url):
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
    assert r["cell"] == "cell_a"
    assert r["kind"] == "sum"
    assert r["query"] == "sum_over_time(foo[1m])"
    assert r["plan_id"] == "plan-7"
    assert r["warm_answer"] == "100.000000"
    assert r["archive_answer"] == "110.000000"
    # rel_err = |100 - 110| / max(|110|, 1) = 10/110 ≈ 0.090909
    assert float(r["rel_err"]) == pytest.approx(10.0 / 110.0, rel=1e-3)
    assert r["archive_status"] == "ok"
    # Cost-of-truth-fetching is captured.
    assert float(r["archive_query_latency_ms"]) >= 0.0
    # Legacy aliases still populated for downstream consumers.
    assert r["truth"] == r["archive_answer"]
    assert r["answer"] == r["warm_answer"]
    assert r["error"] == r["rel_err"]


def test_reduce_cell_handles_archive_miss_without_crashing(tmp_path):
    """When the archive returns no result (chunk hasn't landed), the
    row must still be emitted with `archive_status=archive_miss` and
    NaN-treated rel_err. The reducer MUST NOT crash."""
    cell_dir = tmp_path / "cell_b"
    _write_replay(
        cell_dir,
        [
            {
                "ts": "2026-05-06T12:00:00Z",
                "query": "sum_over_time(missing_metric[1m])",
                "kind": "sum",
                "duration_ms": 5.0,
                "plan_id": "plan-7",
                "result": [{"metric": {}, "value": [1700000000, "42"]}],
                "result_type": "vector",
                "status": "success",
            },
        ],
    )
    out_path = tmp_path / "accuracy.csv"
    with _stub_backend(default_body=_empty_vector()) as (_, base_url):
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
    assert r["archive_status"] == "archive_miss"
    assert r["rel_err"] == ""
    assert r["archive_answer"] == ""
    # Warm answer is preserved for diagnostic value
    assert r["warm_answer"] == "42.000000"


def test_reduce_cell_emits_parse_skip_for_unrecognised_query(tmp_path):
    cell_dir = tmp_path / "cell_c"
    _write_replay(
        cell_dir,
        [
            {
                "ts": "2026-05-06T12:00:00Z",
                # Not a shape parse_query knows about.
                "query": "label_replace(foo, 'a', 'b', 'c', 'd')",
                "kind": "other",
                "duration_ms": 1.0,
                "plan_id": None,
                "result": None,
                "status": "success",
            },
        ],
    )
    out_path = tmp_path / "accuracy.csv"
    with _stub_backend() as (server, base_url):
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
    assert rows[0]["archive_status"] == "parse_skip"
    # We didn't send a query to the backend for parse_skip rows.
    assert len(server.received) == 0


def test_reduce_cell_quantile_computes_rel_err_against_archive(tmp_path):
    """quantile case — warm-tier quantile sketch returns 0.5; archive
    exact returns 0.55; rel-err is computed."""
    cell_dir = tmp_path / "cell_q"
    _write_replay(
        cell_dir,
        [
            {
                "ts": "2026-05-06T12:00:00Z",
                "query": "quantile_over_time(0.95, foo[1m])",
                "kind": "quantile",
                "duration_ms": 7.0,
                "plan_id": "plan-q",
                "result": [{"metric": {}, "value": [1700000000, "0.5"]}],
                "result_type": "vector",
                "status": "success",
            },
        ],
    )
    responses = {
        "quantile_over_time(0.95, foo[1m])": (200, _ok_vector(0.55)),
    }
    out_path = tmp_path / "accuracy.csv"
    with _stub_backend(responses=responses) as (_, base_url):
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
    assert r["warm_answer"] == "0.500000"
    assert r["archive_answer"] == "0.550000"
    # rel_err = |0.5 - 0.55| / max(|0.55|, 1) = 0.05 / 1 = 0.05
    # (archive answer < 1, so the denominator clamps to 1)
    assert float(r["rel_err"]) == pytest.approx(0.05, rel=1e-6)


def test_csv_header_includes_fix1_columns(tmp_path):
    """Schema-stability test: the new columns are at the documented
    positions so downstream readers (mvp_report.py) keep working."""
    cell_dir = tmp_path / "cell_h"
    _write_replay(cell_dir, [])
    out_path = tmp_path / "accuracy.csv"
    with _stub_backend() as (_, base_url):
        rc = accuracy_reduce.main(
            [
                "--cell-dir", str(cell_dir),
                "--backend", base_url,
                "--out", str(out_path),
            ]
        )
    assert rc == 0

    with out_path.open() as f:
        reader = csv.reader(f)
        header = next(reader)
    # New Fix-1 columns must be present.
    for col in (
        "warm_answer",
        "archive_answer",
        "rel_err",
        "archive_query_latency_ms",
        "archive_status",
        "n_chunks_read",
    ):
        assert col in header, f"missing column {col}: {header}"
    # Legacy columns retained for downstream reader back-compat.
    for col in ("truth", "answer", "error", "n_truth_samples"):
        assert col in header, f"missing legacy column {col}: {header}"


def test_use_jsonl_flag_falls_back_to_legacy_path(tmp_path):
    """`--use-jsonl` reads from `cold-truth/` and skips the archive
    fetch entirely. Proves the transition fallback works."""
    cell_dir = tmp_path / "cell_jsonl"
    cell_dir.mkdir()
    _write_replay(
        cell_dir,
        [
            {
                "ts": "2026-05-06T12:00:00Z",
                "query": "sum_over_time(foo[1m])",
                "kind": "sum",
                "duration_ms": 1.0,
                "plan_id": "plan-jsonl",
                "result": [{"metric": {}, "value": [1700000000, "20"]}],
                "result_type": "vector",
                "status": "success",
            },
        ],
    )
    # Synthesise a cold-truth/foo/.../part-0.jsonl with three samples
    # summing to 20 → warm == truth → rel_err ≈ 0.
    cold_dir = cell_dir / "cold-truth" / "foo" / "2026" / "05" / "06" / "12"
    cold_dir.mkdir(parents=True)
    samples = [{"ts_ms": 1700000000, "labels": {}, "value": v} for v in (5.0, 7.0, 8.0)]
    with (cold_dir / "part-0.jsonl").open("w") as f:
        for s in samples:
            f.write(json.dumps(s) + "\n")

    out_path = tmp_path / "accuracy.csv"
    rc = accuracy_reduce.main(
        [
            "--cell-dir", str(cell_dir),
            "--use-jsonl",
            # backend URL is irrelevant when --use-jsonl is set
            "--backend", "http://127.0.0.1:1",
            "--out", str(out_path),
        ]
    )
    assert rc == 0

    rows = list(csv.DictReader(out_path.open()))
    assert len(rows) == 1
    r = rows[0]
    assert r["archive_status"] == "jsonl"
    assert float(r["truth"]) == pytest.approx(20.0)
    assert float(r["answer"]) == pytest.approx(20.0)
    assert float(r["error"]) == pytest.approx(0.0)
    # New aliases mirror the legacy values.
    assert r["archive_answer"] == r["truth"]
    assert r["warm_answer"] == r["answer"]
    assert r["rel_err"] == r["error"]


def test_reduce_cell_topk_recall_against_archive(tmp_path):
    """topk recall: archive returns top-3 keys; warm sketch returns 2
    of them → recall = 2/3."""
    cell_dir = tmp_path / "cell_topk"
    warm_topk = [
        {"metric": {"path": "/a"}, "value": [1700000000, "100"]},
        {"metric": {"path": "/b"}, "value": [1700000000, "90"]},
        {"metric": {"path": "/x"}, "value": [1700000000, "80"]},  # wrong
    ]
    _write_replay(
        cell_dir,
        [
            {
                "ts": "2026-05-06T12:00:00Z",
                "query": "topk(3, foo)",
                "kind": "topk",
                "duration_ms": 3.0,
                "plan_id": "p",
                "result": warm_topk,
                "result_type": "vector",
                "status": "success",
            },
        ],
    )
    # Archive (ground truth): /a, /b, /c. Warm got /a + /b right but
    # missed /c (returned /x instead). recall = 2/3.
    archive_body = json.dumps(
        {
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [
                    {"metric": {"path": "/a"}, "value": [1700000000, "100"]},
                    {"metric": {"path": "/b"}, "value": [1700000000, "90"]},
                    {"metric": {"path": "/c"}, "value": [1700000000, "85"]},
                ],
            },
            "infos": ["data_source: gorilla_archive"],
        }
    ).encode("utf-8")

    out_path = tmp_path / "accuracy.csv"
    with _stub_backend(responses={"topk(3, foo)": (200, archive_body)}) as (_, base_url):
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
    assert r["kind"] == "topk"
    assert float(r["recall"]) == pytest.approx(2.0 / 3.0, rel=1e-3)
