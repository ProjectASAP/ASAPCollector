"""Unit tests for measure_freshness.py (v6 mode).

The v6 mode is a poll-only client. The fake-exporter (probes.go) is
the producer. Tests below cover the small parts of the script that
have non-trivial behavior:

  * `query_once_v6` — JSON-decode + result extraction, including
    empty / error / malformed responses.
  * `run_v6` — the delta-ms math, against a stub HTTP endpoint that
    returns a known cumulative value.
  * `write_v6_summary` — count + p50 + p99 line shape.

We do NOT spin up an actual Prometheus or fake-exporter; tests are
hermetic. urllib.urlopen is monkeypatched per-test.
"""

from __future__ import annotations

import csv
import importlib.util
import io
import json
import os
import sys
import tempfile
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

# Load measure_freshness.py without requiring an installed package.
HERE = Path(__file__).resolve().parent
SCRIPT = HERE.parent / "measure_freshness.py"
spec = importlib.util.spec_from_file_location("measure_freshness", SCRIPT)
assert spec is not None and spec.loader is not None
measure_freshness = importlib.util.module_from_spec(spec)
sys.modules["measure_freshness"] = measure_freshness
spec.loader.exec_module(measure_freshness)


# -- query_once_v6 -------------------------------------------------


class _FakeResponse:
    def __init__(self, body: bytes):
        self._body = body

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _patch_urlopen(monkeypatch, body: bytes | None, raise_exc: Exception | None = None):
    def fake_urlopen(req, timeout=None):
        if raise_exc is not None:
            raise raise_exc
        return _FakeResponse(body or b"")

    monkeypatch.setattr(measure_freshness.urllib.request, "urlopen", fake_urlopen)


def _vector_response(value: float, ts: float = 0.0) -> bytes:
    """Build a Prometheus-shape success vector response with one
    series. The metric labels don't matter — measure_freshness picks
    the first series and only reads value[1]."""
    payload = {
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [
                {"metric": {}, "value": [ts, str(value)]}
            ],
        },
    }
    return json.dumps(payload).encode("utf-8")


def test_query_once_extracts_observed_value(monkeypatch):
    _patch_urlopen(monkeypatch, _vector_response(1714974050123.0))
    ts_ms, observed = measure_freshness.query_once_v6(
        "http://stub", "last_over_time(p[10s])", 1.0
    )
    assert observed == 1714974050123.0
    assert ts_ms > 0


def test_query_once_empty_result(monkeypatch):
    body = json.dumps(
        {"status": "success", "data": {"resultType": "vector", "result": []}}
    ).encode("utf-8")
    _patch_urlopen(monkeypatch, body)
    ts_ms, observed = measure_freshness.query_once_v6("http://stub", "p", 1.0)
    assert observed is None
    assert ts_ms > 0


def test_query_once_http_error(monkeypatch):
    _patch_urlopen(monkeypatch, None, raise_exc=OSError("boom"))
    ts_ms, observed = measure_freshness.query_once_v6("http://stub", "p", 1.0)
    assert observed is None
    assert ts_ms > 0


def test_query_once_bad_json(monkeypatch):
    _patch_urlopen(monkeypatch, b"not-json{{")
    ts_ms, observed = measure_freshness.query_once_v6("http://stub", "p", 1.0)
    assert observed is None


def test_query_once_non_success(monkeypatch):
    body = json.dumps({"status": "error", "error": "metric not found"}).encode()
    _patch_urlopen(monkeypatch, body)
    _, observed = measure_freshness.query_once_v6("http://stub", "p", 1.0)
    assert observed is None


# -- end-to-end v6 against an in-process HTTP stub ------------------


class _StubHandler(BaseHTTPRequestHandler):
    """Serves /api/v1/query with a fixed observed value.

    The value is read off the server instance so each test can stamp
    its own. We reply success with a single-vector body shaped exactly
    like Prometheus's response so measure_freshness's parser is
    exercised end-to-end.
    """

    def do_GET(self):  # noqa: N802 — BaseHTTPRequestHandler API
        observed = self.server.observed_value  # type: ignore[attr-defined]
        body = _vector_response(observed)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):  # silence test output
        return


@contextmanager
def _stub_server(observed_value: float):
    """Spin up a single-threaded HTTP server on a free port that
    answers any GET with the given observed value. Yields the base
    URL."""
    server = HTTPServer(("127.0.0.1", 0), _StubHandler)
    server.observed_value = observed_value  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        yield f"http://{host}:{port}"
    finally:
        server.shutdown()
        thread.join(timeout=2)


def test_run_v6_writes_csv_with_correct_delta(tmp_path):
    """End-to-end: a stub Prom returns observed=now-50ms; the script
    should compute delta_ms ≈ 50 and write rows in the v6 schema."""
    import time as _time

    # Pin observed to "50ms ago" so delta is well-defined.
    observed = int(_time.time() * 1000) - 50

    out_path = tmp_path / "raw.csv"
    with _stub_server(float(observed)) as base_url:
        rc = measure_freshness.main(
            [
                "--query-endpoint", base_url,
                "--probe", "http_freshness_probe_raw",
                "--path-label", "raw",
                "--duration", "0.4",
                "--poll-interval-ms", "50",
                "--output", str(out_path),
            ]
        )
    assert rc == 0
    assert out_path.exists()

    rows = list(csv.DictReader(out_path.open()))
    # 0.4s / 50ms = 8 ticks; allow some scheduler jitter.
    assert len(rows) >= 3, f"expected ≥3 rows, got {len(rows)}: {rows}"

    for r in rows:
        assert r["path"] == "raw"
        sample_ts = int(r["sample_ts_ms"])
        observed_ts = int(r["observed_ts_ms"])
        delta = int(r["delta_ms"])
        # Sanity: sample_ts should be a real wall-clock value.
        assert sample_ts > 1_700_000_000_000
        # observed_ts is what the stub returned, which we pinned.
        assert observed_ts == observed
        # delta = sample_ts - observed_ts. Stub is fixed, so this
        # grows monotonically as the loop runs. Lower bound is
        # ~50ms (the offset we baked in). Upper bound is loose for
        # slow CI.
        assert delta >= 40, f"delta {delta} too small"
        assert delta < 5000, f"delta {delta} unreasonable"


def test_run_v6_handles_empty_results(tmp_path):
    """If the endpoint always returns empty, the CSV should have
    header-only output and the script should exit 0 (probe just
    hasn't reached the query layer yet — keep polling)."""

    class EmptyHandler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            body = json.dumps(
                {"status": "success", "data": {"resultType": "vector", "result": []}}
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a, **kw):
            return

    server = HTTPServer(("127.0.0.1", 0), EmptyHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        out_path = tmp_path / "warm.csv"
        rc = measure_freshness.main(
            [
                "--query-endpoint", f"http://{host}:{port}",
                "--probe", "http_freshness_probe_warm",
                "--path-label", "warm",
                "--duration", "0.2",
                "--poll-interval-ms", "50",
                "--output", str(out_path),
            ]
        )
    finally:
        server.shutdown()
        thread.join(timeout=2)

    assert rc == 0
    rows = list(csv.reader(out_path.open()))
    assert rows == [["path", "sample_ts_ms", "observed_ts_ms", "delta_ms"]]


# -- mode selection -------------------------------------------------


def test_v6_requires_full_quad():
    """v6 mode rejects partial flag sets."""
    parser = measure_freshness.build_parser()
    args = parser.parse_args(
        [
            "--query-endpoint", "http://x",
            "--probe", "http_freshness_probe_raw",
            # --path-label, --output missing
        ]
    )
    with pytest.raises(SystemExit):
        measure_freshness.select_mode(args)


def test_mixed_modes_rejected():
    parser = measure_freshness.build_parser()
    args = parser.parse_args(
        [
            "--query-endpoint", "http://x",
            "--probe", "http_freshness_probe_raw",
            "--path-label", "raw",
            "--output", "/tmp/x",
            "--baseline", "b0",
        ]
    )
    with pytest.raises(SystemExit):
        measure_freshness.select_mode(args)


def test_no_flags_rejected():
    parser = measure_freshness.build_parser()
    args = parser.parse_args([])
    with pytest.raises(SystemExit):
        measure_freshness.select_mode(args)


def test_v4_quad_selects_v4():
    parser = measure_freshness.build_parser()
    args = parser.parse_args(
        [
            "--baseline", "b0",
            "--otlp-http", "http://otlp",
            "--query", "http://prom",
            "--out", "/tmp/freshness.csv",
        ]
    )
    assert measure_freshness.select_mode(args) == "v4"


def test_probe_choices_enforced():
    """argparse enforces --probe is one of the three canonical names."""
    parser = measure_freshness.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["--probe", "http_freshness_probe_bogus"])


# -- summary --------------------------------------------------------


def test_summary_no_samples(capsys):
    measure_freshness.write_v6_summary([], "raw", count_attempted=10)
    err = capsys.readouterr().err
    assert "freshness:" in err
    assert "got=0" in err
    assert "p50=NA" in err


def test_summary_single_sample(capsys):
    measure_freshness.write_v6_summary([42.0], "warm", count_attempted=5)
    err = capsys.readouterr().err
    assert "got=1" in err
    assert "p50=42.0ms" in err
    # p99 falls back to the single sample value.
    assert "p99=42.0ms" in err


def test_summary_many_samples(capsys):
    deltas = [float(x) for x in range(1, 101)]  # 1..100
    measure_freshness.write_v6_summary(deltas, "archive", count_attempted=120)
    err = capsys.readouterr().err
    assert "got=100" in err
    # Median of 1..100 is 50.5 (statistics.median).
    assert "p50=50.5ms" in err
    # p99 (inclusive) of 1..100 is 99.01 — we just check the
    # leading two digits to avoid binding to statistics.quantiles
    # implementation detail.
    assert "p99=99" in err
