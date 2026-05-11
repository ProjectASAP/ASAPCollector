"""Tests for metricsql_replay.py — the MetricsQL replay client (P5).

Contract under test:

  - load_queries() accepts the five kinds: quantile, topk, count_unique,
    sum, frequency. It rejects unknown kinds with a clear error.

  - run_query() correctly parses the three Prometheus result shapes
    that the five-sketch workload exercises (issue #46):
      * `count(...)`            → scalar / vector(1)        (HLL)
      * `topk(K, ...)`          → vector(K)                 (CountSketch)
      * `rate(...[5m])`         → vector(per-series)        (CountMinSketch)
      * `quantile_over_time`    → vector / matrix           (DDSketch / KLL)
      * `sum by (...)`          → vector / matrix           (Sum / passthrough)

We don't spin up a real backend — instead we monkey-patch
`urllib.request.urlopen` to return canned PromQL JSON envelopes and
verify the replay client maps them to its (status, result_type,
result) tuple correctly across all six query classes.
"""

from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import unittest
from unittest import mock

# Make `import metricsql_replay` resolve from the scripts dir.
SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

import metricsql_replay  # noqa: E402


# ── load_queries ─────────────────────────────────────────────────


class TestLoadQueries(unittest.TestCase):
    """load_queries accepts each of the five kinds and rejects bad input."""

    def _write(self, payload: list[dict]) -> str:
        f = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
        json.dump(payload, f)
        f.flush()
        return f.name

    def test_accepts_all_five_kinds(self) -> None:
        path = self._write([
            {"kind": "quantile", "metricsql": "quantile_over_time(0.99, http_latency_ms[1m])"},
            {"kind": "sum", "metricsql": "sum by (zone) (http_requests_total)"},
            {"kind": "count_unique", "metricsql": "count(unique_users_per_min)"},
            {"kind": "topk", "metricsql": "topk(5, top_endpoint_qps)"},
            {"kind": "frequency", "metricsql": "rate(endpoint_request_freq[5m])"},
        ])
        got = metricsql_replay.load_queries(path)
        self.assertEqual(len(got), 5)
        self.assertEqual({q["kind"] for q in got},
                         {"quantile", "sum", "count_unique", "topk", "frequency"})

    def test_rejects_unknown_kind(self) -> None:
        path = self._write([{"kind": "bogus", "metricsql": "x"}])
        with self.assertRaises(SystemExit):
            metricsql_replay.load_queries(path)

    def test_rejects_missing_field(self) -> None:
        path = self._write([{"kind": "sum"}])  # no metricsql
        with self.assertRaises(SystemExit):
            metricsql_replay.load_queries(path)

    def test_accepts_legacy_promql_field(self) -> None:
        # Back-compat: external workload JSONs may still use the
        # pre-rename `promql` key. load_queries should accept it.
        path = self._write([{"kind": "sum", "promql": "sum(http_requests_total)"}])
        got = metricsql_replay.load_queries(path)
        self.assertEqual(len(got), 1)
        self.assertEqual(got[0]["metricsql"], "sum(http_requests_total)")


# ── run_query result-shape parsing ────────────────────────────────


def _canned(body: dict, code: int = 200):
    """Return a context manager mimicking urllib.request.urlopen."""
    raw = json.dumps(body).encode("utf-8")

    class _Resp:
        def __init__(self) -> None:
            self._buf = io.BytesIO(raw)

        def read(self) -> bytes:
            return self._buf.read()

        def getcode(self) -> int:
            return code

        def __enter__(self):  # noqa: D401
            return self

        def __exit__(self, *_args) -> None:
            return None

    return _Resp()


class TestRunQueryShapes(unittest.TestCase):
    """run_query parses each MetricsQL/PromQL result-type the 5-sketch workload uses."""

    def _patch_urlopen(self, body: dict):
        return mock.patch.object(
            metricsql_replay.urllib.request,
            "urlopen",
            return_value=_canned(body),
        )

    def test_quantile_vector(self) -> None:
        body = {
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [
                    {"metric": {"zone": "z0"}, "value": [1715000000, "42.5"]},
                ],
            },
        }
        with self._patch_urlopen(body):
            _, res = metricsql_replay.run_query("http://x", "q", 1.0)
        self.assertEqual(res["status"], "success")
        self.assertEqual(res["result_type"], "vector")
        self.assertEqual(len(res["result"]), 1)

    def test_count_unique_scalar(self) -> None:
        body = {
            "status": "success",
            "data": {
                "resultType": "scalar",
                "result": [1715000000, "1234"],
            },
        }
        with self._patch_urlopen(body):
            _, res = metricsql_replay.run_query("http://x", "count(unique_users_per_min)", 1.0)
        self.assertEqual(res["result_type"], "scalar")
        self.assertEqual(res["result"], [1715000000, "1234"])

    def test_topk_vector_of_k(self) -> None:
        body = {
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [
                    {"metric": {"endpoint": f"/api/ep{i:03d}"}, "value": [1715000000, str(100 - i)]}
                    for i in range(5)
                ],
            },
        }
        with self._patch_urlopen(body):
            _, res = metricsql_replay.run_query("http://x", "topk(5, top_endpoint_qps)", 1.0)
        self.assertEqual(res["result_type"], "vector")
        self.assertEqual(len(res["result"]), 5)

    def test_frequency_rate_vector(self) -> None:
        body = {
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [
                    {"metric": {"endpoint": "/api/ep000"}, "value": [1715000000, "12.5"]},
                    {"metric": {"endpoint": "/api/ep001"}, "value": [1715000000, "8.0"]},
                ],
            },
        }
        with self._patch_urlopen(body):
            _, res = metricsql_replay.run_query(
                "http://x", "rate(endpoint_request_freq[5m])", 1.0,
            )
        self.assertEqual(res["result_type"], "vector")
        self.assertEqual(len(res["result"]), 2)

    def test_sum_by_vector(self) -> None:
        body = {
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [
                    {"metric": {"zone": "z0"}, "value": [1715000000, "9999"]},
                ],
            },
        }
        with self._patch_urlopen(body):
            _, res = metricsql_replay.run_query(
                "http://x", "sum by (zone) (http_requests_total)", 1.0,
            )
        self.assertEqual(res["result_type"], "vector")

    def test_json_error_recorded(self) -> None:
        # Simulate a non-JSON body — run_query must surface the failure
        # mode so the reducer can drop the row, not fail the whole soak.
        class _BadResp:
            def read(self) -> bytes:
                return b"<html>oops</html>"

            def getcode(self) -> int:
                return 200

            def __enter__(self):
                return self

            def __exit__(self, *_args) -> None:
                return None

        with mock.patch.object(metricsql_replay.urllib.request, "urlopen",
                               return_value=_BadResp()):
            _, res = metricsql_replay.run_query("http://x", "q", 1.0)
        self.assertEqual(res["status"], "json_error")


if __name__ == "__main__":
    unittest.main()
