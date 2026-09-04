from __future__ import annotations

import importlib.util
import pathlib
import unittest


SCRIPT = pathlib.Path(__file__).parents[1] / "measure_collector_telemetry.py"
SPEC = importlib.util.spec_from_file_location("measure_collector_telemetry", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
SPEC.loader.exec_module(MODULE)


class CollectorTelemetryTest(unittest.TestCase):
    def test_prometheus_parser_sums_label_series(self) -> None:
        values = MODULE.parse_prometheus(
            "# TYPE otelcol_receiver_accepted_metric_points counter\n"
            'otelcol_receiver_accepted_metric_points_total{receiver="otlp",transport="grpc"} 10\n'
            'otelcol_receiver_accepted_metric_points_total{receiver="otlp",transport="http"} 2\n'
            "otelcol_receiver_accepted_metric_points_created 1700000000\n"
            'otelcol_receiver_refused_metric_points_total{receiver="otlp"} 0\n'
        )
        total, names = MODULE.counter_total(values, "receiver_accepted_metric_points")
        self.assertEqual(12, total)
        self.assertEqual(["otelcol_receiver_accepted_metric_points_total"], names)

    def test_non_finite_samples_are_ignored(self) -> None:
        values = MODULE.parse_prometheus("metric_a NaN\nmetric_b +Inf\nmetric_c 3\n")
        self.assertEqual({"metric_c": 3}, values)


if __name__ == "__main__":
    unittest.main()
