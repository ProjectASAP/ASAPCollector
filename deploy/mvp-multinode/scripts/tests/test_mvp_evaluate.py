from __future__ import annotations

import csv
import importlib.util
import json
import pathlib
import tempfile
import unittest


SCRIPT = pathlib.Path(__file__).parents[1] / "mvp_evaluate.py"
SPEC = importlib.util.spec_from_file_location("mvp_evaluate", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
SPEC.loader.exec_module(MODULE)


class EvaluatorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.run = pathlib.Path(self.tmp.name) / "run-1"
        self.run.mkdir()
        self.config = {
            "baseline_arm": "b1", "asap_arm": "asap-gzip", "minimum_successful_queries_per_query": 2,
            "logical_time_max_skew_ms": 100, "expected_agents": ["agent-a", "agent-b"],
            "queries": {"sum-one": {"metric": "relative_error", "sla": 0.05}},
            "accuracy": {"minimum_within_sla_fraction": 1.0},
            "freshness": {"p95_ms": 1000, "maximum_ms": 2000, "minimum_samples_per_tier": 2,
                          "required_tiers": ["warm", "archive"]},
            "query_latency": {"require_asap_lower_p50": True, "require_asap_lower_p95": True},
            "cost": {"cpu_core_weight": 1.0, "rss_gib_weight": 0.1, "network_mib_per_s_weight": 0.01,
                     "storage_gib_weight": 0.01,
                     "baseline_storage_components": ["victoriametrics"],
                     "asap_storage_components": ["minio", "gorilla-merger", "sketch-persistence"],
                     "collector_regression_guardrail_ratio": 2.0, "require_collector_total_lower": True,
                     "require_end_to_end_total_lower": True},
        }

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def write_fixture(self, *, plan=True, error=0.01) -> None:
        manifest = {"run_id": "run-1", "started_at": "2026-08-26T00:00:00Z", "collector_commit": "abc",
                    "backend_commit": "def", "arms": {"b1": {"seed": 42}, "asap-gzip": {"seed": 42}}}
        (self.run / "run-manifest.json").write_text(json.dumps(manifest))
        for arm, duration, value in (("b1", 20.0, 100.0), ("asap-gzip", 5.0, 100.0 * (1 + error))):
            arm_dir = self.run / arm
            arm_dir.mkdir()
            records = [{"query": "sum(x)", "query_id": "sum-one", "logical_seq": i,
                        "logical_elapsed_ms": i * 10, "kind": "sum", "duration_ms": duration + i, "status": "success",
                        "http_code": 200, "plan_id": "p1" if arm == "asap-gzip" and plan else None,
                        "result": [{"metric": {"zone": "a"}, "value": [1, str(value)]}]} for i in range(3)]
            (arm_dir / "replay.jsonl").write_text("".join(json.dumps(row) + "\n" for row in records))
            with (arm_dir / "stages-node0.csv").open("w", newline="") as handle:
                writer = csv.writer(handle); writer.writerow(["baseline", "stage", "container", "cpu_cores", "rss_mib"])
                writer.writerow([arm, "agent", "asap-otel", 1 if arm == "b1" else .5, 100])
                writer.writerow([arm, "backend", "backend", 1 if arm == "b1" else .2, 100])
            with (arm_dir / "nic-node0.csv").open("w", newline="") as handle:
                writer = csv.writer(handle); writer.writerow(["host", "tx_bytes_per_s"])
                writer.writerow(["node0", 1000000 if arm == "b1" else 100000])
            components = ["victoriametrics"] if arm == "b1" else ["minio", "gorilla-merger", "sketch-persistence"]
            with (arm_dir / "storage-node1.csv").open("w", newline="") as handle:
                writer = csv.writer(handle); writer.writerow(["arm", "node", "component", "bytes"])
                for component in components: writer.writerow([arm, "node1", component, 1000])
        with (self.run / "asap-gzip" / "freshness-asap-gzip.csv").open("w", newline="") as handle:
            writer = csv.writer(handle); writer.writerow(["arm", "probe", "tier", "poll_idx", "poll_ts_ms", "observed_value_ms", "delta_ms"])
            writer.writerow(["asap-gzip", "probe", "warm", 1, 100, 90, 10]); writer.writerow(["asap-gzip", "probe", "warm", 2, 200, 180, 20])
            writer.writerow(["asap-gzip", "probe", "archive", 1, 100, 90, 10]); writer.writerow(["asap-gzip", "probe", "archive", 2, 200, 180, 20])
        (self.run / "asap-gzip" / "controller-agents.json").write_text(json.dumps({"agent-a": "agent", "agent-b": "agent"}))
        (self.run / "asap-gzip" / "controller-config.yaml").write_text("delta_transmission: true\ndelta_transmission: false\n")
        (self.run / "asap-gzip" / "controller.log").write_text("agent reported remote-config status agent=agent-a status=Applied\nagent reported remote-config status agent=agent-b status=Applied\n")

    def test_complete_run_passes(self) -> None:
        self.write_fixture()
        result = MODULE.evaluate(str(self.run), self.config)
        self.assertEqual("PASS", result["overall_verdict"])

    def test_missing_artifact_fails_closed(self) -> None:
        self.write_fixture()
        (self.run / "asap-gzip" / "freshness-asap-gzip.csv").unlink()
        result = MODULE.evaluate(str(self.run), self.config)
        self.assertEqual("FAIL", result["overall_verdict"])
        self.assertIn("missing artifact", result["failures"][0])

    def test_missing_plan_evidence_fails(self) -> None:
        self.write_fixture(plan=False)
        result = MODULE.evaluate(str(self.run), self.config)
        self.assertEqual("FAIL", result["overall_verdict"])
        self.assertIn("functional correctness", " ".join(result["failures"]))

    def test_missing_applied_full_delta_evidence_fails(self) -> None:
        self.write_fixture()
        (self.run / "asap-gzip" / "controller.log").write_text("status=Applying\n")
        result = MODULE.evaluate(str(self.run), self.config)
        self.assertEqual("FAIL", result["overall_verdict"])
        self.assertIn("no Applied", " ".join(result["failures"]))

    def test_missing_archive_freshness_fails(self) -> None:
        self.write_fixture()
        path = self.run / "asap-gzip" / "freshness-asap-gzip.csv"
        path.write_text("arm,probe,tier,poll_idx,poll_ts_ms,observed_value_ms,delta_ms\nasap-gzip,p,warm,1,100,90,10\nasap-gzip,p,warm,2,200,180,20\n")
        self.assertEqual("FAIL", MODULE.evaluate(str(self.run), self.config)["overall_verdict"])

    def test_missing_storage_component_fails(self) -> None:
        self.write_fixture()
        path = self.run / "asap-gzip" / "storage-node1.csv"
        path.write_text("arm,node,component,bytes\nasap-gzip,node1,minio,1000\n")
        self.assertEqual("FAIL", MODULE.evaluate(str(self.run), self.config)["overall_verdict"])

    def test_logical_time_skew_fails(self) -> None:
        self.write_fixture()
        path = self.run / "asap-gzip" / "replay.jsonl"
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        rows[0]["logical_elapsed_ms"] = 1000
        path.write_text("".join(json.dumps(row) + "\n" for row in rows))
        self.assertEqual("FAIL", MODULE.evaluate(str(self.run), self.config)["overall_verdict"])

    def test_artifact_older_than_manifest_fails(self) -> None:
        self.write_fixture()
        manifest_path = self.run / "run-manifest.json"
        manifest = json.loads(manifest_path.read_text())
        manifest["started_at"] = "2999-01-01T00:00:00+00:00"
        manifest_path.write_text(json.dumps(manifest))
        result = MODULE.evaluate(str(self.run), self.config)
        self.assertEqual("FAIL", result["overall_verdict"])
        self.assertIn("artifact predates this run", " ".join(result["failures"]))

    def test_accuracy_outside_sla_fails(self) -> None:
        self.write_fixture(error=.2)
        result = MODULE.evaluate(str(self.run), self.config)
        self.assertEqual("FAIL", result["overall_verdict"])
        self.assertIn("accuracy SLA failed", result["failures"])


if __name__ == "__main__":
    unittest.main()
