#!/usr/bin/env python3
"""Fail-closed MVP evaluator for issue #46.

Consumes artifacts from run_demo.sh and emits MVP_RESULTS.json and
MVP_REPORT.md. Missing, malformed, empty, or stale artifacts are failures.
Only Python's standard library is required.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import glob
import json
import math
import os
import statistics
import sys
from collections import defaultdict
from typing import Any


def percentile(values: list[float], p: float) -> float:
    if not values:
        return math.nan
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil(p * len(ordered)) - 1))
    return ordered[index]


def load_json(path: str) -> Any:
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def load_jsonl(path: str) -> list[dict[str, Any]]:
    rows = []
    with open(path, encoding="utf-8") as handle:
        for number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(f"{path}:{number}: {error}") from error
            if not isinstance(value, dict):
                raise ValueError(f"{path}:{number}: expected object")
            rows.append(value)
    return rows


def successful_replay(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row for row in rows
        if row.get("status") == "success"
        and row.get("measurement_phase", "steady_state") == "steady_state"
        and row.get("http_code") == 200
        and isinstance(row.get("result"), list)
        and row["result"]
    ]


def latency_summary(rows: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
    grouped: dict[str, list[float]] = defaultdict(list)
    for row in successful_replay(rows):
        grouped[str(row.get("query_id", "missing"))].append(float(row["duration_ms"]))
    return {
        kind: {"n": len(values), "p50_ms": percentile(values, .50), "p95_ms": percentile(values, .95),
               "mean_ms": statistics.mean(values), "stdev_ms": statistics.stdev(values) if len(values) > 1 else 0.0,
               "min_ms": min(values), "max_ms": max(values)}
        for kind, values in grouped.items()
    }


def vector(row: dict[str, Any]) -> dict[str, float]:
    out: dict[str, float] = {}
    for sample in row.get("result") or []:
        if not isinstance(sample, dict) or not isinstance(sample.get("value"), list):
            continue
        labels = sample.get("metric") or {}
        key = json.dumps(labels, sort_keys=True, separators=(",", ":"))
        try:
            out[key] = float(sample["value"][1])
        except (IndexError, TypeError, ValueError):
            continue
    return out


def vector_timestamps(row: dict[str, Any]) -> dict[str, float]:
    out: dict[str, float] = {}
    for sample in row.get("result") or []:
        if not isinstance(sample, dict) or not isinstance(sample.get("value"), list):
            continue
        key = json.dumps(sample.get("metric") or {}, sort_keys=True, separators=(",", ":"))
        try:
            out[key] = float(sample["value"][0])
        except (IndexError, TypeError, ValueError):
            continue
    return out


def accuracy_summary(exact: list[dict[str, Any]], approx: list[dict[str, Any]], cfg: dict[str, Any]) -> dict[str, Any]:
    exact_by_query: dict[str, dict[int, dict[str, Any]]] = defaultdict(dict)
    approx_by_query: dict[str, dict[int, dict[str, Any]]] = defaultdict(dict)
    for row in successful_replay(exact):
        exact_by_query[str(row.get("query_id"))][int(row.get("logical_seq", -1))] = row
    for row in successful_replay(approx):
        approx_by_query[str(row.get("query_id"))][int(row.get("logical_seq", -1))] = row
    output: dict[str, Any] = {}
    contracts = cfg["queries"]
    for query in sorted(contracts):
        left, right = exact_by_query[query], approx_by_query[query]
        errors: list[float] = []
        missing_sequences = sorted(set(left) ^ set(right))
        shape_errors = len(missing_sequences)
        timestamp_errors = 0
        elapsed_skew_ms = []
        metric = contracts[query]["metric"]
        for sequence in sorted(set(left) & set(right)):
            exact_row, approx_row = left[sequence], right[sequence]
            elapsed_skew_ms.append(abs(float(exact_row.get("logical_elapsed_ms", math.inf)) - float(approx_row.get("logical_elapsed_ms", -math.inf))))
            ev, av = vector(exact_row), vector(approx_row)
            et, at = vector_timestamps(exact_row), vector_timestamps(approx_row)
            timestamp_errors += sum(
                key not in at or abs(at[key] - timestamp) > float(cfg.get("result_timestamp_tolerance_s", 0))
                for key, timestamp in et.items()
            )
            if metric == "topk_recall":
                # Heavy-hitter correctness is set recall, not numeric error on
                # the returned rank score. Encode error as 1-recall so it uses
                # the same predeclared maximum-error gate below.
                errors.append(len(set(ev) & set(av)) / max(len(ev), 1))
                continue
            if set(ev) != set(av):
                shape_errors += len(set(ev) ^ set(av)) or 1
                continue
            for key, expected in ev.items():
                observed = av[key]
                errors.append(abs(observed - expected) / max(abs(expected), 1e-12))
        sla = float(contracts[query]["sla"])
        within = sum((error >= sla) if metric == "topk_recall" else (error <= sla) for error in errors)
        fraction = within / len(errors) if errors else 0.0
        skew_limit = float(cfg["logical_time_max_skew_ms"])
        passed = bool(errors) and shape_errors == 0 and timestamp_errors == 0 and max(elapsed_skew_ms, default=math.inf) <= skew_limit and fraction >= float(cfg["minimum_within_sla_fraction"])
        output[query] = {
            "metric": metric, "comparisons": len(errors), "shape_errors": shape_errors,
            "timestamp_errors": timestamp_errors,
            "missing_sequences": missing_sequences, "max_logical_time_skew_ms": max(elapsed_skew_ms, default=math.inf),
            "sla": sla, "within_sla_fraction": fraction,
            "p50_relative_error": percentile(errors, .50),
            "p95_relative_error": percentile(errors, .95),
            "p99_relative_error": percentile(errors, .99),
            "max_relative_error": max(errors) if errors else math.nan,
            "passed": passed,
        }
    return output


def freshness_summary(path: str, cfg: dict[str, Any]) -> dict[str, Any]:
    grouped: dict[str, list[float]] = defaultdict(list)
    with open(path, encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row.get("delta_ms"):
                value = float(row["delta_ms"])
                if math.isfinite(value) and value >= 0:
                    key = f"{row.get('query_class')}:{row.get('transmission_mode')}" if row.get("query_class") else (row.get("tier") or "unknown")
                    grouped[key].append(value)
    output = {}
    required = cfg.get("required_query_modes") or cfg["required_tiers"]
    for tier in required:
        values = grouped.get(tier, [])
        p95, maximum = percentile(values, .95), max(values, default=math.inf)
        output[tier] = {
            "n": len(values), "p50_ms": percentile(values, .50), "p95_ms": p95,
            "p99_ms": percentile(values, .99), "max_ms": maximum,
            "passed": len(values) >= int(cfg["minimum_samples_per_tier"])
            and p95 <= float(cfg["p95_ms"]) and maximum <= float(cfg["maximum_ms"]),
        }
    return output


def resource_summary(arm_dir: str, weights: dict[str, Any]) -> dict[str, Any]:
    cpu = cpu_time_s = rss_mib = peak_rss_mib = network_bps = disk_mib = 0.0
    collector_cpu = collector_rss = collector_network_bps = 0.0
    stage_files = glob.glob(os.path.join(arm_dir, "stages-*.csv"))
    nic_files = glob.glob(os.path.join(arm_dir, "nic-*.csv"))
    storage_files = glob.glob(os.path.join(arm_dir, "storage-*.csv"))
    observed_containers: set[str] = set()
    for path in stage_files:
        with open(path, encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                if (row.get("stage") or "").lower() == "producer":
                    continue
                observed_containers.add((row.get("container") or "").removeprefix("asap-"))
                row_cpu = float(row.get("cpu_cores") or 0)
                row_cpu_time = float(row.get("cpu_time_s") or 0)
                row_rss = float(row.get("rss_mib") or 0)
                row_peak_rss = float(row.get("peak_rss_mib") or 0)
                row_net_out = float(row.get("net_out_kibps") or 0)
                row_disk = float(row.get("disk_mib") or 0)
                row_cpu = row_cpu if math.isfinite(row_cpu) else 0.0
                row_rss = row_rss if math.isfinite(row_rss) else 0.0
                row_disk = row_disk if math.isfinite(row_disk) else 0.0
                cpu += row_cpu
                cpu_time_s += row_cpu_time if math.isfinite(row_cpu_time) else 0.0
                rss_mib += row_rss
                peak_rss_mib += row_peak_rss if math.isfinite(row_peak_rss) else 0.0
                disk_mib += row_disk
                text = " ".join((row.get("stage") or "", row.get("container") or "")).lower()
                if "agent" in text or "collector" in text or "otel" in text:
                    collector_cpu += row_cpu
                    collector_rss += row_rss
                    if math.isfinite(row_net_out):
                        collector_network_bps += row_net_out * 1024
    for path in nic_files:
        values: list[float] = []
        with open(path, encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                value = float(row.get("tx_bytes_per_s") or 0)
                if math.isfinite(value):
                    values.append(value)
        if values:
            mean_tx = statistics.mean(values)
            network_bps += mean_tx
    storage_components = set()
    for path in storage_files:
        with open(path, encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                value = float(row.get("bytes") or math.nan)
                if math.isfinite(value) and value >= 0:
                    disk_mib += value / 1024 / 1024
                    storage_components.add(row.get("component"))
    def cost(c: float, r: float, n: float, d: float = 0.0) -> float:
        return (c * float(weights["cpu_core_weight"]) + r / 1024 * float(weights["rss_gib_weight"])
                + n / 1024 / 1024 * float(weights["network_mib_per_s_weight"])
                + d / 1024 * float(weights.get("storage_gib_weight", 0.0)))
    return {
        "stage_files": len(stage_files), "nic_files": len(nic_files), "storage_files": len(storage_files),
        "storage_components": sorted(x for x in storage_components if x),
        "observed_containers": sorted(observed_containers),
        "cpu_cores": cpu, "cpu_time_s": cpu_time_s, "rss_mib": rss_mib,
        "peak_rss_mib": peak_rss_mib, "network_bytes_per_s": network_bps, "disk_mib": disk_mib,
        "normalized_cost": cost(cpu, rss_mib, network_bps, disk_mib),
        "collector_cpu_cores": collector_cpu, "collector_rss_mib": collector_rss,
        "collector_network_bytes_per_s": collector_network_bps,
        "collector_normalized_cost": cost(collector_cpu, collector_rss, collector_network_bps),
    }


def check_manifest(manifest: dict[str, Any], run_dir: str, baseline: str, asap: str,
                   artifacts: list[str]) -> tuple[bool, list[str]]:
    errors = []
    if manifest.get("run_id") != os.path.basename(os.path.abspath(run_dir)):
        errors.append("manifest run_id does not match run directory")
    if not manifest.get("started_at") or not manifest.get("collector_commit") or not manifest.get("backend_commit"):
        errors.append("manifest lacks timestamp or component commit")
    for field in ("load_generator", "exact_backend", "images", "remote_image_digests", "configuration_sha256", "workload", "time_alignment"):
        if not manifest.get(field):
            errors.append(f"manifest lacks {field} provenance")
    if any(value == "missing" for value in (manifest.get("images") or {}).values()):
        errors.append("manifest contains an unresolved image digest")
    arms = manifest.get("arms") or {}
    for arm in (baseline, asap):
        if arm not in arms:
            errors.append(f"manifest lacks arm {arm}")
    if arms.get(baseline, {}).get("seed") != arms.get(asap, {}).get("seed"):
        errors.append("paired arms do not use the same deterministic seed")
    try:
        started = dt.datetime.fromisoformat(str(manifest["started_at"]).replace("Z", "+00:00")).timestamp()
        for relative in artifacts:
            if os.path.getmtime(os.path.join(run_dir, relative)) + 1 < started:
                errors.append(f"artifact predates this run: {relative}")
    except (KeyError, TypeError, ValueError):
        errors.append("manifest started_at is not a valid timestamp")
    return not errors, errors


def control_plane_evidence(arm_dir: str, expected_agents: list[str]) -> dict[str, bool]:
    """Require connected agents, applied acknowledgements, and desired decisions."""
    value = load_json(os.path.join(arm_dir, "controller-agents.json"))
    connected = isinstance(value, dict) and all(value.get(agent) == "agent" for agent in expected_agents)
    with open(os.path.join(arm_dir, "controller-config.yaml"), encoding="utf-8") as handle:
        config_text = handle.read().lower()
    configs_equal = True
    for agent in expected_agents:
        path = os.path.join(arm_dir, f"controller-config-{agent}.yaml")
        if not os.path.isfile(path):
            configs_equal = False
            continue
        with open(path, encoding="utf-8") as handle:
            configs_equal = configs_equal and handle.read().lower() == config_text
    with open(os.path.join(arm_dir, "controller.log"), encoding="utf-8") as handle:
        log_text = handle.read().lower()
    applied = all(any("agent reported remote-config status" in line and f"agent={agent}" in line and "status=applied" in line for line in log_text.splitlines()) for agent in expected_agents)
    full_only = any(token in config_text for token in
                    ("kllprocessor", "kll/", "sumprocessor", "sum/"))
    return {
        "connected": connected, "applied": applied,
        "configs_equal": configs_equal,
        "delta": "delta_transmission: true" in config_text,
        "full": full_only or "delta_transmission: false" in config_text,
        "raw_passthrough": "raw_passthrough" in config_text,
    }


def unsupported_query_evidence(path: str) -> bool:
    evidence = load_json(path)
    response = evidence.get("response") or {}
    if int(evidence.get("http_code") or 0) >= 400 or response.get("status") == "error":
        return True
    fallback = response.get("fallback_used")
    infos = " ".join(str(item).lower() for item in response.get("infos") or [])
    return bool(fallback) or any(name in infos for name in ("prometheus", "victoria", "thanos", "gorilla_archive"))


def evaluate(run_dir: str, config: dict[str, Any]) -> dict[str, Any]:
    baseline, asap = config["baseline_arm"], config["asap_arm"]
    failures: list[str] = []
    required = ["run-manifest.json", f"{baseline}/replay.jsonl", f"{asap}/replay.jsonl",
                f"{asap}/freshness-{asap}.csv", f"{asap}/controller-agents.json",
                f"{asap}/controller-config.yaml", f"{asap}/controller-config-agent-a.yaml", f"{asap}/controller-config-agent-b.yaml",
                f"{asap}/controller.log", f"{asap}/unsupported-query.json", "image-digests.json"]
    missing = [path for path in required if not os.path.isfile(os.path.join(run_dir, path))]
    if missing:
        return {"schema_version": 1, "overall_verdict": "FAIL", "failures": [f"missing artifact: {p}" for p in missing]}
    if not glob.glob(os.path.join(run_dir, asap, "logs", "*.log")):
        missing.append(f"{asap}/logs/*.log")
    if missing:
        return {"schema_version": 1, "overall_verdict": "FAIL", "failures": [f"missing artifact: {p}" for p in missing]}
    manifest = load_json(os.path.join(run_dir, "run-manifest.json"))
    manifest_ok, manifest_errors = check_manifest(manifest, run_dir, baseline, asap, required[1:])
    failures.extend(manifest_errors)
    exact = load_jsonl(os.path.join(run_dir, baseline, "replay.jsonl"))
    approximate = load_jsonl(os.path.join(run_dir, asap, "replay.jsonl"))
    exact_success, asap_success = successful_replay(exact), successful_replay(approximate)
    control = control_plane_evidence(os.path.join(run_dir, asap), config["expected_agents"])
    applied = all(control.values())
    unsupported_safe = unsupported_query_evidence(os.path.join(run_dir, asap, "unsupported-query.json"))
    if not unsupported_safe:
        failures.append("unsupported query was neither rejected nor explicitly routed to exact fallback")
    if not control["applied"]:
        failures.append("control plane has no Applied/LIVE remote-config acknowledgement")
    if not control["delta"] or not control["full"] or not control["raw_passthrough"]:
        failures.append("effective config does not prove raw, full-sketch, and delta-sketch decisions")
    minimum = int(config["minimum_successful_queries_per_query"])
    query_ids = sorted(config["queries"])
    correctness = {}
    for query_id in query_ids:
        exact_n = sum(row.get("query_id") == query_id for row in exact_success)
        asap_rows = [row for row in asap_success if row.get("query_id") == query_id]
        asap_n = len(asap_rows)
        planned = all(row.get("plan_id") for row in asap_rows)
        passed = exact_n >= minimum and asap_n >= minimum and planned and applied and unsupported_safe
        correctness[query_id] = {"baseline_success": exact_n, "asap_success": asap_n, "plan_observed": planned,
                             "control_plane_evidence": control, "passed": passed}
        if not passed: failures.append(f"functional correctness failed for {query_id}")
    accuracy_cfg = dict(config["accuracy"], queries=config["queries"], logical_time_max_skew_ms=config["logical_time_max_skew_ms"])
    accuracy = accuracy_summary(exact, approximate, accuracy_cfg)
    if not accuracy or not all(item["passed"] for item in accuracy.values()): failures.append("accuracy SLA failed")
    freshness = freshness_summary(os.path.join(run_dir, asap, f"freshness-{asap}.csv"), config["freshness"])
    if not freshness or not all(item["passed"] for item in freshness.values()): failures.append("freshness SLA failed")
    latency = {baseline: latency_summary(exact), asap: latency_summary(approximate)}
    latency_passed = True
    speedups: dict[str, Any] = {}
    for query_id in query_ids:
        left, right = latency[baseline].get(query_id), latency[asap].get(query_id)
        minimum_repetitions = int(config["query_latency"].get("minimum_steady_state_repetitions", minimum))
        if left and right:
            speedups[query_id] = {
                "p50": left["p50_ms"] / max(right["p50_ms"], 1e-12),
                "p95": left["p95_ms"] / max(right["p95_ms"], 1e-12),
            }
        if not left or not right or left["n"] < minimum_repetitions or right["n"] < minimum_repetitions or (config["query_latency"]["require_asap_lower_p50"] and right["p50_ms"] >= left["p50_ms"]) or (config["query_latency"]["require_asap_lower_p95"] and right["p95_ms"] >= left["p95_ms"]):
            latency_passed = False
    if not latency_passed: failures.append("query latency gate failed")
    resources = {baseline: resource_summary(os.path.join(run_dir, baseline), config["cost"]), asap: resource_summary(os.path.join(run_dir, asap), config["cost"])}
    expected_storage = {baseline: set(config["cost"]["baseline_storage_components"]), asap: set(config["cost"]["asap_storage_components"])}
    required_processes = {
        baseline: set(config["cost"].get("baseline_required_processes", [])),
        asap: set(config["cost"].get("asap_required_processes", [])),
    }
    resource_complete = all(
        item["stage_files"] and item["nic_files"] and item["storage_files"]
        and expected_storage[arm] <= set(item["storage_components"])
        and required_processes[arm] <= set(item["observed_containers"])
        and item["cpu_time_s"] > 0 and item["peak_rss_mib"] > 0
        for arm, item in resources.items()
    )
    guardrail = float(config["cost"]["collector_regression_guardrail_ratio"])
    collector_ratios = {
        key: resources[asap][key] / max(resources[baseline][key], 1e-12)
        for key in ("collector_cpu_cores", "collector_rss_mib", "collector_network_bytes_per_s")
    }
    collector_cost_passed = (resource_complete
        and resources[asap]["collector_normalized_cost"] < resources[baseline]["collector_normalized_cost"]
        and all(ratio <= guardrail for ratio in collector_ratios.values())) if config["cost"]["require_collector_total_lower"] else resource_complete
    e2e_cost_passed = (resource_complete and resources[asap]["normalized_cost"] < resources[baseline]["normalized_cost"]) if config["cost"]["require_end_to_end_total_lower"] else resource_complete
    if not collector_cost_passed: failures.append("collector cost gate failed")
    if not e2e_cost_passed: failures.append("end-to-end cost gate failed")
    return {
        "schema_version": 1, "run_id": manifest.get("run_id"), "overall_verdict": "PASS" if not failures else "FAIL",
        "failures": failures, "manifest": {"passed": manifest_ok}, "run_manifest": manifest,
        "functional_correctness": correctness,
        "accuracy": accuracy, "freshness": freshness, "query_latency": {"passed": latency_passed, "arms": latency, "speedups": speedups},
        "collector_cost": {"passed": collector_cost_passed, "resource_ratios": collector_ratios, "guardrail": guardrail},
        "end_to_end_cost": {"passed": e2e_cost_passed}, "resources": resources,
    }


def markdown(result: dict[str, Any]) -> str:
    lines = ["# MVP demo report", "", f"Overall verdict: **{result['overall_verdict']}**", ""]
    if result.get("failures"):
        lines.extend(["## Failures", ""] + [f"- {failure}" for failure in result["failures"]] + [""])
    def verdict(key: str) -> str:
        value = result.get(key, {})
        passed = value.get("passed") if isinstance(value, dict) and "passed" in value else bool(value) and all(v.get("passed", False) for v in value.values())
        return "PASS" if passed else "FAIL"
    baseline = result.get("resources", {}).get("b1", {})
    asap = result.get("resources", {}).get("asap-gzip", {})
    accuracy = result.get("accuracy", {})
    worst_error = max((v.get("max_relative_error", math.inf) for v in accuracy.values()), default=math.inf)
    freshness_p95 = max((v.get("p95_ms", math.inf) for v in result.get("freshness", {}).values()), default=math.inf)
    latency = result.get("query_latency", {}).get("arms", {})
    def latency_cell(arm: str) -> str:
        return "; ".join(f"{q}: {v['p50_ms']:.2f}/{v['p95_ms']:.2f} ms" for q, v in sorted(latency.get(arm, {}).items())) or "missing"
    functional = result.get("functional_correctness", {})
    functional_n = sum(v.get("passed", False) for v in functional.values())
    lines.extend([
        "## Run provenance", "", "```json", json.dumps(result.get("run_manifest", {}), indent=2, sort_keys=True), "```", "",
        "## Acceptance summary", "",
        "| Category | Metric | ASAP | Exact baseline | Required result | Verdict |",
        "|---|---|---:|---:|---|---|",
        f"| Functional correctness | passed scenarios / total | {functional_n}/{len(functional)} | {functional_n}/{len(functional)} | all | {verdict('functional_correctness')} |",
        f"| Query accuracy | worst error | {worst_error:.6g} | ground truth | per-query SLA | {verdict('accuracy')} |",
        f"| Query freshness | worst p95 lag | {freshness_p95:.2f} ms | n/a | within SLA | {verdict('freshness')} |",
        f"| Query performance | per-query p50/p95 | {latency_cell('asap-gzip')} | {latency_cell('b1')} | ASAP lower | {verdict('query_latency')} |",
        f"| Collector cost | normalized cost | {asap.get('collector_normalized_cost', math.nan):.6g} | {baseline.get('collector_normalized_cost', math.nan):.6g} | ASAP lower | {verdict('collector_cost')} |",
        f"| End-to-end cost | normalized cost | {asap.get('normalized_cost', math.nan):.6g} | {baseline.get('normalized_cost', math.nan):.6g} | ASAP lower | {verdict('end_to_end_cost')} |",
        "", "Artifacts: [machine-readable results](MVP_RESULTS.json), [acceptance configuration](acceptance.json), [query workload](queries-e2e.json).", "",
    ])
    return "\n".join(lines)


def json_safe(value: Any) -> Any:
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {key: json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_safe(item) for item in value]
    return value


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    try:
        result = evaluate(args.run_dir, load_json(args.config))
    except Exception as error:
        result = {"schema_version": 1, "overall_verdict": "FAIL", "failures": [f"evaluator error: {error}"]}
    with open(os.path.join(args.run_dir, "MVP_RESULTS.json"), "w", encoding="utf-8") as handle:
        json.dump(json_safe(result), handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
    with open(os.path.join(args.run_dir, "MVP_REPORT.md"), "w", encoding="utf-8") as handle:
        handle.write(markdown(result))
    return 0 if result["overall_verdict"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
