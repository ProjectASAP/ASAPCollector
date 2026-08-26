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
        and row.get("http_code") == 200
        and isinstance(row.get("result"), list)
        and row["result"]
    ]


def latency_summary(rows: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
    grouped: dict[str, list[float]] = defaultdict(list)
    for row in successful_replay(rows):
        grouped[str(row.get("kind", "unknown"))].append(float(row["duration_ms"]))
    return {
        kind: {"n": len(values), "p50_ms": percentile(values, .50), "p95_ms": percentile(values, .95)}
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


def accuracy_summary(exact: list[dict[str, Any]], approx: list[dict[str, Any]], cfg: dict[str, Any]) -> dict[str, Any]:
    exact_by_query: dict[str, list[dict[str, Any]]] = defaultdict(list)
    approx_by_query: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in successful_replay(exact):
        exact_by_query[str(row.get("query"))].append(row)
    for row in successful_replay(approx):
        approx_by_query[str(row.get("query"))].append(row)
    output: dict[str, Any] = {}
    for query in sorted(set(exact_by_query) | set(approx_by_query)):
        left, right = exact_by_query[query], approx_by_query[query]
        errors: list[float] = []
        shape_errors = abs(len(left) - len(right))
        kind = str((left or right)[0].get("kind", "unknown"))
        for exact_row, approx_row in zip(left, right):
            ev, av = vector(exact_row), vector(approx_row)
            if kind == "topk":
                # Heavy-hitter correctness is set recall, not numeric error on
                # the returned rank score. Encode error as 1-recall so it uses
                # the same predeclared maximum-error gate below.
                errors.append(1.0 - len(set(ev) & set(av)) / max(len(ev), 1))
                continue
            if set(ev) != set(av):
                shape_errors += len(set(ev) ^ set(av)) or 1
                continue
            for key, expected in ev.items():
                observed = av[key]
                errors.append(abs(observed - expected) / max(abs(expected), 1e-12))
        sla = float(cfg["relative_error_by_kind"].get(kind, 0.0))
        within = sum(error <= sla for error in errors)
        fraction = within / len(errors) if errors else 0.0
        passed = bool(errors) and shape_errors == 0 and fraction >= float(cfg["minimum_within_sla_fraction"])
        output[query] = {
            "kind": kind, "comparisons": len(errors), "shape_errors": shape_errors,
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
                grouped[row.get("tier") or "unknown"].append(float(row["delta_ms"]))
    output = {}
    for tier, values in grouped.items():
        p95, maximum = percentile(values, .95), max(values)
        output[tier] = {
            "n": len(values), "p50_ms": percentile(values, .50), "p95_ms": p95,
            "p99_ms": percentile(values, .99), "max_ms": maximum,
            "passed": len(values) >= int(cfg["minimum_samples_per_tier"])
            and p95 <= float(cfg["p95_ms"]) and maximum <= float(cfg["maximum_ms"]),
        }
    return output


def resource_summary(arm_dir: str, weights: dict[str, Any]) -> dict[str, Any]:
    cpu = rss_mib = network_bps = disk_mib = 0.0
    collector_cpu = collector_rss = 0.0
    stage_files = glob.glob(os.path.join(arm_dir, "stages-*.csv"))
    edge_files = glob.glob(os.path.join(arm_dir, "edge-*.csv"))
    for path in stage_files:
        with open(path, encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                if (row.get("stage") or "").lower() == "producer":
                    continue
                row_cpu = float(row.get("cpu_cores") or 0)
                row_rss = float(row.get("rss_mib") or 0)
                row_disk = float(row.get("disk_mib") or 0)
                row_cpu = row_cpu if math.isfinite(row_cpu) else 0.0
                row_rss = row_rss if math.isfinite(row_rss) else 0.0
                row_disk = row_disk if math.isfinite(row_disk) else 0.0
                cpu += row_cpu
                rss_mib += row_rss
                disk_mib += row_disk
                text = " ".join((row.get("stage") or "", row.get("container") or "")).lower()
                if "agent" in text or "collector" in text or "otel" in text:
                    collector_cpu += row_cpu
                    collector_rss += row_rss
    for path in edge_files:
        per_edge: dict[str, list[float]] = defaultdict(list)
        with open(path, encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                value = float(row.get("bytes_per_s") or 0)
                if math.isfinite(value):
                    per_edge[row.get("edge") or "unknown"].append(value)
        network_bps += sum(statistics.mean(values) for values in per_edge.values() if values)
    def cost(c: float, r: float, n: float, d: float = 0.0) -> float:
        return (c * float(weights["cpu_core_weight"]) + r / 1024 * float(weights["rss_gib_weight"])
                + n / 1024 / 1024 * float(weights["network_mib_per_s_weight"])
                + d / 1024 * float(weights.get("storage_gib_weight", 0.0)))
    return {
        "stage_files": len(stage_files), "edge_files": len(edge_files),
        "cpu_cores": cpu, "rss_mib": rss_mib, "network_bytes_per_s": network_bps, "disk_mib": disk_mib,
        "normalized_cost": cost(cpu, rss_mib, network_bps, disk_mib),
        "collector_cpu_cores": collector_cpu, "collector_rss_mib": collector_rss,
        "collector_normalized_cost": cost(collector_cpu, collector_rss, network_bps),
    }


def check_manifest(manifest: dict[str, Any], run_dir: str, baseline: str, asap: str,
                   artifacts: list[str]) -> tuple[bool, list[str]]:
    errors = []
    if manifest.get("run_id") != os.path.basename(os.path.abspath(run_dir)):
        errors.append("manifest run_id does not match run directory")
    if not manifest.get("started_at") or not manifest.get("collector_commit") or not manifest.get("backend_commit"):
        errors.append("manifest lacks timestamp or component commit")
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


def control_plane_evidence(path: str) -> dict[str, bool]:
    """Require applied status plus full/delta decisions in effective config."""
    value = load_json(path)
    text = json.dumps(value, sort_keys=True).lower()
    return {
        "applied": any(marker in text for marker in ('"applied"', 'status=applied', '"live"')),
        "delta": "delta_transmission" in text and ("true" in text or "use_delta" in text),
        "full": "delta_transmission" in text and ("false" in text or "use_full_sketch" in text),
    }


def evaluate(run_dir: str, config: dict[str, Any]) -> dict[str, Any]:
    baseline, asap = config["baseline_arm"], config["asap_arm"]
    failures: list[str] = []
    required = ["run-manifest.json", f"{baseline}/replay.jsonl", f"{asap}/replay.jsonl",
                f"{asap}/freshness-{asap}.csv", f"{asap}/controller-agents.json"]
    missing = [path for path in required if not os.path.isfile(os.path.join(run_dir, path))]
    if missing:
        return {"schema_version": 1, "overall_verdict": "FAIL", "failures": [f"missing artifact: {p}" for p in missing]}
    manifest = load_json(os.path.join(run_dir, "run-manifest.json"))
    manifest_ok, manifest_errors = check_manifest(manifest, run_dir, baseline, asap, required[1:])
    failures.extend(manifest_errors)
    exact = load_jsonl(os.path.join(run_dir, baseline, "replay.jsonl"))
    approximate = load_jsonl(os.path.join(run_dir, asap, "replay.jsonl"))
    exact_success, asap_success = successful_replay(exact), successful_replay(approximate)
    control = control_plane_evidence(os.path.join(run_dir, asap, "controller-agents.json"))
    applied = all(control.values())
    if not control["applied"]:
        failures.append("control plane has no Applied/LIVE remote-config acknowledgement")
    if not control["delta"] or not control["full"]:
        failures.append("effective config does not prove both full- and delta-sketch decisions")
    minimum = int(config["minimum_successful_queries_per_kind"])
    kinds = sorted({str(row.get("kind")) for row in exact + approximate})
    correctness = {}
    for kind in kinds:
        exact_n = sum(row.get("kind") == kind for row in exact_success)
        asap_rows = [row for row in asap_success if row.get("kind") == kind]
        asap_n = len(asap_rows)
        planned = all(row.get("plan_id") for row in asap_rows)
        passed = exact_n >= minimum and asap_n >= minimum and planned and applied
        correctness[kind] = {"baseline_success": exact_n, "asap_success": asap_n, "plan_observed": planned,
                             "control_plane_evidence": control, "passed": passed}
        if not passed: failures.append(f"functional correctness failed for {kind}")
    accuracy = accuracy_summary(exact, approximate, config["accuracy"])
    if not accuracy or not all(item["passed"] for item in accuracy.values()): failures.append("accuracy SLA failed")
    freshness = freshness_summary(os.path.join(run_dir, asap, f"freshness-{asap}.csv"), config["freshness"])
    if not freshness or not all(item["passed"] for item in freshness.values()): failures.append("freshness SLA failed")
    latency = {baseline: latency_summary(exact), asap: latency_summary(approximate)}
    latency_passed = True
    for kind in kinds:
        left, right = latency[baseline].get(kind), latency[asap].get(kind)
        if not left or not right or right["p50_ms"] >= left["p50_ms"] or right["p95_ms"] >= left["p95_ms"]:
            latency_passed = False
    if not latency_passed: failures.append("query latency gate failed")
    resources = {baseline: resource_summary(os.path.join(run_dir, baseline), config["cost"]), asap: resource_summary(os.path.join(run_dir, asap), config["cost"])}
    resource_complete = all(item["stage_files"] and item["edge_files"] for item in resources.values())
    guardrail = float(config["cost"]["collector_regression_guardrail_ratio"])
    collector_ratios = {
        key: resources[asap][key] / max(resources[baseline][key], 1e-12)
        for key in ("collector_cpu_cores", "collector_rss_mib", "network_bytes_per_s")
    }
    collector_cost_passed = (resource_complete
        and resources[asap]["collector_normalized_cost"] < resources[baseline]["collector_normalized_cost"]
        and all(ratio <= guardrail for ratio in collector_ratios.values()))
    e2e_cost_passed = resource_complete and resources[asap]["normalized_cost"] < resources[baseline]["normalized_cost"]
    if not collector_cost_passed: failures.append("collector cost gate failed")
    if not e2e_cost_passed: failures.append("end-to-end cost gate failed")
    return {
        "schema_version": 1, "run_id": manifest.get("run_id"), "overall_verdict": "PASS" if not failures else "FAIL",
        "failures": failures, "manifest": {"passed": manifest_ok}, "functional_correctness": correctness,
        "accuracy": accuracy, "freshness": freshness, "query_latency": {"passed": latency_passed, "arms": latency},
        "collector_cost": {"passed": collector_cost_passed, "resource_ratios": collector_ratios, "guardrail": guardrail},
        "end_to_end_cost": {"passed": e2e_cost_passed}, "resources": resources,
    }


def markdown(result: dict[str, Any]) -> str:
    lines = ["# MVP demo report", "", f"Overall verdict: **{result['overall_verdict']}**", ""]
    if result.get("failures"):
        lines.extend(["## Failures", ""] + [f"- {failure}" for failure in result["failures"]] + [""])
    lines.extend(["## Acceptance summary", "", "| Category | Verdict |", "|---|---|"])
    for key, label in (("manifest", "Run provenance"), ("functional_correctness", "Functional correctness"), ("accuracy", "Query accuracy"), ("freshness", "Query freshness"), ("query_latency", "Query performance"), ("collector_cost", "Collector cost"), ("end_to_end_cost", "End-to-end cost")):
        value = result.get(key, {})
        passed = value.get("passed") if isinstance(value, dict) and "passed" in value else bool(value) and all(v.get("passed", False) for v in value.values())
        lines.append(f"| {label} | {'PASS' if passed else 'FAIL'} |")
    lines.append("")
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
