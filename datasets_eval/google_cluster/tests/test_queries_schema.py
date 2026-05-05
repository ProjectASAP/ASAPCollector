"""Schema-shape tests for queries.json.

Asserts:
  - well-formed JSON
  - matches the schema of `deploy/scripts/queries-e2e.json`
    (every entry has the keys that file requires + has an additional
    `expected_ground_truth_query` per the prompt)
  - covers all 5 evaluation claims (quantile / topk / sum / count_unique)
  - run.py validate exits 0 on this file
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
REPO_ROOT = ROOT.parent.parent
QUERIES_PATH = ROOT / "queries.json"
REFERENCE_PATH = REPO_ROOT / "deploy" / "scripts" / "queries-e2e.json"


def _load_json(p: Path):
    return json.loads(p.read_text())


def test_queries_file_exists():
    assert QUERIES_PATH.is_file(), f"missing: {QUERIES_PATH}"


def test_queries_json_is_well_formed():
    data = _load_json(QUERIES_PATH)
    assert isinstance(data, list)
    assert len(data) > 0
    for i, q in enumerate(data):
        assert isinstance(q, dict), f"queries[{i}] must be object"
        assert "kind" in q, f"queries[{i}] missing kind"
        assert "promql" in q, f"queries[{i}] missing promql"
        assert isinstance(q["promql"], str) and q["promql"].strip()


def test_queries_match_reference_schema():
    """Every key required by deploy/scripts/queries-e2e.json must be present here."""
    if not REFERENCE_PATH.is_file():
        pytest.skip(f"reference file missing: {REFERENCE_PATH}")
    ref = _load_json(REFERENCE_PATH)
    assert isinstance(ref, list) and ref
    ref_keys = set()
    ref_kinds = set()
    for q in ref:
        if isinstance(q, dict):
            ref_keys.update(q.keys())
            if "kind" in q:
                ref_kinds.add(q["kind"])
    data = _load_json(QUERIES_PATH)
    for i, q in enumerate(data):
        for k in ref_keys:
            assert k in q, f"queries[{i}] missing reference key {k!r}"
        assert q["kind"] in ref_kinds, (
            f"queries[{i}].kind={q['kind']!r} not in reference kinds {sorted(ref_kinds)}"
        )


def test_queries_cover_all_five_claims():
    """Every kind in the reference must appear at least once in our log."""
    if not REFERENCE_PATH.is_file():
        pytest.skip(f"reference file missing: {REFERENCE_PATH}")
    ref = _load_json(REFERENCE_PATH)
    ref_kinds = {q["kind"] for q in ref if isinstance(q, dict) and "kind" in q}
    data = _load_json(QUERIES_PATH)
    our_kinds = {q["kind"] for q in data}
    missing = ref_kinds - our_kinds
    assert not missing, f"queries.json does not cover kinds {sorted(missing)}"


def test_queries_have_ground_truth_pointer():
    """Each entry should expose a ground-truth comparator for the accuracy reducer."""
    data = _load_json(QUERIES_PATH)
    for i, q in enumerate(data):
        assert "expected_ground_truth_query" in q, (
            f"queries[{i}] missing expected_ground_truth_query"
        )
        assert isinstance(q["expected_ground_truth_query"], str)
        assert q["expected_ground_truth_query"].strip()


def test_run_py_validate_passes():
    rc = subprocess.call([
        sys.executable,
        str(ROOT / "run.py"),
        "validate",
        "--queries", str(QUERIES_PATH),
    ])
    assert rc == 0, "run.py validate must exit 0 on the shipped queries.json"
