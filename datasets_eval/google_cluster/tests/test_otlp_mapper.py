"""Unit tests for otlp_mapper.py against golden fixtures.

Each golden fixture is a 5-row CSV (the `task_usage.csv` /
`instance_usage.csv` layout the fetcher writes) paired with its
expected JSONL output. The test asserts byte-stable equivalence.
"""

from __future__ import annotations

import json
import shutil
import sys
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = Path(__file__).resolve().parent / "fixtures"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import otlp_mapper  # noqa: E402  (path adjusted above)


def _stage_dataset(year: str, src_csv: Path) -> tuple[Path, Path]:
    """Copy a fixture into a temp <in-dir>/<year>/<table>.csv layout.

    Returns (in_dir, out_jsonl). Caller must clean up the temp tree.
    """
    tmp = Path(tempfile.mkdtemp(prefix="gct-test-"))
    year_dir = tmp / year
    year_dir.mkdir()
    table_filename = "task_usage.csv" if year == "2011" else "instance_usage.csv"
    shutil.copy(src_csv, year_dir / table_filename)
    out_jsonl = tmp / "out.jsonl"
    return tmp, out_jsonl


def _read_jsonl(path: Path) -> list[dict]:
    out = []
    with open(path, "r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


@pytest.mark.parametrize(
    "year,src,golden,cap",
    [
        ("2011", "2011_task_usage.csv", "2011_expected_no_cap.jsonl", 0),
        ("2019", "2019_instance_usage.csv", "2019_expected_no_cap.jsonl", 0),
        ("2019", "2019_instance_usage.csv", "2019_expected_cap8.jsonl", 8),
    ],
)
def test_mapper_matches_golden(year, src, golden, cap):
    in_dir, out = _stage_dataset(year, FIXTURES / src)
    try:
        rc = otlp_mapper.main([
            "--year", year,
            "--in-dir", str(in_dir),
            "--out", str(out),
            "--cardinality-cap", str(cap),
        ])
        assert rc == 0
        actual = out.read_text()
        expected = (FIXTURES / golden).read_text()
        assert actual == expected, (
            f"mapper output drifted from golden {golden}.\n"
            f"--- expected (first 200 chars) ---\n{expected[:200]}\n"
            f"--- actual (first 200 chars) ---\n{actual[:200]}"
        )
    finally:
        shutil.rmtree(in_dir, ignore_errors=True)


def test_mapper_emits_two_metric_families_per_row():
    in_dir, out = _stage_dataset("2011", FIXTURES / "2011_task_usage.csv")
    try:
        rc = otlp_mapper.main([
            "--year", "2011",
            "--in-dir", str(in_dir),
            "--out", str(out),
            "--cardinality-cap", "0",
        ])
        assert rc == 0
        rows = _read_jsonl(out)
        # 5 input rows × 2 metric families (cpu_rate + memory_usage) = 10
        assert len(rows) == 10
        metrics = {r["metric"] for r in rows}
        assert metrics == {
            "google_cluster_2011_cpu_rate",
            "google_cluster_2011_memory_usage",
        }
    finally:
        shutil.rmtree(in_dir, ignore_errors=True)


def test_mapper_attribute_keys_match_fake_exporter_schema():
    """Validate the OTLP attribute keys match the schema documented in the README.

    The keys must be {zone, rack, host, service, task} so the same
    PromQL queries that work against fake-exporter's synthetic
    workload also work against the Google trace.
    """
    in_dir, out = _stage_dataset("2019", FIXTURES / "2019_instance_usage.csv")
    try:
        otlp_mapper.main([
            "--year", "2019",
            "--in-dir", str(in_dir),
            "--out", str(out),
            "--cardinality-cap", "0",
        ])
        rows = _read_jsonl(out)
        for r in rows:
            assert set(r["attributes"].keys()) == {"zone", "rack", "host", "service", "task"}
    finally:
        shutil.rmtree(in_dir, ignore_errors=True)


def test_mapper_is_deterministic_byte_stable():
    """Same input + same flags must produce byte-identical output across runs."""
    in_dir, out_a = _stage_dataset("2019", FIXTURES / "2019_instance_usage.csv")
    out_b = in_dir / "out_b.jsonl"
    try:
        for o in (out_a, out_b):
            otlp_mapper.main([
                "--year", "2019",
                "--in-dir", str(in_dir),
                "--out", str(o),
                "--cardinality-cap", "1000",
            ])
        assert out_a.read_bytes() == out_b.read_bytes()
    finally:
        shutil.rmtree(in_dir, ignore_errors=True)


def test_cardinality_cap_saturates():
    """With cap=2 and 3 unique services, distinct service-cells must be <= 2."""
    in_dir, out = _stage_dataset("2019", FIXTURES / "2019_instance_usage.csv")
    try:
        otlp_mapper.main([
            "--year", "2019",
            "--in-dir", str(in_dir),
            "--out", str(out),
            "--cardinality-cap", "2",
        ])
        rows = _read_jsonl(out)
        # Distinct (service, task) cells must collapse to <= 2.
        cells = {(r["attributes"]["service"], r["attributes"]["task"]) for r in rows}
        assert len(cells) <= 2
    finally:
        shutil.rmtree(in_dir, ignore_errors=True)


def test_cardinality_cap_zero_passes_identity():
    """cap=0 must NOT collapse identities; (service, task) cardinality
    equals number of distinct (collection_id, instance_index) pairs."""
    in_dir, out = _stage_dataset("2019", FIXTURES / "2019_instance_usage.csv")
    try:
        otlp_mapper.main([
            "--year", "2019",
            "--in-dir", str(in_dir),
            "--out", str(out),
            "--cardinality-cap", "0",
        ])
        rows = _read_jsonl(out)
        cells = {(r["attributes"]["service"], r["attributes"]["task"]) for r in rows}
        # Fixture has 4 distinct (collection_id, instance_index) pairs.
        assert len(cells) == 4
    finally:
        shutil.rmtree(in_dir, ignore_errors=True)


def test_mapper_rejects_bad_year():
    with pytest.raises(ValueError):
        list(otlp_mapper.map_rows(
            FIXTURES / "2019_instance_usage.csv",
            year="9999",
            cardinality_cap=0,
            zone_vals=4,
            rack_vals=10,
        ))


def test_mapper_rejects_header_mismatch(tmp_path):
    bad = tmp_path / "bad.csv"
    bad.write_text("not,a,real,header\n1,2,3,4\n")
    with pytest.raises(RuntimeError, match="header mismatch"):
        list(otlp_mapper.map_rows(
            bad, year="2019", cardinality_cap=0, zone_vals=4, rack_vals=10,
        ))
