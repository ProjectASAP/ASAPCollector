from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

BENCH_ROOT = Path(__file__).resolve().parent
REPO_ROOT   = Path(__file__).resolve().parent.parent.parent.parent

SNOWSET_ROOT = BENCH_ROOT.parent

# Partitioned parquet directory (one row per completed query, 69 M rows, 93 columns).
# Use pyarrow.dataset to stream it; do NOT pass as a single-file path.
PARQUET_PATH = SNOWSET_ROOT / "data" / "snowset-main.parquet"
JOINED_PARQUET_PATH = SNOWSET_ROOT / "analysis" / "results" / "joined" / "full_join.parquet"

PATCH_CMD = REPO_ROOT / "opentelemetry-collector-contrib-patch" / "cmd"

DEFAULT_COLLECTOR_PATHS = {
    "ddsketch":       PATCH_CMD / "ddsketchcol" / "ddsketchcol",
    "kll":            PATCH_CMD / "kll" / "KLL",
    "hll":            PATCH_CMD / "hllcol" / "HLL",
    "countsketch":    PATCH_CMD / "countsketchcol" / "dist" / "countsketchcol",
    "countminsketch": PATCH_CMD / "countminsketchcol" / "dist" / "countminsketchcol",
    "nop":            PATCH_CMD / "nopcol" / "dist" / "nopcol",
}

PROMETHEUS_METRICS_URL = os.environ.get("PROMETHEUS_METRICS_URL", "http://localhost:8889/metrics")
DEFAULT_ACCURACY_SLA = 0.5
DEFAULT_SKETCH_ACCURACY_SLA = 0.05
QUERY_SKETCH_ACCURACY_SLA = {
    # DDSketch's guarantee is parameterized by relative accuracy alpha, not by
    # the benchmark pass/fail threshold. Q3 needs the planned 1% DDSketch alpha.
    "Q3": 0.01,
    # KLL and CMS: 5% target accuracy → controller configures sketch accordingly.
    "Q1": 0.05,
    "Q2": 0.05,
    "Q4": 0.05,
    "Q5": 0.05,
    "Q6": 0.05,
}

DEFAULT_SLICES = ("full",)


@dataclass(frozen=True)
class QueryCfg:
    """Per-query benchmark configuration."""

    metric_name: str
    aggregations: tuple[str, ...]
    time_window: str
    sketch_family: str        # "quantile" | "frequency" | "cardinality" | "nop"
    group_by: tuple[str, ...]
    value_column: str         # parquet column used as gauge value (or synthetic name)
    filter_expr: str | None   # pandas .query() string, or None


# Q1 uses persistentReadRequestsS3 (present in snowset-main) as the CMS frequency weight.
# Q4 uses profHjRso / profSortRso / profAggRso / profScanRso / profFilterRso
#   (all present in snowset-main) to derive query archetypes.
QUERY_CONFIG: dict[str, QueryCfg] = {
    "Q1": QueryCfg(
        metric_name="snowset.warehouse.s3_read_requests",
        aggregations=("frequency",),
        time_window="300s",
        sketch_family="frequency",
        group_by=("warehouseId",),
        value_column="persistentReadRequestsS3",
        filter_expr="warehouseSize == 4 and persistentReadRequestsS3 > 0",
    ),
    "Q2": QueryCfg(
        metric_name="snowset.query.duration_ms",
        aggregations=("quantile",),
        time_window="300s",
        sketch_family="quantile",
        group_by=("warehouseId",),
        value_column="durationTotal",
        filter_expr="durationTotal > 0",
    ),
    "Q3": QueryCfg(
        metric_name="snowset.warehouse.s3_read_bytes_nz",
        aggregations=("quantile",),
        time_window="300s",
        sketch_family="quantile",
        group_by=("warehouseId",),
        value_column="persistentReadBytesS3",
        filter_expr="persistentReadBytesS3 > 0",
    ),
    "Q4": QueryCfg(
        metric_name="snowset.query.archetype_count",
        aggregations=("frequency",),
        time_window="300s",
        sketch_family="frequency",
        group_by=("query_archetype", "warehouseSize"),
        value_column="archetype_value",   # synthetic column set to 1.0 in replay
        filter_expr=None,                 # archetype derivation handles filtering internally
    ),
    "Q5": QueryCfg(
        metric_name="snowset.warehouse.active_id",
        aggregations=("cardinality",),
        time_window="300s",
        sketch_family="cardinality",
        group_by=(),
        value_column="warehouseId",
        filter_expr=None,
    ),
    "Q6": QueryCfg(
        metric_name="snowset.query.duration_ms_by_concurrency_band",
        aggregations=("quantile",),
        time_window="300s",
        sketch_family="quantile",
        group_by=("warehouseSize", "concurrency_band"),
        value_column="durationTotal",
        filter_expr="warehouseSize == 4 and durationTotal > 0",
    ),
}

NOP_QUERIES: frozenset[str] = frozenset()
DEFAULT_QUERIES = tuple(QUERY_CONFIG)

BENCH_LATENCY_SLA_FOR_BATCH_MODE = "30s"


def _env_path(key: str, default: Path) -> Path:
    v = os.environ.get(key)
    return Path(v).expanduser() if v else default


def resolve_collector_bin(query: str, sketch: str, collector_override: str | None) -> Path:
    if collector_override:
        return Path(collector_override).expanduser()
    family = QUERY_CONFIG[query].sketch_family
    if family == "nop":
        return _env_path("COLLECTOR_NOP", DEFAULT_COLLECTOR_PATHS["nop"])
    if family == "cardinality":
        return _env_path("COLLECTOR_HLL", DEFAULT_COLLECTOR_PATHS["hll"])
    if family == "frequency":
        if sketch == "countminsketch":
            return _env_path("COLLECTOR_COUNTMINSKETCH", DEFAULT_COLLECTOR_PATHS["countminsketch"])
        return _env_path("COLLECTOR_COUNTSKETCH", DEFAULT_COLLECTOR_PATHS["countsketch"])
    if family == "quantile":
        if sketch == "kll":
            return _env_path("COLLECTOR_KLL", DEFAULT_COLLECTOR_PATHS["kll"])
        return _env_path("COLLECTOR_DDSKETCH", DEFAULT_COLLECTOR_PATHS["ddsketch"])
    return _env_path("COLLECTOR_DDSKETCH", DEFAULT_COLLECTOR_PATHS["ddsketch"])


def plan_aggregations(query: str) -> list[str]:
    return list(QUERY_CONFIG[query].aggregations)


def time_window_for_query(query: str) -> str:
    return QUERY_CONFIG[query].time_window


def sketch_for_matrix_cell(
    query: str, sketch_quantile: str, sketch_freq: str, sketch_card: str,
) -> str:
    family = QUERY_CONFIG[query].sketch_family
    if family == "frequency":
        return sketch_freq
    if family == "cardinality":
        return sketch_card
    if family == "nop":
        return "nop"
    return sketch_quantile


def sketch_type_for_plan(query: str, sketch: str | None) -> str | None:
    s = (sketch or "").strip().lower()
    family = QUERY_CONFIG[query].sketch_family
    if family == "frequency":
        return "countminsketch" if s == "countminsketch" else "countsketch"
    if family == "cardinality":
        return "hll"
    if family == "quantile":
        return "kll" if s == "kll" else "ddsketch"
    return None


def build_plan_body(
    metric: str,
    query: str,
    sketch: str | None,
    latency_sla: str | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "metric_name": metric,
        "aggregations": plan_aggregations(query),
        "time_window": time_window_for_query(query),
        "latency_sla": latency_sla or BENCH_LATENCY_SLA_FOR_BATCH_MODE,
        "group_by_labels": list(QUERY_CONFIG[query].group_by),
        "accuracy_sla": QUERY_SKETCH_ACCURACY_SLA.get(query, DEFAULT_SKETCH_ACCURACY_SLA),
        "workload": {
            "series_count": 2051,
            "samples_per_sec_per_series": 10,
            "bytes_per_raw_sample": 64,
            "data_distribution": "zipf",
        },
    }
    st = sketch_type_for_plan(query, sketch)
    if st is not None:
        body["sketch_type"] = st
    return body


def controller_ready(url: str) -> bool:
    try:
        r = requests.get(f"{url.rstrip('/')}/api/v1/agents", timeout=2)
        return r.status_code == 200
    except requests.RequestException:
        return False


def parse_controller_api_port(controller: str) -> int:
    parsed = urlparse(controller)
    if parsed.port is not None:
        return parsed.port
    return 443 if parsed.scheme == "https" else 8080


def nop_config_path(collector_bin: Path) -> Path:
    parent = collector_bin.parent
    cand = parent.parent / "config-bench.yaml"
    if cand.is_file():
        return cand
    return parent / "config-bench.yaml"
