from __future__ import annotations

from pathlib import Path

EXATHLON_ROOT = Path(__file__).resolve().parent.parent
METRIC_NAME = "system.telemetry"
RAW_DATA_ROOT = EXATHLON_ROOT / "exathlon" / "data" / "raw"

# Aggregation suffixes present in exathlon metric column names.
KNOWN_AGGREGATIONS: frozenset[str] = frozenset({
    "count", "max", "mean", "min",
    "p50", "p75", "p95", "p98", "p99", "p999",
    "stddev", "unknown", "value",
})

# Sentinel value used in exathlon CSVs to represent missing / not-yet-active data.
SENTINEL_VALUE: float = -1.0

# Representative baseline files used when no --files argument is given.
# Format: "appN/STEM" where STEM is the CSV filename without extension.
DEFAULT_FILES = (
    "app1/1_0_10000_17",
    "app5/5_0_50000_38",
    "app9/9_0_100000_1",
)


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def file_csv_path(file_tag: str) -> Path:
    """Return absolute CSV path for a file tag like 'app1/1_0_10000_17'."""
    tag = file_tag.strip()
    if tag.endswith(".csv"):
        return RAW_DATA_ROOT / tag
    return RAW_DATA_ROOT / f"{tag}.csv"


def file_tag_safe(file_tag: str) -> str:
    """Return a filesystem-safe version of file_tag ('app1/1_0_10000_17' → 'app1_1_0_10000_17')."""
    return file_tag.strip().replace("/", "_").replace("\\", "_")


# ---------------------------------------------------------------------------
# Column-name parsing
# ---------------------------------------------------------------------------

def parse_metric_column(col: str) -> tuple[str, str, str] | None:
    """Parse '{entity}_{metric_base}_{aggregation}' from a metric column name.

    Returns (entity, metric_base, aggregation) or None if the column cannot be
    parsed as a metric column.

    Entity prefixes are numeric strings ("1".."10") or "driver" (the Spark
    driver node).  Any column whose first underscore-delimited token is empty,
    or that has no underscore at all, is treated as a non-metric column.

    Examples
    --------
    >>> parse_metric_column("1_CodeGenerator_compilationTime_count")
    ('1', 'CodeGenerator_compilationTime', 'count')
    >>> parse_metric_column("driver_BlockManager_disk_diskSpaceUsed_MB_value")
    ('driver', 'BlockManager_disk_diskSpaceUsed_MB', 'value')
    >>> parse_metric_column("t")
    None
    """
    first_sep = col.find("_")
    if first_sep <= 0:
        return None
    entity = col[:first_sep]
    rest = col[first_sep + 1:]
    if not rest:
        return None
    last_sep = rest.rfind("_")
    if last_sep < 0:
        return entity, rest, "unknown"
    suffix = rest[last_sep + 1:]
    if suffix not in KNOWN_AGGREGATIONS:
        return entity, rest, "unknown"
    metric_base = rest[:last_sep]
    return entity, metric_base, suffix


def build_column_metadata(columns: list[str]) -> dict[str, tuple[str, str, str]]:
    """Return {col_name: (entity, metric_base, aggregation)} for all metric columns."""
    meta: dict[str, tuple[str, str, str]] = {}
    for col in columns:
        if col == "t":
            continue
        result = parse_metric_column(col)
        if result is not None:
            meta[col] = result
    return meta
