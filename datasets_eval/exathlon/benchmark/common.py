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


# ---------------------------------------------------------------------------
# Q1 KLL benchmark — stream definitions and accuracy constants  # Q1_KLL
# ---------------------------------------------------------------------------

Q1_KLL_FILE_TAG: str = "app2/2_5_1000000_87"  # Q1_KLL  — source trace for KLL benchmark streams

Q1_KLL_STREAMS: list[dict] = [  # Q1_KLL
    {
        "column":    "2_executor_jvmGCTime_count",  # Q1_KLL
        "unit":      "ms",  # Q1_KLL
        "input":     "increment",  # Q1_KLL  — feed tick-to-tick deltas, not raw accumulator totals
        "n_valid":   33106,  # Q1_KLL  — valid increment ticks after dropping sentinel boundaries
        "exact_p50": 0.0,    # Q1_KLL  — 62 % of ticks are zero (GC idle)
        "exact_p95": 72.0,   # Q1_KLL
        "exact_p99": 290.0,  # Q1_KLL
        "difficulty": "medium",  # Q1_KLL
        "note":      "jvmGCTime accumulator increments; right-skewed, 336 distinct values 0–936 ms",  # Q1_KLL
    },
    {
        "column":    "2_jvm_PS-MarkSweep_time_value",  # Q1_KLL
        "unit":      "ms",  # Q1_KLL
        "input":     "increment",  # Q1_KLL
        "n_valid":   33106,  # Q1_KLL
        "exact_p50": 0.0,    # Q1_KLL  — 99.1 % of ticks are zero (rare full-GC pauses)
        "exact_p95": 0.0,    # Q1_KLL
        "exact_p99": 161.0,  # Q1_KLL
        "difficulty": "hard",   # Q1_KLL  — sparse rare-event stream; tests KLL on near-zero distributions
        "note":      "PS MarkSweep GC increments; 381 nonzero ticks out of 33k, rare-event case",  # Q1_KLL
    },
    {
        "column":    "2_executor_cpuTime_count",  # Q1_KLL
        "unit":      "ns",  # Q1_KLL
        "input":     "increment",  # Q1_KLL
        "n_valid":   33106,  # Q1_KLL
        "exact_p50": 1086883165.5,    # Q1_KLL  — ~1.09 s/tick median CPU time
        "exact_p95": 1973395615.25,   # Q1_KLL
        "exact_p99": 2130124824.3,    # Q1_KLL
        "difficulty": "hard",  # Q1_KLL  — 24 612 distinct values; continuous high-cardinality distribution
        "note":      "cpuTime accumulator increments; 24 612 distinct values, range 0–3.78 s/tick",  # Q1_KLL
    },
]
"""Three GC / CPU increment streams from Exathlon app2/2_5_1000000_87 used as KLL benchmark inputs."""  # Q1_KLL

Q1_KLL_ERROR_TARGET: float = 1 / 400  # Q1_KLL  — rank error threshold = 1/k
Q1_KLL_REPEAT_N: int = 10_000        # Q1_KLL  — column repeat count for throughput measurement
Q1_KLL_SENTINEL: float = -1.0        # Q1_KLL  — skip rows with this value before inserting
Q1_KLL_SKETCH_K: int = 400           # Q1_KLL  — KLL sketch parameter K
Q1_KLL_MEMORY_LIMIT_B: int = 6_000   # Q1_KLL  — max sketch memory per stream in bytes (~4–6 KB at k=400)
Q1_KLL_RANK_GRID_STEP: float = 0.001 # Q1_KLL  — quantile grid resolution for rank-error computation (1001 pts)


def q1_kll_report_value(stream: dict, value: float) -> float:  # Q1_KLL
    """Convert a raw sketch query value to the reporting unit.

    cpuTime (unit='ns') is stored in nanoseconds but reported in seconds;
    all other streams (unit='ms') pass through unchanged.

    Parameters
    ----------
    stream:
        One entry from :data:`Q1_KLL_STREAMS` (must contain key ``'unit'``).
    value:
        Raw value returned by the sketch (or exact sort) for this stream.

    Returns
    -------
    float
        Value in the stream's *reporting* unit.
    """  # Q1_KLL
    if stream["unit"] == "ns":  # Q1_KLL
        return value / 1e9  # Q1_KLL  — nanoseconds → seconds
    return value  # Q1_KLL  — ms and all other units pass through
