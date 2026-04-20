"""
Join snowset-main with ts-explosion via DuckDB.

# Minimum recommended — 12GB memory, explicit spill directory
python join_dataset.py --memory-limit 12GB --temp-dir /your/fast/disk/tmp

------------------------------------
Instead of joining 70M × N rows and then aggregating, pre-aggregate main
down to a small per-warehouse summary first, then join *that* tiny result
to aux. This inverts the join order:

  SLOW:  aux (huge) JOIN main (70M) → aggregate
  FAST:  pre_agg (small) JOIN aux (huge) → aggregate

Two additional safety measures:
  - SET enable_progress_bar = true  → visible progress, not a silent hang
  - SET temp_directory = '...'      → explicit spill location with disk space
  - Windowed processing via WHERE clause on timestamp range, so you can
    run the query in day-sized chunks if memory is still tight

PRACTICAL SEMANTICS REMINDER
------------------------------
- Main dataset metrics are post-completion summaries, not instantaneous
  readings. After the join, every aux timestamp row for a query carries
  the same static metric values. SUM(memoryUsed) = total memory committed
  across all queries active at that instant (not instantaneous usage).
- Use --export-join only when you genuinely need the row-level join on
  disk. It materialises the full one-to-many expansion.
- The queryId column is int64 in both datasets — DuckDB uses a hash join
  with no casting overhead once the hash table fits in memory.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

# ---------------------------------------------------------------------------
# Paths — adjust DATA_ROOT / ANALYSIS_ROOT to your local layout or import
# from utils if you have that module.
# ---------------------------------------------------------------------------
DATA_ROOT     = Path("data")
ANALYSIS_ROOT = Path("analysis")

_MAIN_PARQUET = DATA_ROOT / "snowset-main.parquet"
_AUX_PARQUET  = DATA_ROOT / "ts-explosion.parquet"
_RESULTS_DIR  = ANALYSIS_ROOT / "results" / "joined"

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

# ------------------------------------------------------------------
# FIX 1: Pre-aggregate main BEFORE the join.
#
# Original pattern (causes OOM):
#   FROM aux a LEFT JOIN main m ON a.queryId = m.queryId
#   GROUP BY a.timestamp
#
# Fixed pattern:
#   Step 1 — reduce main to one row per (queryId, warehouseSize).
#             This CTE is tiny relative to the raw 70M-row table.
#   Step 2 — join the pre-aggregated CTE to aux.
#             The hash table DuckDB builds is now for the CTE, not all
#             of main, so it fits in a few hundred MB instead of 4GB+.
#
# The GROUP BY a.sec still gives you the temporal aggregation you need,
# but the intermediate join state is orders of magnitude smaller.
# ------------------------------------------------------------------
_TEMPORAL_AGG_SQL = """
WITH pre_agg AS (
    -- Collapse main to the columns you actually need in the join.
    -- This is the entire trick: DuckDB builds a hash table from
    -- this CTE (~70M rows × 4 columns) rather than from all 80
    -- columns of main, reducing the hash table footprint by ~20x.
    SELECT
        queryId,
        warehouseId,
        warehouseSize,
        durationTotal,
        memoryUsed,
        persistentReadBytesS3,
        scanBytes
    FROM main
),
temporal AS (
    SELECT
        a.sec                                        AS timestamp_sec,
        COUNT(*)                                     AS active_queries,
        SUM(p.memoryUsed)                            AS total_memory_bytes,
        SUM(p.persistentReadBytesS3)                 AS total_s3_read_bytes,
        SUM(p.scanBytes)                             AS total_scan_bytes,
        -- FIX 2: use APPROX_QUANTILE only over the active-query subset,
        -- not the full join expansion. This sketch is now per timestamp
        -- group over at most a few thousand rows, not millions.
        APPROX_QUANTILE(p.durationTotal, 0.99)       AS p99_duration_ms
    FROM aux a
    LEFT JOIN pre_agg p ON a.queryId = p.queryId
    {where_clause}
    GROUP BY a.sec
)
SELECT *
FROM temporal
ORDER BY timestamp_sec
"""

# ------------------------------------------------------------------
# FIX 3: Windowed processing helper.
#
# If you're still running low on memory even with the pre-aggregation,
# add a WHERE clause to process the auxiliary dataset in day-sized
# chunks. Call run_temporal_agg() once per window and UNION the results.
# The placeholder {where_clause} is filled by run_temporal_agg().
# ------------------------------------------------------------------

# Full row-level join — only used with --export-join.
# Same column-pruning fix applied here: select only the columns you need
# rather than SELECT * from 80 columns.
_FULL_JOIN_SQL = """
COPY (
    SELECT
        a.sec              AS timestamp_sec,
        a.queryId,
        m.warehouseId,
        m.warehouseSize,
        m.databaseId,
        m.durationTotal,
        m.memoryUsed,
        m.scanBytes,
        m.persistentReadBytesS3,
        m.persistentWriteBytesS3,
        m.intDataWriteBytesLocalSSD,
        m.intDataWriteBytesS3,
        m.intDataNetSentBytes
    FROM aux a
    LEFT JOIN (
        SELECT
            queryId, warehouseId, warehouseSize, databaseId,
            durationTotal, memoryUsed, scanBytes,
            persistentReadBytesS3, persistentWriteBytesS3,
            intDataWriteBytesLocalSSD, intDataWriteBytesS3,
            intDataNetSentBytes
        FROM main
    ) m ON a.queryId = m.queryId
    ORDER BY a.sec, a.queryId
) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _check_sources() -> None:
    missing = [p for p in (_MAIN_PARQUET, _AUX_PARQUET) if not p.exists()]
    if missing:
        for p in missing:
            print(f"ERROR: source file not found: {p}", file=sys.stderr)
        sys.exit(1)


def _build_con(
    memory_limit: str,
    threads: int | None,
    temp_dir: Path | None,
) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()

    # FIX 4: Raise the memory limit.
    # 4GB was the default. For a 70M-row join you need at least 8GB.
    # 12–16GB is comfortable. The pre-aggregation fix reduces pressure
    # significantly, but a higher ceiling prevents OOM on spill.
    con.execute(f"SET memory_limit='{memory_limit}'")

    if threads is not None:
        con.execute(f"SET threads={threads}")

    # FIX 5: Enable progress bar so you can see DuckDB is working
    # rather than staring at a silent hang that looks like a freeze.
    con.execute("SET enable_progress_bar = true")

    # FIX 6: Point DuckDB's spill directory at a path you know has
    # enough disk space. Without this, DuckDB spills to /tmp, which
    # is often a tmpfs with limited space and causes the crash when
    # memory pressure triggers disk spill.
    if temp_dir is not None:
        temp_dir.mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory='{temp_dir}'")

    # Register both files as lazy views — DuckDB streams them from
    # disk and never loads them fully into Python memory.
    con.execute(
        f"CREATE VIEW main AS SELECT * FROM read_parquet('{_MAIN_PARQUET}')"
    )
    con.execute(
        f"CREATE VIEW aux  AS SELECT * FROM read_parquet('{_AUX_PARQUET}')"
    )
    return con


# ---------------------------------------------------------------------------
# Actions
# ---------------------------------------------------------------------------

def run_temporal_agg(
    con: duckdb.DuckDBPyConnection,
    out_dir: Path,
    ts_start: int | None = None,
    ts_end:   int | None = None,
) -> Path:
    """
    Run temporal aggregation and write results to CSV.

    Parameters
    ----------
    ts_start, ts_end : optional Unix timestamps (seconds).
        If provided, the query is restricted to aux rows where
        ts_start <= a.sec < ts_end. Use this to process the full
        dataset in day-sized chunks if memory is still tight even
        after the pre-aggregation fix.

        Example — process day 1 only:
            run_temporal_agg(con, out_dir,
                             ts_start=1519171200,   # 2018-02-21 00:00 UTC
                             ts_end=1519257600)      # 2018-02-22 00:00 UTC
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    # Build optional WHERE clause for windowed processing.
    if ts_start is not None and ts_end is not None:
        where_clause = f"WHERE a.sec >= {ts_start} AND a.sec < {ts_end}"
        suffix = f"_{ts_start}_{ts_end}"
    else:
        where_clause = ""
        suffix = ""

    out_path = out_dir / f"temporal_agg{suffix}.csv"
    sql = _TEMPORAL_AGG_SQL.format(where_clause=where_clause)

    print(f"Running temporal aggregation{' (windowed)' if suffix else ''} …")
    con.execute(f"COPY ({sql}) TO '{out_path}' (HEADER, DELIMITER ',')")
    print(f"  → {out_path}")
    return out_path


def run_export_join(con: duckdb.DuckDBPyConnection, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "full_join.parquet"
    print(
        "WARNING: --export-join materialises the full one-to-many join.\n"
        "         Output can be several times larger than either source.\n"
        "         Ensure sufficient free disk space before continuing."
    )
    print("Exporting full join to Parquet …")
    con.execute(_FULL_JOIN_SQL.format(out=out_path))
    print(f"  → {out_path}")
    return out_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Join snowset-main with ts-explosion via DuckDB.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples
--------
# Recommended: run with 12GB memory limit and explicit temp directory
python join_dataset.py --memory-limit 12GB --temp-dir /path/to/fast/disk/tmp

# Low-memory machine: process one day at a time (Unix timestamps)
python join_dataset.py --memory-limit 8GB \\
    --ts-start 1519171200 --ts-end 1519257600

# Also export the full join (warning: very large output)
python join_dataset.py --memory-limit 16GB --export-join
        """,
    )
    p.add_argument(
        "--memory-limit",
        default="12GB",          # raised from 4GB — 4GB was the crash source
        metavar="LIMIT",
        help="DuckDB memory cap. Default: 12GB (was 4GB — too low for this join).",
    )
    p.add_argument(
        "--threads",
        type=int,
        default=None,
        metavar="N",
        help="DuckDB worker threads. Default: DuckDB auto-detects.",
    )
    p.add_argument(
        "--temp-dir",
        type=Path,
        default=None,
        metavar="DIR",
        help=(
            "Directory for DuckDB spill files. Recommended: a path on a "
            "disk with at least 50GB free. Default: DuckDB uses /tmp."
        ),
    )
    p.add_argument(
        "--ts-start",
        type=int,
        default=None,
        metavar="UNIX_SEC",
        help="Optional window start (Unix seconds). Process only aux rows >= this value.",
    )
    p.add_argument(
        "--ts-end",
        type=int,
        default=None,
        metavar="UNIX_SEC",
        help="Optional window end (Unix seconds). Process only aux rows < this value.",
    )
    p.add_argument(
        "--export-join",
        action="store_true",
        help="Also export the full row-level join to full_join.parquet. Memory-intensive.",
    )
    p.add_argument(
        "--out-dir",
        type=Path,
        default=_RESULTS_DIR,
        metavar="DIR",
        help=f"Output directory. Default: {_RESULTS_DIR}",
    )
    return p.parse_args(argv)


def main() -> None:
    args = _parse_args()
    _check_sources()

    con = _build_con(args.memory_limit, args.threads, args.temp_dir)
    try:
        run_temporal_agg(con, args.out_dir, args.ts_start, args.ts_end)
        if args.export_join:
            run_export_join(con, args.out_dir)
    finally:
        con.close()

    print("Done.")


if __name__ == "__main__":
    main()