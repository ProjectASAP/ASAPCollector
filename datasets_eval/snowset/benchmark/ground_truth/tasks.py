from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

BENCH_ROOT = Path(__file__).resolve().parent.parent
if str(BENCH_ROOT) not in sys.path:
    sys.path.insert(0, str(BENCH_ROOT))

from ground_truth.common import (
    _PROF_COLS,
    derive_archetype,
    floor_to_window,
    log_phase,
    open_dataset,
)
from common import JOINED_PARQUET_PATH


def _load(columns: list[str], chunksize: int) -> pd.DataFrame:
    """Stream the full partitioned parquet dataset and return a single DataFrame."""
    dataset = open_dataset()
    parts: list[pd.DataFrame] = []
    for batch in dataset.to_batches(batch_size=chunksize, columns=columns):
        parts.append(batch.to_pandas())
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=columns)


# ── Q1: exact per-warehouse S3 request volume per 5-min window ──────────────

def _gt_q1(output_dir: Path, slice_tag: str, chunksize: int) -> None:
    log_phase("Q1", "load")
    cols = ["createdTime", "warehouseId", "warehouseSize", "persistentReadRequestsS3"]
    df = _load(cols, chunksize)
    df = df[(df["warehouseSize"] == 4) & (df["persistentReadRequestsS3"] > 0)].copy()
    df["window_start"] = floor_to_window(df["createdTime"])
    agg = (
        df.groupby(["window_start", "warehouseId"])["persistentReadRequestsS3"]
        .sum()
        .reset_index()
        .rename(columns={"persistentReadRequestsS3": "total_requests"})
    )
    out = output_dir / "Q1"
    out.mkdir(parents=True, exist_ok=True)
    agg.to_csv(out / f"{slice_tag}.csv", index=False)
    log_phase("Q1", "done", rows=len(agg))


# ── Q2: exact p50/p95/p99 of durationTotal per warehouse per 5-min window ───

def _gt_q2(output_dir: Path, slice_tag: str, chunksize: int) -> None:
    log_phase("Q2", "load")
    cols = ["createdTime", "warehouseId", "durationTotal"]
    df = _load(cols, chunksize)
    df = df[df["durationTotal"] > 0].copy()
    df["window_start"] = floor_to_window(df["createdTime"])

    rows: list[dict] = []
    for (ws, wh), grp in df.groupby(["window_start", "warehouseId"]):
        vals = grp["durationTotal"].to_numpy(dtype=np.float64)
        rows.append({
            "window_start": ws,
            "warehouseId": wh,
            "query_count": len(vals),
            "p50_ms": float(np.percentile(vals, 50)),
            "p95_ms": float(np.percentile(vals, 95)),
            "p99_ms": float(np.percentile(vals, 99)),
        })

    pd.DataFrame(rows).pipe(
        lambda d: (output_dir / "Q2").mkdir(parents=True, exist_ok=True) or d
    ).to_csv(output_dir / "Q2" / f"{slice_tag}.csv", index=False)
    log_phase("Q2", "done", rows=len(rows))


# ── Q3: exact p95/p99 of persistentReadBytesS3 per warehouse per window ─────

def _gt_q3(output_dir: Path, slice_tag: str, chunksize: int) -> None:
    log_phase("Q3", "load")
    cols = ["createdTime", "warehouseId", "persistentReadBytesS3"]
    df = _load(cols, chunksize)
    df = df[df["persistentReadBytesS3"] > 0].copy()
    df["window_start"] = floor_to_window(df["createdTime"])

    rows: list[dict] = []
    for (ws, wh), grp in df.groupby(["window_start", "warehouseId"]):
        vals = grp["persistentReadBytesS3"].to_numpy(dtype=np.float64)
        rows.append({
            "window_start": ws,
            "warehouseId": wh,
            "cache_miss_queries": len(vals),
            "p95_bytes": float(np.percentile(vals, 95)),
            "p99_bytes": float(np.percentile(vals, 99)),
        })

    (output_dir / "Q3").mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(output_dir / "Q3" / f"{slice_tag}.csv", index=False)
    log_phase("Q3", "done", rows=len(rows))


# ── Q4: exact archetype frequency per warehouseSize per 5-min window ─────────

def _gt_q4(output_dir: Path, slice_tag: str, chunksize: int) -> None:
    log_phase("Q4", "load")
    cols = ["createdTime", "warehouseId", "warehouseSize"] + _PROF_COLS
    df = _load(cols, chunksize)
    present = [c for c in _PROF_COLS if c in df.columns]
    if not present:
        log_phase("Q4", "no profiling columns found — skipping")
        return
    df = df[(df[present] > 0).any(axis=1)].copy()
    df["window_start"] = floor_to_window(df["createdTime"])
    df["query_archetype"] = derive_archetype(df)

    agg = (
        df.groupby(["window_start", "warehouseSize", "query_archetype"])
        .size()
        .reset_index(name="frequency")
    )
    (output_dir / "Q4").mkdir(parents=True, exist_ok=True)
    agg.to_csv(output_dir / "Q4" / f"{slice_tag}.csv", index=False)
    log_phase("Q4", "done", rows=len(agg))


# ── Q5: exact distinct warehouse count per 5-min window ─────────────────────

def _gt_q5(output_dir: Path, slice_tag: str, chunksize: int) -> None:
    log_phase("Q5", "load")
    cols = ["createdTime", "warehouseId"]
    df = _load(cols, chunksize)
    df["window_start"] = floor_to_window(df["createdTime"])

    result = (
        df.groupby("window_start")
        .agg(
            exact_distinct_warehouses=("warehouseId", "nunique"),
            total_queries=("warehouseId", "count"),
        )
        .reset_index()
    )
    (output_dir / "Q5").mkdir(parents=True, exist_ok=True)
    result.to_csv(output_dir / "Q5" / f"{slice_tag}.csv", index=False)
    log_phase("Q5", "done", rows=len(result))


# ── Q6: exact p50/p95/p99 of durationTotal by concurrency band per window ───

def _gt_q6(output_dir: Path, slice_tag: str, chunksize: int) -> None:
    del chunksize  # Q6 uses DuckDB over the joined parquet rather than pandas loading.
    log_phase("Q6", "load_joined", path=str(JOINED_PARQUET_PATH))
    try:
        import duckdb
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Q6 ground truth requires duckdb. Install benchmark requirements including duckdb."
        ) from exc

    if not JOINED_PARQUET_PATH.is_file():
        raise FileNotFoundError(f"Joined parquet not found: {JOINED_PARQUET_PATH}")

    sql = f"""
WITH joined AS (
    SELECT
        CASE
            WHEN typeof(timestamp_sec) LIKE 'TIMESTAMP%%'
                THEN CAST(epoch(timestamp_sec) AS BIGINT)
            ELSE CAST(timestamp_sec AS BIGINT)
        END                                         AS ts_sec,
        warehouseSize,
        durationTotal,
        COUNT(*) OVER (PARTITION BY timestamp_sec)  AS concurrent_queries
    FROM read_parquet('{JOINED_PARQUET_PATH}')
    WHERE warehouseSize = 4
      AND durationTotal > 0
),
banded AS (
    SELECT
        (ts_sec / 300) * 300                        AS window_start,
        warehouseSize,
        (concurrent_queries / 25) * 25             AS concurrency_band,
        durationTotal
    FROM joined
)
SELECT
    window_start,
    warehouseSize,
    concurrency_band,
    COUNT(*)                           AS sample_count,
    quantile_cont(durationTotal, 0.50) AS p50_ms,
    quantile_cont(durationTotal, 0.95) AS p95_ms,
    quantile_cont(durationTotal, 0.99) AS p99_ms
FROM banded
GROUP BY window_start, warehouseSize, concurrency_band
ORDER BY window_start, warehouseSize, concurrency_band
"""

    con = duckdb.connect()
    try:
        df = con.execute(sql).fetch_df()
    finally:
        con.close()

    out = output_dir / "Q6"
    out.mkdir(parents=True, exist_ok=True)
    df.to_csv(out / f"{slice_tag}.csv", index=False)
    log_phase("Q6", "done", rows=len(df))


_GT_RUNNERS = {
    "Q1": _gt_q1,
    "Q2": _gt_q2,
    "Q3": _gt_q3,
    "Q4": _gt_q4,
    "Q5": _gt_q5,
    "Q6": _gt_q6,
}


def run_ground_truth_task(
    query_id: str,
    slice_tag: str,
    output_dir: Path,
    chunksize: int,
) -> None:
    fn = _GT_RUNNERS.get(query_id)
    if fn is None:
        raise ValueError(f"No ground truth runner for query {query_id!r}")
    fn(output_dir, slice_tag, chunksize)
