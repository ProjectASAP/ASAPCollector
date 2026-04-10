from __future__ import annotations

"""Q6 — Distinct active metrics per window, grouped by entity.

Purpose
-------
Estimate the active-series footprint over time.

Observability systems need to monitor distinct active metrics per entity and
window to catch cardinality explosions without enumerating the full series
space online. This module computes the exact reference for the documented Q6:
per 5-minute window, count distinct ``metric_base`` values for each ``entity``.

Formula
-------
For each (entity, window):
    active_metric_count(entity, w) =
        | { metric_base | metric_base appears in window w for entity } |

This matches the SQL/SeQuAL template in ``docs/02_benchmark_queries.md``:

    SELECT
      entity,
      COUNT(DISTINCT metric_base) AS active_metric_count
    FROM exathlon_metrics
    GROUP BY entity, TUMBLE(ts, INTERVAL '5' MINUTE)

Output columns
--------------
entity, window_start_s, window_size_s, window_label, exact_active_metric_count
"""

import argparse
import sys
import time
from collections import defaultdict
from pathlib import Path

import pandas as pd

_BENCHMARK_ROOT = Path(__file__).resolve().parent.parent
if str(_BENCHMARK_ROOT) not in sys.path:
    sys.path.insert(0, str(_BENCHMARK_ROOT))

from common import file_csv_path, file_tag_safe
from ground_truth.common import (
    WINDOW_5MIN_S,
    _stream_long_chunks,
    log_phase,
)

PRIMARY_WINDOW_S = WINDOW_5MIN_S
PRIMARY_WINDOW_LABEL = "5min"

_Q6_COLS = [
    "entity",
    "window_start_s",
    "window_size_s",
    "window_label",
    "exact_active_metric_count",
]


def compute_q6_single_window(
    csv_path: Path,
    window_s: int,
    window_label: str,
    chunksize: int,
) -> pd.DataFrame:
    """Return exact distinct metric_base count per (entity, window)."""
    active_metric_bases: dict[tuple[str, int], set[str]] = defaultdict(set)

    for chunk in _stream_long_chunks(csv_path, chunksize):
        if chunk.empty:
            continue
        ts = chunk["ts_s"].to_numpy()
        entities = chunk["entity"].astype(str).to_numpy()
        metric_bases = chunk["metric_base"].astype(str).to_numpy()
        window_starts = (ts // window_s) * window_s

        for i in range(len(metric_bases)):
            active_metric_bases[(entities[i], int(window_starts[i]))].add(metric_bases[i])

    if not active_metric_bases:
        return pd.DataFrame(columns=_Q6_COLS)

    rows = [
        {
            "entity": entity,
            "window_start_s": window_start_s,
            "window_size_s": window_s,
            "window_label": window_label,
            "exact_active_metric_count": len(metric_bases),
        }
        for (entity, window_start_s), metric_bases in sorted(active_metric_bases.items())
    ]
    return pd.DataFrame(rows, columns=_Q6_COLS)


def run_q6(
    file_tag: str,
    output_dir: Path,
    chunksize: int = 200,
    window_s: int = PRIMARY_WINDOW_S,
    window_label: str = PRIMARY_WINDOW_LABEL,
) -> Path:
    """Compute Q6 ground truth for one file."""
    csv_path = file_csv_path(file_tag)
    if not csv_path.is_file():
        raise FileNotFoundError(f"Exathlon CSV not found: {csv_path}")

    tag = file_tag_safe(file_tag)
    out_dir = output_dir / "Q6"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{tag}.csv"

    log_phase(
        "Q6",
        tag,
        "start",
        file=str(csv_path),
        window=window_label,
        chunksize=chunksize,
    )

    t0 = time.perf_counter()
    df = compute_q6_single_window(csv_path, window_s, window_label, chunksize)
    df.to_csv(out_path, index=False)
    elapsed = time.perf_counter() - t0

    log_phase(
        "Q6",
        tag,
        "done",
        rows=len(df),
        windows_n=df["window_start_s"].nunique() if not df.empty else 0,
        entities_n=df["entity"].nunique() if not df.empty else 0,
        elapsed_s=f"{elapsed:.1f}",
        output=str(out_path),
    )
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute Q6 exact distinct active metric counts per entity/window.",
    )
    parser.add_argument("--file", required=True, help="Exathlon file tag, e.g. app1/1_0_10000_17")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=_BENCHMARK_ROOT / "results" / "ground_truth",
    )
    parser.add_argument("--chunksize", type=int, default=200)
    args = parser.parse_args()

    run_q6(
        file_tag=args.file,
        output_dir=args.out_dir,
        chunksize=args.chunksize,
    )


if __name__ == "__main__":
    main()
