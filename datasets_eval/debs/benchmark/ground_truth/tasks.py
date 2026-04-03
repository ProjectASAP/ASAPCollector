from __future__ import annotations

import sys
from pathlib import Path

_BENCHMARK_ROOT = Path(__file__).resolve().parent.parent
if str(_BENCHMARK_ROOT) not in sys.path:
    sys.path.insert(0, str(_BENCHMARK_ROOT))


def run_ground_truth_task(
    query_id: str,
    day: str,
    output_dir: Path,
    chunksize: int,
    max_event_minutes: int | None = None,
) -> None:
    if query_id == "Q1":
        from ground_truth.q1 import run_q1_only

        run_q1_only(day, output_dir, chunksize, max_event_minutes=max_event_minutes)
    else:
        raise ValueError(f"No ground truth runner for query {query_id!r} in this checkout.")
