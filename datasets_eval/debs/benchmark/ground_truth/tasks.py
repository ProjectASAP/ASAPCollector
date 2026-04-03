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
    elif query_id == "Q2":
        from ground_truth.q2 import run_q2_only

        run_q2_only(day, output_dir, chunksize, max_event_minutes=max_event_minutes)
    elif query_id == "Q1Q2":
        from ground_truth.q2 import run_q1q2

        run_q1q2(day, output_dir, chunksize, max_event_minutes=max_event_minutes)
    elif query_id == "Q3":
        from ground_truth.q3 import run_q3

        run_q3(day, output_dir, chunksize)
    elif query_id == "Q4":
        from ground_truth.q4 import run_q4

        run_q4(day, output_dir, chunksize)
    elif query_id == "Q5":
        from ground_truth.q5 import run_q5

        run_q5(day, output_dir, chunksize)
    elif query_id == "Q6":
        from ground_truth.q6 import run_q6

        run_q6(day, output_dir, chunksize)
    else:
        raise ValueError(f"No ground truth runner for query {query_id!r} in this checkout.")
