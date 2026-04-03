from __future__ import annotations

import argparse
import os
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

_BENCHMARK_ROOT = Path(__file__).resolve().parent.parent
if str(_BENCHMARK_ROOT) not in sys.path:
    sys.path.insert(0, str(_BENCHMARK_ROOT))

from common import DEFAULT_DAYS

from ground_truth.tasks import run_ground_truth_task

ACCEPTED_GT_QUERIES: tuple[str, ...] = ("Q1", "Q2", "Q1Q2", "Q3", "Q4")
ALL_GT_BATCH: tuple[str, ...] = ("Q1Q2", "Q3", "Q4")


def _shutdown_executor_on_failure(
    executor: ProcessPoolExecutor,
    futures: dict,
) -> None:
    for future in futures:
        if not future.done():
            future.cancel()
    if sys.version_info >= (3, 9):
        executor.shutdown(wait=False, cancel_futures=True)
    else:
        executor.shutdown(wait=False)


def _wait_for_futures(
    executor: ProcessPoolExecutor,
    futures: dict,
):
    failed: tuple[str, str, BaseException] | None = None
    try:
        for future in as_completed(futures):
            query_id, day = futures[future]
            try:
                future.result()
            except Exception as exc:
                failed = (query_id, day, exc)
                break
            print(
                "ground_truth done",
                query_id,
                day,
                file=sys.stderr,
                flush=True,
            )
    finally:
        if failed is not None:
            _shutdown_executor_on_failure(executor, futures)
        else:
            executor.shutdown(wait=True)
    return failed


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute offline ground truth CSVs for the DEBS benchmark.",
    )
    parser.add_argument(
        "--query",
        default="all",
        help='Query id, or "all" for every query supported by this checkout.',
    )
    parser.add_argument(
        "--day",
        default="all",
        help='Trading day tag (e.g. 08-11-21) or "all" for DEFAULT_DAYS.',
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=_BENCHMARK_ROOT / "results" / "ground_truth",
    )
    parser.add_argument("--chunksize", type=int, default=200_000)
    parser.add_argument(
        "--max-event-minutes",
        type=int,
        default=0,
        help="Stop after this many minutes of event time from the first tick (0 = full day).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Parallel worker processes (default: min(CPU count, number of tasks))",
    )
    args = parser.parse_args()
    max_event_minutes = args.max_event_minutes or None

    if args.query != "all" and args.query not in ACCEPTED_GT_QUERIES:
        parser.error(
            f"--query must be 'all' or one of {ACCEPTED_GT_QUERIES}, got {args.query!r}"
        )

    days = list(DEFAULT_DAYS) if args.day == "all" else [args.day]
    if args.query == "all":
        query_ids = list(ALL_GT_BATCH)
    else:
        query_ids = [args.query]

    tasks = [(query_id, day) for query_id in query_ids for day in days]
    task_count = len(tasks)
    cpu_count = os.cpu_count() or 4
    max_workers = args.workers if args.workers is not None else min(cpu_count, task_count)
    max_workers = max(1, min(max_workers, task_count))

    print(
        "ground_truth start",
        f"query_tasks={len(query_ids)}",
        f"days={len(days)}",
        f"total={task_count}",
        f"workers={max_workers}",
        file=sys.stderr,
        flush=True,
    )

    if max_workers == 1:
        for index, (query_id, day) in enumerate(tasks, 1):
            print(
                "ground_truth step",
                f"{index}/{task_count}",
                query_id,
                day,
                file=sys.stderr,
                flush=True,
            )
            try:
                run_ground_truth_task(
                    query_id,
                    day,
                    args.out_dir,
                    args.chunksize,
                    max_event_minutes=max_event_minutes,
                )
            except Exception as exc:
                print(
                    "ground_truth FAILED",
                    f"query={query_id}",
                    f"day={day}",
                    str(exc),
                    file=sys.stderr,
                    flush=True,
                )
                traceback.print_exception(
                    type(exc), exc, exc.__traceback__, file=sys.stderr
                )
                print("ground_truth aborted after first failure", file=sys.stderr, flush=True)
                raise SystemExit(1) from exc
    else:
        executor = ProcessPoolExecutor(max_workers=max_workers)
        future_map = {
            executor.submit(
                run_ground_truth_task,
                query_id,
                day,
                args.out_dir,
                args.chunksize,
                max_event_minutes=max_event_minutes,
            ): (query_id, day)
            for query_id, day in tasks
        }
        print(
            "ground_truth submitted",
            f"{len(future_map)} tasks",
            file=sys.stderr,
            flush=True,
        )
        failed = _wait_for_futures(executor, future_map)
        if failed is not None:
            fq, fd, exc = failed
            print(
                "ground_truth FAILED",
                f"query={fq}",
                f"day={fd}",
                str(exc),
                file=sys.stderr,
                flush=True,
            )
            traceback.print_exception(
                type(exc), exc, exc.__traceback__, file=sys.stderr
            )
            print(
                "ground_truth aborted after first failure",
                file=sys.stderr,
                flush=True,
            )
            raise SystemExit(1)

    print("ground_truth finished", file=sys.stderr, flush=True)


if __name__ == "__main__":
    main()
