from __future__ import annotations

"""Parallel coordinator for offline ground-truth computation (exathlon benchmark)."""

import argparse
import os
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

_BENCHMARK_ROOT = Path(__file__).resolve().parent.parent
if str(_BENCHMARK_ROOT) not in sys.path:
    sys.path.insert(0, str(_BENCHMARK_ROOT))

from common import DEFAULT_FILES
from ground_truth.tasks import ACCEPTED_GT_QUERIES, run_ground_truth_task


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
    failed = None
    try:
        for future in as_completed(futures):
            query_id, file_tag = futures[future]
            try:
                future.result()
            except Exception as exc:
                failed = (query_id, file_tag, exc)
                break
            print(
                "ground_truth done",
                query_id,
                file_tag,
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
        description="Compute offline ground truth CSVs for the Exathlon benchmark.",
    )
    parser.add_argument(
        "--query",
        default="all",
        help=f'Query id or "all". Accepted: {ACCEPTED_GT_QUERIES}',
    )
    parser.add_argument(
        "--file",
        default="all",
        help='File tag (e.g. app1/1_0_10000_17) or "all" for DEFAULT_FILES.',
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=_BENCHMARK_ROOT / "results" / "ground_truth",
    )
    parser.add_argument("--chunksize", type=int, default=200)
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Parallel worker processes (default: min(CPU count, task count)).",
    )
    args = parser.parse_args()

    if args.query != "all" and args.query not in ACCEPTED_GT_QUERIES:
        parser.error(
            f"--query must be 'all' or one of {ACCEPTED_GT_QUERIES}, got {args.query!r}"
        )

    file_tags = list(DEFAULT_FILES) if args.file == "all" else [args.file]
    query_ids = list(ACCEPTED_GT_QUERIES) if args.query == "all" else [args.query]

    tasks = [(q, f) for q in query_ids for f in file_tags]
    task_count = len(tasks)
    cpu_count = os.cpu_count() or 4
    max_workers = args.workers if args.workers is not None else min(cpu_count, task_count)
    max_workers = max(1, min(max_workers, task_count))

    print(
        "ground_truth start",
        f"query_tasks={len(query_ids)}",
        f"files={len(file_tags)}",
        f"total={task_count}",
        f"workers={max_workers}",
        file=sys.stderr,
        flush=True,
    )

    if max_workers == 1:
        for index, (query_id, file_tag) in enumerate(tasks, 1):
            print(
                "ground_truth step",
                f"{index}/{task_count}",
                query_id,
                file_tag,
                file=sys.stderr,
                flush=True,
            )
            try:
                run_ground_truth_task(query_id, file_tag, args.out_dir, args.chunksize)
            except Exception as exc:
                print(
                    "ground_truth FAILED",
                    f"query={query_id}",
                    f"file={file_tag}",
                    str(exc),
                    file=sys.stderr,
                    flush=True,
                )
                traceback.print_exception(type(exc), exc, exc.__traceback__, file=sys.stderr)
                raise SystemExit(1) from exc
    else:
        executor = ProcessPoolExecutor(max_workers=max_workers)
        future_map = {
            executor.submit(
                run_ground_truth_task, query_id, file_tag, args.out_dir, args.chunksize
            ): (query_id, file_tag)
            for query_id, file_tag in tasks
        }
        print(
            "ground_truth submitted",
            f"{len(future_map)} tasks",
            file=sys.stderr,
            flush=True,
        )
        failed = _wait_for_futures(executor, future_map)
        if failed is not None:
            fq, ff, exc = failed
            print(
                "ground_truth FAILED",
                f"query={fq}",
                f"file={ff}",
                str(exc),
                file=sys.stderr,
                flush=True,
            )
            traceback.print_exception(type(exc), exc, exc.__traceback__, file=sys.stderr)
            raise SystemExit(1)

    print("ground_truth finished", file=sys.stderr, flush=True)


if __name__ == "__main__":
    main()
