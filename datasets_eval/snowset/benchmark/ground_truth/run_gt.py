from __future__ import annotations

import argparse
import os
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

BENCH_ROOT = Path(__file__).resolve().parent.parent
if str(BENCH_ROOT) not in sys.path:
    sys.path.insert(0, str(BENCH_ROOT))

from common import DEFAULT_QUERIES, DEFAULT_SLICES
from ground_truth.tasks import run_ground_truth_task

ACCEPTED_GT_QUERIES: tuple[str, ...] = tuple(DEFAULT_QUERIES)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute ground truth CSVs for the Snowset benchmark."
    )
    parser.add_argument("--query", default="all",
                        help='"all" or one of Q1–Q6.')
    parser.add_argument("--slice", default="all",
                        help='"all" or a named slice (e.g. "full").')
    parser.add_argument("--out-dir", type=Path,
                        default=BENCH_ROOT / "results" / "ground_truth")
    parser.add_argument("--chunksize", type=int, default=200_000)
    parser.add_argument("--workers", type=int, default=None)
    args = parser.parse_args()

    if args.query != "all" and args.query not in ACCEPTED_GT_QUERIES:
        parser.error(f"--query must be 'all' or one of {ACCEPTED_GT_QUERIES}")

    queries = list(ACCEPTED_GT_QUERIES) if args.query == "all" else [args.query]
    slices = list(DEFAULT_SLICES) if args.slice == "all" else [args.slice]
    tasks = [(q, s) for q in queries for s in slices]
    task_count = len(tasks)

    cpu_count = os.cpu_count() or 4
    max_workers = args.workers if args.workers is not None else min(cpu_count, task_count)
    max_workers = max(1, min(max_workers, task_count))

    print(f"ground_truth start queries={len(queries)} slices={len(slices)} "
          f"total={task_count} workers={max_workers}", file=sys.stderr, flush=True)

    if max_workers == 1:
        for i, (q, s) in enumerate(tasks, 1):
            print(f"ground_truth step {i}/{task_count} {q} {s}", file=sys.stderr, flush=True)
            try:
                run_ground_truth_task(q, s, args.out_dir, args.chunksize)
            except Exception as exc:
                print(f"ground_truth FAILED query={q} slice={s} {exc}",
                      file=sys.stderr, flush=True)
                traceback.print_exception(type(exc), exc, exc.__traceback__, file=sys.stderr)
                raise SystemExit(1) from exc
    else:
        with ProcessPoolExecutor(max_workers=max_workers) as ex:
            futures = {
                ex.submit(run_ground_truth_task, q, s, args.out_dir, args.chunksize): (q, s)
                for q, s in tasks
            }
            for future in as_completed(futures):
                q, s = futures[future]
                try:
                    future.result()
                    print(f"ground_truth done {q} {s}", file=sys.stderr, flush=True)
                except Exception as exc:
                    print(f"ground_truth FAILED query={q} slice={s} {exc}",
                          file=sys.stderr, flush=True)
                    traceback.print_exception(type(exc), exc, exc.__traceback__, file=sys.stderr)
                    for f in futures:
                        f.cancel()
                    raise SystemExit(1) from exc

    print("ground_truth finished", file=sys.stderr, flush=True)


if __name__ == "__main__":
    main()
