from __future__ import annotations

"""Q7 — Top-K entities by anomaly volume.

Purpose
-------
Rank entities by anomaly activity per window.

This implements the Q7 definition from ``docs/02_benchmark_queries.md``:
for each 5-minute window, count anomaly events per entity and keep the Top-K
entities by anomaly-event volume.

Formula
-------
For each (entity, window):
    score(entity, w) = sum_{t in w} 1[anomaly_t(entity)]

Return the TopK entities by ``score`` within each window.

An anomaly event uses the same IQR/Tukey-fence rule as Q5. For each
``(entity, metric_base, window)``:
    lower_fence = q1 - 1.5 * iqr
    upper_fence = q3 + 1.5 * iqr

An event ``x_t`` is anomalous when:
    x_t < lower_fence or x_t > upper_fence

Output columns
--------------
entity, window_start_s, window_size_s, window_label, exact_count, rank
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
    TOP_K_ENTITIES,
    WINDOW_5MIN_S,
    _stream_long_chunks,
    log_phase,
)
from ground_truth.q5 import MIN_WINDOW_SAMPLES, compute_q5_single_window

PRIMARY_WINDOW_S = WINDOW_5MIN_S
PRIMARY_WINDOW_LABEL = "5min"

_Q7_COLS = [
    "entity",
    "window_start_s",
    "window_size_s",
    "window_label",
    "exact_count",
    "rank",
]


def _build_anomaly_bounds(
    csv_path: Path,
    window_s: int,
    window_label: str,
    chunksize: int,
) -> dict[tuple[str, str, int], tuple[float, float]]:
    """Return Tukey-fence bounds for each (entity, metric_base, window)."""
    q5_df = compute_q5_single_window(csv_path, window_s, window_label, chunksize)
    if q5_df.empty:
        return {}

    bounds: dict[tuple[str, str, int], tuple[float, float]] = {}
    for _, row in q5_df.iterrows():
        if int(row["n_total"]) < MIN_WINDOW_SAMPLES:
            continue
        key = (
            str(row["entity"]),
            str(row["metric_base"]),
            int(row["window_start_s"]),
        )
        bounds[key] = (float(row["lower_fence"]), float(row["upper_fence"]))
    return bounds


def compute_q7_single_window(
    csv_path: Path,
    window_s: int,
    window_label: str,
    k: int,
    chunksize: int,
) -> pd.DataFrame:
    """Return exact Top-K entities by anomaly-event count per window."""
    bounds = _build_anomaly_bounds(csv_path, window_s, window_label, chunksize)
    if not bounds:
        return pd.DataFrame(columns=_Q7_COLS)

    entity_counts: dict[tuple[str, int], int] = defaultdict(int)
    for chunk in _stream_long_chunks(csv_path, chunksize):
        if chunk.empty:
            continue

        ts = chunk["ts_s"].to_numpy()
        entities = chunk["entity"].astype(str).to_numpy()
        metric_bases = chunk["metric_base"].astype(str).to_numpy()
        values = chunk["value"].to_numpy(dtype=float)
        window_starts = (ts // window_s) * window_s

        for i in range(len(values)):
            bounds_key = (entities[i], metric_bases[i], int(window_starts[i]))
            fence = bounds.get(bounds_key)
            if fence is None:
                continue
            lower_fence, upper_fence = fence
            if values[i] < lower_fence or values[i] > upper_fence:
                entity_counts[(entities[i], int(window_starts[i]))] += 1

    if not entity_counts:
        return pd.DataFrame(columns=_Q7_COLS)

    per_window: dict[int, list[tuple[str, int]]] = defaultdict(list)
    for (entity, window_start_s), count in entity_counts.items():
        per_window[window_start_s].append((entity, count))

    rows = []
    for window_start_s in sorted(per_window):
        top_entities = sorted(
            per_window[window_start_s],
            key=lambda item: (-item[1], item[0]),
        )[:k]
        for rank, (entity, count) in enumerate(top_entities, start=1):
            rows.append({
                "entity": entity,
                "window_start_s": window_start_s,
                "window_size_s": window_s,
                "window_label": window_label,
                "exact_count": count,
                "rank": rank,
            })

    return pd.DataFrame(rows, columns=_Q7_COLS)


def run_q7(
    file_tag: str,
    output_dir: Path,
    k: int = TOP_K_ENTITIES,
    chunksize: int = 200,
    window_s: int = PRIMARY_WINDOW_S,
    window_label: str = PRIMARY_WINDOW_LABEL,
) -> Path:
    """Compute Q7 ground truth for one file."""
    csv_path = file_csv_path(file_tag)
    if not csv_path.is_file():
        raise FileNotFoundError(f"Exathlon CSV not found: {csv_path}")

    tag = file_tag_safe(file_tag)
    out_dir = output_dir / "Q7"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{tag}.csv"

    log_phase(
        "Q7",
        tag,
        "start",
        file=str(csv_path),
        window=window_label,
        k=k,
        chunksize=chunksize,
    )

    t0 = time.perf_counter()
    df = compute_q7_single_window(csv_path, window_s, window_label, k, chunksize)
    df.to_csv(out_path, index=False)
    elapsed = time.perf_counter() - t0

    log_phase(
        "Q7",
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
        description="Compute Q7 exact Top-K entities by anomaly-event count.",
    )
    parser.add_argument("--file", required=True, help="Exathlon file tag, e.g. app1/1_0_10000_17")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=_BENCHMARK_ROOT / "results" / "ground_truth",
    )
    parser.add_argument("--top-k", type=int, default=TOP_K_ENTITIES)
    parser.add_argument("--chunksize", type=int, default=200)
    args = parser.parse_args()

    run_q7(
        file_tag=args.file,
        output_dir=args.out_dir,
        k=args.top_k,
        chunksize=args.chunksize,
    )


if __name__ == "__main__":
    main()
