#!/usr/bin/env python3
"""Display a row window from selected Snowset parquet part files."""

from __future__ import annotations

import argparse
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
DATASETS = {
    "snowset-main-part-0": DATA_DIR / "snowset-main.parquet" / "part.0.parquet",
    "ts-explosion-part-0": DATA_DIR / "ts-explosion.parquet" / "part.0.parquet",
}


def load_pandas():
    try:
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError(
            "pandas is required to run this script. Install it with a parquet engine such as "
            "'pyarrow' or 'fastparquet'."
        ) from exc

    try:
        import pyarrow  # noqa: F401
    except ImportError:
        try:
            import fastparquet  # noqa: F401
        except ImportError as exc:
            raise RuntimeError(
                "A parquet engine is required. Install one of: 'pyarrow' or 'fastparquet'."
            ) from exc

    return pd


def load_rows(pd, dataset_path: Path, start_row: int, end_row: int):
    if not dataset_path.exists():
        raise FileNotFoundError(f"Dataset not found: {dataset_path}")

    dataframe = pd.read_parquet(dataset_path)
    start_index = max(start_row - 1, 0)
    end_index = max(end_row, start_index)
    return dataframe.iloc[start_index:end_index]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Display selected rows from the Snowset parquet datasets."
    )
    parser.add_argument(
        "--start-row",
        type=int,
        default=20,
        help="1-based starting row number to display. Default: 20.",
    )
    parser.add_argument(
        "--end-row",
        type=int,
        default=30,
        help="1-based ending row number to display, inclusive. Default: 30.",
    )
    args = parser.parse_args()

    if args.start_row < 1:
        raise ValueError("--start-row must be at least 1")
    if args.end_row < args.start_row:
        raise ValueError("--end-row must be greater than or equal to --start-row")

    pd = load_pandas()
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)

    for name, path in DATASETS.items():
        sample = load_rows(pd, path, args.start_row, args.end_row)
        print(f"\n=== {name} ({path}) | rows {args.start_row}-{args.end_row} ===")
        if sample.empty:
            print("No rows in the requested range.")
            continue
        print(sample.to_string(index=True))


if __name__ == "__main__":
    main()
