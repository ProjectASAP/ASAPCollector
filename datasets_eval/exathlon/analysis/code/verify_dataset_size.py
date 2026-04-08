from __future__ import annotations

import argparse
import csv
from pathlib import Path

DEFAULT_TARGET_ITEMS = 2_335_781
SENTINEL_VALUE = "-1.0"


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def raw_dir() -> Path:
    root = project_root()
    candidates = [
        root / "exathlon" / "data" / "raw",
        root / "data" / "raw",
    ]
    for path in candidates:
        if path.is_dir():
            return path
    joined = ", ".join(str(p) for p in candidates)
    raise FileNotFoundError(f"Unable to locate raw data directory. Checked: {joined}")


def out_dir() -> Path:
    return project_root() / "analysis" / "results" / "summaries"


def list_csv_files() -> list[Path]:
    files = [p for p in sorted(raw_dir().glob("app*/*.csv")) if p.is_file()]
    if not files:
        raise FileNotFoundError(
            f"No CSV files found under expected app folders in {raw_dir()}"
        )
    return files


def verify_file(path: Path, count_active_cells: bool) -> dict[str, int | str]:
    row_count = 0
    metric_cols = 0
    active_metric_cells = 0

    with path.open("r", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, [])
        metric_cols = max(len(header) - 1, 0)

        for row in reader:
            if not row:
                continue
            row_count += 1
            if not count_active_cells:
                continue
            for value in row[1:]:
                cell = value.strip()
                if not cell or cell.lower() == "nan" or cell == SENTINEL_VALUE:
                    continue
                active_metric_cells += 1

    result: dict[str, int | str] = {
        "file": path.name,
        "rows": row_count,
        "metric_columns": metric_cols,
        "wide_cells": row_count * metric_cols,
    }
    if count_active_cells:
        result["active_metric_cells"] = active_metric_cells
    return result


def write_summary(rows: list[dict[str, int | str]], count_active_cells: bool) -> Path:
    summaries = out_dir()
    summaries.mkdir(parents=True, exist_ok=True)
    out_path = summaries / "dataset_size_summary.csv"
    fieldnames = ["file", "rows", "metric_columns", "wide_cells"]
    if count_active_cells:
        fieldnames.append("active_metric_cells")

    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify Exathlon dataset size against the paper's N."
    )
    parser.add_argument(
        "--target-items",
        type=int,
        default=DEFAULT_TARGET_ITEMS,
        help=f"Expected total item count to compare against (default: {DEFAULT_TARGET_ITEMS}).",
    )
    parser.add_argument(
        "--count-active-cells",
        action="store_true",
        help="Also count non-sentinel metric cells after skipping blank/NaN/-1.0 values.",
    )
    args = parser.parse_args()

    per_file = [
        verify_file(path, count_active_cells=args.count_active_cells)
        for path in list_csv_files()
    ]
    summary_path = write_summary(per_file, count_active_cells=args.count_active_cells)

    total_rows = sum(int(row["rows"]) for row in per_file)
    total_wide_cells = sum(int(row["wide_cells"]) for row in per_file)
    total_metric_columns = sum(int(row["metric_columns"]) for row in per_file)

    print(f"files={len(per_file)}")
    print(f"total_rows={total_rows}")
    print(f"total_metric_columns_summed={total_metric_columns}")
    print(f"total_wide_cells={total_wide_cells}")

    if args.count_active_cells:
        total_active_metric_cells = sum(int(row["active_metric_cells"]) for row in per_file)
        print(f"total_active_metric_cells={total_active_metric_cells}")

    if total_rows == args.target_items:
        print(f"match_rows_target=yes target={args.target_items}")
    else:
        diff = total_rows - args.target_items
        print(
            f"match_rows_target=no target={args.target_items} actual={total_rows} diff={diff}"
        )

    print(f"per_file_summary={summary_path}")


if __name__ == "__main__":
    main()
