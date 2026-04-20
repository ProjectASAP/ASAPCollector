from __future__ import annotations

from utils import (
    DATASETS,
    dataset_specs,
    ensure_dirs,
    parse_dataset_and_configure,
    read_parquet,
    results_dir,
    write_csv_rows,
)


def main() -> None:
    parse_dataset_and_configure()
    ensure_dirs()
    summary_rows: list[dict] = []

    for spec in dataset_specs():
        df = read_parquet(spec.path, columns=spec.entity_columns)
        row = {
            "dataset": spec.name,
            "rows": len(df),
        }
        for column in ("queryId", "warehouseId", "databaseId"):
            row[f"unique_{column}"] = int(df[column].nunique()) if column in df.columns else ""
        summary_rows.append(row)

    write_csv_rows(
        results_dir() / "summaries" / "cardinality_summary.csv",
        ("dataset", "rows", "unique_queryId", "unique_warehouseId", "unique_databaseId"),
        summary_rows,
    )

    # Join analysis requires both datasets; skip when running a single-dataset mode.
    specs_by_name = {d.name: d for d in dataset_specs()}
    if "snowset-main" in specs_by_name and "ts-explosion" in specs_by_name:
        main_df = read_parquet(DATASETS[0].path, columns=["queryId"])
        aux_df = read_parquet(DATASETS[1].path, columns=["queryId"])
        main_qids = set(main_df["queryId"].tolist())
        aux_qids = set(aux_df["queryId"].tolist())
        intersection = main_qids & aux_qids
        join_rows = [
            {"metric": "main_unique_queryId", "value": len(main_qids)},
            {"metric": "aux_unique_queryId", "value": len(aux_qids)},
            {"metric": "shared_queryId", "value": len(intersection)},
            {
                "metric": "main_query_coverage_pct",
                "value": (len(intersection) / len(main_qids) * 100.0) if main_qids else 0.0,
            },
            {
                "metric": "aux_rows_per_shared_query_avg",
                "value": (len(aux_df) / len(intersection)) if intersection else 0.0,
            },
        ]
        write_csv_rows(
            results_dir() / "summaries" / "join_summary.csv",
            ("metric", "value"),
            join_rows,
        )


if __name__ == "__main__":
    main()
