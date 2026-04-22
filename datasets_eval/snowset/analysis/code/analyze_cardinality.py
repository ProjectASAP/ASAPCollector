from __future__ import annotations

from utils import (
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
    specs_by_name = {spec.name: spec for spec in dataset_specs()}

    for spec in specs_by_name.values():
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
    if "snowset-main" in specs_by_name and "fully-joined" in specs_by_name:
        main_df = read_parquet(specs_by_name["snowset-main"].path, columns=["queryId"])
        aux_df = read_parquet(specs_by_name["fully-joined"].path, columns=["queryId"])
        main_qids = set(main_df["queryId"].tolist())
        aux_qids = set(aux_df["queryId"].tolist())
        intersection = main_qids & aux_qids
        join_rows = [
            {"metric": "main_unique_queryId", "value": len(main_qids)},
            {"metric": "fully_joined_unique_queryId", "value": len(aux_qids)},
            {"metric": "shared_queryId", "value": len(intersection)},
            {
                "metric": "main_query_coverage_pct",
                "value": (len(intersection) / len(main_qids) * 100.0) if main_qids else 0.0,
            },
            {
                "metric": "fully_joined_rows_per_shared_query_avg",
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
