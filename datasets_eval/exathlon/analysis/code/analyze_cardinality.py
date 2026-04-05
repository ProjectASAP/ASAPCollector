from __future__ import annotations

import csv
from pathlib import Path

WINDOWS = [
    (60, "1min"),
    (300, "5min"),
    (900, "15min"),
    (1800, "30min"),
    (3600, "1hour"),
]

KNOWN_SUFFIXES = [
    "count",
    "max",
    "mean",
    "min",
    "p50",
    "p75",
    "p95",
    "p98",
    "p99",
    "p999",
    "stddev",
    "value",
]


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def raw_dir() -> Path:
    return project_root() / "MIT_SUPERCLOUD" / "data" / "raw"


def out_dir() -> Path:
    return project_root() / "analysis" / "results"


def ensure_dirs() -> Path:
    summaries = out_dir() / "summaries"
    summaries.mkdir(parents=True, exist_ok=True)
    return summaries


def list_csv_files() -> list[Path]:
    return [p for p in sorted(raw_dir().glob("app*/*.csv")) if p.is_file()]


def read_header(path: Path) -> list[str]:
    with path.open("r", newline="") as f:
        reader = csv.reader(f)
        return next(reader, [])


def split_suffix(metric: str) -> tuple[str, str]:
    for suffix in KNOWN_SUFFIXES:
        token = f"_{suffix}"
        if metric.endswith(token):
            return metric[: -len(token)], suffix
    return metric, "unknown"


def entity_from_metric(metric: str) -> str:
    if "_" not in metric:
        return "global"
    return metric.split("_", 1)[0]


def detect_domain(metrics: list[str]) -> str:
    lower_blob = " ".join(m.lower() for m in metrics[:2000])
    spark_tokens = ("executor", "jvm", "hive", "codegenerator", "spark")
    hpc_tokens = ("node", "cpu", "disk", "net", "mem", "proc", "vm")
    has_spark = any(t in lower_blob for t in spark_tokens)
    has_hpc = any(t in lower_blob for t in hpc_tokens)
    if has_spark and has_hpc:
        return "mixed spark-app + hpc node telemetry"
    if has_spark:
        return "spark application telemetry"
    if has_hpc:
        return "hpc node/system telemetry"
    return "generic multivariate telemetry"


def supported_queries(
    total_series: int, entities: int, suffixes: set[str], metric_bases: int
) -> tuple[str, str]:
    queries = [
        "windowed sum/avg/min/max",
        "trend and anomaly rollups",
    ]
    sketches = {"KLL/DDSketch"}
    if "count" in suffixes or total_series > 500:
        queries.append("top-k heavy metrics per window")
        sketches.add("Count-Min Sketch + SpaceSaving")
    if metric_bases > 100:
        queries.append("distinct metric families / active series")
        sketches.add("HyperLogLog")
    if entities > 1:
        queries.append("group-by entity (node/executor) aggregation")
    return "; ".join(queries), "; ".join(sorted(sketches))


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    summaries_dir = ensure_dirs()
    per_file_rows: list[dict[str, str]] = []
    overall_rows: list[dict[str, str]] = []
    query_rows: list[dict[str, str]] = []

    global_series: set[str] = set()
    global_entities: set[str] = set()
    global_metric_bases: set[str] = set()
    global_suffixes: set[str] = set()

    for file_path in list_csv_files():
        header = read_header(file_path)
        metrics = [h.strip() for h in header if h and h.strip() and h.strip() != "t"]
        series_set = set(metrics)
        entities = {entity_from_metric(m) for m in metrics}
        metric_bases: set[str] = set()
        suffixes: set[str] = set()
        for metric in metrics:
            base, suffix = split_suffix(metric)
            metric_bases.add(base)
            suffixes.add(suffix)

        global_series.update(series_set)
        global_entities.update(entities)
        global_metric_bases.update(metric_bases)
        global_suffixes.update(suffixes)

        domain = detect_domain(metrics)
        queries, sketches = supported_queries(
            len(series_set), len(entities), suffixes, len(metric_bases)
        )

        per_file_rows.append(
            {
                "file": file_path.name,
                "time_series_cardinality": str(len(series_set)),
                "entity_cardinality": str(len(entities)),
                "metric_base_cardinality": str(len(metric_bases)),
                "aggregation_suffix_cardinality": str(len(suffixes)),
                "aggregation_suffixes": ";".join(sorted(suffixes)),
            }
        )

        query_rows.append(
            {
                "file": file_path.name,
                "use_case_domain": domain,
                "supported_query_types": queries,
                "candidate_sketches": sketches,
            }
        )

    overall_rows.append(
        {"dimension": "time_series", "unique_count_global": str(len(global_series))}
    )
    overall_rows.append(
        {"dimension": "entity", "unique_count_global": str(len(global_entities))}
    )
    overall_rows.append(
        {
            "dimension": "metric_base",
            "unique_count_global": str(len(global_metric_bases)),
        }
    )
    overall_rows.append(
        {
            "dimension": "aggregation_suffix",
            "unique_count_global": str(len(global_suffixes)),
        }
    )
    overall_rows.append(
        {
            "dimension": "tested_window_sizes",
            "unique_count_global": ";".join(label for _, label in WINDOWS),
        }
    )

    write_csv(
        summaries_dir / "cardinality_summary.csv",
        [
            "file",
            "time_series_cardinality",
            "entity_cardinality",
            "metric_base_cardinality",
            "aggregation_suffix_cardinality",
            "aggregation_suffixes",
        ],
        per_file_rows,
    )
    write_csv(
        summaries_dir / "cardinality_overall.csv",
        ["dimension", "unique_count_global"],
        overall_rows,
    )
    write_csv(
        summaries_dir / "supported_queries_by_dataset.csv",
        ["file", "use_case_domain", "supported_query_types", "candidate_sketches"],
        query_rows,
    )


if __name__ == "__main__":
    main()
