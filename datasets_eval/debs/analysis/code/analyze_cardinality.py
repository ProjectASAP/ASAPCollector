from __future__ import annotations

import pandas as pd

from utils import (
    ensure_dirs,
    iter_csv_chunks,
    list_csv_files,
    parse_dataset_and_configure,
    results_dir,
    split_series_symbol_exchange,
    write_csv_rows,
)


def main() -> None:
    parse_dataset_and_configure()
    ensure_dirs()
    summ_path = results_dir() / "summaries" / "cardinality_summary.csv"
    overall_path = results_dir() / "summaries" / "cardinality_overall.csv"
    sum_rows: list[dict] = []
    g_sym: set[str] = set()
    g_exc: set[str] = set()
    g_sec: set[str] = set()
    g_otlp: set[tuple[str, str, str]] = set()
    for path in list_csv_files():
        name = path.name
        s_sym: set[str] = set()
        s_exc: set[str] = set()
        s_sec: set[str] = set()
        s_otlp: set[tuple[str, str, str]] = set()
        for ch in iter_csv_chunks(path):
            ids = ch["ID"].astype("string").str.strip()
            sts = ch["SecType"].astype("string").str.strip()
            m = ids.notna() & (ids != "")
            if not m.any():
                continue
            ids_m = ids[m]
            sts_m = sts[m]
            sym, exc = split_series_symbol_exchange(ids_m)
            df = pd.DataFrame(
                {"symbol": sym, "exchange": exc, "sectype": sts_m}
            )
            s_sym.update(df["symbol"].unique().tolist())
            exnz = df["exchange"].astype("string").str.strip()
            mex = exnz.notna() & (exnz != "")
            if mex.any():
                s_exc.update(df.loc[mex, "exchange"].astype("string").str.strip().unique().tolist())
            snz = df["sectype"].notna() & (df["sectype"].astype("string").str.strip() != "")
            if snz.any():
                s_sec.update(df.loc[snz, "sectype"].astype("string").str.strip().unique().tolist())
            u = df[["symbol", "exchange", "sectype"]].drop_duplicates()
            if not u.empty:
                s_otlp.update(
                    map(
                        tuple,
                        u.to_numpy(dtype=object),
                    )
                )
        g_sym.update(s_sym)
        g_exc.update(s_exc)
        g_sec.update(s_sec)
        g_otlp.update(s_otlp)
        sum_rows.append({"file": name, "dim_name": "symbol", "unique_count": len(s_sym)})
        sum_rows.append({"file": name, "dim_name": "exchange", "unique_count": len(s_exc)})
        sum_rows.append({"file": name, "dim_name": "sectype", "unique_count": len(s_sec)})
        sum_rows.append(
            {
                "file": name,
                "dim_name": "symbol_exchange_sectype",
                "unique_count": len(s_otlp),
            }
        )
    overall = [
        {"dimension": "symbol", "unique_count_global": len(g_sym)},
        {"dimension": "exchange", "unique_count_global": len(g_exc)},
        {"dimension": "sectype", "unique_count_global": len(g_sec)},
        {
            "dimension": "symbol_exchange_sectype",
            "unique_count_global": len(g_otlp),
        },
    ]
    write_csv_rows(summ_path, ("file", "dim_name", "unique_count"), sum_rows)
    write_csv_rows(overall_path, ("dimension", "unique_count_global"), overall)


if __name__ == "__main__":
    main()
