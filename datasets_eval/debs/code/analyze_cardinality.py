from __future__ import annotations

import pandas as pd

from utils import ensure_dirs, iter_csv_chunks, list_csv_files, results_dir, write_csv_rows


def main() -> None:
    ensure_dirs()
    summ_path = results_dir() / "summaries" / "cardinality_summary.csv"
    overall_path = results_dir() / "summaries" / "cardinality_overall.csv"
    sum_rows: list[dict] = []
    g_id: set[str] = set()
    g_st: set[str] = set()
    g_ids: set[tuple[str, str]] = set()
    g_idd: set[tuple[str, str]] = set()
    g_std: set[tuple[str, str]] = set()
    for path in list_csv_files():
        name = path.name
        s_id: set[str] = set()
        s_st: set[str] = set()
        s_ids: set[tuple[str, str]] = set()
        s_idd: set[tuple[str, str]] = set()
        s_std: set[tuple[str, str]] = set()
        for ch in iter_csv_chunks(path):
            ids = ch["ID"].astype("string").str.strip()
            sts = ch["SecType"].astype("string").str.strip()
            dts = ch["Date"].astype("string").str.strip()
            m = ids.notna() & (ids != "")
            if not m.any():
                continue
            df = pd.DataFrame({"ID": ids[m], "SecType": sts[m], "Date": dts[m]})
            s_id.update(df["ID"].unique().tolist())
            sm = df["SecType"].notna() & (df["SecType"] != "")
            if sm.any():
                s_st.update(df.loc[sm, "SecType"].unique().tolist())
            u = df[["ID", "SecType"]].drop_duplicates()
            if not u.empty:
                s_ids.update(map(tuple, u.to_numpy(dtype=object)))
            u2 = df[["ID", "Date"]].drop_duplicates()
            if not u2.empty:
                s_idd.update(map(tuple, u2.to_numpy(dtype=object)))
            sts2 = df["SecType"].fillna("").astype("string").str.strip()
            dts2 = df["Date"].fillna("").astype("string").str.strip()
            vm = (sts2 != "") & (dts2 != "")
            if vm.any():
                u3 = df.loc[vm, ["SecType", "Date"]].drop_duplicates()
                s_std.update(map(tuple, u3.to_numpy(dtype=object)))
        g_id.update(s_id)
        g_st.update(s_st)
        g_ids.update(s_ids)
        g_idd.update(s_idd)
        g_std.update(s_std)
        sum_rows.append({"file": name, "dim_name": "ID", "unique_count": len(s_id)})
        sum_rows.append({"file": name, "dim_name": "SecType", "unique_count": len(s_st)})
        sum_rows.append({"file": name, "dim_name": "ID_SecType", "unique_count": len(s_ids)})
        sum_rows.append({"file": name, "dim_name": "ID_Date", "unique_count": len(s_idd)})
        sum_rows.append({"file": name, "dim_name": "SecType_Date", "unique_count": len(s_std)})
    overall = [
        {"dimension": "ID", "unique_count_global": len(g_id)},
        {"dimension": "SecType", "unique_count_global": len(g_st)},
        {"dimension": "ID_SecType", "unique_count_global": len(g_ids)},
        {"dimension": "ID_Date", "unique_count_global": len(g_idd)},
        {"dimension": "SecType_Date", "unique_count_global": len(g_std)},
    ]
    write_csv_rows(summ_path, ("file", "dim_name", "unique_count"), sum_rows)
    write_csv_rows(overall_path, ("dimension", "unique_count_global"), overall)


if __name__ == "__main__":
    main()
