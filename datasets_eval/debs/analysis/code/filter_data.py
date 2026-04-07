from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from utils import DEBS_ROOT

_DEFAULT_TRADING_START = "09:00:00"


def nz(s: pd.Series) -> pd.Series:
    x = s.astype("string").str.strip()
    return x.notna() & (x != "") & (x.str.lower() != "nan")


def main() -> None:
    p = argparse.ArgumentParser(
        description="Write rows with Last and Trading time to data_filtered/. "
                    "Rows whose Trading time is before --trading-start are dropped.",
    )
    p.add_argument(
        "--source",
        type=Path,
        default=DEBS_ROOT / "data",
        help="Directory of raw DEBS CSV files",
    )
    p.add_argument(
        "--dest",
        type=Path,
        default=DEBS_ROOT / "data_filtered",
        help="Output directory for price-stream CSVs",
    )
    p.add_argument("--chunksize", type=int, default=200_000)
    p.add_argument(
        "--trading-start",
        default=_DEFAULT_TRADING_START,
        help="Earliest Trading time to keep, HH:MM:SS format in CET "
             f"(default: {_DEFAULT_TRADING_START}). "
             "Rows with a Trading time before this value are excluded.",
    )
    args = p.parse_args()
    args.dest.mkdir(parents=True, exist_ok=True)

    for path in sorted(args.source.glob("*.csv")):
        if not path.is_file():
            continue
        out = args.dest / path.name
        first_write = True
        any_row = False
        for chunk in pd.read_csv(
            path,
            comment="#",
            chunksize=args.chunksize,
            dtype=object,
            low_memory=False,
            index_col=False,
        ):
            if "Last" not in chunk.columns or "Trading time" not in chunk.columns:
                raise SystemExit(f"missing Last or Trading time column in {path}")
            m = nz(chunk["Last"]) & nz(chunk["Trading time"])
            last_num = pd.to_numeric(chunk.loc[m, "Last"], errors="coerce")
            m[m] = last_num > 0
            # Keep only rows at or after the configured trading start time.
            # Trading time is HH:MM:SS[.ssss] in CET — lexicographic comparison works.
            tt = chunk["Trading time"].astype(str).str.strip()
            m &= tt >= args.trading_start
            sub = chunk.loc[m]
            if sub.empty:
                continue
            any_row = True
            sub.to_csv(
                out,
                index=False,
                mode="w" if first_write else "a",
                header=first_write,
            )
            first_write = False
        if not any_row:
            hdr = pd.read_csv(path, comment="#", nrows=0, index_col=False)
            hdr.to_csv(out, index=False)


if __name__ == "__main__":
    main()
