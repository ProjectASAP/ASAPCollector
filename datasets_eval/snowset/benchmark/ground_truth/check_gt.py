#!/usr/bin/env python3
"""Sanity-check Snowset ground truth CSV files.

Usage:
  cd datasets_eval/snowset/benchmark
  python3 ground_truth/check_gt.py
  python3 ground_truth/check_gt.py --queries Q1 Q5 --slices full
  python3 ground_truth/check_gt.py --no-color
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ALL_QUERIES = ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6"]
ALL_SLICES  = ["full"]

REQUIRED_COLS: dict[str, list[str]] = {
    "Q1": ["window_start", "warehouseId", "total_requests"],
    "Q2": ["window_start", "warehouseId", "query_count", "p50_ms", "p95_ms", "p99_ms"],
    "Q3": ["window_start", "warehouseId", "cache_miss_queries", "p95_bytes", "p99_bytes"],
    "Q4": ["window_start", "warehouseSize", "query_archetype", "frequency"],
    "Q5": ["window_start", "exact_distinct_warehouses", "total_queries"],
    "Q6": ["window_start", "warehouseSize", "concurrency_band", "sample_count",
            "p50_ms", "p95_ms", "p99_ms"],
}

NON_NUMERIC = {"warehouseId", "warehouseSize", "query_archetype", "window_start"}


class _C:
    GREEN  = "\033[32m"
    RED    = "\033[31m"
    YELLOW = "\033[33m"
    BOLD   = "\033[1m"
    RESET  = "\033[0m"

_USE_COLOR = True


def _tag(s: str) -> str:
    if not _USE_COLOR:
        return f"{s:<4}"
    m = {"PASS": _C.GREEN, "WARN": _C.YELLOW, "FAIL": _C.RED}
    return f"{m.get(s,'')}{s:<4}{_C.RESET}"


class Check:
    def __init__(self, status: str, label: str, detail: str = ""):
        self.status = status
        self.label = label
        self.detail = detail

    @property
    def passed(self) -> bool:
        return self.status != "FAIL"

    @property
    def is_warn(self) -> bool:
        return self.status == "WARN"

    def __str__(self) -> str:
        d = f"  {self.detail}" if self.detail else ""
        return f"    {_tag(self.status)}  {self.label:<44}{d}"


def _p(l: str, d: str = "") -> Check: return Check("PASS", l, d)
def _w(l: str, d: str = "") -> Check: return Check("WARN", l, d)
def _f(l: str, d: str = "") -> Check: return Check("FAIL", l, d)


def _base_checks(df: pd.DataFrame, required: list[str]) -> list[Check]:
    out: list[Check] = []
    missing = [c for c in required if c not in df.columns]
    if missing:
        out.append(_f("required_columns", f"missing: {missing}"))
        return out
    out.append(_p("required_columns"))
    if df.empty:
        out.append(_f("non_empty", "0 rows"))
        return out
    out.append(_p("non_empty", f"{len(df):,} rows"))
    num_cols = [c for c in required if c not in NON_NUMERIC
                and pd.api.types.is_numeric_dtype(df[c])]
    if num_cols:
        nans = int(df[num_cols].isnull().sum().sum())
        out.append(_p("no_nulls") if nans == 0
                   else _w("no_nulls", f"{nans} NaN values"))
    return out


def _check_q1(df: pd.DataFrame) -> list[Check]:
    neg = (df["total_requests"] < 0).sum()
    n_wh = df["warehouseId"].nunique()
    return [
        _p("nonneg_requests") if neg == 0 else _f("nonneg_requests", f"{neg} negative"),
        _p("warehouse_count", f"{n_wh} warehouses"),
    ]


def _check_q2(df: pd.DataFrame) -> list[Check]:
    bad = (df["p99_ms"] < df["p95_ms"]).sum()
    neg = (df["p50_ms"] < 0).sum()
    return [
        _p("p99_ge_p95") if bad == 0 else _f("p99_ge_p95", f"{bad} rows"),
        _p("p50_nonneg") if neg == 0 else _w("p50_nonneg", f"{neg} negative"),
    ]


def _check_q3(df: pd.DataFrame) -> list[Check]:
    bad = (df["p99_bytes"] < df["p95_bytes"]).sum()
    return [_p("p99_ge_p95") if bad == 0 else _f("p99_ge_p95", f"{bad} rows")]


def _check_q4(df: pd.DataFrame) -> list[Check]:
    valid_archetypes = {
        "scan_heavy", "memory_heavy", "local_spill_heavy",
        "remote_spill_heavy", "s3_read_heavy", "other",
    }
    bad = set(df["query_archetype"].unique()) - valid_archetypes
    neg = (df["frequency"] < 0).sum()
    return [
        _p("valid_archetypes") if not bad else _f("valid_archetypes", f"unknown: {bad}"),
        _p("freq_nonneg") if neg == 0 else _f("freq_nonneg", f"{neg} negative"),
    ]


def _check_q5(df: pd.DataFrame) -> list[Check]:
    n_windows = len(df)
    low = (df["exact_distinct_warehouses"] < 1).sum()
    return [
        _p("window_count", f"{n_windows} windows"),
        _p("distinct_ge_1") if low == 0 else _f("distinct_ge_1", f"{low} windows with 0"),
    ]


def _check_q6(df: pd.DataFrame) -> list[Check]:
    bad = (df["p99_ms"] < df["p95_ms"]).sum()
    neg = (df["concurrency_band"] < 0).sum()
    low = (df["sample_count"] < 1).sum()
    return [
        _p("p99_ge_p95") if bad == 0 else _f("p99_ge_p95", f"{bad} rows"),
        _p("band_nonneg") if neg == 0 else _f("band_nonneg", f"{neg} negative"),
        _p("sample_count_ge_1") if low == 0 else _f("sample_count_ge_1", f"{low} rows"),
    ]


_QUERY_CHECKER = {
    "Q1": _check_q1,
    "Q2": _check_q2,
    "Q3": _check_q3,
    "Q4": _check_q4,
    "Q5": _check_q5,
    "Q6": _check_q6,
}


def check_file(q: str, df: pd.DataFrame) -> list[Check]:
    checks = _base_checks(df, REQUIRED_COLS.get(q, []))
    if any(c.status == "FAIL" for c in checks):
        return checks
    fn = _QUERY_CHECKER.get(q)
    if fn:
        checks.extend(fn(df))
    return checks


def main() -> None:
    global _USE_COLOR
    ap = argparse.ArgumentParser(description="Sanity-check Snowset ground truth files.")
    ap.add_argument("--gt-dir", type=Path,
                    default=Path(__file__).resolve().parent.parent / "results" / "ground_truth")
    ap.add_argument("--queries", nargs="+", default=ALL_QUERIES)
    ap.add_argument("--slices",  nargs="+", default=ALL_SLICES)
    ap.add_argument("--no-color", action="store_true")
    args = ap.parse_args()
    if args.no_color:
        _USE_COLOR = False

    n_pass = n_warn = n_fail = 0
    bold = (lambda s: f"{_C.BOLD}{s}{_C.RESET}") if _USE_COLOR else (lambda s: s)

    print(f"\n{bold('Snowset Ground Truth Verification')}")
    print(f"  gt_dir  : {args.gt_dir}")
    print(f"  queries : {' '.join(args.queries)}")
    print(f"  slices  : {' '.join(args.slices)}\n")

    for q in args.queries:
        print(bold(f"[{q}]"))
        for sl in args.slices:
            path = args.gt_dir / q / f"{sl}.csv"
            if not path.is_file():
                print(f"  {sl}")
                print(_f("coverage", f"FILE MISSING: {path}"))
                n_fail += 1
                continue
            try:
                df = pd.read_csv(path)
            except Exception as exc:
                print(f"  {sl}")
                print(_f("readable", f"READ ERROR: {exc}"))
                n_fail += 1
                continue
            print(f"  {sl}  ({path.stat().st_size // 1024} KB)")
            for c in check_file(q, df):
                print(c)
                if not c.passed:
                    n_fail += 1
                elif c.is_warn:
                    n_warn += 1
                else:
                    n_pass += 1
        print()

    total = n_pass + n_warn + n_fail
    print(
        f"{bold('Summary:')}  "
        f"{_tag('PASS')} {n_pass}  "
        f"{_tag('WARN')} {n_warn}  "
        f"{_tag('FAIL')} {n_fail}  "
        f"({total} checks)\n"
    )
    sys.exit(1 if n_fail > 0 else 0)


if __name__ == "__main__":
    main()
