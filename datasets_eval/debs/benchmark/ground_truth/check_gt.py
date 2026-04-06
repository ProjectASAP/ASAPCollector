#!/usr/bin/env python3
"""
ground_truth/check_gt.py — Sanity-check DEBS 2022 ground truth files.

Checks performed per (query, day):
  - File coverage: CSV exists and is readable
  - Required columns: all expected columns are present
  - Non-empty: at least 1 data row
  - No NaN values in numeric columns
  - Per-query invariants (see details below per query)

Special cross-query checks:
  - Q2 crossovers must match EMA sign-flips from Q1 (sampled over first 50 symbols)
  - Q6 timestamp sanity: flags garbage windows with pre-2020 timestamps
    (these come from malformed rows in the full-feed CSVs)

Usage:
  cd datasets_eval/debs/benchmark
  python3 ground_truth/check_gt.py
  python3 ground_truth/check_gt.py --gt-dir results/ground_truth
  python3 ground_truth/check_gt.py --queries Q1 Q2 Q6 --days 08-11-21
  python3 ground_truth/check_gt.py --no-color
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Valid epoch-ms range for DEBS 2022 window timestamps (Nov 2021 trading days).
# Anything outside [MIN, MAX] is a garbage window from malformed rows in the CSV.
#   1_630_000_000_000 ≈ 2021-08-28 UTC  (a few months before the dataset)
#   1_640_000_000_000 ≈ 2021-12-19 UTC  (a few months after the dataset)
TRADING_TS_MIN_MS = 1_630_000_000_000
TRADING_TS_MAX_MS = 1_640_000_000_000

ALL_DAYS = ["08-11-21", "09-11-21", "10-11-21", "11-11-21", "12-11-21"]
ALL_QUERIES = [f"Q{i}" for i in range(1, 9)]

# Required columns per query
REQUIRED_COLS: dict[str, list[str]] = {
    "Q1":  ["symbol", "window_start_ms", "ema38", "ema100"],
    "Q2":  ["symbol", "window_start_ms", "signal"],
    "Q3":  ["window_start_ms", "rank", "symbol", "metric", "value"],
    "Q4":  ["symbol", "window_start_ms", "min_v", "max_v", "last_v", "range_v"],
    "Q5":  ["symbol", "window_start_ms", "sigma"],
    "Q6":  ["window_start_ms", "distinct"],
    "Q7":  ["symbol", "window_start_ms", "mean"],
    "Q8":  ["symbol", "window_start_ms", "mu", "sigma", "flag_count", "tick_count"],
}

# Columns whose dtype is not numeric / should be excluded from NaN numeric check
NON_NUMERIC = {"symbol", "exchange", "signal", "metric", "sectype"}


# ── colour helpers ────────────────────────────────────────────────────────────

class _Colors:
    GREEN  = "\033[32m"
    RED    = "\033[31m"
    YELLOW = "\033[33m"
    BOLD   = "\033[1m"
    RESET  = "\033[0m"

_C = _Colors()
_USE_COLOR = True


def _tag(status: str) -> str:
    if not _USE_COLOR:
        return f"{status:<4}"
    colors = {"PASS": _C.GREEN, "WARN": _C.YELLOW, "FAIL": _C.RED}
    return f"{colors.get(status, '')}{status:<4}{_C.RESET}"


def _bold(s: str) -> str:
    return f"{_C.BOLD}{s}{_C.RESET}" if _USE_COLOR else s


# ── check result ──────────────────────────────────────────────────────────────

class Check:
    def __init__(self, status: str, label: str, detail: str = ""):
        assert status in ("PASS", "WARN", "FAIL")
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
        detail_str = f"  {self.detail}" if self.detail else ""
        return f"    {_tag(self.status)}  {self.label:<44}{detail_str}"


def _pass(label: str, detail: str = "") -> Check:
    return Check("PASS", label, detail)

def _warn(label: str, detail: str = "") -> Check:
    return Check("WARN", label, detail)

def _fail(label: str, detail: str = "") -> Check:
    return Check("FAIL", label, detail)


# ── per-query checkers ────────────────────────────────────────────────────────

def _check_q1(df: pd.DataFrame) -> list[Check]:
    out: list[Check] = []

    # Prices can legitimately be 0.0 in the raw data; only flag strictly negative.
    neg = ((df["ema38"] < 0) | (df["ema100"] < 0)).sum()
    out.append(_pass("ema_nonneg") if neg == 0
               else _fail("ema_nonneg", f"{neg} negative EMA values"))

    n_sym = df["symbol"].nunique()
    sym_chk = _pass if 4_000 <= n_sym <= 6_000 else _warn
    out.append(sym_chk("symbol_count", f"{n_sym} unique symbols (expect ~5 178)"))

    return out


def _check_q2(q2: pd.DataFrame, q1: pd.DataFrame | None) -> list[Check]:
    out: list[Check] = []

    bad_sig = set(q2["signal"].dropna().unique()) - {"bullish", "bearish"}
    out.append(_pass("valid_signals") if not bad_sig
               else _fail("valid_signals", f"unknown values: {bad_sig}"))

    if q1 is None:
        out.append(_warn("q2_vs_q1", "Q1 file not available — crossover check skipped"))
        return out

    syms = q1["symbol"].unique()[:50]  # sample first 50 symbols
    checked = misses = 0
    for sym in syms:
        g = q1[q1["symbol"] == sym].sort_values("window_start_ms")
        if len(g) < 2:
            continue
        e38  = g["ema38"].values
        e100 = g["ema100"].values
        ws   = g["window_start_ms"].values
        for i in range(1, len(g)):
            d_prev = float(e38[i - 1]) - float(e100[i - 1])
            d_cur  = float(e38[i])     - float(e100[i])
            sig = None
            if d_prev <= 0 < d_cur:
                sig = "bullish"
            elif d_prev >= 0 > d_cur:
                sig = "bearish"
            if sig is None:
                continue
            checked += 1
            found = (
                (q2["symbol"] == sym)
                & (q2["window_start_ms"] == ws[i])
                & (q2["signal"] == sig)
            ).any()
            if not found:
                misses += 1

    if checked == 0:
        out.append(_warn("q2_vs_q1", "no crossovers found in sampled symbols"))
    else:
        pct = 100.0 * (checked - misses) / checked
        label = f"{checked - misses}/{checked} matched ({pct:.0f}%)"
        out.append(_pass("q2_vs_q1", label) if misses == 0
                   else (_warn("q2_vs_q1", label) if pct >= 95
                         else _fail("q2_vs_q1", label)))
    return out


def _check_q3(df: pd.DataFrame) -> list[Check]:
    out: list[Check] = []

    bad_m = set(df["metric"].dropna().unique()) - {"count", "range"}
    out.append(_pass("valid_metrics") if not bad_m
               else _fail("valid_metrics", f"unknown: {bad_m}"))

    bad_r = (~df["rank"].between(1, 10)).sum()
    out.append(_pass("rank_1_to_10") if bad_r == 0
               else _fail("rank_1_to_10", f"{bad_r} rows outside [1,10]"))

    neg_v = (df["value"] < 0).sum()
    out.append(_pass("values_nonneg") if neg_v == 0
               else _fail("values_nonneg", f"{neg_v} negative values"))

    # Q3 also uses load_full_day — check for the same column-mismatch symptom.
    garbage = (
        (df["window_start_ms"] < TRADING_TS_MIN_MS)
        | (df["window_start_ms"] > TRADING_TS_MAX_MS)
    ).sum()
    valid_count = len(df) - garbage
    if valid_count == 0:
        out.append(_fail(
            "valid_windows_exist",
            "ZERO windows in Nov 2021 range — same load_full_day() column-mismatch "
            "issue as Q6. Fix full-feed CSV loading before using Q3 ground truth.",
        ))
    elif garbage > 0:
        out.append(_warn("no_garbage_windows",
                         f"{garbage} out-of-range windows (valid: {valid_count})"))
    else:
        out.append(_pass("no_garbage_windows"))

    return out


def _check_q4(df: pd.DataFrame) -> list[Check]:
    out: list[Check] = []

    bad_mm = (df["min_v"] > df["max_v"]).sum()
    out.append(_pass("min_le_max") if bad_mm == 0
               else _fail("min_le_max", f"{bad_mm} rows where min > max"))

    diff = (df["range_v"] - (df["max_v"] - df["min_v"])).abs()
    bad_rng = (diff > 1e-6).sum()
    out.append(_pass("range_eq_max_minus_min") if bad_rng == 0
               else _fail("range_eq_max_minus_min", f"{bad_rng} inconsistent rows"))

    neg_p = ((df["min_v"] < 0) | (df["max_v"] < 0)).sum()
    out.append(_pass("prices_nonneg") if neg_p == 0
               else _warn("prices_nonneg", f"{neg_p} rows with negative price"))

    return out


def _check_q5(df: pd.DataFrame) -> list[Check]:
    out: list[Check] = []

    neg_s = (df["sigma"] < 0).sum()
    out.append(_pass("sigma_nonneg") if neg_s == 0
               else _fail("sigma_nonneg", f"{neg_s} negative sigma"))

    n_sym = df["symbol"].nunique()
    out.append(_pass("symbol_count", f"{n_sym} symbols") if n_sym > 1_000
               else _warn("symbol_count", f"only {n_sym} symbols"))

    return out


def _check_q6(df: pd.DataFrame) -> list[Check]:
    out: list[Check] = []

    # Two-sided bound: the DEBS 2022 dataset covers Nov 2021 trading days.
    # Rows outside this range come from malformed Date/Time fields in the full-feed
    # CSV (some parse as 1970s timestamps, others as year 2026-2075 timestamps).
    garbage = (
        (df["window_start_ms"] < TRADING_TS_MIN_MS)
        | (df["window_start_ms"] > TRADING_TS_MAX_MS)
    ).sum()
    if garbage > 0:
        out.append(_warn(
            "no_garbage_windows",
            f"{garbage} out-of-range windows (expected Nov 2021, "
            f"got timestamps outside [{TRADING_TS_MIN_MS}, {TRADING_TS_MAX_MS}]) — "
            "filter to valid range before comparing with sketch output",
        ))
    else:
        out.append(_pass("no_garbage_windows"))

    valid = df[
        (df["window_start_ms"] >= TRADING_TS_MIN_MS)
        & (df["window_start_ms"] <= TRADING_TS_MAX_MS)
    ]
    if not valid.empty:
        low  = (valid["distinct"] < 100).sum()
        mean = valid["distinct"].mean()
        out.append(
            _pass("distinct_gte_100", f"mean distinct={mean:.0f}") if low == 0
            else _warn("distinct_gte_100",
                       f"{low} windows with distinct < 100 (mean={mean:.0f})"),
        )
    else:
        out.append(_fail(
            "valid_windows_exist",
            "ZERO windows fall in the Nov 2021 range — ground truth is unusable. "
            "Likely cause: load_full_day() misreads the full-feed CSV columns "
            "(symbol='E' = SecType, not the stock ID). "
            "Inspect the CSV encoding/column order and fix load_full_day().",
        ))

    return out


def _check_q7(df: pd.DataFrame) -> list[Check]:
    # Some symbols have price 0.0 in the raw data; mean of 0.0 is valid.
    neg = (df["mean"] < 0).sum()
    return [
        _pass("mean_nonneg") if neg == 0
        else _fail("mean_nonneg", f"{neg} negative mean values"),
    ]


def _check_q8(df: pd.DataFrame) -> list[Check]:
    out: list[Check] = []

    neg_s = (df["sigma"] < 0).sum()
    out.append(_pass("sigma_nonneg") if neg_s == 0
               else _fail("sigma_nonneg", f"{neg_s} rows"))

    neg_f = (df["flag_count"] < 0).sum()
    out.append(_pass("flag_count_ge_0") if neg_f == 0
               else _fail("flag_count_ge_0", f"{neg_f} rows"))

    low_t = (df["tick_count"] < 2).sum()
    out.append(_pass("tick_count_ge_2") if low_t == 0
               else _warn("tick_count_ge_2", f"{low_t} rows with tick_count < 2"))

    overflow = (df["flag_count"] > df["tick_count"]).sum()
    out.append(_pass("flag_le_tick_count") if overflow == 0
               else _fail("flag_le_tick_count", f"{overflow} rows where flag_count > tick_count"))

    return out


_QUERY_CHECKER = {
    "Q1":  _check_q1,
    "Q3":  _check_q3,
    "Q4":  _check_q4,
    "Q5":  _check_q5,
    "Q6":  _check_q6,
    "Q7":  _check_q7,
    "Q8":  _check_q8,
}


# ── file-level checks ─────────────────────────────────────────────────────────

def _base_checks(df: pd.DataFrame, required_cols: list[str]) -> list[Check]:
    """Column presence, non-empty, no NaN in numeric columns."""
    out: list[Check] = []

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        out.append(_fail("required_columns", f"missing: {missing}"))
        return out  # can't run further checks without columns
    out.append(_pass("required_columns"))

    if df.empty:
        out.append(_fail("non_empty", "0 rows — cannot proceed"))
        return out
    out.append(_pass("non_empty", f"{len(df):,} rows"))

    num_cols = [c for c in required_cols if c not in NON_NUMERIC and pd.api.types.is_numeric_dtype(df[c])]
    if num_cols:
        nan_total = int(df[num_cols].isnull().sum().sum())
        out.append(_pass("no_nulls_in_numeric") if nan_total == 0
                   else _warn("no_nulls_in_numeric", f"{nan_total} NaN values across {num_cols}"))

    return out


def check_file(
    q: str,
    day: str,
    df: pd.DataFrame,
    q1_df: pd.DataFrame | None = None,
) -> list[Check]:
    required = REQUIRED_COLS.get(q, [])
    checks = _base_checks(df, required)

    if any(c.status == "FAIL" for c in checks):
        return checks  # base checks already failed; no point continuing

    if q == "Q2":
        checks.extend(_check_q2(df, q1_df))
    elif q in _QUERY_CHECKER:
        checks.extend(_QUERY_CHECKER[q](df))

    return checks


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    global _USE_COLOR  # noqa: PLW0603

    ap = argparse.ArgumentParser(
        description="Sanity-check DEBS 2022 ground truth files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument(
        "--gt-dir",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "results" / "ground_truth",
        help="Root directory of ground truth files (default: ../results/ground_truth)",
    )
    ap.add_argument(
        "--queries",
        nargs="+",
        default=ALL_QUERIES,
        metavar="Q",
        help="Queries to check (default: Q1..Q8).",
    )
    ap.add_argument("--days", nargs="+", default=ALL_DAYS, metavar="D",
                    help="Trading days to check (default: all 5)")
    ap.add_argument("--no-color", action="store_true",
                    help="Disable ANSI colour output")
    args = ap.parse_args()

    if args.no_color:
        _USE_COLOR = False

    n_pass = n_warn = n_fail = 0

    print(f"\n{_bold('DEBS 2022 Ground Truth Verification')}")
    print(f"  gt_dir  : {args.gt_dir}")
    print(f"  queries : {' '.join(args.queries)}")
    print(f"  days    : {' '.join(args.days)}\n")

    for q in args.queries:
        print(_bold(f"[{q}]"))
        for day in args.days:
            path = args.gt_dir / q / f"{day}.csv"

            # ── file existence ──────────────────────────────────────────────
            if not path.is_file():
                chk = _fail("coverage", f"FILE MISSING: {path}")
                print(f"  {day}")
                print(chk)
                n_fail += 1
                continue

            # ── load ────────────────────────────────────────────────────────
            try:
                df = pd.read_csv(path)
            except Exception as exc:
                chk = _fail("readable", f"READ ERROR: {exc}")
                print(f"  {day}")
                print(chk)
                n_fail += 1
                continue

            # ── load Q1 for Q2 cross-validation ────────────────────────────
            q1_df: pd.DataFrame | None = None
            if q == "Q2":
                q1_path = args.gt_dir / "Q1" / f"{day}.csv"
                if q1_path.is_file():
                    try:
                        q1_df = pd.read_csv(q1_path)
                    except Exception:
                        pass

            # ── run checks ─────────────────────────────────────────────────
            size_kb = path.stat().st_size // 1024
            print(f"  {day}  ({size_kb} KB)")
            checks = check_file(q, day, df, q1_df)
            for c in checks:
                print(c)
                if not c.passed:
                    n_fail += 1
                elif c.is_warn:
                    n_warn += 1
                else:
                    n_pass += 1

        print()

    # ── summary ───────────────────────────────────────────────────────────────
    total = n_pass + n_warn + n_fail
    print(
        f"{_bold('Summary:')}  "
        f"{_tag('PASS')} {n_pass}  "
        f"{_tag('WARN')} {n_warn}  "
        f"{_tag('FAIL')} {n_fail}  "
        f"({total} checks total)\n"
    )

    sys.exit(1 if n_fail > 0 else 0)


if __name__ == "__main__":
    main()
