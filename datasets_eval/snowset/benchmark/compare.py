from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Callable

BENCH_ROOT = Path(__file__).resolve().parent
if str(BENCH_ROOT) not in sys.path:
    sys.path.insert(0, str(BENCH_ROOT))

import numpy as np
import pandas as pd

# Maps query → regex matching its sketch metric in Prometheus scrape CSVs.
_SKETCH_METRIC_PATTERN: dict[str, str] = {
    "Q1": r"countsketch|countminsketch",
    "Q2": r"_kll",
    "Q3": r"_ddsketch",
    "Q4": r"countsketch|countminsketch",
    "Q5": r"_hll_cardinality",
    "Q6": r"_kll",
}

HEAVY_HITTER_PHI = 0.10   # Q1: warehouse is a heavy hitter if freq >= 10% of fleet total
DEFAULT_ACCURACY_THRESHOLD = 0.5


def _latest_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    wall = pd.to_numeric(df["scrape_wall_ns"], errors="coerce")
    df = df[wall.notna()].copy()
    wall = wall[wall.notna()]
    if df.empty:
        return df
    return df[wall == wall.max()]


def _best_snapshot(sketch: pd.DataFrame, query: str) -> pd.DataFrame:
    pat = _SKETCH_METRIC_PATTERN.get(query)
    if not pat or sketch.empty:
        return _latest_snapshot(sketch)
    rel = sketch[sketch["metric"].astype(str).str.contains(pat, regex=True, na=False)]
    if rel.empty:
        return _latest_snapshot(sketch)
    numeric = pd.to_numeric(rel["value"], errors="coerce").fillna(0)
    nz = rel[numeric > 0]
    pool = nz if not nz.empty else rel
    best = pool.groupby("scrape_wall_ns").size().idxmax()
    return sketch[sketch["scrape_wall_ns"] == best]


def _qval(labels: dict, keys: tuple[str, ...]) -> float | None:
    for k in keys:
        if k not in labels:
            continue
        try:
            return float(labels[k])
        except (TypeError, ValueError):
            continue
    return None


# ── Q1: CMS heavy-hitter frequency accuracy ──────────────────────────────────

def _compare_q1(gt: pd.DataFrame, sketch: pd.DataFrame, **_kw) -> dict:
    if sketch.empty or gt.empty:
        return {
            "metric": "cms_freq_rel_err",
            "value": float("nan"),
            "threshold": DEFAULT_ACCURACY_THRESHOLD,
            "pass": False,
        }

    # GT: sum total_requests per warehouse across last window
    last_w = gt["window_start"].max()
    gt_w = gt[gt["window_start"] == last_w].copy()
    fleet_total = gt_w["total_requests"].sum()
    gt_map = dict(zip(gt_w["warehouseId"].astype(str), gt_w["total_requests"]))

    # Sketch: read CMS estimates
    estimates: dict[str, float] = {}
    for _, row in sketch.iterrows():
        m = str(row.get("metric", ""))
        if "countsketch_partition" not in m and "countminsketch" not in m:
            continue
        try:
            labels = json.loads(row["labels"])
        except Exception:
            continue
        wh = labels.get("warehouse_id") or labels.get("warehouseId")
        if not wh:
            continue
        try:
            estimates[str(wh)] = float(row["value"])
        except (TypeError, ValueError):
            continue

    if not estimates or fleet_total == 0:
        return {
            "metric": "cms_freq_rel_err",
            "value": float("nan"),
            "threshold": DEFAULT_ACCURACY_THRESHOLD,
            "pass": False,
        }

    errs = []
    for wh, true_f in gt_map.items():
        est = estimates.get(wh, 0.0)
        errs.append(abs(est - true_f) / fleet_total)

    rel_err = float(np.mean(errs))
    return {
        "metric": "cms_freq_rel_err",
        "value": rel_err,
        "threshold": DEFAULT_ACCURACY_THRESHOLD,
        "pass": rel_err <= DEFAULT_ACCURACY_THRESHOLD,
    }


# ── Q2: KLL quantile accuracy ─────────────────────────────────────────────────

def _compare_q2(gt: pd.DataFrame, sketch: pd.DataFrame, **_kw) -> dict:
    if sketch.empty or gt.empty:
        return {
            "metric": "kll_p99_rel_err",
            "value": float("nan"),
            "threshold": DEFAULT_ACCURACY_THRESHOLD,
            "pass": False,
        }

    last_w = gt["window_start"].max()
    gt_w = gt[gt["window_start"] == last_w].copy()
    gt_map = dict(zip(gt_w["warehouseId"].astype(str), gt_w["p99_ms"]))

    sk_p99: dict[str, float] = {}
    for _, row in sketch.iterrows():
        m = str(row.get("metric", ""))
        if "_kll" not in m:
            continue
        try:
            labels = json.loads(row["labels"])
        except Exception:
            continue
        q = _qval(labels, ("kll.quantile", "kll_quantile"))
        if q is None or abs(q - 0.99) > 1e-4:
            continue
        wh = labels.get("warehouse_id") or labels.get("warehouseId")
        if not wh:
            continue
        try:
            sk_p99[str(wh)] = float(row["value"])
        except (TypeError, ValueError):
            continue

    if not sk_p99:
        return {
            "metric": "kll_p99_rel_err",
            "value": float("nan"),
            "threshold": DEFAULT_ACCURACY_THRESHOLD,
            "pass": False,
        }

    errs = []
    for wh, true_q in gt_map.items():
        if true_q <= 0:
            continue
        est = sk_p99.get(wh)
        if est is not None:
            errs.append(abs(est - true_q) / true_q)

    if not errs:
        return {
            "metric": "kll_p99_rel_err",
            "value": float("nan"),
            "threshold": DEFAULT_ACCURACY_THRESHOLD,
            "pass": False,
        }

    rel_err = float(np.mean(errs))
    return {
        "metric": "kll_p99_rel_err",
        "value": rel_err,
        "threshold": DEFAULT_ACCURACY_THRESHOLD,
        "pass": rel_err <= DEFAULT_ACCURACY_THRESHOLD,
    }


# ── Q3: DDSketch quantile accuracy ───────────────────────────────────────────

def _compare_q3(gt: pd.DataFrame, sketch: pd.DataFrame, **_kw) -> dict:
    if sketch.empty or gt.empty:
        return {
            "metric": "dds_p99_rel_err",
            "value": float("nan"),
            "threshold": DEFAULT_ACCURACY_THRESHOLD,
            "pass": False,
        }

    last_w = gt["window_start"].max()
    gt_w = gt[gt["window_start"] == last_w].copy()
    gt_map = dict(zip(gt_w["warehouseId"].astype(str), gt_w["p99_bytes"]))

    sk_p99: dict[str, float] = {}
    for _, row in sketch.iterrows():
        m = str(row.get("metric", ""))
        if "_ddsketch" not in m:
            continue
        try:
            labels = json.loads(row["labels"])
        except Exception:
            continue
        q = _qval(labels, ("ddsketch.quantile", "ddsketch_quantile"))
        if q is None or abs(q - 0.99) > 1e-4:
            continue
        wh = labels.get("warehouse_id") or labels.get("warehouseId")
        if not wh:
            continue
        try:
            sk_p99[str(wh)] = float(row["value"])
        except (TypeError, ValueError):
            continue

    if not sk_p99:
        return {
            "metric": "dds_p99_rel_err",
            "value": float("nan"),
            "threshold": DEFAULT_ACCURACY_THRESHOLD,
            "pass": False,
        }

    errs = []
    for wh, true_q in gt_map.items():
        if true_q <= 0:
            continue
        est = sk_p99.get(wh)
        if est is not None:
            errs.append(abs(est - true_q) / true_q)

    if not errs:
        return {
            "metric": "dds_p99_rel_err",
            "value": float("nan"),
            "threshold": DEFAULT_ACCURACY_THRESHOLD,
            "pass": False,
        }

    rel_err = float(np.mean(errs))
    return {
        "metric": "dds_p99_rel_err",
        "value": rel_err,
        "threshold": DEFAULT_ACCURACY_THRESHOLD,
        "pass": rel_err <= DEFAULT_ACCURACY_THRESHOLD,
    }


# ── Q4: CMS archetype share accuracy ─────────────────────────────────────────

def _compare_q4(gt: pd.DataFrame, sketch: pd.DataFrame, **_kw) -> dict:
    if sketch.empty or gt.empty:
        return {
            "metric": "cms_arch_share_err",
            "value": float("nan"),
            "threshold": DEFAULT_ACCURACY_THRESHOLD,
            "pass": False,
        }

    last_w = gt["window_start"].max()
    gt_w = gt[gt["window_start"] == last_w].copy()
    total = gt_w["frequency"].sum()
    gt_shares = {}
    for _, row in gt_w.iterrows():
        key = str(row["query_archetype"])
        gt_shares[key] = row["frequency"] / total if total > 0 else 0.0

    sk_counts: dict[str, float] = {}
    sk_total = 0.0
    for _, row in sketch.iterrows():
        m = str(row.get("metric", ""))
        if "countsketch_partition" not in m and "countminsketch" not in m:
            continue
        try:
            labels = json.loads(row["labels"])
        except Exception:
            continue
        arch = labels.get("query_archetype")
        if not arch:
            continue
        try:
            v = float(row["value"])
        except (TypeError, ValueError):
            continue
        sk_counts[arch] = sk_counts.get(arch, 0.0) + v
        sk_total += v

    if not sk_counts or sk_total == 0:
        return {
            "metric": "cms_arch_share_err",
            "value": float("nan"),
            "threshold": DEFAULT_ACCURACY_THRESHOLD,
            "pass": False,
        }

    errs = []
    for arch, true_s in gt_shares.items():
        est_s = sk_counts.get(arch, 0.0) / sk_total
        errs.append(abs(est_s - true_s))

    err = float(np.mean(errs))
    return {
        "metric": "cms_arch_share_err",
        "value": err,
        "threshold": DEFAULT_ACCURACY_THRESHOLD,
        "pass": err <= DEFAULT_ACCURACY_THRESHOLD,
    }


# ── Q5: HLL distinct warehouse count accuracy ─────────────────────────────────

def _compare_q5(gt: pd.DataFrame, sketch: pd.DataFrame, **_kw) -> dict:
    if sketch.empty or gt.empty:
        return {
            "metric": "hll_cardinality_rel_err",
            "value": float("nan"),
            "threshold": DEFAULT_ACCURACY_THRESHOLD,
            "pass": False,
        }

    last_w = gt["window_start"].max()
    exact = gt.loc[gt["window_start"] == last_w, "exact_distinct_warehouses"].iloc[0]
    if exact <= 0:
        return {
            "metric": "hll_cardinality_rel_err",
            "value": float("nan"),
            "threshold": DEFAULT_ACCURACY_THRESHOLD,
            "pass": False,
        }

    # HLL emits one cardinality row per active series
    hll_count = 0
    for _, row in sketch.iterrows():
        if "_hll_cardinality" not in str(row.get("metric", "")):
            continue
        try:
            v = float(row["value"])
        except (TypeError, ValueError):
            continue
        if v > 0:
            hll_count += 1

    # Fallback: read hll.cardinality attribute if available
    if hll_count == 0:
        for _, row in sketch.iterrows():
            try:
                labels = json.loads(row["labels"])
            except Exception:
                continue
            c = labels.get("hll.cardinality")
            if c is not None:
                try:
                    hll_count = int(float(c))
                    break
                except (TypeError, ValueError):
                    pass

    rel_err = abs(hll_count - exact) / exact
    return {
        "metric": "hll_cardinality_rel_err",
        "value": rel_err,
        "threshold": DEFAULT_ACCURACY_THRESHOLD,
        "pass": rel_err <= DEFAULT_ACCURACY_THRESHOLD,
    }


# ── Q6: KLL p95 latency by concurrency band accuracy ─────────────────────────

def _compare_q6(gt: pd.DataFrame, sketch: pd.DataFrame, **_kw) -> dict:
    if sketch.empty or gt.empty:
        return {
            "metric": "kll_p95_concurrency_band_rel_err",
            "value": float("nan"),
            "threshold": DEFAULT_ACCURACY_THRESHOLD,
            "pass": False,
        }

    last_w = gt["window_start"].max()
    gt_w = gt[gt["window_start"] == last_w].copy()
    gt_map = {
        (str(row["warehouseSize"]), str(row["concurrency_band"])): float(row["p95_ms"])
        for _, row in gt_w.iterrows()
        if float(row["p95_ms"]) > 0
    }

    sk_p95: dict[tuple[str, str], float] = {}
    for _, row in sketch.iterrows():
        m = str(row.get("metric", ""))
        if "_kll" not in m:
            continue
        try:
            labels = json.loads(row["labels"])
        except Exception:
            continue
        q = _qval(labels, ("kll.quantile", "kll_quantile"))
        if q is None or abs(q - 0.95) > 1e-4:
            continue
        ws = labels.get("warehouse_size") or labels.get("warehouseSize")
        band = labels.get("concurrency_band")
        if ws is None or band is None:
            continue
        try:
            sk_p95[(str(ws), str(band))] = float(row["value"])
        except (TypeError, ValueError):
            continue

    if not sk_p95:
        return {
            "metric": "kll_p95_concurrency_band_rel_err",
            "value": float("nan"),
            "threshold": DEFAULT_ACCURACY_THRESHOLD,
            "pass": False,
        }

    errs = []
    for key, true_q in gt_map.items():
        est = sk_p95.get(key)
        if est is not None:
            errs.append(abs(est - true_q) / true_q)

    if not errs:
        return {
            "metric": "kll_p95_concurrency_band_rel_err",
            "value": float("nan"),
            "threshold": DEFAULT_ACCURACY_THRESHOLD,
            "pass": False,
        }

    rel_err = float(np.mean(errs))
    return {
        "metric": "kll_p95_concurrency_band_rel_err",
        "value": rel_err,
        "threshold": DEFAULT_ACCURACY_THRESHOLD,
        "pass": rel_err <= DEFAULT_ACCURACY_THRESHOLD,
    }


_COMPARE_DISPATCH: dict[str, Callable] = {
    "Q1": _compare_q1,
    "Q2": _compare_q2,
    "Q3": _compare_q3,
    "Q4": _compare_q4,
    "Q5": _compare_q5,
    "Q6": _compare_q6,
}


def run_comparison(
    query: str,
    slice_tag: str,
    gt_dir: Path,
    sketch_dir: Path,
    out_dir: Path,
) -> None:
    gt_path = gt_dir / query / f"{slice_tag}.csv"
    sketch_path = sketch_dir / query / f"{slice_tag}.csv"
    if not gt_path.is_file():
        print(f"GT file missing: {gt_path}", file=sys.stderr)
        return
    gt = pd.read_csv(gt_path)
    sketch = (pd.read_csv(sketch_path, on_bad_lines="skip", low_memory=False)
              if sketch_path.is_file() else pd.DataFrame())
    out_dir.mkdir(parents=True, exist_ok=True)
    fn = _COMPARE_DISPATCH.get(query)
    if fn is None:
        return
    snapshot = _best_snapshot(sketch, query)
    result = fn(gt, snapshot)
    pd.DataFrame([{"query": query, "slice": slice_tag, **result}]).to_csv(
        out_dir / f"{query}_{slice_tag}.csv", index=False
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare sketch scrapes to ground truth.")
    parser.add_argument("--query", default="Q1")
    parser.add_argument("--slice", default="full")
    parser.add_argument("--gt-dir", type=Path,
                        default=Path(__file__).resolve().parent / "results" / "ground_truth")
    parser.add_argument("--sketch-dir", type=Path,
                        default=Path(__file__).resolve().parent / "results" / "sketch_output")
    parser.add_argument("--out-dir", type=Path,
                        default=Path(__file__).resolve().parent / "results" / "comparison")
    args = parser.parse_args()
    run_comparison(args.query, args.slice, args.gt_dir, args.sketch_dir, args.out_dir)


if __name__ == "__main__":
    main()
