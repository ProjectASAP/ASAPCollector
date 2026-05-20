#!/usr/bin/env python3
"""
Part 4 — Advanced PromQL Verification with manual calculation cross-check.

Tests: rate(), avg(), avg_over_time(), quantile(), quantile_over_time() for
both p50 and p95 against hand-computed expected values derived from raw data.

Configuration assumed (matches topology.env defaults):
  EXPORTER_FIXED_LATENCY = 42.0    (gauge always 42.0)
  EXPORTER_FREQ_HZ       = 1       (counter increments 1/s)
  N_PRODUCERS_PER_NODE   = 1       (p-a-1 on node0, p-b-1 on node3)
  PER_AGENT_CARDINALITY  = 4       (zones z0-z3)
"""

import math
import sys
import time
import requests

THANOS = "http://10.10.1.3:10903"
COUNTER_METRIC = "http_requests_total"
GAUGE_METRIC   = "http_requests_total_latency_ms"

# Pinned series for deterministic manual calculations
SERIES_PA_Z0 = '{producer_id="p-a-1",zone="z0"}'
SERIES_PB_Z0 = '{producer_id="p-b-1",zone="z0"}'

FIXED_LATENCY = 42.0   # EXPORTER_FIXED_LATENCY
FREQ_HZ       = 1.0    # EXPORTER_FREQ_HZ
N_PRODUCERS   = 1      # per node → 2 total (p-a-1, p-b-1)
N_ZONES       = 4      # z0-z3
TOL_REL       = 0.15   # 15 % relative tolerance for rate (extrapolation wiggle)
TOL_ABS       = 1e-6   # absolute tolerance for gauge checks

PASS = "[PASS]"
FAIL = "[FAIL]"

results = []


# ─── helpers ────────────────────────────────────────────────────────────────

def instant_query(q, t=None):
    params = {"query": q}
    if t is not None:
        params["time"] = t
    r = requests.get(f"{THANOS}/api/v1/query", params=params, timeout=15)
    r.raise_for_status()
    d = r.json()
    assert d["status"] == "success", d
    return d["data"]["result"]


def range_query(q, start, end, step="15s"):
    params = {"query": q, "start": start, "end": end, "step": step}
    r = requests.get(f"{THANOS}/api/v1/query_range", params=params, timeout=15)
    r.raise_for_status()
    d = r.json()
    assert d["status"] == "success", d
    return d["data"]["result"]


def check(tag, got, expected, tol_rel=None, tol_abs=None):
    """Print a check line and accumulate pass/fail."""
    if tol_abs is not None:
        ok = abs(got - expected) <= tol_abs
    elif tol_rel is not None:
        ok = abs(got - expected) <= tol_rel * expected if expected != 0 else abs(got) < 1e-9
    else:
        ok = (got == expected)
    status = PASS if ok else FAIL
    results.append(ok)
    if tol_rel:
        print(f"  {status} {tag}: got={got:.6f}  expected≈{expected:.6f}  "
              f"tol={tol_rel*100:.0f}%  diff={abs(got-expected):.6f}")
    else:
        print(f"  {status} {tag}: got={got:.6f}  expected={expected:.6f}")
    return ok


# ─── Prometheus rate() implementation (for cross-check) ─────────────────────

def prometheus_rate(samples, range_secs, eval_time):
    """
    Exact reproduction of Prometheus extrapolatedRate() (isCounter=True, isRate=True).

    Algorithm from prometheus/prometheus/promql/functions.go:
      resultFloat = v_last - v_first
      sampledInterval = t_last - t_first
      averageDurationBetweenSamples = sampledInterval / (N - 1)
      extrapolationThreshold = averageDurationBetweenSamples * 1.1
      extrapolateToInterval = sampledInterval
        + (durationToStart if durationToStart < threshold else avg/2)
        + (durationToEnd   if durationToEnd   < threshold else avg/2)
      factor = extrapolateToInterval / sampledInterval / range_secs   (isRate)
      result = resultFloat * factor

    samples   : list of (timestamp_float, value_float) sorted ascending
    range_secs: the [Xs] window (e.g. 120 for [2m])
    eval_time : the evaluation timestamp (float unix seconds)
    """
    if len(samples) < 2:
        return None

    t_first, v_first = samples[0]
    t_last,  v_last  = samples[-1]

    result_float             = v_last - v_first      # no reset handling needed (counter never resets)
    sampled_interval         = t_last - t_first
    avg_step                 = sampled_interval / (len(samples) - 1)
    duration_to_start        = t_first - (eval_time - range_secs)
    duration_to_end          = eval_time - t_last
    extrapolation_threshold  = avg_step * 1.1

    extrapolate_to_interval = sampled_interval
    extrapolate_to_interval += duration_to_start if duration_to_start < extrapolation_threshold else avg_step / 2
    extrapolate_to_interval += duration_to_end   if duration_to_end   < extrapolation_threshold else avg_step / 2

    factor = extrapolate_to_interval / sampled_interval / range_secs
    return result_float * factor


# ─── Part 4-A  Manual rate vs Thanos rate() ─────────────────────────────────

def check_rate():
    print()
    print("=" * 68)
    print("PART 4-A — rate() manual calculation cross-check (exact match)")
    print("=" * 68)
    RANGE_S = 120   # [2m] window

    # ── Step 1: Query Thanos rate() and choose eval_time ─────────────────────
    # We first let Thanos pick its own eval_time so we can use the SAME eval_time
    # in both the manual formula and the Thanos query.
    eval_time = math.floor(time.time())

    thanos_q = f"rate({COUNTER_METRIC}{SERIES_PA_Z0}[{RANGE_S}s])"
    thanos_res = instant_query(thanos_q, t=eval_time)
    if not thanos_res:
        print(f"  {FAIL} Thanos returned no result for: {thanos_q}")
        results.append(False)
        return
    thanos_rate_val = float(thanos_res[0]["value"][1])
    print(f"\n  Thanos query:  {thanos_q}")
    print(f"  eval_time:     t={eval_time}  ({time.strftime('%H:%M:%S', time.gmtime(eval_time))} UTC)")
    print(f"  Thanos result: {thanos_rate_val:.9f} req/s")

    # ── Step 2: Retrieve the EXACT raw samples Thanos used ───────────────────
    # Prometheus instant query with a range selector (matrix selector) returns
    # the actual TSDB chunk samples with their real sub-second timestamps —
    # the same data Thanos's extrapolatedRate() operates on internally.
    matrix_q = f"{COUNTER_METRIC}{SERIES_PA_Z0}[{RANGE_S}s]"
    raw_res = instant_query(matrix_q, t=eval_time)
    if not raw_res:
        print(f"  {FAIL} No matrix data for: {matrix_q}")
        results.append(False)
        return

    # The result type is "matrix"; values are [[timestamp, value], ...]
    raw_samples = raw_res[0]["values"]
    in_window = [(float(t), float(v)) for t, v in raw_samples]

    if len(in_window) < 2:
        print(f"  {FAIL} Only {len(in_window)} sample(s) in window — not enough")
        results.append(False)
        return

    t_first, v_first = in_window[0]
    t_last,  v_last  = in_window[-1]

    print(f"\n  Raw TSDB samples inside [{RANGE_S}s] window (matrix instant query):")
    print(f"    total samples: {len(in_window)}")
    # Show first 5 and last 5
    for ts, val in in_window[:5]:
        print(f"    t={ts:.3f}  v={val:.1f}")
    if len(in_window) > 10:
        print(f"    ... ({len(in_window)-10} points omitted) ...")
    for ts, val in in_window[-5:]:
        print(f"    t={ts:.3f}  v={val:.1f}")
    print(f"\n  First sample: t={t_first:.3f}  v={v_first:.1f}")
    print(f"  Last  sample: t={t_last:.3f}  v={v_last:.1f}")

    # ── Step 3: Manual calculation ───────────────────────────────────────────
    delta_v    = v_last - v_first
    delta_t    = t_last - t_first

    print()
    print("  Manual rate calculation (step-by-step):")
    print(f"    1. counter_increase = v_last - v_first")
    print(f"                        = {v_last:.1f} - {v_first:.1f} = {delta_v:.1f}")
    print(f"    2. sampled_interval = t_last - t_first")
    print(f"                        = {t_last:.3f} - {t_first:.3f} = {delta_t:.6f}s")
    naive_rate = delta_v / delta_t
    print(f"    3. naive rate       = {delta_v:.1f} / {delta_t:.6f} = {naive_rate:.9f} req/s")

    # ── Step 4: Prometheus extrapolation ─────────────────────────────────────
    # Exact algorithm from prometheus/prometheus/promql/functions.go extrapolatedRate()
    N              = len(in_window)
    avg_step       = delta_t / (N - 1)
    dur_to_start   = t_first - (eval_time - RANGE_S)   # gap: window_start → first sample
    dur_to_end     = eval_time - t_last                 # gap: last sample → window_end
    threshold      = avg_step * 1.1
    extrap_start   = dur_to_start if dur_to_start < threshold else avg_step / 2
    extrap_end     = dur_to_end   if dur_to_end   < threshold else avg_step / 2
    extrap_interval = delta_t + extrap_start + extrap_end

    manual_rate = prometheus_rate(in_window, RANGE_S, eval_time)

    print(f"\n  Prometheus extrapolation (extrapolatedRate, promql/functions.go):")
    print(f"    N samples in window          = {N}")
    print(f"    avg_step                     = {delta_t:.6f} / ({N}-1) = {avg_step:.6f}s")
    print(f"    window_start                 = {eval_time} - {RANGE_S} = {eval_time - RANGE_S:.3f}")
    print(f"    duration_to_start            = {t_first:.3f} - {eval_time - RANGE_S:.3f} = {dur_to_start:.6f}s")
    print(f"    duration_to_end              = {eval_time:.3f} - {t_last:.3f} = {dur_to_end:.6f}s")
    print(f"    extrapolation_threshold      = {avg_step:.6f} × 1.1 = {threshold:.6f}s")
    if dur_to_start < threshold:
        print(f"    extrap_start: {dur_to_start:.6f}s < {threshold:.6f}s → add {extrap_start:.6f}s")
    else:
        print(f"    extrap_start: {dur_to_start:.6f}s ≥ {threshold:.6f}s → add avg/2 = {extrap_start:.6f}s")
    if dur_to_end < threshold:
        print(f"    extrap_end:   {dur_to_end:.6f}s < {threshold:.6f}s → add {extrap_end:.6f}s")
    else:
        print(f"    extrap_end:   {dur_to_end:.6f}s ≥ {threshold:.6f}s → add avg/2 = {extrap_end:.6f}s")
    print(f"    extrapolated_interval        = {delta_t:.6f} + {extrap_start:.6f} + {extrap_end:.6f}")
    print(f"                                 = {extrap_interval:.6f}s")
    factor = extrap_interval / delta_t / RANGE_S
    print(f"    factor = extrap / sampled / range")
    print(f"           = {extrap_interval:.6f} / {delta_t:.6f} / {RANGE_S} = {factor:.9f}")
    print(f"\n    manual rate = counter_increase × factor")
    print(f"                = {delta_v:.1f} × {factor:.9f}")
    print(f"                = {manual_rate:.9f} req/s")

    # ── Step 5: Compare ───────────────────────────────────────────────────────
    print(f"\n  Thanos rate():  {thanos_rate_val:.9f} req/s")
    print(f"  Manual formula: {manual_rate:.9f} req/s")
    abs_diff = abs(manual_rate - thanos_rate_val)
    rel_diff = abs_diff / thanos_rate_val if thanos_rate_val != 0 else float("inf")
    print(f"  Absolute diff:  {abs_diff:.2e} req/s")
    print(f"  Relative diff:  {rel_diff*100:.4f}%")

    print()
    print("  Cross-checks:")
    check("Manual formula == Thanos rate (≤0.01%)", manual_rate, thanos_rate_val, tol_rel=0.0001)

    lag_frac = 1.0 - (thanos_rate_val / FREQ_HZ)
    lag_secs = lag_frac * RANGE_S
    print(f"  INFO: rate={thanos_rate_val:.6f} req/s vs FREQ_HZ={FREQ_HZ};"
          f" freshness lag ≈ {lag_secs:.0f}s ({lag_frac*100:.0f}% of [{RANGE_S}s] window)")

    # ── sum by (zone) rate ───────────────────────────────────────────────────
    print()
    print("  sum by (zone) rate (expected: ~2× per-series rate, same freshness lag):")
    sum_q = f"sum by (zone)(rate({COUNTER_METRIC}[{RANGE_S}s]))"
    sum_res = instant_query(sum_q)
    per_series_expected = thanos_rate_val
    for r in sum_res:
        zone = r["metric"].get("zone", "?")
        val  = float(r["value"][1])
        check(f"sum rate zone={zone} ~ 2×per-series", val, 2 * per_series_expected, tol_rel=0.10)

    # ── avg by (zone) rate ───────────────────────────────────────────────────
    print()
    print("  avg by (zone) rate (expected: ~per-series rate):")
    avg_q = f"avg by (zone)(rate({COUNTER_METRIC}[{RANGE_S}s]))"
    avg_res = instant_query(avg_q)
    for r in avg_res:
        zone = r["metric"].get("zone", "?")
        val  = float(r["value"][1])
        check(f"avg rate zone={zone} ~ per-series", val, per_series_expected, tol_rel=0.10)


# ─── Part 4-B  avg_over_time() manual calculation ───────────────────────────

def check_avg():
    print()
    print("=" * 68)
    print("PART 4-B — avg_over_time() manual calculation cross-check")
    print("=" * 68)
    RANGE_S = 120

    now   = time.time()
    start = now - 240
    end   = now

    series_q = f"{GAUGE_METRIC}{SERIES_PA_Z0}"
    raw = range_query(series_q, start=start, end=end, step="1s")
    if not raw:
        print(f"  {FAIL} No raw data for {series_q}")
        results.append(False)
        return

    samples = [(float(t), float(v)) for t, v in raw[0]["values"]]
    eval_time    = samples[-1][0]
    window_start = eval_time - RANGE_S
    in_window    = [(t, v) for t, v in samples if t >= window_start]

    print(f"\n  Gauge samples in [{RANGE_S}s] window ({len(in_window)} points at 1s step):")
    for ts, val in in_window[:4]:
        print(f"    t={ts:.0f}  v={val:.1f}")
    if len(in_window) > 8:
        print(f"    ... ({len(in_window)-8} points omitted, all v=42.0) ...")
    for ts, val in in_window[-4:]:
        print(f"    t={ts:.0f}  v={val:.1f}")

    vals_in_window = [v for _, v in in_window]
    manual_avg = sum(vals_in_window) / len(vals_in_window)
    print(f"\n  Manual avg_over_time = sum({len(vals_in_window)} samples, all={vals_in_window[0]:.1f}) / {len(vals_in_window)}")
    print(f"                       = {sum(vals_in_window):.1f} / {len(vals_in_window)}")
    print(f"                       = {manual_avg:.6f}")
    print(f"  Expected (FIXED_LATENCY=42.0): 42.0")

    avg_q = f"avg_over_time({GAUGE_METRIC}{SERIES_PA_Z0}[{RANGE_S}s])"
    thanos_res = instant_query(avg_q, t=eval_time)
    if not thanos_res:
        print(f"  {FAIL} Thanos returned nothing for: {avg_q}")
        results.append(False)
        return
    thanos_avg = float(thanos_res[0]["value"][1])

    print(f"\n  Thanos query: {avg_q}")
    print(f"  Thanos avg_over_time: {thanos_avg:.6f}")

    print()
    print("  Cross-checks:")
    check("manual avg == 42.0", manual_avg, FIXED_LATENCY, tol_abs=TOL_ABS)
    check("Thanos avg_over_time == 42.0", thanos_avg, FIXED_LATENCY, tol_abs=TOL_ABS)
    check("Thanos == manual", thanos_avg, manual_avg, tol_abs=TOL_ABS)

    # avg across all series (not over time)
    print()
    print("  avg across all series check (expected: 42.0 — all series fixed):")
    avg_all_q = f"avg({GAUGE_METRIC})"
    r2 = instant_query(avg_all_q)
    if r2:
        val = float(r2[0]["value"][1])
        check("avg() across all series", val, FIXED_LATENCY, tol_abs=TOL_ABS)
    else:
        print(f"  {FAIL} No result for {avg_all_q}")
        results.append(False)

    print()
    print("  avg by (zone) check (expected: 42.0 per zone):")
    avgz_q = f"avg by (zone)({GAUGE_METRIC})"
    rz = instant_query(avgz_q)
    for r in rz:
        zone = r["metric"].get("zone", "?")
        val  = float(r["value"][1])
        check(f"avg zone={zone}", val, FIXED_LATENCY, tol_abs=TOL_ABS)


# ─── Part 4-C  quantile_over_time() p50 / p95 manual calculation ────────────

def check_quantile():
    print()
    print("=" * 68)
    print("PART 4-C — quantile_over_time(p50/p95) manual calculation cross-check")
    print("=" * 68)
    RANGE_S = 120

    now   = time.time()
    start = now - 240
    end   = now

    series_q = f"{GAUGE_METRIC}{SERIES_PA_Z0}"
    raw = range_query(series_q, start=start, end=end, step="1s")
    if not raw:
        print(f"  {FAIL} No raw data for {series_q}")
        results.append(False)
        return

    samples  = [(float(t), float(v)) for t, v in raw[0]["values"]]
    eval_time    = samples[-1][0]
    window_start = eval_time - RANGE_S
    in_window    = [v for t, v in samples if t >= window_start]

    print(f"\n  {len(in_window)} gauge values in [{RANGE_S}s] window (all={in_window[0] if in_window else 'n/a'} — fixed latency)")
    sorted_vals = sorted(in_window)
    N = len(sorted_vals)
    print(f"  Sorted: [{sorted_vals[0]}, {sorted_vals[0]}, ... × {N}] (all identical)")

    def manual_quantile(phi, vals):
        """Prometheus quantile_over_time uses rank = ceil(phi*N) - 1 (0-based)."""
        rank = math.ceil(phi * len(vals)) - 1
        rank = max(0, min(rank, len(vals) - 1))
        return vals[rank]

    p50_manual  = manual_quantile(0.5, sorted_vals)
    p95_manual  = manual_quantile(0.95, sorted_vals)

    print(f"\n  Manual p50 calculation:")
    print(f"    rank = ceil(0.5 × {N}) - 1 = {math.ceil(0.5 * N) - 1}")
    print(f"    value at rank {math.ceil(0.5*N)-1} = {p50_manual}")
    print(f"  Manual p95 calculation:")
    print(f"    rank = ceil(0.95 × {N}) - 1 = {math.ceil(0.95 * N) - 1}")
    print(f"    value at rank {math.ceil(0.95*N)-1} = {p95_manual}")
    print(f"  Expected (all values = {FIXED_LATENCY}): p50 = {FIXED_LATENCY}, p95 = {FIXED_LATENCY}")

    for phi, label, manual_val in [(0.5, "p50", p50_manual), (0.95, "p95", p95_manual)]:
        q = f"quantile_over_time({phi}, {GAUGE_METRIC}{SERIES_PA_Z0}[{RANGE_S}s])"
        res = instant_query(q, t=eval_time)
        if not res:
            print(f"  {FAIL} No result for: {q}")
            results.append(False)
            continue
        thanos_val = float(res[0]["value"][1])
        print(f"\n  Thanos query: {q}")
        print(f"  Thanos {label}: {thanos_val}")

        print(f"  Cross-checks ({label}):")
        check(f"manual {label} == {FIXED_LATENCY}", manual_val, FIXED_LATENCY, tol_abs=TOL_ABS)
        check(f"Thanos {label} == {FIXED_LATENCY}", thanos_val, FIXED_LATENCY, tol_abs=TOL_ABS)
        check(f"Thanos {label} == manual {label}", thanos_val, manual_val, tol_abs=TOL_ABS)

    # quantile() across series (not over time) for p50/p95
    print()
    print("  quantile() across all series (expected: 42.0 for both p50 and p95):")
    for phi, label in [(0.5, "p50"), (0.95, "p95")]:
        q = f"quantile({phi}, {GAUGE_METRIC})"
        res = instant_query(q)
        if not res:
            print(f"  {FAIL} No result for {q}")
            results.append(False)
            continue
        val = float(res[0]["value"][1])
        print(f"  Thanos {label} across series: {val}")
        check(f"quantile({phi}) across series == {FIXED_LATENCY}", val, FIXED_LATENCY, tol_abs=TOL_ABS)


# ─── Summary ─────────────────────────────────────────────────────────────────

def main():
    print(f"Thanos endpoint : {THANOS}")
    print(f"Eval time (now) : {time.time():.0f}")
    print()

    # Quick health check
    r = requests.get(f"{THANOS}/-/healthy", timeout=5)
    if r.status_code != 200:
        print(f"FATAL: Thanos not healthy — {r.status_code}")
        sys.exit(1)
    print("Thanos health: OK")

    check_rate()
    check_avg()
    check_quantile()

    passed = sum(results)
    total  = len(results)
    print()
    print("=" * 68)
    print(f"SUMMARY: {passed}/{total} checks passed")
    if passed == total:
        print("ALL CHECKS PASSED")
    else:
        print(f"FAILURES: {total - passed}")
    print("=" * 68)
    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()
