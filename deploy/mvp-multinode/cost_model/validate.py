"""Validate that the model reproduces the FINDINGS wire-bandwidth numbers.

The wire model is calibrated to the FINDINGS sweep at the MVP operating point,
so at that exact point it must reproduce the table within rounding. This script
also checks the SCALING behaviour (raw arms scale linearly with raw-sample rate,
asap arms scale with series) so a reviewer can see the model is not just echoing
constants.

Run:
    python -m cost_model.validate
Exit code is non-zero if any arm's reproduction error exceeds TOLERANCE.
"""

from __future__ import annotations

import sys

from . import coefficients as C
from .model import wire_mbps
from .workloads import ARMS, RAW_ARMS, mvp_workload


# FINDINGS.md §2 final sweep — the ground truth.
FINDINGS_MBPS = {
    "b0": 18.59,
    "b1": 1.50,
    "b2": 3.03,
    "b3": 9.10,
    "asap": 0.98,
    "asap-gzip": 0.137,
}

# Reproduction tolerance at the calibration point (the model is calibrated TO
# these, so error should be ~0; allow float noise).
TOLERANCE_PCT = 0.5


def main() -> int:
    w = mvp_workload()
    print("# Wire-bandwidth validation vs FINDINGS.md (MVP operating point)\n")
    print(f"  calibration point: {C.CAL_SERIES:,} series @ {C.CAL_SAMPLE_HZ:g} Hz, "
          f"{C.CAL_METRICS} metrics\n")
    header = f"{'arm':<10} {'model Mbps':>11} {'FINDINGS':>10} {'err %':>8}"
    print(header)
    print("-" * len(header))

    worst = 0.0
    for arm in ARMS:
        model = wire_mbps(arm, w)
        truth = FINDINGS_MBPS[arm]
        err = abs(model - truth) / truth * 100.0
        worst = max(worst, err)
        print(f"{arm:<10} {model:>11.3f} {truth:>10.3f} {err:>7.3f}%")

    print(f"\n  worst reproduction error: {worst:.3f}%  (tolerance {TOLERANCE_PCT}%)")

    # ── scaling sanity: raw arms scale ~linearly with raw-sample rate ──
    print("\n## Scaling sanity (not measured — model behaviour check)\n")
    w2 = mvp_workload()
    w2.series = 20_000  # double the series
    print(f"  doubling series 10k -> 20k:")
    for arm in ["b0", "asap"]:
        base = wire_mbps(arm, w)
        scaled = wire_mbps(arm, w2)
        print(f"    {arm:<10} {base:.3f} -> {scaled:.3f} Mbps  ({scaled/base:.2f}x)")
    print("    (raw b0 should ~2x; asap ~2x in series too — sketch states scale "
          "with series)")

    w3 = mvp_workload()
    w3.sample_hz = 100.0  # 10x the scrape rate
    print(f"\n  10x scrape rate 10 -> 100 Hz:")
    for arm in ["b0", "asap"]:
        base = wire_mbps(arm, w)
        scaled = wire_mbps(arm, w3)
        print(f"    {arm:<10} {base:.3f} -> {scaled:.3f} Mbps  ({scaled/base:.2f}x)")
    print(f"    (raw b0 should ~10x; asap near-flat — window folds samples, "
          f"Hz exponent={C.ASAP_WIRE_HZ_EXPONENT})")

    ok = worst <= TOLERANCE_PCT
    print(f"\n{'PASS' if ok else 'FAIL'}: FINDINGS reproduction "
          f"{'within' if ok else 'EXCEEDS'} {TOLERANCE_PCT}% tolerance.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
