"""Smoke + invariant tests for the cost model. Run with:  python -m pytest
(or `python -m cost_model.test_cost_model` for a stdlib-only runner).

These assert the load-bearing invariants the report relies on, NOT exact
dollar magnitudes (those move as coefficients are recalibrated).
"""

from __future__ import annotations

from . import coefficients as C
from .model import build_resource_table, simulate, wire_mbps, cold_puts_per_month
from .pricing import Pricing
from .workloads import ARMS, ASAP_ARMS, RAW_ARMS, ASAPConfig, mvp_workload
from .validate import FINDINGS_MBPS, TOLERANCE_PCT


def test_wire_reproduces_findings_at_cal_point():
    w = mvp_workload()
    for arm in ARMS:
        model = wire_mbps(arm, w)
        truth = FINDINGS_MBPS[arm]
        err = abs(model - truth) / truth * 100.0
        assert err <= TOLERANCE_PCT, f"{arm}: {model} vs {truth} ({err:.2f}%)"


def test_raw_wire_scales_linearly_with_sample_rate():
    w = mvp_workload()
    w2 = mvp_workload(); w2.series = 20_000
    for arm in RAW_ARMS:
        assert abs(wire_mbps(arm, w2) / wire_mbps(arm, w) - 2.0) < 1e-6


def test_asap_wire_near_flat_in_hz():
    w = mvp_workload()
    w_hi = mvp_workload(); w_hi.sample_hz = 100.0
    # raw goes ~10x, asap stays well under 2x (window folds samples).
    assert wire_mbps("b0", w_hi) / wire_mbps("b0", w) > 9.0
    assert wire_mbps("asap", w_hi) / wire_mbps("asap", w) < 2.0


def test_asap_cheaper_than_raw_at_mvp():
    w = mvp_workload(); cfg = ASAPConfig(); p = Pricing()
    totals = {a: simulate(a, w, cfg, p)[1].total_usd for a in ARMS}
    for raw in RAW_ARMS:
        assert totals["asap"] < totals[raw], f"asap should beat {raw}"


def test_asap_dominant_cost_is_compute():
    w = mvp_workload()
    _, ct = simulate("asap", w, ASAPConfig(), Pricing())
    cat = ct.by_category()
    assert max(cat, key=cat.get) == "compute"


def test_intchunk_batching_cuts_puts():
    w = mvp_workload()
    batched = cold_puts_per_month(w, ASAPConfig(cold_format="intchunk", cold_part_batched=True))
    unbatched = cold_puts_per_month(w, ASAPConfig(cold_format="intchunk", cold_part_batched=False))
    assert unbatched / batched >= 50.0, "batching should cut PUTs ~60x"


def test_intchunk_stores_smaller_than_fragment():
    w = mvp_workload()
    from .model import cold_storage_gb_avg
    ic = cold_storage_gb_avg(w, ASAPConfig(cold_format="intchunk"))
    fr = cold_storage_gb_avg(w, ASAPConfig(cold_format="fragment"))
    assert fr / ic > 2.0, "intchunk ~2.33x smaller than fragment"


def test_sampling_reduces_edge_cpu():
    w = mvp_workload()
    rt1, _ = simulate("asap", w, ASAPConfig(default_sample_p=1.0))
    rt5, _ = simulate("asap", w, ASAPConfig(default_sample_p=0.5))
    e1 = next(r.get("cpu_vcpu") for r in rt1.rows if r.component == "edge_agent")
    e5 = next(r.get("cpu_vcpu") for r in rt5.rows if r.component == "edge_agent")
    assert e5 < e1


def test_all_arms_priced_positive():
    w = mvp_workload()
    for arm in ARMS:
        _, ct = simulate(arm, w, ASAPConfig(), Pricing())
        assert ct.total_usd > 0.0


def test_pricing_is_editable():
    w = mvp_workload()
    cheap = Pricing(transfer_cross_az_per_gb=0.0, transfer_egress_per_gb=0.0)
    _, ct = simulate("b0", w, ASAPConfig(), cheap)
    assert ct.by_category()["egress"] == 0.0


def _run_all():
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    failed = 0
    for fn in fns:
        try:
            fn()
            print(f"PASS {fn.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"FAIL {fn.__name__}: {e}")
    print(f"\n{len(fns) - failed}/{len(fns)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    import sys
    sys.exit(_run_all())
