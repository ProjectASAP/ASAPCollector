"""CLI: arm + workload [+ asap config] -> resource table -> $ table -> comparison.

Run:
    python -m cost_model.simulator                 # full MVP arm comparison
    python -m cost_model.simulator --arm asap      # one arm, full breakdown
    python -m cost_model.simulator --sweep cold    # config sweep on cold format
    python -m cost_model.simulator --workload mvp-5sketch

Output is plain text (optionally prettier with `tabulate` if installed). No
heavy deps; stdlib only by default.
"""

from __future__ import annotations

import argparse
import sys
from typing import List, Optional

from . import coefficients as C
from .model import (
    ARM_COMPONENTS,
    RESOURCES,
    build_resource_table,
    price_resource_table,
    simulate,
    wire_mbps,
)
from .pricing import DEFAULT_PRICING, Pricing
from .workloads import ARMS, ASAP_ARMS, ASAPConfig, PRESETS, Workload


# ── optional pretty tables ───────────────────────────────────────────────────
try:
    from tabulate import tabulate  # type: ignore

    def _table(rows, headers):
        return tabulate(rows, headers=headers, floatfmt=".4g", tablefmt="github")
except Exception:  # pragma: no cover - fallback path
    def _table(rows, headers):
        widths = [len(str(h)) for h in headers]
        srows = []
        for r in rows:
            sr = []
            for i, c in enumerate(r):
                s = f"{c:.4g}" if isinstance(c, float) else str(c)
                sr.append(s)
                widths[i] = max(widths[i], len(s))
            srows.append(sr)
        out = []
        out.append(" | ".join(str(h).ljust(widths[i]) for i, h in enumerate(headers)))
        out.append("-+-".join("-" * widths[i] for i in range(len(headers))))
        for sr in srows:
            out.append(" | ".join(sr[i].ljust(widths[i]) for i in range(len(sr))))
        return "\n".join(out)


def _usd(x: float) -> str:
    return f"${x:,.2f}"


def print_resource_table(arm: str, w: Workload, cfg: ASAPConfig) -> None:
    rt = build_resource_table(arm, w, cfg)
    headers = ["component"] + RESOURCES
    rows = []
    for r in rt.rows:
        rows.append([r.component] + [r.get(res) for res in RESOURCES])
    rows.append(["TOTAL"] + [rt.total(res) for res in RESOURCES])
    print(f"\n## Resource table — arm `{arm}`, workload `{w.name}`")
    print(f"   wire = {wire_mbps(arm, w):.3f} Mbps\n")
    print(_table(rows, headers))


def print_cost_breakdown(arm: str, w: Workload, cfg: ASAPConfig, pricing: Pricing) -> None:
    _, ct = simulate(arm, w, cfg, pricing)
    headers = ["component", "compute$", "storage$", "request$", "egress$", "total$/mo"]
    rows = []
    for r in ct.rows:
        rows.append([r.component, r.compute_usd, r.storage_usd, r.request_usd, r.egress_usd, r.total_usd])
    cat = ct.by_category()
    rows.append(["TOTAL", cat["compute"], cat["storage"], cat["requests"], cat["egress"], ct.total_usd])
    print(f"\n## Cost breakdown — arm `{arm}`, workload `{w.name}`  ({pricing.region})\n")
    print(_table(rows, headers))


def print_arm_comparison(w: Workload, cfg: ASAPConfig, pricing: Pricing) -> None:
    print(f"\n# Arm comparison — workload `{w.name}` ({w.series:,} series @ {w.sample_hz:g} Hz, "
          f"{len(w.metrics)} metrics, {pricing.region})\n")
    headers = ["arm", "wire Mbps", "compute$", "storage$", "request$", "egress$", "TOTAL $/mo", "$/GB-ingested"]
    rows = []
    totals = {}
    for arm in ARMS:
        _, ct = simulate(arm, w, cfg, pricing)
        cat = ct.by_category()
        # GB ingested over the priced wire per month (the comparison denominator).
        rt = build_resource_table(arm, w, cfg)
        gb_ing = rt.total("egress_gb_mo")
        per_gb = ct.total_usd / gb_ing if gb_ing > 0 else float("nan")
        totals[arm] = ct.total_usd
        rows.append([arm, wire_mbps(arm, w), cat["compute"], cat["storage"],
                     cat["requests"], cat["egress"], ct.total_usd, per_gb])
    print(_table(rows, headers))

    # ASAP-vs-baseline tradeoff summary.
    base = totals["b0"]
    print("\n## ASAP vs baselines — total $/mo and the tradeoff\n")
    cmp_rows = []
    for arm in ARMS:
        delta = totals[arm] - totals["asap"]
        cmp_rows.append([arm, totals[arm], totals[arm] / base, delta])
    print(_table(cmp_rows, ["arm", "total $/mo", "x vs b0", "Δ$ (arm - asap)"]))

    asap = totals["asap"]
    print("\n## Dominant cost term per arm\n")
    dom_rows = []
    for arm in ARMS:
        _, ct = simulate(arm, w, cfg, pricing)
        cat = ct.by_category()
        dom = max(cat.items(), key=lambda kv: kv[1])
        dom_rows.append([arm, dom[0], dom[1], dom[1] / ct.total_usd if ct.total_usd else 0.0])
    print(_table(dom_rows, ["arm", "dominant term", "$/mo", "share"]))

    print(f"\nVerdict: asap total = {_usd(asap)}/mo vs b0 raw = {_usd(base)}/mo "
          f"({base/asap:.1f}x cheaper)." if asap > 0 else "")
    print("  ASAP trades MORE edge-EC2 CPU (edge aggregation) for LESS egress + "
          "S3 + VM storage.\n")


def cold_format_sweep(w: Workload, pricing: Pricing) -> None:
    print(f"\n# Cold-format sweep — arm `asap`, workload `{w.name}`\n")
    headers = ["cold_format", "batched", "s3_storage_gb", "PUT/mo", "storage$", "request$", "total $/mo"]
    rows = []
    configs = [
        ("intchunk", True),
        ("intchunk", False),
        ("fragment", True),
    ]
    for fmt, batched in configs:
        cfg = ASAPConfig(cold_format=fmt, cold_part_batched=batched)
        rt, ct = simulate("asap", w, cfg, pricing)
        cat = ct.by_category()
        rows.append([fmt, batched, rt.total("s3_storage_gb"), rt.total("s3_put_per_mo"),
                     cat["storage"], cat["requests"], ct.total_usd])
    print(_table(rows, headers))
    print("\nNote: intchunk+batched (#442) is the deployed default — ~12-60x fewer PUTs\n"
          "than the per-window/per-emit path, and ~2.33x smaller stored bytes than fragment.\n")


def sampling_sweep(w: Workload, pricing: Pricing) -> None:
    print(f"\n# Sampling-p sweep — arm `asap`, workload `{w.name}`\n")
    headers = ["sample_p", "edge_cpu vCPU", "edge compute$", "total $/mo"]
    rows = []
    for p in [1.0, 0.75, 0.5, 0.25]:
        cfg = ASAPConfig(default_sample_p=p)
        rt, ct = simulate("asap", w, cfg, pricing)
        edge_cpu = next(r.get("cpu_vcpu") for r in rt.rows if r.component == "edge_agent")
        edge_cost = next(r.compute_usd for r in ct.rows if r.component == "edge_agent")
        rows.append([p, edge_cpu, edge_cost, ct.total_usd])
    print(_table(rows, headers))
    print(f"\nNote: SAMPLING_CPU_EXPONENT={C.SAMPLING_CPU_EXPONENT} is ASSUMED pending the\n"
          "live sampling-bench at p=0.5; refine in coefficients.py.\n")


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="ASAP AWS cost-model simulator")
    ap.add_argument("--workload", default="mvp", choices=list(PRESETS),
                    help="workload preset (default: mvp)")
    ap.add_argument("--arm", choices=ARMS,
                    help="show full per-component breakdown for one arm")
    ap.add_argument("--cold-format", choices=["intchunk", "fragment"], default="intchunk")
    ap.add_argument("--no-batch", action="store_true",
                    help="disable intchunk per-block batching (#442 off)")
    ap.add_argument("--sample-p", type=float, default=1.0,
                    help="global default sampling probability")
    ap.add_argument("--retention-days", type=int, default=None)
    ap.add_argument("--sweep", choices=["cold", "sampling"],
                    help="run a config sweep instead of the arm comparison")
    ap.add_argument("--region", default=None, help="override pricing region label")
    ap.add_argument("--internet-egress", action="store_true",
                    help="charge the wire hop at internet-egress rate (else cross-AZ)")
    return ap


def main(argv: Optional[List[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)
    w = PRESETS[args.workload]()
    if args.retention_days is not None:
        w.retention_days = args.retention_days
    cfg = ASAPConfig(
        cold_format=args.cold_format,
        cold_part_batched=not args.no_batch,
        default_sample_p=args.sample_p,
    )
    pricing = Pricing()
    if args.region:
        pricing.region = args.region
    if args.internet_egress:
        pricing.wire_uses_internet_egress = True

    if args.sweep == "cold":
        cold_format_sweep(w, pricing)
        return 0
    if args.sweep == "sampling":
        sampling_sweep(w, pricing)
        return 0
    if args.arm:
        print_resource_table(args.arm, w, cfg)
        print_cost_breakdown(args.arm, w, cfg, pricing)
        return 0

    # default: full arm comparison + the asap full breakdown.
    print_arm_comparison(w, cfg, pricing)
    print_resource_table("asap", w, cfg)
    print_cost_breakdown("asap", w, cfg, pricing)
    print_cost_breakdown("b0", w, cfg, pricing)
    return 0


if __name__ == "__main__":
    sys.exit(main())
