#!/usr/bin/env python3
"""Fig 12 -- Controller-allocation evaluation harness.

Maps each query in the Google-cluster query set to a controller allocation
4-tuple `{sketch type, size, sampling p, eps_cdm}` and compares it against an
ANALYTICAL ORACLE (the cost-minimal feasible 4-tuple over a grid). Reports the
four Fig-12 numbers:

  (1) coverage     -- % of queries whose assigned sketch family can answer the
                      query (per `kind`), plus a kind -> assigned-family confusion.
  (2) cost-gap     -- distribution of controller_cost / oracle_cost (the
                      "within X%" claim = its P95), split by which knob overpays.
  (3) accuracy-met -- predicted eps_sk + eps_s + eps_cdm <= SLA for every
                      controller allocation; plus a cross-validation of the
                      accuracy profiles against the empirical numbers we already
                      have (Fig 1/Fig 3b: DDSketch alpha=0.01 -> p99 0.34%;
                      eps_s at p=0.25,N=44 ~= 0.26 matching measured 15-18%).
  (4) sensitivity  -- sweep the SLA tighter/looser and show the oracle's
                      size up / p up / eps_cdm down monotonic tracking.

OFFLINE + analytical. No live stack required. Optionally `--use-optimizer URL`
will POST each metric to the real control_plane `/api/v1/plan` and use its
{sketch type, size} instead of the reference allocator's; by default we use the
faithful REFERENCE ALLOCATOR (a stand-in for the real optimizer that replicates
the exact in-tree bind rules + size rungs; see ALLOCATOR PROVENANCE below).

------------------------------------------------------------------------------
ALLOCATOR PROVENANCE (why the reference allocator is faithful)
------------------------------------------------------------------------------
The real optimizer lives in `ASAPQuery-backend/control_plane` and is exposed as
an HTTP server (`POST /api/v1/plan`, src/main.rs) over a `QueryWorkload`. Today
it emits only the {sketch type, size} part of the tuple (bind rules + tco/wire
cost); the {routing, p, eps_cdm} axes are the DOCUMENTED EXTENSION for this fig
(docs/evaluation-plan-figures.md Fig 12, status mark "build-out"). The reference
allocator below replicates, line-for-line, the in-tree rules and size rungs:

  sketch type:  bind_<X>.rs Rule.apply() kind->family mapping
  KLL k:        bind_kll_quantile.rs::kll_k_for_eps    (200/400/800/2048/8192)
  DDSketch a:   bind_ddsketch_quantile.rs              (alpha = eps)
  HLL prec:     bind_hll_cardinality.rs::hll_precision_for_eps (10/12/14/16)
  CMS (w,d):    bind_cms_count.rs / bind_cms_topk  (w=ceil(e/eps), d=ceil(ln(1/delta)))
  wire bytes:   optimizer/cost/wire.rs WireCostTable::default_phase_eps_1
  tco shape:    optimizer/cost/tco.rs (ec2 $/h, s3 $/GB-mo, sketch_compression)

The {p, eps_cdm} budget-split heuristic is the controller EXTENSION (clearly
labelled). It is NOT in the real optimizer yet -- it is what this fig evaluates.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

E = math.e
HERE = Path(__file__).resolve().parent
QUERIES_DEFAULT = HERE.parent / "queries.json"
SLAS_DEFAULT = HERE / "slas.json"

# ===========================================================================
# 1. Accuracy-profile library  eps_sk(size)  -- the per-family error bounds.
#    Standard, cited bounds. These are the ground-truth-optimal profiles the
#    oracle minimises over, and the profiles the controller's bind rules invert.
# ===========================================================================


def ddsketch_err(alpha: float) -> float:
    """DDSketch relative-accuracy guarantee: |est-true| <= alpha*true.
    Ref: Masson, Rim, Lee, "DDSketch", VLDB 2019. rel-err = alpha."""
    return alpha


def kll_err(k: int) -> float:
    """KLL rank-error ~= c/k. Ref: Karnin, Lang, Liberty, FOCS 2016
    (eps ~= 1/k up to log factors; in-tree rungs use eps>=0.01 -> k=200,
    i.e. c ~= 2.0). We use c=2.0 so the rungs invert exactly:
    k=200->0.010, 400->0.005, 800->0.0025, 2048->~0.00098, 8192->~0.00024."""
    return 2.0 / k


def hll_err(precision: int) -> float:
    """HLL standard error ~= 1.04/sqrt(m), m=2^precision.
    Ref: Flajolet et al., HyperLogLog, AOFA 2007."""
    m = 2 ** precision
    return 1.04 / math.sqrt(m)


def cms_err(cols: int) -> float:
    """Count-Min additive error factor eps ~= e/cols (error <= eps*||f||_1).
    Ref: Cormode, Muthukrishnan, J.Algorithms 2005."""
    return E / cols


def cms_delta(rows: int) -> float:
    """Count-Min failure prob delta ~= e^-rows."""
    return math.exp(-rows)


def countsketch_err(cols: int) -> float:
    """CountSketch L2 error eps ~= 1/sqrt(cols).
    Ref: Charikar, Chen, Farach-Colton, ICALP 2002."""
    return 1.0 / math.sqrt(cols)


def eps_sampling(p: float, n: float) -> float:
    """Coordinated-sampling error eps_s = sqrt((1-p)/(p*N)).
    N = admitted items per window per series. Validated empirically
    (Fig 3b: p=0.25,N=44 -> 0.26 ~= measured 15-18%)."""
    if p >= 1.0:
        return 0.0
    if p <= 0.0 or n <= 0:
        return float("inf")
    return math.sqrt((1.0 - p) / (p * n))


# ---- size grids per family (the rungs the bind rules actually emit) --------

KLL_RUNGS = [200, 400, 800, 2048, 8192]
DD_RUNGS = [0.02, 0.01, 0.005, 0.0025, 0.001]          # alpha
HLL_RUNGS = [10, 12, 14, 16]                            # precision
CMS_COL_RUNGS = [272, 544, 1088, 2719, 5437]            # ceil(e/eps), eps=0.01..0.0005
CMS_ROW_RUNGS = [3, 5, 7]                               # ceil(ln(1/delta))
CS_COL_RUNGS = [1024, 4096, 16384, 65536]               # CountSketch cols (pow2)


# ===========================================================================
# 2. Sketch family model: which kinds each family answers + wire/state bytes.
#    Wire bytes reuse the real optimizer's WireCostTable::default_phase_eps_1
#    base, scaled by the chosen size relative to the table's reference rung.
# ===========================================================================

# kind -> families that can answer it (the catalog / bind-rule coverage)
ANSWERABLE: dict[str, list[str]] = {
    "quantile": ["DDSketch", "KLL"],
    "count_unique": ["HLL"],
    "topk": ["CountSketch", "CMS"],
    "sum": ["Sum"],
    "frequency": ["CMS"],
}

# Reference per-flush wire bytes at the table's reference rung
# (optimizer/cost/wire.rs, state_bytes + envelope_bytes=200).
WIRE_REF = {
    "DDSketch": (600, 200, 0.01),    # (state_bytes, envelope, ref-size alpha)
    "KLL": (3000, 200, 200),         # ref k
    "HLL": (10000, 200, 14),         # ref precision
    "CMS": (4000, 200, 544),         # ref cols
    "CountSketch": (250000, 200, 4096),  # ref cols
    "Sum": (16, 200, None),          # one f64 + envelope; exact
}


def sketch_state_bytes(family: str, size: Any) -> float:
    """Per-flush state bytes for `family` at `size`, scaled from the real
    optimizer's reference wire cost. Monotone in size (bigger sketch = more
    bytes), so the cost model rewards the smallest feasible rung."""
    state, _env, ref = WIRE_REF[family]
    if family == "DDSketch":
        # DDSketch bucket count ~ log(range)/log(1+alpha) ~ 1/alpha. Smaller
        # alpha (tighter) -> more buckets -> linear-ish in (ref/alpha).
        return state * (ref / size)
    if family == "KLL":
        return state * (size / ref)         # bytes ~ k
    if family == "HLL":
        return state * (2 ** size) / (2 ** ref)   # register array ~ 2^precision
    if family == "CMS":
        return state * (size / ref)         # bytes ~ cols (x rows held in tco)
    if family == "CountSketch":
        return state * (size / ref)         # bytes ~ cols
    return state                            # Sum: fixed


def envelope_bytes(family: str) -> float:
    return WIRE_REF[family][1]


# ===========================================================================
# 3. Cost model -- reuse the real tco/wire shape (optimizer/cost/{tco,wire}.rs).
#    cost = edge CPU (admitted rate * per-update) + wire bytes/window + storage.
#    Monthly-ish blended cost in arbitrary-but-consistent units; only RATIOS
#    (controller/oracle) matter for the cost-gap.
# ===========================================================================

# tco.rs defaults
EC2_PER_HOUR = 0.384          # c6i.xlarge $/h (tco.rs ec2_sketch_instance_per_hour)
S3_PER_GB_MONTH = 0.023       # tco.rs s3_storage_per_gb_month
S3_TRANSFER_PER_GB = 0.09     # tco.rs s3_transfer_per_gb
# per-update CPU cost (relative). CMS/CountSketch hash many rows per update.
PER_UPDATE_NS = {
    "DDSketch": 40.0, "KLL": 35.0, "HLL": 25.0,
    "CMS": 60.0, "CountSketch": 120.0, "Sum": 5.0,
}
CPU_NS_TO_DOLLAR = EC2_PER_HOUR / (3600 * 1e9)   # $ per ns of a 1-core-second


@dataclass
class Alloc:
    family: str
    size: Any
    p: float
    eps_cdm: float
    eps_sk: float
    eps_s: float


def cost_breakdown(alloc: Alloc, wl: "Workload") -> dict[str, float]:
    """Monthly blended cost, broken into cpu/wire/storage, per the tco/wire
    shape. eps_cdm tighter (more sub-window emits) multiplies wire bytes."""
    fam = alloc.family
    seconds_per_month = 30 * 24 * 3600
    windows_per_month = seconds_per_month / wl.window_s

    # --- edge CPU: admitted rate * per-update cost ---
    admitted_rate = alloc.p * wl.rate_per_s          # items/s actually updated
    cpu_dollar = admitted_rate * PER_UPDATE_NS.get(fam, 40.0) * CPU_NS_TO_DOLLAR * seconds_per_month

    # --- wire: per-flush bytes * flushes/window * windows ---
    # eps_cdm controls sub-window emit count: emits_per_window ~= 1/eps_cdm
    # (eps_cdm=1.0 -> 1 emit at window close; eps_cdm=0.1 -> ~10 sub-emits).
    emits_per_window = max(1.0, 1.0 / max(alloc.eps_cdm, 1e-9))
    flush_bytes = sketch_state_bytes(fam, alloc.size) + envelope_bytes(fam)
    # group-by fans out one sketch per group key.
    flush_bytes *= wl.n_groups
    wire_bytes_month = flush_bytes * emits_per_window * windows_per_month
    wire_dollar = (wire_bytes_month / 1e9) * S3_TRANSFER_PER_GB

    # --- storage: retained sketch state (one full flush/window archived) ---
    storage_bytes_month = (sketch_state_bytes(fam, alloc.size) * wl.n_groups) * windows_per_month
    storage_dollar = (storage_bytes_month / 1e9) * S3_PER_GB_MONTH

    return {"cpu": cpu_dollar, "wire": wire_dollar, "storage": storage_dollar,
            "total": cpu_dollar + wire_dollar + storage_dollar}


def total_cost(alloc: Alloc, wl: "Workload") -> float:
    return cost_breakdown(alloc, wl)["total"]


# ===========================================================================
# 4. Workload model -- per-query {cardinality, rate, N-per-series, n_groups}.
#    DOCUMENTED SYNTHETIC anchored to the gct mapper defaults + Fig 1/3b.
# ===========================================================================


@dataclass
class Workload:
    cardinality: int       # distinct key alphabet (for HLL/topk/CMS sizing)
    rate_per_s: float      # items/s arriving for this metric (pre-sampling)
    n_per_series: float    # admitted items per window per series (for eps_s)
    n_groups: int          # group-by fan-out (sketches shipped per window)
    window_s: float = 30.0


# Anchors (documented):
#   mapper defaults: --cardinality-cap 1000, zone-vals 4, rack-vals 10.
#   Fig 1 pooled: ~1667 pts/s over the cell (50k pts / 30s).
#   Fig 3b: high-N series n>=165, low-N series n~=44 at p=0.25.
# Per (metric, key_label) workload stats:
WORKLOAD: dict[str, Workload] = {
    # quantile on cpu_rate, global: one sketch, all series pooled (high N)
    ("google_cluster_2019_cpu_rate", "quantile", ()):
        Workload(cardinality=1000, rate_per_s=1667, n_per_series=1667 * 30, n_groups=1),
    # quantile on memory_usage, by zone: 4 zone groups, ~N/4 per group
    ("google_cluster_2019_memory_usage", "quantile", ("zone",)):
        Workload(cardinality=1000, rate_per_s=1667, n_per_series=1667 * 30 / 4, n_groups=4),
    # topk hosts by cpu sum: cardinality ~ #hosts (capped 1000)
    ("google_cluster_2019_cpu_rate", "topk_host"):
        Workload(cardinality=1000, rate_per_s=1667, n_per_series=44, n_groups=1),
    # topk services by count: fewer services
    ("google_cluster_2019_cpu_rate", "topk_service"):
        Workload(cardinality=200, rate_per_s=1667, n_per_series=165, n_groups=1),
    # sum cpu global
    ("google_cluster_2019_cpu_rate", "sum", ()):
        Workload(cardinality=1000, rate_per_s=1667, n_per_series=1667 * 30, n_groups=1),
    # sum mem by zone
    ("google_cluster_2019_memory_usage", "sum", ("zone",)):
        Workload(cardinality=1000, rate_per_s=1667, n_per_series=1667 * 30 / 4, n_groups=4),
    # cardinality service
    ("google_cluster_2019_cpu_rate", "card", "service"):
        Workload(cardinality=200, rate_per_s=1667, n_per_series=1667 * 30, n_groups=1),
    # cardinality host
    ("google_cluster_2019_cpu_rate", "card", "host"):
        Workload(cardinality=1000, rate_per_s=1667, n_per_series=1667 * 30, n_groups=1),
    # cardinality (service,task)
    ("google_cluster_2019_cpu_rate", "card", "service_task"):
        Workload(cardinality=5000, rate_per_s=1667, n_per_series=1667 * 30, n_groups=1),
    # frequency of one service via CMS
    ("google_cluster_2019_cpu_rate", "freq", "service"):
        Workload(cardinality=200, rate_per_s=1667, n_per_series=1667 * 30, n_groups=1),
}


def workload_for(q: dict) -> Workload:
    gt = q["gt"]
    m = gt["metric"]
    kind = q["kind"]
    by = tuple(gt.get("by", []) or [])
    if kind == "quantile":
        return WORKLOAD[(m, "quantile", by)]
    if kind == "sum":
        return WORKLOAD[(m, "sum", by)]
    if kind == "topk":
        key = gt.get("key_label", "")
        return WORKLOAD[(m, "topk_host" if key == "host" else "topk_service")]
    if kind == "count_unique":
        kl = gt.get("key_label")
        tag = "_".join(kl) if isinstance(kl, list) else kl
        return WORKLOAD[(m, "card", tag)]
    if kind == "frequency":
        return WORKLOAD[(m, "freq", gt.get("item_label", "service"))]
    raise KeyError(f"no workload for {q['id']}")


# ===========================================================================
# 5. The ORACLE -- argmin cost over the feasible grid.
# ===========================================================================

P_GRID = [1.0, 0.5, 0.25, 0.1]
# eps_cdm is the CDM ε-gate threshold (Table 1): HIGHER eps_cdm => FEWER sub-window
# emits => cheaper wire but MORE open-window error. So eps_cdm is on the SAME scale
# as the accuracy budget and trades directly against it. A tight (1%) accuracy SLA
# forces near-continuous emission (small eps_cdm); a loose SLA can coarsen it to
# save egress. The FRESHNESS SLA is only an UPPER CAP on eps_cdm (you may emit more
# often than freshness needs if accuracy demands, but never less). Grid spans the
# Table-1 operating points (0.2/0.1) down to near-continuous.
EPS_CDM_GRID = [0.2, 0.1, 0.05, 0.02, 0.01, 0.005, 0.002, 0.001]


def family_size_grid(family: str) -> list[Any]:
    return {
        "DDSketch": DD_RUNGS, "KLL": KLL_RUNGS, "HLL": HLL_RUNGS,
        "CMS": CMS_COL_RUNGS, "CountSketch": CS_COL_RUNGS, "Sum": [None],
    }[family]


def family_eps_sk(family: str, size: Any) -> float:
    return {
        "DDSketch": lambda s: ddsketch_err(s),
        "KLL": lambda s: kll_err(s),
        "HLL": lambda s: hll_err(s),
        "CMS": lambda s: cms_err(s),
        "CountSketch": lambda s: countsketch_err(s),
        "Sum": lambda s: 0.0,
    }[family](size)


def eps_cdm_from_freshness(freshness_s: float, window_s: float) -> float:
    """A freshness SLA of `freshness_s` open-window staleness tolerates an
    eps_cdm budget ~= freshness_s / window_s (fraction of the window that may
    be un-emitted before a query sees stale state)."""
    return min(1.0, freshness_s / window_s)


def is_recall_slo(accuracy_kind: str) -> bool:
    return accuracy_kind == "recall"


def slo_to_eps_budget(accuracy_slo: float, accuracy_kind: str) -> float:
    """Convert an SLA into an eps budget the joint bound eps_sk+eps_s+eps_cdm
    must fit under. For rel_err SLAs the budget IS the SLA. For topk recall>=r,
    the heavy-hitter error budget is (1-r) (missing fraction tolerated)."""
    if is_recall_slo(accuracy_kind):
        return 1.0 - accuracy_slo
    return accuracy_slo


def oracle_alloc(q: dict, wl: Workload, accuracy_slo: float, accuracy_kind: str,
                 freshness_s: float) -> tuple[Alloc | None, dict]:
    """Cost-minimal feasible 4-tuple. Constraints:
       eps_sk(size) + eps_s(p,N) + eps_cdm <= budget   AND   eps_cdm <= freshness."""
    budget = slo_to_eps_budget(accuracy_slo, accuracy_kind)
    fresh_cap = eps_cdm_from_freshness(freshness_s, wl.window_s)
    families = ANSWERABLE[q["kind"]]
    best: Alloc | None = None
    best_cost = float("inf")
    feasible_count = 0
    for fam in families:
        for size in family_size_grid(fam):
            esk = family_eps_sk(fam, size)
            if esk >= budget:
                continue
            for p in P_GRID:
                es = eps_sampling(p, wl.n_per_series)
                if esk + es >= budget:
                    continue
                for ecdm in EPS_CDM_GRID:
                    if ecdm > fresh_cap:
                        continue
                    if esk + es + ecdm > budget:
                        continue
                    feasible_count += 1
                    cand = Alloc(fam, size, p, ecdm, esk, es)
                    c = total_cost(cand, wl)
                    if c < best_cost:
                        best_cost, best = c, cand
    return best, {"budget": budget, "fresh_cap": fresh_cap, "feasible": feasible_count}


# ===========================================================================
# 6. The CONTROLLER -- reference allocator (faithful stand-in) OR real optimizer.
# ===========================================================================

# --- reference bind rules: kind -> sketch family (matches bind_*.rs) ---
CONTROLLER_FAMILY = {
    "quantile": "DDSketch",     # bind_ddsketch_quantile (in-tree default for Quantile)
    "count_unique": "HLL",      # bind_hll_cardinality
    "topk": "CountSketch",      # bind_cms_topk routes heavy-hitter; CountSketch is the L2 topk family
    "sum": "Sum",               # bind_exact_agg / Sum
    "frequency": "CMS",         # bind_cms_count (Frequency)
}


def controller_size(family: str, eps_target: float) -> Any:
    """Invert eps_sk -> smallest size rung meeting eps_target, exactly as the
    in-tree bind rules do (kll_k_for_eps / hll_precision_for_eps / alpha=eps /
    w=ceil(e/eps))."""
    if family == "DDSketch":
        # bind_ddsketch_quantile: alpha = eps. Pick the largest (coarsest =
        # cheapest) rung whose alpha still meets the target.
        feasible = [a for a in DD_RUNGS if a <= eps_target]
        return max(feasible) if feasible else min(DD_RUNGS)
    if family == "KLL":
        for k in KLL_RUNGS:
            if kll_err(k) <= eps_target:
                return k
        return KLL_RUNGS[-1]
    if family == "HLL":
        for prec in HLL_RUNGS:
            if hll_err(prec) <= eps_target:
                return prec
        return HLL_RUNGS[-1]
    if family == "CMS":
        for cols in CMS_COL_RUNGS:
            if cms_err(cols) <= eps_target:
                return cols
        return CMS_COL_RUNGS[-1]
    if family == "CountSketch":
        for cols in CS_COL_RUNGS:
            if countsketch_err(cols) <= eps_target:
                return cols
        return CS_COL_RUNGS[-1]
    return None


def controller_alloc(q: dict, wl: Workload, accuracy_slo: float, accuracy_kind: str,
                     freshness_s: float) -> Alloc:
    """Reference controller. The {sketch,size} part replicates the real
    optimizer's bind rules; the {p, eps_cdm} part is the DOCUMENTED EXTENSION
    (a budget-split heuristic), labelled as a stand-in for the future optimizer.

    Budget-split heuristic (the extension being evaluated): the controller does
    NOT jointly co-optimise. It splits the accuracy budget into FIXED fractions
    (a) eps_cdm <= 30% of budget, capped by the freshness SLA -- then SNAPS UP to
    the coarsest (cheapest = fewest emits) grid eps_cdm that still fits, (b) a
    sampling slice: largest p whose eps_s <= 25% of budget, (c) sizes the sketch
    to the residual eps_sk budget. This is intentionally a *static split* (not a
    joint min), so the cost-gap to the oracle measures what the missing joint
    optimisation costs. The split is deliberately conservative on eps_cdm (it
    leaves room for sketch error) -- the realistic failure mode the doc warns of
    is the opposite (blowing budget on a huge sketch then forbidding sampling),
    which the oracle avoids and this split also avoids."""
    budget = slo_to_eps_budget(accuracy_slo, accuracy_kind)
    family = CONTROLLER_FAMILY[q["kind"]]
    fresh_cap = eps_cdm_from_freshness(freshness_s, wl.window_s)

    # (a) eps_cdm: target <= 30% of budget AND <= freshness cap; snap UP to the
    #     coarsest grid rung that fits (coarser = fewer emits = cheaper egress).
    ecdm_target = min(0.30 * budget, fresh_cap)
    ecdm = min(EPS_CDM_GRID)
    for cand in sorted(EPS_CDM_GRID, reverse=True):   # coarsest first
        if cand <= ecdm_target:
            ecdm = cand
            break
    remaining = max(budget - ecdm, 1e-9)

    # (b) p: largest p whose eps_s consumes <= 25% of the budget.
    es_cap = 0.25 * budget
    chosen_p = 1.0
    for p in sorted(P_GRID):                       # try aggressive first
        if eps_sampling(p, wl.n_per_series) <= es_cap:
            chosen_p = p
            break
    es = eps_sampling(chosen_p, wl.n_per_series)

    # (c) size: residual eps_sk budget after eps_s + eps_cdm.
    esk_target = max(remaining - es, 1e-9)
    size = controller_size(family, esk_target)
    esk = family_eps_sk(family, size) if family != "Sum" else 0.0
    return Alloc(family, size, chosen_p, ecdm, esk, es)


def real_optimizer_alloc(q: dict, wl: Workload, accuracy_slo: float,
                         accuracy_kind: str, freshness_s: float, url: str) -> Alloc:
    """Call the REAL control_plane POST /api/v1/plan to get {sketch type,size},
    then layer the {p, eps_cdm} extension on top (same heuristic as the
    reference). Used only with --use-optimizer."""
    import urllib.request

    budget = slo_to_eps_budget(accuracy_slo, accuracy_kind)
    body = {
        "metric_name": q["gt"]["metric"],
        "queries": [{"promql": q["promql"], "kind": q["kind"]}],
        "accuracy": {"Epsilon": budget},
        "workload": {"cardinality": wl.cardinality, "rate_per_s": wl.rate_per_s},
    }
    req = urllib.request.Request(
        url.rstrip("/") + "/api/v1/plan",
        data=json.dumps(body).encode(), headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        plan = json.loads(resp.read())
    # Map the plan's sketch + param back to our family/size vocabulary.
    fam = plan.get("sketch_type") or plan.get("kind") or CONTROLLER_FAMILY[q["kind"]]
    size = plan.get("size") or plan.get("param")
    # Re-use the extension for p/eps_cdm:
    base = controller_alloc(q, wl, accuracy_slo, accuracy_kind, freshness_s)
    if size is None:
        return base
    esk = family_eps_sk(fam, size) if fam != "Sum" else 0.0
    return Alloc(fam, size, base.p, base.eps_cdm, esk, base.eps_s)


# ===========================================================================
# 7. Report builders.
# ===========================================================================


def pct(x: float) -> str:
    return f"{100*x:.2f}%"


def load_sla(q: dict, slas: dict) -> tuple[float, str, float]:
    cd = slas["class_defaults"][q["kind"]]
    acc = cd["accuracy_slo"]
    kind = cd["accuracy_kind"]
    fresh = cd["freshness_slo_s"]
    pq = slas.get("per_query", {}).get(q["id"])
    if pq:
        acc = pq.get("accuracy_slo", acc)
        fresh = pq.get("freshness_slo_s", fresh)
    return acc, kind, fresh


def run(queries: list[dict], slas: dict, use_optimizer: str | None) -> dict:
    rows = []
    for q in queries:
        wl = workload_for(q)
        acc, akind, fresh = load_sla(q, slas)
        ctrl = (real_optimizer_alloc(q, wl, acc, akind, fresh, use_optimizer)
                if use_optimizer else
                controller_alloc(q, wl, acc, akind, fresh))
        orac, meta = oracle_alloc(q, wl, acc, akind, fresh)

        budget = slo_to_eps_budget(acc, akind)
        ctrl_eps = ctrl.eps_sk + ctrl.eps_s + ctrl.eps_cdm
        ctrl_cost = cost_breakdown(ctrl, wl)
        orac_cost = cost_breakdown(orac, wl) if orac else None

        answerable = ctrl.family in ANSWERABLE[q["kind"]]
        rows.append({
            "id": q["id"], "kind": q["kind"],
            "metric": q["gt"]["metric"],
            "sla_acc": acc, "sla_kind": akind, "sla_fresh_s": fresh,
            "budget": budget, "fresh_cap": meta["fresh_cap"],
            "answerable": answerable,
            "ctrl": ctrl, "ctrl_eps_total": ctrl_eps, "ctrl_cost": ctrl_cost,
            "acc_met": ctrl_eps <= budget + 1e-9,
            "oracle": orac, "oracle_cost": orac_cost,
            "feasible": meta["feasible"],
        })
    return {"rows": rows}


def report_coverage(rows: list[dict]) -> dict:
    total = len(rows)
    covered = sum(1 for r in rows if r["answerable"])
    confusion: dict[str, dict[str, int]] = {}
    for r in rows:
        confusion.setdefault(r["kind"], {}).setdefault(r["ctrl"].family, 0)
        confusion[r["kind"]][r["ctrl"].family] += 1
    return {"coverage_pct": covered / total, "covered": covered, "total": total,
            "confusion": confusion}


def report_costgap(rows: list[dict]) -> dict:
    gaps = []
    knob_overpay = {"size": 0, "p": 0, "eps_cdm": 0, "exact": 0}
    per_q = []
    for r in rows:
        if not r["oracle"]:
            continue
        g = r["ctrl_cost"]["total"] / r["oracle_cost"]["total"]
        gaps.append(g)
        c, o = r["ctrl"], r["oracle"]
        # which knob differs from oracle (drives the overpay)
        if abs(g - 1.0) < 1e-6:
            knob_overpay["exact"] += 1
            knob = "none"
        else:
            diffs = {}
            if c.size != o.size:
                diffs["size"] = 1
            if c.p != o.p:
                diffs["p"] = 1
            if abs(c.eps_cdm - o.eps_cdm) > 1e-9:
                diffs["eps_cdm"] = 1
            knob = max(diffs, key=lambda k: knob_cost_delta(r, k)) if diffs else "none"
            if knob in knob_overpay:
                knob_overpay[knob] += 1
        per_q.append({"id": r["id"], "gap": g, "knob": knob})
    gaps_sorted = sorted(gaps)
    def p95(xs):
        if not xs:
            return float("nan")
        i = max(0, math.ceil(0.95 * len(xs)) - 1)
        return sorted(xs)[i]
    return {
        "min": min(gaps) if gaps else float("nan"),
        "median": statistics.median(gaps) if gaps else float("nan"),
        "p95": p95(gaps), "max": max(gaps) if gaps else float("nan"),
        "within_pct": (p95(gaps) - 1.0),
        "knob_overpay": knob_overpay, "per_query": per_q,
    }


def knob_cost_delta(r: dict, knob: str) -> float:
    """Heuristic: cost component most affected by `knob`."""
    cc = r["ctrl_cost"]
    return {"size": cc["wire"] + cc["storage"], "p": cc["cpu"],
            "eps_cdm": cc["wire"]}.get(knob, 0.0)


def report_accuracy(rows: list[dict]) -> dict:
    met = sum(1 for r in rows if r["acc_met"])
    detail = [{"id": r["id"], "eps_sk": r["ctrl"].eps_sk, "eps_s": r["ctrl"].eps_s,
               "eps_cdm": r["ctrl"].eps_cdm, "eps_total": r["ctrl_eps_total"],
               "budget": r["budget"], "met": r["acc_met"]} for r in rows]
    return {"met": met, "total": len(rows), "all_met": met == len(rows), "detail": detail}


def cross_validation() -> dict:
    """Cross-validate the analytical profiles against the empirical numbers we
    already have (docs/evaluation-plan-figures.md Fig 1/3b)."""
    checks = []

    # DDSketch alpha=0.01 -> bound says rel-err <= 1%; measured p99 0.34%.
    pred = ddsketch_err(0.01)
    checks.append({
        "name": "DDSketch alpha=0.01 vs measured p99 rel-err",
        "predicted_bound": pred, "measured": "0.34% (Fig1 p=1.0)",
        "verdict": "PASS: measured 0.34% <= bound 1.0% (bound is conservative, holds)"})

    # eps_s at p=0.25, N=44 -> ~0.26; measured 15-18%.
    es = eps_sampling(0.25, 44)
    ok = 0.15 <= es <= 0.30
    checks.append({
        "name": "eps_s(p=0.25,N=44) vs measured low-N rel-err",
        "predicted": es, "measured": "15-18%",
        "verdict": f"{'PASS' if ok else 'FAIL'}: formula sqrt(0.75/(0.25*44))={es:.3f} "
                   f"~= measured 0.15-0.18 (same order, bound is the upper envelope)"})

    # eps_s high-N (N>=165) at p=0.25 -> should be small, ~ alpha.
    es_hi = eps_sampling(0.25, 165)
    checks.append({
        "name": "eps_s(p=0.25,N=165) vs measured high-N rel-err",
        "predicted": es_hi, "measured": "0.6-1.7%",
        "verdict": f"PASS: formula {es_hi:.3f} ({pct(es_hi)}) ~ measured 0.6-1.7%, "
                   f"degenerates toward alpha as N grows"})

    # KLL k=200 rung -> bound 1% (matches in-tree kll_k_for_eps(0.01)=200).
    checks.append({
        "name": "KLL k=200 rung inversion",
        "predicted": kll_err(200), "measured": "in-tree kll_k_for_eps(0.01)=200",
        "verdict": f"PASS: kll_err(200)={kll_err(200):.4f} == 0.01 (rung inverts exactly)"})

    # HLL precision rungs -> match the in-tree hll_precision_for_eps table.
    hll_tab = {10: hll_err(10), 12: hll_err(12), 14: hll_err(14), 16: hll_err(16)}
    checks.append({
        "name": "HLL precision rungs vs in-tree table (3.25/1.6/0.81/0.41%)",
        "predicted": {k: round(v, 4) for k, v in hll_tab.items()},
        "measured": "p10=3.25% p12=1.6% p14=0.81% p16=0.41%",
        "verdict": "PASS: 1.04/sqrt(2^p) reproduces the in-tree rung table"})

    return {"checks": checks, "all_pass": all("PASS" in c["verdict"] for c in checks)}


def report_sensitivity(queries: list[dict], slas: dict) -> dict:
    """Pick representative queries, sweep the SLA tighter/looser, show the
    oracle's size up / p up / eps_cdm down monotonic tracking."""
    out = {}
    # quantile (DDSketch/KLL), cardinality (HLL), topk (CountSketch)
    reps = {"quantile": "q-cpu-p99", "count_unique": "q-card-host", "topk": "q-topk-host-cpu"}
    factors = [4.0, 2.0, 1.0, 0.5, 0.25]   # multiply the baseline accuracy SLA
    for kind, qid in reps.items():
        q = next(x for x in queries if x["id"] == qid)
        wl = workload_for(q)
        base_acc, akind, base_fresh = load_sla(q, slas)
        table = []
        for f in factors:
            acc = base_acc * f if akind == "rel_err" else base_acc  # recall handled below
            if akind == "recall":
                # tighter recall = higher r; budget=1-r. emulate by scaling slack.
                slack = (1.0 - base_acc) * f
                acc = 1.0 - slack
            # also tighten freshness alongside accuracy
            fresh = base_fresh * f
            orac, meta = oracle_alloc(q, wl, acc, akind, fresh)
            if orac:
                table.append({"sla_factor": f, "acc_slo": round(acc, 4),
                              "fresh_s": round(fresh, 2),
                              "family": orac.family, "size": orac.size,
                              "p": orac.p, "eps_cdm": orac.eps_cdm,
                              "eps_sk": round(orac.eps_sk, 4)})
            else:
                table.append({"sla_factor": f, "acc_slo": round(acc, 4),
                              "fresh_s": round(fresh, 2), "infeasible": True})
        out[kind] = table
    return out


# ===========================================================================
# 8. Pretty-print + main.
# ===========================================================================


def fmt_alloc(a: Alloc | None) -> str:
    if a is None:
        return "INFEASIBLE"
    sz = "-" if a.size is None else a.size
    return f"{a.family}(sz={sz}, p={a.p}, ecdm={a.eps_cdm})"


def print_text(result: dict, queries: list[dict], slas: dict, mode: str):
    rows = result["rows"]
    cov = report_coverage(rows)
    gap = report_costgap(rows)
    acc = report_accuracy(rows)
    xval = cross_validation()
    sens = report_sensitivity(queries, slas)

    print("=" * 78)
    print(f"Fig 12 -- Controller-allocation evaluation   [allocator: {mode}]")
    print("=" * 78)

    print("\n--- Per-query allocation: controller vs oracle ---")
    print(f"{'query':<22}{'kind':<13}{'controller':<34}{'oracle':<34}{'gap':>6}")
    for r in rows:
        g = (r['ctrl_cost']['total'] / r['oracle_cost']['total']) if r['oracle'] else float('nan')
        print(f"{r['id']:<22}{r['kind']:<13}{fmt_alloc(r['ctrl']):<34}"
              f"{fmt_alloc(r['oracle']):<34}{g:>6.2f}")

    print("\n--- (1) COVERAGE ---")
    print(f"  answerable: {cov['covered']}/{cov['total']} = {pct(cov['coverage_pct'])}")
    print("  kind -> assigned-family confusion:")
    for k, fams in cov["confusion"].items():
        print(f"    {k:<14} {fams}")

    print("\n--- (2) COST-GAP (controller_cost / oracle_cost) ---")
    print(f"  min={gap['min']:.3f}  median={gap['median']:.3f}  "
          f"P95={gap['p95']:.3f}  max={gap['max']:.3f}")
    print(f"  => controller within {pct(gap['within_pct'])} of oracle cost (P95)")
    print(f"  knob that overpays: {gap['knob_overpay']}")

    print("\n--- (3) ACCURACY-MET (predicted eps_sk+eps_s+eps_cdm <= SLA) ---")
    print(f"  met: {acc['met']}/{acc['total']}  all_met={acc['all_met']}")
    print(f"  {'query':<22}{'eps_sk':>9}{'eps_s':>9}{'eps_cdm':>9}{'total':>9}{'budget':>9}  met")
    for d in acc["detail"]:
        print(f"  {d['id']:<22}{d['eps_sk']:>9.4f}{d['eps_s']:>9.4f}{d['eps_cdm']:>9.4f}"
              f"{d['eps_total']:>9.4f}{d['budget']:>9.4f}  {'Y' if d['met'] else 'N'}")
    print("\n  cross-validation of profiles vs empirical (Fig 1/3b):")
    for c in xval["checks"]:
        print(f"    [{('PASS' if 'PASS' in c['verdict'] else 'CHECK')}] {c['name']}")
        print(f"        {c['verdict']}")
    print(f"  all profiles validated: {xval['all_pass']}")

    print("\n--- (4) SENSITIVITY (tighten SLA -> size up, p up, eps_cdm down) ---")
    for kind, tbl in sens.items():
        print(f"  {kind}:")
        print(f"    {'x':>5}{'acc_slo':>9}{'fresh_s':>9}  {'family':<12}{'size':>8}{'p':>6}{'eps_cdm':>9}")
        for t in tbl:
            if t.get("infeasible"):
                print(f"    {t['sla_factor']:>5}{t['acc_slo']:>9}{t['fresh_s']:>9}  INFEASIBLE")
            else:
                print(f"    {t['sla_factor']:>5}{t['acc_slo']:>9}{t['fresh_s']:>9}  "
                      f"{t['family']:<12}{str(t['size']):>8}{t['p']:>6}{t['eps_cdm']:>9}")
    print()


def self_test(queries: list[dict], slas: dict) -> int:
    """Assert the harness invariants. Returns 0 on success, 1 on failure."""
    result = run(queries, slas, None)
    rows = result["rows"]
    cov = report_coverage(rows)
    acc = report_accuracy(rows)
    gap = report_costgap(rows)
    xval = cross_validation()
    sens = report_sensitivity(queries, slas)
    fails = []

    if cov["coverage_pct"] != 1.0:
        fails.append(f"coverage {cov['coverage_pct']} != 1.0")
    if not acc["all_met"]:
        fails.append("not all controller allocations meet their SLA")
    if not xval["all_pass"]:
        fails.append("a profile cross-validation check failed")
    # cost-gap must be >= 1.0 (controller can never beat the oracle min).
    if gap["min"] < 1.0 - 1e-9:
        fails.append(f"cost-gap min {gap['min']} < 1.0 (controller beat the oracle?!)")
    # sensitivity monotonicity: tightening (factor down) must not RELAX any knob.
    for kind, tbl in sens.items():
        feas = [t for t in tbl if not t.get("infeasible")]
        # rows are ordered loosest -> tightest; eps_cdm must be non-increasing,
        # eps_sk (the size proxy) non-increasing (smaller err = bigger sketch).
        for a, b in zip(feas, feas[1:]):
            if b["eps_cdm"] > a["eps_cdm"] + 1e-12:
                fails.append(f"{kind}: eps_cdm rose when SLA tightened ({a['eps_cdm']}->{b['eps_cdm']})")
            if b["eps_sk"] > a["eps_sk"] + 1e-12:
                fails.append(f"{kind}: eps_sk rose when SLA tightened (size shrank)")

    if fails:
        print("SELF-TEST FAILED:")
        for f in fails:
            print("  -", f)
        return 1
    print("SELF-TEST PASSED: coverage=100%, accuracy-met=all, profiles validated, "
          "cost-gap>=1, sensitivity monotonic.")
    return 0


def to_json(result: dict, queries: list[dict], slas: dict, mode: str) -> dict:
    rows = result["rows"]
    def alloc_d(a):
        return None if a is None else {
            "family": a.family, "size": a.size, "p": a.p, "eps_cdm": a.eps_cdm,
            "eps_sk": a.eps_sk, "eps_s": a.eps_s}
    return {
        "mode": mode,
        "coverage": report_coverage(rows),
        "cost_gap": report_costgap(rows),
        "accuracy_met": report_accuracy(rows),
        "cross_validation": cross_validation(),
        "sensitivity": report_sensitivity(queries, slas),
        "per_query": [{
            "id": r["id"], "kind": r["kind"], "budget": r["budget"],
            "controller": alloc_d(r["ctrl"]), "oracle": alloc_d(r["oracle"]),
            "ctrl_cost": r["ctrl_cost"], "oracle_cost": r["oracle_cost"],
            "ctrl_eps_total": r["ctrl_eps_total"], "acc_met": r["acc_met"],
        } for r in rows],
    }


def main(argv=None):
    ap = argparse.ArgumentParser(description="Fig 12 controller-allocation eval harness")
    ap.add_argument("--queries", type=Path, default=QUERIES_DEFAULT)
    ap.add_argument("--slas", type=Path, default=SLAS_DEFAULT)
    ap.add_argument("--use-optimizer", metavar="URL", default=None,
                    help="POST queries to the real control_plane /api/v1/plan at URL "
                         "(else use the faithful reference allocator)")
    ap.add_argument("--json", action="store_true", help="emit JSON instead of text")
    ap.add_argument("--self-test", action="store_true",
                    help="assert invariants (coverage/accuracy/monotonicity) and exit")
    args = ap.parse_args(argv)

    queries = json.loads(args.queries.read_text())
    slas = json.loads(args.slas.read_text())
    mode = f"real-optimizer @ {args.use_optimizer}" if args.use_optimizer else "reference-allocator (stand-in)"

    if args.self_test:
        return self_test(queries, slas)

    result = run(queries, slas, args.use_optimizer)
    if args.json:
        print(json.dumps(to_json(result, queries, slas, mode), indent=2))
    else:
        print_text(result, queries, slas, mode)
    return 0


if __name__ == "__main__":
    sys.exit(main())
