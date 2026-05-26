"""AWS price book — the single editable module for all USD rates.

Region: us-east-1 (N. Virginia), on-demand, Linux. Rates are public
list-price as of 2026-05; edit `DEFAULT_PRICING` (or pass a `Pricing` instance
into the model) to re-price for another region or for committed-use discounts.

Compute model
-------------
We decompose EC2 on-demand cost into a $/vCPU-hr + $/GB-RAM-hr pair (the
"resource-based" view, the same decomposition Fargate bills on and that AWS's
own Compute Optimizer uses). This lets the cost model charge each component for
the vCPU-fraction and memory it actually uses, instead of forcing whole-instance
rounding. The pair is derived from a reference general-purpose instance so the
blended $/hr matches a real instance:

    m6i.large : 2 vCPU, 8 GiB, $0.096/hr (us-east-1 on-demand).

Splitting that ~55/45 between compute and memory (AWS's rough internal split
for general-purpose) gives the per-vCPU-hr / per-GB-hr rates below. A whole-
instance rate is also provided for sensitivity checks.

S3
--
Standard tier, us-east-1: storage $/GB-month, plus $ per 1,000 PUT and per
1,000 GET requests. Request pricing is what makes the cold-archive object-count
matter: many tiny parts (pre-#442) => many PUTs; batched parts (#442) => ~60x
fewer.

Data transfer
-------------
Egress to internet $/GB (first 10 TB tier). Cross-AZ data transfer is billed
both directions at $0.01/GB each; we expose it separately so an edge->backend
hop that crosses an AZ can be charged.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Pricing:
    """AWS list prices, us-east-1 on-demand (2026-05). All editable."""

    region: str = "us-east-1"

    # ── EC2 compute (resource-decomposed) ──
    # Derived from m6i.large ($0.096/hr, 2 vCPU, 8 GiB) split ~55% compute / 45%
    # memory:  compute 0.0528/2 = 0.0264 /vCPU-hr ; memory 0.0432/8 = 0.0054 /GB-hr.
    ec2_per_vcpu_hour: float = 0.0264
    ec2_per_gb_ram_hour: float = 0.0054
    # Whole-instance reference rate (for the sensitivity/sanity column).
    ec2_reference_instance: str = "m6i.large"
    ec2_reference_per_hour: float = 0.096
    ec2_reference_vcpu: float = 2.0
    ec2_reference_gb: float = 8.0

    # ── S3 (Standard, us-east-1) ──
    s3_storage_per_gb_month: float = 0.023      # first 50 TB tier
    s3_put_per_1k: float = 0.005                # PUT/COPY/POST/LIST
    s3_get_per_1k: float = 0.0004               # GET/SELECT

    # ── EBS gp3 (us-east-1) — the block storage the raw TSDB (VM/Prometheus)
    #    keeps its samples on. Raw arms pay this for the full raw stream over
    #    retention; ASAP keeps only small warm state on EBS (cold goes to S3).
    ebs_gp3_per_gb_month: float = 0.08

    # ── Data transfer ──
    transfer_egress_per_gb: float = 0.09        # to internet, first 10 TB/mo tier
    transfer_cross_az_per_gb: float = 0.01      # each direction, intra-region

    # ── Component -> AZ-crossing toggle ──
    # If the edge->backend hop crosses an AZ (typical multi-AZ deploy) the wire
    # egress is charged at cross-AZ; if it's egress to the internet it's the
    # higher rate. The MVP demo is single-LAN (no charge), but the realistic
    # cloud deploy crosses an AZ, so default to cross-AZ for the wire hop.
    wire_uses_internet_egress: bool = False     # False => cross-AZ rate on the wire

    def wire_transfer_per_gb(self) -> float:
        return (
            self.transfer_egress_per_gb
            if self.wire_uses_internet_egress
            else self.transfer_cross_az_per_gb
        )

    def ec2_cost_per_hour(self, vcpu: float, gb_ram: float) -> float:
        """USD/hour for a component using `vcpu` vCPU-fraction and `gb_ram` GiB."""
        return vcpu * self.ec2_per_vcpu_hour + gb_ram * self.ec2_per_gb_ram_hour


DEFAULT_PRICING = Pricing()


# A couple of alternate reference instances, handy when a reviewer wants to
# re-price against compute- or memory-optimised families. Not wired into the
# model by default; copy the numbers into a Pricing(...) override to use.
ALT_INSTANCE_RATES = {
    # name: (per_hour, vcpu, gib)
    "m6i.large": (0.096, 2, 8),      # general purpose (reference)
    "c6i.xlarge": (0.170, 4, 8),     # compute optimised (matches tco.rs default)
    "r6i.large": (0.126, 2, 16),     # memory optimised
    "t3.medium": (0.0416, 2, 4),     # burstable (cheap edge agent candidate)
}
