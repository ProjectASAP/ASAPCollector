"""Per-component x per-resource resource model + USD mapping.

For an (arm, workload, asap_config) triple this produces:

  1. a ResourceTable: per COMPONENT x per RESOURCE physical quantities
       (vCPU, memory GiB, egress GB/month, S3 stored GB, S3 PUT/GET counts/mo),
  2. a CostTable: each resource priced in USD/month via the AWS price book.

The components match run_demo.sh's deployed containers:

  raw arms (b0..b3):  raw_agent, vm  (+ serf_gateway for b3)
  asap arms:          edge_agent, data_plane, gorilla_merger, minio,
                      thanos_query, thanos_store_gateway, thanos_compact,
                      controller

Producers are excluded — they are the workload SOURCE, identical across all
arms, so they cancel out of the comparison (the cost story is about the
telemetry pipeline, not the application emitting metrics).

Resource scaling
----------------
Each component's calibrated point (coefficients.*_AT_CAL) is at CAL_SERIES /
CAL_SAMPLE_HZ. We scale:

  CPU  = idle_fraction*cal + (1-idle_fraction)*cal * (throughput / cal_throughput)
  MEM  = idle_fraction*cal + (1-idle_fraction)*cal * (series / cal_series)
  WIRE = cal_mbps scaled by raw-sample-rate (raw arms) or series-state (asap)

Sampling p shrinks the throughput-scaled CPU on the asap edge agent.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

from . import coefficients as C
from .pricing import Pricing, DEFAULT_PRICING
from .workloads import Workload, ASAPConfig, ASAP_ARMS, RAW_ARMS


# Resource axis names (columns of the per-component table).
RESOURCES = [
    "cpu_vcpu",          # average vCPU-fraction consumed
    "mem_gb",            # GiB resident
    "egress_gb_mo",      # GB of wire egress per month (edge->backend hop)
    "ebs_gb",            # GB on block storage (raw TSDB / ASAP warm tier)
    "s3_storage_gb",     # GB resident in object storage (avg over retention)
    "s3_put_per_mo",     # PUT request count / month
    "s3_get_per_mo",     # GET request count / month
]

# Which components belong to each arm.
ARM_COMPONENTS = {
    "b0": ["raw_agent", "vm"],
    "b1": ["raw_agent", "vm"],
    "b2": ["raw_agent", "vm"],
    "b3": ["raw_agent", "serf_gateway", "vm"],
    "asap": [
        "edge_agent",
        "data_plane",
        "gorilla_merger",
        "minio",
        "thanos_query",
        "thanos_store_gateway",
        "thanos_compact",
        "controller",
    ],
    "asap-gzip": [
        "edge_agent",
        "data_plane",
        "gorilla_merger",
        "minio",
        "thanos_query",
        "thanos_store_gateway",
        "thanos_compact",
        "controller",
    ],
}

# Which components actually move telemetry over the priced wire hop. For raw
# arms the wire egress is charged at the raw agent (edge -> VM). For asap arms
# the priced wire is the edge_agent -> data_plane sketch hop.
WIRE_COMPONENT = {
    "b0": "raw_agent", "b1": "raw_agent", "b2": "raw_agent", "b3": "raw_agent",
    "asap": "edge_agent", "asap-gzip": "edge_agent",
}


@dataclass
class ComponentResources:
    component: str
    values: Dict[str, float] = field(default_factory=dict)

    def get(self, r: str) -> float:
        return self.values.get(r, 0.0)


@dataclass
class ResourceTable:
    arm: str
    rows: List[ComponentResources] = field(default_factory=list)

    def total(self, resource: str) -> float:
        return sum(r.get(resource) for r in self.rows)


# ─────────────────────────────────────────────────────────────────────────────
# Resource scaling primitives.
# ─────────────────────────────────────────────────────────────────────────────

def _scaled_cpu(component: str, throughput_ratio: float) -> float:
    cal = C.CPU_VCPU_AT_CAL.get(component, 0.0)
    idle = C.CPU_IDLE_FRACTION.get(component, 0.5)
    return cal * idle + cal * (1.0 - idle) * throughput_ratio


def _scaled_mem(component: str, series_ratio: float) -> float:
    cal = C.MEM_GB_AT_CAL.get(component, 0.0)
    idle = C.MEM_IDLE_FRACTION.get(component, 0.5)
    return cal * idle + cal * (1.0 - idle) * series_ratio


# ─────────────────────────────────────────────────────────────────────────────
# Wire bandwidth (Mbps) for an arm at a given workload.
# ─────────────────────────────────────────────────────────────────────────────

def wire_mbps(arm: str, w: Workload) -> float:
    """Edge wire bandwidth in Mbps, scaled off the FINDINGS calibration point.

    Raw arms (b0..b3): wire scales linearly with raw sample rate (series x Hz x
    metrics) relative to the calibration point — raw ships every sample.
    asap arms: wire is dominated by per-window aggregated sketch state, which
    scales with series (sketch states in flight) and only weakly with Hz.
    """
    cal_mbps = C.WIRE_MBPS_AT_CAL[arm]
    # CAL_RAW_SAMPLES_PER_SEC already includes the metric count (series*Hz*metrics),
    # matching w.raw_samples_per_sec, so the ratio is 1.0 at the MVP preset.
    cal_rate = C.CAL_RAW_SAMPLES_PER_SEC
    raw_rate = w.raw_samples_per_sec
    if arm in RAW_ARMS:
        return cal_mbps * (raw_rate / cal_rate)
    # asap: scale with series count, weak Hz dependence.
    series_ratio = w.series / C.CAL_SERIES
    hz_ratio = (w.sample_hz / C.CAL_SAMPLE_HZ) ** C.ASAP_WIRE_HZ_EXPONENT
    metric_ratio = max(1, len(w.metrics)) / C.CAL_METRICS
    return cal_mbps * series_ratio * hz_ratio * metric_ratio


def _mbps_to_gb_per_month(mbps: float) -> float:
    bytes_per_sec = mbps * C.MBIT / C.BITS_PER_BYTE
    return bytes_per_sec * C.SECONDS_PER_MONTH / C.BYTES_PER_GB


# ─────────────────────────────────────────────────────────────────────────────
# Cold archive (asap only): stored bytes + S3 request counts.
# ─────────────────────────────────────────────────────────────────────────────

def cold_raw_mbps(w: Workload) -> float:
    """Encoded cold stream (Gorilla-XOR fragment) in Mbps, scaled by raw rate.

    Only metrics with tier in {cold, both} contribute. The calibration figure
    (~3.57 Mbps) was on the 2-metric workload where both are cold-archived.
    """
    cold_metrics = [m for m in w.metrics if m.tier in ("cold", "both")]
    cal_cold_metrics = C.CAL_METRICS  # both calibration metrics were cold-archived
    rate_ratio = (w.series * w.sample_hz * max(1, len(cold_metrics))) / (
        C.CAL_SERIES * C.CAL_SAMPLE_HZ * cal_cold_metrics
    )
    return C.COLD_RAW_MBPS_AT_CAL * rate_ratio


def cold_storage_gb_avg(w: Workload, cfg: ASAPConfig) -> float:
    """Average GB resident in object storage over the retention window.

    The cold stream accumulates at the encoded write rate; with a fixed
    retention the steady-state resident volume is rate * retention_seconds.
    intchunk stores SMALLER than Gorilla-XOR per the codec ratio.
    """
    write_mbps = cold_raw_mbps(w)
    store_ratio = (
        C.INTCHUNK_STORE_RATIO if cfg.cold_format == "intchunk" else C.GORILLA_XOR_STORE_RATIO
    )
    write_bytes_per_sec = write_mbps * C.MBIT / C.BITS_PER_BYTE * store_ratio
    retention_sec = w.retention_days * 86_400.0
    return write_bytes_per_sec * retention_sec / C.BYTES_PER_GB


def cold_puts_per_month(w: Workload, cfg: ASAPConfig) -> float:
    """S3 PUT count/month for the cold archive.

    One PUT per (shard x seal). Seal cadence:
      intchunk batched (#442): every block_duration (default 60s).
      intchunk un-batched / fragment per-window: every window_interval (5s).
    => batching cuts PUTs by block_duration/window_interval (~12x at the
    deployed cadences; the headline "~60x" is window_duration(60s) vs the
    ~1s per-emit density the pre-#442 path actually hit, captured by setting
    cold_part_batched=False with a 1s effective seal).
    """
    if cfg.cold_format == "intchunk" and cfg.cold_part_batched:
        seal_period = cfg.window_duration_sec  # one part per block
    elif cfg.cold_format == "fragment":
        seal_period = C.COLD_WINDOW_INTERVAL_SEC  # fragment ships per window
    else:
        # intchunk un-batched: the pathological tiny-part path (~per-emit).
        seal_period = 1.0
    seals_per_sec = 1.0 / seal_period
    puts_per_sec = seals_per_sec * C.COLD_PUT_OBJECTS_PER_SEAL
    return puts_per_sec * C.SECONDS_PER_MONTH


def thanos_store_blocks(w: Workload, cfg: ASAPConfig) -> float:
    """Approximate count of distinct blocks resident in object storage.

    Blocks are sealed every block_duration; thanos-compact merges them but the
    store-gateway syncs the live set. Used to drive GET counts.
    """
    retention_sec = w.retention_days * 86_400.0
    # post-compaction the long tail merges into 2h blocks; approximate live
    # block count as retention / 2h plus the recent un-compacted block_duration tail.
    two_hours = 7200.0
    compacted = retention_sec / two_hours
    recent = (two_hours / cfg.window_duration_sec)  # un-compacted recent blocks
    return compacted + recent


def thanos_gets_per_month(w: Workload, cfg: ASAPConfig) -> float:
    """S3 GET count/month: store-gateway block discovery + query-time chunk reads.

    The store-gateway does NOT re-GET every resident block every sync — it
    caches block index-headers in memory and only fetches meta.json (+ the
    index header once) for NEWLY appeared blocks. So the steady-state sync GET
    rate is driven by the block CREATION rate, not the resident block count.
    """
    # New blocks created per month (one per block_duration seal of the cold
    # stream; thanos-compact later merges them but each is discovered once).
    new_blocks_per_month = C.SECONDS_PER_MONTH / cfg.window_duration_sec
    discovery_gets = new_blocks_per_month * C.THANOS_GETS_PER_BLOCK_SYNC
    # query-time GETs: ~1% of queries hit the cold tier, each touches a few blocks.
    query_gets = w.queries_per_sec * C.SECONDS_PER_MONTH * 0.01 * 5.0
    return discovery_gets + query_gets


def raw_tsdb_storage_gb_avg(arm: str, w: Workload) -> float:
    """Average GB on the raw TSDB's EBS volume over retention (b0..b3 VM)."""
    raw_bytes_per_sec = w.raw_samples_per_sec * C.RAW_TSDB_BYTES_PER_SAMPLE
    retention_sec = w.retention_days * 86_400.0
    return raw_bytes_per_sec * retention_sec / C.BYTES_PER_GB


def asap_warm_ebs_gb(w: Workload, cfg: ASAPConfig) -> float:
    """ASAP warm-tier EBS footprint — a few windows of sketch state, not history."""
    total = 0.0
    for m in w.metrics:
        if m.tier not in ("warm", "both"):
            continue
        state = C.SKETCH_STATE_BYTES.get(m.sketch_family, 4096)
        groups = w.group_cardinality if m.aggregate_by else w.series
        total += state * groups
    return total * C.ASAP_WARM_EBS_WINDOWS_RESIDENT / C.BYTES_PER_GB


def thanos_compact_puts_per_month(w: Workload, cfg: ASAPConfig) -> float:
    """PUTs from the compactor rewriting/downsampling blocks."""
    base = cold_puts_per_month(w, cfg)
    return base * C.THANOS_COMPACT_REWRITE_FACTOR * 0.1  # compactor rewrites ~10% of churn


# ─────────────────────────────────────────────────────────────────────────────
# Build the per-component resource table.
# ─────────────────────────────────────────────────────────────────────────────

def _edge_sampling_factor(arm: str, w: Workload, cfg: ASAPConfig) -> float:
    """Multiplier on edge-agent throughput-scaled CPU from per-metric sampling."""
    if arm not in ASAP_ARMS:
        return 1.0
    # Average effective sample_p across warm metrics, raised to the CPU exponent.
    warm = [m for m in w.metrics if m.tier in ("warm", "both")]
    if not warm:
        return 1.0
    avg_p = sum(cfg.effective_sample_p(m) for m in warm) / len(warm)
    return avg_p ** C.SAMPLING_CPU_EXPONENT


def build_resource_table(
    arm: str, w: Workload, cfg: ASAPConfig | None = None
) -> ResourceTable:
    if cfg is None:
        cfg = ASAPConfig()
    if arm not in ARM_COMPONENTS:
        raise ValueError(f"unknown arm {arm!r}; valid: {list(ARM_COMPONENTS)}")

    throughput_ratio = w.raw_samples_per_sec / C.CAL_RAW_SAMPLES_PER_SEC
    series_ratio = w.series / C.CAL_SERIES
    table = ResourceTable(arm=arm)

    wire_gb_mo = _mbps_to_gb_per_month(wire_mbps(arm, w))
    sampling = _edge_sampling_factor(arm, w, cfg)

    for comp in ARM_COMPONENTS[arm]:
        vals: Dict[str, float] = {r: 0.0 for r in RESOURCES}

        cpu = _scaled_cpu(comp, throughput_ratio)
        if comp == "edge_agent":
            cpu *= sampling  # sampling shrinks the warm per-sample work
        vals["cpu_vcpu"] = cpu
        vals["mem_gb"] = _scaled_mem(comp, series_ratio)

        # Wire egress charged once, at the wire component.
        if comp == WIRE_COMPONENT[arm]:
            vals["egress_gb_mo"] = wire_gb_mo

        # Raw arms: the VM/Prometheus sink keeps the full raw stream on EBS.
        if arm in RAW_ARMS and comp == "vm":
            vals["ebs_gb"] = raw_tsdb_storage_gb_avg(arm, w)

        # S3 storage + requests live on the object-store + thanos components.
        if arm in ASAP_ARMS:
            if comp == "data_plane":
                # ASAP warm tier keeps only recent sketch state on EBS (not history).
                vals["ebs_gb"] = asap_warm_ebs_gb(w, cfg)
            if comp == "minio":
                # MinIO models the S3 backend: resident storage = cold archive.
                vals["s3_storage_gb"] = cold_storage_gb_avg(w, cfg)
            if comp == "gorilla_merger":
                # The merger/edge cold path writes the parts: charge cold PUTs.
                vals["s3_put_per_mo"] = cold_puts_per_month(w, cfg)
            if comp == "thanos_store_gateway":
                vals["s3_get_per_mo"] = thanos_gets_per_month(w, cfg)
            if comp == "thanos_compact":
                vals["s3_put_per_mo"] = thanos_compact_puts_per_month(w, cfg)

        table.rows.append(ComponentResources(component=comp, values=vals))

    return table


# ─────────────────────────────────────────────────────────────────────────────
# Map resources -> USD/month.
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ComponentCost:
    component: str
    # per-resource USD/month
    compute_usd: float = 0.0
    storage_usd: float = 0.0
    request_usd: float = 0.0
    egress_usd: float = 0.0

    @property
    def total_usd(self) -> float:
        return self.compute_usd + self.storage_usd + self.request_usd + self.egress_usd


@dataclass
class CostTable:
    arm: str
    rows: List[ComponentCost] = field(default_factory=list)

    @property
    def total_usd(self) -> float:
        return sum(r.total_usd for r in self.rows)

    def by_category(self) -> Dict[str, float]:
        return {
            "compute": sum(r.compute_usd for r in self.rows),
            "storage": sum(r.storage_usd for r in self.rows),
            "requests": sum(r.request_usd for r in self.rows),
            "egress": sum(r.egress_usd for r in self.rows),
        }


def price_resource_table(rt: ResourceTable, pricing: Pricing | None = None) -> CostTable:
    p = pricing or DEFAULT_PRICING
    ct = CostTable(arm=rt.arm)
    for row in rt.rows:
        compute = p.ec2_cost_per_hour(row.get("cpu_vcpu"), row.get("mem_gb")) * C.HOURS_PER_MONTH
        storage = (
            row.get("s3_storage_gb") * p.s3_storage_per_gb_month
            + row.get("ebs_gb") * p.ebs_gp3_per_gb_month
        )
        requests = (
            row.get("s3_put_per_mo") / 1000.0 * p.s3_put_per_1k
            + row.get("s3_get_per_mo") / 1000.0 * p.s3_get_per_1k
        )
        egress = row.get("egress_gb_mo") * p.wire_transfer_per_gb()
        ct.rows.append(
            ComponentCost(
                component=row.component,
                compute_usd=compute,
                storage_usd=storage,
                request_usd=requests,
                egress_usd=egress,
            )
        )
    return ct


def simulate(
    arm: str, w: Workload, cfg: ASAPConfig | None = None, pricing: Pricing | None = None
) -> tuple[ResourceTable, CostTable]:
    rt = build_resource_table(arm, w, cfg)
    ct = price_resource_table(rt, pricing)
    return rt, ct
