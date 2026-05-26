"""Workload + ASAP-config dataclasses, and the MVP-multinode preset.

A `Workload` is the data-generation operating point (cardinality, sample rate,
metric set) — shared by every arm so the comparison is apples-to-apples. An
`ASAPConfig` is the ASAP-only knob set (per-metric sketch family, sampling p,
cold format, flush cadence) that only the asap / asap-gzip arms read.

The `mvp_workload()` preset mirrors deploy/mvp-multinode/topology.env and
configs/asap/mvp-workload.yaml: 2 agent-nodes x 5 producers x 1000 series at
10 Hz, the 2-metric bandwidth-sweep operating point that FINDINGS measured.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List


# Valid arms (matches run_demo.sh).
ARMS = ["b0", "b1", "b2", "b3", "asap", "asap-gzip"]
RAW_ARMS = ["b0", "b1", "b2", "b3"]
ASAP_ARMS = ["asap", "asap-gzip"]


@dataclass
class MetricSpec:
    """One metric in the workload + how ASAP treats it."""

    name: str
    # Fraction of total series this metric contributes (the producer emits all
    # metrics over the same label sets, so each metric == `series` series).
    # Default: every metric spans the full cardinality.
    sketch_family: str = "ddsketch"   # ddsketch|kll|hll|countsketch|countminsketch|sum
    # ASAP aggregate_by collapses series into groups for the warm tier (e.g.
    # [zone] => 4 groups). Empty list => per-series (no collapse).
    aggregate_by: List[str] = field(default_factory=list)
    tier: str = "both"                # warm|cold|both
    sample_p: float = 1.0             # per-metric sampling probability (#441)


@dataclass
class Workload:
    """Data-generation operating point — shared by all arms."""

    name: str
    series: int                       # total active time series (across all nodes)
    sample_hz: float                  # samples/sec per series (scrape freq)
    metrics: List[MetricSpec] = field(default_factory=list)
    # Group cardinality for edge aggregation (e.g. distinct zones). Used to size
    # the asap aggregated output for metrics with aggregate_by.
    group_cardinality: int = 4        # DESIGN: otel-app -zone-vals=4
    retention_days: int = 30          # cold-archive retention (S3 storage months)
    # query rate against the backend (GETs from thanos store-gateway on cold reads)
    queries_per_sec: float = 1.0

    @property
    def raw_samples_per_sec(self) -> float:
        return self.series * self.sample_hz * max(1, len(self.metrics))


@dataclass
class ASAPConfig:
    """ASAP-only configuration knobs (asap / asap-gzip arms)."""

    # Cold object format: "intchunk" (best-of-N INT codec, batched per #442) or
    # "fragment" (Gorilla-XOR fragments). Drives cold S3 stored bytes + PUT $.
    cold_format: str = "intchunk"     # intchunk|fragment
    # Whether intchunk parts are batched per block_duration (#442, ~60x fewer
    # PUTs) or shipped per window_interval (pre-#442 tiny parts).
    cold_part_batched: bool = True
    # Warm flush / cold block cadence (seconds).
    window_duration_sec: float = 60.0
    # Global default sampling probability applied to metrics that don't set one.
    default_sample_p: float = 1.0

    def effective_sample_p(self, metric: MetricSpec) -> float:
        return metric.sample_p if metric.sample_p != 1.0 else self.default_sample_p


def mvp_workload() -> Workload:
    """The MVP-multinode bandwidth-sweep operating point (FINDINGS calibration).

    topology.env: PER_AGENT_CARDINALITY=1000, -freq-hz=10 (per the
    workload.yaml / runbook bandwidth operating point), 5 producers/node x
    2 agent-nodes => 10_000 series. the five-sketch gate (removed) => the 2 sweep
    metrics only: the http_requests_total counter (Sum-by-zone) and the
    latency gauge (DDSketch quantile).
    """
    return Workload(
        name="mvp",
        series=10_000,
        sample_hz=10.0,
        group_cardinality=4,
        retention_days=30,
        metrics=[
            # Sum-role counter: edge-aggregated to per-zone sums (4 groups).
            MetricSpec(
                name="http_requests_total",
                sketch_family="sum",
                aggregate_by=["zone"],
                tier="both",
            ),
            # Latency gauge: per-series DDSketch (no aggregate_by) for
            # apples-to-apples quantile accuracy. tier=both => also cold-archived.
            MetricSpec(
                name="http_requests_total_latency_ms",
                sketch_family="ddsketch",
                aggregate_by=[],
                tier="both",
            ),
        ],
    )


def mvp_five_sketch_workload() -> Workload:
    """The full 5-sketch coverage workload (configs/asap/mvp-workload.yaml).

    Adds the HLL / KLL / CountSketch / CountMinSketch metrics. Heavier than the
    bandwidth-sweep operating point (FINDINGS ran with FIVE_SKETCH=off), so its
    wire/CPU are EXTRAPOLATED off the calibration point, not directly measured.
    """
    w = mvp_workload()
    w.name = "mvp-5sketch"
    w.metrics += [
        MetricSpec("request_size_bytes", "kll", [], "both"),
        MetricSpec("unique_users_per_min", "hll", ["zone"], "warm"),
        MetricSpec("top_endpoint_qps", "countsketch", ["zone"], "warm"),
        MetricSpec("endpoint_request_freq", "countminsketch", ["zone"], "warm"),
    ]
    return w


PRESETS: Dict[str, "callable"] = {
    "mvp": mvp_workload,
    "mvp-5sketch": mvp_five_sketch_workload,
}
