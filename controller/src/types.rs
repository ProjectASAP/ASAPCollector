use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

// ── Workload characteristics ───────────────────────────────────────────────────

/// Hint about the statistical distribution of keys in the data stream.
/// Affects fill-rate estimation and therefore delta compression projections.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DataDistribution {
    /// Zipf-distributed keys (s ≈ 1.1).  A small number of keys dominate,
    /// so only a fraction of sketch cells are touched per window.  This is
    /// the typical production case.
    #[default]
    Zipf,
    /// All keys are equally probable.  Every window fills the sketch more
    /// uniformly; delta compression benefit is lower.
    Uniform,
    /// Traffic arrives in bursts with a concentrated key set.  Effective
    /// fill rate is lower on average but spikes can reach Uniform levels.
    Bursty,
}

/// Observable characteristics of the incoming data stream.
///
/// Callers supply these alongside a [`QueryWorkload`] so the planner can
/// compare raw vs. sketch-full vs. sketch-delta transmission costs and
/// estimate the CPU / memory overhead at the SDK or agent collector.
///
/// All fields have conservative defaults so callers can omit the struct
/// entirely and still get a valid (if pessimistic) decision.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadCharacteristics {
    /// Number of distinct active time series for this metric.
    pub series_count: u64,
    /// Sample rate per series at the SDK / agent (Hz).
    pub samples_per_sec_per_series: f64,
    /// Wire size of one raw OTLP metric data point after protobuf encoding
    /// (bytes).  Typical range: 50–200 bytes.
    pub bytes_per_raw_sample: u32,
    /// Known distinct key values per flush period for frequency / cardinality
    /// sketches.  `None` → inferred analytically from inserts and distribution.
    pub distinct_keys_per_window: Option<u64>,
    /// Statistical distribution of keys in the stream.
    pub data_distribution: DataDistribution,
    /// Optional memory cap at the SDK / agent collector (bytes).
    /// `None` → no budget constraint applied.
    pub memory_budget_bytes: Option<u64>,
}

impl Default for WorkloadCharacteristics {
    fn default() -> Self {
        Self {
            series_count: 1_000,
            samples_per_sec_per_series: 100.0,
            bytes_per_raw_sample: 100,
            distinct_keys_per_window: None,
            data_distribution: DataDistribution::Zipf,
            memory_budget_bytes: None,
        }
    }
}

// ── Delta transmission decision ────────────────────────────────────────────────

/// Reason the planner chose not to enable delta encoding.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeltaSkipReason {
    /// Estimated fill rate is so high that delta compression ratio < 2×,
    /// making snapshot overhead unjustifiable.
    FillRateTooHigh,
    /// Snapshot memory for all series × sketches would exceed the configured
    /// memory budget at the agent.
    MemoryBudgetExceeded,
    /// This sketch type has no delta implementation (e.g. KLL).
    SketchTypeUnsupported,
    /// Compression ratio fell below the minimum acceptable threshold.
    CompressionRatioBelowThreshold,
}

/// Reason the planner chose raw pass-through over sketch transmission.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RawDataReason {
    /// Series count × sample rate is so small that sketch CPU / memory
    /// overhead is not justified by the bandwidth savings.
    WorkloadTooSmall,
}

/// The controller's resolved decision on transmission mode.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum DeltaDecision {
    /// Enable delta-compressed sketch payloads.
    UseDelta {
        /// Minimum absolute cell change included in a delta payload (T).
        threshold: f64,
        /// Estimated compression ratio (full_bytes / delta_bytes).
        estimated_compression_ratio: f64,
        /// Estimated outbound bandwidth with delta enabled (bytes/sec).
        estimated_delta_bytes_per_sec: f64,
        /// Additional CPU at the agent per sample due to snapshot diff and
        /// sparse encoding (µs/sample), amortised over the flush period.
        delta_cpu_overhead_micros_per_sample: f64,
        /// Additional memory at the agent for storing snapshots (bytes).
        delta_memory_overhead_bytes: f64,
    },
    /// Transmit full (non-delta) sketch payloads each flush.
    UseFullSketch {
        reason: DeltaSkipReason,
        /// Estimated outbound bandwidth with full sketches (bytes/sec).
        estimated_full_bytes_per_sec: f64,
    },
    /// Skip sketch aggregation; pass raw OTLP samples through.
    UseRaw {
        reason: RawDataReason,
        /// Estimated outbound bandwidth with raw samples (bytes/sec).
        estimated_raw_bytes_per_sec: f64,
    },
}

impl Default for DeltaDecision {
    fn default() -> Self {
        DeltaDecision::UseFullSketch {
            reason: DeltaSkipReason::CompressionRatioBelowThreshold,
            estimated_full_bytes_per_sec: 0.0,
        }
    }
}

/// Bandwidth and overhead estimates for all three transmission strategies.
/// Carried on every [`CollectionPlan`] for observability and debugging.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TransmissionCostSummary {
    /// Raw OTLP pass-through bandwidth (bytes/sec).
    pub raw_bytes_per_sec: f64,
    /// Full-sketch transmission bandwidth (bytes/sec).
    pub sketch_full_bytes_per_sec: f64,
    /// Delta-sketch transmission bandwidth (bytes/sec); 0 if delta not viable.
    pub sketch_delta_bytes_per_sec: f64,
    /// Extra CPU at the agent per sample in delta mode (µs/sample).
    pub delta_cpu_overhead_micros_per_sample: f64,
    /// Extra memory at the agent for delta snapshots (bytes).
    pub delta_memory_overhead_bytes: f64,
    /// Estimated fill rate (fraction of sketch cells changed per flush).
    pub estimated_fill_rate: f64,
    /// Flush rate derived from window_duration or repeat_every (Hz).
    pub flush_rate_hz: f64,
}

// ── Enumerations ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AggType {
    Quantile,
    Cardinality,
    Frequency,
}

impl std::fmt::Display for AggType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggType::Quantile => write!(f, "quantile"),
            AggType::Cardinality => write!(f, "cardinality"),
            AggType::Frequency => write!(f, "frequency"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SketchType {
    DDSketch,
    KLL,
    HLL,
    CountSketch,
    CountMinSketch,
}

impl std::fmt::Display for SketchType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SketchType::DDSketch => write!(f, "ddsketch"),
            SketchType::KLL => write!(f, "kll"),
            SketchType::HLL => write!(f, "hll"),
            SketchType::CountSketch => write!(f, "countsketch"),
            SketchType::CountMinSketch => write!(f, "countminsketch"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum OutputMode {
    Raw,
    Sketch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessorMode {
    Batch,
    Window,
}

impl std::fmt::Display for ProcessorMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProcessorMode::Batch => write!(f, "batch"),
            ProcessorMode::Window => write!(f, "window"),
        }
    }
}

// ── Core types ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct QueryWorkload {
    pub metric_name: String,
    pub label_filters: HashMap<String, String>,
    pub group_by_labels: Vec<String>,
    pub aggregations: Vec<AggType>,
    pub time_window: Duration,
    pub repeat_every: Option<Duration>,
    pub accuracy_sla: f64,
    pub latency_sla: Option<Duration>,
    /// When set, the planner must use this sketch type instead of running
    /// the cost model. Allows pinning for collectors that support a subset.
    pub sketch_type_override: Option<SketchType>,
}

#[derive(Debug, Clone, Default)]
pub struct SketchParams {
    pub relative_accuracy: f64,
    pub k: u32,
    pub precision: u32,
    pub rows: u32,
    pub cols: u32,
    pub quantiles: Vec<f64>,
}

#[derive(Debug, Clone)]
pub struct AgentCollectorConfig {
    pub output_mode: OutputMode,
    pub sketch_type: SketchType,
    pub sketch_params: SketchParams,
    pub aggregate_by: Vec<String>,
    pub label_matchers: Vec<String>,
    pub window_duration: Option<Duration>,
    pub mode: ProcessorMode,
    pub enable_self_monitoring: bool,
    pub transmit_sketch: bool,
    pub drop_original: bool,
    /// Whether the agent processor should enable delta encoding.
    /// Set by the delta cost model after sketch type selection.
    pub delta_transmission: bool,
    /// Minimum absolute cell change included in a delta payload (T).
    /// Ignored when `delta_transmission` is false.
    pub delta_threshold: f64,
}

#[derive(Debug, Clone)]
pub struct GatewayCollectorConfig {
    pub passthrough: bool,
}

#[derive(Debug, Clone)]
pub struct BackendCollectorConfig {
    pub merge_sketch_type: SketchType,
    pub group_by: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct PrecomputeJob {
    pub query_expr: String,
    pub granularity: Duration,
    pub sketch_source: String,
    pub store_path: String,
}

#[derive(Debug, Clone)]
pub struct CollectionPlan {
    pub agent_config: AgentCollectorConfig,
    pub gateway_config: GatewayCollectorConfig,
    pub backend_config: BackendCollectorConfig,
    pub precompute: Vec<PrecomputeJob>,
    pub valid_until: DateTime<Utc>,
    /// Resolved delta transmission decision and rationale.
    pub delta_decision: DeltaDecision,
    /// Bandwidth and overhead estimates for all three transmission strategies.
    pub transmission_cost_summary: TransmissionCostSummary,
}
