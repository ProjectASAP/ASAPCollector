use std::collections::HashMap;
use std::time::Duration;
use chrono::{DateTime, Utc};

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
            AggType::Quantile    => write!(f, "quantile"),
            AggType::Cardinality => write!(f, "cardinality"),
            AggType::Frequency   => write!(f, "frequency"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
            SketchType::DDSketch       => write!(f, "ddsketch"),
            SketchType::KLL            => write!(f, "kll"),
            SketchType::HLL            => write!(f, "hll"),
            SketchType::CountSketch    => write!(f, "countsketch"),
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
            ProcessorMode::Batch  => write!(f, "batch"),
            ProcessorMode::Window => write!(f, "window"),
        }
    }
}

// ── Core types ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct QueryWorkload {
    pub metric_name:    String,
    pub label_filters:  HashMap<String, String>,
    pub group_by_labels: Vec<String>,
    pub aggregations:   Vec<AggType>,
    pub time_window:    Duration,
    pub repeat_every:   Option<Duration>,
    pub accuracy_sla:   f64,
    pub latency_sla:    Option<Duration>,
}

#[derive(Debug, Clone, Default)]
pub struct SketchParams {
    pub relative_accuracy: f64,
    pub k:                 u32,
    pub precision:         u32,
    pub rows:              u32,
    pub cols:              u32,
    pub quantiles:         Vec<f64>,
}

#[derive(Debug, Clone)]
pub struct AgentCollectorConfig {
    pub output_mode:     OutputMode,
    pub sketch_type:     SketchType,
    pub sketch_params:   SketchParams,
    pub aggregate_by:    Vec<String>,
    pub label_matchers:  Vec<String>,
    pub window_duration: Option<Duration>,
    pub mode:            ProcessorMode,
    pub transmit_sketch: bool,
    pub drop_original:   bool,
}

#[derive(Debug, Clone)]
pub struct GatewayCollectorConfig {
    pub passthrough: bool,
}

#[derive(Debug, Clone)]
pub struct BackendCollectorConfig {
    pub merge_sketch_type: SketchType,
    pub group_by:          Vec<String>,
}

#[derive(Debug, Clone)]
pub struct PrecomputeJob {
    pub query_expr:    String,
    pub granularity:   Duration,
    pub sketch_source: String,
    pub store_path:    String,
}

#[derive(Debug, Clone)]
pub struct CollectionPlan {
    pub agent_config:   AgentCollectorConfig,
    pub gateway_config: GatewayCollectorConfig,
    pub backend_config: BackendCollectorConfig,
    pub precompute:     Vec<PrecomputeJob>,
    pub valid_until:    DateTime<Utc>,
}
