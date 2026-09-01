//! Validation and runtime projection for ASAPQuery's MVP `CollectorPlan`.
//!
//! This boundary consumes a physical decision; it never parses queries or
//! chooses a different sketch family. Invalid or unsupported plans are rejected
//! as a unit before a [`PrecomputeConfigSet`](crate::config::PrecomputeConfigSet)
//! is exposed to a runtime.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::config::{AggregationMode, PrecomputeConfig, PrecomputeConfigSet, WindowSpec};
use crate::envelope::{Encoding, SketchType};

/// Shared physical-plan identity emitted by ASAPQuery.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlanEnvelope {
    /// Stable content-derived plan identifier.
    pub plan_id: u64,
    /// Compilation timestamp.
    pub generated_at_unix_ms: u64,
    /// Exact ASAPPlanner revision used for candidate selection.
    pub planner_revision: String,
    /// Capability snapshot against which the plan was compiled.
    pub capability_snapshot_id: String,
}

/// One executable materialization assigned to this Collector.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CollectorMaterialization {
    /// Stable workload query identifier.
    pub query_id: String,
    /// Exact source metric name.
    pub metric: String,
    /// Committed Planner sketch algorithm.
    pub algorithm: String,
    /// Committed typed algorithm parameters.
    pub parameters: Value,
    /// Labels retained as independent subpopulations.
    pub group_by: Vec<String>,
    /// Tumbling-window duration.
    pub window_secs: u64,
    /// Evidence source for evidence-gated selections such as TopK.
    pub evidence_source: Option<String>,
}

/// Complete per-target physical plan emitted by ASAPQuery.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CollectorPlan {
    /// Collector instance this plan may be applied to.
    pub collector_id: String,
    /// Shared bundle identity.
    pub envelope: PlanEnvelope,
    /// Complete materialization set for this target.
    pub materializations: Vec<CollectorMaterialization>,
}

/// Fail-closed CollectorPlan validation errors.
#[derive(Debug, Error, PartialEq)]
pub enum CollectorPlanError {
    /// JSON decoding failed.
    #[error("invalid CollectorPlan JSON: {0}")]
    Decode(String),
    /// The plan targets another Collector.
    #[error("CollectorPlan target mismatch: expected {expected}, got {actual}")]
    WrongTarget {
        /// Collector identity supplied by the local runtime.
        expected: String,
        /// Collector identity encoded by the candidate plan.
        actual: String,
    },
    /// A required identity field is empty or duplicated.
    #[error("invalid plan identity: {0}")]
    Identity(String),
    /// Materialization content is invalid or unsupported.
    #[error("materialization {query_id}: {reason}")]
    Materialization {
        /// Query whose physical materialization is invalid.
        query_id: String,
        /// Concrete validation failure.
        reason: String,
    },
}

impl CollectorPlan {
    /// Decode and validate a complete JSON plan for `collector_id`.
    pub fn from_json(bytes: &[u8], collector_id: &str) -> Result<Self, CollectorPlanError> {
        let plan: Self = serde_json::from_slice(bytes)
            .map_err(|error| CollectorPlanError::Decode(error.to_string()))?;
        plan.validate(collector_id)?;
        Ok(plan)
    }

    /// Validate every materialization without applying partial state.
    pub fn validate(&self, collector_id: &str) -> Result<(), CollectorPlanError> {
        if self.collector_id != collector_id {
            return Err(CollectorPlanError::WrongTarget {
                expected: collector_id.into(),
                actual: self.collector_id.clone(),
            });
        }
        if self.envelope.plan_id == 0
            || self.envelope.planner_revision.trim().is_empty()
            || self.envelope.capability_snapshot_id.trim().is_empty()
        {
            return Err(CollectorPlanError::Identity(
                "plan_id, planner_revision, and capability_snapshot_id are required".into(),
            ));
        }
        let mut ids = HashSet::new();
        for materialization in &self.materializations {
            if materialization.query_id.trim().is_empty()
                || !ids.insert(materialization.query_id.as_str())
            {
                return Err(CollectorPlanError::Identity(
                    "query IDs must be non-empty and unique".into(),
                ));
            }
            materialization.to_config(self.envelope.plan_id)?;
        }
        Ok(())
    }

    /// Atomically project the validated plan into the host-neutral runtime's
    /// versioned configuration set.
    pub fn to_precompute_config_set(&self) -> Result<PrecomputeConfigSet, CollectorPlanError> {
        self.validate(&self.collector_id)?;
        let configs = self
            .materializations
            .iter()
            .map(|m| m.to_config(self.envelope.plan_id))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(PrecomputeConfigSet {
            version: self.envelope.plan_id,
            configs,
        })
    }
}

impl CollectorMaterialization {
    fn to_config(&self, plan_id: u64) -> Result<PrecomputeConfig, CollectorPlanError> {
        if self.metric.trim().is_empty() || self.window_secs == 0 {
            return Err(self.invalid("metric must be non-empty and window_secs must be positive"));
        }
        let (sketch_type, params, evidence_required) = decode_algorithm(self)?;
        if evidence_required && self.evidence_source.as_deref().is_none_or(str::is_empty) {
            return Err(self.invalid("heap-bearing TopK plan requires evidence_source"));
        }
        Ok(PrecomputeConfig {
            agg_id: materialization_id(plan_id, &self.query_id),
            sketch_type,
            mode: AggregationMode::Tumbling,
            window: WindowSpec {
                size: Duration::from_secs(self.window_secs),
                slide: Duration::from_secs(self.window_secs),
                allowed_lateness: Duration::ZERO,
            },
            aggregate_by: self.group_by.clone(),
            transmit_sketch: true,
            delta_transmission: false,
            encoding: Encoding::ProtoFull,
            sketch_params: params,
            metric_name: self.metric.clone(),
            ..PrecomputeConfig::default()
        })
    }

    fn invalid(&self, reason: impl Into<String>) -> CollectorPlanError {
        CollectorPlanError::Materialization {
            query_id: self.query_id.clone(),
            reason: reason.into(),
        }
    }
}

fn decode_algorithm(
    materialization: &CollectorMaterialization,
) -> Result<(SketchType, HashMap<String, f64>, bool), CollectorPlanError> {
    let mut params = HashMap::new();
    let mut number = |wire: &str, runtime: &str| -> Result<(), CollectorPlanError> {
        let value = materialization
            .parameters
            .get(wire)
            .and_then(Value::as_f64)
            .filter(|value| value.is_finite() && *value > 0.0)
            .ok_or_else(|| materialization.invalid(format!("missing/invalid parameter {wire}")))?;
        params.insert(runtime.into(), value);
        Ok(())
    };
    let (kind, topk) = match materialization.algorithm.as_str() {
        "ddsketch" => {
            number("alpha", "relative_accuracy")?;
            (SketchType::DDSketch, false)
        }
        "kll" => {
            number("k", "k")?;
            (SketchType::KLLSketch, false)
        }
        "hll" => {
            number("precision", "precision")?;
            (SketchType::HLLSketch, false)
        }
        "cms" | "cmswithheap" => {
            number("width", "columns")?;
            number("depth", "rows")?;
            if materialization.algorithm == "cmswithheap" {
                number("heap_size", "heap_size")?;
            }
            (
                SketchType::CountMinSketch,
                materialization.algorithm == "cmswithheap",
            )
        }
        "countsketch" | "countsketchwithheap" => {
            number("width", "width")?;
            number("depth", "depth")?;
            if materialization.algorithm == "countsketchwithheap" {
                number("heap_size", "heap_size")?;
            }
            (
                SketchType::CountSketch,
                materialization.algorithm == "countsketchwithheap",
            )
        }
        other => return Err(materialization.invalid(format!("unsupported algorithm {other}"))),
    };
    match kind {
        SketchType::DDSketch if params["relative_accuracy"] >= 1.0 => {
            return Err(materialization.invalid("alpha must be in (0, 1)"));
        }
        SketchType::KLLSketch if params["k"] < 8.0 => {
            return Err(materialization.invalid("KLL k must be at least 8"));
        }
        SketchType::HLLSketch if !(4.0..=18.0).contains(&params["precision"]) => {
            return Err(materialization.invalid("HLL precision must be in [4, 18]"));
        }
        _ => {}
    }
    Ok((kind, params, topk))
}

fn materialization_id(plan_id: u64, query_id: &str) -> u64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    let id = plan_id
        .to_be_bytes()
        .into_iter()
        .chain(std::iter::once(0))
        .chain(query_id.bytes())
        .fold(OFFSET, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(PRIME)
        });
    if id == 0 {
        1
    } else {
        id
    }
}
