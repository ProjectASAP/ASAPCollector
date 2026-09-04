//! Validation and runtime projection for ASAPQuery's MVP `CollectorPlan`.
//!
//! This boundary consumes a physical decision; it never parses queries or
//! chooses a different sketch family. Invalid or unsupported plans are rejected
//! as a unit before a [`PrecomputeConfigSet`](crate::config::PrecomputeConfigSet)
//! is exposed to a runtime.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::config::{AggregationMode, PrecomputeConfig, PrecomputeConfigSet, WindowSpec};
use crate::envelope::{Encoding, SketchType};

/// Shared physical-plan identity emitted by ASAPQuery.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PlanEnvelope {
    /// Stable content-derived plan identifier.
    pub plan_id: u64,
    /// Monotonic generation within one stable plan identity.
    pub plan_version: u64,
    /// Compilation timestamp.
    pub generated_at_unix_ms: u64,
    /// Earliest wall-clock time at which this generation may activate.
    pub activation_unix_ms: u64,
    /// Optional end of the generation's validity interval.
    pub expiry_unix_ms: Option<u64>,
    /// Exact backend wire/ABI compatibility identifier.
    pub backend_compat: String,
    /// Exact ASAPPlanner revision used for candidate selection.
    pub planner_revision: String,
    /// Capability snapshot against which the plan was compiled.
    pub capability_snapshot_id: String,
}

/// One executable materialization assigned to this Collector.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CollectorMaterialization {
    /// Stable workload query identifier.
    pub query_id: String,
    /// Content identity shared with PrecomputePlan, BackendPlan and QueryPlan.
    pub materialization: u64,
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
    /// Planner-selected abstract summary-window primitive.
    pub abstract_window_framework: SummaryWindowFramework,
    /// Backend-owned concrete runtime realization selected for this framework.
    pub window_implementation_id: String,
    /// Concrete pane width used by the Collector runtime.
    pub pane_secs: u64,
    /// Concrete state-layout compatibility identity.
    pub state_layout: String,
    /// Evidence source for evidence-gated selections such as TopK.
    pub evidence_source: Option<String>,
    /// ASAPPlanner-selected summary-maintenance commitment.
    pub lifecycle: CollectorLifecycle,
}

/// Planner-owned abstract summary-window vocabulary. This mirrors the wire
/// representation but does not let Collector select a different framework.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SummaryWindowFramework {
    /// Disjoint fixed-width logical windows.
    Tumbling,
    /// Overlapping logical windows.
    Sliding,
    /// Hierarchical buckets with exponentially increasing coverage.
    ExponentialHistogram,
    /// Provider-registered Planner primitive.
    Extension(String),
}

/// Lifecycle shape currently executable by the Collector window runtime.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CollectorLifecycle {
    /// State retention/update policy.
    pub kind: String,
    /// State construction policy.
    pub maintenance_mode: String,
    /// When the runtime evaluates this summary.
    pub evaluation_schedule: String,
    /// Representation delivered to the backend consumer.
    pub output_representation: String,
}

/// Complete per-target physical plan emitted by ASAPQuery.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CollectorPlan {
    /// Collector instance this plan may be applied to.
    pub collector_id: String,
    /// Shared bundle identity.
    pub envelope: PlanEnvelope,
    /// Complete materialization set for this target.
    pub materializations: Vec<CollectorMaterialization>,
    /// Exact producer/frame rules for this target.
    pub transmission_rules: Vec<TransmissionRule>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[allow(missing_docs)]
pub enum TransmissionMode {
    Full,
    Delta,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[allow(missing_docs)]
pub enum StateEncoding {
    SketchlibProtobufV1,
    SketchCoreMsgpackV1,
    ExactAccumulatorV1,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[allow(missing_docs)]
pub struct TransmissionRule {
    pub materialization: u64,
    pub producer_id: String,
    pub schema_id: String,
    pub mode: TransmissionMode,
    pub encoding: StateEncoding,
    pub emit_every_ms: u64,
    pub full_checkpoint_every_ms: Option<u64>,
    pub destination_ref: String,
    #[serde(default)]
    pub runtime_policy: RuntimeRulePolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
#[allow(missing_docs)]
pub struct RuntimeRulePolicy {
    #[serde(default)]
    pub sampling: SamplingPolicy,
    pub delta: Option<DeltaPolicy>,
    #[serde(default)]
    pub adaptation: RuntimeAdaptationPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
#[allow(missing_docs)]
pub enum SamplingPolicy {
    #[default]
    Disabled,
    Fixed {
        probability: f64,
        estimator: SamplingEstimator,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[allow(missing_docs)]
pub enum SamplingEstimator {
    HashThreshold,
    GeometricAdmission,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[allow(missing_docs)]
pub struct DeltaPolicy {
    pub absolute_threshold: f64,
    pub gos: Option<GosPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[allow(missing_docs)]
pub struct GosPolicy {
    pub epsilon_staleness: f64,
    pub sites: u32,
    pub threshold_mode: GosThresholdMode,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[allow(missing_docs)]
pub enum GosThresholdMode {
    Isotropic,
    Anisotropic,
}

/// Collector consumes guardrails but never changes an active generation in
/// place; a controller adaptation arrives as another versioned plan.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
#[allow(missing_docs)]
pub struct RuntimeAdaptationPolicy {
    pub enabled: bool,
    pub not_before_unix_ms: u64,
    pub max_evidence_age_ms: u64,
    pub min_evidence_samples: u64,
    pub sample_probability: Option<AdaptiveF64Bounds>,
    pub emit_every_ms: Option<AdaptiveU64Bounds>,
    pub delta_threshold: Option<AdaptiveF64Bounds>,
    pub gos_epsilon_staleness: Option<AdaptiveF64Bounds>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[allow(missing_docs)]
pub struct AdaptiveF64Bounds {
    pub min: f64,
    pub max: f64,
    pub max_step: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[allow(missing_docs)]
pub struct AdaptiveU64Bounds {
    pub min: u64,
    pub max: u64,
    pub max_step: u64,
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[allow(missing_docs)]
pub enum PlanPhase {
    Staged,
    Active,
    Draining,
    Retired,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[allow(missing_docs)]
pub struct PlanStatus {
    pub plan_id: u64,
    pub plan_version: u64,
    pub phase: PlanPhase,
}

/// Atomic Collector-side staged activation state. `stage` validates the whole
/// plan without changing the active config; `activate` performs the cutover.
#[derive(Debug, Default)]
pub struct CollectorPlanLifecycle {
    active: Option<CollectorPlan>,
    staged: BTreeMap<(u64, u64), CollectorPlan>,
    statuses: BTreeMap<(u64, u64), PlanStatus>,
}

impl CollectorPlanLifecycle {
    /// Validate and stage one complete generation without changing active state.
    pub fn stage(
        &mut self,
        plan: CollectorPlan,
        collector_id: &str,
        now_unix_ms: u64,
    ) -> Result<(), CollectorPlanError> {
        plan.validate(collector_id)?;
        if plan
            .envelope
            .expiry_unix_ms
            .is_some_and(|expiry| expiry <= now_unix_ms)
        {
            return Err(CollectorPlanError::Identity(
                "cannot stage an expired plan".into(),
            ));
        }
        if self
            .statuses
            .contains_key(&(plan.envelope.plan_id, plan.envelope.plan_version))
        {
            return Err(CollectorPlanError::Identity(
                "plan generation is already staged".into(),
            ));
        }
        let newest_known_version = self
            .statuses
            .keys()
            .map(|(_, version)| *version)
            .chain(
                self.active
                    .iter()
                    .map(|active| active.envelope.plan_version),
            )
            .max()
            .unwrap_or(0);
        if plan.envelope.plan_version <= newest_known_version {
            return Err(CollectorPlanError::Identity(
                "plan version is not newer than the latest known generation".into(),
            ));
        }
        let key = (plan.envelope.plan_id, plan.envelope.plan_version);
        self.statuses.insert(
            key,
            PlanStatus {
                plan_id: key.0,
                plan_version: key.1,
                phase: PlanPhase::Staged,
            },
        );
        self.staged.insert(key, plan);
        Ok(())
    }

    /// Atomically promote a staged generation once its activation time arrives.
    pub fn activate(
        &mut self,
        plan_id: u64,
        plan_version: u64,
        now_unix_ms: u64,
    ) -> Result<&CollectorPlan, CollectorPlanError> {
        let key = (plan_id, plan_version);
        let plan = self
            .staged
            .remove(&key)
            .ok_or_else(|| CollectorPlanError::Identity("plan generation is not staged".into()))?;
        if now_unix_ms < plan.envelope.activation_unix_ms {
            self.staged.insert(key, plan);
            return Err(CollectorPlanError::Identity(
                "activation time has not been reached".into(),
            ));
        }
        if plan
            .envelope
            .expiry_unix_ms
            .is_some_and(|expiry| expiry <= now_unix_ms)
        {
            self.statuses.get_mut(&key).expect("staged status").phase = PlanPhase::Retired;
            return Err(CollectorPlanError::Identity(
                "cannot activate an expired plan".into(),
            ));
        }
        if self
            .active
            .as_ref()
            .is_some_and(|active| active.envelope.plan_version >= plan.envelope.plan_version)
        {
            self.staged.insert(key, plan);
            return Err(CollectorPlanError::Identity(
                "activation would downgrade the active generation".into(),
            ));
        }
        if let Some(previous) = self.active.replace(plan) {
            let previous_key = (previous.envelope.plan_id, previous.envelope.plan_version);
            if let Some(status) = self.statuses.get_mut(&previous_key) {
                status.phase = PlanPhase::Draining;
            }
        }
        self.statuses.get_mut(&key).expect("staged status").phase = PlanPhase::Active;
        Ok(self.active.as_ref().expect("active plan installed"))
    }

    /// Return the currently active immutable plan generation.
    pub fn active(&self) -> Option<&CollectorPlan> {
        self.active.as_ref()
    }

    /// Snapshot all known generation phases.
    pub fn statuses(&self) -> Vec<PlanStatus> {
        self.statuses.values().cloned().collect()
    }

    /// Mark generations whose old windows have drained as retired.
    pub fn retire_drained(&mut self) {
        for status in self.statuses.values_mut() {
            if status.phase == PlanPhase::Draining {
                status.phase = PlanPhase::Retired;
            }
        }
    }
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
            || self.envelope.plan_version == 0
            || self.envelope.generated_at_unix_ms == 0
            || self.envelope.activation_unix_ms == 0
            || self.envelope.generated_at_unix_ms > self.envelope.activation_unix_ms
            || self.envelope.backend_compat.trim().is_empty()
            || self.envelope.planner_revision.trim().is_empty()
            || self.envelope.capability_snapshot_id.trim().is_empty()
            || self
                .envelope
                .expiry_unix_ms
                .is_some_and(|expiry| expiry <= self.envelope.activation_unix_ms)
        {
            return Err(CollectorPlanError::Identity(
                "plan/version/activation/backend compatibility and planner/capability identity are required"
                    .into(),
            ));
        }
        let mut materializations = HashSet::new();
        for materialization in &self.materializations {
            if materialization.query_id.trim().is_empty()
                || materialization.materialization == 0
                || !materializations.insert(materialization.materialization)
            {
                return Err(CollectorPlanError::Identity(
                    "query IDs must be non-empty and materialization fingerprints must be unique"
                        .into(),
                ));
            }
        }
        let mut ruled = HashSet::new();
        for rule in &self.transmission_rules {
            if rule.producer_id != self.collector_id
                || !materializations.contains(&rule.materialization)
                || !ruled.insert(rule.materialization)
            {
                return Err(CollectorPlanError::Identity(
                    "transmission rules must exactly bind this collector and materialization set"
                        .into(),
                ));
            }
            validate_rule(rule, self.materialization(rule.materialization)?)?;
        }
        if ruled != materializations {
            return Err(CollectorPlanError::Identity(
                "every materialization requires exactly one transmission rule".into(),
            ));
        }
        for materialization in &self.materializations {
            materialization.to_config(self.rule(materialization.materialization)?)?;
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
            .map(|m| {
                self.rule(m.materialization)
                    .and_then(|rule| m.to_config(rule))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(PrecomputeConfigSet {
            version: self.envelope.plan_version,
            configs,
        })
    }

    fn materialization(
        &self,
        fingerprint: u64,
    ) -> Result<&CollectorMaterialization, CollectorPlanError> {
        self.materializations
            .iter()
            .find(|materialization| materialization.materialization == fingerprint)
            .ok_or_else(|| CollectorPlanError::Identity("unknown materialization reference".into()))
    }

    fn rule(&self, fingerprint: u64) -> Result<&TransmissionRule, CollectorPlanError> {
        self.transmission_rules
            .iter()
            .find(|rule| rule.materialization == fingerprint)
            .ok_or_else(|| CollectorPlanError::Identity("missing transmission rule".into()))
    }
}

impl CollectorMaterialization {
    fn to_config(&self, rule: &TransmissionRule) -> Result<PrecomputeConfig, CollectorPlanError> {
        if self.metric.trim().is_empty()
            || self.window_secs == 0
            || self.window_implementation_id.trim().is_empty()
            || self.pane_secs == 0
            || self.state_layout.trim().is_empty()
        {
            return Err(self.invalid(
                "metric, window implementation, pane, state layout, and window_secs are required",
            ));
        }
        if self.abstract_window_framework != SummaryWindowFramework::Tumbling
            || self.window_implementation_id != "collector-tumbling-v1"
            || self.pane_secs != self.window_secs
            || self.state_layout != "anchored-pane-v1"
        {
            return Err(self.invalid(
                "unsupported window realization; this runtime requires Planner tumbling + equal anchored panes + anchored-pane-v1",
            ));
        }
        if self.lifecycle.kind != "continuously_maintained"
            || self.lifecycle.maintenance_mode != "incremental"
            || self.lifecycle.evaluation_schedule != "per_update"
            || self.lifecycle.output_representation != "summary_state"
        {
            return Err(self.invalid(
                "unsupported lifecycle; Collector requires continuously_maintained/incremental/per_update/summary_state",
            ));
        }
        let (sketch_type, mut params, evidence_required) = decode_algorithm(self)?;
        if evidence_required && self.evidence_source.as_deref().is_none_or(str::is_empty) {
            return Err(self.invalid("heap-bearing TopK plan requires evidence_source"));
        }
        if let SamplingPolicy::Fixed { probability, .. } = &rule.runtime_policy.sampling {
            params.insert("sample_p".into(), *probability);
        }
        if let Some(gos) = rule
            .runtime_policy
            .delta
            .as_ref()
            .and_then(|delta| delta.gos.as_ref())
        {
            params.insert("gos_delta_epsilon".into(), gos.epsilon_staleness);
            params.insert("gos_sites".into(), f64::from(gos.sites));
        }
        Ok(PrecomputeConfig {
            agg_id: self.materialization,
            sketch_type,
            mode: AggregationMode::Tumbling,
            window: WindowSpec {
                size: Duration::from_secs(self.window_secs),
                slide: Duration::from_secs(self.window_secs),
                allowed_lateness: Duration::ZERO,
            },
            aggregate_by: self.group_by.clone(),
            transmit_sketch: true,
            delta_transmission: rule.mode == TransmissionMode::Delta,
            delta_threshold: rule
                .runtime_policy
                .delta
                .as_ref()
                .map(|policy| policy.absolute_threshold as u64)
                .unwrap_or(0),
            encoding: match rule.encoding {
                StateEncoding::SketchlibProtobufV1 => Encoding::ProtoFull,
                StateEncoding::SketchCoreMsgpackV1 => Encoding::Msgpack,
                StateEncoding::ExactAccumulatorV1 => {
                    return Err(self.invalid("exact accumulator encoding is not a sketch runtime"));
                }
            },
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

fn validate_rule(
    rule: &TransmissionRule,
    materialization: &CollectorMaterialization,
) -> Result<(), CollectorPlanError> {
    let invalid = |reason: &str| materialization.invalid(reason);
    if rule.schema_id.trim().is_empty()
        || rule.destination_ref.trim().is_empty()
        || rule.emit_every_ms == 0
        || rule.emit_every_ms != materialization.window_secs.saturating_mul(1_000)
        || (rule.mode == TransmissionMode::Delta
            && rule.full_checkpoint_every_ms.is_none_or(|value| value == 0))
        || (rule.mode == TransmissionMode::Full && rule.full_checkpoint_every_ms.is_some())
    {
        return Err(invalid(
            "invalid transmission identity/cadence/checkpoint contract",
        ));
    }
    if (rule.mode == TransmissionMode::Delta) != rule.runtime_policy.delta.is_some() {
        return Err(invalid(
            "delta policy must be present exactly for delta mode",
        ));
    }
    if let Some(delta) = &rule.runtime_policy.delta {
        if !delta.absolute_threshold.is_finite()
            || delta.absolute_threshold < 0.0
            || delta.absolute_threshold.fract() != 0.0
        {
            return Err(invalid(
                "delta threshold must be a finite non-negative integer for this runtime",
            ));
        }
        if !matches!(
            materialization.algorithm.as_str(),
            "ddsketch" | "hll" | "cms" | "cmswithheap" | "countsketch" | "countsketchwithheap"
        ) {
            return Err(invalid("delta transmission is unsupported for this family"));
        }
        if let Some(gos) = &delta.gos {
            if !materialization.algorithm.starts_with("countsketch")
                || !gos.epsilon_staleness.is_finite()
                || !(0.0..=1.0).contains(&gos.epsilon_staleness)
                || gos.epsilon_staleness == 0.0
                || gos.sites == 0
                || gos.threshold_mode != GosThresholdMode::Isotropic
            {
                return Err(invalid(
                    "GOS requires CountSketch, epsilon in (0,1], and sites > 0",
                ));
            }
        }
    }
    if let SamplingPolicy::Fixed {
        probability,
        estimator,
    } = &rule.runtime_policy.sampling
    {
        if !probability.is_finite() || !(0.0..=1.0).contains(probability) || *probability == 0.0 {
            return Err(invalid("sample probability must be in (0,1]"));
        }
        let supported = matches!(
            (materialization.algorithm.as_str(), estimator),
            ("hll", SamplingEstimator::HashThreshold)
                | ("cms", SamplingEstimator::GeometricAdmission)
        );
        if !supported {
            return Err(invalid("sampling estimator is unsupported for this family"));
        }
    }
    validate_adaptation(rule, materialization)?;
    Ok(())
}

fn validate_adaptation(
    rule: &TransmissionRule,
    materialization: &CollectorMaterialization,
) -> Result<(), CollectorPlanError> {
    let invalid = |reason: &str| materialization.invalid(reason);
    let policy = &rule.runtime_policy.adaptation;
    if policy.enabled && (policy.max_evidence_age_ms == 0 || policy.min_evidence_samples == 0) {
        return Err(invalid(
            "enabled adaptation requires evidence age and sample-count requirements",
        ));
    }
    validate_f64_bounds(policy.sample_probability.as_ref(), 0.0, 1.0).map_err(invalid)?;
    validate_f64_bounds(policy.delta_threshold.as_ref(), 0.0, f64::MAX).map_err(invalid)?;
    validate_f64_bounds(policy.gos_epsilon_staleness.as_ref(), 0.0, 1.0).map_err(invalid)?;
    if let Some(bounds) = &policy.emit_every_ms {
        if bounds.min == 0
            || bounds.min > bounds.max
            || bounds.max_step == 0
            || !(bounds.min..=bounds.max).contains(&rule.emit_every_ms)
        {
            return Err(invalid("emit interval guardrails are invalid"));
        }
    }
    if let Some(bounds) = &policy.sample_probability {
        let current = match rule.runtime_policy.sampling {
            SamplingPolicy::Disabled => 1.0,
            SamplingPolicy::Fixed { probability, .. } => probability,
        };
        if current < bounds.min || current > bounds.max {
            return Err(invalid("current sample probability is outside guardrails"));
        }
    }
    if let Some(bounds) = &policy.delta_threshold {
        let Some(current) = rule
            .runtime_policy
            .delta
            .as_ref()
            .map(|delta| delta.absolute_threshold)
        else {
            return Err(invalid("delta guardrails require a delta policy"));
        };
        if current < bounds.min || current > bounds.max {
            return Err(invalid("current delta threshold is outside guardrails"));
        }
    }
    if let Some(bounds) = &policy.gos_epsilon_staleness {
        let Some(current) = rule
            .runtime_policy
            .delta
            .as_ref()
            .and_then(|delta| delta.gos.as_ref())
            .map(|gos| gos.epsilon_staleness)
        else {
            return Err(invalid("GOS guardrails require a GOS policy"));
        };
        if current < bounds.min || current > bounds.max {
            return Err(invalid("current GOS epsilon is outside guardrails"));
        }
    }
    Ok(())
}

fn validate_f64_bounds(
    bounds: Option<&AdaptiveF64Bounds>,
    domain_min: f64,
    domain_max: f64,
) -> Result<(), &'static str> {
    let Some(bounds) = bounds else {
        return Ok(());
    };
    if !bounds.min.is_finite()
        || !bounds.max.is_finite()
        || !bounds.max_step.is_finite()
        || bounds.min < domain_min
        || bounds.max > domain_max
        || bounds.min > bounds.max
        || bounds.max_step <= 0.0
    {
        Err("floating-point adaptation guardrails are invalid")
    } else {
        Ok(())
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
        "cms" => {
            // The Rust OTAP runtime consumes the canonical matrix names
            // `width`/`depth` (unlike the Go runtime's `columns`/`rows`). Keep
            // the plan projection aligned with the runtime so a committed
            // non-default CMS shape cannot silently fall back to defaults.
            number("width", "width")?;
            number("depth", "depth")?;
            (SketchType::CountMinSketch, false)
        }
        "cmswithheap" => {
            return Err(materialization.invalid("keyed CMS heap serving path is not implemented"));
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
