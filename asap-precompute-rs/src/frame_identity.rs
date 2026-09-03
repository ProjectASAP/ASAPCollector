//! Plan-derived summary-frame identity and sequence generation.

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::collector_plan::{CollectorPlan, StateEncoding, TransmissionMode, TransmissionRule};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[allow(missing_docs)]
pub enum SummaryFrameKind {
    Full,
    Delta,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[allow(missing_docs)]
pub struct SummaryFrameIdentity {
    pub identity_version: u32,
    pub plan_id: u64,
    pub plan_version: u64,
    pub backend_compat: String,
    pub materialization: u64,
    pub series_identity: String,
    pub schema_id: String,
    pub producer_id: String,
    pub producer_epoch: String,
    pub window_start_unix_nano: u64,
    pub window_end_unix_nano: u64,
    pub sequence: u64,
    pub kind: SummaryFrameKind,
    pub encoding: StateEncoding,
    pub checkpoint_id: Option<String>,
    pub base_checkpoint_id: Option<String>,
}

impl SummaryFrameIdentity {
    /// Encode the reserved modified-OTLP attributes consumed by the
    /// ASAPQuery backend. Window bounds remain the data-point timestamps.
    pub fn otlp_attributes(&self) -> BTreeMap<String, String> {
        let mut attributes = BTreeMap::from([
            (
                "asap.frame.identity_version".into(),
                self.identity_version.to_string(),
            ),
            ("asap.frame.plan_id".into(), self.plan_id.to_string()),
            (
                "asap.frame.plan_version".into(),
                self.plan_version.to_string(),
            ),
            (
                "asap.frame.backend_compat".into(),
                self.backend_compat.clone(),
            ),
            (
                "asap.frame.materialization".into(),
                self.materialization.to_string(),
            ),
            (
                "asap.frame.series_identity".into(),
                self.series_identity.clone(),
            ),
            ("asap.frame.schema_id".into(), self.schema_id.clone()),
            ("asap.frame.producer_id".into(), self.producer_id.clone()),
            (
                "asap.frame.producer_epoch".into(),
                self.producer_epoch.clone(),
            ),
            ("asap.frame.sequence".into(), self.sequence.to_string()),
            (
                "asap.frame.kind".into(),
                match self.kind {
                    SummaryFrameKind::Full => "full",
                    SummaryFrameKind::Delta => "delta",
                }
                .into(),
            ),
            (
                "asap.frame.encoding".into(),
                match self.encoding {
                    StateEncoding::SketchlibProtobufV1 => "sketchlib_protobuf_v1",
                    StateEncoding::SketchCoreMsgpackV1 => "sketch_core_msgpack_v1",
                    StateEncoding::ExactAccumulatorV1 => "exact_accumulator_v1",
                }
                .into(),
            ),
        ]);
        if let Some(checkpoint_id) = &self.checkpoint_id {
            attributes.insert("asap.frame.checkpoint_id".into(), checkpoint_id.clone());
        }
        if let Some(base_checkpoint_id) = &self.base_checkpoint_id {
            attributes.insert(
                "asap.frame.base_checkpoint_id".into(),
                base_checkpoint_id.clone(),
            );
        }
        attributes
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct LineageKey {
    materialization: u64,
    series_identity: String,
    producer_id: String,
    producer_epoch: String,
    window_start_unix_nano: u64,
    window_end_unix_nano: u64,
}

#[derive(Debug, Clone)]
struct LineageState {
    sequence: u64,
    checkpoint_id: String,
    checkpoint_at_unix_ms: u64,
}

#[derive(Debug, Error, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum FrameIdentityError {
    #[error("transmission rule does not belong to this plan/collector")]
    RuleMismatch,
    #[error("frame window is invalid")]
    InvalidWindow,
}

/// One sequencer belongs to one running collector process. A new process uses
/// a new producer epoch, so backend lineage cannot confuse pre/post-restart
/// sequences.
#[derive(Debug, Default)]
pub struct FrameSequencer {
    lineages: HashMap<LineageKey, LineageState>,
}

impl FrameSequencer {
    /// Produce the next full/delta identity for one plan-bound lineage.
    pub fn next(
        &mut self,
        plan: &CollectorPlan,
        rule: &TransmissionRule,
        producer_epoch: &str,
        series_identity: &str,
        window_start_unix_nano: u64,
        window_end_unix_nano: u64,
        now_unix_ms: u64,
    ) -> Result<SummaryFrameIdentity, FrameIdentityError> {
        if rule.producer_id != plan.collector_id
            || !plan
                .transmission_rules
                .iter()
                .any(|candidate| candidate == rule)
            || producer_epoch.is_empty()
            || series_identity.is_empty()
        {
            return Err(FrameIdentityError::RuleMismatch);
        }
        if window_start_unix_nano >= window_end_unix_nano {
            return Err(FrameIdentityError::InvalidWindow);
        }
        let key = LineageKey {
            materialization: rule.materialization,
            series_identity: series_identity.into(),
            producer_id: rule.producer_id.clone(),
            producer_epoch: producer_epoch.into(),
            window_start_unix_nano,
            window_end_unix_nano,
        };
        let state = self.lineages.entry(key).or_insert_with(|| LineageState {
            sequence: 0,
            checkpoint_id: String::new(),
            checkpoint_at_unix_ms: 0,
        });
        state.sequence = state.sequence.saturating_add(1);
        let checkpoint_due = state.sequence == 1
            || rule.mode == TransmissionMode::Full
            || rule.full_checkpoint_every_ms.is_some_and(|cadence| {
                now_unix_ms.saturating_sub(state.checkpoint_at_unix_ms) >= cadence
            });
        let kind = if checkpoint_due {
            SummaryFrameKind::Full
        } else {
            SummaryFrameKind::Delta
        };
        if kind == SummaryFrameKind::Full {
            state.checkpoint_id = format!(
                "{}:{}:{}:{}",
                rule.producer_id, producer_epoch, window_start_unix_nano, state.sequence
            );
            state.checkpoint_at_unix_ms = now_unix_ms;
        }
        Ok(SummaryFrameIdentity {
            identity_version: 1,
            plan_id: plan.envelope.plan_id,
            plan_version: plan.envelope.plan_version,
            backend_compat: plan.envelope.backend_compat.clone(),
            materialization: rule.materialization,
            series_identity: series_identity.into(),
            schema_id: rule.schema_id.clone(),
            producer_id: rule.producer_id.clone(),
            producer_epoch: producer_epoch.into(),
            window_start_unix_nano,
            window_end_unix_nano,
            sequence: state.sequence,
            kind,
            encoding: rule.encoding,
            checkpoint_id: (kind == SummaryFrameKind::Full).then(|| state.checkpoint_id.clone()),
            base_checkpoint_id: (kind == SummaryFrameKind::Delta)
                .then(|| state.checkpoint_id.clone()),
        })
    }
}
