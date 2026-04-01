use chrono::Utc;
use std::time::Duration;

use crate::types::*;

pub const DEFAULT_VALID_FOR: Duration = Duration::from_secs(10 * 60);

pub struct RulesPlanner {
    pub valid_for: Duration,
}

impl RulesPlanner {
    pub fn new() -> Self {
        Self {
            valid_for: DEFAULT_VALID_FOR,
        }
    }

    pub fn plan(&self, w: &QueryWorkload) -> CollectionPlan {
        // When exact computation is required (RSI, MACD, stateful indicators),
        // skip sketch selection and return a raw-passthrough plan.
        if w.exact_required {
            return self.raw_passthrough_plan(w);
        }

        let sketch_type = select_sketch_type(&w.aggregations);
        let sketch_params = default_sketch_params_with_quantiles(&sketch_type, w.accuracy_sla, &w.quantiles);
        let (mode, window_duration) = select_window_strategy(w);

        let mut aggregate_by = w.group_by_labels.clone();
        aggregate_by.sort();

        let mut label_matchers: Vec<String> = w
            .label_filters
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        label_matchers.sort();

        let backend_sketch = sketch_type.clone();
        let group_by = aggregate_by.clone();

        let valid_until = Utc::now() + chrono::Duration::seconds(self.valid_for.as_secs() as i64);

        CollectionPlan {
            agent_config: AgentCollectorConfig {
                output_mode: OutputMode::Sketch,
                sketch_type,
                sketch_params,
                aggregate_by,
                label_matchers,
                window_duration,
                mode,
                enable_self_monitoring: true,
                transmit_sketch: false,
                drop_original: true,
                // Delta fields are left as disabled defaults here; the
                // CostModelPlanner overwrites them via decide_delta().
                delta_transmission: false,
                delta_threshold: 0.0,
            },
            gateway_config: GatewayCollectorConfig { passthrough: true },
            backend_config: BackendCollectorConfig {
                merge_sketch_type: backend_sketch,
                group_by,
            },
            precompute: vec![],
            valid_until,
            delta_decision: DeltaDecision::default(),
            transmission_cost_summary: TransmissionCostSummary::default(),
            staged_plan: None,
        }
    }

    /// Returns a raw-passthrough plan for queries that require exact per-sample
    /// computation (RSI, MACD, stochastic oscillator, etc.).
    fn raw_passthrough_plan(&self, w: &QueryWorkload) -> CollectionPlan {
        let valid_until = Utc::now()
            + chrono::Duration::seconds(self.valid_for.as_secs() as i64);

        let mut label_matchers: Vec<String> = w
            .label_filters
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        label_matchers.sort();

        CollectionPlan {
            agent_config: AgentCollectorConfig {
                output_mode:          OutputMode::Raw,
                sketch_type:          SketchType::DDSketch, // unused for raw mode
                sketch_params:        SketchParams::default(),
                aggregate_by:         vec![],
                label_matchers,
                window_duration:      None,
                mode:                 ProcessorMode::Batch,
                enable_self_monitoring: true,
                transmit_sketch:      false,
                drop_original:        false,
                delta_transmission:   false,
                delta_threshold:      0.0,
            },
            gateway_config: GatewayCollectorConfig { passthrough: true },
            backend_config: BackendCollectorConfig {
                merge_sketch_type: SketchType::DDSketch,
                group_by:          vec![],
            },
            precompute:               vec![],
            valid_until,
            delta_decision:           DeltaDecision::default(),
            transmission_cost_summary: TransmissionCostSummary::default(),
            staged_plan: None,
        }
    }
}

// ── Sketch selection ──────────────────────────────────────────────────────────

/// Picks the primary sketch type from the aggregation list.
/// Priority order: Quantile → Cardinality → Frequency.
fn select_sketch_type(aggs: &[AggType]) -> SketchType {
    for agg in aggs {
        match agg {
            AggType::Quantile => return SketchType::DDSketch,
            AggType::Cardinality => return SketchType::HLL,
            AggType::Frequency => return SketchType::CountSketch,
        }
    }
    SketchType::DDSketch
}

/// Returns type-appropriate default parameters for the given accuracy SLA.
pub fn default_sketch_params(st: &SketchType, accuracy_sla: f64) -> SketchParams {
    default_sketch_params_with_quantiles(st, accuracy_sla, &[])
}

/// Like [`default_sketch_params`] but seeds the quantiles list from the
/// query-parsed φ values when non-empty; falls back to an extended DEBS-style grid.
pub fn default_sketch_params_with_quantiles(
    st: &SketchType,
    accuracy_sla: f64,
    query_quantiles: &[f64],
) -> SketchParams {
    let acc = if accuracy_sla <= 0.0 { 0.01 } else { accuracy_sla };
    let quantiles: Vec<f64> = if !query_quantiles.is_empty() {
        query_quantiles.to_vec()
    } else {
        DEFAULT_QUANTILE_GRID.to_vec()
    };
    match st {
        SketchType::DDSketch => SketchParams::DDSketch {
            relative_accuracy: acc,
            quantiles,
        },
        SketchType::KLL => {
            let k = ((1.0 / acc) as u32).max(32);
            SketchParams::KLL { k, quantiles }
        }
        SketchType::HLL => {
            let precision = if acc > 0.02 { 10u32 } else { 14u32 };
            SketchParams::HLL { precision }
        }
        SketchType::CountSketch => SketchParams::CountSketch {
            // epsilon ≈ 1/sqrt(cols), delta ≈ e^(-rows) for the equivalent sketch size.
            epsilon: DEFAULT_CS_EPSILON,
            delta: DEFAULT_CS_DELTA,
        },
        SketchType::CountMinSketch => SketchParams::CountMinSketch {
            rows: 5,
            cols: 2048,
            metric_name: "countsketch_partition".to_string(),
        },
    }
}

// ── Window strategy ───────────────────────────────────────────────────────────

/// Decides processor mode.
///
/// Rule: if `latency_sla >= time_window` (or unset) → window mode.
///       otherwise → batch mode (gateway/backend merges on query).
pub fn select_window_strategy(w: &QueryWorkload) -> (ProcessorMode, Option<Duration>) {
    match w.latency_sla {
        None => (ProcessorMode::Window, Some(w.time_window)),
        Some(ls) if ls >= w.time_window => (ProcessorMode::Window, Some(w.time_window)),
        _ => (ProcessorMode::Batch, None),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn workload(aggs: Vec<AggType>) -> QueryWorkload {
        QueryWorkload {
            metric_name: "test".into(),
            label_filters: HashMap::new(),
            group_by_labels: vec![],
            aggregations: aggs,
            time_window: Duration::from_secs(300),
            repeat_every: None,
            accuracy_sla: 0.01,
            latency_sla: None,
            sketch_type_override: None,
            exact_required: false,
            quantiles: vec![],
        }
    }

    #[test]
    fn quantile_selects_ddsketch() {
        let plan = RulesPlanner::new().plan(&workload(vec![AggType::Quantile]));
        assert_eq!(plan.agent_config.sketch_type, SketchType::DDSketch);
    }

    #[test]
    fn cardinality_selects_hll() {
        let plan = RulesPlanner::new().plan(&workload(vec![AggType::Cardinality]));
        assert_eq!(plan.agent_config.sketch_type, SketchType::HLL);
    }

    #[test]
    fn frequency_selects_countsketch() {
        let plan = RulesPlanner::new().plan(&workload(vec![AggType::Frequency]));
        assert_eq!(plan.agent_config.sketch_type, SketchType::CountSketch);
    }

    #[test]
    fn quantile_priority_wins() {
        let plan =
            RulesPlanner::new().plan(&workload(vec![AggType::Quantile, AggType::Cardinality]));
        assert_eq!(
            plan.agent_config.sketch_type,
            SketchType::DDSketch,
            "quantile should take priority over cardinality"
        );
    }

    #[test]
    fn window_mode_when_latency_geq_time_window() {
        let mut w = workload(vec![AggType::Quantile]);
        w.latency_sla = Some(Duration::from_secs(600)); // 10m >= 5m
        let plan = RulesPlanner::new().plan(&w);
        assert_eq!(plan.agent_config.mode, ProcessorMode::Window);
        assert_eq!(
            plan.agent_config.window_duration,
            Some(Duration::from_secs(300))
        );
    }

    #[test]
    fn batch_mode_when_latency_lt_time_window() {
        let mut w = workload(vec![AggType::Quantile]);
        w.latency_sla = Some(Duration::from_secs(60)); // 1m < 5m
        let plan = RulesPlanner::new().plan(&w);
        assert_eq!(plan.agent_config.mode, ProcessorMode::Batch);
        assert_eq!(plan.agent_config.window_duration, None);
    }

    #[test]
    fn no_latency_sla_defaults_to_window() {
        let mut w = workload(vec![AggType::Quantile]);
        w.latency_sla = None;
        let plan = RulesPlanner::new().plan(&w);
        assert_eq!(plan.agent_config.mode, ProcessorMode::Window);
    }

    #[test]
    fn aggregate_by_sorted() {
        let mut w = workload(vec![AggType::Quantile]);
        w.group_by_labels = vec!["zone".into(), "host.name".into(), "service".into()];
        let plan = RulesPlanner::new().plan(&w);
        assert_eq!(
            plan.agent_config.aggregate_by,
            vec!["host.name", "service", "zone"]
        );
    }

    #[test]
    fn label_matchers_from_filters() {
        let mut w = workload(vec![AggType::Quantile]);
        w.label_filters = [
            ("env".into(), "prod".into()),
            ("service".into(), "web".into()),
        ]
        .into();
        let plan = RulesPlanner::new().plan(&w);
        assert_eq!(plan.agent_config.label_matchers.len(), 2);
    }

    #[test]
    fn ddsketch_accuracy_params() {
        let mut w = workload(vec![AggType::Quantile]);
        w.accuracy_sla = 0.005;
        let plan = RulesPlanner::new().plan(&w);
        match &plan.agent_config.sketch_params {
            SketchParams::DDSketch { relative_accuracy, .. } => assert_eq!(*relative_accuracy, 0.005),
            other => panic!("expected DDSketch, got {:?}", other),
        }
    }

    #[test]
    fn hll_precision_coarse_sla() {
        let mut w = workload(vec![AggType::Cardinality]);
        w.accuracy_sla = 0.03;
        let plan = RulesPlanner::new().plan(&w);
        match &plan.agent_config.sketch_params {
            SketchParams::HLL { precision } => assert_eq!(*precision, 10, "coarse SLA should use lower precision"),
            other => panic!("expected HLL, got {:?}", other),
        }
    }

    #[test]
    fn valid_until_in_future() {
        let plan = RulesPlanner::new().plan(&workload(vec![AggType::Quantile]));
        assert!(
            plan.valid_until > Utc::now(),
            "valid_until should be in the future"
        );
    }

    #[test]
    fn backend_config_matches_sketch_type() {
        let plan = RulesPlanner::new().plan(&workload(vec![AggType::Quantile]));
        assert_eq!(
            plan.backend_config.merge_sketch_type,
            plan.agent_config.sketch_type
        );
    }

    #[test]
    fn gateway_passthrough() {
        let plan = RulesPlanner::new().plan(&workload(vec![AggType::Quantile]));
        assert!(plan.gateway_config.passthrough);
    }
}
