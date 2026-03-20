use std::time::Duration;
use chrono::Utc;

use crate::types::*;

pub const DEFAULT_VALID_FOR: Duration = Duration::from_secs(10 * 60);

pub struct RulesPlanner {
    pub valid_for: Duration,
}

impl RulesPlanner {
    pub fn new() -> Self {
        Self { valid_for: DEFAULT_VALID_FOR }
    }

    pub fn plan(&self, w: &QueryWorkload) -> CollectionPlan {
        let sketch_type = select_sketch_type(&w.aggregations);
        let sketch_params = default_sketch_params(&sketch_type, w.accuracy_sla);
        let (mode, window_duration) = select_window_strategy(w);

        let mut aggregate_by = w.group_by_labels.clone();
        aggregate_by.sort();

        let mut label_matchers: Vec<String> = w.label_filters.iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        label_matchers.sort();

        let backend_sketch = sketch_type.clone();
        let group_by = aggregate_by.clone();

        let valid_until = Utc::now()
            + chrono::Duration::seconds(self.valid_for.as_secs() as i64);

        CollectionPlan {
            agent_config: AgentCollectorConfig {
                output_mode:     OutputMode::Sketch,
                sketch_type,
                sketch_params,
                aggregate_by,
                label_matchers,
                window_duration,
                mode,
                transmit_sketch: true,
                drop_original:   true,
            },
            gateway_config: GatewayCollectorConfig { passthrough: true },
            backend_config: BackendCollectorConfig {
                merge_sketch_type: backend_sketch,
                group_by,
            },
            precompute:  vec![],
            valid_until,
        }
    }
}

// ── Sketch selection ──────────────────────────────────────────────────────────

/// Picks the primary sketch type from the aggregation list.
/// Priority order: Quantile → Cardinality → Frequency.
fn select_sketch_type(aggs: &[AggType]) -> SketchType {
    for agg in aggs {
        match agg {
            AggType::Quantile    => return SketchType::DDSketch,
            AggType::Cardinality => return SketchType::HLL,
            AggType::Frequency   => return SketchType::CountSketch,
        }
    }
    SketchType::DDSketch
}

/// Returns type-appropriate default parameters for the given accuracy SLA.
pub fn default_sketch_params(st: &SketchType, accuracy_sla: f64) -> SketchParams {
    let acc = if accuracy_sla <= 0.0 { 0.01 } else { accuracy_sla };
    match st {
        SketchType::DDSketch => SketchParams {
            relative_accuracy: acc,
            quantiles: vec![0.5, 0.9, 0.99],
            ..Default::default()
        },
        SketchType::KLL => {
            let k = ((1.0 / acc) as u32).max(32);
            SketchParams { k, quantiles: vec![0.5, 0.9, 0.99], ..Default::default() }
        }
        SketchType::HLL => {
            // precision = log2(registers); higher → lower error.
            let precision = if acc > 0.02 { 10u32 } else { 14u32 };
            SketchParams { precision, ..Default::default() }
        }
        SketchType::CountSketch | SketchType::CountMinSketch => {
            SketchParams { rows: 5, cols: 2048, ..Default::default() }
        }
    }
}

// ── Window strategy ───────────────────────────────────────────────────────────

/// Decides processor mode.
///
/// Rule: if `latency_sla >= time_window` (or unset) → window mode.
///       otherwise → batch mode (gateway/backend merges on query).
pub fn select_window_strategy(w: &QueryWorkload) -> (ProcessorMode, Option<Duration>) {
    match w.latency_sla {
        None                                  => (ProcessorMode::Window, Some(w.time_window)),
        Some(ls) if ls >= w.time_window       => (ProcessorMode::Window, Some(w.time_window)),
        _                                     => (ProcessorMode::Batch,  None),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn workload(aggs: Vec<AggType>) -> QueryWorkload {
        QueryWorkload {
            metric_name:    "test".into(),
            label_filters:  HashMap::new(),
            group_by_labels: vec![],
            aggregations:   aggs,
            time_window:    Duration::from_secs(300),
            repeat_every:   None,
            accuracy_sla:         0.01,
            latency_sla:          None,
            sketch_type_override: None,
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
        let plan = RulesPlanner::new().plan(&workload(vec![AggType::Quantile, AggType::Cardinality]));
        assert_eq!(plan.agent_config.sketch_type, SketchType::DDSketch,
            "quantile should take priority over cardinality");
    }

    #[test]
    fn window_mode_when_latency_geq_time_window() {
        let mut w = workload(vec![AggType::Quantile]);
        w.latency_sla = Some(Duration::from_secs(600)); // 10m >= 5m
        let plan = RulesPlanner::new().plan(&w);
        assert_eq!(plan.agent_config.mode, ProcessorMode::Window);
        assert_eq!(plan.agent_config.window_duration, Some(Duration::from_secs(300)));
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
        assert_eq!(plan.agent_config.aggregate_by,
            vec!["host.name", "service", "zone"]);
    }

    #[test]
    fn label_matchers_from_filters() {
        let mut w = workload(vec![AggType::Quantile]);
        w.label_filters = [("env".into(), "prod".into()), ("service".into(), "web".into())].into();
        let plan = RulesPlanner::new().plan(&w);
        assert_eq!(plan.agent_config.label_matchers.len(), 2);
    }

    #[test]
    fn ddsketch_accuracy_params() {
        let mut w = workload(vec![AggType::Quantile]);
        w.accuracy_sla = 0.005;
        let plan = RulesPlanner::new().plan(&w);
        assert_eq!(plan.agent_config.sketch_params.relative_accuracy, 0.005);
    }

    #[test]
    fn hll_precision_coarse_sla() {
        let mut w = workload(vec![AggType::Cardinality]);
        w.accuracy_sla = 0.03;
        let plan = RulesPlanner::new().plan(&w);
        assert_eq!(plan.agent_config.sketch_params.precision, 10,
            "coarse SLA should use lower precision");
    }

    #[test]
    fn valid_until_in_future() {
        let plan = RulesPlanner::new().plan(&workload(vec![AggType::Quantile]));
        assert!(plan.valid_until > Utc::now(), "valid_until should be in the future");
    }

    #[test]
    fn backend_config_matches_sketch_type() {
        let plan = RulesPlanner::new().plan(&workload(vec![AggType::Quantile]));
        assert_eq!(plan.backend_config.merge_sketch_type, plan.agent_config.sketch_type);
    }

    #[test]
    fn gateway_passthrough() {
        let plan = RulesPlanner::new().plan(&workload(vec![AggType::Quantile]));
        assert!(plan.gateway_config.passthrough);
    }
}
