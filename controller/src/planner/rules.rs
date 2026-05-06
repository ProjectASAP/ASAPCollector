use chrono::Utc;
use std::time::Duration;

use crate::types::*;

pub const DEFAULT_VALID_FOR: Duration = Duration::from_secs(10 * 60);

/// Env-var that opts the planner into the typed L4 binding path
/// (`sketch_algebra::bind_query_expr`). Additive — when unset, the
/// existing untyped `algebra::directory::sketch_type_for_agg` path runs
/// unchanged. Phase E (stage_split refactor) is the natural migration
/// point at which the typed path becomes the only path.
///
/// Set `USE_TYPED_SKETCH_ALGEBRA=1` to opt in.
#[allow(dead_code)]
pub const ENV_USE_TYPED_SKETCH_ALGEBRA: &str = "USE_TYPED_SKETCH_ALGEBRA";

/// Whether the typed L4 binding path is enabled for this process.
/// Reads the env var once per call (cheap; called per `plan()` invocation
/// at most). Phase C is additive — both code paths produce the same
/// `CollectionPlan` shape; the typed path is a *parallel* binding that
/// the planner can compare against the legacy path during development.
#[allow(dead_code)]
pub fn typed_sketch_algebra_enabled() -> bool {
    matches!(
        std::env::var(ENV_USE_TYPED_SKETCH_ALGEBRA).as_deref(),
        Ok("1") | Ok("true") | Ok("yes")
    )
}

/// Bind a `QueryWorkload` into the typed L4 [`crate::sketch_algebra::SketchExpr`]
/// IR, when callers want to inspect the typed binding alongside the
/// legacy `CollectionPlan` output.
///
/// Lowers the workload's first aggregation intent into an `AggIntent`
/// (Phase C scope: single-intent workloads — workloads with multiple
/// intents fall through to `None`), then calls
/// `sketch_algebra::bind_query_expr` with the workload's accuracy SLA
/// translated through `AccuracyTarget::from_legacy_accuracy_sla`.
///
/// Returns `None` when the workload shape is not yet supported by the
/// typed path (multi-intent, raw-required, or no aggregations) — the
/// caller should then fall back to the legacy `plan()` output.
///
/// Phase B (MVP v6) wires `main::handle_plan` to call this whenever
/// the parallel `USE_TYPED_STAGE_SPLIT` gate is enabled — the bound
/// `SketchExpr` is then fed into `planner::stage_split::split_typed_three_stage`
/// + the per-stage emitters in `config::stage_config`.
pub fn bind_workload_typed(
    w: &QueryWorkload,
) -> Option<crate::sketch_algebra::SketchExpr> {
    use crate::intent_algebra::{AggIntent as L3AggIntent, QueryExpr, Schema, Source, WindowKind};
    use crate::intent_algebra::schema::{Column, DataType};
    use crate::sketch_algebra::bind_query_expr;
    use crate::types_v2::AccuracyTarget;

    if w.exact_required {
        return None;
    }
    if w.aggregations.len() != 1 {
        return None;
    }

    // QueryWorkload::accuracy_sla in the legacy planner is interpreted
    // directly as the ε bound (e.g. `0.01` ⇒ ε=0.01). The L3/L4 typed
    // form is `AccuracyTarget::Epsilon(eps)` with the same semantic.
    let accuracy = if w.accuracy_sla > 0.0 {
        AccuracyTarget::Epsilon(w.accuracy_sla)
    } else {
        AccuracyTarget::Exact
    };
    let intent_accuracy = accuracy.clone();

    let intent = match w.aggregations[0] {
        AggType::Quantile => L3AggIntent::Quantile {
            q: w.quantiles.first().copied().unwrap_or(0.99),
            accuracy: intent_accuracy,
        },
        AggType::Cardinality => L3AggIntent::Cardinality {
            accuracy: intent_accuracy,
        },
        AggType::Frequency => L3AggIntent::Frequency {
            accuracy: intent_accuracy,
        },
    };

    let scan = QueryExpr::Scan {
        source: Source::TimeSeries {
            metric: w.metric_name.clone(),
        },
        label_filters: w
            .label_filters
            .iter()
            .map(|(k, v)| crate::intent_algebra::LabelFilter {
                label: k.clone(),
                equals: v.clone(),
            })
            .collect(),
        schema: Schema::with_time_index(
            vec![
                Column {
                    name: "ts".into(),
                    dtype: DataType::Timestamp,
                    nullable: false,
                },
                Column {
                    name: "value".into(),
                    dtype: DataType::Float64,
                    nullable: false,
                },
            ],
            0,
            vec![vec![0]],
        ),
    };
    let windowed = QueryExpr::Window {
        kind: WindowKind::Sliding,
        size: w.time_window,
        slide: None,
        child: Box::new(scan),
    };
    let aggregate = QueryExpr::Aggregate {
        by: vec![],
        aggs: vec![intent],
        having: None,
        child: Box::new(windowed),
    };

    bind_query_expr(&aggregate, accuracy).ok()
}

pub struct RulesPlanner {
    pub valid_for: Duration,
    pub sketch_defaults: SketchDefaults,
}

impl RulesPlanner {
    pub fn new() -> Self {
        Self {
            valid_for: DEFAULT_VALID_FOR,
            sketch_defaults: SketchDefaults::default(),
        }
    }

    pub fn with_defaults(defaults: SketchDefaults) -> Self {
        Self {
            valid_for: DEFAULT_VALID_FOR,
            sketch_defaults: defaults,
        }
    }

    pub fn plan(&self, w: &QueryWorkload) -> CollectionPlan {
        // When exact computation is required (RSI, MACD, stateful indicators),
        // skip sketch selection and return a raw-passthrough plan.
        if w.exact_required {
            return self.raw_passthrough_plan(w);
        }

        let sketch_type = crate::algebra::directory::sketch_type_for_agg(&w.aggregations);
        let sketch_params = crate::algebra::directory::build_sketch_params(&self.sketch_defaults, &sketch_type, w.accuracy_sla, &w.quantiles);
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
                enable_series_id: true,
                series_id_ttl_secs: 0,

                data_sink: AgentDataSink::default(),
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
                enable_series_id: true,
                series_id_ttl_secs: 0,

                data_sink: AgentDataSink::default(),
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

// ── Sketch selection (delegated to algebra::directory) ───────────────────────

pub use crate::algebra::directory::{default_sketch_params, build_sketch_params};

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
