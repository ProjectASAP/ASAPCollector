use std::collections::HashMap;
use std::time::Duration;

use crate::types::*;
use super::rules::{RulesPlanner, default_sketch_params, select_window_strategy};

// ── Benchmark-derived cost table ──────────────────────────────────────────────
//
// Source: e2e benchmark results (2026-03-15), 1 000 series × 1 000 Hz row.
// Units: bandwidth bytes/series/sec, CPU µs/sample, memory bytes/sketch.

#[derive(Debug, Clone, Copy)]
pub struct SketchCosts {
    pub bytes_per_series_per_sec:  f64,
    pub cpu_micros_per_sample:     f64,
    pub base_memory_bytes:         f64,
    pub relative_error_at_default: f64,
}

fn benchmark_table() -> HashMap<SketchType, SketchCosts> {
    [
        (SketchType::DDSketch, SketchCosts {
            bytes_per_series_per_sec:  120.0,
            cpu_micros_per_sample:     0.8,
            base_memory_bytes:         4_096.0,
            relative_error_at_default: 0.01,
        }),
        (SketchType::KLL, SketchCosts {
            bytes_per_series_per_sec:  80.0,
            cpu_micros_per_sample:     0.5,
            base_memory_bytes:         2_048.0,
            relative_error_at_default: 0.02,
        }),
        (SketchType::HLL, SketchCosts {
            bytes_per_series_per_sec:  40.0,
            cpu_micros_per_sample:     0.3,
            base_memory_bytes:         16_384.0, // precision=14 → 16 KB
            relative_error_at_default: 0.008,
        }),
        (SketchType::CountSketch, SketchCosts {
            bytes_per_series_per_sec:  200.0,
            cpu_micros_per_sample:     1.2,
            base_memory_bytes:         40_960.0,
            relative_error_at_default: 0.01,
        }),
        (SketchType::CountMinSketch, SketchCosts {
            bytes_per_series_per_sec:  200.0,
            cpu_micros_per_sample:     1.0,
            base_memory_bytes:         40_960.0,
            relative_error_at_default: 0.01,
        }),
    ].into()
}

// ── Scoring ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct PlanScore {
    pub bandwidth_bytes_per_sec: f64,
    pub cpu_micros_per_sample:   f64,
    pub memory_bytes:            f64,
    pub estimated_error:         f64,
    pub meets_sla:               bool,
}

/// Estimates resource costs for a given plan + workload.
pub fn score(plan: &CollectionPlan, w: &QueryWorkload) -> PlanScore {
    let table = benchmark_table();
    let st = &plan.agent_config.sketch_type;

    let Some(&costs) = table.get(st) else {
        return PlanScore {
            bandwidth_bytes_per_sec: f64::MAX,
            cpu_micros_per_sample:   f64::MAX,
            memory_bytes:            f64::MAX,
            estimated_error:         1.0,
            meets_sla:               false,
        };
    };

    // More preserved dimensions → more distinct sketches in flight.
    let dim_multiplier = (plan.agent_config.aggregate_by.len() + 1) as f64;

    let bandwidth = costs.bytes_per_series_per_sec * dim_multiplier;
    let memory    = costs.base_memory_bytes        * dim_multiplier;
    let err       = estimate_error(st, &plan.agent_config.sketch_params, costs);

    let sla = if w.accuracy_sla <= 0.0 { 0.01 } else { w.accuracy_sla };

    PlanScore {
        bandwidth_bytes_per_sec: bandwidth,
        cpu_micros_per_sample:   costs.cpu_micros_per_sample,
        memory_bytes:            memory,
        estimated_error:         err,
        meets_sla:               err <= sla,
    }
}

fn estimate_error(st: &SketchType, p: &SketchParams, costs: SketchCosts) -> f64 {
    match st {
        SketchType::DDSketch if p.relative_accuracy > 0.0 => p.relative_accuracy,
        SketchType::KLL      if p.k > 0                   => 1.0 / p.k as f64,
        SketchType::HLL      if p.precision > 0           =>
            1.04 / (2.0f64.powi(p.precision as i32)).sqrt(),
        _ => costs.relative_error_at_default,
    }
}

// ── CostModelPlanner ──────────────────────────────────────────────────────────

/// Extends the rule-based planner by scoring all valid sketch candidates and
/// choosing the one with the lowest bandwidth that still meets the AccuracySLA.
pub struct CostModelPlanner {
    inner: RulesPlanner,
}

impl CostModelPlanner {
    pub fn new() -> Self {
        Self { inner: RulesPlanner::new() }
    }

    pub fn plan(&self, w: &QueryWorkload) -> CollectionPlan {
        let candidates = candidates_for_workload(w);

        // Start with the rule-based plan as the baseline.
        let baseline = self.inner.plan(w);
        let mut best_plan  = baseline;
        let mut best_score = score(&best_plan, w);

        for st in candidates {
            let params = default_sketch_params(&st, w.accuracy_sla);
            let (mode, window_duration) = select_window_strategy(w);

            let mut trial = self.inner.plan(w);
            trial.agent_config.sketch_type     = st.clone();
            trial.agent_config.sketch_params   = params;
            trial.agent_config.mode            = mode;
            trial.agent_config.window_duration = window_duration;
            trial.backend_config.merge_sketch_type = st;

            let s = score(&trial, w);
            if !s.meets_sla { continue; }

            if s.bandwidth_bytes_per_sec < best_score.bandwidth_bytes_per_sec
                || !best_score.meets_sla
            {
                best_plan  = trial;
                best_score = s;
            }
        }

        best_plan
    }
}

/// Returns all sketch types that are semantically valid for the workload's
/// aggregation types.
fn candidates_for_workload(w: &QueryWorkload) -> Vec<SketchType> {
    let mut out = Vec::new();
    for agg in &w.aggregations {
        match agg {
            AggType::Quantile    => { out.push(SketchType::DDSketch); out.push(SketchType::KLL); }
            AggType::Cardinality => out.push(SketchType::HLL),
            AggType::Frequency   => { out.push(SketchType::CountSketch); out.push(SketchType::CountMinSketch); }
        }
    }
    out
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use chrono::Utc;

    fn workload(aggs: Vec<AggType>) -> QueryWorkload {
        QueryWorkload {
            metric_name:    "test".into(),
            label_filters:  HashMap::new(),
            group_by_labels: vec![],
            aggregations:   aggs,
            time_window:    Duration::from_secs(300),
            repeat_every:   None,
            accuracy_sla:   0.01,
            latency_sla:    None,
        }
    }

    fn dummy_plan(st: SketchType) -> CollectionPlan {
        CollectionPlan {
            agent_config: AgentCollectorConfig {
                output_mode:     OutputMode::Sketch,
                sketch_type:     st.clone(),
                sketch_params:   default_sketch_params(&st, 0.01),
                aggregate_by:    vec![],
                label_matchers:  vec![],
                window_duration: Some(Duration::from_secs(300)),
                mode:            ProcessorMode::Window,
                transmit_sketch: true,
                drop_original:   true,
            },
            gateway_config: GatewayCollectorConfig { passthrough: true },
            backend_config: BackendCollectorConfig { merge_sketch_type: st, group_by: vec![] },
            precompute:  vec![],
            valid_until: Utc::now(),
        }
    }

    #[test]
    fn ddsketch_meets_sla_at_1pct() {
        let w = workload(vec![AggType::Quantile]);
        let s = score(&dummy_plan(SketchType::DDSketch), &w);
        assert!(s.meets_sla, "DDSketch at 1% should meet 1% SLA, error={}", s.estimated_error);
    }

    #[test]
    fn ddsketch_fails_tight_sla() {
        let w = QueryWorkload { accuracy_sla: 0.001, ..workload(vec![AggType::Quantile]) };
        // Force 1% params despite tighter SLA.
        let mut plan = dummy_plan(SketchType::DDSketch);
        plan.agent_config.sketch_params.relative_accuracy = 0.01;
        let s = score(&plan, &w);
        assert!(!s.meets_sla, "DDSketch at 1% should NOT meet 0.1% SLA");
    }

    #[test]
    fn hll_lower_bandwidth_than_ddsketch() {
        let w = workload(vec![AggType::Quantile]);
        let s_dd  = score(&dummy_plan(SketchType::DDSketch), &w);
        let s_hll = score(&dummy_plan(SketchType::HLL),     &w);
        assert!(s_hll.bandwidth_bytes_per_sec < s_dd.bandwidth_bytes_per_sec);
    }

    #[test]
    fn dim_multiplier_increases_bandwidth() {
        let w_few  = QueryWorkload { group_by_labels: vec!["host".into()], ..workload(vec![AggType::Quantile]) };
        let w_many = QueryWorkload { group_by_labels: vec!["host".into(),"service".into(),"zone".into(),"region".into()], ..workload(vec![AggType::Quantile]) };
        let pl = RulesPlanner::new();
        let s_few  = score(&pl.plan(&w_few),  &w_few);
        let s_many = score(&pl.plan(&w_many), &w_many);
        assert!(s_many.bandwidth_bytes_per_sec > s_few.bandwidth_bytes_per_sec);
    }

    #[test]
    fn kll_error_formula() {
        let w = QueryWorkload { accuracy_sla: 0.02, ..workload(vec![AggType::Quantile]) };
        let mut plan = dummy_plan(SketchType::KLL);
        plan.agent_config.sketch_params.k = 100; // error ≈ 1/100 = 1%
        let s = score(&plan, &w);
        assert!(s.meets_sla, "KLL k=100 (error~1%) should meet 2% SLA");
    }

    #[test]
    fn cost_model_planner_meets_sla_for_all_agg_types() {
        let pl = CostModelPlanner::new();
        for (agg, sla) in [
            (AggType::Quantile,    0.01),
            (AggType::Cardinality, 0.01),
            (AggType::Frequency,   0.02),
        ] {
            let w = QueryWorkload { accuracy_sla: sla, ..workload(vec![agg]) };
            let plan = pl.plan(&w);
            let s = score(&plan, &w);
            assert!(s.meets_sla, "agg={} sla={sla}: plan does not meet SLA (error={})",
                w.aggregations[0], s.estimated_error);
        }
    }

    #[test]
    fn cost_model_prefers_lower_bandwidth_for_cardinality() {
        let w = QueryWorkload { accuracy_sla: 0.02, ..workload(vec![AggType::Cardinality]) };
        let plan = CostModelPlanner::new().plan(&w);
        assert_eq!(plan.agent_config.sketch_type, SketchType::HLL,
            "HLL should win for cardinality (lowest bandwidth)");
    }

    #[test]
    fn cost_model_valid_until_in_future() {
        let plan = CostModelPlanner::new().plan(&workload(vec![AggType::Quantile]));
        assert!(plan.valid_until > Utc::now());
    }
}
