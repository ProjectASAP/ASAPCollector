//! SP-9: AST-aware hierarchical stage assignment.
//!
//! Splits the optimized [`SketchExpr`] tree produced by the query parser
//! across pipeline stages, emitting a [`StagedPlan`] that carries per-stage
//! sub-plans for:
//!
//! | Stage | Nodes |
//! |---|---|
//! | Agent OTel Collector | `Source`, `Filter`, `Window`, `Agg` (sketch ops) |
//! | Backend OTel Collector | `Partition`, `Merge`, `Dedup`, `Agg { Exact(Sum\|Count\|Min\|Max) }` |
//! | ASAPQuery Precompute Engine | `TopK`, deferred sketch `Agg` ops |
//! | DB-side query | `Agg { Exact(Avg) }` (non-mergeable) |
//!
//! # ExactAgg deferral
//!
//! Mergeability drives `Exact` op placement:
//! - `Sum`, `Count`, `Min`, `Max` — mergeable (`agg(A∪B) = merge(agg(A), agg(B))`) →
//!   **Backend** (the backend collector can combine partial results from N agents).
//! - `Avg` — **not** mergeable (avg-of-avgs ≠ global avg) → **DB-side** query
//!   (must see all data before computing).
//!
//! # Budget-driven deferral chain
//!
//! When a sketch `Agg` node's estimated memory cost exceeds the stage cap in
//! [`StageResourceBudgets`], it is deferred to the next stage:
//!
//! `Agent → Backend → Precompute`
//!
//! The degenerate-but-valid fallback (all sketch ops deferred to Precompute)
//! matches the current flat SP-3 behaviour and ensures the query is always
//! answerable.
//!
//! # PromQL serialisation
//!
//! [`expr_to_promql`] converts the full `SketchExpr` tree to a PromQL
//! expression consumed by the ASAPQuery Precompute Engine's query engine.
//! The sketch data is already ingested by the precompute engine from the
//! backend OTel Collector; the PromQL describes what operation to apply.

use std::time::Duration;

use crate::analyzer::format_duration;
use crate::query_parser::sketch_algebra::{
    ExactAgg, FilterOp, FilterVal, PartitionKeys, SketchAggOp, SketchExpr,
};
use crate::types::{
    AgentSubPlan, BackendSubPlan, DbSubPlan, PrecomputeSubPlan, SketchParams, SketchType,
    StagedPlan, StageResourceBudgets,
};

// ── Public entry point ────────────────────────────────────────────────────────

/// Split a `SketchExpr` tree across pipeline stages, respecting per-stage
/// resource budgets and the `ExactAgg` mergeability rules described above.
///
/// The returned [`StagedPlan`] is attached to
/// [`crate::types::CollectionPlan::staged_plan`] by the caller
/// (`handle_plan` in `main.rs`).
pub fn split_expr_by_stage(expr: &SketchExpr, budgets: &StageResourceBudgets) -> StagedPlan {
    let mut plan = StagedPlan::default();
    walk(expr, &mut plan, budgets);

    // Build the precompute query_expr from the full tree when the precompute
    // stage is active (TopK or deferred sketch ops assigned there).
    if plan.precompute.active {
        plan.precompute.query_expr = expr_to_promql(expr);
    }

    // Build the DB query_expr when Avg was deferred to the DB stage.
    if plan.db.active && plan.db.query_expr.is_empty() {
        plan.db.query_expr = expr_to_promql(expr);
    }

    plan
}

/// Serialise a [`SketchExpr`] tree to a valid PromQL expression.
///
/// The output is consumed by the ASAPQuery Precompute Engine's query engine
/// (PromQL / SQL).  Sketch data is already present in the engine from the
/// backend OTel Collector; the PromQL describes the aggregation to apply.
pub fn expr_to_promql(expr: &SketchExpr) -> String {
    let mut ctx = QueryCtx::default();
    collect_ctx(expr, &mut ctx);
    ctx.to_promql()
}

// ── Tree walker ───────────────────────────────────────────────────────────────

fn walk(expr: &SketchExpr, plan: &mut StagedPlan, budgets: &StageResourceBudgets) {
    match expr {
        // Source — always at Agent; no additional plan fields needed.
        SketchExpr::Source(_) => {}

        // Filter — push label predicates to Agent.
        SketchExpr::Filter { pred, input } => {
            for p in pred {
                if let (FilterOp::Eq, FilterVal::Str(v)) = (&p.op, &p.val) {
                    plan.agent.label_filters.push(format!("{}={}", p.col, v));
                }
            }
            walk(input, plan, budgets);
        }

        // Window — time window lives at Agent.
        SketchExpr::Window { duration, input } => {
            plan.agent.window_secs = Some(duration.as_secs());
            walk(input, plan, budgets);
        }

        // Partition — group-by dims always go to Backend after agent merge.
        SketchExpr::Partition { keys, input } => {
            for k in keys.keys() {
                if !plan.backend.group_by.contains(k) {
                    plan.backend.group_by.push(k.clone());
                }
            }
            walk(input, plan, budgets);
        }

        // Agg — the key assignment decision (see module docs).
        SketchExpr::Agg { op, input, .. } => {
            assign_agg(op, plan, budgets);
            walk(input, plan, budgets);
        }

        // Dedup — absorbed at Backend (HLL dedup is eliminated by R6 upstream).
        SketchExpr::Dedup { input, .. } => {
            plan.backend.has_dedup = true;
            walk(input, plan, budgets);
        }

        // Merge — Backend merges N agent sketches.
        SketchExpr::Merge { inputs } => {
            plan.backend.has_merge = true;
            for i in inputs {
                walk(i, plan, budgets);
            }
        }

        // TopK — always at the Precompute Engine (upper AST).
        SketchExpr::TopK { k, input } => {
            plan.precompute.topk = Some(*k);
            plan.precompute.active = true;
            walk(input, plan, budgets);
        }

        // JoinSketch — outer and inner both walked; join itself stays at Backend.
        SketchExpr::JoinSketch { outer, inner, .. } => {
            plan.backend.has_merge = true;
            walk(outer, plan, budgets);
            walk(inner, plan, budgets);
        }
    }
}

// ── Agg node assignment ───────────────────────────────────────────────────────

fn assign_agg(op: &SketchAggOp, plan: &mut StagedPlan, budgets: &StageResourceBudgets) {
    match op {
        // ── Exact ops: mergeability decides stage ─────────────────────────────
        //
        // Sum/Count/Min/Max satisfy agg(A∪B) = merge(agg(A), agg(B)).
        // The Backend collector can combine partial results from N agents.
        SketchAggOp::Exact(
            ExactAgg::Sum | ExactAgg::Count | ExactAgg::Min | ExactAgg::Max,
        ) => {
            plan.backend.has_merge = true;
        }

        // Avg is NOT mergeable: avg(avg(A), avg(B)) ≠ avg(A∪B).
        // Must see all raw data → DB-side exact query.
        SketchAggOp::Exact(ExactAgg::Avg) => {
            plan.db.active = true;
        }

        // ── Sketch ops: assign to Agent, defer if budget exceeded ─────────────
        sketch_op => {
            let est_mem = estimated_sketch_memory_bytes(sketch_op);
            let stage = resolve_sketch_stage(est_mem, budgets, &mut plan.deferral_log, sketch_op);
            match stage {
                SketchStage::Agent => {
                    plan.agent.sketch_type = Some(agg_op_to_sketch_type(sketch_op));
                    plan.agent.sketch_params = agg_op_to_sketch_params(sketch_op);
                }
                SketchStage::Backend => {
                    // Deferred from Agent: Backend does the sketch insertion.
                    plan.backend.has_merge = true;
                    // Record sketch type on agent as None (passthrough raw).
                }
                SketchStage::Precompute => {
                    plan.precompute.active = true;
                }
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum SketchStage {
    Agent,
    Backend,
    Precompute,
}

/// Walk the deferral chain Agent → Backend → Precompute based on budget caps.
fn resolve_sketch_stage(
    est_mem: u64,
    budgets: &StageResourceBudgets,
    log: &mut Vec<String>,
    op: &SketchAggOp,
) -> SketchStage {
    // Try Agent first.
    if let Some(cap) = budgets.agent_memory_bytes {
        if est_mem > cap {
            log.push(format!(
                "deferred {op:?} Agent→Backend: est_mem={est_mem}B > agent_cap={cap}B"
            ));
            // Try Backend.
            if let Some(be_cap) = budgets.backend_memory_bytes {
                if est_mem > be_cap {
                    log.push(format!(
                        "deferred {op:?} Backend→Precompute: est_mem={est_mem}B > backend_cap={be_cap}B"
                    ));
                    return SketchStage::Precompute;
                }
            }
            return SketchStage::Backend;
        }
    }
    SketchStage::Agent
}

// ── Cost estimation (heuristic, based on benchmark table in cost_model.rs) ────

/// Estimate peak sketch memory per series (bytes) for the given operator.
fn estimated_sketch_memory_bytes(op: &SketchAggOp) -> u64 {
    match op {
        SketchAggOp::DDSketch { .. } | SketchAggOp::ExactMinMax { .. } => 4_096,
        SketchAggOp::HLL { registers } => 1u64 << (*registers as u64),
        SketchAggOp::CountMin { width, depth } => (*width as u64) * (*depth as u64) * 8,
        SketchAggOp::CountSketch { k } => k * 8 * 5, // 5-row hash table approx
        SketchAggOp::Hydra { inner, partition_keys } => {
            // Hydra maintains one inner sketch per distinct key-tuple.
            // Estimate 2^|keys| partitions (capped at 1024 to avoid absurd values).
            let factor = 1u64 << partition_keys.len().min(10);
            estimated_sketch_memory_bytes(inner).saturating_mul(factor)
        }
        SketchAggOp::Exact(_) => 8,
    }
}

// ── Sketch-type helpers ───────────────────────────────────────────────────────

fn agg_op_to_sketch_type(op: &SketchAggOp) -> SketchType {
    match op {
        SketchAggOp::DDSketch { .. } | SketchAggOp::ExactMinMax { .. } => SketchType::DDSketch,
        SketchAggOp::HLL { .. } => SketchType::HLL,
        SketchAggOp::CountMin { .. } => SketchType::CountMinSketch,
        SketchAggOp::CountSketch { .. } => SketchType::CountSketch,
        SketchAggOp::Hydra { inner, .. } => agg_op_to_sketch_type(inner),
        SketchAggOp::Exact(_) => SketchType::DDSketch, // unreachable for sketch stage
    }
}

fn agg_op_to_sketch_params(op: &SketchAggOp) -> SketchParams {
    match op {
        SketchAggOp::DDSketch { quantiles, epsilon } => SketchParams {
            relative_accuracy: *epsilon,
            quantiles: quantiles.clone(),
            ..Default::default()
        },
        SketchAggOp::HLL { registers } => SketchParams {
            precision: *registers as u32,
            ..Default::default()
        },
        SketchAggOp::CountMin { width, depth } => SketchParams {
            rows: *depth as u32,
            cols: *width,
            ..Default::default()
        },
        SketchAggOp::CountSketch { .. } => SketchParams {
            rows: 5,
            cols: 2_048,
            ..Default::default()
        },
        SketchAggOp::Hydra { inner, .. } => agg_op_to_sketch_params(inner),
        SketchAggOp::ExactMinMax { .. } => SketchParams {
            relative_accuracy: 0.01,
            quantiles: vec![0.0, 1.0],
            ..Default::default()
        },
        SketchAggOp::Exact(_) => SketchParams::default(),
    }
}

// ── PromQL serialiser ─────────────────────────────────────────────────────────

/// Context accumulated while walking the tree for PromQL serialisation.
#[derive(Default)]
struct QueryCtx {
    metric:   String,
    filters:  Vec<String>,
    window:   Option<Duration>,
    group_by: Vec<String>,
    agg:      Option<AggInfo>,
    topk:     Option<u64>,
}

#[derive(Clone)]
enum AggInfo {
    DDSketch { phi: f64 },
    HLL,
    CountMin,
    CountSketch,
    ExactCount,
    ExactSum,
    ExactAvg,
    ExactMin,
    ExactMax,
    ExactMinMax,
}

impl QueryCtx {
    fn to_promql(&self) -> String {
        let selector = if self.filters.is_empty() {
            self.metric.clone()
        } else {
            format!("{}{{{}}}", self.metric, self.filters.join(", "))
        };

        let window = self
            .window
            .map(|w| format!("[{}]", format_duration(w)))
            .unwrap_or_default();

        let by_clause = if self.group_by.is_empty() {
            String::new()
        } else {
            format!(" by ({})", self.group_by.join(", "))
        };

        let inner = match &self.agg {
            Some(AggInfo::DDSketch { phi }) => {
                format!("quantile_over_time({phi}, {selector}{window}){by_clause}")
            }
            Some(AggInfo::HLL) => {
                format!("count_distinct_over_time({selector}{window}){by_clause}")
            }
            Some(AggInfo::CountMin | AggInfo::CountSketch | AggInfo::ExactCount) => {
                format!("count_over_time({selector}{window}){by_clause}")
            }
            Some(AggInfo::ExactSum) => {
                format!("sum_over_time({selector}{window}){by_clause}")
            }
            Some(AggInfo::ExactAvg) => {
                format!("avg_over_time({selector}{window}){by_clause}")
            }
            Some(AggInfo::ExactMin) => {
                format!("min_over_time({selector}{window}){by_clause}")
            }
            Some(AggInfo::ExactMax) => {
                format!("max_over_time({selector}{window}){by_clause}")
            }
            Some(AggInfo::ExactMinMax) => {
                // Represent as quantile range [0,1] for min/max via DDSketch.
                format!("quantile_over_time(0.5, {selector}{window}){by_clause}")
            }
            None => format!("{selector}{window}{by_clause}"),
        };

        if let Some(k) = self.topk {
            format!("topk({k}, {inner})")
        } else {
            inner
        }
    }
}

/// Walk the tree and fill [`QueryCtx`]; string building is deferred to
/// [`QueryCtx::to_promql`].
fn collect_ctx(expr: &SketchExpr, ctx: &mut QueryCtx) {
    match expr {
        SketchExpr::Source(s) => {
            if ctx.metric.is_empty() {
                ctx.metric = s.name.clone();
            }
        }
        SketchExpr::Filter { pred, input } => {
            for p in pred {
                match (&p.op, &p.val) {
                    (FilterOp::Eq, FilterVal::Str(v)) => {
                        ctx.filters.push(format!("{}=\"{}\"", p.col, v));
                    }
                    (FilterOp::Ne, FilterVal::Str(v)) => {
                        ctx.filters.push(format!("{}!=\"{}\"", p.col, v));
                    }
                    (FilterOp::Regex(re), _) => {
                        ctx.filters.push(format!("{}=~\"{}\"", p.col, re));
                    }
                    (FilterOp::NotRegex(re), _) => {
                        ctx.filters.push(format!("{}!~\"{}\"", p.col, re));
                    }
                    _ => {} // other ops (Lt/Gt/…) not natively supported in PromQL label matchers
                }
            }
            collect_ctx(input, ctx);
        }
        SketchExpr::Window { duration, input } => {
            if ctx.window.is_none() {
                ctx.window = Some(*duration);
            }
            collect_ctx(input, ctx);
        }
        SketchExpr::Partition { keys, input } => {
            for k in keys.keys() {
                if !ctx.group_by.contains(k) {
                    ctx.group_by.push(k.clone());
                }
            }
            collect_ctx(input, ctx);
        }
        SketchExpr::Agg { op, input, .. } => {
            if ctx.agg.is_none() {
                ctx.agg = Some(agg_info(op));
            }
            collect_ctx(input, ctx);
        }
        SketchExpr::TopK { k, input } => {
            ctx.topk = Some(*k);
            collect_ctx(input, ctx);
        }
        SketchExpr::Dedup { input, .. } => collect_ctx(input, ctx),
        SketchExpr::Merge { inputs } => {
            // All branches should share the same shape; serialise the first.
            if let Some(first) = inputs.first() {
                collect_ctx(first, ctx);
            }
        }
        SketchExpr::JoinSketch { outer, .. } => collect_ctx(outer, ctx),
    }
}

fn agg_info(op: &SketchAggOp) -> AggInfo {
    match op {
        SketchAggOp::DDSketch { quantiles, .. } => AggInfo::DDSketch {
            phi: quantiles.first().copied().unwrap_or(0.99),
        },
        SketchAggOp::HLL { .. } => AggInfo::HLL,
        SketchAggOp::CountMin { .. } => AggInfo::CountMin,
        SketchAggOp::CountSketch { .. } => AggInfo::CountSketch,
        SketchAggOp::ExactMinMax { .. } => AggInfo::ExactMinMax,
        SketchAggOp::Exact(ExactAgg::Count) => AggInfo::ExactCount,
        SketchAggOp::Exact(ExactAgg::Sum) => AggInfo::ExactSum,
        SketchAggOp::Exact(ExactAgg::Avg) => AggInfo::ExactAvg,
        SketchAggOp::Exact(ExactAgg::Min) => AggInfo::ExactMin,
        SketchAggOp::Exact(ExactAgg::Max) => AggInfo::ExactMax,
        SketchAggOp::Hydra { inner, .. } => agg_info(inner),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_parser::sketch_algebra::{
        ColumnRef, FilterVal, Predicate, SketchExpr, SketchAggOp, SourceSpec,
    };

    fn source(name: &str) -> SketchExpr {
        SketchExpr::Source(SourceSpec { name: name.into() })
    }

    fn no_budget() -> StageResourceBudgets {
        StageResourceBudgets::default()
    }

    // ── Node-to-stage assignment ──────────────────────────────────────────────

    /// Source + Filter + Window + Agg(DDSketch) → all at Agent.
    #[test]
    fn ddsketch_agg_goes_to_agent() {
        let expr = SketchExpr::Window {
            duration: Duration::from_secs(300),
            input: Box::new(SketchExpr::Agg {
                op: SketchAggOp::default_ddsketch(vec![0.99]),
                col: ColumnRef::SampleValue,
                input: Box::new(source("latency")),
            }),
        };
        let plan = split_expr_by_stage(&expr, &no_budget());
        assert_eq!(plan.agent.sketch_type, Some(SketchType::DDSketch));
        assert_eq!(plan.agent.window_secs, Some(300));
        assert!(!plan.precompute.active);
        assert!(!plan.db.active);
    }

    /// HLL stays at Agent by default.
    #[test]
    fn hll_agg_goes_to_agent() {
        let expr = SketchExpr::Agg {
            op: SketchAggOp::default_hll(),
            col: ColumnRef::Wildcard,
            input: Box::new(source("requests")),
        };
        let plan = split_expr_by_stage(&expr, &no_budget());
        assert_eq!(plan.agent.sketch_type, Some(SketchType::HLL));
    }

    /// Partition keys go to Backend regardless.
    #[test]
    fn partition_goes_to_backend() {
        let expr = SketchExpr::Partition {
            keys: PartitionKeys::By(vec!["host".into(), "region".into()]),
            input: Box::new(SketchExpr::Agg {
                op: SketchAggOp::default_ddsketch(vec![0.5]),
                col: ColumnRef::SampleValue,
                input: Box::new(source("cpu")),
            }),
        };
        let plan = split_expr_by_stage(&expr, &no_budget());
        assert!(plan.backend.group_by.contains(&"host".to_string()));
        assert!(plan.backend.group_by.contains(&"region".to_string()));
    }

    /// TopK → Precompute Engine.
    #[test]
    fn topk_goes_to_precompute() {
        let expr = SketchExpr::TopK {
            k: 10,
            input: Box::new(SketchExpr::Agg {
                op: SketchAggOp::CountSketch { k: 10 },
                col: ColumnRef::Wildcard,
                input: Box::new(source("events")),
            }),
        };
        let plan = split_expr_by_stage(&expr, &no_budget());
        assert!(plan.precompute.active);
        assert_eq!(plan.precompute.topk, Some(10));
    }

    /// Exact(Sum) is mergeable → Backend.
    #[test]
    fn exact_sum_goes_to_backend() {
        let expr = SketchExpr::Agg {
            op: SketchAggOp::Exact(ExactAgg::Sum),
            col: ColumnRef::Named("bytes".into()),
            input: Box::new(source("network")),
        };
        let plan = split_expr_by_stage(&expr, &no_budget());
        // Sum is handled at backend (merge); agent gets no sketch type.
        assert!(plan.agent.sketch_type.is_none());
        assert!(!plan.db.active);
        assert!(plan.backend.has_merge);
    }

    /// Exact(Count) is mergeable → Backend.
    #[test]
    fn exact_count_goes_to_backend() {
        let expr = SketchExpr::Agg {
            op: SketchAggOp::Exact(ExactAgg::Count),
            col: ColumnRef::Wildcard,
            input: Box::new(source("hits")),
        };
        let plan = split_expr_by_stage(&expr, &no_budget());
        assert!(plan.agent.sketch_type.is_none());
        assert!(plan.backend.has_merge);
        assert!(!plan.db.active);
    }

    /// Exact(Avg) is NOT mergeable → DB stage.
    #[test]
    fn exact_avg_goes_to_db() {
        let expr = SketchExpr::Agg {
            op: SketchAggOp::Exact(ExactAgg::Avg),
            col: ColumnRef::Named("price".into()),
            input: Box::new(source("ticks")),
        };
        let plan = split_expr_by_stage(&expr, &no_budget());
        assert!(plan.db.active);
        assert!(!plan.precompute.active);
    }

    /// Exact(Min) is mergeable → Backend (min(A∪B) = min(min(A), min(B))).
    #[test]
    fn exact_min_goes_to_backend() {
        let expr = SketchExpr::Agg {
            op: SketchAggOp::Exact(ExactAgg::Min),
            col: ColumnRef::Named("latency".into()),
            input: Box::new(source("svc")),
        };
        let plan = split_expr_by_stage(&expr, &no_budget());
        assert!(plan.backend.has_merge);
        assert!(!plan.db.active);
    }

    /// Exact(Max) is mergeable → Backend.
    #[test]
    fn exact_max_goes_to_backend() {
        let expr = SketchExpr::Agg {
            op: SketchAggOp::Exact(ExactAgg::Max),
            col: ColumnRef::Named("latency".into()),
            input: Box::new(source("svc")),
        };
        let plan = split_expr_by_stage(&expr, &no_budget());
        assert!(plan.backend.has_merge);
        assert!(!plan.db.active);
    }

    /// Dedup node → Backend.
    #[test]
    fn dedup_goes_to_backend() {
        let expr = SketchExpr::Dedup {
            col: "user_id".into(),
            input: Box::new(SketchExpr::Agg {
                op: SketchAggOp::default_hll(),
                col: ColumnRef::Named("user_id".into()),
                input: Box::new(source("events")),
            }),
        };
        let plan = split_expr_by_stage(&expr, &no_budget());
        assert!(plan.backend.has_dedup);
    }

    // ── Budget-driven deferral ────────────────────────────────────────────────

    /// When the DDSketch memory (4 096 B) exceeds the agent cap, it defers
    /// to Backend.
    #[test]
    fn ddsketch_deferred_to_backend_when_agent_budget_exceeded() {
        let budgets = StageResourceBudgets {
            agent_memory_bytes: Some(1_024), // tighter than 4 096
            ..Default::default()
        };
        let expr = SketchExpr::Agg {
            op: SketchAggOp::default_ddsketch(vec![0.99]),
            col: ColumnRef::SampleValue,
            input: Box::new(source("latency")),
        };
        let plan = split_expr_by_stage(&expr, &budgets);
        // Deferred: no sketch at Agent.
        assert!(plan.agent.sketch_type.is_none());
        // Backend takes over (merge path).
        assert!(plan.backend.has_merge);
        // Deferral logged.
        assert!(!plan.deferral_log.is_empty());
        assert!(plan.deferral_log[0].contains("Agent→Backend"));
    }

    /// When both agent AND backend caps are exceeded, defers to Precompute.
    #[test]
    fn ddsketch_deferred_to_precompute_when_both_budgets_exceeded() {
        let budgets = StageResourceBudgets {
            agent_memory_bytes:   Some(1_024),
            backend_memory_bytes: Some(1_024),
            ..Default::default()
        };
        let expr = SketchExpr::Agg {
            op: SketchAggOp::default_ddsketch(vec![0.99]),
            col: ColumnRef::SampleValue,
            input: Box::new(source("latency")),
        };
        let plan = split_expr_by_stage(&expr, &budgets);
        assert!(plan.precompute.active);
        assert!(plan.deferral_log.iter().any(|l| l.contains("Backend→Precompute")));
    }

    /// No budget set → Agent assignment regardless of sketch size.
    #[test]
    fn no_budget_always_assigns_to_agent() {
        let expr = SketchExpr::Agg {
            op: SketchAggOp::CountMin { width: 2_000_000, depth: 255 }, // huge
            col: ColumnRef::Wildcard,
            input: Box::new(source("events")),
        };
        let plan = split_expr_by_stage(&expr, &no_budget());
        assert_eq!(plan.agent.sketch_type, Some(SketchType::CountMinSketch));
        assert!(plan.deferral_log.is_empty());
    }

    // ── PromQL serialisation ──────────────────────────────────────────────────

    /// TopK(CountSketch) over filtered, windowed, partitioned metric.
    #[test]
    fn topk_countsketch_promql() {
        let expr = SketchExpr::TopK {
            k: 10,
            input: Box::new(SketchExpr::Partition {
                keys: PartitionKeys::By(vec!["symbol".into()]),
                input: Box::new(SketchExpr::Window {
                    duration: Duration::from_secs(300),
                    input: Box::new(SketchExpr::Filter {
                        pred: vec![Predicate {
                            col: "sectype".into(),
                            op:  FilterOp::Eq,
                            val: FilterVal::Str("E".into()),
                        }],
                        input: Box::new(SketchExpr::Agg {
                            op:    SketchAggOp::CountSketch { k: 10 },
                            col:   ColumnRef::Wildcard,
                            input: Box::new(source("financial.last_trade_price")),
                        }),
                    }),
                }),
            }),
        };
        let ql = expr_to_promql(&expr);
        assert!(ql.contains("topk(10,"), "missing topk: {ql}");
        assert!(ql.contains("count_over_time"), "missing count_over_time: {ql}");
        assert!(ql.contains("financial.last_trade_price"), "missing metric: {ql}");
        assert!(ql.contains("sectype=\"E\""), "missing filter: {ql}");
        assert!(ql.contains("[5m]"), "missing window: {ql}");
        assert!(ql.contains("by (symbol)"), "missing group_by: {ql}");
    }

    /// DDSketch quantile query.
    #[test]
    fn ddsketch_promql() {
        let expr = SketchExpr::Agg {
            op:    SketchAggOp::default_ddsketch(vec![0.99]),
            col:   ColumnRef::SampleValue,
            input: Box::new(SketchExpr::Window {
                duration: Duration::from_secs(300),
                input: Box::new(source("latency")),
            }),
        };
        let ql = expr_to_promql(&expr);
        assert!(ql.contains("quantile_over_time(0.99,"), "missing quantile: {ql}");
        assert!(ql.contains("latency"), "missing metric: {ql}");
        assert!(ql.contains("[5m]"), "missing window: {ql}");
    }

    /// HLL cardinality query.
    #[test]
    fn hll_promql() {
        let expr = SketchExpr::Agg {
            op:    SketchAggOp::default_hll(),
            col:   ColumnRef::Wildcard,
            input: Box::new(source("users")),
        };
        let ql = expr_to_promql(&expr);
        assert!(ql.contains("count_distinct_over_time"), "missing fn: {ql}");
        assert!(ql.contains("users"), "missing metric: {ql}");
    }

    /// Exact(Avg) produces avg_over_time.
    #[test]
    fn exact_avg_promql() {
        let expr = SketchExpr::Agg {
            op:    SketchAggOp::Exact(ExactAgg::Avg),
            col:   ColumnRef::Named("price".into()),
            input: Box::new(source("ticks")),
        };
        let ql = expr_to_promql(&expr);
        assert!(ql.contains("avg_over_time"), "missing avg: {ql}");
    }

    /// Exact(Sum) produces sum_over_time.
    #[test]
    fn exact_sum_promql() {
        let expr = SketchExpr::Agg {
            op:    SketchAggOp::Exact(ExactAgg::Sum),
            col:   ColumnRef::Named("bytes".into()),
            input: Box::new(source("net")),
        };
        let ql = expr_to_promql(&expr);
        assert!(ql.contains("sum_over_time"), "missing sum: {ql}");
    }

    // ── Sketch parameter extraction ───────────────────────────────────────────

    #[test]
    fn ddsketch_params_extracted_correctly() {
        let expr = SketchExpr::Agg {
            op:    SketchAggOp::DDSketch { quantiles: vec![0.5, 0.99], epsilon: 0.005 },
            col:   ColumnRef::SampleValue,
            input: Box::new(source("latency")),
        };
        let plan = split_expr_by_stage(&expr, &no_budget());
        let p = &plan.agent.sketch_params;
        assert_eq!(p.relative_accuracy, 0.005);
        assert_eq!(p.quantiles, vec![0.5, 0.99]);
    }

    #[test]
    fn hll_precision_extracted() {
        let expr = SketchExpr::Agg {
            op:    SketchAggOp::HLL { registers: 14 },
            col:   ColumnRef::Wildcard,
            input: Box::new(source("users")),
        };
        let plan = split_expr_by_stage(&expr, &no_budget());
        assert_eq!(plan.agent.sketch_params.precision, 14);
    }

    #[test]
    fn countmin_params_extracted() {
        let expr = SketchExpr::Agg {
            op:    SketchAggOp::CountMin { width: 2_000, depth: 5 },
            col:   ColumnRef::Wildcard,
            input: Box::new(source("events")),
        };
        let plan = split_expr_by_stage(&expr, &no_budget());
        assert_eq!(plan.agent.sketch_params.rows, 5);
        assert_eq!(plan.agent.sketch_params.cols, 2_000);
    }

    // ── Filter predicate extraction ───────────────────────────────────────────

    #[test]
    fn filter_eq_captured_in_agent_plan() {
        let expr = SketchExpr::Filter {
            pred: vec![Predicate {
                col: "env".into(),
                op:  FilterOp::Eq,
                val: FilterVal::Str("prod".into()),
            }],
            input: Box::new(source("latency")),
        };
        let plan = split_expr_by_stage(&expr, &no_budget());
        assert!(plan.agent.label_filters.contains(&"env=prod".to_string()));
    }

    // ── Precompute query_expr generation ─────────────────────────────────────

    /// When TopK is present, `staged_plan.precompute.query_expr` must be a
    /// non-empty PromQL string.
    #[test]
    fn precompute_query_expr_set_when_topk_present() {
        let expr = SketchExpr::TopK {
            k: 5,
            input: Box::new(SketchExpr::Agg {
                op:    SketchAggOp::default_hll(),
                col:   ColumnRef::Wildcard,
                input: Box::new(source("sessions")),
            }),
        };
        let plan = split_expr_by_stage(&expr, &no_budget());
        assert!(plan.precompute.active);
        assert!(!plan.precompute.query_expr.is_empty());
        assert!(plan.precompute.query_expr.contains("topk(5,"));
    }

    // ── StageResourceBudgets::from_workload_chars ─────────────────────────────

    #[test]
    fn budgets_from_workload_chars_propagates_memory() {
        use crate::types::WorkloadCharacteristics;
        let wc = WorkloadCharacteristics {
            memory_budget_bytes: Some(8_192),
            ..Default::default()
        };
        let b = StageResourceBudgets::from_workload_chars(&wc);
        assert_eq!(b.agent_memory_bytes, Some(8_192));
        assert!(b.backend_memory_bytes.is_none());
    }
}
