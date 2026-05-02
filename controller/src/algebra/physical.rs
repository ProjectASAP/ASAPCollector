//! Layer 5 — Physical plan IR.
//!
//! Maps the implementation-independent [`AggIntent`] (Layer 3) to concrete
//! sketch implementations and pipeline stages.
//!
//! # Key types
//!
//! - [`PhysicalAggOp`] — resolved AggIntent → concrete SketchType + SketchParams
//! - [`PhysicalOp`] — a physical operator (sketch build, merge, exchange, eval, etc.)
//! - [`PhysicalNode`] — a node in the physical plan tree (operator + placement + cost)
//! - [`Placement`] — where a physical operator runs (Agent, Backend, PromSketch, DB, etc.)

use std::time::Duration;

use crate::algebra::directory;
use crate::algebra::expr::{AggIntent, WindowKind, WindowSpec};
use crate::types::{SketchParams, SketchType};

// ── PhysicalAggOp (resolved sketch intent) ──────────────────────────────────

/// A resolved physical aggregation operation.
///
/// This is the output of `resolve()`: concrete sketch implementation
/// chosen for a logical [`AggIntent`].
#[derive(Debug, Clone)]
pub struct PhysicalAggOp {
    /// The logical intent this was derived from.
    pub intent: AggIntent,
    /// Concrete sketch type.
    pub sketch_type: SketchType,
    /// Concrete sketch parameters.
    pub sketch_params: SketchParams,
    /// Estimated memory footprint per series (bytes).
    pub estimated_memory_bytes: u64,
}

/// Resolve an [`AggIntent`] into a [`PhysicalAggOp`] using default mapping.
///
/// This is the Layer 3 → Layer 5 boundary.
pub fn resolve(intent: &AggIntent) -> PhysicalAggOp {
    PhysicalAggOp {
        intent: intent.clone(),
        sketch_type: directory::sketch_type_for_op(intent),
        sketch_params: directory::sketch_params_for_op(intent),
        estimated_memory_bytes: directory::estimated_sketch_memory_bytes(intent),
    }
}

// ── Physical operators ──────────────────────────────────────────────────────

/// A physical operator — concrete implementation of a logical operator.
#[derive(Debug, Clone)]
pub enum PhysicalOp {
    // ── Scan / ingest ─────────────────────────────────────────────
    /// Read raw OTLP metrics from an SDK or scrape target.
    OtlpScan {
        endpoint: String,
        label_matchers: Vec<String>,
    },

    /// Read from an existing PromSketch store.
    PromSketchScan {
        store_addr: String,
        series_selector: String,
    },

    // ── Sketch build ──────────────────────────────────────────────
    /// Build sketch via OTel Collector processor (tumbling window flush).
    ///
    /// `aggregate_by` is the GROUP BY key list pushed down from a parent
    /// `Partition` node. Empty = single global sketch. Non-empty = the
    /// processor maintains a `map[groupKey] -> sketch` and flushes one
    /// row per group on each window boundary. Per-group sketch instances
    /// are runtime state; the plan only declares the keys.
    OtelSketchBuild {
        sketch_type: SketchType,
        sketch_params: SketchParams,
        window: PhysicalWindow,
        delta_encoding: bool,
        aggregate_by: Vec<String>,
    },

    /// Build sketch via PromSketch's ExponentialHistogram layer.
    PromSketchBuild {
        sketch_type: SketchType,
        eh_k: usize,
        time_window: Duration,
    },

    // ── Sketch merge ──────────────────────────────────────────────
    /// Merge sketches from N upstream nodes.
    SketchMerge {
        sketch_type: SketchType,
        group_by: Vec<String>,
    },

    // ── Sketch query ──────────────────────────────────────────────
    /// Extract result from a sketch (quantile, cardinality, frequency).
    SketchEval {
        sketch_type: SketchType,
        func: EvalFunc,
    },

    // ── Data exchange ─────────────────────────────────────────────
    /// Data transfer between pipeline stages.
    Exchange {
        format: ExchangeFormat,
    },

    // ── Relational / passthrough ──────────────────────────────────
    /// Filter rows.
    Filter { pred: String },
    /// Top-K ranking.
    TopK { k: u64 },
    /// Hash-partitioned aggregation.
    HashAggregate { keys: Vec<String> },
    /// SQL query to database.
    DbQuery { sql: String },
    /// Passthrough — no transformation.
    Passthrough,
}

/// Physical window implementation.
#[derive(Debug, Clone)]
pub enum PhysicalWindow {
    /// OTel Collector: `time.NewTicker` flush + sketch reset.
    OtelTumblingFlush { duration: Duration },
    /// PromSketch ExponentialHistogram: time-decaying buckets.
    PromSketchEH { eh_k: usize, time_window: Duration },
    /// Database-side: `GROUP BY time_bucket(interval, ts)`.
    SqlTimeBucket { interval: Duration, time_col: String },
    /// No windowing (unbounded / landmark).
    None,
}

/// What to extract from a sketch at query time.
#[derive(Debug, Clone)]
pub enum EvalFunc {
    Quantile(Vec<f64>),
    Cardinality,
    Frequency { key: String },
    TopK { k: u64 },
    Extrema { min: bool, max: bool },
}

/// Data format for Exchange operators.
#[derive(Debug, Clone)]
pub enum ExchangeFormat {
    /// OTLP gRPC / HTTP.
    Otlp,
    /// Sketch-specific binary (merged sketch bytes).
    SketchBinary,
    /// Raw samples (for non-sketch path).
    RawSamples,
}

/// Where a physical operator runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Placement {
    /// Agent OTel Collector (co-located with SDK).
    AgentCollector,
    /// Backend OTel Collector (merge tier).
    BackendCollector,
    /// PromSketch store (ASAPQuery).
    PromSketchStore,
    /// General query engine (ASAPQuery / DataFusion).
    QueryEngine,
    /// Database (ClickHouse, TimescaleDB, etc.).
    Database,
}

// ── Stage capabilities ──────────────────────────────────────────────────────
//
// Every stage is, in principle, capable of running every node in the query
// tree. The Controller's job is to *decide which stage runs which subset* —
// it is NOT to declare ops as inherently belonging to one stage. The only
// hard pins are source-bound ops (`OtlpScan`, `PromSketchScan`,
// `PromSketchBuild`, `DbQuery`), which are tied to the data origin and
// cannot be relocated. Everything else has a cost on every stage, and the
// planner picks the minimum-cost stage subject to budget fit.

/// Per-stage capability + cost surface.
///
/// Each stage implements this trait to declare:
/// - which `PhysicalOp` variants it can run, via `op_cost` returning `Some`
/// - the relative cost weight of running each variant on this stage
/// - whether the stage's resource budget admits the op (`fits`)
///
/// Cost is unitless and used only for ranking; concrete numbers come from
/// the deployment-tuned tables in `sketch_capabilities.yml` over time.
pub trait StageCapabilities: Send + Sync {
    fn placement(&self) -> Placement;

    /// Return `Some(cost)` if this stage can run `op`, `None` if not.
    /// `None` is used for source-bound ops on stages that aren't the source.
    fn op_cost(&self, op: &PhysicalOp) -> Option<f64>;

    /// Whether the stage's resource budget admits this op. Defaults to true;
    /// stages with a sketch-build capability use `StageBudget::fits` against
    /// the resolved `SketchCapability`.
    fn fits(&self, _op: &PhysicalOp, _budget: &crate::algebra::optimizer::StageBudget) -> bool {
        true
    }
}

/// Penalty added to a candidate stage's `op_cost` when its input is on a
/// different stage (i.e. an Exchange must be inserted). Tunable; the value
/// is large enough to dominate small intra-stage cost differences and keep
/// data-locality-sensitive ops (Filter, Project) co-located with their child.
pub const EXCHANGE_COST_PENALTY: f64 = 10.0;

// ── Per-stage capability implementations ────────────────────────────────────

pub struct AgentCollectorCaps;
pub struct BackendCollectorCaps;
pub struct PromSketchStoreCaps;
pub struct QueryEngineCaps;
pub struct DatabaseCaps;

impl StageCapabilities for AgentCollectorCaps {
    fn placement(&self) -> Placement { Placement::AgentCollector }
    fn op_cost(&self, op: &PhysicalOp) -> Option<f64> {
        use PhysicalOp::*;
        Some(match op {
            // Source: this is where OTLP arrives.
            OtlpScan { .. }         => 1.0,
            // Sketch build at the edge is the cheapest option (push-down).
            OtelSketchBuild { .. }  => 1.0,
            // Filter / Project / Passthrough run anywhere; on Agent they're free.
            Filter { .. }           => 1.0,
            Passthrough             => 0.0,
            Exchange { .. }         => 0.0,
            // Backend-or-later ops can in principle run at Agent but are
            // disfavored: the agent is single-host, so it can't merge across
            // agents, can't see global TopK, etc. Express as expensive.
            SketchEval { .. }       => 20.0,
            TopK { .. }              => 20.0,
            HashAggregate { .. }    => 20.0,
            // Source-bound ops on the wrong stage.
            PromSketchScan { .. } | PromSketchBuild { .. } | DbQuery { .. } | SketchMerge { .. } =>
                return None,
        })
    }
    fn fits(&self, op: &PhysicalOp, budget: &crate::algebra::optimizer::StageBudget) -> bool {
        sketch_op_fits(op, budget)
    }
}

impl StageCapabilities for BackendCollectorCaps {
    fn placement(&self) -> Placement { Placement::BackendCollector }
    fn op_cost(&self, op: &PhysicalOp) -> Option<f64> {
        use PhysicalOp::*;
        Some(match op {
            // Sketch build deferred from the edge: workable but more expensive.
            OtelSketchBuild { .. }  => 5.0,
            // Backend collector is the natural place for cross-agent merge
            // and for hash-partitioned aggregation.
            SketchMerge { .. }      => 1.0,
            HashAggregate { .. }    => 1.0,
            Filter { .. }           => 1.0,
            // Final-shaping ops (Eval / TopK) can run here, but should
            // strongly prefer QueryEngine even when input is already at
            // Backend (i.e. cost > QueryEngine.cost + EXCHANGE_COST_PENALTY).
            SketchEval { .. }       => 15.0,
            TopK { .. }              => 15.0,
            Passthrough             => 0.0,
            Exchange { .. }         => 0.0,
            // Source-bound ops are not at Backend.
            OtlpScan { .. } | PromSketchScan { .. } | PromSketchBuild { .. } | DbQuery { .. } =>
                return None,
        })
    }
    fn fits(&self, op: &PhysicalOp, budget: &crate::algebra::optimizer::StageBudget) -> bool {
        sketch_op_fits(op, budget)
    }
}

impl StageCapabilities for PromSketchStoreCaps {
    fn placement(&self) -> Placement { Placement::PromSketchStore }
    fn op_cost(&self, op: &PhysicalOp) -> Option<f64> {
        use PhysicalOp::*;
        Some(match op {
            // PromSketch is the source for its own scans/builds.
            PromSketchScan { .. }   => 1.0,
            PromSketchBuild { .. }  => 1.0,
            // Other ops are possible but uncommon; rank as expensive.
            OtelSketchBuild { .. }  => 10.0,
            SketchMerge { .. }      => 5.0,
            SketchEval { .. }       => 3.0,
            HashAggregate { .. }    => 10.0,
            TopK { .. }              => 10.0,
            Filter { .. }           => 2.0,
            Passthrough             => 0.0,
            Exchange { .. }         => 0.0,
            OtlpScan { .. } | DbQuery { .. } => return None,
        })
    }
}

impl StageCapabilities for QueryEngineCaps {
    fn placement(&self) -> Placement { Placement::QueryEngine }
    fn op_cost(&self, op: &PhysicalOp) -> Option<f64> {
        use PhysicalOp::*;
        Some(match op {
            // Final-stage shaping.
            SketchEval { .. }       => 1.0,
            TopK { .. }              => 1.0,
            // Always-available fallback for sketch build and merge.
            OtelSketchBuild { .. }  => 20.0,
            SketchMerge { .. }      => 5.0,
            HashAggregate { .. }    => 5.0,
            Filter { .. }           => 1.0,
            Passthrough             => 0.0,
            Exchange { .. }         => 0.0,
            // Source-bound ops are pinned elsewhere.
            OtlpScan { .. } | PromSketchScan { .. } | PromSketchBuild { .. } | DbQuery { .. } =>
                return None,
        })
    }
    fn fits(&self, op: &PhysicalOp, budget: &crate::algebra::optimizer::StageBudget) -> bool {
        sketch_op_fits(op, budget)
    }
}

impl StageCapabilities for DatabaseCaps {
    fn placement(&self) -> Placement { Placement::Database }
    fn op_cost(&self, op: &PhysicalOp) -> Option<f64> {
        use PhysicalOp::*;
        Some(match op {
            // DB is the natural place for exact aggregation expressed as SQL.
            DbQuery { .. }          => 1.0,
            // Filter pushdown into the DB is fine.
            Filter { .. }           => 2.0,
            HashAggregate { .. }    => 2.0,
            Passthrough             => 0.0,
            Exchange { .. }         => 0.0,
            // Sketch ops aren't first-class on the DB side.
            OtlpScan { .. } | PromSketchScan { .. } | PromSketchBuild { .. }
            | OtelSketchBuild { .. } | SketchMerge { .. } | SketchEval { .. } | TopK { .. } =>
                return None,
        })
    }
}

/// Budget check for sketch-build ops. Non-sketch ops have no enforced budget yet.
fn sketch_op_fits(op: &PhysicalOp, budget: &crate::algebra::optimizer::StageBudget) -> bool {
    if let PhysicalOp::OtelSketchBuild { sketch_type, .. } = op {
        let cap = crate::algebra::optimizer::sketch_capability(sketch_type);
        return budget.fits(&cap);
    }
    true
}

/// All known stages, ordered by typical preference. Order is not load-bearing
/// since `decide_placement` ranks by cost; it just stabilises tie-breaks.
fn all_stage_caps() -> [&'static dyn StageCapabilities; 5] {
    [
        &AgentCollectorCaps,
        &BackendCollectorCaps,
        &PromSketchStoreCaps,
        &QueryEngineCaps,
        &DatabaseCaps,
    ]
}

impl crate::algebra::optimizer::DeploymentConstraints {
    /// Map a `Placement` to its corresponding `StageBudget`.
    pub fn budget_for(&self, p: &Placement) -> &crate::algebra::optimizer::StageBudget {
        match p {
            Placement::AgentCollector   => &self.agent,
            Placement::BackendCollector => &self.backend_collector,
            // PromSketchStore + QueryEngine share the backend_db budget today.
            Placement::PromSketchStore  => &self.backend_db,
            Placement::QueryEngine      => &self.backend_db,
            Placement::Database         => &self.original_db,
        }
    }
}

/// Decide which stage should run `op`, given its input's placement (if any)
/// and the deployment's per-stage budgets.
///
/// Iterates every known stage, takes those that can run `op` (`op_cost`
/// returns `Some`) and have budget headroom (`fits`), adds an Exchange
/// penalty when `input_placement` is on a different stage, and picks the
/// minimum total. Falls back to `QueryEngine` only if nothing fits — that
/// matches today's behavior where QueryEngine is the always-available
/// stage.
pub fn decide_placement(
    op: &PhysicalOp,
    input_placement: Option<&Placement>,
    constraints: &crate::algebra::optimizer::DeploymentConstraints,
) -> Placement {
    all_stage_caps()
        .iter()
        .filter_map(|caps| {
            let cost = caps.op_cost(op)?;
            let placement = caps.placement();
            if !caps.fits(op, constraints.budget_for(&placement)) {
                return None;
            }
            let exchange_penalty = match input_placement {
                Some(p) if *p != placement => EXCHANGE_COST_PENALTY,
                _ => 0.0,
            };
            Some((placement, cost + exchange_penalty))
        })
        .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
        .map(|(p, _)| p)
        .unwrap_or(Placement::QueryEngine)
}

// ── Physical plan tree ──────────────────────────────────────────────────────

/// A node in the physical plan tree.
#[derive(Debug, Clone)]
pub struct PhysicalNode {
    /// The physical operator at this node.
    pub op: PhysicalOp,
    /// Where this operator runs.
    pub placement: Placement,
    /// Estimated cost.
    pub cost: PhysicalCost,
    /// Child nodes (ordered: left, right, or input list).
    pub children: Vec<PhysicalNode>,
}

/// Cost estimate for a physical operator.
#[derive(Debug, Clone, Default)]
pub struct PhysicalCost {
    /// Estimated output bandwidth (bytes/sec).
    pub bytes_per_sec: f64,
    /// Estimated memory usage (bytes).
    pub memory_bytes: f64,
    /// Estimated CPU cost (microseconds per sample).
    pub cpu_per_sample: f64,
}

// ── Window resolution ───────────────────────────────────────────────────────

/// Resolve a logical [`WindowSpec`] to a [`PhysicalWindow`] for a given placement.
pub fn resolve_window(window: &WindowSpec, placement: &Placement) -> PhysicalWindow {
    match (&window.kind, placement) {
        (WindowKind::Tumbling { size }, Placement::AgentCollector) =>
            PhysicalWindow::OtelTumblingFlush { duration: *size },
        (WindowKind::Tumbling { size }, Placement::PromSketchStore) =>
            PhysicalWindow::PromSketchEH { eh_k: 50, time_window: *size },
        (WindowKind::Sliding { size, .. }, Placement::PromSketchStore) =>
            PhysicalWindow::PromSketchEH { eh_k: 50, time_window: *size },
        (WindowKind::Tumbling { size }, Placement::Database) =>
            PhysicalWindow::SqlTimeBucket {
                interval: *size,
                time_col: window.time_col.clone().unwrap_or_else(|| "ts".into()),
            },
        (WindowKind::Unbounded | WindowKind::Landmark, _) =>
            PhysicalWindow::None,
        // Fallback: tumbling at the given size for any other combo.
        (WindowKind::Tumbling { size } | WindowKind::Sliding { size, .. } | WindowKind::Session { gap: size }, _) =>
            PhysicalWindow::OtelTumblingFlush { duration: *size },
    }
}

// ── Physical planner ────────────────────────────────────────────────────────

use crate::algebra::expr::*;
use crate::algebra::optimizer::DeploymentConstraints;
use crate::types::StageResourceBudgets;

/// Physical planner configuration.
#[derive(Debug, Clone)]
pub struct PhysicalPlannerConfig {
    pub budgets: StageResourceBudgets,
    pub constraints: DeploymentConstraints,
}

/// Build a physical plan from an optimized `QueryExpr`.
///
/// Walks the logical tree bottom-up, assigning each node to a pipeline stage
/// (`Placement`), resolving sketch intents to concrete implementations, and
/// inserting `Exchange` nodes at stage boundaries.
pub fn plan(expr: &QueryExpr, config: &PhysicalPlannerConfig) -> PhysicalNode {
    plan_node(expr, config)
}

fn plan_node(expr: &QueryExpr, config: &PhysicalPlannerConfig) -> PhysicalNode {
    // Helper: build a single-child PhysicalNode for the given op, decide its
    // placement via the capability-based cost model (using the child's
    // placement as the locality reference), and insert an Exchange if the
    // child ends up on a different stage.
    fn build_unary(
        op: PhysicalOp,
        child: PhysicalNode,
        cost: PhysicalCost,
        config: &PhysicalPlannerConfig,
    ) -> PhysicalNode {
        let placement = decide_placement(&op, Some(&child.placement), &config.constraints);
        let mut node = PhysicalNode { op, placement, cost, children: vec![child] };
        insert_exchange_if_needed(&mut node);
        node
    }

    match expr {
        // ── Leaf: OtlpScan is source-pinned to Agent via cost model ─
        QueryExpr::Source(_) => {
            let op = PhysicalOp::OtlpScan {
                endpoint: String::new(),
                label_matchers: vec![],
            };
            let placement = decide_placement(&op, None, &config.constraints);
            PhysicalNode { op, placement, cost: PhysicalCost::default(), children: vec![] }
        }

        // ── Filter: cost model co-locates with child via Exchange penalty ──
        QueryExpr::Filter { pred, input } => {
            let child = plan_node(input, config);
            build_unary(
                PhysicalOp::Filter { pred: format!("{pred:?}") },
                child,
                PhysicalCost::default(),
                config,
            )
        }

        // ── SketchAgg: build OtelSketchBuild; cost model picks stage ──
        QueryExpr::SketchAgg { op, col: _, input } => {
            let child = plan_node(input, config);
            let resolved = resolve(op);
            let physical_op = PhysicalOp::OtelSketchBuild {
                sketch_type: resolved.sketch_type.clone(),
                sketch_params: resolved.sketch_params.clone(),
                window: PhysicalWindow::None,
                delta_encoding: false,
                aggregate_by: vec![],
            };
            build_unary(
                physical_op,
                child,
                PhysicalCost {
                    memory_bytes: resolved.estimated_memory_bytes as f64,
                    ..Default::default()
                },
                config,
            )
        }

        // ── WindowedAgg: OtelSketchBuild with a window resolved ─────
        // Window resolution depends on placement (different stages have
        // different window implementations), so we ask the cost model
        // first using a placeholder window, then re-build with the
        // resolved window.
        QueryExpr::WindowedAgg { agg, window, col: _, input } => {
            let child = plan_node(input, config);
            let resolved = resolve(agg);
            let placeholder = PhysicalOp::OtelSketchBuild {
                sketch_type: resolved.sketch_type.clone(),
                sketch_params: resolved.sketch_params.clone(),
                window: PhysicalWindow::None,
                delta_encoding: false,
                aggregate_by: vec![],
            };
            let placement = decide_placement(&placeholder, Some(&child.placement), &config.constraints);
            let phys_window = resolve_window(window, &placement);
            let physical_op = PhysicalOp::OtelSketchBuild {
                sketch_type: resolved.sketch_type.clone(),
                sketch_params: resolved.sketch_params.clone(),
                window: phys_window,
                delta_encoding: false,
                aggregate_by: vec![],
            };
            let mut node = PhysicalNode {
                op: physical_op,
                placement,
                cost: PhysicalCost {
                    memory_bytes: resolved.estimated_memory_bytes as f64,
                    ..Default::default()
                },
                children: vec![child],
            };
            insert_exchange_if_needed(&mut node);
            node
        }

        // ── Partition: fold keys into upstream OtelSketchBuild if present ──
        QueryExpr::Partition { keys, input } => {
            let mut child = plan_node(input, config);
            let key_list = keys.keys().to_vec();

            // If the planned input root is OtelSketchBuild, push keys down
            // into the sketch processor so it builds per-group sketches at
            // its placement (Agent in the common case). Wrap with SketchMerge
            // so the multi-agent merge step is preserved; the cost model
            // picks where the merge runs.
            let parent_op = match &mut child.op {
                PhysicalOp::OtelSketchBuild { aggregate_by, sketch_type, .. } => {
                    *aggregate_by = key_list.clone();
                    PhysicalOp::SketchMerge {
                        sketch_type: sketch_type.clone(),
                        group_by: key_list,
                    }
                }
                // Fallback: non-sketch input — hash-partition.
                _ => PhysicalOp::HashAggregate { keys: key_list },
            };
            build_unary(parent_op, child, PhysicalCost::default(), config)
        }

        QueryExpr::Merge { inputs } => {
            let children: Vec<PhysicalNode> = inputs.iter()
                .map(|i| plan_node(i, config))
                .collect();
            let sketch_type = children.first()
                .and_then(|c| match &c.op {
                    PhysicalOp::OtelSketchBuild { sketch_type, .. } => Some(sketch_type.clone()),
                    _ => None,
                })
                .unwrap_or(SketchType::DDSketch);
            let op = PhysicalOp::SketchMerge { sketch_type, group_by: vec![] };
            // Use first child's placement as locality reference; if children
            // disagree, the cross-stage Exchanges are inserted below.
            let input_placement = children.first().map(|c| c.placement.clone());
            let placement = decide_placement(&op, input_placement.as_ref(), &config.constraints);
            let mut node = PhysicalNode {
                op,
                placement,
                cost: PhysicalCost::default(),
                children,
            };
            insert_exchange_if_needed(&mut node);
            node
        }

        QueryExpr::Dedup { col, input } => {
            let child = plan_node(input, config);
            build_unary(
                PhysicalOp::Filter { pred: format!("dedup({col})") },
                child,
                PhysicalCost::default(),
                config,
            )
        }

        QueryExpr::TopK { k, input, .. } => {
            let child = plan_node(input, config);
            build_unary(
                PhysicalOp::TopK { k: *k },
                child,
                PhysicalCost::default(),
                config,
            )
        }

        QueryExpr::HistogramQuantile { phi, input } => {
            let child = plan_node(input, config);
            build_unary(
                PhysicalOp::SketchEval {
                    sketch_type: SketchType::DDSketch,
                    func: EvalFunc::Quantile(vec![*phi]),
                },
                child,
                PhysicalCost::default(),
                config,
            )
        }

        QueryExpr::BinaryOp { op: _, lhs, rhs, .. } => {
            let left = plan_node(lhs, config);
            let right = plan_node(rhs, config);
            let physical_op = PhysicalOp::Passthrough;
            let placement = decide_placement(&physical_op, Some(&left.placement), &config.constraints);
            let mut node = PhysicalNode {
                op: physical_op,
                placement,
                cost: PhysicalCost::default(),
                children: vec![left, right],
            };
            insert_exchange_if_needed(&mut node);
            node
        }

        QueryExpr::PromQLSubquery { input, .. } => {
            let child = plan_node(input, config);
            build_unary(PhysicalOp::Passthrough, child, PhysicalCost::default(), config)
        }

        // ── Aggregate (non-sketch, exact): emitted as a DbQuery, which
        // is source-bound to Database via the cost model. ──────────
        QueryExpr::Aggregate { keys, input, .. } => {
            let child = plan_node(input, config);
            build_unary(
                PhysicalOp::DbQuery { sql: format!("GROUP BY {:?}", keys) },
                child,
                PhysicalCost::default(),
                config,
            )
        }

        // ── Sort / Limit / Project / Window / WindowFunc: passthrough.
        // Cost model co-locates with child via Exchange penalty. ───
        QueryExpr::Sort { input, .. }
        | QueryExpr::Limit { input, .. }
        | QueryExpr::Project { input, .. }
        | QueryExpr::Window { input, .. }
        | QueryExpr::WindowFunc { input, .. } => {
            let child = plan_node(input, config);
            build_unary(PhysicalOp::Passthrough, child, PhysicalCost::default(), config)
        }

        // ── Join / SetOp: passthrough wrapper; cost model picks stage. ──
        QueryExpr::Join { left, right, .. }
        | QueryExpr::JoinSketch { outer: left, inner: right, .. }
        | QueryExpr::SetOp { left, right, .. } => {
            let l = plan_node(left, config);
            let r = plan_node(right, config);
            let op = PhysicalOp::Passthrough;
            let placement = decide_placement(&op, Some(&l.placement), &config.constraints);
            let mut node = PhysicalNode {
                op,
                placement,
                cost: PhysicalCost::default(),
                children: vec![l, r],
            };
            insert_exchange_if_needed(&mut node);
            node
        }

        // ── Subquery / LetBinding ───────────────────────────────────
        QueryExpr::Subquery { expr, .. } => plan_node(expr, config),
        QueryExpr::LetBinding { body, .. } => plan_node(body, config),
        QueryExpr::Ref(_) => {
            let op = PhysicalOp::Passthrough;
            let placement = decide_placement(&op, None, &config.constraints);
            PhysicalNode { op, placement, cost: PhysicalCost::default(), children: vec![] }
        }
    }
}

/// If a node's child is at a different stage, insert an Exchange node between them.
fn insert_exchange_if_needed(node: &mut PhysicalNode) {
    let parent_placement = node.placement.clone();
    for child in &mut node.children {
        if child.placement != parent_placement {
            let format = match (&child.placement, &parent_placement) {
                (Placement::AgentCollector, Placement::BackendCollector) => ExchangeFormat::Otlp,
                (Placement::AgentCollector, Placement::QueryEngine) => ExchangeFormat::Otlp,
                (Placement::BackendCollector, Placement::QueryEngine) => ExchangeFormat::SketchBinary,
                (Placement::AgentCollector, Placement::Database) => ExchangeFormat::RawSamples,
                _ => ExchangeFormat::Otlp,
            };
            // Wrap the child in an Exchange node
            let original_child = std::mem::replace(child, PhysicalNode {
                op: PhysicalOp::Passthrough,
                placement: parent_placement.clone(),
                cost: PhysicalCost::default(),
                children: vec![],
            });
            *child = PhysicalNode {
                op: PhysicalOp::Exchange { format },
                placement: parent_placement.clone(),
                cost: PhysicalCost::default(),
                children: vec![original_child],
            };
        }
    }
}

impl PhysicalNode {
    /// Count total nodes in the tree.
    pub fn node_count(&self) -> usize {
        1 + self.children.iter().map(|c| c.node_count()).sum::<usize>()
    }

    /// Collect all distinct placements in the tree.
    pub fn placements(&self) -> Vec<Placement> {
        let mut out = vec![self.placement.clone()];
        for child in &self.children {
            for p in child.placements() {
                if !out.contains(&p) {
                    out.push(p);
                }
            }
        }
        out
    }

    /// Count Exchange nodes (= stage boundary crossings).
    pub fn exchange_count(&self) -> usize {
        let self_count = if matches!(self.op, PhysicalOp::Exchange { .. }) { 1 } else { 0 };
        self_count + self.children.iter().map(|c| c.exchange_count()).sum::<usize>()
    }

    /// Extract a flat [`StagedPlan`] from this physical plan tree.
    ///
    /// Walks the tree and populates each sub-plan based on node placement
    /// and operator type.  This bridges the physical planner to the existing
    /// config generators that consume `StagedPlan`.
    pub fn to_staged_plan(&self) -> crate::types::StagedPlan {
        use crate::types::{
            AgentSubPlan, BackendSubPlan, DbSubPlan, PrecomputeSubPlan, StagedPlan,
        };

        let mut staged = StagedPlan::default();
        self.collect_into_staged(&mut staged);
        staged
    }

    fn collect_into_staged(&self, staged: &mut crate::types::StagedPlan) {
        use crate::types::StagedPlan;

        match (&self.placement, &self.op) {
            // Agent: sketch build → populate agent sub-plan
            (Placement::AgentCollector, PhysicalOp::OtelSketchBuild {
                sketch_type, sketch_params, window, aggregate_by, ..
            }) => {
                staged.agent.sketch_type = Some(sketch_type.clone());
                staged.agent.sketch_params = sketch_params.clone();
                if let PhysicalWindow::OtelTumblingFlush { duration } = window {
                    staged.agent.window_secs = Some(duration.as_secs());
                }
                if !aggregate_by.is_empty() {
                    staged.agent.aggregate_by = aggregate_by.clone();
                }
            }

            // Agent: filter → label filters
            (Placement::AgentCollector, PhysicalOp::Filter { pred }) => {
                staged.agent.label_filters.push(pred.clone());
            }

            // Backend: sketch build deferred from Agent due to budget. The
            // processor here builds per-key sketches itself.
            (Placement::BackendCollector, PhysicalOp::OtelSketchBuild {
                aggregate_by, ..
            }) => {
                if !aggregate_by.is_empty() {
                    staged.backend.group_by = aggregate_by.clone();
                }
            }

            // Backend: merge/aggregate
            (Placement::BackendCollector, PhysicalOp::SketchMerge { group_by, .. }) => {
                staged.backend.has_merge = true;
                staged.backend.group_by = group_by.clone();
            }
            (Placement::BackendCollector, PhysicalOp::HashAggregate { keys }) => {
                staged.backend.has_merge = true;
                staged.backend.group_by = keys.clone();
            }
            (Placement::BackendCollector, PhysicalOp::Filter { .. }) => {
                staged.backend.has_dedup = true;
            }

            // QueryEngine: TopK, SketchEval
            (Placement::QueryEngine, PhysicalOp::TopK { k }) => {
                staged.precompute.active = true;
                staged.precompute.topk = Some(*k);
            }
            (Placement::QueryEngine, PhysicalOp::SketchEval { .. }) => {
                staged.precompute.active = true;
            }
            (Placement::QueryEngine, PhysicalOp::Passthrough) => {
                staged.precompute.active = true;
            }

            // Database: exact computation
            (Placement::Database, PhysicalOp::DbQuery { sql }) => {
                staged.db.active = true;
                staged.db.query_expr = sql.clone();
            }

            // Exchange: record deferral
            (_, PhysicalOp::Exchange { format }) => {
                staged.deferral_log.push(format!("Exchange({:?})", format));
            }

            _ => {}
        }

        // Recurse into children
        for child in &self.children {
            child.collect_into_staged(staged);
        }
    }
}

// ── Public entry point for main.rs ──────────────────────────────────────────

/// Run the full physical planning pipeline: optimize → plan → staged plan.
///
/// This is the single function `main.rs` calls to get a `StagedPlan`
/// from a parsed `QueryExpr`.
pub fn physical_plan_to_staged(
    expr: &QueryExpr,
    budgets: &StageResourceBudgets,
) -> (crate::types::StagedPlan, PhysicalNode) {
    let constraints = DeploymentConstraints::from_budgets(budgets);
    let config = PhysicalPlannerConfig {
        budgets: budgets.clone(),
        constraints,
    };
    let tree = plan(expr, &config);
    let staged = tree.to_staged_plan();
    (staged, tree)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_quantile() {
        let p = resolve(&AggIntent::default_quantile(vec![0.99]));
        assert_eq!(p.sketch_type, SketchType::DDSketch);
        assert!(matches!(p.sketch_params, SketchParams::DDSketch { .. }));
        assert!(p.estimated_memory_bytes > 0);
    }

    #[test]
    fn resolve_cardinality() {
        let p = resolve(&AggIntent::default_cardinality());
        assert_eq!(p.sketch_type, SketchType::HLL);
        assert!(matches!(p.sketch_params, SketchParams::HLL { .. }));
    }

    #[test]
    fn resolve_frequency() {
        let p = resolve(&AggIntent::default_frequency());
        assert_eq!(p.sketch_type, SketchType::CountSketch);
        assert!(matches!(p.sketch_params, SketchParams::CountSketch { .. }));
    }

    #[test]
    fn resolve_preserves_intent() {
        let intent = AggIntent::Quantile { quantiles: vec![0.5, 0.99], accuracy: 0.005 };
        let p = resolve(&intent);
        assert_eq!(p.intent, intent);
    }

    #[test]
    fn tumbling_window_at_agent() {
        let ws = WindowSpec {
            kind: WindowKind::Tumbling { size: Duration::from_secs(300) },
            time_col: None,
        };
        let pw = resolve_window(&ws, &Placement::AgentCollector);
        assert!(matches!(pw, PhysicalWindow::OtelTumblingFlush { .. }));
    }

    #[test]
    fn sliding_window_at_promsketch() {
        let ws = WindowSpec {
            kind: WindowKind::Sliding { size: Duration::from_secs(300), slide: Duration::from_secs(60) },
            time_col: None,
        };
        let pw = resolve_window(&ws, &Placement::PromSketchStore);
        assert!(matches!(pw, PhysicalWindow::PromSketchEH { .. }));
    }

    #[test]
    fn tumbling_window_at_database() {
        let ws = WindowSpec {
            kind: WindowKind::Tumbling { size: Duration::from_secs(60) },
            time_col: Some("event_time".into()),
        };
        let pw = resolve_window(&ws, &Placement::Database);
        match pw {
            PhysicalWindow::SqlTimeBucket { interval, time_col } => {
                assert_eq!(interval, Duration::from_secs(60));
                assert_eq!(time_col, "event_time");
            }
            other => panic!("expected SqlTimeBucket, got {other:?}"),
        }
    }

    #[test]
    fn unbounded_window_is_none() {
        let ws = WindowSpec { kind: WindowKind::Unbounded, time_col: None };
        let pw = resolve_window(&ws, &Placement::AgentCollector);
        assert!(matches!(pw, PhysicalWindow::None));
    }

    // ── Physical planner tests ──────────────────────────────────────────

    fn default_config() -> PhysicalPlannerConfig {
        PhysicalPlannerConfig {
            budgets: StageResourceBudgets::default(),
            constraints: DeploymentConstraints::default(),
        }
    }

    fn src(name: &str) -> QueryExpr {
        QueryExpr::Source(SourceSpec { name: name.into() })
    }

    #[test]
    fn plan_simple_sketch_at_agent() {
        // SketchAgg { Quantile, Source } → Agent placement
        let expr = QueryExpr::SketchAgg {
            op: AggIntent::default_quantile(vec![0.99]),
            col: ColumnRef::SampleValue,
            input: Box::new(src("m")),
        };
        let node = plan(&expr, &default_config());
        assert_eq!(node.placement, Placement::AgentCollector);
        assert!(matches!(node.op, PhysicalOp::OtelSketchBuild { .. }));
        assert_eq!(node.children.len(), 1); // Source child
    }

    #[test]
    fn plan_windowed_agg_has_window() {
        let expr = QueryExpr::WindowedAgg {
            agg: AggIntent::default_quantile(vec![0.5]),
            window: WindowSpec { kind: WindowKind::Tumbling { size: Duration::from_secs(300) }, time_col: None },
            col: ColumnRef::SampleValue,
            input: Box::new(src("m")),
        };
        let node = plan(&expr, &default_config());
        assert_eq!(node.placement, Placement::AgentCollector);
        match &node.op {
            PhysicalOp::OtelSketchBuild { window, .. } => {
                assert!(matches!(window, PhysicalWindow::OtelTumblingFlush { .. }));
            }
            other => panic!("expected OtelSketchBuild, got {other:?}"),
        }
    }

    #[test]
    fn plan_topk_at_query_engine() {
        let expr = QueryExpr::TopK {
            k: 10,
            by: vec!["svc".into()],
            input: Box::new(QueryExpr::SketchAgg {
                op: AggIntent::default_frequency(),
                col: ColumnRef::SampleValue,
                input: Box::new(src("m")),
            }),
        };
        let node = plan(&expr, &default_config());
        assert_eq!(node.placement, Placement::QueryEngine);
        assert!(matches!(node.op, PhysicalOp::TopK { k: 10 }));
    }

    #[test]
    fn plan_topk_inserts_exchange() {
        // TopK(QueryEngine) wrapping SketchAgg(Agent) → Exchange between them
        let expr = QueryExpr::TopK {
            k: 5,
            by: vec![],
            input: Box::new(QueryExpr::SketchAgg {
                op: AggIntent::default_frequency(),
                col: ColumnRef::SampleValue,
                input: Box::new(src("m")),
            }),
        };
        let node = plan(&expr, &default_config());
        assert!(node.exchange_count() > 0, "expected Exchange between Agent and QueryEngine");
    }

    #[test]
    fn plan_partition_at_backend() {
        let expr = QueryExpr::Partition {
            keys: PartitionKeys::By(vec!["region".into()]),
            input: Box::new(QueryExpr::SketchAgg {
                op: AggIntent::default_cardinality(),
                col: ColumnRef::SampleValue,
                input: Box::new(src("m")),
            }),
        };
        let node = plan(&expr, &default_config());
        assert_eq!(node.placement, Placement::BackendCollector);
    }

    #[test]
    fn plan_partition_folds_keys_into_sketch() {
        // Partition { region } over SketchAgg(Cardinality) collapses to
        // SketchMerge @ Backend wrapping OtelSketchBuild { aggregate_by: ["region"] } @ Agent.
        let expr = QueryExpr::Partition {
            keys: PartitionKeys::By(vec!["region".into()]),
            input: Box::new(QueryExpr::SketchAgg {
                op: AggIntent::default_cardinality(),
                col: ColumnRef::SampleValue,
                input: Box::new(src("m")),
            }),
        };
        let node = plan(&expr, &default_config());

        assert_eq!(node.placement, Placement::BackendCollector);
        match &node.op {
            PhysicalOp::SketchMerge { sketch_type, group_by } => {
                assert_eq!(*sketch_type, SketchType::HLL);
                assert_eq!(group_by, &vec!["region".to_string()]);
            }
            other => panic!("expected SketchMerge, got {other:?}"),
        }

        fn find_sketch_build(n: &PhysicalNode) -> Option<&PhysicalOp> {
            if matches!(n.op, PhysicalOp::OtelSketchBuild { .. }) {
                return Some(&n.op);
            }
            for c in &n.children {
                if let Some(op) = find_sketch_build(c) {
                    return Some(op);
                }
            }
            None
        }
        match find_sketch_build(&node) {
            Some(PhysicalOp::OtelSketchBuild { aggregate_by, sketch_type, .. }) => {
                assert_eq!(*sketch_type, SketchType::HLL);
                assert_eq!(aggregate_by, &vec!["region".to_string()],
                    "partition keys should be folded into agent-side sketch");
            }
            other => panic!("expected OtelSketchBuild somewhere in tree, got {other:?}"),
        }

        let staged = node.to_staged_plan();
        assert_eq!(staged.agent.aggregate_by, vec!["region".to_string()]);
        assert_eq!(staged.backend.group_by, vec!["region".to_string()]);
        assert!(staged.backend.has_merge);
    }

    #[test]
    fn plan_partition_without_sketch_falls_back_to_hashaggregate() {
        let expr = QueryExpr::Partition {
            keys: PartitionKeys::By(vec!["svc".into()]),
            input: Box::new(QueryExpr::Filter {
                pred: ScalarExpr::Literal(LiteralValue::Bool(true)),
                input: Box::new(src("m")),
            }),
        };
        let node = plan(&expr, &default_config());
        assert_eq!(node.placement, Placement::BackendCollector);
        assert!(matches!(node.op, PhysicalOp::HashAggregate { .. }));
    }

    // ── Capability-driven placement ─────────────────────────────────────

    #[test]
    fn capability_op_costs_pick_natural_stages() {
        // Sanity: each "natural" host stage offers the cheapest cost for
        // its op. This is what makes the planner reproduce the old
        // hardcoded preferences purely via cost-ranking.
        use PhysicalOp::*;
        let topk = TopK { k: 10 };
        assert!(QueryEngineCaps.op_cost(&topk).unwrap() < BackendCollectorCaps.op_cost(&topk).unwrap());
        assert!(QueryEngineCaps.op_cost(&topk).unwrap() < AgentCollectorCaps.op_cost(&topk).unwrap());

        let merge = SketchMerge { sketch_type: SketchType::HLL, group_by: vec![] };
        assert!(BackendCollectorCaps.op_cost(&merge).unwrap() < QueryEngineCaps.op_cost(&merge).unwrap());

        let build = OtelSketchBuild {
            sketch_type: SketchType::HLL,
            sketch_params: SketchParams::HLL { precision: 14 },
            window: PhysicalWindow::None,
            delta_encoding: false,
            aggregate_by: vec![],
        };
        assert!(AgentCollectorCaps.op_cost(&build).unwrap() < BackendCollectorCaps.op_cost(&build).unwrap());

        // Source pinning: PromSketchScan refuses to run anywhere except
        // PromSketchStore, even though every other stage is asked.
        let scan = PromSketchScan { store_addr: String::new(), series_selector: String::new() };
        assert!(AgentCollectorCaps.op_cost(&scan).is_none());
        assert!(BackendCollectorCaps.op_cost(&scan).is_none());
        assert!(QueryEngineCaps.op_cost(&scan).is_none());
        assert!(DatabaseCaps.op_cost(&scan).is_none());
        assert!(PromSketchStoreCaps.op_cost(&scan).is_some());
    }

    #[test]
    fn capability_filter_co_locates_with_child_via_exchange_penalty() {
        // Filter has the same op_cost (1.0) on Agent / Backend / QueryEngine,
        // but the Exchange penalty makes the planner co-locate it with
        // the child to avoid a wasted stage hop. Child is at Agent
        // (OtlpScan), so Filter should also land at Agent.
        let expr = QueryExpr::Filter {
            pred: ScalarExpr::Literal(LiteralValue::Bool(true)),
            input: Box::new(src("m")),
        };
        let node = plan(&expr, &default_config());
        assert_eq!(node.placement, Placement::AgentCollector,
            "Filter should co-locate with its child (Agent) via Exchange penalty, not jump stages");
    }

    #[test]
    fn capability_budget_failure_migrates_op() {
        // With a tiny Agent memory budget, the sketch can't fit at Agent.
        // The cost model must skip Agent and pick Backend (the next
        // cheapest stage where it fits). This is the same behavior the
        // old `decide_sketch_placement` had — but now expressed as a
        // single `fits` check inside the unified placement function.
        let budgets = StageResourceBudgets {
            agent_memory_bytes: Some(1),
            ..Default::default()
        };
        let config = PhysicalPlannerConfig {
            constraints: DeploymentConstraints::from_budgets(&budgets),
            budgets,
        };
        let expr = QueryExpr::SketchAgg {
            op: AggIntent::default_quantile(vec![0.99]),
            col: ColumnRef::SampleValue,
            input: Box::new(src("m")),
        };
        let node = plan(&expr, &config);
        assert_eq!(node.placement, Placement::BackendCollector,
            "sketch should migrate to Backend when Agent doesn't fit");
    }

    #[test]
    fn plan_aggregate_at_database() {
        let expr = QueryExpr::Aggregate {
            keys: vec!["symbol".into()],
            aggs: vec![AggItem {
                alias: "avg".into(),
                func: AggFunc::Avg,
                col: ColumnRef::Named("price".into()),
                distinct: false,
            }],
            having: None,
            input: Box::new(src("trades")),
        };
        let node = plan(&expr, &default_config());
        assert_eq!(node.placement, Placement::Database);
    }

    #[test]
    fn plan_full_pipeline_has_multiple_stages() {
        // TopK(Partition(WindowedAgg(Filter(Source))))
        // Should span: Agent → Backend → QueryEngine
        let expr = QueryExpr::TopK {
            k: 10,
            by: vec!["svc".into()],
            input: Box::new(QueryExpr::Partition {
                keys: PartitionKeys::By(vec!["svc".into()]),
                input: Box::new(QueryExpr::WindowedAgg {
                    agg: AggIntent::default_frequency(),
                    window: WindowSpec { kind: WindowKind::Tumbling { size: Duration::from_secs(60) }, time_col: None },
                    col: ColumnRef::SampleValue,
                    input: Box::new(QueryExpr::Filter {
                        pred: ScalarExpr::Literal(LiteralValue::Bool(true)),
                        input: Box::new(src("requests")),
                    }),
                }),
            }),
        };
        let node = plan(&expr, &default_config());
        let placements = node.placements();
        assert!(placements.contains(&Placement::AgentCollector), "should have Agent: {placements:?}");
        assert!(placements.contains(&Placement::BackendCollector), "should have Backend: {placements:?}");
        assert!(placements.contains(&Placement::QueryEngine), "should have QueryEngine: {placements:?}");
        assert!(node.exchange_count() >= 2, "should have ≥2 exchanges: {}", node.exchange_count());
    }

    #[test]
    fn plan_budget_deferral() {
        // With tiny agent budget, sketch should defer to Backend
        let budgets = StageResourceBudgets {
            agent_memory_bytes: Some(1), // 1 byte = too small
            ..Default::default()
        };
        let config = PhysicalPlannerConfig {
            constraints: DeploymentConstraints::from_budgets(&budgets),
            budgets,
        };
        let expr = QueryExpr::SketchAgg {
            op: AggIntent::default_quantile(vec![0.99]),
            col: ColumnRef::SampleValue,
            input: Box::new(src("m")),
        };
        let node = plan(&expr, &config);
        assert_eq!(node.placement, Placement::BackendCollector,
            "sketch should be deferred to Backend when agent budget is tiny");
    }

    // ── to_staged_plan tests ────────────────────────────────────────────

    #[test]
    fn staged_plan_simple_sketch() {
        let expr = QueryExpr::WindowedAgg {
            agg: AggIntent::Quantile { quantiles: vec![0.99], accuracy: 0.01 },
            window: WindowSpec { kind: WindowKind::Tumbling { size: Duration::from_secs(300) }, time_col: None },
            col: ColumnRef::SampleValue,
            input: Box::new(src("m")),
        };
        let (staged, _) = physical_plan_to_staged(&expr, &StageResourceBudgets::default());
        assert_eq!(staged.agent.sketch_type, Some(SketchType::DDSketch));
        assert_eq!(staged.agent.window_secs, Some(300));
        assert!(!staged.precompute.active);
        assert!(!staged.db.active);
    }

    #[test]
    fn staged_plan_topk_multi_stage() {
        let expr = QueryExpr::TopK {
            k: 10,
            by: vec!["svc".into()],
            input: Box::new(QueryExpr::Partition {
                keys: PartitionKeys::By(vec!["svc".into()]),
                input: Box::new(QueryExpr::WindowedAgg {
                    agg: AggIntent::default_frequency(),
                    window: WindowSpec { kind: WindowKind::Tumbling { size: Duration::from_secs(60) }, time_col: None },
                    col: ColumnRef::SampleValue,
                    input: Box::new(src("m")),
                }),
            }),
        };
        let (staged, tree) = physical_plan_to_staged(&expr, &StageResourceBudgets::default());
        // Agent has sketch
        assert!(staged.agent.sketch_type.is_some());
        // Backend has merge
        assert!(staged.backend.has_merge);
        // Precompute has topk
        assert!(staged.precompute.active);
        assert_eq!(staged.precompute.topk, Some(10));
        // Exchanges recorded in deferral log
        assert!(tree.exchange_count() >= 2);
    }

    #[test]
    fn staged_plan_exact_agg_at_db() {
        let expr = QueryExpr::Aggregate {
            keys: vec!["symbol".into()],
            aggs: vec![AggItem {
                alias: "avg".into(),
                func: AggFunc::Avg,
                col: ColumnRef::Named("price".into()),
                distinct: false,
            }],
            having: None,
            input: Box::new(src("trades")),
        };
        let (staged, _) = physical_plan_to_staged(&expr, &StageResourceBudgets::default());
        assert!(staged.db.active);
        assert!(staged.agent.sketch_type.is_none());
    }
}
