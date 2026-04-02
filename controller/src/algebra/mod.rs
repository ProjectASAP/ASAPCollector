//! General query algebra — full SQL/PromQL AST, cost-based optimizer,
//! and sketch-stage allocator.
//!
//! # Module layout
//!
//! | Module | Contents |
//! |--------|----------|
//! | [`expr`] | [`QueryExpr`] + [`ScalarExpr`] — the complete relational+scalar algebra |
//! | [`plan`] | [`PlanNode`], [`PipelineStage`], [`ExecutionMode`], [`CostEstimate`] |
//! | [`optimizer`] | [`QueryOptimizer`] + 12 rewrite rules |
//! | [`allocator`] | [`SketchAllocator`] — assigns stages and sketch types |
//!
//! # Typical usage
//!
//! ```rust,ignore
//! use controller::algebra::{
//!     expr::QueryExpr,
//!     optimizer::QueryOptimizer,
//!     allocator::SketchAllocator,
//! };
//! use controller::query_parser;
//! use controller::types::StageResourceBudgets;
//!
//! // 1. Parse a PromQL / SQL query string into QueryExpr.
//! let query_expr = query_parser::parse_query_expr("quantile_over_time(0.99, latency[5m])")?;
//!
//! // 2. Optimise (cost-based fixed-point rewriting).
//! let (opt_expr, _iters) = QueryOptimizer::new(raw_bps).optimize(query_expr);
//!
//! // 4. Allocate stages.
//! let budgets   = StageResourceBudgets::from_workload_chars(&workload_chars);
//! let plan_root = SketchAllocator::new(budgets, raw_bps).allocate(opt_expr);
//!
//! // 5. Inspect or serialise.
//! let summary = plan_root.summarise(raw_bps);
//! ```

pub mod allocator;
pub mod directory;
pub mod expr;
pub mod optimizer;
pub mod plan;

// Convenience re-exports.
pub use allocator::SketchAllocator;
pub use expr::{AggFunc, AggIntent, BinaryOpKind, QueryExpr, ScalarExpr, WindowKind, WindowSpec};
pub use optimizer::QueryOptimizer;
pub use plan::{CostEstimate, ExecutionMode, PipelineStage, PlanNode, PlanSummary};
