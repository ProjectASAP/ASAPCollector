//! Layer 3 IR — `core::intent_algebra` per `controller/docs/design.md` §6.
//!
//! Phase B introduces the L3 vocabulary the planner pivots on:
//!
//! - [`AggIntent`] — what to compute, not how (no sketch types here;
//!   sketch binding is L4).
//! - [`QueryExpr`] — the L3 algebra DAG (intent-only, language-orthogonal,
//!   deployment-independent). Single-rooted per query; multi-root
//!   workload-level CSE lives one layer up in `types_v2::WorkloadPlan`.
//! - [`Schema`] — typed schema flowing on every L3 edge. `unique_keys` is
//!   the load-bearing field for CSE legality (`design.md` §6 line ~1284).
//! - [`lower_parsed_query`] — `query_parser::ParsedQuery` → [`QueryExpr`]
//!   single-query lowering.
//!
//! Phase F adds the CSE surface that consumes `Schema::unique_keys`:
//!
//! - [`cse_reuse_is_legal`] — gatekeeper. Two `QueryExpr::Ref` consumers
//!   may share a `LetBinding` only when the producer's output schema
//!   has at least one `unique_keys` set. This is the proof point that
//!   `unique_keys` is load-bearing.
//! - [`dedupe_subtrees`] — basic workload-level CSE pass that hoists
//!   structurally-identical sub-trees into shared `LetBinding`s
//!   (`design.md` §6 batched-queries example, ~line 1256). The full
//!   alpha-equivalence + nested-CSE algorithm is downstream.
//!
//! Scope reduction. The PR ships the variants the DC + PromQL deployment
//! actually needs (`Scan`, `Window`, `Aggregate`, `LetBinding`, `Ref`).
//! The full `design.md` §6 list is larger (`Filter`, `Project`,
//! `Partition`, `Distinct`, `Merge`, `Join`, `SetOp`, `Sort`, `Limit`,
//! `Subquery`, `WindowFunc`, `BinaryOp`); they are deferred to follow-up
//! phases so each variant lands with a planner consumer rather than as
//! dead code. Adding more is purely additive.
//!
//! Wire-up state. Nothing in `analyzer::Analyzer` or `planner/` consumes
//! these types yet — that's a downstream PR. Phase B exposes the IR so
//! that wiring becomes a focused change rather than a co-emission of new
//! types + new consumers. Phase F's `cse_reuse_is_legal` and
//! `dedupe_subtrees` are similarly defined here for the planner to grow
//! into; the cost-model side that consumes them lives in
//! `planner::cost_model::workload_cost`.

// The intent_algebra module is the new L3 surface — its re-exports are
// the public API that downstream phases will consume. Until Phase C
// (analyzer wiring) lands, none of these symbols have an in-tree call
// site, so the "unused" lints would fire on every build. Suppressing
// them keeps the lint baseline clean. `dead_code` covers the per-variant
// fields and per-impl helpers; `unused_imports` covers the re-export
// surface itself.
#![allow(dead_code, unused_imports)]

pub mod agg_intent;
pub mod cse;
pub mod lower;
pub mod query_expr;
pub mod schema;

// Re-exports for the canonical surface — `crate::intent_algebra::*` for
// downstream callers that don't want to chase sub-module paths.
pub use agg_intent::AggIntent;
pub use cse::{dedupe_subtrees, CseWorkloadPlan};
pub use lower::{lower_parsed_query, LoweringError};
pub use query_expr::{
    BindingScope, HavingPredicate, LabelFilter, QueryExpr, QueryExprError, Source, WindowKind,
};
pub use schema::{cse_reuse_is_legal, Column, ColumnId, CseError, DataType, Schema};
