//! SP-1 query workload extraction — PromQL and SQL parsers.
//!
//! Both parsers compile to the shared [`SketchExpr`] algebra IR defined in
//! [`sketch_algebra`].  The optimizer in [`sketch_rules`] applies algebraic
//! rewrite rules before the planner receives the result.
//!
//! # Entry points
//!
//! | Function | Returns | Use |
//! |---|---|---|
//! | [`parse_query_sketch`] | `SketchExpr` | New callers — full algebra IR |
//! | [`parse_query`] | `ParsedQuery` | Backward compat with existing analyzer |
//!
//! # Supported PromQL patterns (via `promql-parser` AST)
//! - `quantile_over_time(φ, m{f}[w]) by (dims)`
//! - `histogram_quantile(φ, rate(m{f}[w])) by (le)`
//! - `avg/min/max/stddev/stdvar_over_time(m{f}[w]) by (dims)`
//! - `sum/count_over_time(m{f}[w]) by (dims)`
//! - `topk(k, *_over_time(…) by (dims))`
//! - `count(*_over_time(…) by (dims))` — cardinality
//! - `changes/resets(m{f}[w])`
//! - Bare metric selector / binary op → `exact_required`
//!
//! # Supported SQL patterns (doc §SQL Operators)
//! - `COUNT(*)` with/without GROUP BY → frequency / exact
//! - `COUNT(DISTINCT col)` ± GROUP BY → cardinality / Hydra
//! - `AVG/MIN/MAX(col)` ± GROUP BY → quantile / exact extrema
//! - `SUM(col)` → exact
//! - ORDER BY … DESC LIMIT k → heavy-hitter CountSketch
//! - Multiple aggs in one SELECT → all ops collected (Merge)
//! - JOIN … ON key → JoinSketch push-down
//! - UNION ALL → Merge (sketch linearity)

pub mod promql;
pub mod sql;
pub mod sketch_algebra;
pub mod sketch_rules;

use std::collections::HashMap;
use std::time::Duration;

use crate::algebra::expr::QueryExpr;
use crate::types::AggType;
pub use sketch_algebra::SketchExpr;

// ── Output types (legacy — consumed by analyzer and planner) ──────────────────

/// Flat intermediate representation consumed by [`crate::analyzer::Analyzer`].
///
/// Produced by [`parse_query`] via [`SketchExpr::to_parsed_query`].
/// New code should use [`parse_query_sketch`] → [`SketchExpr`] directly.
#[derive(Debug, Clone)]
pub struct ParsedQuery {
    /// Metric name (PromQL: from selector; SQL: FROM clause table).
    pub metric_name: String,
    /// Aggregation types inferred from the query.
    pub aggregations: Vec<AggType>,
    /// Dimensions that must be preserved for GROUP BY / `by (dims)`.
    pub group_by_labels: Vec<String>,
    /// Equality label filters extracted from the query.
    pub label_filters: HashMap<String, String>,
    /// Time window extracted from the range vector or query context.
    pub time_window: Duration,
    /// True when the query requires per-sample exact values.
    pub exact_required: bool,
    /// Quantile φ values implied by the query.
    pub quantiles: Vec<f64>,
    /// Named pattern hint for domain-specific planner defaults.
    pub hint: Option<QueryHint>,
}

/// Named query pattern recognised by the DEBS-aware planner.
#[derive(Debug, Clone)]
pub enum QueryHint {
    // ── DEBS 2022 financial queries ───────────────────────────────────────────
    /// Q1 – per-symbol EMA via quantile proxy (DDSketch / KLL).
    DebsEma,
    /// Q3 – top-K symbols by event count or price move (CountSketch).
    DebsTopK { k: u64 },
    /// Q4 – per-symbol high / low / last / range (extreme-quantile DDSketch).
    DebsPriceStats,
    /// Q5 / Q9 – realized volatility / Bollinger bands via IQR proxy.
    DebsVolatility,
    /// Q6 – distinct active symbols per window (HLL).
    DebsCardinality,
    /// Q7 – TWAP as median / p50 (DDSketch).
    DebsTwap,
    /// Q8 – price anomaly detection via IQR (DDSketch).
    DebsAnomaly,
    // ── Exact-only patterns ───────────────────────────────────────────────────
    /// Query requires stateful per-sample computation; no sketch benefit.
    ExactRequired { reason: String },
}

// ── Public entry points ───────────────────────────────────────────────────────

/// Parse a raw query string (PromQL or SQL) into the general [`QueryExpr`] IR.
///
/// Unlike [`parse_query_sketch`], this path emits `QueryExpr` *directly*
/// from the AST — preserving HistogramQuantile, PromQLSubquery,
/// vector-binary-op matching, Sort+Limit, Join, and SetOp without any
/// lossy round-trip through [`SketchExpr`].
pub fn parse_query_expr(query: &str) -> anyhow::Result<QueryExpr> {
    let q = query.trim();
    let upper = q.to_ascii_uppercase();
    if upper.starts_with("SELECT") || upper.starts_with("WITH") {
        sql::parse_sql_expr(q)
    } else {
        promql::parse_promql_expr(q)
    }
}

/// Parse a raw query string (PromQL or SQL) into the full [`SketchExpr`] IR.
///
/// The returned tree has already been through the algebraic optimizer
/// ([`sketch_rules::optimize`]).
pub fn parse_query_sketch(query: &str) -> anyhow::Result<SketchExpr> {
    let q = query.trim();
    let upper = q.to_ascii_uppercase();
    if upper.starts_with("SELECT") || upper.starts_with("WITH") {
        sql::parse_sql(q)
    } else {
        promql::parse_promql(q)
    }
}

/// Parse a raw query string (PromQL or SQL) into a [`ParsedQuery`].
///
/// This is the backward-compatible entry point for the existing
/// [`crate::analyzer::Analyzer`].  Internally it calls [`parse_query_sketch`]
/// and converts via [`SketchExpr::to_parsed_query`].
pub fn parse_query(query: &str) -> anyhow::Result<ParsedQuery> {
    Ok(parse_query_sketch(query)?.to_parsed_query())
}

// ── DEBS hint classifier (shared by both parsers via to_parsed_query) ─────────

/// Returns the DEBS-specific hint for `financial.last_trade_price` queries.
pub(super) fn debs_hint(
    metric:         &str,
    aggs:           &[AggType],
    quantiles:      &[f64],
    exact_required: bool,
    topk:           Option<u64>,
) -> Option<QueryHint> {
    let is_debs = metric == "financial.last_trade_price"
        || metric == "financial_last_trade_price";
    if !is_debs { return None; }

    if exact_required {
        return Some(QueryHint::ExactRequired {
            reason: "query requires per-sample stateful computation".into(),
        });
    }
    if let Some(k) = topk {
        return Some(QueryHint::DebsTopK { k });
    }
    let primary = aggs.first()?;
    match primary {
        AggType::Cardinality => Some(QueryHint::DebsCardinality),
        AggType::Frequency   => Some(QueryHint::DebsTopK { k: 10 }),
        AggType::Quantile    => {
            let qs: std::collections::HashSet<i32> = quantiles
                .iter()
                .map(|&q| (q * 100.0).round() as i32)
                .collect();
            if qs.contains(&50) && qs.len() == 1 {
                Some(QueryHint::DebsTwap)
            } else if qs.contains(&0) || qs.contains(&100) {
                Some(QueryHint::DebsPriceStats)
            } else if qs.contains(&25) && qs.contains(&75) && qs.contains(&50) {
                Some(QueryHint::DebsAnomaly)
            } else if qs.contains(&25) && qs.contains(&75) {
                Some(QueryHint::DebsVolatility)
            } else {
                Some(QueryHint::DebsEma)
            }
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // Smoke tests for the unified entry point.

    #[test]
    fn sql_dispatched_correctly() {
        let pq = parse_query("SELECT COUNT(*) FROM hits GROUP BY AdvEngineID").unwrap();
        assert!(pq.aggregations.contains(&AggType::Frequency));
    }

    #[test]
    fn promql_dispatched_correctly() {
        // `by` belongs to the aggregate operator, not the function call.
        let pq = parse_query(
            "sum by (host) (quantile_over_time(0.99, latency[5m]))"
        ).unwrap();
        assert!(pq.aggregations.contains(&AggType::Quantile));
        assert_eq!(pq.quantiles, vec![0.99]);
    }

    #[test]
    fn parse_query_sketch_returns_expr() {
        let expr = parse_query_sketch(
            "topk by (symbol) (10, count_over_time(financial_last_trade_price[5m]))"
        ).unwrap();
        // Should have been optimized — result is some SketchExpr tree.
        // Just check it doesn't error.
        let _ = expr.to_parsed_query();
    }
}
