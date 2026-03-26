//! SP-1 query workload extraction — PromQL and SQL parsers.
//!
//! Both parsers produce a [`ParsedQuery`] that is then merged with any
//! explicit overrides in [`QuerySpec`](crate::analyzer::QuerySpec) by the
//! [`Analyzer`](crate::analyzer::Analyzer).
//!
//! # Supported PromQL patterns
//! - `quantile_over_time(φ, metric{filters}[range]) by (dims)`
//! - `histogram_quantile(φ, rate(metric{filters}[range])) by (le)`
//! - `avg_over_time / min_over_time / max_over_time(metric{filters}[range]) by (dims)`
//! - `sum_over_time / count_over_time(metric{filters}[range]) by (dims)`
//! - `topk(k, count_over_time(metric{filters}[range]) by (dims))`
//! - `count(count_over_time(metric{filters}[range]) by (dims))` — cardinality
//! - Bare metric selector (no agg fn) → `exact_required = true`
//!
//! # Supported SQL patterns (doc §SQL Operators)
//! - `COUNT(*)` with GROUP BY → frequency sketch
//! - `COUNT(DISTINCT col)` → cardinality sketch (HLL)
//! - `AVG(col)` with GROUP BY → quantile p50 proxy (DDSketch)
//! - `MIN(col)` / `MAX(col)` → extreme-quantile proxy (DDSketch)
//! - `SUM(col)` → exact aggregation (no sketch)
//! - `COUNT(*) … ORDER BY … DESC LIMIT k` → heavy-hitter frequency (CountSketch)
//! - Multiple aggregations in one SELECT → all agg types collected; priority:
//!   Cardinality > Frequency (top-K) > Frequency > Quantile > (exact)

pub mod promql;
pub mod sql;

use std::collections::HashMap;
use std::time::Duration;

use crate::types::AggType;

// ── Output types ──────────────────────────────────────────────────────────────

/// Intermediate representation produced by either parser.
#[derive(Debug, Clone)]
pub struct ParsedQuery {
    /// Metric name (PromQL: from selector; SQL: from FROM clause / table).
    pub metric_name: String,
    /// Aggregation types inferred from the query.
    pub aggregations: Vec<AggType>,
    /// Dimensions that must be preserved for GROUP BY / `by (dims)`.
    pub group_by_labels: Vec<String>,
    /// Equality label filters extracted from the query (`{k="v"}` / WHERE k = 'v').
    pub label_filters: HashMap<String, String>,
    /// Time window extracted from the range vector or from the query context.
    pub time_window: Duration,
    /// True when the query requires per-sample exact values (RSI, MACD,
    /// stochastic oscillator, etc.) and sketches offer no benefit.
    pub exact_required: bool,
    /// Quantile φ values implied by the query expression (used to initialise
    /// DDSketch / KLL `quantiles` params).
    pub quantiles: Vec<f64>,
    /// Named pattern hint, when recognised (used by the planner for
    /// domain-specific defaults such as DEBS 5-minute windows).
    pub hint: Option<QueryHint>,
}

/// Named query pattern.  The planner may use this to select defaults that
/// are more appropriate than the generic cost-model result.
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

// ── Entry point ───────────────────────────────────────────────────────────────

/// Parse a raw query string (PromQL or SQL) into a [`ParsedQuery`].
pub fn parse_query(query: &str) -> anyhow::Result<ParsedQuery> {
    let q = query.trim();
    let upper = q.to_ascii_uppercase();
    if upper.starts_with("SELECT") || upper.starts_with("WITH") {
        sql::parse_sql(q)
    } else {
        promql::parse_promql(q)
    }
}

// ── Shared helpers ────────────────────────────────────────────────────────────

/// Parse a PromQL label-selector body (`key="val", key2="val2"`) into a map.
/// Only `=` (exact equality) matchers are captured; `!=`, `=~`, `!~` are skipped.
pub(super) fn parse_label_filters(selector_body: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    // Each matcher: key op "value"   (op = =, !=, =~, !~)
    let re = regex::Regex::new(r#"(\w+)\s*=\s*"([^"]*)"#).unwrap();
    for cap in re.captures_iter(selector_body) {
        out.insert(cap[1].to_string(), cap[2].to_string());
    }
    out
}

/// Parse a comma-separated `by (dim1, dim2)` body into a label list.
pub(super) fn parse_by_dims(by_body: &str) -> Vec<String> {
    by_body
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Returns the DEBS-specific hint for `financial.last_trade_price` queries,
/// or `None` for other metrics.
pub(super) fn debs_hint(
    metric: &str,
    aggs: &[AggType],
    quantiles: &[f64],
    exact_required: bool,
    topk: Option<u64>,
) -> Option<QueryHint> {
    let is_debs = metric == "financial.last_trade_price"
        || metric == "financial_last_trade_price";
    if !is_debs {
        return None;
    }
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
            // Classify by quantile set.
            let qs: std::collections::HashSet<_> = quantiles
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
