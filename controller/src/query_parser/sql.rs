//! SQL-to-sketch mapping for SP-1 workload extraction.
//!
//! Implements the AST traversal algorithm from the design doc:
//! <https://docs.google.com/document/d/1rXNjQwiiJ_jz-DCdPYCh16_1Gr0sdyfPsnvSVhES_iY/>
//!
//! # Mapping rules (doc §SQL Operators)
//!
//! | SQL construct                              | Sketch type        |
//! |--------------------------------------------|--------------------|
//! | COUNT(*) GROUP BY k                        | Frequency (CMS)    |
//! | COUNT(*) GROUP BY k ORDER BY … DESC LIMIT k | Frequency (CS)   |
//! | COUNT(DISTINCT col)                        | Cardinality (HLL)  |
//! | COUNT(DISTINCT col) GROUP BY k             | Cardinality per k  |
//! | AVG(col) GROUP BY k                        | Quantile p50 (DD)  |
//! | MIN/MAX(col) GROUP BY k                    | Quantile p0/p100   |
//! | SUM(col) or COUNT(*) no GROUP BY           | Exact (no sketch)  |
//! | Multiple agg fns in one SELECT             | All collected      |
//! | JOIN + aggregation                         | Pre-agg on join key|
//! | UNION ALL + aggregation                    | Mergeable sketches |

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{anyhow, Context};
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, GroupByExpr,
    ObjectName, OrderByExpr, SelectItem, SetExpr, Statement,
    TableFactor, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use super::{debs_hint, ParsedQuery, QueryHint};
use crate::types::AggType;

// ── Public entry point ────────────────────────────────────────────────────────

/// Parse a SQL SELECT statement into a [`ParsedQuery`].
pub fn parse_sql(sql: &str) -> anyhow::Result<ParsedQuery> {
    let dialect = GenericDialect {};
    let mut stmts = Parser::parse_sql(&dialect, sql)
        .with_context(|| format!("SQL parse error in: {sql:?}"))?;

    let stmt = stmts
        .pop()
        .ok_or_else(|| anyhow!("no statement found in SQL input"))?;

    let query = match stmt {
        Statement::Query(q) => *q,
        other => return Err(anyhow!("expected SELECT query, got {:?}", other)),
    };

    extract_from_query(&query)
}

// ── AST traversal ─────────────────────────────────────────────────────────────

fn extract_from_query(query: &sqlparser::ast::Query) -> anyhow::Result<ParsedQuery> {
    // Unwrap top-level WITH / subquery wrappers until we reach a SELECT.
    match query.body.as_ref() {
        SetExpr::Select(sel) => extract_from_select(sel, query),
        SetExpr::Query(inner) => extract_from_query(inner),
        // UNION ALL → all branches must produce the same sketch type;
        // collect from the left side (dominant).
        SetExpr::SetOperation { left, .. } => {
            let inner_q = sqlparser::ast::Query {
                with: None,
                body: left.clone(),
                order_by: query.order_by.clone(),
                limit: query.limit.clone(),
                limit_by: vec![],
                offset: None,
                fetch: None,
                locks: vec![],
                for_clause: None,
            };
            extract_from_query(&inner_q)
        }
        other => Err(anyhow!("unsupported query body: {:?}", other)),
    }
}

fn extract_from_select(
    sel: &sqlparser::ast::Select,
    query: &sqlparser::ast::Query,
) -> anyhow::Result<ParsedQuery> {
    // ── Step 1: metric name from FROM clause ──────────────────────────────────
    let metric_name = extract_table_name(sel)?;

    // ── Step 2: aggregation functions from SELECT list ────────────────────────
    // Following doc §Func Try_Replace_subtree_SQL_to_Sketch: traverse the
    // projection, find FUNCTION edges, classify each aggregation.
    let agg_infos = collect_agg_infos(&sel.projection);

    // ── Step 3: GROUP BY → aggregate_by dimensions ───────────────────────────
    let group_by_labels = extract_group_by(&sel.group_by);

    // ── Step 4: WHERE → label_filters (equality predicates only) ─────────────
    let label_filters = sel
        .selection
        .as_ref()
        .map(|e| extract_eq_filters(e))
        .unwrap_or_default();

    // ── Step 5: ORDER BY + LIMIT → top-K detection ───────────────────────────
    let topk: Option<u64> = detect_topk(query);

    // ── Step 6: JOIN detection ────────────────────────────────────────────────
    let has_join = sel.from.iter().any(|t| !t.joins.is_empty());

    // ── Step 7: map aggregation infos → AggTypes (doc §Generate_SQL_Aggregation_Sketch_Mapping)
    let (aggregations, quantiles, exact_required) =
        map_agg_infos_to_sketch(&agg_infos, &group_by_labels, topk, has_join);

    // Default time window for SQL queries (no range vector).  Caller may
    // override via explicit `time_window` in QuerySpec.
    let time_window = Duration::from_secs(300); // 5 min DEBS default

    let hint = debs_hint(&metric_name, &aggregations, &quantiles, exact_required, topk);

    Ok(ParsedQuery {
        metric_name,
        aggregations,
        group_by_labels,
        label_filters,
        time_window,
        exact_required,
        quantiles,
        hint,
    })
}

// ── Table name extraction ─────────────────────────────────────────────────────

fn extract_table_name(sel: &sqlparser::ast::Select) -> anyhow::Result<String> {
    sel.from
        .first()
        .and_then(|t| match &t.relation {
            TableFactor::Table { name, .. } => Some(object_name_to_string(name)),
            _ => None,
        })
        .ok_or_else(|| anyhow!("could not determine table/metric name from FROM clause"))
}

fn object_name_to_string(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(|i| i.value.as_str())
        .collect::<Vec<_>>()
        .join(".")
}

// ── Aggregation info collection ───────────────────────────────────────────────

/// Internal classification of a single aggregation function call found in
/// the SELECT list.
#[derive(Debug, Clone)]
enum AggInfo {
    /// COUNT(*) or COUNT(col) without DISTINCT.
    Count,
    /// COUNT(DISTINCT col) or COUNT(DISTINCT *).
    CountDistinct { col: String },
    /// SUM(col).
    Sum { col: String },
    /// AVG(col) — mapped to quantile p50.
    Avg { col: String },
    /// MIN(col) — mapped to quantile p0.
    Min { col: String },
    /// MAX(col) — mapped to quantile p100.
    Max { col: String },
}

fn collect_agg_infos(projection: &[SelectItem]) -> Vec<AggInfo> {
    let mut out = Vec::new();
    for item in projection {
        let expr = match item {
            SelectItem::UnnamedExpr(e)              => e,
            SelectItem::ExprWithAlias { expr, .. }  => expr,
            _                                       => continue,
        };
        collect_from_expr(expr, &mut out);
    }
    out
}

fn collect_from_expr(expr: &Expr, out: &mut Vec<AggInfo>) {
    match expr {
        Expr::Function(f) => {
            let name = f.name.0.last().map(|i| i.value.to_uppercase()).unwrap_or_default();
            let distinct = f.distinct;
            let first_col = first_col_arg(&f.args);
            match name.as_str() {
                "COUNT" => {
                    if distinct {
                        out.push(AggInfo::CountDistinct {
                            col: first_col.unwrap_or_else(|| "*".into()),
                        });
                    } else {
                        out.push(AggInfo::Count);
                    }
                }
                "SUM" => out.push(AggInfo::Sum { col: first_col.unwrap_or_default() }),
                "AVG" => out.push(AggInfo::Avg { col: first_col.unwrap_or_default() }),
                "MIN" => out.push(AggInfo::Min { col: first_col.unwrap_or_default() }),
                "MAX" => out.push(AggInfo::Max { col: first_col.unwrap_or_default() }),
                _     => {}
            }
        }
        // Recurse into binary ops, casts, etc. that may wrap aggregations.
        Expr::BinaryOp { left, right, .. } => {
            collect_from_expr(left, out);
            collect_from_expr(right, out);
        }
        Expr::Nested(inner) => collect_from_expr(inner, out),
        _ => {}
    }
}

/// Returns the column name from the first non-wildcard function argument.
fn first_col_arg(args: &[FunctionArg]) -> Option<String> {
    for arg in args {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(id))) => {
                return Some(id.value.clone());
            }
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::CompoundIdentifier(parts))) => {
                return Some(parts.last().map(|i| i.value.clone()).unwrap_or_default());
            }
            _ => {}
        }
    }
    None
}

// ── GROUP BY extraction ───────────────────────────────────────────────────────

fn extract_group_by(group_by: &GroupByExpr) -> Vec<String> {
    let exprs = match group_by {
        GroupByExpr::All              => return vec![],
        GroupByExpr::Expressions(e)   => e,
    };
    exprs
        .iter()
        .filter_map(|e| match e {
            Expr::Identifier(id) => Some(id.value.clone()),
            Expr::CompoundIdentifier(parts) => {
                parts.last().map(|i| i.value.clone())
            }
            _ => None,
        })
        .collect()
}

// ── WHERE equality filter extraction ─────────────────────────────────────────

/// Recursively extracts `col = 'literal'` predicates from a WHERE expression.
fn extract_eq_filters(expr: &Expr) -> HashMap<String, String> {
    let mut out = HashMap::new();
    extract_eq_filters_rec(expr, &mut out);
    out
}

fn extract_eq_filters_rec(expr: &Expr, out: &mut HashMap<String, String>) {
    match expr {
        Expr::BinaryOp { left, op: BinaryOperator::Eq, right } => {
            let col = match left.as_ref() {
                Expr::Identifier(id) => Some(id.value.clone()),
                Expr::CompoundIdentifier(parts) => parts.last().map(|i| i.value.clone()),
                _ => None,
            };
            let val = match right.as_ref() {
                Expr::Value(Value::SingleQuotedString(s)) => Some(s.clone()),
                Expr::Value(Value::DoubleQuotedString(s)) => Some(s.clone()),
                _ => None,
            };
            if let (Some(k), Some(v)) = (col, val) {
                out.insert(k, v);
            }
        }
        Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
            extract_eq_filters_rec(left, out);
            extract_eq_filters_rec(right, out);
        }
        Expr::Nested(inner) => extract_eq_filters_rec(inner, out),
        _ => {}
    }
}

// ── Top-K detection ───────────────────────────────────────────────────────────

/// Returns `Some(k)` when the query has an `ORDER BY … DESC LIMIT k` pattern
/// that implies a heavy-hitter / top-K sketch (CountSketch).
fn detect_topk(query: &sqlparser::ast::Query) -> Option<u64> {
    let limit = match query.limit.as_ref()? {
        Expr::Value(Value::Number(n, _)) => n.parse::<u64>().ok()?,
        _ => return None,
    };
    // Must have at least one DESC order-by column.
    let has_desc = query.order_by.iter().any(|o: &OrderByExpr| {
        matches!(o.asc, Some(false) | None) // None = default (ASC), Some(false) = DESC
    });
    if has_desc { Some(limit) } else { None }
}

// ── Sketch mapping (doc §Generate_SQL_Aggregation_Sketch_Mapping) ─────────────

/// Map collected `AggInfo`s to `(agg_types, quantiles, exact_required)`.
///
/// Priority (highest to lowest):
/// 1. Cardinality (`COUNT DISTINCT`) — HLL
/// 2. Frequency heavy-hitter (COUNT + top-K ORDER BY LIMIT) — CountSketch
/// 3. Frequency (COUNT GROUP BY) — CountMinSketch
/// 4. Quantile (AVG → p50, MIN → p0, MAX → p100) — DDSketch
/// 5. Sum/Count without GROUP BY → exact, no sketch
fn map_agg_infos_to_sketch(
    infos: &[AggInfo],
    group_by: &[String],
    topk: Option<u64>,
    _has_join: bool,
) -> (Vec<AggType>, Vec<f64>, bool) {
    let mut aggs: Vec<AggType> = Vec::new();
    let mut quantiles: Vec<f64> = Vec::new();
    let mut has_exact_only = false;

    for info in infos {
        match info {
            // COUNT(DISTINCT col) → Cardinality (HLL), doc §Distinct with Aggregation
            AggInfo::CountDistinct { .. } => {
                if !aggs.contains(&AggType::Cardinality) {
                    aggs.push(AggType::Cardinality);
                }
            }
            // COUNT(*) with GROUP BY → Frequency.
            // With ORDER BY … DESC LIMIT k → heavy-hitter (CountSketch).
            AggInfo::Count => {
                if !group_by.is_empty() || topk.is_some() {
                    if !aggs.contains(&AggType::Frequency) {
                        aggs.push(AggType::Frequency);
                    }
                } else {
                    has_exact_only = true; // global COUNT(*) — trivially exact
                }
            }
            // AVG → p50 proxy, doc §Projection with Aggregation
            AggInfo::Avg { .. } => {
                if !aggs.contains(&AggType::Quantile) {
                    aggs.push(AggType::Quantile);
                }
                if !quantiles.contains(&0.5) {
                    quantiles.push(0.5);
                }
            }
            // MIN → p0, MAX → p100, doc §Selection with Aggregation (extrema via sketches)
            AggInfo::Min { .. } => {
                if !aggs.contains(&AggType::Quantile) {
                    aggs.push(AggType::Quantile);
                }
                if !quantiles.contains(&0.0) {
                    quantiles.push(0.0);
                }
            }
            AggInfo::Max { .. } => {
                if !aggs.contains(&AggType::Quantile) {
                    aggs.push(AggType::Quantile);
                }
                if !quantiles.contains(&1.0) {
                    quantiles.push(1.0);
                }
            }
            // SUM is always exact — no sketch benefit.
            AggInfo::Sum { .. } => {
                has_exact_only = true;
            }
        }
    }

    // If we have only exact-only aggregations and no sketch-eligible ones,
    // mark as exact_required.
    let exact_required = aggs.is_empty() && has_exact_only;

    // Sort quantiles ascending.
    quantiles.sort_by(|a, b| a.partial_cmp(b).unwrap());
    quantiles.dedup();

    (aggs, quantiles, exact_required)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(sql: &str) -> ParsedQuery {
        parse_sql(sql).unwrap_or_else(|e| panic!("parse_sql failed: {e}\nSQL: {sql}"))
    }

    // ── ClickBench patterns ───────────────────────────────────────────────────

    #[test]
    fn count_distinct_cardinality() {
        let pq = parse("SELECT COUNT(DISTINCT UserID) FROM hits");
        assert!(pq.aggregations.contains(&AggType::Cardinality));
        assert!(!pq.exact_required);
    }

    #[test]
    fn count_star_group_by_frequency() {
        let pq = parse("SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID");
        assert!(pq.aggregations.contains(&AggType::Frequency));
        assert!(pq.group_by_labels.contains(&"AdvEngineID".to_string()));
    }

    #[test]
    fn topk_order_by_desc_limit() {
        let pq = parse(
            "SELECT SearchPhrase, COUNT(*) AS c FROM hits \
             WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
        );
        assert!(pq.aggregations.contains(&AggType::Frequency));
    }

    #[test]
    fn avg_maps_to_quantile_p50() {
        let pq = parse("SELECT symbol, AVG(last) FROM hits GROUP BY symbol");
        assert!(pq.aggregations.contains(&AggType::Quantile));
        assert!(pq.quantiles.contains(&0.5));
    }

    #[test]
    fn min_max_maps_to_quantile_extremes() {
        let pq = parse("SELECT symbol, MIN(last), MAX(last) FROM hits GROUP BY symbol");
        assert!(pq.aggregations.contains(&AggType::Quantile));
        assert!(pq.quantiles.contains(&0.0));
        assert!(pq.quantiles.contains(&1.0));
    }

    #[test]
    fn sum_exact_no_sketch() {
        let pq = parse("SELECT SUM(AdvEngineID) FROM hits");
        assert!(pq.exact_required);
        assert!(pq.aggregations.is_empty());
    }

    #[test]
    fn count_distinct_with_group_by() {
        // doc §ClickBench: RegionID + COUNT(DISTINCT UserID) → per-region HLL
        let pq = parse(
            "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10",
        );
        assert!(pq.aggregations.contains(&AggType::Cardinality));
        assert!(pq.group_by_labels.contains(&"RegionID".to_string()));
    }

    #[test]
    fn multi_agg_collects_all() {
        // doc §ClickBench: SUM + COUNT + AVG + COUNT(DISTINCT) → multiple sketches
        let pq = parse(
            "SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) \
             FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10",
        );
        assert!(pq.aggregations.contains(&AggType::Cardinality), "should have cardinality");
        assert!(pq.aggregations.contains(&AggType::Frequency),   "should have frequency");
        assert!(pq.aggregations.contains(&AggType::Quantile),    "should have quantile");
    }

    #[test]
    fn where_equality_filter_extracted() {
        let pq = parse("SELECT COUNT(*) FROM hits WHERE sectype = 'E' GROUP BY symbol");
        assert_eq!(pq.label_filters.get("sectype").map(String::as_str), Some("E"));
    }

    #[test]
    fn metric_name_from_table() {
        let pq = parse("SELECT COUNT(*) FROM financial.last_trade_price GROUP BY symbol");
        assert_eq!(pq.metric_name, "financial.last_trade_price");
    }

    // ── DEBS SQL variants ─────────────────────────────────────────────────────

    #[test]
    fn debs_q6_sql_cardinality() {
        let pq = parse(
            "SELECT COUNT(DISTINCT symbol) AS active_symbols \
             FROM financial.last_trade_price",
        );
        assert!(pq.aggregations.contains(&AggType::Cardinality));
        matches!(pq.hint, Some(QueryHint::DebsCardinality));
    }

    #[test]
    fn debs_q3_sql_topk() {
        let pq = parse(
            "SELECT symbol, COUNT(*) AS c FROM financial.last_trade_price \
             GROUP BY symbol ORDER BY c DESC LIMIT 10",
        );
        assert!(pq.aggregations.contains(&AggType::Frequency));
    }
}
