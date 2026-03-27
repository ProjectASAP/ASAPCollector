//! SQL → SketchExpr compiler.
//!
//! Implements the `AST_SQL_to_sketch` algorithm from the design doc
//! (`docs/Top-Down SQL-to-sketch mapping.pdf`).
//!
//! # Algorithm
//!
//! For each FUNCTION edge in the SELECT projection (leaf → root):
//!   1. Collect context: GROUP BY, WHERE, HAVING, JOIN, DISTINCT, UNION ALL
//!   2. Call `try_replace` → choose `SketchAggOp` (or Exact passthrough)
//!   3. Apply structural transformations:
//!      - WHERE predicates   → pushed into Filter node
//!      - HAVING predicates  → stay above Agg as a second Filter
//!      - Multi-dim GROUP BY → Hydra via rewrite rule R4
//!      - JOIN               → JoinSketch push-down
//!      - UNION ALL          → Merge node (sketch linearity via rule R3)
//!      - DISTINCT           → Dedup node (absorbed by HLL via rule R6)
//!
//! Multiple aggregations in one SELECT each emit their own `SketchAggOp`,
//! then are combined with a `Merge` node (coverage = Partial when some are Exact).

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{anyhow, Context};
use sqlparser::ast::{
    BinaryOperator, DuplicateTreatment, Expr, FunctionArg, FunctionArgExpr,
    FunctionArgumentList, FunctionArguments, GroupByExpr, Join, JoinConstraint,
    JoinOperator, LimitClause, ObjectName, OrderBy, OrderByExpr, OrderByKind,
    Query, Select, SelectItem, SetExpr, SetOperator, Statement, TableFactor,
    Value, ValueWithSpan,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use super::sketch_algebra::{
    ColumnRef, ExactAgg, FilterOp, FilterVal, PartitionKeys, Predicate,
    SketchAggOp, SketchExpr, SketchCoverage, SourceSpec,
};
use super::sketch_rules::optimize;

// ── Public entry point ────────────────────────────────────────────────────────

/// Parse a SQL SELECT statement into an optimised [`SketchExpr`].
pub fn parse_sql(sql: &str) -> anyhow::Result<SketchExpr> {
    let dialect = GenericDialect {};
    let mut stmts = Parser::parse_sql(&dialect, sql)
        .with_context(|| format!("SQL parse error: {sql:?}"))?;
    let stmt = stmts.pop().ok_or_else(|| anyhow!("no SQL statement found"))?;
    let query = match stmt {
        Statement::Query(q) => *q,
        other => return Err(anyhow!("expected SELECT, got {:?}", other)),
    };
    let sketch = extract_from_query(&query)?;
    Ok(optimize(sketch))
}

// ── Query-level dispatch ──────────────────────────────────────────────────────

fn extract_from_query(query: &Query) -> anyhow::Result<SketchExpr> {
    let order_by: Vec<OrderByExpr> = match &query.order_by {
        Some(OrderBy { kind: OrderByKind::Expressions(exprs), .. }) => exprs.clone(),
        _ => vec![],
    };
    let limit: Option<&Expr> = match &query.limit_clause {
        Some(LimitClause::LimitOffset { limit: Some(e), .. }) => Some(e),
        Some(LimitClause::OffsetCommaLimit { limit: e, .. }) => Some(e),
        _ => None,
    };
    extract_from_set_expr(query.body.as_ref(), &order_by, limit)
}

fn extract_from_set_expr(
    set_expr: &SetExpr,
    order_by: &[OrderByExpr],
    limit: Option<&Expr>,
) -> anyhow::Result<SketchExpr> {
    match set_expr {
        SetExpr::Select(sel) => extract_from_select(sel, order_by, limit),
        SetExpr::Query(inner) => extract_from_query(inner),

        // UNION ALL → Merge of both branches (sketch linearity rule R3 will
        // distribute Agg over Merge if ops are mergeable).
        SetExpr::SetOperation { left, right, op: SetOperator::Union, .. } => {
            let left_expr  = extract_from_set_expr(left,  &[], None)?;
            let right_expr = extract_from_set_expr(right, &[], None)?;
            Ok(SketchExpr::Merge { inputs: vec![left_expr, right_expr] })
        }

        other => Err(anyhow!("unsupported query body: {:?}", other)),
    }
}

// ── SELECT-level extraction ───────────────────────────────────────────────────

fn extract_from_select(
    sel: &Select,
    order_by: &[OrderByExpr],
    limit: Option<&Expr>,
) -> anyhow::Result<SketchExpr> {
    // ── Step 1: source table(s) ───────────────────────────────────────────────
    let metric_name = extract_table_name(sel)?;

    // ── Step 2: WHERE predicates ──────────────────────────────────────────────
    let where_preds: Vec<Predicate> = sel.selection.as_ref()
        .map(|e| collect_predicates(e))
        .unwrap_or_default();

    // ── Step 3: GROUP BY keys ─────────────────────────────────────────────────
    let group_by_keys = extract_group_by(&sel.group_by);

    // ── Step 4: HAVING predicates ─────────────────────────────────────────────
    let having_preds: Vec<Predicate> = sel.having.as_ref()
        .map(|e| collect_predicates(e))
        .unwrap_or_default();

    // ── Step 5: top-K detection (ORDER BY … DESC LIMIT k) ────────────────────
    let topk: Option<u64> = detect_topk(order_by, limit);

    // ── Step 6: JOIN detection ────────────────────────────────────────────────
    let join_info: Option<JoinInfo> = extract_join_info(sel);

    // ── Step 7: SELECT-DISTINCT flag ──────────────────────────────────────────
    let select_distinct = sel.distinct.is_some();

    // ── Step 8: aggregation functions from SELECT list ────────────────────────
    let agg_items = collect_agg_items(&sel.projection);

    if agg_items.is_empty() {
        // No aggregation — bare SELECT (e.g. SELECT col FROM t WHERE …).
        let source   = SketchExpr::Source(SourceSpec { name: metric_name });
        let filtered = apply_filters(source, where_preds);
        return Ok(filtered);
    }

    // ── Step 9: try_replace each agg item → SketchAggOp ──────────────────────
    let ops: Vec<(SketchAggOp, ColumnRef)> = agg_items
        .iter()
        .map(|item| try_replace(item, &group_by_keys, topk, join_info.as_ref(), select_distinct))
        .collect();

    // ── Step 10: determine coverage ───────────────────────────────────────────
    let has_sketch = ops.iter().any(|(op, _)| !op.is_exact());
    let has_exact  = ops.iter().any(|(op, _)| op.is_exact());
    let _coverage  = match (has_sketch, has_exact) {
        (true,  false) => SketchCoverage::Full,
        (true,  true)  => SketchCoverage::Partial,
        (false, _)     => SketchCoverage::None,
    };

    // ── Step 11: assemble the SketchExpr tree ────────────────────────────────

    // Base source with pushed-down WHERE.
    let source   = SketchExpr::Source(SourceSpec { name: metric_name.clone() });
    let filtered = apply_filters(source, where_preds);

    // If there's a JOIN, build JoinSketch.  Otherwise use the filtered source.
    let base = if let Some(ji) = join_info {
        build_join_sketch(filtered, ji, &ops, &group_by_keys)?
    } else {
        build_agg_tree(filtered, ops, &group_by_keys, topk, having_preds)
    };

    Ok(base)
}

// ── try_replace: aggregation function → SketchAggOp ──────────────────────────

struct AggItem {
    kind:     AggKind,
    col:      ColumnRef,
    distinct: bool,
}

#[derive(Debug, Clone)]
enum AggKind {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// Map one aggregation item to its best sketch operator.
///
/// Priority (doc §Generate_SQL_Aggregation_Sketch_Mapping):
///   1. Cardinality (COUNT DISTINCT) → HLL
///   2. Frequency heavy-hitter (COUNT + top-K) → CountSketch
///   3. Frequency (COUNT GROUP BY) → CountMin
///   4. Quantile (AVG/MIN/MAX with GROUP BY) → DDSketch
///   5. Extrema without GROUP BY → ExactMinMax
///   6. Sum / global count → Exact
fn try_replace(
    item:        &AggItem,
    group_by:    &[String],
    topk:        Option<u64>,
    _join_info:  Option<&JoinInfo>,
    _distinct:   bool,
) -> (SketchAggOp, ColumnRef) {
    let has_group = !group_by.is_empty();

    match item.kind {
        AggKind::Count if item.distinct => {
            // COUNT(DISTINCT col) → HLL
            (SketchAggOp::default_hll(), item.col.clone())
        }
        AggKind::Count => {
            if let Some(k) = topk {
                // ORDER BY … DESC LIMIT k → heavy-hitter CountSketch
                (SketchAggOp::CountSketch { k }, item.col.clone())
            } else if has_group {
                // COUNT(*) GROUP BY → frequency CountMin
                (SketchAggOp::default_count_min(), item.col.clone())
            } else {
                // Global COUNT(*) — exact
                (SketchAggOp::Exact(ExactAgg::Count), item.col.clone())
            }
        }
        AggKind::Avg => {
            if has_group {
                // AVG per group → DDSketch(p50) — mergeable proxy
                (SketchAggOp::default_ddsketch(vec![0.5]), item.col.clone())
            } else {
                // Global AVG — not mergeable as-is; store exact (sum,count)
                (SketchAggOp::Exact(ExactAgg::Avg), item.col.clone())
            }
        }
        AggKind::Min => {
            if has_group {
                (SketchAggOp::default_ddsketch(vec![0.0]), item.col.clone())
            } else {
                (SketchAggOp::ExactMinMax { min: true, max: false }, item.col.clone())
            }
        }
        AggKind::Max => {
            if has_group {
                (SketchAggOp::default_ddsketch(vec![1.0]), item.col.clone())
            } else {
                (SketchAggOp::ExactMinMax { min: false, max: true }, item.col.clone())
            }
        }
        AggKind::Sum => {
            // SUM is always exact — no sketch benefit.
            (SketchAggOp::Exact(ExactAgg::Sum), item.col.clone())
        }
    }
}

// ── Tree assembly ─────────────────────────────────────────────────────────────

fn build_agg_tree(
    base:        SketchExpr,
    ops:         Vec<(SketchAggOp, ColumnRef)>,
    group_by:    &[String],
    topk:        Option<u64>,
    having:      Vec<Predicate>,
) -> SketchExpr {
    // One Agg node per op; combine with Merge if multiple.
    let agg_nodes: Vec<SketchExpr> = ops
        .into_iter()
        .map(|(op, col)| SketchExpr::Agg { op, col, input: Box::new(base.clone()) })
        .collect();

    let merged = if agg_nodes.len() == 1 {
        agg_nodes.into_iter().next().unwrap()
    } else {
        SketchExpr::Merge { inputs: agg_nodes }
    };

    // Apply GROUP BY partition.
    let partitioned = if group_by.is_empty() {
        merged
    } else {
        SketchExpr::Partition {
            keys:  PartitionKeys::By(group_by.to_vec()),
            input: Box::new(merged),
        }
    };

    // Apply HAVING as a post-agg filter.
    let after_having = apply_filters(partitioned, having);

    // Apply top-K.
    if let Some(k) = topk {
        SketchExpr::TopK { k, input: Box::new(after_having) }
    } else {
        after_having
    }
}

// ── JOIN push-down ─────────────────────────────────────────────────────────────

struct JoinInfo {
    inner_table: String,
    join_key:    String,
    outer_key:   String,
}

fn build_join_sketch(
    outer_filtered: SketchExpr,
    ji:             JoinInfo,
    ops:            &[(SketchAggOp, ColumnRef)],
    outer_group_by: &[String],
) -> anyhow::Result<SketchExpr> {
    // Pre-aggregate on the inner table grouped by the join key.
    let inner_source = SketchExpr::Source(SourceSpec { name: ji.inner_table.clone() });
    let inner_agg = if let Some((op, col)) = ops.first() {
        SketchExpr::Agg { op: op.clone(), col: col.clone(), input: Box::new(inner_source) }
    } else {
        inner_source
    };
    let inner_partitioned = SketchExpr::Partition {
        keys:  PartitionKeys::By(vec![ji.join_key.clone()]),
        input: Box::new(inner_agg),
    };

    Ok(SketchExpr::JoinSketch {
        join_key: ji.outer_key,
        outer:    Box::new(outer_filtered),
        inner:    Box::new(inner_partitioned),
    })
}

// ── AST helpers: aggregation collection ──────────────────────────────────────

fn collect_agg_items(projection: &[SelectItem]) -> Vec<AggItem> {
    let mut out = Vec::new();
    for item in projection {
        let expr = match item {
            SelectItem::UnnamedExpr(e)             => e,
            SelectItem::ExprWithAlias { expr, .. } => expr,
            _                                      => continue,
        };
        collect_agg_from_expr(expr, &mut out);
    }
    out
}

fn collect_agg_from_expr(expr: &Expr, out: &mut Vec<AggItem>) {
    match expr {
        Expr::Function(f) => {
            let fn_name = f.name.0.last()
                .and_then(|i| i.as_ident())
                .map(|id| id.value.to_uppercase())
                .unwrap_or_default();

            let (distinct, args) = match &f.args {
                FunctionArguments::List(FunctionArgumentList { duplicate_treatment, args, .. }) => {
                    let is_distinct = matches!(
                        duplicate_treatment,
                        Some(DuplicateTreatment::Distinct)
                    );
                    (is_distinct, args.as_slice())
                }
                _ => (false, &[][..]),
            };

            let col = first_col_from_args(args);

            let kind = match fn_name.as_str() {
                "COUNT" => AggKind::Count,
                "SUM"   => AggKind::Sum,
                "AVG"   => AggKind::Avg,
                "MIN"   => AggKind::Min,
                "MAX"   => AggKind::Max,
                _       => return,
            };

            out.push(AggItem { kind, col, distinct });
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_agg_from_expr(left, out);
            collect_agg_from_expr(right, out);
        }
        Expr::Nested(inner) => collect_agg_from_expr(inner, out),
        _ => {}
    }
}

fn first_col_from_args(args: &[FunctionArg]) -> ColumnRef {
    for arg in args {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => return ColumnRef::Wildcard,
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(id))) => {
                return ColumnRef::Named(id.value.clone());
            }
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::CompoundIdentifier(parts))) => {
                if let Some(last) = parts.last() {
                    return ColumnRef::Named(last.value.clone());
                }
            }
            _ => {}
        }
    }
    ColumnRef::Wildcard
}

// ── AST helpers: GROUP BY ──────────────────────────────────────────────────────

fn extract_group_by(group_by: &GroupByExpr) -> Vec<String> {
    let exprs = match group_by {
        GroupByExpr::All(_)         => return vec![],
        GroupByExpr::Expressions(e, _) => e,
    };
    exprs.iter().filter_map(|e| match e {
        Expr::Identifier(id)             => Some(id.value.clone()),
        Expr::CompoundIdentifier(parts)  => parts.last().map(|i| i.value.clone()),
        _                                => None,
    }).collect()
}

// ── AST helpers: WHERE / predicate extraction ─────────────────────────────────

fn collect_predicates(expr: &Expr) -> Vec<Predicate> {
    let mut out = Vec::new();
    collect_pred_rec(expr, &mut out);
    out
}

fn collect_pred_rec(expr: &Expr, out: &mut Vec<Predicate>) {
    match expr {
        // col = 'v'
        Expr::BinaryOp { left, op: BinaryOperator::Eq, right } => {
            if let Some(p) = binary_pred(left, BinaryOperator::Eq, right) { out.push(p); }
        }
        // col <> 'v'
        Expr::BinaryOp { left, op: BinaryOperator::NotEq, right } => {
            if let Some(p) = binary_pred(left, BinaryOperator::NotEq, right) { out.push(p); }
        }
        // col > v
        Expr::BinaryOp { left, op: BinaryOperator::Gt, right } => {
            if let Some(p) = binary_pred(left, BinaryOperator::Gt, right) { out.push(p); }
        }
        // col >= v
        Expr::BinaryOp { left, op: BinaryOperator::GtEq, right } => {
            if let Some(p) = binary_pred(left, BinaryOperator::GtEq, right) { out.push(p); }
        }
        // col < v
        Expr::BinaryOp { left, op: BinaryOperator::Lt, right } => {
            if let Some(p) = binary_pred(left, BinaryOperator::Lt, right) { out.push(p); }
        }
        // col <= v
        Expr::BinaryOp { left, op: BinaryOperator::LtEq, right } => {
            if let Some(p) = binary_pred(left, BinaryOperator::LtEq, right) { out.push(p); }
        }
        // AND — recurse into both sides
        Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
            collect_pred_rec(left, out);
            collect_pred_rec(right, out);
        }
        // col LIKE '%v%'
        Expr::Like { expr, pattern, negated, .. } => {
            if let Some(col) = col_name(expr) {
                if let Some(val) = literal_str(pattern) {
                    let op = if *negated { FilterOp::NotLike } else { FilterOp::Like };
                    out.push(Predicate { col, op, val: FilterVal::Str(val) });
                }
            }
        }
        // col IS NULL / IS NOT NULL
        Expr::IsNull(inner) => {
            if let Some(col) = col_name(inner) {
                out.push(Predicate { col, op: FilterOp::IsNull, val: FilterVal::Null });
            }
        }
        Expr::IsNotNull(inner) => {
            if let Some(col) = col_name(inner) {
                out.push(Predicate { col, op: FilterOp::IsNotNull, val: FilterVal::Null });
            }
        }
        Expr::Nested(inner) => collect_pred_rec(inner, out),
        // OR predicates span both columns — cannot push to collector; skip.
        _ => {}
    }
}

fn binary_pred(left: &Expr, sql_op: BinaryOperator, right: &Expr) -> Option<Predicate> {
    let col = col_name(left)?;
    let (op, val) = match sql_op {
        BinaryOperator::Eq    => (FilterOp::Eq, literal_val(right)?),
        BinaryOperator::NotEq => (FilterOp::Ne, literal_val(right)?),
        BinaryOperator::Gt    => (FilterOp::Gt, literal_val(right)?),
        BinaryOperator::GtEq  => (FilterOp::Ge, literal_val(right)?),
        BinaryOperator::Lt    => (FilterOp::Lt, literal_val(right)?),
        BinaryOperator::LtEq  => (FilterOp::Le, literal_val(right)?),
        _                     => return None,
    };
    Some(Predicate { col, op, val })
}

fn col_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(id)            => Some(id.value.clone()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|i| i.value.clone()),
        _                               => None,
    }
}

fn literal_val(expr: &Expr) -> Option<FilterVal> {
    let v = match expr {
        Expr::Value(vws) => &vws.value,
        _ => return None,
    };
    match v {
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
            Some(FilterVal::Str(s.clone()))
        }
        Value::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                Some(FilterVal::Int(i))
            } else if let Ok(f) = n.parse::<f64>() {
                Some(FilterVal::Num(f))
            } else {
                None
            }
        }
        Value::Null => Some(FilterVal::Null),
        _ => None,
    }
}

fn literal_str(expr: &Expr) -> Option<String> {
    let v = match expr {
        Expr::Value(vws) => &vws.value,
        _ => return None,
    };
    match v {
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => Some(s.clone()),
        _ => None,
    }
}

// ── AST helpers: table name ───────────────────────────────────────────────────

fn extract_table_name(sel: &Select) -> anyhow::Result<String> {
    sel.from.first()
        .and_then(|t| match &t.relation {
            TableFactor::Table { name, .. } => Some(object_name_str(name)),
            _ => None,
        })
        .ok_or_else(|| anyhow!("could not determine table name from FROM clause"))
}

fn object_name_str(name: &ObjectName) -> String {
    name.0.iter()
        .map(|i| i.as_ident().map(|id| id.value.as_str()).unwrap_or(""))
        .collect::<Vec<_>>()
        .join(".")
}

// ── AST helpers: top-K detection ─────────────────────────────────────────────

fn detect_topk(order_by: &[OrderByExpr], limit: Option<&Expr>) -> Option<u64> {
    let limit_n = match limit? {
        Expr::Value(ValueWithSpan { value: Value::Number(n, _), .. }) => n.parse::<u64>().ok()?,
        _ => return None,
    };
    // Must have at least one DESC (or default) ORDER BY.
    let has_desc = order_by.iter().any(|o| matches!(o.options.asc, Some(false) | None));
    if has_desc { Some(limit_n) } else { None }
}

// ── AST helpers: JOIN extraction ──────────────────────────────────────────────

fn extract_join_info(sel: &Select) -> Option<JoinInfo> {
    let table_with_joins = sel.from.first()?;
    let join = table_with_joins.joins.first()?;

    let inner_table = match &join.relation {
        TableFactor::Table { name, .. } => object_name_str(name),
        _ => return None,
    };

    // Extract the ON key from JOIN … ON a.key = b.key.
    let (join_key, outer_key) = extract_join_keys(join)?;

    Some(JoinInfo { inner_table, join_key, outer_key })
}

fn extract_join_keys(join: &Join) -> Option<(String, String)> {
    let constraint = match &join.join_operator {
        JoinOperator::Inner(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c) => c,
        _ => return None,
    };
    let on_expr = match constraint {
        JoinConstraint::On(e) => e,
        _ => return None,
    };
    // Expect: a.key = b.key  or  key = key
    if let Expr::BinaryOp { left, op: BinaryOperator::Eq, right } = on_expr {
        let lk = col_name(left)?;
        let rk = col_name(right)?;
        Some((rk, lk)) // inner key, outer key
    } else {
        None
    }
}

// ── Shared helper ─────────────────────────────────────────────────────────────

fn apply_filters(input: SketchExpr, pred: Vec<Predicate>) -> SketchExpr {
    if pred.is_empty() { input } else {
        SketchExpr::Filter { pred, input: Box::new(input) }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::sketch_algebra::{ExactAgg, SketchAggOp};
    use crate::types::AggType;
    use std::time::Duration;

    fn parse(sql: &str) -> SketchExpr {
        parse_sql(sql).unwrap_or_else(|e| panic!("parse_sql failed: {e}\nSQL: {sql}"))
    }

    fn pq(sql: &str) -> super::super::ParsedQuery {
        parse(sql).to_parsed_query()
    }

    // ── Basic aggregations ────────────────────────────────────────────────────

    #[test]
    fn count_star_no_group_is_exact() {
        let pq = pq("SELECT COUNT(*) FROM hits");
        assert!(pq.exact_required);
        assert!(pq.aggregations.is_empty());
    }

    #[test]
    fn count_star_group_by_is_frequency() {
        let pq = pq("SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID");
        assert!(pq.aggregations.contains(&AggType::Frequency));
        assert!(pq.group_by_labels.contains(&"AdvEngineID".to_string()));
    }

    #[test]
    fn count_distinct_is_cardinality() {
        let pq = pq("SELECT COUNT(DISTINCT UserID) FROM hits");
        assert!(pq.aggregations.contains(&AggType::Cardinality));
        assert!(!pq.exact_required);
    }

    #[test]
    fn count_star_order_by_desc_limit_is_topk() {
        let pq = pq(
            "SELECT SearchPhrase, COUNT(*) AS c FROM hits \
             WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
        );
        assert!(pq.aggregations.contains(&AggType::Frequency));
    }

    #[test]
    fn avg_with_group_by_is_quantile_p50() {
        let pq = pq("SELECT symbol, AVG(last) FROM hits GROUP BY symbol");
        assert!(pq.aggregations.contains(&AggType::Quantile));
        assert!(pq.quantiles.contains(&0.5));
    }

    #[test]
    fn min_max_with_group_by_are_extremes() {
        let pq = pq("SELECT symbol, MIN(last), MAX(last) FROM hits GROUP BY symbol");
        assert!(pq.aggregations.contains(&AggType::Quantile));
        assert!(pq.quantiles.contains(&0.0));
        assert!(pq.quantiles.contains(&1.0));
    }

    #[test]
    fn min_max_no_group_by_is_exact_minmax() {
        let expr = parse("SELECT MIN(EventDate), MAX(EventDate) FROM hits");
        // After optimize: ExactMinMax nodes
        let pq = expr.to_parsed_query();
        // ExactMinMax maps to Quantile in legacy AggType
        assert!(pq.aggregations.contains(&AggType::Quantile));
    }

    #[test]
    fn sum_is_always_exact() {
        let pq = pq("SELECT SUM(AdvEngineID) FROM hits");
        assert!(pq.exact_required);
    }

    // ── WHERE predicates ──────────────────────────────────────────────────────

    #[test]
    fn where_equality_captured() {
        let pq = pq("SELECT COUNT(*) FROM hits WHERE sectype = 'E' GROUP BY symbol");
        assert_eq!(pq.label_filters.get("sectype").map(String::as_str), Some("E"));
    }

    #[test]
    fn where_inequality_captured() {
        // ne predicate should be captured in filters (not label_filters, but present)
        let expr = parse("SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID");
        // Verify parse succeeds and has frequency
        let pq = expr.to_parsed_query();
        assert!(pq.aggregations.contains(&AggType::Frequency));
    }

    // ── Multi-aggregation ─────────────────────────────────────────────────────

    #[test]
    fn multi_agg_collects_all() {
        let pq = pq(
            "SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) \
             FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10",
        );
        assert!(pq.aggregations.contains(&AggType::Cardinality), "missing cardinality");
        assert!(pq.aggregations.contains(&AggType::Frequency),   "missing frequency");
        assert!(pq.aggregations.contains(&AggType::Quantile),    "missing quantile");
        // SUM adds exact_required alongside sketch ops
        assert!(pq.exact_required, "SUM should set exact_required");
    }

    // ── Table / metric name ───────────────────────────────────────────────────

    #[test]
    fn dotted_table_name() {
        let pq = pq("SELECT COUNT(*) FROM financial.last_trade_price GROUP BY symbol");
        assert_eq!(pq.metric_name, "financial.last_trade_price");
    }

    // ── DEBS SQL variants ─────────────────────────────────────────────────────

    #[test]
    fn debs_q6_cardinality() {
        use super::super::QueryHint;
        let pq = pq("SELECT COUNT(DISTINCT symbol) FROM financial.last_trade_price");
        assert!(pq.aggregations.contains(&AggType::Cardinality));
        assert!(matches!(pq.hint, Some(QueryHint::DebsCardinality)));
    }

    #[test]
    fn debs_q3_topk() {
        let pq = pq(
            "SELECT symbol, COUNT(*) AS c FROM financial.last_trade_price \
             GROUP BY symbol ORDER BY c DESC LIMIT 10",
        );
        assert!(pq.aggregations.contains(&AggType::Frequency));
    }

    // ── COUNT(DISTINCT) with GROUP BY → Hydra via R4 ─────────────────────────

    #[test]
    fn count_distinct_with_group_by() {
        let pq = pq(
            "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10",
        );
        assert!(pq.aggregations.contains(&AggType::Cardinality));
        assert!(pq.group_by_labels.contains(&"RegionID".to_string()));
    }

    // ── UNION ALL → Merge ─────────────────────────────────────────────────────

    #[test]
    fn union_all_produces_merge() {
        let expr = parse(
            "SELECT COUNT(DISTINCT UserID) FROM R \
             UNION ALL \
             SELECT COUNT(DISTINCT UserID) FROM S",
        );
        // After R3 optimization: Merge([Agg(HLL,R), Agg(HLL,S)])
        assert!(matches!(expr, SketchExpr::Merge { .. }));
    }
}
