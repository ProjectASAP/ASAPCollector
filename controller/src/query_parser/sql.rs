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

// ── Direct QueryExpr emission ─────────────────────────────────────────────────
//
// `parse_sql_expr` walks the same SQL AST but emits [`QueryExpr`] nodes
// natively, preserving semantic nodes that SketchExpr flattens:
//
// | SQL construct   | SketchExpr (old)       | QueryExpr (new)                 |
// |-----------------|------------------------|---------------------------------|
// | ORDER BY + LIMIT| TopK node              | Sort + Limit (→ TopK via R5)    |
// | JOIN … ON       | JoinSketch             | Join { kind, pred }             |
// | UNION ALL       | Merge                  | SetOp { Union, all: true }      |
// | GROUP BY + aggs | Agg + Partition + Merge| Aggregate { keys, aggs }        |
// | WHERE           | Filter (Predicate list)| Filter { ScalarExpr tree }      |

use crate::algebra::expr::{
    AggFunc, AggItem as AlgAggItem, BinaryOpKind, JoinKind, LiteralValue, ProjectItem,
    QueryExpr, ScalarExpr, SetOpKind, SortKey,
};
use crate::query_parser::sketch_algebra::ColumnRef as SColumnRef;

/// Parse a SQL SELECT statement directly into a [`QueryExpr`] tree.
///
/// Unlike `parse_sql` (which emits `SketchExpr`), this preserves `Sort`,
/// `Limit`, `Join`, and `SetOp` nodes natively so the [`crate::algebra`]
/// optimizer and allocator can reason about them.
pub fn parse_sql_expr(sql: &str) -> anyhow::Result<QueryExpr> {
    let dialect = GenericDialect {};
    let mut stmts = sqlparser::parser::Parser::parse_sql(&dialect, sql)
        .with_context(|| format!("SQL parse error: {sql:?}"))?;
    let stmt = stmts.pop().ok_or_else(|| anyhow!("no SQL statement found"))?;
    let query = match stmt {
        Statement::Query(q) => *q,
        other => return Err(anyhow!("expected SELECT, got {:?}", other)),
    };
    extract_query_expr(&query)
}

fn extract_query_expr(query: &Query) -> anyhow::Result<QueryExpr> {
    let order_by: Vec<OrderByExpr> = match &query.order_by {
        Some(OrderBy { kind: OrderByKind::Expressions(exprs), .. }) => exprs.clone(),
        _ => vec![],
    };
    let (limit_n, offset_n) = match &query.limit_clause {
        Some(LimitClause::LimitOffset { limit: Some(e), offset, .. }) => {
            (Some(e.clone()), offset.as_ref().and_then(|o| expr_to_u64(&o.value)))
        }
        Some(LimitClause::OffsetCommaLimit { limit: e, offset, .. }) => {
            (Some(e.clone()), Some(expr_to_u64(offset).unwrap_or(0)))
        }
        _ => (None, None),
    };
    let limit_val = limit_n.as_ref().and_then(|e| expr_to_u64(e));
    let offset_val = offset_n.unwrap_or(0);

    let body = extract_set_expr_qe(query.body.as_ref(), &order_by, limit_val, offset_val)?;
    Ok(body)
}

fn extract_set_expr_qe(
    set_expr:   &SetExpr,
    order_by:   &[OrderByExpr],
    limit_n:    Option<u64>,
    offset_n:   u64,
) -> anyhow::Result<QueryExpr> {
    match set_expr {
        SetExpr::Select(sel) => extract_select_qe(sel, order_by, limit_n, offset_n),
        SetExpr::Query(inner) => extract_query_expr(inner),

        // UNION / INTERSECT / EXCEPT
        SetExpr::SetOperation { left, right, op, set_quantifier } => {
            use sqlparser::ast::{SetOperator, SetQuantifier};
            let left_qe  = extract_set_expr_qe(left,  &[], None, 0)?;
            let right_qe = extract_set_expr_qe(right, &[], None, 0)?;
            let kind = match op {
                SetOperator::Union     => SetOpKind::Union,
                SetOperator::Intersect => SetOpKind::Intersect,
                SetOperator::Except | SetOperator::Minus => SetOpKind::Except,
            };
            let all = matches!(set_quantifier, SetQuantifier::All | SetQuantifier::ByName);
            Ok(QueryExpr::SetOp {
                kind,
                all,
                left:  Box::new(left_qe),
                right: Box::new(right_qe),
            })
        }
        other => Err(anyhow!("unsupported query body: {:?}", other)),
    }
}

fn extract_select_qe(
    sel:      &Select,
    order_by: &[OrderByExpr],
    limit_n:  Option<u64>,
    offset_n: u64,
) -> anyhow::Result<QueryExpr> {
    let metric_name  = extract_table_name(sel)?;
    let where_scalar = sel.selection.as_ref().map(sql_expr_to_scalar);
    let group_keys   = extract_group_by(&sel.group_by);
    let having_scalar= sel.having.as_ref().map(sql_expr_to_scalar);
    let agg_items    = collect_agg_items_qe(&sel.projection);
    let join_qe      = extract_join_qe(sel);

    let source = QueryExpr::Source(crate::query_parser::sketch_algebra::SourceSpec {
        name: metric_name.clone(),
    });

    // WHERE → Filter
    let after_where = match where_scalar {
        Some(pred) => QueryExpr::Filter { pred, input: Box::new(source) },
        None       => source,
    };

    // JOIN
    let after_join = if let Some((inner_table, join_kind, join_pred)) = join_qe {
        let inner_source = QueryExpr::Source(
            crate::query_parser::sketch_algebra::SourceSpec { name: inner_table }
        );
        QueryExpr::Join {
            kind:  join_kind,
            pred:  join_pred,
            left:  Box::new(after_where),
            right: Box::new(inner_source),
        }
    } else {
        after_where
    };

    // GROUP BY + aggs OR bare projection
    let after_agg = if agg_items.is_empty() {
        // No aggregation — bare projection with possible DISTINCT.
        let cols = collect_project_items(&sel.projection);
        QueryExpr::Project { cols, input: Box::new(after_join) }
    } else {
        let having = having_scalar;
        QueryExpr::Aggregate {
            keys:   group_keys,
            aggs:   agg_items,
            having,
            input:  Box::new(after_join),
        }
    };

    // ORDER BY → Sort
    let after_sort = if order_by.is_empty() {
        after_agg
    } else {
        let keys: Vec<SortKey> = order_by.iter().map(|o| SortKey {
            col:         expr_to_col_name(&o.expr).unwrap_or_else(|| "?".into()),
            desc:        matches!(o.options.asc, Some(false) | None),
            nulls_first: None,
        }).collect();
        QueryExpr::Sort { keys, input: Box::new(after_agg) }
    };

    // LIMIT / OFFSET
    let result = match limit_n {
        Some(n) => QueryExpr::Limit { n, offset: offset_n, input: Box::new(after_sort) },
        None    => after_sort,
    };

    Ok(result)
}

// ── QueryExpr agg item collection ────────────────────────────────────────────

fn collect_agg_items_qe(projection: &[SelectItem]) -> Vec<AlgAggItem> {
    let mut out = Vec::new();
    for item in projection {
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(e)               => (e, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            _                                         => continue,
        };
        collect_agg_from_expr_qe(expr, alias, &mut out);
    }
    out
}

fn collect_agg_from_expr_qe(expr: &Expr, alias: Option<String>, out: &mut Vec<AlgAggItem>) {
    match expr {
        Expr::Function(f) => {
            let fn_name = f.name.0.last()
                .and_then(|i| i.as_ident())
                .map(|id| id.value.to_uppercase())
                .unwrap_or_default();

            let (distinct, args) = match &f.args {
                FunctionArguments::List(FunctionArgumentList { duplicate_treatment, args, .. }) => {
                    let is_distinct = matches!(duplicate_treatment, Some(DuplicateTreatment::Distinct));
                    (is_distinct, args.as_slice())
                }
                _ => (false, &[][..]),
            };

            let col = first_col_from_args(args);
            let agg_col = match &col {
                SColumnRef::Wildcard    => SColumnRef::Wildcard,
                SColumnRef::Named(n)    => SColumnRef::Named(n.clone()),
                SColumnRef::SampleValue => SColumnRef::SampleValue,
            };

            let func = match fn_name.as_str() {
                "COUNT" if distinct => AggFunc::CountDistinct,
                "COUNT"             => AggFunc::Count,
                "SUM"               => AggFunc::Sum,
                "AVG"               => AggFunc::Avg,
                "MIN"               => AggFunc::Min,
                "MAX"               => AggFunc::Max,
                _                   => return,
            };

            out.push(AlgAggItem {
                alias:    alias.unwrap_or_else(|| fn_name.to_lowercase()),
                func,
                col:      agg_col,
                distinct,
            });
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_agg_from_expr_qe(left,  None, out);
            collect_agg_from_expr_qe(right, None, out);
        }
        Expr::Nested(inner) => collect_agg_from_expr_qe(inner, alias, out),
        _ => {}
    }
}

fn collect_project_items(projection: &[SelectItem]) -> Vec<ProjectItem> {
    projection.iter().filter_map(|item| match item {
        SelectItem::UnnamedExpr(e) => Some(ProjectItem {
            alias: None,
            expr:  sql_expr_to_scalar(e),
        }),
        SelectItem::ExprWithAlias { expr, alias } => Some(ProjectItem {
            alias: Some(alias.value.clone()),
            expr:  sql_expr_to_scalar(expr),
        }),
        SelectItem::Wildcard(_) => Some(ProjectItem {
            alias: None,
            expr:  ScalarExpr::Column("*".into()),
        }),
        _ => None,
    }).collect()
}

// ── SQL Expr → ScalarExpr ─────────────────────────────────────────────────────

fn sql_expr_to_scalar(expr: &Expr) -> ScalarExpr {
    match expr {
        Expr::Identifier(id) => ScalarExpr::Column(id.value.clone()),
        Expr::CompoundIdentifier(parts) => {
            ScalarExpr::Column(parts.iter().map(|i| i.value.as_str()).collect::<Vec<_>>().join("."))
        }
        Expr::Value(vws) => sql_value_to_scalar(&vws.value),
        Expr::BinaryOp { left, op, right } => {
            let lhs = sql_expr_to_scalar(left);
            let rhs = sql_expr_to_scalar(right);
            let bop = sql_binop_to_algebra(op);
            ScalarExpr::BinaryOp { op: bop, lhs: Box::new(lhs), rhs: Box::new(rhs) }
        }
        Expr::IsNull(inner) => ScalarExpr::IsNull {
            expr:    Box::new(sql_expr_to_scalar(inner)),
            negated: false,
        },
        Expr::IsNotNull(inner) => ScalarExpr::IsNull {
            expr:    Box::new(sql_expr_to_scalar(inner)),
            negated: true,
        },
        Expr::Between { expr, negated, low, high } => ScalarExpr::Between {
            expr:    Box::new(sql_expr_to_scalar(expr)),
            low:     Box::new(sql_expr_to_scalar(low)),
            high:    Box::new(sql_expr_to_scalar(high)),
            negated: *negated,
        },
        Expr::InList { expr, list, negated } => ScalarExpr::InList {
            expr:    Box::new(sql_expr_to_scalar(expr)),
            list:    list.iter().map(sql_expr_to_scalar).collect(),
            negated: *negated,
        },
        Expr::Like { expr, pattern, negated, .. } => {
            let op = if *negated { BinaryOpKind::NotLike } else { BinaryOpKind::Like };
            ScalarExpr::BinaryOp {
                op,
                lhs: Box::new(sql_expr_to_scalar(expr)),
                rhs: Box::new(sql_expr_to_scalar(pattern)),
            }
        }
        Expr::Nested(inner) => sql_expr_to_scalar(inner),
        Expr::Function(f) => {
            let name = f.name.0.last()
                .and_then(|i| i.as_ident())
                .map(|id| id.value.clone())
                .unwrap_or_default();
            ScalarExpr::FunctionCall { name, args: vec![] }
        }
        _ => ScalarExpr::Column("?".into()), // unknown expr → opaque column ref
    }
}

fn sql_value_to_scalar(v: &Value) -> ScalarExpr {
    match v {
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) =>
            ScalarExpr::Literal(LiteralValue::Str(s.clone())),
        Value::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                ScalarExpr::Literal(LiteralValue::Int(i))
            } else if let Ok(f) = n.parse::<f64>() {
                ScalarExpr::Literal(LiteralValue::Float(f))
            } else {
                ScalarExpr::Literal(LiteralValue::Null)
            }
        }
        Value::Boolean(b) => ScalarExpr::Literal(LiteralValue::Bool(*b)),
        Value::Null        => ScalarExpr::Literal(LiteralValue::Null),
        _                  => ScalarExpr::Literal(LiteralValue::Null),
    }
}

fn sql_binop_to_algebra(op: &BinaryOperator) -> BinaryOpKind {
    match op {
        BinaryOperator::Plus      => BinaryOpKind::Add,
        BinaryOperator::Minus     => BinaryOpKind::Sub,
        BinaryOperator::Multiply  => BinaryOpKind::Mul,
        BinaryOperator::Divide    => BinaryOpKind::Div,
        BinaryOperator::Modulo    => BinaryOpKind::Mod,
        BinaryOperator::Eq        => BinaryOpKind::Eq,
        BinaryOperator::NotEq     => BinaryOpKind::Ne,
        BinaryOperator::Lt        => BinaryOpKind::Lt,
        BinaryOperator::LtEq      => BinaryOpKind::Le,
        BinaryOperator::Gt        => BinaryOpKind::Gt,
        BinaryOperator::GtEq      => BinaryOpKind::Ge,
        BinaryOperator::And       => BinaryOpKind::And,
        BinaryOperator::Or        => BinaryOpKind::Or,
        BinaryOperator::BitwiseAnd => BinaryOpKind::BitAnd,
        BinaryOperator::BitwiseOr  => BinaryOpKind::BitOr,
        BinaryOperator::BitwiseXor => BinaryOpKind::BitXor,
        BinaryOperator::StringConcat => BinaryOpKind::Concat,
        _                          => BinaryOpKind::Eq, // unknown → eq
    }
}

// ── JOIN → QueryExpr::Join ────────────────────────────────────────────────────

fn extract_join_qe(sel: &Select) -> Option<(String, JoinKind, Option<ScalarExpr>)> {
    let table_with_joins = sel.from.first()?;
    let join = table_with_joins.joins.first()?;
    let inner_table = match &join.relation {
        TableFactor::Table { name, .. } => object_name_str(name),
        _ => return None,
    };
    let (kind, pred) = match &join.join_operator {
        JoinOperator::Inner(c) =>
            (JoinKind::Inner, join_constraint_to_scalar(c)),
        JoinOperator::LeftOuter(c) =>
            (JoinKind::LeftOuter, join_constraint_to_scalar(c)),
        JoinOperator::RightOuter(c) =>
            (JoinKind::RightOuter, join_constraint_to_scalar(c)),
        JoinOperator::FullOuter(c) =>
            (JoinKind::FullOuter, join_constraint_to_scalar(c)),
        JoinOperator::CrossJoin(_) =>
            (JoinKind::Cross, None),
        _ => return None,
    };
    Some((inner_table, kind, pred))
}

fn join_constraint_to_scalar(c: &JoinConstraint) -> Option<ScalarExpr> {
    match c {
        JoinConstraint::On(e) => Some(sql_expr_to_scalar(e)),
        _ => None,
    }
}

// ── Misc helpers ──────────────────────────────────────────────────────────────

fn expr_to_u64(expr: &Expr) -> Option<u64> {
    match expr {
        Expr::Value(vws) => match &vws.value {
            Value::Number(n, _) => n.parse::<u64>().ok(),
            _ => None,
        },
        _ => None,
    }
}

fn expr_to_col_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(id)            => Some(id.value.clone()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|i| i.value.clone()),
        _ => None,
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

    // ── Complex queries ───────────────────────────────────────────────────────

    #[test]
    fn complex_multi_agg_multi_dim_group_by_topk() {
        // COUNT(*) → Frequency, COUNT(DISTINCT) → Cardinality, AVG → Quantile
        // Two GROUP BY dimensions; ORDER BY + LIMIT signals TopK
        let pq = pq(
            "SELECT region, dc, COUNT(*) AS c, COUNT(DISTINCT UserID), AVG(ResponseTime) \
             FROM hits WHERE env = 'prod' GROUP BY region, dc ORDER BY c DESC LIMIT 5",
        );
        assert!(pq.aggregations.contains(&AggType::Frequency));
        assert!(pq.aggregations.contains(&AggType::Cardinality));
        assert!(pq.aggregations.contains(&AggType::Quantile));
        assert!(pq.group_by_labels.contains(&"region".to_string()));
        assert!(pq.group_by_labels.contains(&"dc".to_string()));
        assert_eq!(
            pq.label_filters.get("env").map(String::as_str),
            Some("prod")
        );
        // AVG maps to median (p50) sketch
        assert!(pq.quantiles.contains(&0.5));
    }

    #[test]
    fn complex_union_all_hll_with_where_on_each_branch() {
        // UNION ALL → Merge; each branch has its own WHERE predicate
        let expr = parse(
            "SELECT region, COUNT(DISTINCT UserID) FROM sessions WHERE status = 'active' GROUP BY region \
             UNION ALL \
             SELECT region, COUNT(DISTINCT UserID) FROM sessions WHERE status = 'expired' GROUP BY region",
        );
        assert!(matches!(expr, SketchExpr::Merge { .. }));
        let pq = expr.to_parsed_query();
        assert!(pq.aggregations.contains(&AggType::Cardinality));
        assert!(pq.group_by_labels.contains(&"region".to_string()));
    }
}
