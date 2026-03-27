//! PromQL → SketchExpr compiler.
//!
//! Uses the `promql-parser` crate (GreptimeTeam) for a full AST parse, then
//! walks the expression tree to emit a [`SketchExpr`] following the mapping
//! rules in `docs/sketch-algebra-query-mapping.md §2.3`.
//!
//! # PromQL → Sketch mapping (summary)
//!
//! | Expression | SketchAggOp |
//! |---|---|
//! | `quantile_over_time(φ, m[w])` | DDSketch([φ]) |
//! | `histogram_quantile(φ, rate(m[w]))` | DDSketch([φ]) |
//! | `avg_over_time(m[w])` | DDSketch([0.5]) |
//! | `min_over_time(m[w]) by (d)` | DDSketch([0.0]) |
//! | `max_over_time(m[w]) by (d)` | DDSketch([1.0]) |
//! | `min/max_over_time(m[w])` (no by) | ExactMinMax |
//! | `stddev/stdvar_over_time(m[w])` | DDSketch([0.25,0.75]) |
//! | `count_over_time(m[w])` | CountMin |
//! | `sum_over_time(m[w])` | Exact(Sum) |
//! | `last_over_time / delta / deriv / predict_linear` | Exact (stateful) |
//! | `changes / resets` | CountMin |
//! | `topk(k, …)` outer | CountSketch(k) |
//! | `count(…over_time… by (d))` outer | HLL |
//! | `m{filters}` bare | Exact (required) |
//! | `m_a op m_b` binary | Exact (required) |

use std::time::Duration;

use anyhow::anyhow;
use promql_parser::parser::{self, AggregateExpr, Call, Expr, LabelModifier, MatrixSelector, VectorSelector};

use super::sketch_algebra::{
    ColumnRef, FilterOp, FilterVal, PartitionKeys, Predicate, SketchAggOp, SketchExpr, SourceSpec,
};
use super::sketch_rules::optimize;

// ── Public entry point ────────────────────────────────────────────────────────

/// Parse a PromQL expression string into an optimised [`SketchExpr`].
pub fn parse_promql(query: &str) -> anyhow::Result<SketchExpr> {
    let expr = parser::parse(query)
        .map_err(|e| anyhow!("PromQL parse error: {e}"))?;
    let sketch = walk(&expr, WalkCtx::default())?;
    Ok(optimize(sketch))
}

// ── Walk context ──────────────────────────────────────────────────────────────

/// Context accumulated as we descend the AST.
#[derive(Default, Clone)]
struct WalkCtx {
    /// GROUP BY / `without` clause from an outer Aggregate node.
    partition: Option<PartitionKeys>,
    /// Top-K k from an outer `topk` / `bottomk` operator.
    topk: Option<u64>,
    /// Whether the outer context is a `count()` aggregate (→ HLL).
    outer_count: bool,
}

// ── AST walker ────────────────────────────────────────────────────────────────

fn walk(expr: &Expr, ctx: WalkCtx) -> anyhow::Result<SketchExpr> {
    match expr {
        // ── Aggregate operators: topk, count, sum by, avg by, … ───────────
        Expr::Aggregate(agg) => walk_aggregate(agg, ctx),

        // ── Function calls: *_over_time, histogram_quantile, rate, … ──────
        Expr::Call(call) => walk_call(call, ctx),

        // ── Binary operations: metric_a / metric_b → exact ────────────────
        Expr::Binary(bin) => {
            // Binary op between two series: both sides need exact values.
            let left  = walk(bin.lhs.as_ref(), WalkCtx::default())?;
            let right = walk(bin.rhs.as_ref(), WalkCtx::default())?;
            // Wrap both in a Merge that signals exact requirement to callers.
            Ok(SketchExpr::Agg {
                op:    SketchAggOp::Exact(super::sketch_algebra::ExactAgg::Sum), // placeholder
                col:   ColumnRef::SampleValue,
                input: Box::new(SketchExpr::Merge { inputs: vec![left, right] }),
            })
        }

        // ── Parenthesised ─────────────────────────────────────────────────
        Expr::Paren(p) => walk(p.expr.as_ref(), ctx),

        // ── Subquery: metric[5m:1m] — treat as windowed frequency ─────────
        Expr::Subquery(sq) => {
            let inner = walk(sq.expr.as_ref(), ctx.clone())?;
            Ok(SketchExpr::Window { duration: sq.range, input: Box::new(inner) })
        }

        // ── Bare vector selector ──────────────────────────────────────────
        Expr::VectorSelector(vs) => {
            let (name, filters) = extract_vs_info(vs);
            let source = SketchExpr::Source(SourceSpec { name });
            let filtered = apply_filters(source, filters);
            // Wrap with partition and exact agg.
            let agg = SketchExpr::Agg {
                op:    SketchAggOp::Exact(super::sketch_algebra::ExactAgg::Sum),
                col:   ColumnRef::SampleValue,
                input: Box::new(filtered),
            };
            Ok(apply_partition(agg, ctx.partition))
        }

        // ── Number / string literals — only appear as args inside Call ────
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) => {
            Err(anyhow!("unexpected literal at top level of PromQL expression"))
        }

        // ── Extension / unknown ───────────────────────────────────────────
        #[allow(unreachable_patterns)]
        _ => Err(anyhow!("unsupported PromQL expression type")),
    }
}

// ── Aggregate operator walk ───────────────────────────────────────────────────

fn walk_aggregate(
    agg: &AggregateExpr,
    ctx: WalkCtx,
) -> anyhow::Result<SketchExpr> {
    // Extract partition keys from the by/without modifier.
    let partition = agg.modifier.as_ref().map(modifier_to_partition);

    // Extract the operator name from the token via Display (gives lowercase).
    let op_name = format!("{}", agg.op);

    match op_name.as_str() {
        // topk(k, inner) / bottomk(k, inner) → CountSketch(k)
        "topk" | "bottomk" => {
            let k = extract_number_param(&agg.param)? as u64;
            let inner_ctx = WalkCtx { partition: partition.clone(), topk: Some(k), outer_count: false };
            let inner = walk(agg.expr.as_ref(), inner_ctx)?;
            let result = SketchExpr::TopK { k, input: Box::new(inner) };
            Ok(apply_partition(result, partition))
        }

        // count(inner_by) → HLL cardinality of distinct groups
        "count" => {
            let inner_ctx = WalkCtx { partition: partition.clone(), topk: None, outer_count: true };
            let inner = walk(agg.expr.as_ref(), inner_ctx)?;
            // Wrap with HLL at this level.
            let result = SketchExpr::Agg {
                op:    SketchAggOp::default_hll(),
                col:   ColumnRef::SampleValue,
                input: Box::new(inner),
            };
            Ok(apply_partition(result, partition))
        }

        // sum by (d) (inner) — outer sum doesn't change the inner sketch type.
        // The inner *_over_time already chose the right sketch; we just set partition.
        "sum" | "avg" | "min" | "max" | "group" => {
            let inner_ctx = WalkCtx { partition: partition.clone(), topk: ctx.topk, outer_count: false };
            let inner = walk(agg.expr.as_ref(), inner_ctx)?;
            Ok(apply_partition(inner, partition))
        }

        // stddev by (d) / stdvar by (d) → DDSketch IQR proxy
        "stddev" | "stdvar" => {
            let inner_ctx = WalkCtx { partition: partition.clone(), topk: None, outer_count: false };
            let inner = walk(agg.expr.as_ref(), inner_ctx)?;
            let result = SketchExpr::Agg {
                op:    SketchAggOp::default_ddsketch(vec![0.25, 0.75]),
                col:   ColumnRef::SampleValue,
                input: Box::new(inner),
            };
            Ok(apply_partition(result, partition))
        }

        // quantile(φ, inner) → DDSketch(φ)
        "quantile" => {
            let phi = extract_number_param(&agg.param)?;
            let inner_ctx = WalkCtx { partition: partition.clone(), topk: None, outer_count: false };
            let inner = walk(agg.expr.as_ref(), inner_ctx)?;
            let result = SketchExpr::Agg {
                op:    SketchAggOp::default_ddsketch(vec![phi]),
                col:   ColumnRef::SampleValue,
                input: Box::new(inner),
            };
            Ok(apply_partition(result, partition))
        }

        other => Err(anyhow!("unsupported PromQL aggregate operator: {other}")),
    }
}

// ── Function call walk ────────────────────────────────────────────────────────

fn walk_call(
    call: &Call,
    ctx: WalkCtx,
) -> anyhow::Result<SketchExpr> {
    let name = call.func.name;

    match name {
        // ── quantile_over_time(φ, m{f}[w]) ───────────────────────────────
        "quantile_over_time" => {
            let phi = extract_call_num_arg(call, 0)?;
            let (source, filters, window) = extract_matrix_arg(call, 1)?;
            Ok(build_sketched(
                source, filters, window,
                SketchAggOp::default_ddsketch(vec![phi]),
                ctx,
            ))
        }

        // ── histogram_quantile(φ, rate(m{f}[w])) ─────────────────────────
        "histogram_quantile" => {
            let phi = extract_call_num_arg(call, 0)?;
            // The second arg is a call to rate/irate wrapping a MatrixSelector.
            let rate_expr = call.args.args[1].as_ref();
            let (source, filters, window) = extract_inner_matrix(rate_expr)?;
            Ok(build_sketched(
                source, filters, window,
                SketchAggOp::default_ddsketch(vec![phi]),
                ctx,
            ))
        }

        // ── avg_over_time ─────────────────────────────────────────────────
        "avg_over_time" => {
            let (source, filters, window) = extract_matrix_arg(call, 0)?;
            Ok(build_sketched(source, filters, window, SketchAggOp::default_ddsketch(vec![0.5]), ctx))
        }

        // ── min_over_time ─────────────────────────────────────────────────
        "min_over_time" => {
            let (source, filters, window) = extract_matrix_arg(call, 0)?;
            let op = if ctx.partition.as_ref().map(|p| !p.is_empty()).unwrap_or(false) {
                SketchAggOp::default_ddsketch(vec![0.0])
            } else {
                SketchAggOp::ExactMinMax { min: true, max: false }
            };
            Ok(build_sketched(source, filters, window, op, ctx))
        }

        // ── max_over_time ─────────────────────────────────────────────────
        "max_over_time" => {
            let (source, filters, window) = extract_matrix_arg(call, 0)?;
            let op = if ctx.partition.as_ref().map(|p| !p.is_empty()).unwrap_or(false) {
                SketchAggOp::default_ddsketch(vec![1.0])
            } else {
                SketchAggOp::ExactMinMax { min: false, max: true }
            };
            Ok(build_sketched(source, filters, window, op, ctx))
        }

        // ── stddev_over_time / stdvar_over_time → IQR proxy ──────────────
        "stddev_over_time" | "stdvar_over_time" => {
            let (source, filters, window) = extract_matrix_arg(call, 0)?;
            Ok(build_sketched(
                source, filters, window,
                SketchAggOp::default_ddsketch(vec![0.25, 0.75]),
                ctx,
            ))
        }

        // ── count_over_time ───────────────────────────────────────────────
        "count_over_time" => {
            let (source, filters, window) = extract_matrix_arg(call, 0)?;
            // Outer count() context → HLL (cardinality of distinct groups).
            let op = if ctx.outer_count {
                SketchAggOp::default_hll()
            } else {
                SketchAggOp::default_count_min()
            };
            Ok(build_sketched(source, filters, window, op, ctx))
        }

        // ── sum_over_time ─────────────────────────────────────────────────
        "sum_over_time" => {
            let (source, filters, window) = extract_matrix_arg(call, 0)?;
            Ok(build_sketched(
                source, filters, window,
                SketchAggOp::Exact(super::sketch_algebra::ExactAgg::Sum),
                ctx,
            ))
        }

        // ── last_over_time / stateful functions → exact ───────────────────
        "last_over_time" | "present_over_time" | "absent_over_time" => {
            let (source, filters, window) = extract_matrix_arg(call, 0)?;
            Ok(build_sketched(
                source, filters, window,
                SketchAggOp::Exact(super::sketch_algebra::ExactAgg::Sum),
                ctx,
            ))
        }

        // ── delta / idelta / deriv / predict_linear → stateful exact ──────
        "delta" | "idelta" | "deriv" | "predict_linear" => {
            let (source, filters, window) = extract_matrix_arg(call, 0)?;
            Ok(build_sketched(
                source, filters, window,
                SketchAggOp::Exact(super::sketch_algebra::ExactAgg::Sum),
                ctx,
            ))
        }

        // ── changes / resets → frequency ──────────────────────────────────
        "changes" | "resets" => {
            let (source, filters, window) = extract_matrix_arg(call, 0)?;
            Ok(build_sketched(source, filters, window, SketchAggOp::default_count_min(), ctx))
        }

        // ── rate / irate / increase — pass-through, inner carries the sketch
        "rate" | "irate" | "increase" => {
            if call.args.is_empty() {
                return Err(anyhow!("rate/irate/increase requires a matrix arg"));
            }
            let (source, filters, window) = extract_inner_matrix(call.args.args[0].as_ref())?;
            Ok(build_sketched(
                source, filters, window,
                SketchAggOp::default_count_min(),
                ctx,
            ))
        }

        other => Err(anyhow!("unsupported PromQL function: {other}")),
    }
}

// ── Helpers: MatrixSelector extraction ───────────────────────────────────────

/// Extract `(metric_name, filters, window)` from a MatrixSelector argument at
/// position `arg_idx` of a Call.
fn extract_matrix_arg(
    call: &Call,
    arg_idx: usize,
) -> anyhow::Result<(String, Vec<Predicate>, Duration)> {
    let arg = call.args.args.get(arg_idx)
        .map(|b| b.as_ref())
        .ok_or_else(|| anyhow!("missing arg {} in call to {}", arg_idx, call.func.name))?;
    extract_inner_matrix(arg)
}

/// Walk into an expression until we find a MatrixSelector, then extract its info.
fn extract_inner_matrix(expr: &Expr) -> anyhow::Result<(String, Vec<Predicate>, Duration)> {
    match expr {
        Expr::MatrixSelector(ms) => {
            let (name, filters) = extract_vs_info(&ms.vs);
            Ok((name, filters, ms.range))
        }
        Expr::Paren(p) => extract_inner_matrix(p.expr.as_ref()),
        Expr::Call(c) => {
            // rate/irate wraps a MatrixSelector.
            extract_inner_matrix(c.args.args[0].as_ref())
        }
        other => Err(anyhow!("expected MatrixSelector, got {:?}", std::mem::discriminant(other))),
    }
}

// ── Helpers: VectorSelector info ─────────────────────────────────────────────

fn extract_vs_info(vs: &VectorSelector) -> (String, Vec<Predicate>) {
    // Metric name: prefer the explicit name field, fall back to __name__ matcher.
    let name = vs.name.clone().unwrap_or_else(|| {
        vs.matchers.matchers.iter()
            .find(|m| m.name == "__name__")
            .map(|m| m.value.clone())
            .unwrap_or_default()
    });

    let filters = vs.matchers.matchers.iter()
        .filter(|m| m.name != "__name__")
        .filter_map(matcher_to_predicate)
        .collect();

    (name, filters)
}

fn matcher_to_predicate(m: &promql_parser::label::Matcher) -> Option<Predicate> {
    use promql_parser::label::MatchOp;
    let (op, val) = match &m.op {
        MatchOp::Equal    => (FilterOp::Eq, FilterVal::Str(m.value.clone())),
        MatchOp::NotEqual => (FilterOp::Ne, FilterVal::Str(m.value.clone())),
        MatchOp::Re(re)   => (FilterOp::Regex(re.to_string()), FilterVal::Str(m.value.clone())),
        MatchOp::NotRe(re)=> (FilterOp::NotRegex(re.to_string()), FilterVal::Str(m.value.clone())),
    };
    Some(Predicate { col: m.name.clone(), op, val })
}

// ── Helpers: number extraction ────────────────────────────────────────────────

fn extract_call_num_arg(call: &Call, idx: usize) -> anyhow::Result<f64> {
    match call.args.args.get(idx).map(|b| b.as_ref()) {
        Some(Expr::NumberLiteral(n)) => Ok(n.val),
        Some(other) => Err(anyhow!(
            "expected number at arg {} of {}, got {:?}",
            idx, call.func.name, std::mem::discriminant(other)
        )),
        None => Err(anyhow!("missing arg {} in {}", idx, call.func.name)),
    }
}

fn extract_number_param(param: &Option<Box<Expr>>) -> anyhow::Result<f64> {
    match param {
        Some(e) => match e.as_ref() {
            Expr::NumberLiteral(n) => Ok(n.val),
            other => Err(anyhow!("expected number param, got {:?}", std::mem::discriminant(other))),
        },
        None => Err(anyhow!("missing required numeric parameter")),
    }
}

// ── Helpers: PartitionKeys from LabelModifier ─────────────────────────────────

fn modifier_to_partition(modifier: &LabelModifier) -> PartitionKeys {
    match modifier {
        LabelModifier::Include(labels) => PartitionKeys::By(labels.labels.clone()),
        LabelModifier::Exclude(labels) => PartitionKeys::Without(labels.labels.clone()),
    }
}

// ── Tree builders ─────────────────────────────────────────────────────────────

/// Build the standard `Partition(Window(Filter(Agg(Source))))` tree.
fn build_sketched(
    metric:  String,
    filters: Vec<Predicate>,
    window:  Duration,
    op:      SketchAggOp,
    ctx:     WalkCtx,
) -> SketchExpr {
    let source   = SketchExpr::Source(SourceSpec { name: metric });
    let filtered = apply_filters(source, filters);
    let windowed = SketchExpr::Window { duration: window, input: Box::new(filtered) };

    let agg = if let Some(k) = ctx.topk {
        // Outer topk → CountSketch regardless of what op was chosen.
        SketchExpr::Agg {
            op:    SketchAggOp::CountSketch { k },
            col:   ColumnRef::SampleValue,
            input: Box::new(windowed),
        }
    } else {
        SketchExpr::Agg { op, col: ColumnRef::SampleValue, input: Box::new(windowed) }
    };

    apply_partition(agg, ctx.partition)
}

fn apply_filters(input: SketchExpr, pred: Vec<Predicate>) -> SketchExpr {
    if pred.is_empty() {
        input
    } else {
        SketchExpr::Filter { pred, input: Box::new(input) }
    }
}

fn apply_partition(input: SketchExpr, partition: Option<PartitionKeys>) -> SketchExpr {
    match partition {
        None => input,
        Some(p) if p.is_empty() => input,
        Some(keys) => SketchExpr::Partition { keys, input: Box::new(input) },
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::sketch_algebra::{ExactAgg, SketchAggOp};
    use crate::types::AggType;

    fn parse(q: &str) -> SketchExpr {
        parse_promql(q).unwrap_or_else(|e| panic!("parse_promql failed: {e}\nquery={q:?}"))
    }

    fn pq(q: &str) -> super::super::ParsedQuery {
        parse(q).to_parsed_query()
    }

    // ── quantile_over_time ────────────────────────────────────────────────────

    #[test]
    fn quantile_over_time_basic() {
        // PromQL: `by` is part of the aggregate operator, not the function call.
        let pq = pq("sum by (host) (quantile_over_time(0.99, latency{service=\"web\"}[5m]))");
        assert_eq!(pq.aggregations, vec![AggType::Quantile]);
        assert_eq!(pq.quantiles, vec![0.99]);
        assert_eq!(pq.group_by_labels, vec!["host"]);
        assert_eq!(pq.label_filters.get("service").map(String::as_str), Some("web"));
        assert_eq!(pq.time_window, Duration::from_secs(300));
    }

    #[test]
    fn quantile_over_time_debs_ema() {
        // Dotted names are invalid PromQL; use underscores.
        let pq = pq("sum by (symbol) (quantile_over_time(0.5, financial_last_trade_price[5m]))");
        assert_eq!(pq.aggregations, vec![AggType::Quantile]);
        assert_eq!(pq.quantiles, vec![0.5]);
        assert_eq!(pq.group_by_labels, vec!["symbol"]);
    }

    // ── histogram_quantile ────────────────────────────────────────────────────

    #[test]
    fn histogram_quantile_via_rate() {
        let pq = pq("histogram_quantile(0.95, rate(http_duration_seconds_bucket[5m]))");
        assert_eq!(pq.aggregations, vec![AggType::Quantile]);
        assert_eq!(pq.quantiles, vec![0.95]);
    }

    // ── avg_over_time ─────────────────────────────────────────────────────────

    #[test]
    fn avg_over_time_maps_to_p50() {
        let pq = pq("avg by (symbol) (avg_over_time(financial_last_trade_price[5m]))");
        assert_eq!(pq.aggregations, vec![AggType::Quantile]);
        assert_eq!(pq.quantiles, vec![0.5]);
    }

    // ── min/max_over_time ─────────────────────────────────────────────────────

    #[test]
    fn min_over_time_with_by_is_ddsketch() {
        let pq = pq("min by (symbol) (min_over_time(financial_last_trade_price[5m]))");
        assert_eq!(pq.aggregations, vec![AggType::Quantile]);
        assert_eq!(pq.quantiles, vec![0.0]);
    }

    #[test]
    fn max_over_time_with_by_is_ddsketch() {
        let pq = pq("max by (symbol) (max_over_time(financial_last_trade_price[5m]))");
        assert_eq!(pq.aggregations, vec![AggType::Quantile]);
        assert_eq!(pq.quantiles, vec![1.0]);
    }

    // ── topk ──────────────────────────────────────────────────────────────────

    #[test]
    fn topk_count_over_time() {
        let pq = pq("topk by (symbol) (10, count_over_time(financial_last_trade_price[5m]))");
        assert_eq!(pq.aggregations, vec![AggType::Frequency]);
        assert_eq!(pq.group_by_labels, vec!["symbol"]);
    }

    #[test]
    fn topk_avg_over_time() {
        let expr = parse("topk by (host) (5, avg_over_time(cpu[5m]))");
        let pq   = expr.to_parsed_query();
        assert_eq!(pq.aggregations, vec![AggType::Frequency]);
    }

    // ── count cardinality ─────────────────────────────────────────────────────

    #[test]
    fn count_count_over_time_is_hll() {
        let pq = pq("count by (symbol) (count_over_time(financial_last_trade_price[5m]))");
        assert_eq!(pq.aggregations, vec![AggType::Cardinality]);
    }

    // ── stddev_over_time ──────────────────────────────────────────────────────

    #[test]
    fn stddev_over_time_iqr_proxy() {
        let pq = pq("avg by (host) (stddev_over_time(cpu[5m]))");
        assert_eq!(pq.aggregations, vec![AggType::Quantile]);
        assert!(pq.quantiles.contains(&0.25) && pq.quantiles.contains(&0.75));
    }

    // ── sum_over_time → exact ─────────────────────────────────────────────────

    #[test]
    fn sum_over_time_exact() {
        let pq = pq("sum by (service) (sum_over_time(request_bytes[1h]))");
        assert!(pq.exact_required);
    }

    // ── label filters ─────────────────────────────────────────────────────────

    #[test]
    fn label_eq_filter() {
        let pq = pq(r#"sum by (service) (count_over_time(hits{env="prod"}[5m]))"#);
        assert_eq!(pq.label_filters.get("env").map(String::as_str), Some("prod"));
    }

    // ── duration parsing ──────────────────────────────────────────────────────

    #[test]
    fn duration_1h() {
        let pq = pq("avg by (host) (avg_over_time(cpu[1h]))");
        assert_eq!(pq.time_window, Duration::from_secs(3600));
    }

    // ── DEBS hints ────────────────────────────────────────────────────────────

    #[test]
    fn debs_price_stats_min() {
        use super::super::QueryHint;
        let pq = pq("min by (symbol) (min_over_time(financial_last_trade_price[5m]))");
        assert!(matches!(pq.hint, Some(QueryHint::DebsPriceStats)));
    }

    #[test]
    fn debs_cardinality() {
        use super::super::QueryHint;
        let pq = pq("count by (symbol) (count_over_time(financial_last_trade_price[5m]))");
        assert!(matches!(pq.hint, Some(QueryHint::DebsCardinality)));
    }
}
