//! PromQL query parser for SP-1 workload extraction.
//!
//! Uses regex-based pattern matching rather than a full PromQL AST parser.
//! This covers the aggregation patterns encountered in the DEBS 2022 benchmark
//! and the general sketch-eligible query forms described in the design doc.

use std::time::Duration;

use anyhow::anyhow;
use regex::Regex;

use super::{parse_by_dims, parse_label_filters, debs_hint, ParsedQuery, QueryHint};
use crate::analyzer::parse_duration;
use crate::types::AggType;

// ── Regex building blocks ─────────────────────────────────────────────────────
//
// Metric names may contain letters, digits, underscores, and dots
// (e.g. `financial.last_trade_price`).

/// Matches an optional label selector: `{key="val", ...}` (non-greedy body).
fn labels_re() -> &'static str { r"(?:\{([^}]*)\})?" }
/// Matches a range vector: `[5m]`.
fn range_re()  -> &'static str { r"\[([^\]]+)\]" }
/// Matches a trailing `by (dim1, dim2)` clause (optional, whitespace-flexible).
fn by_re()     -> &'static str { r"(?:\s+by\s*\(([^)]*)\))?" }
/// Matches a metric name (no spaces).
fn metric_re() -> &'static str { r"([a-zA-Z_][a-zA-Z0-9_.]*)" }

// ── Public entry point ────────────────────────────────────────────────────────

/// Parse a PromQL expression into a [`ParsedQuery`].
pub fn parse_promql(query: &str) -> anyhow::Result<ParsedQuery> {
    let q = query.trim();

    if let Some(r) = try_quantile_over_time(q)? { return Ok(r); }
    if let Some(r) = try_histogram_quantile(q)?  { return Ok(r); }
    if let Some(r) = try_topk(q)?               { return Ok(r); }
    if let Some(r) = try_count_cardinality(q)?   { return Ok(r); }
    if let Some(r) = try_avg_over_time(q)?       { return Ok(r); }
    if let Some(r) = try_min_over_time(q)?       { return Ok(r); }
    if let Some(r) = try_max_over_time(q)?       { return Ok(r); }
    if let Some(r) = try_count_over_time(q)?     { return Ok(r); }
    if let Some(r) = try_sum_over_time(q)?       { return Ok(r); }
    if let Some(r) = try_bare_selector(q)?       { return Ok(r); }

    Err(anyhow!("unsupported PromQL pattern: {:?}", q))
}

// ── Pattern recognisers ───────────────────────────────────────────────────────

/// `quantile_over_time(φ, metric{filters}[range]) by (dims)`
///
/// Maps to: Quantile → DDSketch / KLL.
fn try_quantile_over_time(q: &str) -> anyhow::Result<Option<ParsedQuery>> {
    let pat = format!(
        r"(?i)quantile_over_time\s*\(\s*([\d.]+)\s*,\s*{}{}{}\s*\){}",
        metric_re(), labels_re(), range_re(), by_re()
    );
    let re = Regex::new(&pat).unwrap();
    let Some(caps) = re.captures(q) else { return Ok(None) };

    let phi:    f64      = caps[1].parse().map_err(|_| anyhow!("bad φ in quantile_over_time"))?;
    let metric: String   = caps[2].to_string();
    let filters          = caps.get(3).map(|m| parse_label_filters(m.as_str())).unwrap_or_default();
    let window: Duration = parse_duration(&caps[4])?;
    let dims             = caps.get(5).map(|m| parse_by_dims(m.as_str())).unwrap_or_default();

    let quantiles = vec![phi];
    let hint = debs_hint(&metric, &[AggType::Quantile], &quantiles, false, None);

    Ok(Some(ParsedQuery {
        metric_name:     metric,
        aggregations:    vec![AggType::Quantile],
        group_by_labels: dims,
        label_filters:   filters,
        time_window:     window,
        exact_required:  false,
        quantiles,
        hint,
    }))
}

/// `histogram_quantile(φ, rate(metric{filters}[range])) [by (dims)]`
///
/// Maps to: Quantile → DDSketch / KLL.
fn try_histogram_quantile(q: &str) -> anyhow::Result<Option<ParsedQuery>> {
    let pat = format!(
        r"(?i)histogram_quantile\s*\(\s*([\d.]+)\s*,\s*\w+\s*\(\s*{}{}{}\s*\)\s*\){}",
        metric_re(), labels_re(), range_re(), by_re()
    );
    let re = Regex::new(&pat).unwrap();
    let Some(caps) = re.captures(q) else { return Ok(None) };

    let phi:    f64      = caps[1].parse().map_err(|_| anyhow!("bad φ in histogram_quantile"))?;
    let metric: String   = caps[2].to_string();
    let filters          = caps.get(3).map(|m| parse_label_filters(m.as_str())).unwrap_or_default();
    let window: Duration = parse_duration(&caps[4])?;
    let dims             = caps.get(5).map(|m| parse_by_dims(m.as_str())).unwrap_or_default();

    let quantiles = vec![phi];
    let hint = debs_hint(&metric, &[AggType::Quantile], &quantiles, false, None);

    Ok(Some(ParsedQuery {
        metric_name:     metric,
        aggregations:    vec![AggType::Quantile],
        group_by_labels: dims,
        label_filters:   filters,
        time_window:     window,
        exact_required:  false,
        quantiles,
        hint,
    }))
}

/// `topk(k, count_over_time(metric{filters}[range]) by (dims))`
///
/// Maps to: Frequency → CountSketch (heavy-hitter top-K).
fn try_topk(q: &str) -> anyhow::Result<Option<ParsedQuery>> {
    // Allow the inner `by (dims)` to appear either inside or outside the
    // count_over_time call.
    let pat = format!(
        r"(?i)topk\s*\(\s*(\d+)\s*,\s*\w+_over_time\s*\(\s*{}{}{}\s*\)\s*(?:by\s*\(([^)]*)\))?\s*\)",
        metric_re(), labels_re(), range_re()
    );
    let re = Regex::new(&pat).unwrap();
    let Some(caps) = re.captures(q) else { return Ok(None) };

    let k:      u64      = caps[1].parse().unwrap_or(10);
    let metric: String   = caps[2].to_string();
    let filters          = caps.get(3).map(|m| parse_label_filters(m.as_str())).unwrap_or_default();
    let window: Duration = parse_duration(&caps[4])?;
    let dims             = caps.get(5).map(|m| parse_by_dims(m.as_str())).unwrap_or_default();

    let hint = debs_hint(&metric, &[AggType::Frequency], &[], false, Some(k));

    Ok(Some(ParsedQuery {
        metric_name:     metric,
        aggregations:    vec![AggType::Frequency],
        group_by_labels: dims,
        label_filters:   filters,
        time_window:     window,
        exact_required:  false,
        quantiles:       vec![],
        hint,
    }))
}

/// `count(count_over_time(metric{filters}[range]) by (dims))`
///
/// Counts distinct values of the grouped dimensions → Cardinality → HLL.
fn try_count_cardinality(q: &str) -> anyhow::Result<Option<ParsedQuery>> {
    // Outer count() wrapping an inner *_over_time with a by clause signals
    // "how many distinct groups are active?" = cardinality.
    let pat = format!(
        r"(?i)^count\s*\(\s*\w+_over_time\s*\(\s*{}{}{}\s*\)\s*by\s*\(([^)]*)\)\s*\)\s*$",
        metric_re(), labels_re(), range_re()
    );
    let re = Regex::new(&pat).unwrap();
    let Some(caps) = re.captures(q) else { return Ok(None) };

    let metric: String   = caps[1].to_string();
    let filters          = caps.get(2).map(|m| parse_label_filters(m.as_str())).unwrap_or_default();
    let window: Duration = parse_duration(&caps[3])?;
    // The by-clause inside count_over_time defines the dimension whose
    // distinct values we are counting; the outer result is global.
    let _inner_dims      = caps.get(4).map(|m| parse_by_dims(m.as_str())).unwrap_or_default();

    let hint = debs_hint(&metric, &[AggType::Cardinality], &[], false, None);

    Ok(Some(ParsedQuery {
        metric_name:     metric,
        aggregations:    vec![AggType::Cardinality],
        group_by_labels: vec![],   // global cardinality
        label_filters:   filters,
        time_window:     window,
        exact_required:  false,
        quantiles:       vec![],
        hint,
    }))
}

/// `avg_over_time(metric{filters}[range]) by (dims)`
///
/// Median (p50) is a good proxy for the mean under most distributions.
/// Maps to: Quantile → DDSketch with quantile [0.5].
fn try_avg_over_time(q: &str) -> anyhow::Result<Option<ParsedQuery>> {
    let pat = format!(
        r"(?i)avg_over_time\s*\(\s*{}{}{}\s*\){}",
        metric_re(), labels_re(), range_re(), by_re()
    );
    let re = Regex::new(&pat).unwrap();
    let Some(caps) = re.captures(q) else { return Ok(None) };

    let metric: String   = caps[1].to_string();
    let filters          = caps.get(2).map(|m| parse_label_filters(m.as_str())).unwrap_or_default();
    let window: Duration = parse_duration(&caps[3])?;
    let dims             = caps.get(4).map(|m| parse_by_dims(m.as_str())).unwrap_or_default();

    let quantiles = vec![0.5];
    let hint = debs_hint(&metric, &[AggType::Quantile], &quantiles, false, None);

    Ok(Some(ParsedQuery {
        metric_name:     metric,
        aggregations:    vec![AggType::Quantile],
        group_by_labels: dims,
        label_filters:   filters,
        time_window:     window,
        exact_required:  false,
        quantiles,
        hint,
    }))
}

/// `min_over_time(metric{filters}[range]) by (dims)`
///
/// Extreme minimum → DDSketch with quantile [0.0].
fn try_min_over_time(q: &str) -> anyhow::Result<Option<ParsedQuery>> {
    let pat = format!(
        r"(?i)min_over_time\s*\(\s*{}{}{}\s*\){}",
        metric_re(), labels_re(), range_re(), by_re()
    );
    let re = Regex::new(&pat).unwrap();
    let Some(caps) = re.captures(q) else { return Ok(None) };

    let metric: String   = caps[1].to_string();
    let filters          = caps.get(2).map(|m| parse_label_filters(m.as_str())).unwrap_or_default();
    let window: Duration = parse_duration(&caps[3])?;
    let dims             = caps.get(4).map(|m| parse_by_dims(m.as_str())).unwrap_or_default();

    let quantiles = vec![0.0];
    let hint = debs_hint(&metric, &[AggType::Quantile], &quantiles, false, None);

    Ok(Some(ParsedQuery {
        metric_name:     metric,
        aggregations:    vec![AggType::Quantile],
        group_by_labels: dims,
        label_filters:   filters,
        time_window:     window,
        exact_required:  false,
        quantiles,
        hint,
    }))
}

/// `max_over_time(metric{filters}[range]) by (dims)`
///
/// Extreme maximum → DDSketch with quantile [1.0].
fn try_max_over_time(q: &str) -> anyhow::Result<Option<ParsedQuery>> {
    let pat = format!(
        r"(?i)max_over_time\s*\(\s*{}{}{}\s*\){}",
        metric_re(), labels_re(), range_re(), by_re()
    );
    let re = Regex::new(&pat).unwrap();
    let Some(caps) = re.captures(q) else { return Ok(None) };

    let metric: String   = caps[1].to_string();
    let filters          = caps.get(2).map(|m| parse_label_filters(m.as_str())).unwrap_or_default();
    let window: Duration = parse_duration(&caps[3])?;
    let dims             = caps.get(4).map(|m| parse_by_dims(m.as_str())).unwrap_or_default();

    let quantiles = vec![1.0];
    let hint = debs_hint(&metric, &[AggType::Quantile], &quantiles, false, None);

    Ok(Some(ParsedQuery {
        metric_name:     metric,
        aggregations:    vec![AggType::Quantile],
        group_by_labels: dims,
        label_filters:   filters,
        time_window:     window,
        exact_required:  false,
        quantiles,
        hint,
    }))
}

/// `count_over_time(metric{filters}[range]) by (dims)`
///
/// Per-series event count in a window → Frequency (CountMinSketch).
fn try_count_over_time(q: &str) -> anyhow::Result<Option<ParsedQuery>> {
    let pat = format!(
        r"(?i)count_over_time\s*\(\s*{}{}{}\s*\){}",
        metric_re(), labels_re(), range_re(), by_re()
    );
    let re = Regex::new(&pat).unwrap();
    let Some(caps) = re.captures(q) else { return Ok(None) };

    let metric: String   = caps[1].to_string();
    let filters          = caps.get(2).map(|m| parse_label_filters(m.as_str())).unwrap_or_default();
    let window: Duration = parse_duration(&caps[3])?;
    let dims             = caps.get(4).map(|m| parse_by_dims(m.as_str())).unwrap_or_default();

    let hint = debs_hint(&metric, &[AggType::Frequency], &[], false, None);

    Ok(Some(ParsedQuery {
        metric_name:     metric,
        aggregations:    vec![AggType::Frequency],
        group_by_labels: dims,
        label_filters:   filters,
        time_window:     window,
        exact_required:  false,
        quantiles:       vec![],
        hint,
    }))
}

/// `sum_over_time(metric{filters}[range]) by (dims)`
///
/// Exact sum — no sketch benefit.  Planner will use raw passthrough.
fn try_sum_over_time(q: &str) -> anyhow::Result<Option<ParsedQuery>> {
    let pat = format!(
        r"(?i)sum_over_time\s*\(\s*{}{}{}\s*\){}",
        metric_re(), labels_re(), range_re(), by_re()
    );
    let re = Regex::new(&pat).unwrap();
    let Some(caps) = re.captures(q) else { return Ok(None) };

    let metric: String   = caps[1].to_string();
    let filters          = caps.get(2).map(|m| parse_label_filters(m.as_str())).unwrap_or_default();
    let window: Duration = parse_duration(&caps[3])?;
    let dims             = caps.get(4).map(|m| parse_by_dims(m.as_str())).unwrap_or_default();

    Ok(Some(ParsedQuery {
        metric_name:     metric,
        aggregations:    vec![],  // no sketch aggregation
        group_by_labels: dims,
        label_filters:   filters,
        time_window:     window,
        exact_required:  true,
        quantiles:       vec![],
        hint:            Some(QueryHint::ExactRequired {
            reason: "sum_over_time requires exact aggregation".into(),
        }),
    }))
}

/// Bare metric selector with no aggregation function.
///
/// Examples: `financial.last_trade_price{symbol="RDSA.NL"}` (RSI, MACD feeds)
/// Maps to: `exact_required = true`.
fn try_bare_selector(q: &str) -> anyhow::Result<Option<ParsedQuery>> {
    // Must look like just a metric name (+ optional label selector), nothing else.
    let pat = format!(r"(?i)^{}{}(?:\[([^\]]+)\])?$", metric_re(), labels_re());
    let re = Regex::new(&pat).unwrap();
    let Some(caps) = re.captures(q) else { return Ok(None) };

    let metric: String = caps[1].to_string();
    let filters        = caps.get(2).map(|m| parse_label_filters(m.as_str())).unwrap_or_default();
    let window = caps.get(3)
        .map(|m| parse_duration(m.as_str()))
        .transpose()?
        .unwrap_or(Duration::from_secs(300)); // default 5 min

    let hint = Some(QueryHint::ExactRequired {
        reason: "no aggregation function — raw samples required".into(),
    });

    Ok(Some(ParsedQuery {
        metric_name:     metric,
        aggregations:    vec![],
        group_by_labels: vec![],
        label_filters:   filters,
        time_window:     window,
        exact_required:  true,
        quantiles:       vec![],
        hint,
    }))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::AggType;
    use std::time::Duration;

    fn parse(q: &str) -> ParsedQuery {
        parse_promql(q).unwrap_or_else(|e| panic!("parse failed: {e}: query={q:?}"))
    }

    // ── DEBS Q1: EMA ─────────────────────────────────────────────────────────

    #[test]
    fn q1_quantile_over_time() {
        let pq = parse("quantile_over_time(0.5, financial.last_trade_price[5m]) by (symbol)");
        assert_eq!(pq.aggregations, vec![AggType::Quantile]);
        assert_eq!(pq.time_window,  Duration::from_secs(300));
        assert_eq!(pq.group_by_labels, vec!["symbol"]);
        assert_eq!(pq.quantiles, vec![0.5]);
        assert!(!pq.exact_required);
    }

    #[test]
    fn q1_avg_over_time() {
        let pq = parse("avg_over_time(financial.last_trade_price[5m]) by (symbol)");
        assert_eq!(pq.aggregations, vec![AggType::Quantile]);
        assert_eq!(pq.quantiles,    vec![0.5]);
    }

    // ── DEBS Q3: top-K ───────────────────────────────────────────────────────

    #[test]
    fn q3_topk() {
        let pq = parse("topk(10, count_over_time(financial.last_trade_price[5m]) by (symbol))");
        assert_eq!(pq.aggregations, vec![AggType::Frequency]);
        assert_eq!(pq.time_window,  Duration::from_secs(300));
        assert_eq!(pq.group_by_labels, vec!["symbol"]);
    }

    // ── DEBS Q4: price stats ─────────────────────────────────────────────────

    #[test]
    fn q4_min_over_time() {
        let pq = parse("min_over_time(financial.last_trade_price[5m]) by (symbol)");
        assert_eq!(pq.aggregations, vec![AggType::Quantile]);
        assert_eq!(pq.quantiles,    vec![0.0]);
        matches!(pq.hint, Some(QueryHint::DebsPriceStats));
    }

    #[test]
    fn q4_max_over_time() {
        let pq = parse("max_over_time(financial.last_trade_price[5m]) by (symbol)");
        assert_eq!(pq.quantiles, vec![1.0]);
        matches!(pq.hint, Some(QueryHint::DebsPriceStats));
    }

    // ── DEBS Q6: cardinality ─────────────────────────────────────────────────

    #[test]
    fn q6_count_cardinality() {
        let pq = parse("count(count_over_time(financial.last_trade_price[5m]) by (symbol))");
        assert_eq!(pq.aggregations, vec![AggType::Cardinality]);
        assert!(pq.group_by_labels.is_empty(), "global cardinality has no group-by");
    }

    // ── DEBS Q10/11/12: exact required ───────────────────────────────────────

    #[test]
    fn exact_required_bare_selector() {
        let pq = parse(r#"financial.last_trade_price{symbol="RDSA.NL"}"#);
        assert!(pq.exact_required);
        assert_eq!(pq.label_filters.get("symbol").map(String::as_str), Some("RDSA.NL"));
    }

    #[test]
    fn sum_over_time_exact_required() {
        let pq = parse("sum_over_time(request_bytes[1h]) by (service)");
        assert!(pq.exact_required);
    }

    // ── Generic patterns ─────────────────────────────────────────────────────

    #[test]
    fn quantile_with_label_filters() {
        let pq = parse(r#"quantile_over_time(0.99, latency{service="web",env="prod"}[5m]) by (host)"#);
        assert_eq!(pq.metric_name, "latency");
        assert_eq!(pq.label_filters.get("service").map(String::as_str), Some("web"));
        assert_eq!(pq.label_filters.get("env").map(String::as_str),     Some("prod"));
        assert_eq!(pq.group_by_labels, vec!["host"]);
        assert_eq!(pq.quantiles,       vec![0.99]);
    }

    #[test]
    fn count_over_time_frequency() {
        let pq = parse("count_over_time(http_requests[1m]) by (path)");
        assert_eq!(pq.aggregations, vec![AggType::Frequency]);
        assert_eq!(pq.time_window,  Duration::from_secs(60));
    }

    #[test]
    fn histogram_quantile() {
        let pq = parse("histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))");
        assert_eq!(pq.aggregations, vec![AggType::Quantile]);
        assert_eq!(pq.quantiles,    vec![0.95]);
    }

    #[test]
    fn unsupported_promql_returns_error() {
        assert!(parse_promql("some_unknown_fn(metric[5m])").is_err());
    }

    #[test]
    fn duration_1h() {
        let pq = parse("avg_over_time(cpu_usage[1h]) by (host)");
        assert_eq!(pq.time_window, Duration::from_secs(3600));
    }
}
