use std::collections::{HashMap, HashSet};
use std::time::Duration;
use anyhow::{anyhow, Context};
use serde::{Deserialize, Serialize};

use crate::query_parser;
use crate::types::{AggType, QueryWorkload, SketchType, WorkloadCharacteristics};

// ── Public API ────────────────────────────────────────────────────────────────

/// JSON-friendly representation of a query workload submitted by callers.
///
/// There are two ways to populate a `QuerySpec`:
///
/// 1. **Explicit fields** — supply `metric_name`, `aggregations`,
///    `time_window`, etc. directly. This is the original API.
///
/// 2. **Query string** — supply a raw PromQL or SQL string in
///    `query_string`.  The analyzer parses it and fills in `metric_name`,
///    `aggregations`, `group_by_labels`, `label_filters`, and `time_window`
///    automatically.  Any explicit fields that are non-empty / non-default
///    **override** the parsed values, so the two approaches compose.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuerySpec {
    /// Raw PromQL or SQL query string to parse (SP-1 automatic extraction).
    /// When provided, metric_name / aggregations / time_window may be omitted
    /// and will be derived from the query.
    #[serde(default)]
    pub query_string:   Option<String>,

    /// Metric name override.  Required when `query_string` is absent.
    #[serde(default)]
    pub metric_name:    String,
    #[serde(default)]
    pub label_filters:  HashMap<String, String>,
    #[serde(default)]
    pub group_by_labels: Vec<String>,
    /// Aggregation type overrides ("quantile", "cardinality", "frequency").
    /// Required when `query_string` is absent.
    #[serde(default)]
    pub aggregations:   Vec<String>,
    /// Time window override (e.g. "5m").  Required when `query_string` is absent.
    #[serde(default)]
    pub time_window:    String,
    #[serde(default)]
    pub repeat_every:   Option<String>,
    pub accuracy_sla:   f64,
    pub latency_sla:    Option<String>,
    /// Optional: pin a specific sketch type, bypassing the cost-model planner.
    pub sketch_type:    Option<SketchType>,
    /// Observable data-stream characteristics used for delta / raw-vs-sketch
    /// bandwidth comparison. Omit to use conservative defaults.
    #[serde(default)]
    pub workload:       WorkloadCharacteristics,
}

pub struct Analyzer;

impl Analyzer {
    pub fn new() -> Self { Self }

    pub fn analyze(&self, spec: QuerySpec) -> anyhow::Result<QueryWorkload> {
        if !(0.0..=1.0).contains(&spec.accuracy_sla) {
            return Err(anyhow!("accuracy_sla must be in [0,1], got {}", spec.accuracy_sla));
        }

        // ── Step 1: parse query_string if provided ─────────────────────────
        let parsed = spec.query_string.as_deref()
            .map(|q| query_parser::parse_query(q))
            .transpose()
            .with_context(|| "failed to parse query_string")?;

        // ── Step 2: resolve metric_name ────────────────────────────────────
        let metric_name = if !spec.metric_name.trim().is_empty() {
            spec.metric_name.clone()
        } else if let Some(ref p) = parsed {
            p.metric_name.clone()
        } else {
            return Err(anyhow!(
                "metric_name is required (or provide query_string)"
            ));
        };

        // ── Step 3: resolve aggregations ───────────────────────────────────
        let aggregations = if !spec.aggregations.is_empty() {
            parse_agg_types(&spec.aggregations)?
        } else if let Some(ref p) = parsed {
            if p.aggregations.is_empty() && !p.exact_required {
                return Err(anyhow!(
                    "could not infer aggregation type from query_string; \
                     provide explicit aggregations"
                ));
            }
            p.aggregations.clone()
        } else {
            return Err(anyhow!("at least one aggregation is required"));
        };

        // ── Step 4: resolve time_window ────────────────────────────────────
        let time_window = if !spec.time_window.trim().is_empty() {
            let d = parse_duration(&spec.time_window)
                .with_context(|| format!("invalid time_window {:?}", spec.time_window))?;
            if d.is_zero() {
                return Err(anyhow!("time_window must be positive"));
            }
            d
        } else if let Some(ref p) = parsed {
            p.time_window
        } else {
            return Err(anyhow!("time_window is required (or provide query_string)"));
        };

        // ── Step 5: resolve dimensions (group_by + label_filter keys) ──────
        // Parsed values are the base; explicit spec fields override / extend.
        let parsed_group_by = parsed.as_ref().map(|p| p.group_by_labels.as_slice()).unwrap_or(&[]);
        let parsed_filters: HashMap<String, String> =
            parsed.as_ref().map(|p| p.label_filters.clone()).unwrap_or_default();

        let merged_filters: HashMap<String, String> = {
            let mut m = parsed_filters;
            m.extend(spec.label_filters.clone());  // explicit overrides parsed
            m
        };

        let filter_keys: Vec<String> = merged_filters.keys().cloned().collect();
        let all_group_by: Vec<String> = dedup_dims(
            &dedup_dims(parsed_group_by, &spec.group_by_labels),
            &filter_keys,
        );

        // ── Step 6: scalar fields ──────────────────────────────────────────
        let repeat_every = spec.repeat_every.as_deref()
            .map(parse_duration)
            .transpose()
            .with_context(|| "invalid repeat_every")?;

        let latency_sla = spec.latency_sla.as_deref()
            .map(parse_duration)
            .transpose()
            .with_context(|| "invalid latency_sla")?;

        let exact_required = parsed.as_ref().map(|p| p.exact_required).unwrap_or(false);
        let quantiles       = parsed.as_ref().map(|p| p.quantiles.clone()).unwrap_or_default();

        Ok(QueryWorkload {
            metric_name,
            label_filters:        merged_filters,
            group_by_labels:      all_group_by,
            aggregations,
            time_window,
            repeat_every,
            accuracy_sla:         spec.accuracy_sla,
            latency_sla,
            sketch_type_override: spec.sketch_type,
            exact_required,
            quantiles,
        })
    }

}

// ── Duration helpers (used by other modules) ──────────────────────────────────

/// Parses duration strings like "5m", "1h", "30s", "1h30m", "1h5m30s".
pub fn parse_duration(s: &str) -> anyhow::Result<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return Err(anyhow!("empty duration string"));
    }
    let mut total_secs: u64 = 0;
    let mut current_num = String::new();
    for ch in s.chars() {
        if ch.is_ascii_digit() {
            current_num.push(ch);
        } else {
            let n: u64 = current_num.parse()
                .map_err(|_| anyhow!("invalid number in duration {:?}", s))?;
            current_num.clear();
            match ch {
                'h' => total_secs += n * 3600,
                'm' => total_secs += n * 60,
                's' => total_secs += n,
                _ => return Err(anyhow!("unknown unit {:?} in duration {:?}", ch, s)),
            }
        }
    }
    if !current_num.is_empty() {
        return Err(anyhow!("trailing digits without unit in {:?}", s));
    }
    Ok(Duration::from_secs(total_secs))
}

/// Formats a Duration as a compact string: "5m", "1h30m", "30s".
pub fn format_duration(d: Duration) -> String {
    let s = d.as_secs();
    let h = s / 3600;
    let m = (s % 3600) / 60;
    let sec = s % 60;
    let mut out = String::new();
    if h   > 0 { out.push_str(&format!("{}h", h)); }
    if m   > 0 { out.push_str(&format!("{}m", m)); }
    if sec > 0 || out.is_empty() { out.push_str(&format!("{}s", sec)); }
    out
}

// ── Private helpers ───────────────────────────────────────────────────────────

fn parse_agg_types(raw: &[String]) -> anyhow::Result<Vec<AggType>> {
    raw.iter().map(|s| match s.to_lowercase().trim() {
        "quantile"    => Ok(AggType::Quantile),
        "cardinality" => Ok(AggType::Cardinality),
        "frequency"   => Ok(AggType::Frequency),
        other => Err(anyhow!(
            "unknown aggregation type {:?} (want: quantile, cardinality, frequency)", other
        )),
    }).collect()
}

fn dedup_dims(a: &[String], b: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out  = Vec::new();
    for v in a.iter().chain(b.iter()) {
        if seen.insert(v.clone()) { out.push(v.clone()); }
    }
    out
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn basic_spec() -> QuerySpec {
        QuerySpec {
            query_string:   None,
            metric_name:    "request_latency".into(),
            label_filters:  [("service".into(), "web".into())].into(),
            group_by_labels: vec!["host.name".into()],
            aggregations:   vec!["quantile".into()],
            time_window:    "5m".into(),
            repeat_every:   Some("1m".into()),
            accuracy_sla:   0.01,
            latency_sla:    Some("10m".into()),
            sketch_type:    None,
            workload:       Default::default(),
        }
    }

    #[test]
    fn valid_spec() {
        let w = Analyzer::new().analyze(basic_spec()).unwrap();
        assert_eq!(w.metric_name, "request_latency");
        assert_eq!(w.accuracy_sla, 0.01);
        assert_eq!(w.time_window,    Duration::from_secs(300));
        assert_eq!(w.repeat_every,   Some(Duration::from_secs(60)));
        assert_eq!(w.latency_sla,    Some(Duration::from_secs(600)));
        assert_eq!(w.aggregations,   vec![AggType::Quantile]);
    }

    #[test]
    fn dimension_merge_dedup() {
        let mut spec = basic_spec();
        spec.label_filters   = [("service".into(), "api".into()),
                                 ("host.name".into(), "h1".into())].into();
        spec.group_by_labels = vec!["host.name".into(), "region".into()];
        let w = Analyzer::new().analyze(spec).unwrap();
        for dim in &["host.name", "region", "service"] {
            assert!(w.group_by_labels.contains(&dim.to_string()), "missing {dim}");
        }
        // host.name must appear exactly once after dedup
        assert_eq!(
            w.group_by_labels.iter().filter(|d| d.as_str() == "host.name").count(), 1
        );
    }

    #[test]
    fn multiple_aggregations() {
        let mut spec = basic_spec();
        spec.aggregations = vec!["cardinality".into(), "frequency".into()];
        let w = Analyzer::new().analyze(spec).unwrap();
        assert_eq!(w.aggregations, vec![AggType::Cardinality, AggType::Frequency]);
    }

    #[test]
    fn missing_metric_name() {
        let mut spec = basic_spec();
        spec.metric_name = "".into();
        assert!(Analyzer::new().analyze(spec).is_err());
    }

    #[test]
    fn missing_aggregations() {
        let mut spec = basic_spec();
        spec.aggregations = vec![];
        assert!(Analyzer::new().analyze(spec).is_err());
    }

    #[test]
    fn invalid_aggregation_type() {
        let mut spec = basic_spec();
        spec.aggregations = vec!["histogram".into()];
        assert!(Analyzer::new().analyze(spec).is_err());
    }

    #[test]
    fn invalid_duration() {
        let mut spec = basic_spec();
        spec.time_window = "not-a-duration".into();
        assert!(Analyzer::new().analyze(spec).is_err());
    }

    #[test]
    fn invalid_accuracy_sla() {
        for bad in &[-0.1f64, 1.5] {
            let mut spec = basic_spec();
            spec.accuracy_sla = *bad;
            assert!(Analyzer::new().analyze(spec).is_err(),
                "expected error for accuracy_sla={bad}");
        }
    }

    #[test]
    fn parse_duration_formats() {
        assert_eq!(parse_duration("30s").unwrap(),    Duration::from_secs(30));
        assert_eq!(parse_duration("5m").unwrap(),     Duration::from_secs(300));
        assert_eq!(parse_duration("1h").unwrap(),     Duration::from_secs(3600));
        assert_eq!(parse_duration("1h30m").unwrap(),  Duration::from_secs(5400));
        assert_eq!(parse_duration("1h5m30s").unwrap(),Duration::from_secs(3930));
    }

    #[test]
    fn format_duration_roundtrip() {
        for secs in [30u64, 300, 3600, 5400, 3930] {
            let d = Duration::from_secs(secs);
            let s = format_duration(d);
            let parsed = parse_duration(&s).unwrap();
            assert_eq!(parsed, d, "roundtrip failed for {secs}s → {s:?}");
        }
    }

    #[test]
    fn trailing_digits_error() {
        assert!(parse_duration("5").is_err());
    }

    // ── query_string path ─────────────────────────────────────────────────────

    /// Build a minimal QuerySpec driven entirely by a query_string.
    fn qs_only(query: &str) -> QuerySpec {
        QuerySpec {
            query_string:    Some(query.into()),
            metric_name:     "".into(),
            label_filters:   Default::default(),
            group_by_labels: vec![],
            aggregations:    vec![],
            time_window:     "".into(),
            repeat_every:    None,
            accuracy_sla:    0.01,
            latency_sla:     None,
            sketch_type:     None,
            workload:        Default::default(),
        }
    }

    /// PromQL query_string auto-populates metric_name, aggregations,
    /// time_window, and quantiles — no explicit fields required.
    #[test]
    fn query_string_promql_populates_workload() {
        let w = Analyzer::new()
            .analyze(qs_only("sum by (host) (quantile_over_time(0.99, latency[5m]))"))
            .unwrap();
        assert_eq!(w.metric_name,  "latency");
        assert_eq!(w.aggregations, vec![AggType::Quantile]);
        assert_eq!(w.time_window,  Duration::from_secs(300));
        assert_eq!(w.quantiles,    vec![0.99]);
        assert!(!w.exact_required);
    }

    /// SQL query_string auto-populates metric_name, aggregations,
    /// and group_by_labels.
    #[test]
    fn query_string_sql_populates_workload() {
        let w = Analyzer::new()
            .analyze(qs_only(
                "SELECT symbol, COUNT(*) FROM financial_last_trade_price GROUP BY symbol",
            ))
            .unwrap();
        assert_eq!(w.metric_name, "financial_last_trade_price");
        assert_eq!(w.aggregations, vec![AggType::Frequency]);
        assert!(w.group_by_labels.contains(&"symbol".to_string()));
    }

    /// Explicit metric_name overrides the name derived from query_string.
    #[test]
    fn explicit_metric_name_overrides_parsed() {
        let mut spec = qs_only("sum by (host) (avg_over_time(cpu[5m]))");
        spec.metric_name = "my_custom_metric".into();
        let w = Analyzer::new().analyze(spec).unwrap();
        assert_eq!(w.metric_name, "my_custom_metric");
        // aggregations still come from parse (avg → DDSketch → Quantile)
        assert_eq!(w.aggregations, vec![AggType::Quantile]);
    }

    /// Explicit time_window overrides the window derived from query_string.
    #[test]
    fn explicit_time_window_overrides_parsed() {
        let mut spec = qs_only("sum by (host) (avg_over_time(cpu[5m]))");
        spec.time_window = "1h".into();
        let w = Analyzer::new().analyze(spec).unwrap();
        assert_eq!(w.time_window, Duration::from_secs(3600));
    }

    /// Explicit aggregations override those derived from query_string.
    #[test]
    fn explicit_aggregations_override_parsed() {
        let mut spec = qs_only("sum by (host) (avg_over_time(cpu[5m]))"); // → Quantile
        spec.aggregations = vec!["cardinality".into()];
        let w = Analyzer::new().analyze(spec).unwrap();
        assert_eq!(w.aggregations, vec![AggType::Cardinality]);
    }

    /// sum_over_time is a stateful exact aggregation; exact_required is set.
    #[test]
    fn query_string_exact_required_propagated() {
        let w = Analyzer::new()
            .analyze(qs_only("sum by (service) (sum_over_time(request_bytes[1h]))"))
            .unwrap();
        assert!(w.exact_required, "sum_over_time must set exact_required");
        assert_eq!(w.aggregations, vec![]);
    }

    /// DDSketch quantile φ values are surfaced through the workload.
    #[test]
    fn query_string_quantiles_populated() {
        let w = Analyzer::new()
            .analyze(qs_only("sum by (host) (quantile_over_time(0.5, latency[5m]))"))
            .unwrap();
        assert_eq!(w.quantiles, vec![0.5]);
    }

    /// Existing callers that supply all fields explicitly and omit
    /// query_string continue to work unchanged (backward compatibility).
    #[test]
    fn backward_compat_no_query_string() {
        let w = Analyzer::new().analyze(basic_spec()).unwrap();
        assert_eq!(w.metric_name,  "request_latency");
        assert_eq!(w.aggregations, vec![AggType::Quantile]);
        assert_eq!(w.time_window,  Duration::from_secs(300));
        assert!(!w.exact_required);
        assert!(w.quantiles.is_empty());
    }
}
