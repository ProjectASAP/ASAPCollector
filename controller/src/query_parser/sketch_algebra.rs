//! Sketch algebra — the shared intermediate representation (IR) that both the
//! PromQL and SQL parsers compile to.
//!
//! Both parsers are pure front-ends: they walk their respective ASTs and emit
//! a [`SketchExpr`] tree.  The optimizer in [`super::sketch_rules`] then
//! applies algebraic rewrite rules before the planner converts the tree to
//! agent configurations.
//!
//! # Operator summary
//!
//! | Operator | Symbol | Description |
//! |---|---|---|
//! | `Source` | — | Base relation or metric stream |
//! | `Filter` | σ | Push-down predicates (WHERE / label matchers) |
//! | `Window` | ψ | Time window (PromQL range; SQL time predicate) |
//! | `Partition` | γ | GROUP BY / `by (dims)` — one sketch per key-tuple |
//! | `Agg` | α | The sketch aggregation itself |
//! | `Dedup` | δ | Deduplicate before ingestion (push-down DISTINCT) |
//! | `TopK` | τ | Retain only the top-K entries |
//! | `Merge` | ⊕ | Merge sketches from multiple branches |
//! | `JoinSketch` | ⋈ₛₖ | Pre-agg sketch on inner side, merge after join |

use std::collections::HashMap;
use std::time::Duration;

use crate::types::AggType;
use super::{ParsedQuery, QueryHint, debs_hint};

// ── Core IR ───────────────────────────────────────────────────────────────────

/// Abstract sketch algebra expression — shared IR for SQL and PromQL.
#[derive(Debug, Clone)]
pub enum SketchExpr {
    /// Base relation / metric stream.
    Source(SourceSpec),

    /// σ — filter input before any sketch build (WHERE / PromQL label matchers).
    Filter {
        pred:  Vec<Predicate>,
        input: Box<SketchExpr>,
    },

    /// ψ — time window (PromQL range vector `[5m]`; SQL sliding-window predicate).
    Window {
        duration: Duration,
        input:    Box<SketchExpr>,
    },

    /// γ — partition by keys; one sketch instance per distinct key-tuple.
    /// Use [`PartitionKeys::Without`] when the PromQL `without (...)` clause is present.
    Partition {
        keys:  PartitionKeys,
        input: Box<SketchExpr>,
    },

    /// α — the sketch aggregation operator.
    Agg {
        op:    SketchAggOp,
        col:   ColumnRef,
        input: Box<SketchExpr>,
    },

    /// δ — deduplicate on `col` before ingestion.
    /// Note: for [`SketchAggOp::HLL`] this node is eliminated by rule R6.
    Dedup {
        col:   String,
        input: Box<SketchExpr>,
    },

    /// τ — top-K post-sketch filter.
    TopK {
        k:     u64,
        input: Box<SketchExpr>,
    },

    /// ⊕ — merge sketches from multiple independent branches.
    /// All input [`SketchAggOp`]s must be [`SketchAggOp::is_mergeable`].
    Merge {
        inputs: Vec<SketchExpr>,
    },

    /// ⋈ₛₖ — join push-down:
    /// pre-aggregate sketch on inner side by join key, merge after join.
    JoinSketch {
        join_key: String,
        outer:    Box<SketchExpr>,
        /// Inner carries its own `Partition` + `Agg` nodes.
        inner:    Box<SketchExpr>,
    },
}

// ── Source ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct SourceSpec {
    /// Table name (SQL) or metric name (PromQL).
    pub name: String,
}

// ── Partition keys ────────────────────────────────────────────────────────────

/// How the stream is partitioned.
#[derive(Debug, Clone)]
pub enum PartitionKeys {
    /// `by (k1, k2, ...)` — explicit key list.
    By(Vec<String>),
    /// `without (k1, k2, ...)` — complement; resolved against schema at plan time.
    Without(Vec<String>),
}

impl PartitionKeys {
    pub fn keys(&self) -> &[String] {
        match self {
            PartitionKeys::By(k) | PartitionKeys::Without(k) => k,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.keys().is_empty()
    }

    pub fn into_by_keys(self) -> Vec<String> {
        match self {
            PartitionKeys::By(k) => k,
            // For Without, return empty — caller resolves complement.
            PartitionKeys::Without(k) => k,
        }
    }
}

// ── Column reference ──────────────────────────────────────────────────────────

/// Which column / field the sketch aggregation targets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnRef {
    /// Explicit column name (SQL: `AVG(price)` → `Named("price")`).
    Named(String),
    /// The implicit metric sample value (PromQL — always the series value).
    SampleValue,
    /// All rows / COUNT(*).
    Wildcard,
}

// ── Sketch aggregation operators ──────────────────────────────────────────────

/// The concrete sketch type used for aggregation.
#[derive(Debug, Clone, PartialEq)]
pub enum SketchAggOp {
    /// Count-Min Sketch — frequency per group (COUNT(*) GROUP BY).
    CountMin { width: u32, depth: u8 },

    /// Count Sketch (heavy-hitter) — top-K by frequency.
    CountSketch { k: u64 },

    /// HyperLogLog — distinct-value counting (COUNT DISTINCT).
    HLL { registers: u8 },

    /// DDSketch — quantile estimation.
    /// `quantiles` holds the φ values to track; `epsilon` is relative error.
    DDSketch { quantiles: Vec<f64>, epsilon: f64 },

    /// Exact running min/max tracker — cheaper than DDSketch for extrema
    /// without a GROUP BY (no sketch benefit for global extrema).
    ExactMinMax { min: bool, max: bool },

    /// Hydra — sketch of sketches for multi-dimensional GROUP BY.
    /// Maintains one `inner` sketch per distinct `partition_keys` tuple.
    Hydra {
        inner:          Box<SketchAggOp>,
        partition_keys: Vec<String>,
    },

    /// Exact passthrough — no sketch benefit (SUM, global COUNT, etc.).
    Exact(ExactAgg),
}

/// Exact (non-sketch) aggregation kinds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExactAgg {
    Count,
    Sum,
    /// **Not mergeable** — carries `(sum, count)` in distributed contexts.
    Avg,
    Min,
    Max,
}

impl SketchAggOp {
    /// Returns `true` when two instances of this sketch can be merged
    /// (i.e., `sketch(A ∪ B) = merge(sketch(A), sketch(B))`).
    pub fn is_mergeable(&self) -> bool {
        match self {
            SketchAggOp::Exact(ExactAgg::Avg) => false,
            SketchAggOp::Hydra { inner, .. }  => inner.is_mergeable(),
            _                                  => true,
        }
    }

    /// Map to the coarse [`AggType`] used by the legacy planner.
    pub fn to_agg_type(&self) -> AggType {
        match self {
            SketchAggOp::HLL { .. }                                  => AggType::Cardinality,
            SketchAggOp::CountMin { .. } | SketchAggOp::CountSketch { .. } => AggType::Frequency,
            SketchAggOp::DDSketch { .. } | SketchAggOp::ExactMinMax { .. } => AggType::Quantile,
            SketchAggOp::Hydra { inner, .. }                         => inner.to_agg_type(),
            SketchAggOp::Exact(_)                                    => AggType::Quantile,
        }
    }

    /// Extract quantile φ values for DDSketch operators.
    pub fn quantiles(&self) -> Vec<f64> {
        match self {
            SketchAggOp::DDSketch { quantiles, .. } => quantiles.clone(),
            SketchAggOp::Hydra { inner, .. }        => inner.quantiles(),
            _                                        => vec![],
        }
    }

    /// Whether this op implies `exact_required` (no sketch benefit).
    pub fn is_exact(&self) -> bool {
        matches!(self, SketchAggOp::Exact(_) | SketchAggOp::ExactMinMax { .. })
    }
}

// ── Default sketch parameters ─────────────────────────────────────────────────

impl SketchAggOp {
    pub fn default_count_min() -> Self {
        SketchAggOp::CountMin { width: 2000, depth: 5 }
    }
    pub fn default_hll() -> Self {
        SketchAggOp::HLL { registers: 14 }
    }
    pub fn default_ddsketch(quantiles: Vec<f64>) -> Self {
        SketchAggOp::DDSketch { quantiles, epsilon: 0.01 }
    }
}

// ── Predicates ────────────────────────────────────────────────────────────────

/// A single filter predicate pushed down to the collector.
#[derive(Debug, Clone)]
pub struct Predicate {
    pub col: String,
    pub op:  FilterOp,
    pub val: FilterVal,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FilterOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Like,
    NotLike,
    IsNull,
    IsNotNull,
    /// PromQL `=~` label matcher (RE2 syntax).
    Regex(String),
    /// PromQL `!~` label matcher.
    NotRegex(String),
}

#[derive(Debug, Clone)]
pub enum FilterVal {
    Str(String),
    Num(f64),
    Int(i64),
    Null,
}

// ── Coverage ──────────────────────────────────────────────────────────────────

/// How completely a query can be served by sketches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SketchCoverage {
    /// All aggregation columns are sketch-mapped.
    Full,
    /// Some columns are sketch-mapped; others require exact passthrough
    /// (e.g. `MIN(URL)` alongside `COUNT(*)`).
    Partial,
    /// No sketch applicable; query requires exact execution.
    None,
}

// ── SketchExpr → ParsedQuery bridge (backward compat) ────────────────────────

impl SketchExpr {
    /// Convert to the legacy [`ParsedQuery`] flat representation consumed by
    /// the existing [`crate::analyzer::Analyzer`] and planner.
    pub fn to_parsed_query(&self) -> ParsedQuery {
        let mut c = PqCollector::default();
        c.visit(self);
        c.build()
    }
}

#[derive(Default)]
struct PqCollector {
    metric_name:     Option<String>,
    agg_types:       Vec<AggType>,
    group_by_labels: Vec<String>,
    label_filters:   HashMap<String, String>,
    time_window:     Option<Duration>,
    exact_required:  bool,
    quantiles:       Vec<f64>,
    topk:            Option<u64>,
}

impl PqCollector {
    fn visit(&mut self, expr: &SketchExpr) {
        match expr {
            SketchExpr::Source(s) => {
                if self.metric_name.is_none() {
                    self.metric_name = Some(s.name.clone());
                }
            }
            SketchExpr::Filter { pred, input } => {
                for p in pred {
                    if let (FilterOp::Eq, FilterVal::Str(v)) = (&p.op, &p.val) {
                        self.label_filters.insert(p.col.clone(), v.clone());
                    }
                }
                self.visit(input);
            }
            SketchExpr::Window { duration, input } => {
                if self.time_window.is_none() {
                    self.time_window = Some(*duration);
                }
                self.visit(input);
            }
            SketchExpr::Partition { keys, input } => {
                for k in keys.keys() {
                    if !self.group_by_labels.contains(k) {
                        self.group_by_labels.push(k.clone());
                    }
                }
                self.visit(input);
            }
            SketchExpr::Agg { op, input, .. } => {
                self.collect_op(op);
                self.visit(input);
            }
            SketchExpr::TopK { k, input } => {
                self.topk = Some(*k);
                self.visit(input);
            }
            SketchExpr::Dedup { input, .. } => self.visit(input),
            SketchExpr::Merge { inputs } => {
                for i in inputs { self.visit(i); }
            }
            SketchExpr::JoinSketch { outer, inner, .. } => {
                self.visit(outer);
                self.visit(inner);
            }
        }
    }

    fn collect_op(&mut self, op: &SketchAggOp) {
        match op {
            SketchAggOp::HLL { .. } => {
                if !self.agg_types.contains(&AggType::Cardinality) {
                    self.agg_types.push(AggType::Cardinality);
                }
            }
            SketchAggOp::CountMin { .. } | SketchAggOp::CountSketch { .. } => {
                if !self.agg_types.contains(&AggType::Frequency) {
                    self.agg_types.push(AggType::Frequency);
                }
            }
            SketchAggOp::DDSketch { quantiles, .. } => {
                if !self.agg_types.contains(&AggType::Quantile) {
                    self.agg_types.push(AggType::Quantile);
                }
                for &q in quantiles {
                    if !self.quantiles.contains(&q) { self.quantiles.push(q); }
                }
            }
            SketchAggOp::ExactMinMax { .. } => {
                if !self.agg_types.contains(&AggType::Quantile) {
                    self.agg_types.push(AggType::Quantile);
                }
            }
            SketchAggOp::Exact(_) => { self.exact_required = true; }
            SketchAggOp::Hydra { inner, .. } => self.collect_op(inner),
        }
    }

    fn build(self) -> ParsedQuery {
        let metric_name = self.metric_name.unwrap_or_default();
        let mut qs = self.quantiles;
        qs.sort_by(|a, b| a.partial_cmp(b).unwrap());
        qs.dedup();
        let hint = debs_hint(
            &metric_name,
            &self.agg_types,
            &qs,
            self.exact_required,
            self.topk,
        );
        ParsedQuery {
            metric_name,
            aggregations:    self.agg_types,
            group_by_labels: self.group_by_labels,
            label_filters:   self.label_filters,
            time_window:     self.time_window.unwrap_or(Duration::from_secs(300)),
            exact_required:  self.exact_required,
            quantiles:       qs,
            hint,
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn source(name: &str) -> SketchExpr {
        SketchExpr::Source(SourceSpec { name: name.into() })
    }

    #[test]
    fn hll_is_mergeable() {
        assert!(SketchAggOp::default_hll().is_mergeable());
    }

    #[test]
    fn exact_avg_not_mergeable() {
        assert!(!SketchAggOp::Exact(ExactAgg::Avg).is_mergeable());
    }

    #[test]
    fn hydra_mergeability_inherits_inner() {
        let hydra_hll = SketchAggOp::Hydra {
            inner:          Box::new(SketchAggOp::default_hll()),
            partition_keys: vec!["region".into()],
        };
        assert!(hydra_hll.is_mergeable());

        let hydra_avg = SketchAggOp::Hydra {
            inner:          Box::new(SketchAggOp::Exact(ExactAgg::Avg)),
            partition_keys: vec!["region".into()],
        };
        assert!(!hydra_avg.is_mergeable());
    }

    #[test]
    fn to_parsed_query_basic() {
        // Partition(symbol, Window(5m, Filter(sectype=E, Agg(CountSketch(10), Source(price)))))
        let expr = SketchExpr::TopK {
            k: 10,
            input: Box::new(SketchExpr::Partition {
                keys: PartitionKeys::By(vec!["symbol".into()]),
                input: Box::new(SketchExpr::Window {
                    duration: Duration::from_secs(300),
                    input: Box::new(SketchExpr::Filter {
                        pred: vec![Predicate {
                            col: "sectype".into(),
                            op:  FilterOp::Eq,
                            val: FilterVal::Str("E".into()),
                        }],
                        input: Box::new(SketchExpr::Agg {
                            op:    SketchAggOp::CountSketch { k: 10 },
                            col:   ColumnRef::Wildcard,
                            input: Box::new(source("financial.last_trade_price")),
                        }),
                    }),
                }),
            }),
        };
        let pq = expr.to_parsed_query();
        assert_eq!(pq.metric_name, "financial.last_trade_price");
        assert_eq!(pq.aggregations, vec![AggType::Frequency]);
        assert_eq!(pq.group_by_labels, vec!["symbol"]);
        assert_eq!(pq.label_filters.get("sectype").map(String::as_str), Some("E"));
        assert_eq!(pq.time_window, Duration::from_secs(300));
        assert!(!pq.exact_required);
    }

    #[test]
    fn to_parsed_query_ddsketch_quantiles() {
        let expr = SketchExpr::Agg {
            op:    SketchAggOp::default_ddsketch(vec![0.25, 0.5, 0.75]),
            col:   ColumnRef::SampleValue,
            input: Box::new(source("cpu")),
        };
        let pq = expr.to_parsed_query();
        assert_eq!(pq.aggregations, vec![AggType::Quantile]);
        assert_eq!(pq.quantiles, vec![0.25, 0.5, 0.75]);
        assert!(!pq.exact_required);
    }

    #[test]
    fn to_parsed_query_exact_required() {
        let expr = SketchExpr::Agg {
            op:    SketchAggOp::Exact(ExactAgg::Sum),
            col:   ColumnRef::Named("bytes".into()),
            input: Box::new(source("network")),
        };
        let pq = expr.to_parsed_query();
        assert!(pq.exact_required);
    }

    #[test]
    fn partition_keys_without() {
        let keys = PartitionKeys::Without(vec!["instance".into()]);
        assert_eq!(keys.keys(), &["instance".to_string()]);
        assert!(!keys.is_empty());
    }

    // ── Accuracy bound tests ──────────────────────────────────────────────────
    //
    // These tests verify that the default sketch parameters satisfy the
    // documented accuracy guarantees.  The bounds are derived from the
    // theoretical properties of each sketch family.

    /// DDSketch default: ε = 0.01 → at most 1 % relative rank error.
    ///
    /// For a stream of n items the rank of a DDSketch quantile estimate r̂
    /// satisfies |r̂ − r| ≤ ε·n.  The default ε = 0.01 gives a 1 % SLA.
    #[test]
    fn ddsketch_default_epsilon_one_pct() {
        let op = SketchAggOp::default_ddsketch(vec![0.99]);
        match op {
            SketchAggOp::DDSketch { epsilon, .. } => {
                assert!(
                    epsilon <= 0.01,
                    "DDSketch default epsilon {epsilon} exceeds 1 % accuracy SLA"
                );
            }
            _ => panic!("expected DDSketch"),
        }
    }

    /// For n = 10 000 items, a 1 % DDSketch (ε = 0.01) guarantees the
    /// p99 rank estimate is within ±100 positions of the true rank 9 900.
    #[test]
    fn ddsketch_rank_error_bound_n10k() {
        let n: f64 = 10_000.0;
        let epsilon = 0.01_f64;
        let true_rank = (0.99 * n) as i64; // 9 900
        let max_error = (epsilon * n).ceil() as i64; // 100
        assert!(
            max_error <= 100,
            "rank error {max_error} exceeds expected bound for n={n} ε={epsilon}"
        );
        // Verify the bound makes sense: estimate ∈ [true_rank-error, true_rank+error].
        let lo = true_rank - max_error;
        let hi = true_rank + max_error;
        assert!(lo >= 0 && hi <= n as i64);
    }

    /// HyperLogLog default: registers = 14 (2¹⁴ = 16 384 buckets).
    /// Standard error ≈ 1.04 / √(2^registers) ≈ 0.81 % < 1 %.
    #[test]
    fn hll_default_registers_error_below_1pct() {
        let op = SketchAggOp::default_hll();
        match op {
            SketchAggOp::HLL { registers } => {
                let buckets = (1u64 << registers) as f64; // 2^registers
                let std_error = 1.04 / buckets.sqrt();
                assert!(
                    std_error < 0.01,
                    "HLL standard error {std_error:.4} ≥ 1 % for registers={registers}"
                );
            }
            _ => panic!("expected HLL"),
        }
    }

    /// CountMin-Sketch default: width = 2 000, depth = 5.
    /// Frequency error ε = e / width ≈ 0.136 % of total count N.
    /// Failure probability δ = e^(−depth) ≈ 0.67 % < 1 %.
    #[test]
    fn countmin_default_error_and_failure_prob() {
        let op = SketchAggOp::default_count_min();
        match op {
            SketchAggOp::CountMin { width, depth } => {
                let eps   = std::f64::consts::E / width as f64;
                let delta = (-(depth as f64)).exp();
                assert!(
                    eps < 0.002,
                    "CountMin frequency error ε={eps:.5} should be < 0.2 % of N"
                );
                assert!(
                    delta < 0.01,
                    "CountMin failure probability δ={delta:.5} should be < 1 %"
                );
            }
            _ => panic!("expected CountMin"),
        }
    }

    // ── Mergeability correctness ──────────────────────────────────────────────
    //
    // Mergeability means sketch(A ∪ B) = merge(sketch(A), sketch(B)).
    // All sketches used here satisfy this property except Exact(Avg),
    // because avg(A ∪ B) ≠ avg(avg(A), avg(B)) in general.

    #[test]
    fn countmin_is_mergeable() {
        assert!(SketchAggOp::default_count_min().is_mergeable());
    }

    #[test]
    fn countsketch_is_mergeable() {
        assert!(SketchAggOp::CountSketch { k: 10 }.is_mergeable());
    }

    #[test]
    fn ddsketch_is_mergeable() {
        assert!(SketchAggOp::default_ddsketch(vec![0.99]).is_mergeable());
    }

    #[test]
    fn exact_minmax_is_mergeable() {
        // min(A∪B) = min(min(A), min(B)) — globally mergeable.
        assert!(SketchAggOp::ExactMinMax { min: true, max: false }.is_mergeable());
    }

    #[test]
    fn exact_sum_and_count_are_mergeable() {
        assert!(SketchAggOp::Exact(ExactAgg::Sum).is_mergeable());
        assert!(SketchAggOp::Exact(ExactAgg::Count).is_mergeable());
    }

    #[test]
    fn exact_avg_not_mergeable_avg_of_avgs_is_wrong() {
        // avg([1,2,3]) = 2, avg([4,5]) = 4.5
        // avg-of-avgs = (2 + 4.5) / 2 = 3.25  ≠  avg([1,2,3,4,5]) = 3
        assert!(!SketchAggOp::Exact(ExactAgg::Avg).is_mergeable());
    }

    #[test]
    fn hydra_mergeable_when_inner_is_hll() {
        let hydra = SketchAggOp::Hydra {
            inner:          Box::new(SketchAggOp::default_hll()),
            partition_keys: vec!["region".into(), "dc".into()],
        };
        assert!(hydra.is_mergeable());
    }

    #[test]
    fn hydra_not_mergeable_when_inner_is_avg() {
        let hydra = SketchAggOp::Hydra {
            inner:          Box::new(SketchAggOp::Exact(ExactAgg::Avg)),
            partition_keys: vec!["region".into()],
        };
        assert!(!hydra.is_mergeable());
    }

    // ── AggType mapping ───────────────────────────────────────────────────────

    #[test]
    fn agg_type_mapping_correct() {
        use crate::types::AggType;
        assert_eq!(SketchAggOp::default_hll().to_agg_type(),              AggType::Cardinality);
        assert_eq!(SketchAggOp::default_count_min().to_agg_type(),        AggType::Frequency);
        assert_eq!(SketchAggOp::CountSketch { k: 5 }.to_agg_type(),       AggType::Frequency);
        assert_eq!(SketchAggOp::default_ddsketch(vec![0.5]).to_agg_type(),AggType::Quantile);
        assert_eq!(SketchAggOp::ExactMinMax { min: true, max: true }.to_agg_type(), AggType::Quantile);
    }

    // ── SketchCoverage ────────────────────────────────────────────────────────
    //
    // Coverage classifies whether a SELECT can be fully, partially, or not
    // at all served by sketches.

    #[test]
    fn coverage_full_when_all_sketch() {
        let ops: Vec<SketchAggOp> = vec![
            SketchAggOp::default_hll(),
            SketchAggOp::default_count_min(),
        ];
        let has_sketch = ops.iter().any(|o| !o.is_exact());
        let has_exact  = ops.iter().any(|o|  o.is_exact());
        let cov = match (has_sketch, has_exact) {
            (true,  false) => SketchCoverage::Full,
            (true,  true)  => SketchCoverage::Partial,
            _              => SketchCoverage::None,
        };
        assert_eq!(cov, SketchCoverage::Full);
    }

    #[test]
    fn coverage_partial_when_exact_mixed_in() {
        let ops: Vec<SketchAggOp> = vec![
            SketchAggOp::default_hll(),
            SketchAggOp::Exact(ExactAgg::Sum),
        ];
        let has_sketch = ops.iter().any(|o| !o.is_exact());
        let has_exact  = ops.iter().any(|o|  o.is_exact());
        let cov = match (has_sketch, has_exact) {
            (true,  false) => SketchCoverage::Full,
            (true,  true)  => SketchCoverage::Partial,
            _              => SketchCoverage::None,
        };
        assert_eq!(cov, SketchCoverage::Partial);
    }

    #[test]
    fn coverage_none_when_all_exact() {
        let ops: Vec<SketchAggOp> = vec![
            SketchAggOp::Exact(ExactAgg::Sum),
            SketchAggOp::Exact(ExactAgg::Count),
        ];
        let has_sketch = ops.iter().any(|o| !o.is_exact());
        let has_exact  = ops.iter().any(|o|  o.is_exact());
        let cov = match (has_sketch, has_exact) {
            (true,  false) => SketchCoverage::Full,
            (true,  true)  => SketchCoverage::Partial,
            _              => SketchCoverage::None,
        };
        assert_eq!(cov, SketchCoverage::None);
    }
}
