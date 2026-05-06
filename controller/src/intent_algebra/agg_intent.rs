//! Layer 3 aggregation-intent vocabulary.
//!
//! Per `controller/docs/design.md` §6 "`AggIntent` — what to compute, not
//! how" (around line ~468). L3 carries intent ("compute a quantile to
//! ε=0.01 accuracy"). The choice between `HashAgg` / `SortAgg` /
//! `SketchAgg(KLL{k=200})` is made by L4 cost-aware rules, not encoded
//! here.
//!
//! Intent vs operator distinction. `AggIntent::TopK` is an *intent* (a
//! dedicated heavy-hitter sketch primitive — SpaceSaving, CMS-with-heap
//! — computes it in a single pass). The generic `Sort + Limit` operator
//! pair survives in `QueryExpr` for non-heavy-hitter cases (`ORDER BY
//! name LIMIT 10`). L1→L2→L3 lowering picks one or the other
//! deterministically.
//!
//! No `QuantileOverTime` intent. The window is fully captured by the
//! surrounding `QueryExpr::Window` node; the quantile *operation* is the
//! same regardless. PromQL `quantile_over_time(0.99, m[5m])` lowers to
//! `Window{size=5m} → Aggregate{aggs:[Quantile{q=0.99}]}`.
//!
//! `Rate` and `Increase` survive that argument because they include
//! PromQL's counter-reset adjustment, a non-trivial transformation that
//! exact `Sum` does not perform.

#![allow(dead_code)]

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::intent_algebra::schema::{Column, DataType};
use crate::types_v2::AccuracyTarget;

/// "What to compute" at L3 — vocabulary the planner pivots on. See module
/// doc for the intent vs operator distinction.
///
/// Variants intentionally mirror `design.md` §6 line ~468; data-model-
/// agnostic intents come first, time-series-streaming derivatives
/// (`Rate` / `Increase`) come last.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AggIntent {
    // ── Data-model-agnostic ──────────────────────────────────────────────
    /// COUNT(*) / `count` — number of rows / samples per group.
    /// `accuracy: Exact` selects an exact counter; `Epsilon` / `EpsilonDelta`
    /// unlock CMS / linear-counting sketch families.
    Count {
        accuracy: AccuracyTarget,
    },
    /// SUM(col). Always exact at L3 — no approximation intent for `Sum`
    /// in the catalog (`design.md` §6 line ~485).
    Sum,
    /// Per-group minimum — exact at L3.
    Min,
    /// Per-group maximum — exact at L3.
    Max,
    /// Arithmetic mean. Exact at L3; sketch backends fold this onto a
    /// `Quantile{q=0.5}` only when the cost model allows the relaxation.
    Avg,
    /// Compute the φ-th quantile (0 ≤ q ≤ 1) to the given accuracy.
    /// Sketch families: KLL, DDSketch, t-digest.
    Quantile {
        q: f64,
        accuracy: AccuracyTarget,
    },
    /// Heavy-hitter top-k. Distinct from generic `Sort + Limit` because
    /// a dedicated sketch primitive (SpaceSaving, CMS-with-heap,
    /// Misra-Gries) computes it as a single operation. L1→L2→L3 lowering
    /// produces this when it recognises `topk(k, …)` (PromQL) or
    /// `ORDER BY count DESC LIMIT k` (SQL).
    TopK {
        k: usize,
        accuracy: AccuracyTarget,
    },
    /// COUNT DISTINCT — number of distinct values in the input column,
    /// to the given accuracy. Sketch families: HLL, theta-sketch.
    Cardinality {
        accuracy: AccuracyTarget,
    },
    /// Frequency of a key in the input — `count(*) WHERE key = k` modeled
    /// as a sketch query. Sketch families: CMS, count-min-log.
    Frequency {
        accuracy: AccuracyTarget,
    },

    // ── Time-series streaming derivatives ────────────────────────────────
    /// Per-second average derivative with PromQL's counter-reset
    /// adjustment. Specialized — exact `Sum / Count over Window` does
    /// NOT serve this intent.
    Rate {
        window: Duration,
    },
    /// Cumulative increase over the given window with counter-reset
    /// adjustment. Specialized — see `Rate` above.
    Increase {
        window: Duration,
    },
}

impl AggIntent {
    /// Output column name + type produced by this intent when applied to
    /// `input`. Used by `QueryExpr::Aggregate`'s schema-derivation rule
    /// (`design.md` §6 schema-flow table: "one new column per entry in
    /// `aggs`, each named and typed by `AggIntent::output_type(input_field)`").
    ///
    /// PromQL convention: aggregate column name = intent kind (`count`,
    /// `quantile_0_99`, …) so consumers can locate it without an alias
    /// lookup.
    pub fn output_column(&self, input: &Column) -> Column {
        match self {
            AggIntent::Count { .. } => Column {
                name: "count".into(),
                dtype: DataType::Int64,
                nullable: false,
            },
            AggIntent::Sum => Column {
                name: "sum".into(),
                dtype: input.dtype.clone(),
                nullable: false,
            },
            AggIntent::Min => Column {
                name: "min".into(),
                dtype: input.dtype.clone(),
                nullable: input.nullable,
            },
            AggIntent::Max => Column {
                name: "max".into(),
                dtype: input.dtype.clone(),
                nullable: input.nullable,
            },
            AggIntent::Avg => Column {
                name: "avg".into(),
                dtype: DataType::Float64,
                nullable: false,
            },
            AggIntent::Quantile { q, .. } => Column {
                name: format!("quantile_{}", quantile_suffix(*q)),
                dtype: DataType::Float64,
                nullable: false,
            },
            AggIntent::TopK { k, .. } => Column {
                name: format!("topk_{k}"),
                // TopK output is a struct/list per row; modeled as Utf8
                // for L3 (the L4 sketch-bound IR upgrades the dtype).
                dtype: DataType::Utf8,
                nullable: false,
            },
            AggIntent::Cardinality { .. } => Column {
                name: "cardinality".into(),
                dtype: DataType::Int64,
                nullable: false,
            },
            AggIntent::Frequency { .. } => Column {
                name: "frequency".into(),
                dtype: DataType::Int64,
                nullable: false,
            },
            AggIntent::Rate { .. } => Column {
                name: "rate".into(),
                dtype: DataType::Float64,
                nullable: false,
            },
            AggIntent::Increase { .. } => Column {
                name: "increase".into(),
                dtype: DataType::Float64,
                nullable: false,
            },
        }
    }
}

/// `0.99` → `"0_99"`, `0.5` → `"0_5"`. Used by `Quantile` output naming
/// so `quantile_0_99` is a valid identifier downstream.
fn quantile_suffix(q: f64) -> String {
    let mut s = format!("{q}");
    if let Some(stripped) = s.strip_prefix('-') {
        s = format!("neg_{stripped}");
    }
    s.replace('.', "_")
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent_algebra::schema::{Column, DataType};

    fn col(name: &str, dtype: DataType) -> Column {
        Column {
            name: name.into(),
            dtype,
            nullable: false,
        }
    }

    #[test]
    fn agg_intent_serde_roundtrip() {
        let cases = vec![
            AggIntent::Count {
                accuracy: AccuracyTarget::Exact,
            },
            AggIntent::Sum,
            AggIntent::Min,
            AggIntent::Max,
            AggIntent::Avg,
            AggIntent::Quantile {
                q: 0.99,
                accuracy: AccuracyTarget::Epsilon(0.01),
            },
            AggIntent::TopK {
                k: 10,
                accuracy: AccuracyTarget::Epsilon(0.05),
            },
            AggIntent::Cardinality {
                accuracy: AccuracyTarget::EpsilonDelta {
                    eps: 0.01,
                    delta: 0.001,
                },
            },
            AggIntent::Frequency {
                accuracy: AccuracyTarget::Epsilon(0.01),
            },
            AggIntent::Rate {
                window: Duration::from_secs(60),
            },
            AggIntent::Increase {
                window: Duration::from_secs(300),
            },
        ];
        for variant in cases {
            let json = serde_json::to_string(&variant).unwrap();
            let back: AggIntent = serde_json::from_str(&json).unwrap();
            assert_eq!(variant, back, "round-trip failed for {variant:?}");
        }
    }

    #[test]
    fn output_column_names_are_intent_keyed() {
        let v = col("value", DataType::Float64);
        assert_eq!(
            AggIntent::Count {
                accuracy: AccuracyTarget::Exact,
            }
            .output_column(&v)
            .name,
            "count"
        );
        assert_eq!(AggIntent::Sum.output_column(&v).name, "sum");
        assert_eq!(
            AggIntent::Quantile {
                q: 0.99,
                accuracy: AccuracyTarget::Epsilon(0.01),
            }
            .output_column(&v)
            .name,
            "quantile_0_99"
        );
        assert_eq!(
            AggIntent::TopK {
                k: 5,
                accuracy: AccuracyTarget::Exact,
            }
            .output_column(&v)
            .name,
            "topk_5"
        );
    }

    #[test]
    fn quantile_output_is_float64() {
        let v = col("value", DataType::Int64);
        let out = AggIntent::Quantile {
            q: 0.5,
            accuracy: AccuracyTarget::Epsilon(0.01),
        }
        .output_column(&v);
        assert!(matches!(out.dtype, DataType::Float64));
    }

    #[test]
    fn sum_preserves_input_dtype() {
        let int_col = col("c", DataType::Int64);
        let float_col = col("c", DataType::Float64);
        assert!(matches!(
            AggIntent::Sum.output_column(&int_col).dtype,
            DataType::Int64
        ));
        assert!(matches!(
            AggIntent::Sum.output_column(&float_col).dtype,
            DataType::Float64
        ));
    }
}
