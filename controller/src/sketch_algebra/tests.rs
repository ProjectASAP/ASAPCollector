//! Integration tests for the L4 IR + `Bind*` rules.

#![cfg(test)]

use std::time::Duration;

use crate::intent_algebra::{
    AggIntent, LabelFilter, QueryExpr, Schema, Source, WindowKind,
};
use crate::intent_algebra::schema::{Column, DataType};
use crate::sketch_algebra::lower::bind_query_expr;
use crate::sketch_algebra::params::{KllParams, SketchKind, SketchParams};
use crate::sketch_algebra::rules::{
    bind_ddsketch_quantile::BindDDSketchOnQuantile, bind_kll_quantile::BindKllOnQuantile, Rule,
};
use crate::sketch_algebra::sketch_expr::{EstimateOp, MergeAlgebra, SketchExpr};
use crate::types_v2::{AccuracyTarget, BindingName};

// ── Test fixtures ─────────────────────────────────────────────────────────────

fn col(name: &str, dtype: DataType) -> Column {
    Column {
        name: name.into(),
        dtype,
        nullable: false,
    }
}

fn ts_scan() -> QueryExpr {
    QueryExpr::Scan {
        source: Source::TimeSeries {
            metric: "http_request_duration_seconds".into(),
        },
        label_filters: vec![LabelFilter {
            label: "service".into(),
            equals: "api".into(),
        }],
        schema: Schema::with_time_index(
            vec![
                col("ts", DataType::Timestamp),
                col("service", DataType::Utf8),
                col("value", DataType::Float64),
            ],
            0,
            vec![vec![0, 1]],
        ),
    }
}

fn windowed_scan() -> QueryExpr {
    QueryExpr::Window {
        kind: WindowKind::Sliding,
        size: Duration::from_secs(300),
        slide: None,
        child: Box::new(ts_scan()),
    }
}

fn agg_quantile(q: f64, accuracy: AccuracyTarget) -> QueryExpr {
    QueryExpr::Aggregate {
        by: vec![],
        aggs: vec![AggIntent::Quantile { q, accuracy }],
        having: None,
        child: Box::new(windowed_scan()),
    }
}

// ── Serde round-trip across all variants ──────────────────────────────────────

#[test]
fn sketch_expr_serde_roundtrip() {
    use crate::sketch_algebra::params::{
        CmsParams, CountSketchParams, DDSketchParams, HllParams,
    };
    let cases = vec![
        SketchExpr::Logical(windowed_scan()),
        SketchExpr::SketchAgg {
            sketch_type: SketchKind::Kll,
            params: SketchParams::Kll(KllParams { k: 200 }),
            child: Box::new(SketchExpr::Logical(windowed_scan())),
        },
        SketchExpr::SketchEstimate {
            op: EstimateOp::Quantile { q: 0.5 },
            child: Box::new(SketchExpr::SketchAgg {
                sketch_type: SketchKind::DDSketch,
                params: SketchParams::DDSketch(DDSketchParams { alpha: 0.005 }),
                child: Box::new(SketchExpr::Logical(windowed_scan())),
            }),
        },
        SketchExpr::SketchMerge {
            algebra: MergeAlgebra::Union,
            children: vec![
                SketchExpr::SketchAgg {
                    sketch_type: SketchKind::Hll,
                    params: SketchParams::Hll(HllParams { precision: 14 }),
                    child: Box::new(SketchExpr::Logical(windowed_scan())),
                },
                SketchExpr::SketchAgg {
                    sketch_type: SketchKind::Hll,
                    params: SketchParams::Hll(HllParams { precision: 14 }),
                    child: Box::new(SketchExpr::Logical(windowed_scan())),
                },
            ],
        },
        SketchExpr::LetBinding {
            name: BindingName::new("kll_state"),
            expr: Box::new(SketchExpr::SketchAgg {
                sketch_type: SketchKind::CountSketch,
                params: SketchParams::CountSketch(CountSketchParams {
                    w: 2048,
                    d: 5,
                    with_heap: false,
                }),
                child: Box::new(SketchExpr::Logical(windowed_scan())),
            }),
            child: Box::new(SketchExpr::Ref {
                name: BindingName::new("kll_state"),
            }),
        },
        SketchExpr::Ref {
            name: BindingName::new("alone"),
        },
        SketchExpr::SketchAgg {
            sketch_type: SketchKind::Cms,
            params: SketchParams::Cms(CmsParams { w: 2048, d: 5 }),
            child: Box::new(SketchExpr::Logical(windowed_scan())),
        },
    ];
    for c in cases {
        let json = serde_json::to_string(&c).unwrap();
        let back: SketchExpr = serde_json::from_str(&json).unwrap();
        assert_eq!(c, back);
    }
}

// ── Bind rule tests ───────────────────────────────────────────────────────────

#[test]
fn bind_kll_quantile_basic() {
    // The KLL rule on its own (priority 5) — DDSketch (priority 6) wins
    // the dispatcher tie-break, so test the KLL rule's `apply` directly.
    let expr = agg_quantile(0.99, AccuracyTarget::Epsilon(0.01));
    let bound = BindKllOnQuantile
        .apply(&expr, &AccuracyTarget::Epsilon(0.01))
        .expect("KLL rule should bind a Quantile{0.99, ε=0.01}");
    match bound {
        SketchExpr::SketchEstimate { op, child } => {
            assert_eq!(op, EstimateOp::Quantile { q: 0.99 });
            match *child {
                SketchExpr::SketchAgg {
                    sketch_type,
                    params,
                    child,
                } => {
                    assert_eq!(sketch_type, SketchKind::Kll);
                    assert_eq!(params, SketchParams::Kll(KllParams { k: 200 }));
                    assert!(matches!(*child, SketchExpr::Logical(QueryExpr::Window { .. })));
                }
                other => panic!("expected SketchAgg, got {other:?}"),
            }
        }
        other => panic!("expected SketchEstimate, got {other:?}"),
    }
}

#[test]
fn bind_ddsketch_quantile_basic() {
    let expr = agg_quantile(0.99, AccuracyTarget::Epsilon(0.01));
    let bound = BindDDSketchOnQuantile
        .apply(&expr, &AccuracyTarget::Epsilon(0.01))
        .expect("DDSketch rule should bind a Quantile{0.99, ε=0.01}");
    match bound {
        SketchExpr::SketchEstimate { op, child } => {
            assert_eq!(op, EstimateOp::Quantile { q: 0.99 });
            match *child {
                SketchExpr::SketchAgg {
                    sketch_type,
                    params,
                    ..
                } => {
                    assert_eq!(sketch_type, SketchKind::DDSketch);
                    match params {
                        SketchParams::DDSketch(p) => assert!((p.alpha - 0.01).abs() < 1e-12),
                        other => panic!("expected DDSketchParams, got {other:?}"),
                    }
                }
                other => panic!("expected SketchAgg, got {other:?}"),
            }
        }
        other => panic!("expected SketchEstimate, got {other:?}"),
    }
}

/// Cost-aware rule selection: the dispatcher should pick DDSketch over
/// KLL for an explicit ε-driven Quantile because DDSketch has higher
/// `priority()` (6 vs 5) — that matches the legacy
/// `algebra::directory::sketch_type_for_agg` default for SP-2/SP-4.
#[test]
fn bind_picks_ddsketch_over_kll_when_eps_explicit() {
    let expr = agg_quantile(0.99, AccuracyTarget::Epsilon(0.01));
    let bound =
        bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).expect("bind_query_expr should not error");
    match bound {
        SketchExpr::SketchEstimate { child, .. } => match *child {
            SketchExpr::SketchAgg { sketch_type, .. } => {
                assert_eq!(
                    sketch_type,
                    SketchKind::DDSketch,
                    "dispatcher should pick DDSketch (priority 6) over KLL (priority 5) on ε-driven Quantile"
                );
            }
            other => panic!("expected SketchAgg, got {other:?}"),
        },
        other => panic!("expected SketchEstimate, got {other:?}"),
    }
}

#[test]
fn bind_cms_topk_basic() {
    let expr = QueryExpr::Aggregate {
        by: vec![],
        aggs: vec![AggIntent::TopK {
            k: 10,
            accuracy: AccuracyTarget::EpsilonDelta {
                eps: 0.01,
                delta: 0.001,
            },
        }],
        having: None,
        child: Box::new(windowed_scan()),
    };
    let bound = bind_query_expr(
        &expr,
        AccuracyTarget::EpsilonDelta {
            eps: 0.01,
            delta: 0.001,
        },
    )
    .expect("bind_query_expr should not error");
    match bound {
        SketchExpr::SketchEstimate { op, child } => {
            assert_eq!(op, EstimateOp::TopK { k: 10 });
            match *child {
                SketchExpr::SketchAgg {
                    sketch_type,
                    params,
                    ..
                } => {
                    assert_eq!(sketch_type, SketchKind::CountSketch);
                    match params {
                        SketchParams::CountSketch(p) => {
                            assert!(p.with_heap, "TopK binding must enable the heavy-hitter heap");
                            assert!(p.w >= 2);
                            assert!(p.d >= 1);
                        }
                        other => panic!("expected CountSketchParams, got {other:?}"),
                    }
                }
                other => panic!("expected SketchAgg, got {other:?}"),
            }
        }
        other => panic!("expected SketchEstimate, got {other:?}"),
    }
}

#[test]
fn bind_hll_cardinality_basic() {
    let expr = QueryExpr::Aggregate {
        by: vec![],
        aggs: vec![AggIntent::Cardinality {
            accuracy: AccuracyTarget::Epsilon(0.01),
        }],
        having: None,
        child: Box::new(windowed_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).expect("no error");
    match bound {
        SketchExpr::SketchEstimate { op, child } => {
            assert_eq!(op, EstimateOp::Cardinality);
            match *child {
                SketchExpr::SketchAgg {
                    sketch_type,
                    params,
                    ..
                } => {
                    assert_eq!(sketch_type, SketchKind::Hll);
                    match params {
                        SketchParams::Hll(p) => {
                            assert!(
                                p.precision >= 12,
                                "ε=0.01 should land on at least precision 12 (~1.6%) per the rung table"
                            );
                        }
                        other => panic!("expected HllParams, got {other:?}"),
                    }
                }
                other => panic!("expected SketchAgg, got {other:?}"),
            }
        }
        other => panic!("expected SketchEstimate, got {other:?}"),
    }
}

#[test]
fn bind_no_match_passes_through_logical() {
    // Sum is exact at L3 — no `Bind*` rule covers it. Should pass
    // through unchanged in `SketchExpr::Logical`.
    let expr = QueryExpr::Aggregate {
        by: vec![],
        aggs: vec![AggIntent::Sum],
        having: None,
        child: Box::new(windowed_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Exact).expect("no error");
    assert!(
        matches!(bound, SketchExpr::Logical(QueryExpr::Aggregate { .. })),
        "Sum should pass through as Logical(Aggregate{{Sum}})"
    );
}

#[test]
fn bind_exact_accuracy_disables_quantile_binding() {
    // Quantile under `AccuracyTarget::Exact` should NOT bind — the
    // optimizer falls back to an exact path. (Per design.md §6 line
    // ~1254 — "the sketch path is selected, not mandated".)
    let expr = agg_quantile(0.99, AccuracyTarget::Exact);
    let bound = bind_query_expr(&expr, AccuracyTarget::Exact).expect("no error");
    assert!(
        matches!(bound, SketchExpr::Logical(QueryExpr::Aggregate { .. })),
        "Exact accuracy should disable sketch binding and pass through as Logical"
    );
}

/// Two `SketchEstimate` parents reading different quantiles can share
/// one underlying `SketchAgg{KLL}` via `LetBinding` / `Ref`. Mirrors the
/// design.md §6 batched-queries example (line ~1326) — within the L4
/// IR, fan-in is expressible as a `LetBinding` whose bound expression
/// is the shared `SketchAgg`.
#[test]
fn let_binding_ref_through_sketch_dag() {
    let shared_agg = SketchExpr::SketchAgg {
        sketch_type: SketchKind::Kll,
        params: SketchParams::Kll(KllParams { k: 200 }),
        child: Box::new(SketchExpr::Logical(windowed_scan())),
    };
    let expr = SketchExpr::LetBinding {
        name: BindingName::new("kll_state"),
        expr: Box::new(shared_agg),
        child: Box::new(SketchExpr::SketchMerge {
            algebra: MergeAlgebra::Union,
            // Two `SketchEstimate` parents reading the shared sketch via
            // `Ref` — the design.md §6 line ~1339 two-tier fan-in shape.
            children: vec![
                SketchExpr::SketchEstimate {
                    op: EstimateOp::Quantile { q: 0.99 },
                    child: Box::new(SketchExpr::Ref {
                        name: BindingName::new("kll_state"),
                    }),
                },
                SketchExpr::SketchEstimate {
                    op: EstimateOp::Quantile { q: 0.95 },
                    child: Box::new(SketchExpr::Ref {
                        name: BindingName::new("kll_state"),
                    }),
                },
            ],
        }),
    };
    // Round-trip the DAG through serde to verify the multi-parent fan-in
    // shape survives wire encoding (the L4 type checker, when it lands,
    // will assert the matching sketch-state schema on each `Ref` reader).
    let json = serde_json::to_string(&expr).unwrap();
    let back: SketchExpr = serde_json::from_str(&json).unwrap();
    assert_eq!(expr, back);
}
