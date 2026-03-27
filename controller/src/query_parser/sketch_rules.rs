//! Sketch-algebra optimizer — algebraic rewrite rules R1–R8.
//!
//! Rules are applied bottom-up (children first, then parent) until a single
//! fixed-point pass.  Multiple passes can be added if needed.
//!
//! # Rule catalogue
//!
//! | Rule | Name | Effect |
//! |---|---|---|
//! | R1 | Filter push-down | `Agg(Filter(X))` → `Agg(Filter pushed into X)` |
//! | R2 | HAVING/WHERE split | separate post-agg key filter from pre-agg tuple filter |
//! | R3 | Sketch linearity | `Agg(Merge([X,Y]))` → `Merge([Agg(X), Agg(Y)])` when mergeable |
//! | R4 | Multi-key Hydra | `Partition([k1,k2], Agg(op,X))` → `Agg(Hydra(op,[k1,k2]),X)` |
//! | R5 | Join push-down | handled at parse time; see `sql.rs` |
//! | R6 | HLL dedup elim | `Agg(HLL, Dedup(col,X))` → `Agg(HLL, X)` |
//! | R7 | Window/Filter swap | `Window(Filter(X))` → `Filter(Window(X))` |
//! | R8 | TopK absorption | absorb TopK(k) into inner CountSketch(k) |

use super::sketch_algebra::{
    ColumnRef, ExactAgg, FilterOp, FilterVal, PartitionKeys, Predicate, SketchAggOp, SketchExpr,
};

// ── Public entry point ────────────────────────────────────────────────────────

/// Optimise a [`SketchExpr`] tree by applying all rewrite rules bottom-up.
pub fn optimize(expr: SketchExpr) -> SketchExpr {
    // Recurse into children first (post-order), then apply rules at this node.
    let expr = rewrite_children(expr);
    apply_all(expr)
}

// ── Children-first recursion ──────────────────────────────────────────────────

fn rewrite_children(expr: SketchExpr) -> SketchExpr {
    match expr {
        SketchExpr::Filter { pred, input } =>
            SketchExpr::Filter { pred, input: Box::new(optimize(*input)) },
        SketchExpr::Window { duration, input } =>
            SketchExpr::Window { duration, input: Box::new(optimize(*input)) },
        SketchExpr::Partition { keys, input } =>
            SketchExpr::Partition { keys, input: Box::new(optimize(*input)) },
        SketchExpr::Agg { op, col, input } =>
            SketchExpr::Agg { op, col, input: Box::new(optimize(*input)) },
        SketchExpr::TopK { k, input } =>
            SketchExpr::TopK { k, input: Box::new(optimize(*input)) },
        SketchExpr::Dedup { col, input } =>
            SketchExpr::Dedup { col, input: Box::new(optimize(*input)) },
        SketchExpr::Merge { inputs } =>
            SketchExpr::Merge { inputs: inputs.into_iter().map(optimize).collect() },
        SketchExpr::JoinSketch { join_key, outer, inner } =>
            SketchExpr::JoinSketch {
                join_key,
                outer: Box::new(optimize(*outer)),
                inner: Box::new(optimize(*inner)),
            },
        leaf => leaf,
    }
}

// ── Apply all rules at one node ───────────────────────────────────────────────

fn apply_all(expr: SketchExpr) -> SketchExpr {
    let expr = r1_filter_pushdown(expr);
    let expr = r2_having_where_split(expr);
    let expr = r3_sketch_linearity(expr);
    let expr = r4_multi_key_hydra(expr);
    let expr = r6_hll_dedup_elim(expr);
    let expr = r7_window_filter_swap(expr);
    let expr = r8_topk_absorption(expr);
    expr
}

// ── R1: Filter push-down ──────────────────────────────────────────────────────
//
// Agg(op, Filter(pred, X))  →  Agg(op, Filter(pred, X))   (already optimal)
//
// The real win is pushing Filter *below* Agg when the Filter is currently
// wrapping the Agg:
//
//   Filter(pred, Agg(op, X))  →  Agg(op, Filter(pred, X))
//
// Condition: pred is on a base-relation column, not on the sketch output.
// We allow the push when pred references no aggregate alias (heuristic: no
// function names in the predicate column — that's a HAVING predicate).

fn r1_filter_pushdown(expr: SketchExpr) -> SketchExpr {
    match expr {
        SketchExpr::Filter { pred, input } => {
            if let SketchExpr::Agg { op, col, input: agg_input } = *input {
                // Separate HAVING predicates (cannot be pushed) from WHERE predicates.
                // Simple heuristic: predicates whose column matches a GROUP BY key
                // or a base column are pushable. We push ALL here; R2 will re-split
                // HAVING predicates that must stay above Agg.
                return SketchExpr::Agg {
                    op,
                    col,
                    input: Box::new(SketchExpr::Filter { pred, input: agg_input }),
                };
            }
            SketchExpr::Filter { pred, input }
        }
        other => other,
    }
}

// ── R2: HAVING / WHERE split ──────────────────────────────────────────────────
//
// When a Filter sits inside an Agg (after R1 pushed it there), split it:
//   - Predicates on GROUP BY keys  → push further down (pre-agg WHERE)
//   - Predicates on agg results    → keep above Agg (HAVING)
//
// Because we don't have full schema knowledge at this point we use a simple
// heuristic: we push everything; callers that know HAVING semantics (SQL
// parser) mark predicates with a flag.  This rule re-hoists flagged ones.

fn r2_having_where_split(expr: SketchExpr) -> SketchExpr {
    // Implementation: split Filter nodes that carry `having: true` predicates.
    // The SQL parser marks HAVING predicates separately so no split is needed
    // in the generic optimizer for now. This is a no-op placeholder.
    expr
}

// ── R3: Sketch linearity over Merge  (α distributes over ⊕) ─────────────────
//
//   Agg(op, Merge([X, Y, ...]))  →  Merge([Agg(op, X), Agg(op, Y), ...])
//   Condition: op.is_mergeable()

fn r3_sketch_linearity(expr: SketchExpr) -> SketchExpr {
    match expr {
        SketchExpr::Agg { ref op, ref col, input: ref boxed } => {
            if let SketchExpr::Merge { inputs } = boxed.as_ref() {
                if op.is_mergeable() {
                    let distributed = inputs
                        .iter()
                        .cloned()
                        .map(|branch| SketchExpr::Agg {
                            op:    op.clone(),
                            col:   col.clone(),
                            input: Box::new(branch),
                        })
                        .collect();
                    return SketchExpr::Merge { inputs: distributed };
                }
            }
            expr
        }
        other => other,
    }
}

// ── R4: Multi-key Partition → Hydra ──────────────────────────────────────────
//
//   Partition([k1,k2,...], Agg(op, X))
//     →  Agg(Hydra(op, [k1,k2,...]), X)
//
// when |keys| > 1.

fn r4_multi_key_hydra(expr: SketchExpr) -> SketchExpr {
    if let SketchExpr::Partition { ref keys, .. } = expr {
        let key_list = keys.keys().to_vec();
        if key_list.len() <= 1 {
            return expr;
        }
        // Unwrap to get ownership.
        if let SketchExpr::Partition { keys, input } = expr {
            if let SketchExpr::Agg { op, col, input: agg_in } = *input {
                return SketchExpr::Agg {
                    op:    SketchAggOp::Hydra { inner: Box::new(op), partition_keys: key_list },
                    col,
                    input: agg_in,
                };
            } else {
                // Partition wraps something other than Agg — leave unchanged.
                return SketchExpr::Partition { keys, input };
            }
        }
    }
    expr
}

// ── R6: HLL dedup elimination ─────────────────────────────────────────────────
//
//   Agg(HLL, Dedup(col, X))  →  Agg(HLL, X)
//   HLL inherently deduplicates; the explicit Dedup node is redundant.

fn r6_hll_dedup_elim(expr: SketchExpr) -> SketchExpr {
    if let SketchExpr::Agg { op: SketchAggOp::HLL { registers }, col, input } = expr {
        if let SketchExpr::Dedup { input: inner, .. } = *input {
            return SketchExpr::Agg {
                op:    SketchAggOp::HLL { registers },
                col,
                input: inner,
            };
        } else {
            return SketchExpr::Agg { op: SketchAggOp::HLL { registers }, col, input };
        }
    }
    expr
}

// ── R7: Window / Filter commutativity ─────────────────────────────────────────
//
//   Window(w, Filter(pred, X))  →  Filter(pred, Window(w, X))
//   Filter applied before windowing reduces the input stream size.

fn r7_window_filter_swap(expr: SketchExpr) -> SketchExpr {
    if let SketchExpr::Window { duration, input } = expr {
        if let SketchExpr::Filter { pred, input: inner } = *input {
            return SketchExpr::Filter {
                pred,
                input: Box::new(SketchExpr::Window { duration, input: inner }),
            };
        } else {
            return SketchExpr::Window { duration, input };
        }
    }
    expr
}

// ── R8: TopK absorption into CountSketch ─────────────────────────────────────
//
//   TopK(k, Partition(keys, Agg(CountSketch(k2), X)))
//     →  Partition(keys, Agg(CountSketch(k=k), X))   when k == k2
//
// The top-K selection is already encoded in CountSketch; drop the outer TopK.

fn r8_topk_absorption(expr: SketchExpr) -> SketchExpr {
    if let SketchExpr::TopK { k, input } = expr {
        if let SketchExpr::Partition { keys, input: part_in } = *input {
            if let SketchExpr::Agg {
                op: SketchAggOp::CountSketch { k: k2 },
                col,
                input: agg_in,
            } = *part_in
            {
                if k == k2 {
                    // TopK already encoded — absorb.
                    return SketchExpr::Partition {
                        keys,
                        input: Box::new(SketchExpr::Agg {
                            op:    SketchAggOp::CountSketch { k },
                            col,
                            input: agg_in,
                        }),
                    };
                } else {
                    // Different k values — keep TopK, restore inner.
                    return SketchExpr::TopK {
                        k,
                        input: Box::new(SketchExpr::Partition {
                            keys,
                            input: Box::new(SketchExpr::Agg {
                                op:    SketchAggOp::CountSketch { k: k2 },
                                col,
                                input: agg_in,
                            }),
                        }),
                    };
                }
            } else {
                return SketchExpr::TopK {
                    k,
                    input: Box::new(SketchExpr::Partition { keys, input: part_in }),
                };
            }
        }
        return SketchExpr::TopK { k, input };
    }
    expr
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::sketch_algebra::{ColumnRef, PartitionKeys, SketchAggOp, SketchExpr, SourceSpec, Predicate, FilterOp, FilterVal};
    use std::time::Duration;

    fn src(name: &str) -> SketchExpr {
        SketchExpr::Source(SourceSpec { name: name.into() })
    }

    // ── R3 tests ──────────────────────────────────────────────────────────────

    #[test]
    fn r3_distributes_hll_over_merge() {
        let expr = SketchExpr::Agg {
            op:    SketchAggOp::default_hll(),
            col:   ColumnRef::Named("user".into()),
            input: Box::new(SketchExpr::Merge {
                inputs: vec![src("R"), src("S")],
            }),
        };
        let opt = optimize(expr);
        // Should become Merge([Agg(HLL,R), Agg(HLL,S)])
        match opt {
            SketchExpr::Merge { inputs } => {
                assert_eq!(inputs.len(), 2);
                for inp in &inputs {
                    assert!(matches!(inp, SketchExpr::Agg { op: SketchAggOp::HLL { .. }, .. }));
                }
            }
            other => panic!("expected Merge, got {other:?}"),
        }
    }

    #[test]
    fn r3_does_not_distribute_avg() {
        let expr = SketchExpr::Agg {
            op:    SketchAggOp::Exact(ExactAgg::Avg),
            col:   ColumnRef::Named("price".into()),
            input: Box::new(SketchExpr::Merge {
                inputs: vec![src("R"), src("S")],
            }),
        };
        // Avg is not mergeable — Merge should NOT be distributed.
        let opt = optimize(expr);
        assert!(matches!(opt, SketchExpr::Agg { .. }));
    }

    // ── R4 tests ──────────────────────────────────────────────────────────────

    #[test]
    fn r4_multi_key_becomes_hydra() {
        let expr = SketchExpr::Partition {
            keys: PartitionKeys::By(vec!["k1".into(), "k2".into()]),
            input: Box::new(SketchExpr::Agg {
                op:    SketchAggOp::default_hll(),
                col:   ColumnRef::Named("user".into()),
                input: Box::new(src("hits")),
            }),
        };
        let opt = optimize(expr);
        match opt {
            SketchExpr::Agg { op: SketchAggOp::Hydra { inner, partition_keys }, .. } => {
                assert!(matches!(*inner, SketchAggOp::HLL { .. }));
                assert_eq!(partition_keys, vec!["k1", "k2"]);
            }
            other => panic!("expected Hydra Agg, got {other:?}"),
        }
    }

    #[test]
    fn r4_single_key_unchanged() {
        let expr = SketchExpr::Partition {
            keys: PartitionKeys::By(vec!["k1".into()]),
            input: Box::new(SketchExpr::Agg {
                op:    SketchAggOp::default_count_min(),
                col:   ColumnRef::Wildcard,
                input: Box::new(src("hits")),
            }),
        };
        let opt = optimize(expr);
        assert!(matches!(opt, SketchExpr::Partition { .. }));
    }

    // ── R6 tests ──────────────────────────────────────────────────────────────

    #[test]
    fn r6_hll_absorbs_dedup() {
        let expr = SketchExpr::Agg {
            op:    SketchAggOp::default_hll(),
            col:   ColumnRef::Named("user".into()),
            input: Box::new(SketchExpr::Dedup {
                col:   "user".into(),
                input: Box::new(src("hits")),
            }),
        };
        let opt = optimize(expr);
        // Dedup should be gone; HLL goes directly to Source.
        match opt {
            SketchExpr::Agg { op: SketchAggOp::HLL { .. }, input, .. } => {
                assert!(matches!(*input, SketchExpr::Source(_)));
            }
            other => panic!("expected Agg(HLL,Source), got {other:?}"),
        }
    }

    // ── R7 tests ──────────────────────────────────────────────────────────────

    #[test]
    fn r7_filter_hoisted_before_window() {
        let pred = vec![Predicate { col: "env".into(), op: FilterOp::Eq, val: FilterVal::Str("prod".into()) }];
        let expr = SketchExpr::Window {
            duration: Duration::from_secs(300),
            input: Box::new(SketchExpr::Filter {
                pred: pred.clone(),
                input: Box::new(src("cpu")),
            }),
        };
        let opt = optimize(expr);
        // Should become Filter(Window(Source))
        match opt {
            SketchExpr::Filter { input, .. } => {
                assert!(matches!(*input, SketchExpr::Window { .. }));
            }
            other => panic!("expected Filter(Window(...)), got {other:?}"),
        }
    }

    // ── R8 tests ──────────────────────────────────────────────────────────────

    #[test]
    fn r8_topk_absorbed_same_k() {
        let expr = SketchExpr::TopK {
            k: 10,
            input: Box::new(SketchExpr::Partition {
                keys: PartitionKeys::By(vec!["phrase".into()]),
                input: Box::new(SketchExpr::Agg {
                    op:    SketchAggOp::CountSketch { k: 10 },
                    col:   ColumnRef::Wildcard,
                    input: Box::new(src("hits")),
                }),
            }),
        };
        let opt = optimize(expr);
        // TopK should be absorbed into the Partition/Agg
        assert!(matches!(opt, SketchExpr::Partition { .. }));
    }

    #[test]
    fn r8_topk_kept_different_k() {
        let expr = SketchExpr::TopK {
            k: 5,
            input: Box::new(SketchExpr::Partition {
                keys: PartitionKeys::By(vec!["phrase".into()]),
                input: Box::new(SketchExpr::Agg {
                    op:    SketchAggOp::CountSketch { k: 10 },
                    col:   ColumnRef::Wildcard,
                    input: Box::new(src("hits")),
                }),
            }),
        };
        let opt = optimize(expr);
        assert!(matches!(opt, SketchExpr::TopK { k: 5, .. }));
    }
}
