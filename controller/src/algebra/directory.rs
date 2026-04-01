//! Sketch directory — single source of truth for mapping aggregation
//! operations to sketch types, parameters, and memory estimates.
//!
//! Previously this logic was duplicated across:
//! - `planner/rules.rs` (`select_sketch_type`)
//! - `planner/stage_split.rs` (`agg_op_to_sketch_type`, `agg_op_to_sketch_params`,
//!    `estimated_sketch_memory_bytes`)
//! - `algebra/allocator.rs` (`sketch_type_for_op`)
//!
//! All callers now go through this module.

use crate::algebra::expr::{ExactAgg, SketchAggOp};
use crate::types::{
    AggType, CountMinSketchDefaults, CountSketchDefaults, SketchDefaults,
    SketchParams, SketchType,
};

// ── AggType → SketchType (high-level query mapping) ──────────────────────────

/// Pick the primary sketch family from a list of aggregation types.
///
/// Priority: Quantile → Cardinality → Frequency.  Falls back to DDSketch.
pub fn sketch_type_for_agg(aggs: &[AggType]) -> SketchType {
    for agg in aggs {
        match agg {
            AggType::Quantile    => return SketchType::DDSketch,
            AggType::Cardinality => return SketchType::HLL,
            AggType::Frequency   => return SketchType::CountSketch,
        }
    }
    SketchType::DDSketch
}

// ── SketchAggOp → SketchType ─────────────────────────────────────────────────

/// Resolve the concrete [`SketchType`] for a [`SketchAggOp`] IR node.
pub fn sketch_type_for_op(op: &SketchAggOp) -> SketchType {
    match op {
        SketchAggOp::DDSketch { .. } | SketchAggOp::ExactMinMax { .. } => SketchType::DDSketch,
        SketchAggOp::HLL { .. }        => SketchType::HLL,
        SketchAggOp::CountMin { .. }   => SketchType::CountMinSketch,
        SketchAggOp::CountSketch { .. } => SketchType::CountSketch,
        SketchAggOp::Hydra { inner, .. } => sketch_type_for_op(inner),
        SketchAggOp::Exact(_)          => SketchType::DDSketch,
    }
}

// ── SketchAggOp → SketchParams ───────────────────────────────────────────────

/// Derive [`SketchParams`] from a [`SketchAggOp`] IR node.
pub fn sketch_params_for_op(op: &SketchAggOp) -> SketchParams {
    match op {
        SketchAggOp::DDSketch { quantiles, epsilon } => SketchParams::DDSketch {
            relative_accuracy: *epsilon,
            quantiles: quantiles.clone(),
        },
        SketchAggOp::HLL { registers } => SketchParams::HLL {
            precision: *registers as u32,
        },
        SketchAggOp::CountMin { width, depth } => SketchParams::CountMinSketch {
            rows: *depth as u32,
            cols: *width,
            metric_name: String::new(),
        },
        SketchAggOp::CountSketch { width, depth } => SketchParams::CountSketch {
            epsilon: CountSketchDefaults::default().epsilon,
            delta: CountSketchDefaults::default().delta,
        },
        SketchAggOp::Hydra { inner, .. } => sketch_params_for_op(inner),
        SketchAggOp::ExactMinMax { .. } => SketchParams::DDSketch {
            relative_accuracy: 0.01,
            quantiles: vec![0.0, 1.0],
        },
        SketchAggOp::Exact(_) => SketchParams::default(),
    }
}

/// Combined (type, params) lookup — convenience for callers that need both.
pub fn sketch_type_and_params(op: &SketchAggOp) -> (SketchType, SketchParams) {
    (sketch_type_for_op(op), sketch_params_for_op(op))
}

// ── SketchAggOp → memory estimate ────────────────────────────────────────────

/// Estimated sketch memory footprint per series (bytes).
///
/// Used by `split_expr_by_stage` to decide whether to defer an operation
/// to a later pipeline stage when the budget is exceeded.
pub fn estimated_sketch_memory_bytes(op: &SketchAggOp) -> u64 {
    match op {
        SketchAggOp::DDSketch { .. } | SketchAggOp::ExactMinMax { .. } => 4_096,
        SketchAggOp::HLL { registers } => 1u64 << (*registers as u64),
        SketchAggOp::CountMin { width, depth } => (*width as u64) * (*depth as u64) * 8,
        SketchAggOp::CountSketch { width, depth } => (*width as u64) * (*depth as u64) * 8,
        SketchAggOp::Hydra { inner, partition_keys } => {
            let factor = 1u64 << partition_keys.len().min(10);
            estimated_sketch_memory_bytes(inner).saturating_mul(factor)
        }
        SketchAggOp::Exact(_) => 8,
    }
}

// ── SketchType + accuracy SLA → SketchParams (configurable defaults) ─────────

/// Build default [`SketchParams`] from a [`SketchDefaults`] config and accuracy SLA.
///
/// Query-specific quantiles override the configured grid when non-empty.
pub fn build_sketch_params(
    defaults: &SketchDefaults,
    st: &SketchType,
    accuracy_sla: f64,
    query_quantiles: &[f64],
) -> SketchParams {
    let acc = if accuracy_sla <= 0.0 {
        defaults.ddsketch.relative_accuracy
    } else {
        accuracy_sla
    };
    let quantiles: Vec<f64> = if !query_quantiles.is_empty() {
        query_quantiles.to_vec()
    } else {
        defaults.quantile_grid.clone()
    };
    match st {
        SketchType::DDSketch => SketchParams::DDSketch {
            relative_accuracy: acc,
            quantiles,
        },
        SketchType::KLL => {
            let k = ((1.0 / acc) as u32).max(defaults.kll.min_k);
            SketchParams::KLL { k, quantiles }
        }
        SketchType::HLL => {
            let d = &defaults.hll;
            let precision = if acc > d.precision_threshold { d.precision_coarse } else { d.precision_fine };
            SketchParams::HLL { precision }
        }
        SketchType::CountSketch => SketchParams::CountSketch {
            epsilon: defaults.count_sketch.epsilon,
            delta: defaults.count_sketch.delta,
        },
        SketchType::CountMinSketch => SketchParams::CountMinSketch {
            rows: defaults.count_min_sketch.rows,
            cols: defaults.count_min_sketch.cols,
            metric_name: defaults.count_min_sketch.metric_name.clone(),
        },
    }
}

/// Convenience: build default params using compiled-in defaults.
pub fn default_sketch_params(st: &SketchType, accuracy_sla: f64) -> SketchParams {
    build_sketch_params(&SketchDefaults::default(), st, accuracy_sla, &[])
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agg_type_quantile_maps_to_ddsketch() {
        assert_eq!(sketch_type_for_agg(&[AggType::Quantile]), SketchType::DDSketch);
    }

    #[test]
    fn agg_type_cardinality_maps_to_hll() {
        assert_eq!(sketch_type_for_agg(&[AggType::Cardinality]), SketchType::HLL);
    }

    #[test]
    fn agg_type_frequency_maps_to_countsketch() {
        assert_eq!(sketch_type_for_agg(&[AggType::Frequency]), SketchType::CountSketch);
    }

    #[test]
    fn empty_aggs_default_to_ddsketch() {
        assert_eq!(sketch_type_for_agg(&[]), SketchType::DDSketch);
    }

    #[test]
    fn op_ddsketch_yields_ddsketch_type_and_params() {
        let op = SketchAggOp::DDSketch { quantiles: vec![0.5], epsilon: 0.01 };
        let (st, p) = sketch_type_and_params(&op);
        assert_eq!(st, SketchType::DDSketch);
        assert!(matches!(p, SketchParams::DDSketch { .. }));
    }

    #[test]
    fn op_hll_yields_hll_type() {
        let op = SketchAggOp::HLL { registers: 14 };
        assert_eq!(sketch_type_for_op(&op), SketchType::HLL);
    }

    #[test]
    fn op_countmin_yields_countminsketch() {
        let op = SketchAggOp::CountMin { width: 2048, depth: 5 };
        assert_eq!(sketch_type_for_op(&op), SketchType::CountMinSketch);
    }

    #[test]
    fn hydra_delegates_to_inner() {
        let op = SketchAggOp::Hydra {
            inner: Box::new(SketchAggOp::HLL { registers: 14 }),
            partition_keys: vec!["k".into()],
        };
        assert_eq!(sketch_type_for_op(&op), SketchType::HLL);
    }

    #[test]
    fn memory_ddsketch() {
        let op = SketchAggOp::DDSketch { quantiles: vec![0.5], epsilon: 0.01 };
        assert_eq!(estimated_sketch_memory_bytes(&op), 4096);
    }

    #[test]
    fn memory_hydra_scales_by_partition_keys() {
        let inner = SketchAggOp::HLL { registers: 14 };
        let base_mem = estimated_sketch_memory_bytes(&inner);
        let op = SketchAggOp::Hydra {
            inner: Box::new(inner),
            partition_keys: vec!["a".into(), "b".into()],
        };
        assert_eq!(estimated_sketch_memory_bytes(&op), base_mem * 4);
    }

    #[test]
    fn configurable_defaults_override_quantile_grid() {
        let mut d = SketchDefaults::default();
        d.quantile_grid = vec![0.5, 0.99];
        let p = build_sketch_params(&d, &SketchType::DDSketch, 0.01, &[]);
        assert_eq!(p.quantiles(), &[0.5, 0.99]);
    }

    #[test]
    fn query_quantiles_override_grid() {
        let d = SketchDefaults::default();
        let p = build_sketch_params(&d, &SketchType::DDSketch, 0.01, &[0.1, 0.9]);
        assert_eq!(p.quantiles(), &[0.1, 0.9]);
    }
}
