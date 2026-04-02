//! Layer 5 — Physical plan IR.
//!
//! Maps the implementation-independent [`AggIntent`] (Layer 3) to concrete
//! sketch implementations ([`SketchType`] + [`SketchParams`]).
//!
//! The physical planner centralises the "intent → implementation" decision
//! that was previously scattered across `directory.rs` call sites in
//! `stage_split.rs` and `allocator.rs`.
//!
//! # Future extensions
//!
//! `resolve` will grow to accept deployment config and cost-model scores:
//! ```rust,ignore
//! resolve(intent, deployment_config, cost_table) -> PhysicalAggOp
//! ```

use crate::algebra::directory;
use crate::algebra::expr::AggIntent;
use crate::types::{SketchParams, SketchType};

/// A resolved physical aggregation operation.
///
/// This is the output of the physical planner: it knows which concrete
/// sketch implementation to use and with what parameters.
#[derive(Debug, Clone)]
pub struct PhysicalAggOp {
    /// The logical intent this was derived from.
    pub intent: AggIntent,
    /// Concrete sketch type (DDSketch, KLL, HLL, CountSketch, CountMinSketch).
    pub sketch_type: SketchType,
    /// Concrete sketch parameters.
    pub sketch_params: SketchParams,
    /// Estimated memory footprint per series (bytes).
    pub estimated_memory_bytes: u64,
}

/// Resolve an [`AggIntent`] into a [`PhysicalAggOp`] using default mapping.
///
/// This is the Layer 3 → Layer 5 boundary.  All callers that need a
/// concrete `SketchType` or `SketchParams` go through this function.
pub fn resolve(intent: &AggIntent) -> PhysicalAggOp {
    PhysicalAggOp {
        intent: intent.clone(),
        sketch_type: directory::sketch_type_for_op(intent),
        sketch_params: directory::sketch_params_for_op(intent),
        estimated_memory_bytes: directory::estimated_sketch_memory_bytes(intent),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_quantile() {
        let p = resolve(&AggIntent::default_quantile(vec![0.99]));
        assert_eq!(p.sketch_type, SketchType::DDSketch);
        assert!(matches!(p.sketch_params, SketchParams::DDSketch { .. }));
        assert!(p.estimated_memory_bytes > 0);
    }

    #[test]
    fn resolve_cardinality() {
        let p = resolve(&AggIntent::default_cardinality());
        assert_eq!(p.sketch_type, SketchType::HLL);
        assert!(matches!(p.sketch_params, SketchParams::HLL { .. }));
    }

    #[test]
    fn resolve_frequency() {
        let p = resolve(&AggIntent::default_frequency());
        assert_eq!(p.sketch_type, SketchType::CountSketch);
        assert!(matches!(p.sketch_params, SketchParams::CountSketch { .. }));
    }

    #[test]
    fn resolve_preserves_intent() {
        let intent = AggIntent::Quantile { quantiles: vec![0.5, 0.99], accuracy: 0.005 };
        let p = resolve(&intent);
        assert_eq!(p.intent, intent);
    }
}
