//! Layer 5 — Physical plan IR.
//!
//! Maps the implementation-independent [`AggIntent`] (Layer 3) to concrete
//! sketch implementations and pipeline stages.
//!
//! # Key types
//!
//! - [`PhysicalAggOp`] — resolved AggIntent → concrete SketchType + SketchParams
//! - [`PhysicalOp`] — a physical operator (sketch build, merge, exchange, eval, etc.)
//! - [`PhysicalNode`] — a node in the physical plan tree (operator + placement + cost)
//! - [`Placement`] — where a physical operator runs (Agent, Backend, PromSketch, DB, etc.)

use std::time::Duration;

use crate::algebra::directory;
use crate::algebra::expr::{AggIntent, WindowKind, WindowSpec};
use crate::types::{SketchParams, SketchType};

// ── PhysicalAggOp (resolved sketch intent) ──────────────────────────────────

/// A resolved physical aggregation operation.
///
/// This is the output of `resolve()`: concrete sketch implementation
/// chosen for a logical [`AggIntent`].
#[derive(Debug, Clone)]
pub struct PhysicalAggOp {
    /// The logical intent this was derived from.
    pub intent: AggIntent,
    /// Concrete sketch type.
    pub sketch_type: SketchType,
    /// Concrete sketch parameters.
    pub sketch_params: SketchParams,
    /// Estimated memory footprint per series (bytes).
    pub estimated_memory_bytes: u64,
}

/// Resolve an [`AggIntent`] into a [`PhysicalAggOp`] using default mapping.
///
/// This is the Layer 3 → Layer 5 boundary.
pub fn resolve(intent: &AggIntent) -> PhysicalAggOp {
    PhysicalAggOp {
        intent: intent.clone(),
        sketch_type: directory::sketch_type_for_op(intent),
        sketch_params: directory::sketch_params_for_op(intent),
        estimated_memory_bytes: directory::estimated_sketch_memory_bytes(intent),
    }
}

// ── Physical operators ──────────────────────────────────────────────────────

/// A physical operator — concrete implementation of a logical operator.
#[derive(Debug, Clone)]
pub enum PhysicalOp {
    // ── Scan / ingest ─────────────────────────────────────────────
    /// Read raw OTLP metrics from an SDK or scrape target.
    OtlpScan {
        endpoint: String,
        label_matchers: Vec<String>,
    },

    /// Read from an existing PromSketch store.
    PromSketchScan {
        store_addr: String,
        series_selector: String,
    },

    // ── Sketch build ──────────────────────────────────────────────
    /// Build sketch via OTel Collector processor (tumbling window flush).
    OtelSketchBuild {
        sketch_type: SketchType,
        sketch_params: SketchParams,
        window: PhysicalWindow,
        delta_encoding: bool,
    },

    /// Build sketch via PromSketch's ExponentialHistogram layer.
    PromSketchBuild {
        sketch_type: SketchType,
        eh_k: usize,
        time_window: Duration,
    },

    // ── Sketch merge ──────────────────────────────────────────────
    /// Merge sketches from N upstream nodes.
    SketchMerge {
        sketch_type: SketchType,
        group_by: Vec<String>,
    },

    // ── Sketch query ──────────────────────────────────────────────
    /// Extract result from a sketch (quantile, cardinality, frequency).
    SketchEval {
        sketch_type: SketchType,
        func: EvalFunc,
    },

    // ── Data exchange ─────────────────────────────────────────────
    /// Data transfer between pipeline stages.
    Exchange {
        format: ExchangeFormat,
    },

    // ── Relational / passthrough ──────────────────────────────────
    /// Filter rows.
    Filter { pred: String },
    /// Top-K ranking.
    TopK { k: u64 },
    /// Hash-partitioned aggregation.
    HashAggregate { keys: Vec<String> },
    /// SQL query to database.
    DbQuery { sql: String },
    /// Passthrough — no transformation.
    Passthrough,
}

/// Physical window implementation.
#[derive(Debug, Clone)]
pub enum PhysicalWindow {
    /// OTel Collector: `time.NewTicker` flush + sketch reset.
    OtelTumblingFlush { duration: Duration },
    /// PromSketch ExponentialHistogram: time-decaying buckets.
    PromSketchEH { eh_k: usize, time_window: Duration },
    /// Database-side: `GROUP BY time_bucket(interval, ts)`.
    SqlTimeBucket { interval: Duration, time_col: String },
    /// No windowing (unbounded / landmark).
    None,
}

/// What to extract from a sketch at query time.
#[derive(Debug, Clone)]
pub enum EvalFunc {
    Quantile(Vec<f64>),
    Cardinality,
    Frequency { key: String },
    TopK { k: u64 },
    Extrema { min: bool, max: bool },
}

/// Data format for Exchange operators.
#[derive(Debug, Clone)]
pub enum ExchangeFormat {
    /// OTLP gRPC / HTTP.
    Otlp,
    /// Sketch-specific binary (merged sketch bytes).
    SketchBinary,
    /// Raw samples (for non-sketch path).
    RawSamples,
}

/// Where a physical operator runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Placement {
    /// Agent OTel Collector (co-located with SDK).
    AgentCollector,
    /// Backend OTel Collector (merge tier).
    BackendCollector,
    /// PromSketch store (ASAPQuery).
    PromSketchStore,
    /// General query engine (ASAPQuery / DataFusion).
    QueryEngine,
    /// Database (ClickHouse, TimescaleDB, etc.).
    Database,
}

// ── Physical plan tree ──────────────────────────────────────────────────────

/// A node in the physical plan tree.
#[derive(Debug, Clone)]
pub struct PhysicalNode {
    /// The physical operator at this node.
    pub op: PhysicalOp,
    /// Where this operator runs.
    pub placement: Placement,
    /// Estimated cost.
    pub cost: PhysicalCost,
    /// Child nodes (ordered: left, right, or input list).
    pub children: Vec<PhysicalNode>,
}

/// Cost estimate for a physical operator.
#[derive(Debug, Clone, Default)]
pub struct PhysicalCost {
    /// Estimated output bandwidth (bytes/sec).
    pub bytes_per_sec: f64,
    /// Estimated memory usage (bytes).
    pub memory_bytes: f64,
    /// Estimated CPU cost (microseconds per sample).
    pub cpu_per_sample: f64,
}

// ── Window resolution ───────────────────────────────────────────────────────

/// Resolve a logical [`WindowSpec`] to a [`PhysicalWindow`] for a given placement.
pub fn resolve_window(window: &WindowSpec, placement: &Placement) -> PhysicalWindow {
    match (&window.kind, placement) {
        (WindowKind::Tumbling { size }, Placement::AgentCollector) =>
            PhysicalWindow::OtelTumblingFlush { duration: *size },
        (WindowKind::Tumbling { size }, Placement::PromSketchStore) =>
            PhysicalWindow::PromSketchEH { eh_k: 50, time_window: *size },
        (WindowKind::Sliding { size, .. }, Placement::PromSketchStore) =>
            PhysicalWindow::PromSketchEH { eh_k: 50, time_window: *size },
        (WindowKind::Tumbling { size }, Placement::Database) =>
            PhysicalWindow::SqlTimeBucket {
                interval: *size,
                time_col: window.time_col.clone().unwrap_or_else(|| "ts".into()),
            },
        (WindowKind::Unbounded | WindowKind::Landmark, _) =>
            PhysicalWindow::None,
        // Fallback: tumbling at the given size for any other combo.
        (WindowKind::Tumbling { size } | WindowKind::Sliding { size, .. } | WindowKind::Session { gap: size }, _) =>
            PhysicalWindow::OtelTumblingFlush { duration: *size },
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

    #[test]
    fn tumbling_window_at_agent() {
        let ws = WindowSpec {
            kind: WindowKind::Tumbling { size: Duration::from_secs(300) },
            time_col: None,
        };
        let pw = resolve_window(&ws, &Placement::AgentCollector);
        assert!(matches!(pw, PhysicalWindow::OtelTumblingFlush { .. }));
    }

    #[test]
    fn sliding_window_at_promsketch() {
        let ws = WindowSpec {
            kind: WindowKind::Sliding { size: Duration::from_secs(300), slide: Duration::from_secs(60) },
            time_col: None,
        };
        let pw = resolve_window(&ws, &Placement::PromSketchStore);
        assert!(matches!(pw, PhysicalWindow::PromSketchEH { .. }));
    }

    #[test]
    fn tumbling_window_at_database() {
        let ws = WindowSpec {
            kind: WindowKind::Tumbling { size: Duration::from_secs(60) },
            time_col: Some("event_time".into()),
        };
        let pw = resolve_window(&ws, &Placement::Database);
        match pw {
            PhysicalWindow::SqlTimeBucket { interval, time_col } => {
                assert_eq!(interval, Duration::from_secs(60));
                assert_eq!(time_col, "event_time");
            }
            other => panic!("expected SqlTimeBucket, got {other:?}"),
        }
    }

    #[test]
    fn unbounded_window_is_none() {
        let ws = WindowSpec { kind: WindowKind::Unbounded, time_col: None };
        let pw = resolve_window(&ws, &Placement::AgentCollector);
        assert!(matches!(pw, PhysicalWindow::None));
    }
}
