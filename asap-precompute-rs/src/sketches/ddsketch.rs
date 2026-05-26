//! DDSketch wrapper over [`asap_sketchlib::DdSketch`].
//!
//! Mirrors `asap-precompute-go/sketches/ddsketch.go`. Adapts the
//! wire-format-aligned `DdSketch` struct to the host-neutral
//! [`Sketch`] + [`QuantileSketch`] interfaces.

use asap_sketchlib::proto::sketchlib::{
    sketch_envelope, DdSketchState, SketchEnvelope as ProtoEnvelope,
};
use asap_sketchlib::DdSketch;
use prost::Message;

use crate::observation::Observation;
use crate::precompute::{DeltaResult, PrecomputeError, QuantileSketch, Sketch, SketchObserver};

/// DDSketch wrapper.
///
/// Owns one `asap_sketchlib::DdSketch` (the wire-format-aligned variant
/// with public-field `store_counts` / `store_offset` / aggregates).
///
/// # Snapshot format
///
/// `Snapshot` produces a `prost`-encoded `SketchEnvelope` carrying the
/// inner `DDSketchState` proto — the same shape Go's
/// `ddsketch.SerializePortable + proto.Marshal` emits.
pub struct DDSketchWrapper {
    sk: DdSketch,
    alpha: f64,
}

impl DDSketchWrapper {
    /// Construct an empty DDSketch with relative-accuracy alpha.
    /// `alpha` must satisfy `0 < alpha < 1`.
    pub fn new(alpha: f64) -> Self {
        Self {
            sk: DdSketch::new(alpha),
            alpha,
        }
    }

    /// Insert a single positive observation.
    pub fn update(&mut self, value: f64) {
        self.sk.update(value);
    }

    /// Borrow the underlying `DdSketch`.
    pub fn inner(&self) -> &DdSketch {
        &self.sk
    }

    fn build_state(&self) -> DdSketchState {
        DdSketchState {
            // Use the gamma-roundtripped alpha so the on-the-wire bytes
            // match `sketchlib-go::DDSketch.SerializePortable` exactly.
            // Closes part of ProjectASAP/ASAPCollector#243.
            alpha: self.sk.wire_alpha(),
            store_counts: self.sk.store_counts.clone(),
            store_offset: self.sk.store_offset,
            count: self.sk.count,
            sum: self.sk.sum,
            min: if self.sk.count == 0 {
                f64::INFINITY
            } else {
                self.sk.min
            },
            max: if self.sk.count == 0 {
                f64::NEG_INFINITY
            } else {
                self.sk.max
            },
        }
    }

    fn encode_envelope(&self) -> Vec<u8> {
        let env = ProtoEnvelope {
            format_version: 1,
            producer: None,
            hash_spec: None,
            sample_p: 0.0,
            sketch_state: Some(sketch_envelope::SketchState::Ddsketch(self.build_state())),
        };
        let mut buf = Vec::with_capacity(env.encoded_len());
        env.encode(&mut buf).expect("prost encode");
        buf
    }

    fn decode_envelope(bytes: &[u8]) -> Result<DdSketch, PrecomputeError> {
        let env = ProtoEnvelope::decode(bytes)
            .map_err(|e| PrecomputeError::Other(format!("DDSketchWrapper decode: {e}")))?;
        let state = match env.sketch_state {
            Some(sketch_envelope::SketchState::Ddsketch(s)) => s,
            _ => {
                return Err(PrecomputeError::Other(
                    "DDSketchWrapper: envelope did not carry DDSketchState".into(),
                ));
            }
        };
        if !(state.alpha > 0.0 && state.alpha < 1.0) {
            return Err(PrecomputeError::Other(format!(
                "DDSketchWrapper: alpha {} out of range",
                state.alpha
            )));
        }
        Ok(DdSketch::from_raw(
            state.alpha,
            state.store_counts,
            state.store_offset,
            state.count,
            state.sum,
            state.min,
            state.max,
        ))
    }
}

impl Sketch for DDSketchWrapper {
    fn snapshot(&self) -> Result<Vec<u8>, PrecomputeError> {
        if self.sk.count == 0 {
            // Mirror Go: empty sketch produces empty snapshot — the
            // runtime drops empty payloads rather than emitting
            // zero-byte envelopes.
            return Ok(Vec::new());
        }
        Ok(self.encode_envelope())
    }

    fn compute_delta_against(
        &self,
        _prev: &[u8],
        _threshold: u64,
    ) -> Result<DeltaResult, PrecomputeError> {
        // `asap_sketchlib` does not expose a `ComputeDelta` helper for
        // DDSketch (Go's `sketchlib-go` does). Until that lands, the
        // wrapper always returns a full snapshot. The runtime sees
        // `is_full = true` on every emit; bandwidth-inefficient
        // compared to Go, but correct.
        let full = self.snapshot()?;
        Ok(DeltaResult {
            payload: full,
            is_full: true,
        })
    }

    fn apply_delta(&mut self, delta: &[u8]) -> Result<(), PrecomputeError> {
        if delta.is_empty() {
            return Ok(());
        }
        // Always treated as a full proto envelope: with `compute_delta_against`
        // pinned to full, the only inbound shape is a full envelope.
        let other = Self::decode_envelope(delta)?;
        self.sk
            .merge(&other)
            .map_err(|e| PrecomputeError::Other(format!("DDSketchWrapper merge: {e}")))
    }

    fn merge(&mut self, other: &dyn Sketch) -> Result<(), PrecomputeError> {
        // The runtime always merges sketches owned by the same
        // Precompute (same alpha). Our trait is generic, so we
        // round-trip through the snapshot bytes.
        let bytes = other.snapshot()?;
        if bytes.is_empty() {
            return Ok(());
        }
        let decoded = Self::decode_envelope(&bytes)?;
        self.sk
            .merge(&decoded)
            .map_err(|e| PrecomputeError::Other(format!("DDSketchWrapper merge: {e}")))
    }

    fn reset(&mut self) {
        self.sk = DdSketch::new(self.alpha);
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl QuantileSketch for DDSketchWrapper {
    fn quantile(&self, q: f64) -> f64 {
        if self.sk.count == 0 {
            return f64::NAN;
        }
        self.sk.quantile(q.clamp(0.0, 1.0)).unwrap_or(f64::NAN)
    }
}

/// Observer routing `Float`-kind observations into a [`DDSketchWrapper`].
pub struct DDSketchObserver;

impl SketchObserver for DDSketchObserver {
    fn observe(&self, sketch: &mut dyn Sketch, obs: &Observation) -> Result<(), PrecomputeError> {
        // Use a `&mut dyn Sketch -> &mut DDSketchWrapper` downcast via
        // the panic-safe method below.
        let w = downcast_mut(sketch)?;
        match obs.value.kind {
            crate::observation::ObservationValueKind::Float => {
                w.update(obs.value.float);
                Ok(())
            }
            other => Err(PrecomputeError::Other(format!(
                "DDSketchObserver: unsupported value kind {}",
                other.name()
            ))),
        }
    }
}

fn downcast_mut(sketch: &mut dyn Sketch) -> Result<&mut DDSketchWrapper, PrecomputeError> {
    sketch
        .as_any_mut()
        .downcast_mut::<DDSketchWrapper>()
        .ok_or_else(|| {
            PrecomputeError::Other("DDSketchObserver: sketch is not a DDSketchWrapper".into())
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_wrapper_is_empty() {
        let w = DDSketchWrapper::new(0.01);
        assert_eq!(w.sk.count, 0);
        assert_eq!(w.snapshot().unwrap().len(), 0);
    }

    #[test]
    fn update_then_quantile_within_bound() {
        let mut w = DDSketchWrapper::new(0.01);
        for i in 1..=100 {
            w.update(i as f64);
        }
        let p50 = w.quantile(0.5);
        // Median of [1..=100] is 50 or 51; with α=0.01 the relative
        // error is ≤ 1%.
        assert!((p50 - 50.0).abs() / 50.0 < 0.05, "p50={p50}");
    }

    #[test]
    fn snapshot_decodes_back_to_equivalent_state() {
        let mut w = DDSketchWrapper::new(0.01);
        for i in 1..=10 {
            w.update(i as f64);
        }
        let bytes = w.snapshot().unwrap();
        let decoded = DDSketchWrapper::decode_envelope(&bytes).unwrap();
        assert_eq!(decoded.count, w.sk.count);
        assert!((decoded.sum - w.sk.sum).abs() < 1e-9);
    }

    #[test]
    fn merge_combines_counts() {
        let mut a = DDSketchWrapper::new(0.01);
        let mut b = DDSketchWrapper::new(0.01);
        for i in 1..=5 {
            a.update(i as f64);
        }
        for i in 6..=10 {
            b.update(i as f64);
        }
        let other_bytes = b.snapshot().unwrap();
        a.apply_delta(&other_bytes).unwrap();
        assert_eq!(a.sk.count, 10);
    }

    #[test]
    fn reset_zeros_state() {
        let mut w = DDSketchWrapper::new(0.01);
        w.update(1.0);
        w.update(2.0);
        w.reset();
        assert_eq!(w.sk.count, 0);
    }
}
