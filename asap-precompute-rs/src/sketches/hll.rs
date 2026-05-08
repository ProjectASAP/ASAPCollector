//! HLL wrapper over [`asap_sketchlib::wrapper::HllSketch`].
//!
//! Mirrors `asap-precompute-go/sketches/hll.go`. HLL is the canonical
//! [`CardinalitySketch`] implementation in this crate.

use asap_sketchlib::proto::sketchlib::{
    sketch_envelope, HllVariant, HyperLogLogState, SketchEnvelope as ProtoEnvelope,
};
use asap_sketchlib::wrapper::{HllSketch, HllVariant as RsHllVariant};
use prost::Message;

use crate::observation::ObservationValue;
use crate::precompute::{CardinalitySketch, DeltaResult, PrecomputeError, Sketch, SketchObserver};

/// HLL wrapper. Owns one `asap_sketchlib::HllSketch`.
pub struct HLLWrapper {
    sk: HllSketch,
    variant: RsHllVariant,
    precision: u32,
}

impl HLLWrapper {
    /// Construct an empty HLL with the given variant and precision.
    pub fn new(variant: RsHllVariant, precision: u32) -> Self {
        Self {
            sk: HllSketch::new(variant, precision),
            variant,
            precision,
        }
    }

    /// Insert a byte slice. Mirrors the Go wrapper's `UpdateValue`
    /// (which the Go wrapper also routes to a hashed-bytes path).
    pub fn update(&mut self, value: &[u8]) {
        self.sk.update(value);
    }

    /// Borrow the underlying `HllSketch`.
    pub fn inner(&self) -> &HllSketch {
        &self.sk
    }

    fn build_state(&self) -> HyperLogLogState {
        let proto_variant = match self.variant {
            RsHllVariant::Unspecified => HllVariant::Unspecified as i32,
            RsHllVariant::Regular => HllVariant::Regular as i32,
            RsHllVariant::Datafusion => HllVariant::ErtlMle as i32,
            RsHllVariant::Hip => HllVariant::Hip as i32,
        };
        HyperLogLogState {
            variant: proto_variant,
            precision: self.precision,
            registers: self.sk.registers.clone(),
            hip_kxq0: self.sk.hip_kxq0,
            hip_kxq1: self.sk.hip_kxq1,
            hip_est: self.sk.hip_est,
        }
    }

    fn encode_envelope(&self) -> Vec<u8> {
        let env = ProtoEnvelope {
            format_version: 1,
            producer: None,
            hash_spec: None,
            sketch_state: Some(sketch_envelope::SketchState::Hll(self.build_state())),
        };
        let mut buf = Vec::with_capacity(env.encoded_len());
        env.encode(&mut buf).expect("prost encode");
        buf
    }

    fn decode_envelope(bytes: &[u8]) -> Result<HllSketch, PrecomputeError> {
        let env = ProtoEnvelope::decode(bytes)
            .map_err(|e| PrecomputeError::Other(format!("HLLWrapper decode: {e}")))?;
        let state = match env.sketch_state {
            Some(sketch_envelope::SketchState::Hll(s)) => s,
            _ => {
                return Err(PrecomputeError::Other(
                    "HLLWrapper: envelope did not carry HyperLogLogState".into(),
                ));
            }
        };
        let variant = match HllVariant::try_from(state.variant) {
            Ok(HllVariant::Regular) => RsHllVariant::Regular,
            Ok(HllVariant::ErtlMle) => RsHllVariant::Datafusion,
            Ok(HllVariant::Hip) => RsHllVariant::Hip,
            _ => RsHllVariant::Unspecified,
        };
        Ok(HllSketch::from_raw(
            variant,
            state.precision,
            state.registers,
            state.hip_kxq0,
            state.hip_kxq1,
            state.hip_est,
        ))
    }
}

impl Sketch for HLLWrapper {
    fn snapshot(&self) -> Result<Vec<u8>, PrecomputeError> {
        if self.sk.registers.iter().all(|&r| r == 0) {
            return Ok(Vec::new());
        }
        Ok(self.encode_envelope())
    }

    fn compute_delta_against(
        &self,
        _prev: &[u8],
        _threshold: u64,
    ) -> Result<DeltaResult, PrecomputeError> {
        // `asap_sketchlib` does not currently expose
        // `compute_register_delta`; until it does, the wrapper emits
        // full snapshots. Honest fallback (matches Go's
        // "decode failure → full" branch).
        let full = self.snapshot()?;
        Ok(DeltaResult {
            payload: full,
            is_full: true,
        })
    }

    fn apply_delta(&mut self, payload: &[u8]) -> Result<(), PrecomputeError> {
        if payload.is_empty() {
            return Ok(());
        }
        let other = Self::decode_envelope(payload)?;
        self.sk
            .merge(&other)
            .map_err(|e| PrecomputeError::Other(format!("HLLWrapper merge: {e}")))
    }

    fn merge(&mut self, other: &dyn Sketch) -> Result<(), PrecomputeError> {
        let bytes = other.snapshot()?;
        if bytes.is_empty() {
            return Ok(());
        }
        let decoded = Self::decode_envelope(&bytes)?;
        self.sk
            .merge(&decoded)
            .map_err(|e| PrecomputeError::Other(format!("HLLWrapper merge: {e}")))
    }

    fn reset(&mut self) {
        self.sk = HllSketch::new(self.variant, self.precision);
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl CardinalitySketch for HLLWrapper {
    fn estimate_cardinality(&self) -> f64 {
        self.sk.estimate()
    }
}

/// Observer routing observations into an [`HLLWrapper`].
///
/// Accepts both `Bytes` (preferred — opaque key) and `Float` (the
/// float bytes are hashed). The Go reference accepts `Float` only;
/// the Rust wrapper mirrors that for compatibility.
pub struct HLLObserver;

impl SketchObserver for HLLObserver {
    fn observe(
        &self,
        sketch: &mut dyn Sketch,
        v: &ObservationValue,
    ) -> Result<(), PrecomputeError> {
        let w = sketch
            .as_any_mut()
            .downcast_mut::<HLLWrapper>()
            .ok_or_else(|| {
                PrecomputeError::Other("HLLObserver: sketch is not an HLLWrapper".into())
            })?;
        match v.kind {
            crate::observation::ObservationValueKind::Float => {
                let bytes = v.float.to_le_bytes();
                w.update(&bytes);
                Ok(())
            }
            crate::observation::ObservationValueKind::Bytes => {
                w.update(&v.bytes);
                Ok(())
            }
            crate::observation::ObservationValueKind::Hash => {
                let bytes = v.hash.to_le_bytes();
                w.update(&bytes);
                Ok(())
            }
            other => Err(PrecomputeError::Other(format!(
                "HLLObserver: unsupported value kind {}",
                other.name()
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_wrapper_is_empty() {
        let w = HLLWrapper::new(RsHllVariant::Regular, 12);
        assert_eq!(w.snapshot().unwrap().len(), 0);
        assert_eq!(w.estimate_cardinality(), 0.0);
    }

    #[test]
    fn update_then_estimate() {
        let mut w = HLLWrapper::new(RsHllVariant::Regular, 12);
        for i in 0..1_000u64 {
            w.update(&i.to_le_bytes());
        }
        let est = w.estimate_cardinality();
        // Loose bounds — HLL with precision=12 has std error ~1.6%.
        assert!(est > 800.0 && est < 1200.0, "est={est}");
    }

    #[test]
    fn snapshot_roundtrip_preserves_registers() {
        let mut w = HLLWrapper::new(RsHllVariant::Regular, 12);
        for i in 0..100u64 {
            w.update(&i.to_le_bytes());
        }
        let bytes = w.snapshot().unwrap();
        let decoded = HLLWrapper::decode_envelope(&bytes).unwrap();
        assert_eq!(decoded.registers, w.sk.registers);
    }

    #[test]
    fn merge_takes_register_max() {
        let mut a = HLLWrapper::new(RsHllVariant::Regular, 12);
        let mut b = HLLWrapper::new(RsHllVariant::Regular, 12);
        for i in 0..500u64 {
            a.update(&i.to_le_bytes());
        }
        for i in 250..750u64 {
            b.update(&i.to_le_bytes());
        }
        let pre = a.estimate_cardinality();
        let other_bytes = b.snapshot().unwrap();
        a.apply_delta(&other_bytes).unwrap();
        let post = a.estimate_cardinality();
        assert!(
            post >= pre,
            "merged estimate decreased: pre={pre} post={post}"
        );
    }
}
