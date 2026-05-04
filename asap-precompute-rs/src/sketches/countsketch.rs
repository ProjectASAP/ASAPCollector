//! CountSketch wrapper over [`asap_sketchlib::sketches::CountSketch`].
//!
//! Mirrors `asap-precompute-go/sketches/countsketch.go`. Implements
//! [`Sketch`] + [`FrequencySketch`].

use asap_sketchlib::proto::sketchlib::{
    sketch_envelope, CountSketchState, CounterType, SketchEnvelope as ProtoEnvelope,
};
use asap_sketchlib::sketches::CountSketch;
use prost::Message;

use crate::observation::ObservationValue;
use crate::precompute::{
    DeltaResult, FrequencyEntry, FrequencySketch, PrecomputeError, Sketch, SketchObserver,
};

/// CountSketch wrapper.
pub struct CountSketchWrapper {
    sk: CountSketch,
    rows: usize,
    cols: usize,
}

impl CountSketchWrapper {
    /// Construct a CountSketch with the given dimensions.
    pub fn new(rows: usize, cols: usize) -> Self {
        Self {
            sk: CountSketch::new(rows, cols),
            rows,
            cols,
        }
    }

    /// Insert a string-keyed observation.
    pub fn update(&mut self, key: &str, value: f64) {
        self.sk.update(key, value);
    }

    /// Borrow the underlying `CountSketch`.
    pub fn inner(&self) -> &CountSketch {
        &self.sk
    }

    fn build_state(&self) -> CountSketchState {
        let mut counts_float = Vec::with_capacity(self.rows * self.cols);
        for row in self.sk.matrix.iter().take(self.rows) {
            for &cell in row.iter().take(self.cols) {
                counts_float.push(cell);
            }
        }
        CountSketchState {
            rows: self.rows as u32,
            cols: self.cols as u32,
            counter_type: CounterType::Float64 as i32,
            counts_int: Vec::new(),
            counts_float,
            l2: Vec::new(),
            topk: None,
        }
    }

    fn encode_envelope(&self) -> Vec<u8> {
        let env = ProtoEnvelope {
            format_version: 1,
            producer: None,
            hash_spec: None,
            sketch_state: Some(sketch_envelope::SketchState::CountSketch(
                self.build_state(),
            )),
        };
        let mut buf = Vec::with_capacity(env.encoded_len());
        env.encode(&mut buf).expect("prost encode");
        buf
    }

    fn decode_envelope(bytes: &[u8]) -> Result<CountSketch, PrecomputeError> {
        let env = ProtoEnvelope::decode(bytes)
            .map_err(|e| PrecomputeError::Other(format!("CountSketchWrapper decode: {e}")))?;
        let state = match env.sketch_state {
            Some(sketch_envelope::SketchState::CountSketch(s)) => s,
            _ => {
                return Err(PrecomputeError::Other(
                    "CountSketchWrapper: envelope did not carry CountSketchState".into(),
                ));
            }
        };
        let rows = state.rows as usize;
        let cols = state.cols as usize;
        let mut matrix = vec![vec![0.0f64; cols]; rows];
        if !state.counts_float.is_empty() {
            for (r, row) in matrix.iter_mut().enumerate().take(rows) {
                for (c, cell) in row.iter_mut().enumerate().take(cols) {
                    let idx = r * cols + c;
                    if idx < state.counts_float.len() {
                        *cell = state.counts_float[idx];
                    }
                }
            }
        } else if !state.counts_int.is_empty() {
            for (r, row) in matrix.iter_mut().enumerate().take(rows) {
                for (c, cell) in row.iter_mut().enumerate().take(cols) {
                    let idx = r * cols + c;
                    if idx < state.counts_int.len() {
                        *cell = state.counts_int[idx] as f64;
                    }
                }
            }
        }
        Ok(CountSketch::from_legacy_matrix(matrix, rows, cols))
    }

    /// Whether the sketch matrix is all zero.
    fn is_empty(&self) -> bool {
        self.sk
            .matrix
            .iter()
            .all(|row| row.iter().all(|&v| v == 0.0))
    }
}

impl Sketch for CountSketchWrapper {
    fn snapshot(&self) -> Result<Vec<u8>, PrecomputeError> {
        if self.is_empty() {
            return Ok(Vec::new());
        }
        Ok(self.encode_envelope())
    }

    fn compute_delta_against(
        &self,
        _prev: &[u8],
        _threshold: u64,
    ) -> Result<DeltaResult, PrecomputeError> {
        // No `compute_delta` on `asap_sketchlib::CountSketch`; emit full.
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
            .map_err(|e| PrecomputeError::Other(format!("CountSketchWrapper merge: {e}")))
    }

    fn merge(&mut self, other: &dyn Sketch) -> Result<(), PrecomputeError> {
        let bytes = other.snapshot()?;
        if bytes.is_empty() {
            return Ok(());
        }
        let decoded = Self::decode_envelope(&bytes)?;
        self.sk
            .merge(&decoded)
            .map_err(|e| PrecomputeError::Other(format!("CountSketchWrapper merge: {e}")))
    }

    fn reset(&mut self) {
        self.sk = CountSketch::new(self.rows, self.cols);
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl FrequencySketch for CountSketchWrapper {
    fn estimate_count(&self, key: &[u8]) -> f64 {
        if key.is_empty() {
            return 0.0;
        }
        let s = std::str::from_utf8(key).unwrap_or_default();
        if s.is_empty() {
            return 0.0;
        }
        self.sk.estimate(s)
    }

    fn top_k(&self, _k: usize) -> Vec<FrequencyEntry> {
        // The wire-format `CountSketch` doesn't carry a TopK heap.
        // Returning empty matches the Go reference's behavior when
        // `TopK == nil` (the legacy CMS processor never queries
        // TopK; the Go CountSketch wrapper exposes TopK only when
        // the underlying sketch tracks it).
        Vec::new()
    }
}

/// Observer that routes `Float`-kind observations into the wrapper
/// using the observation's `bytes` field as the key (or falling back
/// to the configured default).
pub struct CountSketchObserver {
    /// Default key used when the observation's `bytes` field is empty.
    pub default_key: String,
}

impl SketchObserver for CountSketchObserver {
    fn observe(
        &self,
        sketch: &mut dyn Sketch,
        v: &ObservationValue,
    ) -> Result<(), PrecomputeError> {
        let w = sketch
            .as_any_mut()
            .downcast_mut::<CountSketchWrapper>()
            .ok_or_else(|| {
                PrecomputeError::Other(
                    "CountSketchObserver: sketch is not a CountSketchWrapper".into(),
                )
            })?;
        if v.kind != crate::observation::ObservationValueKind::Float {
            return Err(PrecomputeError::Other(format!(
                "CountSketchObserver: unsupported value kind {}",
                v.kind.name()
            )));
        }
        let key_str: String = if !v.bytes.is_empty() {
            String::from_utf8_lossy(&v.bytes).into_owned()
        } else {
            self.default_key.clone()
        };
        w.update(&key_str, v.float);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_wrapper_is_empty() {
        let w = CountSketchWrapper::new(4, 32);
        assert_eq!(w.snapshot().unwrap().len(), 0);
    }

    #[test]
    fn update_then_estimate() {
        let mut w = CountSketchWrapper::new(8, 64);
        for _ in 0..100 {
            w.update("hot-key", 1.0);
        }
        for _ in 0..5 {
            w.update("cold-key", 1.0);
        }
        let hot = w.estimate_count(b"hot-key");
        let cold = w.estimate_count(b"cold-key");
        // Median-of-rows estimator can over/undercount but should
        // place "hot" well above "cold".
        assert!(hot.abs() > cold.abs(), "hot={hot} cold={cold}");
    }

    #[test]
    fn snapshot_roundtrip_preserves_matrix() {
        let mut w = CountSketchWrapper::new(4, 8);
        w.update("k", 1.0);
        let bytes = w.snapshot().unwrap();
        let decoded = CountSketchWrapper::decode_envelope(&bytes).unwrap();
        assert_eq!(decoded.matrix, w.sk.matrix);
    }
}
