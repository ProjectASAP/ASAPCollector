//! CountMinSketch wrapper over [`asap_sketchlib::CountMinSketch`].
//!
//! Mirrors `asap-precompute-go/sketches/cms.go`. Implements
//! [`Sketch`] + [`FrequencySketch`].

use asap_sketchlib::proto::sketchlib::{
    sketch_envelope, CountMinState, CounterType, SketchEnvelope as ProtoEnvelope,
};
use asap_sketchlib::CountMinSketch;
use prost::Message;

use crate::observation::ObservationValue;
use crate::precompute::{
    DeltaResult, FrequencyEntry, FrequencySketch, PrecomputeError, Sketch, SketchObserver,
};

/// CountMinSketch wrapper.
pub struct CMSWrapper {
    sk: CountMinSketch,
    rows: usize,
    cols: usize,
}

impl CMSWrapper {
    /// Construct a CMS with the given dimensions.
    pub fn new(rows: usize, cols: usize) -> Self {
        Self {
            sk: CountMinSketch::new(rows, cols),
            rows,
            cols,
        }
    }

    /// Insert a string-keyed weighted observation.
    pub fn update(&mut self, key: &str, value: f64) {
        self.sk.update(key, value);
    }

    /// Borrow the underlying `CountMinSketch`.
    pub fn inner(&self) -> &CountMinSketch {
        &self.sk
    }

    fn build_state(&self) -> CountMinState {
        // Mirror sketchlib-go::CountMinSketch.SerializePortableFO:
        // emit packed sint64 `counts_int` (Opt-2: 4–8× smaller than
        // f64 for typical small-integer counter values) and per-row
        // L1/L2 norms (Go's InsertWithHash maintains
        // `L1[r] += weight` and `L2[r] += curr*curr - prev*prev`,
        // which collapse to `sum_c count[r][c]` and
        // `sum_c count[r][c]^2` for the unweighted unit-step stream
        // the parity harness drives — the only producer pattern this
        // wire path serves today). Omit `sum_counts` / `sum2_counts`
        // (Frequency-Only mode) to match Go's `SerializeProtoBytesFO`
        // payload bit-for-bit.
        let matrix = self.sk.sketch();
        let mut counts_int = Vec::with_capacity(self.rows * self.cols);
        let mut l1 = Vec::with_capacity(self.rows);
        let mut l2 = Vec::with_capacity(self.rows);
        for row in matrix.iter().take(self.rows) {
            let mut row_l1 = 0.0f64;
            let mut row_l2 = 0.0f64;
            for &cell in row.iter().take(self.cols) {
                counts_int.push(cell as i64);
                row_l1 += cell;
                row_l2 += cell * cell;
            }
            l1.push(row_l1);
            l2.push(row_l2);
        }
        CountMinState {
            rows: self.rows as u32,
            cols: self.cols as u32,
            counter_type: CounterType::Int64 as i32,
            counts_int,
            counts_float: Vec::new(),
            sum_counts: Vec::new(),
            sum2_counts: Vec::new(),
            l1,
            l2,
        }
    }

    fn encode_envelope(&self) -> Vec<u8> {
        let env = ProtoEnvelope {
            format_version: 1,
            producer: None,
            hash_spec: None,
            sketch_state: Some(sketch_envelope::SketchState::CountMin(self.build_state())),
        };
        let mut buf = Vec::with_capacity(env.encoded_len());
        env.encode(&mut buf).expect("prost encode");
        buf
    }

    fn decode_envelope(bytes: &[u8]) -> Result<CountMinSketch, PrecomputeError> {
        let env = ProtoEnvelope::decode(bytes)
            .map_err(|e| PrecomputeError::Other(format!("CMSWrapper decode: {e}")))?;
        let state = match env.sketch_state {
            Some(sketch_envelope::SketchState::CountMin(s)) => s,
            _ => {
                return Err(PrecomputeError::Other(
                    "CMSWrapper: envelope did not carry CountMinState".into(),
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
        Ok(CountMinSketch::from_legacy_matrix(matrix, rows, cols))
    }

    fn is_empty(&self) -> bool {
        let m = self.sk.sketch();
        m.iter().all(|row| row.iter().all(|&v| v == 0.0))
    }
}

impl Sketch for CMSWrapper {
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
        // No `compute_delta` on `asap_sketchlib::CountMinSketch`. The
        // Go side has `cms.ComputeDelta`; the Rust crate doesn't —
        // emit full snapshots until that lands.
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
            .map_err(|e| PrecomputeError::Other(format!("CMSWrapper merge: {e}")))
    }

    fn merge(&mut self, other: &dyn Sketch) -> Result<(), PrecomputeError> {
        let bytes = other.snapshot()?;
        if bytes.is_empty() {
            return Ok(());
        }
        let decoded = Self::decode_envelope(&bytes)?;
        self.sk
            .merge(&decoded)
            .map_err(|e| PrecomputeError::Other(format!("CMSWrapper merge: {e}")))
    }

    fn reset(&mut self) {
        self.sk = CountMinSketch::new(self.rows, self.cols);
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl FrequencySketch for CMSWrapper {
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
        // CountMinSketch is a frequency estimator over a known key
        // set; it does not natively track top-k. Returning an empty
        // slice matches the Go wrapper.
        Vec::new()
    }
}

/// Observer routing `Bytes`-kind observations into the wrapper.
pub struct CMSObserver;

impl SketchObserver for CMSObserver {
    fn observe(
        &self,
        sketch: &mut dyn Sketch,
        v: &ObservationValue,
    ) -> Result<(), PrecomputeError> {
        let w = sketch
            .as_any_mut()
            .downcast_mut::<CMSWrapper>()
            .ok_or_else(|| {
                PrecomputeError::Other("CMSObserver: sketch is not a CMSWrapper".into())
            })?;
        if v.kind != crate::observation::ObservationValueKind::Bytes {
            return Err(PrecomputeError::Other(format!(
                "CMSObserver: expected Bytes, got {}",
                v.kind.name()
            )));
        }
        let s = std::str::from_utf8(&v.bytes).map_err(|e| {
            PrecomputeError::Other(format!("CMSObserver: bytes were not utf-8: {e}"))
        })?;
        w.update(s, 1.0);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_wrapper_is_empty() {
        let w = CMSWrapper::new(4, 32);
        assert_eq!(w.snapshot().unwrap().len(), 0);
    }

    #[test]
    fn update_then_estimate() {
        let mut w = CMSWrapper::new(8, 64);
        for _ in 0..50 {
            w.update("k1", 1.0);
        }
        for _ in 0..3 {
            w.update("k2", 1.0);
        }
        let k1 = w.estimate_count(b"k1");
        let k2 = w.estimate_count(b"k2");
        assert!(k1 >= 50.0, "k1 underestimate: {k1}");
        assert!(k2 >= 3.0, "k2 underestimate: {k2}");
    }

    #[test]
    fn snapshot_roundtrip_preserves_matrix() {
        let mut w = CMSWrapper::new(4, 8);
        w.update("k", 1.0);
        let bytes = w.snapshot().unwrap();
        let decoded = CMSWrapper::decode_envelope(&bytes).unwrap();
        assert_eq!(decoded.sketch(), w.sk.sketch());
    }
}
