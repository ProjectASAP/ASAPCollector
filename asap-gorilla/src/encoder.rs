//! Gorilla XOR-delta encoder. Bit-for-bit compatible with the Go
//! `gorillaprocessor.{bitWriter, gorillaTimestampEncoder,
//! gorillaValueEncoder}` implementations.

use std::collections::BTreeMap;
use std::io::Write;

use crate::block::{SeriesChunk, SeriesMeta, BLOCK_VERSION, MAGIC};
use crate::error::EncodeError;

/// Append-only bit packer. Mirrors the Go `bitWriter` exactly: bits
/// are packed MSB-first into each byte, and `bytes()` zero-pads to
/// the next byte boundary.
#[derive(Debug, Default)]
struct BitWriter {
    buf: Vec<u8>,
    cur_byte: u8,
    /// Number of bits filled in `cur_byte`, in `0..8`.
    nbits: u8,
}

impl BitWriter {
    fn write_bit(&mut self, bit: u8) {
        if bit & 1 != 0 {
            self.cur_byte |= 1 << (7 - self.nbits);
        }
        self.nbits += 1;
        if self.nbits == 8 {
            self.buf.push(self.cur_byte);
            self.cur_byte = 0;
            self.nbits = 0;
        }
    }

    fn write_bits(&mut self, v: u64, n: u8) {
        // n must be 1..=64 (0 is a no-op below).
        let mut i = n as i32 - 1;
        while i >= 0 {
            let bit = ((v >> i as u32) & 1) as u8;
            self.write_bit(bit);
            i -= 1;
        }
    }

    /// Length in bits of everything written so far (before
    /// byte-alignment). The Go encoder reports `len(bytes()) * 8`
    /// which folds the trailing zero pad into the count. We keep the
    /// same convention so the on-wire `*_bits_len` field is identical.
    fn bit_len_padded(&self) -> u32 {
        // After `bytes()` is called, len*8 includes the pad bits. The
        // helper below computes that length without consuming `self`.
        let pad = if self.nbits == 0 { 0 } else { 8 - self.nbits as usize };
        ((self.buf.len() * 8) + self.nbits as usize + pad) as u32
    }

    fn finish(mut self) -> Vec<u8> {
        // Flush any partial byte with trailing zero bits.
        if self.nbits != 0 {
            self.buf.push(self.cur_byte);
            self.cur_byte = 0;
            self.nbits = 0;
        }
        self.buf
    }
}

fn fits_in_signed_bits(v: i64, n: u8) -> bool {
    if n == 0 || n >= 64 {
        return true;
    }
    let min = -(1i64 << (n - 1));
    let max = (1i64 << (n - 1)) - 1;
    v >= min && v <= max
}

#[derive(Debug, Default)]
struct TsEncoder {
    bw: BitWriter,
    prev_ts: i64,
    prev_delta: i64,
    first_set: bool,
}

impl TsEncoder {
    fn new() -> Self {
        Self::default()
    }

    fn push(&mut self, ts: i64) {
        if !self.first_set {
            self.prev_ts = ts;
            self.prev_delta = 0;
            self.first_set = true;
            return;
        }
        let delta = ts.wrapping_sub(self.prev_ts);
        let dd = delta.wrapping_sub(self.prev_delta);
        if dd == 0 {
            self.bw.write_bit(0);
        } else if fits_in_signed_bits(dd, 7) {
            self.bw.write_bits(0b10, 2);
            self.bw.write_bits((dd as u64) & ((1u64 << 7) - 1), 7);
        } else if fits_in_signed_bits(dd, 9) {
            self.bw.write_bits(0b110, 3);
            self.bw.write_bits((dd as u64) & ((1u64 << 9) - 1), 9);
        } else if fits_in_signed_bits(dd, 12) {
            self.bw.write_bits(0b1110, 4);
            self.bw.write_bits((dd as u64) & ((1u64 << 12) - 1), 12);
        } else {
            self.bw.write_bits(0b1111, 4);
            self.bw.write_bits(dd as u64, 64);
        }
        self.prev_ts = ts;
        self.prev_delta = delta;
    }

    fn finish(self) -> (Vec<u8>, u32) {
        let bit_len = self.bw.bit_len_padded();
        (self.bw.finish(), bit_len)
    }
}

#[derive(Debug, Default)]
struct ValEncoder {
    bw: BitWriter,
    prev: u64,
    prev_set: bool,
    leading_zeros: u8,
    trailing_zeros: u8,
    have_prev_window: bool,
}

impl ValEncoder {
    fn new() -> Self {
        Self::default()
    }

    fn push(&mut self, v: f64) {
        let vb = v.to_bits();
        if !self.prev_set {
            self.prev = vb;
            self.prev_set = true;
            self.leading_zeros = 0;
            self.trailing_zeros = 0;
            self.have_prev_window = false;
            return;
        }
        let x = self.prev ^ vb;
        if x == 0 {
            self.bw.write_bit(0);
            self.prev = vb;
            return;
        }
        self.bw.write_bit(1);

        let lz = if x == 0 { 64 } else { x.leading_zeros() as u8 };
        let tz = if x == 0 { 64 } else { x.trailing_zeros() as u8 };
        let mut sig: u8 = 64 - lz - tz;

        if self.have_prev_window && lz >= self.leading_zeros && tz >= self.trailing_zeros {
            self.bw.write_bit(0);
            self.bw.write_bits(
                x >> self.trailing_zeros as u32,
                64 - self.leading_zeros - self.trailing_zeros,
            );
        } else {
            self.bw.write_bit(1);
            let lz5 = if lz > 31 { 31 } else { lz };
            self.bw.write_bits(lz5 as u64, 5);
            if sig == 0 {
                sig = 64;
            }
            let sig6 = sig - 1;
            self.bw.write_bits(sig6 as u64, 6);
            self.bw.write_bits(x >> tz as u32, sig);
            self.leading_zeros = lz;
            self.trailing_zeros = tz;
            self.have_prev_window = true;
        }
        self.prev = vb;
    }

    fn finish(self) -> (Vec<u8>, u32) {
        let bit_len = self.bw.bit_len_padded();
        (self.bw.finish(), bit_len)
    }
}

/// Encode one series's `(ts, value)` samples into the on-wire bit
/// streams documented in [`crate::block`]. The slice is sorted by
/// timestamp first, mirroring the Go encoder.
pub(crate) fn encode_series(samples: &[(i64, f64)]) -> (i64, u64, Vec<u8>, u32, Vec<u8>, u32) {
    if samples.is_empty() {
        return (0, 0, Vec::new(), 0, Vec::new(), 0);
    }
    let mut sorted: Vec<(i64, f64)> = samples.to_vec();
    sorted.sort_by_key(|p| p.0);

    let first_ts = sorted[0].0;
    let first_val_bits = sorted[0].1.to_bits();

    let mut ts_enc = TsEncoder::new();
    let mut val_enc = ValEncoder::new();
    for (ts, v) in &sorted {
        ts_enc.push(*ts);
        val_enc.push(*v);
    }
    let (ts_bits, ts_bits_len) = ts_enc.finish();
    let (val_bits, val_bits_len) = val_enc.finish();
    (
        first_ts,
        first_val_bits,
        ts_bits,
        ts_bits_len,
        val_bits,
        val_bits_len,
    )
}

/// Builder for a single Gorilla series chunk that is then serialized
/// into a `GORILLA1` block by [`GorillaEncoder::finalize`].
///
/// To pack multiple series into a single block, call
/// [`GorillaEncoder::with_series`] for each additional series before
/// calling [`GorillaEncoder::finalize`]. The single-series convenience
/// constructor [`GorillaEncoder::new`] covers the Phase 1 cold-engine
/// "one series per block" case.
#[derive(Debug)]
pub struct GorillaEncoder {
    series: Vec<SeriesChunk>,
    pending_metric: Option<String>,
    pending_labels: Option<Vec<(String, String)>>,
    pending_samples: Vec<(i64, f64)>,
}

impl GorillaEncoder {
    /// Start a single-series block with the given metric name and
    /// label set. Labels do not need to be pre-sorted; they are stored
    /// in a [`BTreeMap`] inside the per-series JSON metadata.
    pub fn new(metric: impl Into<String>, labels: Vec<(String, String)>) -> Self {
        Self {
            series: Vec::new(),
            pending_metric: Some(metric.into()),
            pending_labels: Some(labels),
            pending_samples: Vec::new(),
        }
    }

    /// Append a single sample to the in-progress series.
    ///
    /// Timestamp is interpreted as nanoseconds-since-Unix-epoch (Go
    /// `int64` UnixNano). If you need to encode a value out of `i64`
    /// range, the cast is the same lossless reinterpretation the Go
    /// side performs (`uint64(int64) → int64`).
    pub fn append(&mut self, ts_ns: u64, value: f64) {
        self.pending_samples.push((ts_ns as i64, value));
    }

    /// Append all samples from an iterator at once.
    pub fn append_all<I>(&mut self, it: I)
    where
        I: IntoIterator<Item = (u64, f64)>,
    {
        for (t, v) in it {
            self.append(t, v);
        }
    }

    /// Close the in-progress series and start a new one inside the
    /// same block. After calling this you may [`Self::append`] more
    /// samples, then optionally call this again, etc.
    pub fn with_series(mut self, metric: impl Into<String>, labels: Vec<(String, String)>) -> Self {
        self.flush_pending().expect("encoded chunk fits in u32");
        self.pending_metric = Some(metric.into());
        self.pending_labels = Some(labels);
        self.pending_samples.clear();
        self
    }

    fn flush_pending(&mut self) -> Result<(), EncodeError> {
        let Some(metric) = self.pending_metric.take() else {
            return Ok(());
        };
        let labels = self.pending_labels.take().unwrap_or_default();
        let samples = std::mem::take(&mut self.pending_samples);
        if samples.is_empty() {
            // A series with zero samples is a no-op; the Go side skips
            // the same case in `buildObjects`.
            return Ok(());
        }
        if samples.len() > u32::MAX as usize {
            return Err(EncodeError::SampleCountOverflow(samples.len()));
        }
        let (first_ts, first_val_bits, ts_bits, ts_bits_len, val_bits, val_bits_len) =
            encode_series(&samples);

        // Compute start/end TS from the *sorted* samples — the Go side
        // reads `buf.points[0].ts` and `buf.points[len-1].ts` after
        // `sort.Slice` has already mutated the slice in place. We
        // reproduce that exactly.
        let mut sorted_ts: Vec<i64> = samples.iter().map(|p| p.0).collect();
        sorted_ts.sort_unstable();
        let start_ts = *sorted_ts.first().unwrap();
        let end_ts = *sorted_ts.last().unwrap();

        let mut attributes: BTreeMap<String, String> = BTreeMap::new();
        for (k, v) in labels {
            attributes.insert(k, v);
        }
        let meta = SeriesMeta {
            metric_name: metric,
            attributes,
            start_ts,
            end_ts,
            point_count: samples.len(),
        };
        // Fail-fast on oversized metadata to surface the same error
        // path the Go encoder uses.
        let meta_bytes = serde_json::to_vec(&meta)?;
        if meta_bytes.len() > u16::MAX as usize {
            return Err(EncodeError::MetadataTooLarge(meta_bytes.len()));
        }

        self.series.push(SeriesChunk {
            meta,
            first_ts,
            first_val_bits,
            ts_bits_len,
            ts_bits,
            val_bits_len,
            val_bits,
        });
        Ok(())
    }

    /// Encode the in-progress block and return the full byte vector.
    pub fn finalize(mut self) -> Result<Vec<u8>, EncodeError> {
        self.flush_pending()?;
        let mut out = Vec::with_capacity(64);
        write_block(&mut out, &self.series)?;
        Ok(out)
    }

    /// Streaming variant of [`Self::finalize`] for callers that want
    /// to write directly into an `io::Write` (e.g. a tempfile or an
    /// S3 multipart-upload buffer).
    pub fn finalize_into<W: Write>(mut self, mut w: W) -> Result<(), EncodeError> {
        self.flush_pending()?;
        write_block(&mut w, &self.series)?;
        Ok(())
    }
}

/// Serialize a list of already-encoded series chunks into a complete
/// `GORILLA1` block.
pub fn write_block<W: Write>(w: &mut W, series: &[SeriesChunk]) -> Result<(), EncodeError> {
    w.write_all(&MAGIC)?;
    w.write_all(&[BLOCK_VERSION])?;
    let series_count = series.len() as u32;
    w.write_all(&series_count.to_le_bytes())?;

    for s in series {
        let meta_bytes = serde_json::to_vec(&s.meta)?;
        if meta_bytes.len() > u16::MAX as usize {
            return Err(EncodeError::MetadataTooLarge(meta_bytes.len()));
        }
        w.write_all(&(meta_bytes.len() as u16).to_le_bytes())?;
        w.write_all(&meta_bytes)?;
        w.write_all(&(s.meta.point_count as u32).to_le_bytes())?;
        w.write_all(&(s.first_ts as u64).to_le_bytes())?;
        w.write_all(&s.first_val_bits.to_le_bytes())?;
        w.write_all(&s.ts_bits_len.to_le_bytes())?;
        w.write_all(&s.ts_bits)?;
        w.write_all(&s.val_bits_len.to_le_bytes())?;
        w.write_all(&s.val_bits)?;
    }
    Ok(())
}
