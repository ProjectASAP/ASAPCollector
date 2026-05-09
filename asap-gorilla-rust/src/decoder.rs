//! Streaming Gorilla XOR-delta decoder.
//!
//! Mirrors the test-only decoder in
//! `opentelemetry-collector-contrib-patch/processor/gorillaprocessor/
//! encoder_test.go::{decodeTimestamps, decodeValues}`, but exposes a
//! sample iterator so the backend `GorillaQueryEngine` can stream
//! through cold-store chunks without materializing every sample.

use std::io::Read;

use crate::block::{SeriesChunk, SeriesMeta, BLOCK_VERSION, HEADER_LEN, MAGIC};
use crate::error::DecodeError;

/// Materialized header that the [`GorillaDecoder`] surfaces to the
/// caller before it iterates samples.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedHeader {
    /// Metric name from the chunk's JSON metadata.
    pub metric: String,
    /// Sorted (BTreeMap-ordered) label set.
    pub labels: Vec<(String, String)>,
    /// `(start_ts_ns, end_ts_ns)` from the JSON metadata.
    pub time_range: (u64, u64),
    /// Number of samples — always equals the number the iterator
    /// returns on success.
    pub sample_count: u32,
}

impl From<&SeriesMeta> for DecodedHeader {
    fn from(meta: &SeriesMeta) -> Self {
        Self {
            metric: meta.metric_name.clone(),
            labels: meta
                .attributes
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            time_range: (meta.start_ts as u64, meta.end_ts as u64),
            sample_count: meta.point_count as u32,
        }
    }
}

/// Streaming decoder over a `GORILLA1` block.
///
/// Construct with [`Self::from_reader`], inspect the per-series
/// header(s) via [`Self::header`] / [`Self::headers`], then iterate
/// the per-series samples with [`Self::samples`]. Multi-series blocks
/// are read lazily: subsequent series are not parsed until
/// [`Self::next_series`] advances the cursor.
pub struct GorillaDecoder<R: Read> {
    reader: R,
    series_remaining: u32,
    /// The chunk currently exposed by [`Self::header`] /
    /// [`Self::samples`]. `None` means we have not yet advanced to the
    /// first series, or have run past the last.
    current: Option<LoadedChunk>,
}

struct LoadedChunk {
    header: DecodedHeader,
    chunk: SeriesChunk,
}

impl<R: Read> GorillaDecoder<R> {
    /// Read the block magic + fixed header from `r` and prepare to
    /// stream series. The reader is consumed lazily — only the header
    /// bytes are pulled here.
    pub fn from_reader(mut r: R) -> Result<Self, DecodeError> {
        let mut header = [0u8; HEADER_LEN];
        r.read_exact(&mut header)?;
        let mut magic = [0u8; 8];
        magic.copy_from_slice(&header[..8]);
        if magic != MAGIC {
            return Err(DecodeError::BadMagic {
                expected: MAGIC,
                got: magic,
            });
        }
        let version = header[8];
        if version != BLOCK_VERSION {
            return Err(DecodeError::UnsupportedVersion(version));
        }
        let series_remaining = u32::from_le_bytes(header[9..13].try_into().unwrap());
        let mut me = Self {
            reader: r,
            series_remaining,
            current: None,
        };
        // Eagerly load the first series so `header()` is callable
        // without an extra advance step. If the block contains zero
        // series, `current` stays `None`.
        if me.series_remaining > 0 {
            me.load_next_chunk()?;
        }
        Ok(me)
    }

    /// Number of series in this block (as advertised by the block
    /// header).
    pub fn total_series(&self) -> u32 {
        self.series_remaining + self.current.as_ref().map_or(0, |_| 1)
    }

    /// Header of the *current* series (the one that
    /// [`Self::samples`] would iterate). Returns `None` past the end
    /// of the block.
    pub fn header(&self) -> Option<&DecodedHeader> {
        self.current.as_ref().map(|c| &c.header)
    }

    /// Convenience: read all per-series headers into a `Vec` without
    /// streaming any samples. This re-parses every series chunk so it
    /// is `O(series_count * meta_size)`.
    ///
    /// Multi-series blocks are uncommon in the cold-engine path
    /// (Phase 1 emits one series per block) so this is fine for
    /// catalog-style consumers.
    pub fn headers(reader: R) -> Result<Vec<DecodedHeader>, DecodeError> {
        let mut dec = Self::from_reader(reader)?;
        let mut out = Vec::new();
        while let Some(h) = dec.header() {
            out.push(h.clone());
            dec.skip_to_next_series()?;
        }
        Ok(out)
    }

    /// Advance to the next series in the block. Returns `Ok(true)`
    /// if there is another series, `Ok(false)` otherwise.
    pub fn next_series(&mut self) -> Result<bool, DecodeError> {
        if self.series_remaining == 0 {
            self.current = None;
            return Ok(false);
        }
        self.load_next_chunk()?;
        Ok(true)
    }

    fn skip_to_next_series(&mut self) -> Result<(), DecodeError> {
        // Same as next_series but discards the result.
        let _ = self.next_series()?;
        Ok(())
    }

    fn load_next_chunk(&mut self) -> Result<(), DecodeError> {
        let mut meta_len_buf = [0u8; 2];
        self.reader.read_exact(&mut meta_len_buf)?;
        let meta_len = u16::from_le_bytes(meta_len_buf) as usize;

        let mut meta_bytes = vec![0u8; meta_len];
        self.reader.read_exact(&mut meta_bytes)?;
        let meta: SeriesMeta = serde_json::from_slice(&meta_bytes)?;

        let mut u32_buf = [0u8; 4];
        let mut u64_buf = [0u8; 8];

        self.reader.read_exact(&mut u32_buf)?;
        let point_count = u32::from_le_bytes(u32_buf);

        self.reader.read_exact(&mut u64_buf)?;
        let first_ts = i64::from_le_bytes(u64_buf);

        self.reader.read_exact(&mut u64_buf)?;
        let first_val_bits = u64::from_le_bytes(u64_buf);

        self.reader.read_exact(&mut u32_buf)?;
        let ts_bits_len = u32::from_le_bytes(u32_buf);

        let ts_byte_len = ts_bits_len.div_ceil(8) as usize;
        let mut ts_bits = vec![0u8; ts_byte_len];
        self.reader.read_exact(&mut ts_bits)?;

        self.reader.read_exact(&mut u32_buf)?;
        let val_bits_len = u32::from_le_bytes(u32_buf);

        let val_byte_len = val_bits_len.div_ceil(8) as usize;
        let mut val_bits = vec![0u8; val_byte_len];
        self.reader.read_exact(&mut val_bits)?;

        let chunk = SeriesChunk {
            meta: meta.clone(),
            first_ts,
            first_val_bits,
            ts_bits_len,
            ts_bits,
            val_bits_len,
            val_bits,
        };
        // Cross-check the redundant `point_count` field against the
        // metadata. The Go encoder writes them from the same source so
        // a mismatch indicates a corrupt block.
        if point_count as usize != meta.point_count {
            return Err(DecodeError::Malformed(
                "point_count header disagrees with series metadata",
            ));
        }
        let header = DecodedHeader::from(&chunk.meta);
        self.current = Some(LoadedChunk { header, chunk });
        self.series_remaining -= 1;
        Ok(())
    }

    /// Iterator over `(ts_ns, value)` pairs for the *current* series.
    /// Each call returns a fresh iterator over the same chunk — useful
    /// for double-passes during testing. For the streaming hot path,
    /// store the iterator and consume it once.
    pub fn samples(&self) -> SampleIter<'_> {
        let chunk = self
            .current
            .as_ref()
            .map(|c| &c.chunk)
            .expect("samples() called past end of block; check header().is_some() first");
        SampleIter::new(chunk)
    }
}

/// Iterator returned by [`GorillaDecoder::samples`].
pub struct SampleIter<'a> {
    point_count: u32,
    emitted: u32,
    first_ts: i64,
    first_val_bits: u64,

    ts_reader: BitReader<'a>,
    val_reader: BitReader<'a>,
    ts_state: TsState,
    val_state: ValState,
}

impl<'a> SampleIter<'a> {
    fn new(chunk: &'a SeriesChunk) -> Self {
        Self {
            point_count: chunk.meta.point_count as u32,
            emitted: 0,
            first_ts: chunk.first_ts,
            first_val_bits: chunk.first_val_bits,
            ts_reader: BitReader::new(&chunk.ts_bits, chunk.ts_bits_len),
            val_reader: BitReader::new(&chunk.val_bits, chunk.val_bits_len),
            ts_state: TsState::default(),
            val_state: ValState::default(),
        }
    }
}

impl<'a> Iterator for SampleIter<'a> {
    type Item = Result<(u64, f64), DecodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.emitted >= self.point_count {
            return None;
        }
        let idx = self.emitted as usize;
        self.emitted += 1;

        if idx == 0 {
            self.ts_state.prev_ts = self.first_ts;
            self.ts_state.prev_delta = 0;
            self.val_state.prev = self.first_val_bits;
            return Some(Ok((self.first_ts as u64, f64::from_bits(self.first_val_bits))));
        }

        let ts = match self.ts_state.next(&mut self.ts_reader, idx) {
            Ok(v) => v,
            Err(e) => return Some(Err(e)),
        };
        let v = match self.val_state.next(&mut self.val_reader, idx) {
            Ok(v) => v,
            Err(e) => return Some(Err(e)),
        };
        Some(Ok((ts as u64, f64::from_bits(v))))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = (self.point_count - self.emitted) as usize;
        (remaining, Some(remaining))
    }
}

#[derive(Default)]
struct TsState {
    prev_ts: i64,
    prev_delta: i64,
}

impl TsState {
    fn next(&mut self, r: &mut BitReader<'_>, idx: usize) -> Result<i64, DecodeError> {
        let b = r.read_bit().ok_or(DecodeError::Truncated { sample_idx: idx })?;
        let dd: i64 = if b == 0 {
            0
        } else {
            let b2 = r.read_bit().ok_or(DecodeError::Truncated { sample_idx: idx })?;
            if b2 == 0 {
                let v = r.read_bits(7).ok_or(DecodeError::Truncated { sample_idx: idx })?;
                sign_extend(v, 7)
            } else {
                let b3 = r.read_bit().ok_or(DecodeError::Truncated { sample_idx: idx })?;
                if b3 == 0 {
                    let v = r.read_bits(9).ok_or(DecodeError::Truncated { sample_idx: idx })?;
                    sign_extend(v, 9)
                } else {
                    let b4 = r.read_bit().ok_or(DecodeError::Truncated { sample_idx: idx })?;
                    if b4 == 0 {
                        let v = r.read_bits(12).ok_or(DecodeError::Truncated { sample_idx: idx })?;
                        sign_extend(v, 12)
                    } else {
                        let v = r.read_bits(64).ok_or(DecodeError::Truncated { sample_idx: idx })?;
                        v as i64
                    }
                }
            }
        };
        let delta = self.prev_delta.wrapping_add(dd);
        let ts = self.prev_ts.wrapping_add(delta);
        self.prev_ts = ts;
        self.prev_delta = delta;
        Ok(ts)
    }
}

#[derive(Default)]
struct ValState {
    prev: u64,
    lz: u8,
    tz: u8,
    have_window: bool,
}

impl ValState {
    fn next(&mut self, r: &mut BitReader<'_>, idx: usize) -> Result<u64, DecodeError> {
        let c = r.read_bit().ok_or(DecodeError::Truncated { sample_idx: idx })?;
        let vb: u64 = if c == 0 {
            self.prev
        } else {
            let c2 = r.read_bit().ok_or(DecodeError::Truncated { sample_idx: idx })?;
            if c2 == 0 {
                if !self.have_window {
                    return Err(DecodeError::Malformed(
                        "value reuse-window bit before window was set",
                    ));
                }
                let sig_len = 64 - self.lz - self.tz;
                let sig = r
                    .read_bits(sig_len)
                    .ok_or(DecodeError::Truncated { sample_idx: idx })?;
                let x = sig << self.tz as u32;
                self.prev ^ x
            } else {
                let lz5 = r.read_bits(5).ok_or(DecodeError::Truncated { sample_idx: idx })?;
                self.lz = lz5 as u8;
                let sig_m1 = r.read_bits(6).ok_or(DecodeError::Truncated { sample_idx: idx })?;
                let sig_len = (sig_m1 as u8) + 1;
                let sig = r
                    .read_bits(sig_len)
                    .ok_or(DecodeError::Truncated { sample_idx: idx })?;
                self.tz = if sig_len == 64 {
                    0
                } else {
                    64 - self.lz - sig_len
                };
                let x = sig << self.tz as u32;
                self.have_window = true;
                self.prev ^ x
            }
        };
        self.prev = vb;
        Ok(vb)
    }
}

fn sign_extend(v: u64, n: u8) -> i64 {
    if n == 0 || n >= 64 {
        return v as i64;
    }
    let shift = 64 - n;
    ((v << shift) as i64) >> shift
}

struct BitReader<'a> {
    bytes: &'a [u8],
    bit_len: u32,
    off: u32,
}

impl<'a> BitReader<'a> {
    fn new(bytes: &'a [u8], bit_len: u32) -> Self {
        Self {
            bytes,
            bit_len,
            off: 0,
        }
    }

    fn read_bit(&mut self) -> Option<u8> {
        if self.off >= self.bit_len {
            return None;
        }
        let byte_idx = (self.off / 8) as usize;
        let bit_idx = (self.off % 8) as u8;
        let bit = (self.bytes[byte_idx] >> (7 - bit_idx)) & 1;
        self.off += 1;
        Some(bit)
    }

    fn read_bits(&mut self, n: u8) -> Option<u64> {
        if n == 0 {
            return Some(0);
        }
        let mut v: u64 = 0;
        for _ in 0..n {
            let bit = self.read_bit()?;
            v = (v << 1) | (bit as u64);
        }
        Some(v)
    }
}
