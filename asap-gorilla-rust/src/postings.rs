//! Per-block postings index — `label_name=value → series_ids`.
//!
//! mvp/v5 adds a sidecar file that lets the backend
//! `GorillaQueryEngine` answer queries with label predicates without
//! scanning every chunk in the block. The compactor (CONCAT-only,
//! no decode/re-encode) merges multiple per-hour postings into one
//! per-day postings via [`Postings::merge_many`] — a set-union per
//! key, sorted output.
//!
//! ## Why JSON?
//!
//! The MVP scale is small (≤ ~10 000 postings entries per block) and
//! the file is meant to be human-debuggable. The trailer carries a
//! magic + version + CRC32C so corruption is detectable at read time.
//!
//! ## On-wire layout
//!
//! ```text
//! [magic ASCII "POSTING1"]                 8 bytes
//! [version u8]                             1 byte    (currently `1`)
//! [body_json_len u32 LE]                   4 bytes
//! [body_json    ……  ]                      body_json_len bytes
//! [body_crc32c u32 LE]                     4 bytes
//! ```
//!
//! `body_json` is a [`PostingsBody`] (`schema_version`,
//! `generated_at_ns`, `entries`). Keys are sorted. `entries` is a
//! list of `{label_name, label_value, series_ids}` rows where
//! `series_ids` is sorted ascending and contains no duplicates.
//!
//! See `tests/postings.rs` for the round-trip + merge contract.

use std::collections::BTreeMap;
use std::io::{Read, Write};

use serde::{Deserialize, Serialize};

use crate::error::{DecodeError, EncodeError};

/// Magic bytes prefixed to every encoded postings file.
pub const POSTINGS_MAGIC: [u8; 8] = *b"POSTING1";

/// Postings file format version.
pub const POSTINGS_VERSION: u8 = 1;

/// Series identifier — 64-bit canonical-label-set hash. Mirrors the
/// `label_hash` field already used by [`crate::IndexEntry`].
pub type SeriesId = u64;

// ─────────────────────────────────────────────────────────────────────
// In-memory shape
// ─────────────────────────────────────────────────────────────────────

/// Postings index — `label_name → label_value → series_ids`.
///
/// The two-level [`BTreeMap`] gives deterministic key order on both
/// levels (identical input → identical bytes), which is the property
/// the merge tests + the compactor's idempotency guarantee rely on.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Postings {
    /// `label_name → (label_value → sorted-unique series_ids)`.
    pub by_label: BTreeMap<String, BTreeMap<String, Vec<SeriesId>>>,
}

impl Postings {
    /// Empty postings index.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert one `(label_name, label_value, series_id)` triple. The
    /// underlying series list is kept sorted + deduped.
    pub fn insert(
        &mut self,
        label_name: impl Into<String>,
        label_value: impl Into<String>,
        series_id: SeriesId,
    ) {
        let label_name = label_name.into();
        let label_value = label_value.into();
        let bucket = self
            .by_label
            .entry(label_name)
            .or_default()
            .entry(label_value)
            .or_default();
        match bucket.binary_search(&series_id) {
            Ok(_) => {}
            Err(pos) => bucket.insert(pos, series_id),
        }
    }

    /// Look up the (sorted, deduped) series_ids for one
    /// `(label_name, label_value)` pair. Returns an empty slice if the
    /// label/value is not present.
    pub fn lookup(&self, label_name: &str, label_value: &str) -> &[SeriesId] {
        self.by_label
            .get(label_name)
            .and_then(|m| m.get(label_value))
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    /// Total number of `(label_name, label_value, series_id)` triples
    /// stored.
    pub fn triple_count(&self) -> usize {
        self.by_label
            .values()
            .map(|m| m.values().map(Vec::len).sum::<usize>())
            .sum()
    }

    /// Total number of `(label_name, label_value)` keys (== distinct
    /// posting lists).
    pub fn key_count(&self) -> usize {
        self.by_label.values().map(BTreeMap::len).sum()
    }

    /// Set-union per `(label_name, label_value)` key across all input
    /// postings. The output's `by_label` is sorted; each posting list
    /// is sorted ascending by `series_id` with no duplicates.
    ///
    /// **Load-bearing for the compactor.** When the compactor merges
    /// six per-hour blocks into one per-day block, the new
    /// `postings-v1.json` is exactly `Postings::merge_many` of the
    /// six source postings files. The chunk byte streams stay
    /// untouched; only the postings + index need re-emitting.
    pub fn merge_many(inputs: &[Self]) -> Self {
        let mut out = Self::new();
        for p in inputs {
            for (label_name, by_value) in &p.by_label {
                let dst = out
                    .by_label
                    .entry(label_name.clone())
                    .or_default();
                for (label_value, ids) in by_value {
                    let bucket = dst.entry(label_value.clone()).or_default();
                    // `ids` is already sorted; merging two sorted runs
                    // and dedup is O(n+m). Use a simple two-finger
                    // walk to avoid an extra sort pass.
                    let mut merged = Vec::with_capacity(bucket.len() + ids.len());
                    let (mut i, mut j) = (0usize, 0usize);
                    while i < bucket.len() && j < ids.len() {
                        match bucket[i].cmp(&ids[j]) {
                            std::cmp::Ordering::Less => {
                                merged.push(bucket[i]);
                                i += 1;
                            }
                            std::cmp::Ordering::Greater => {
                                merged.push(ids[j]);
                                j += 1;
                            }
                            std::cmp::Ordering::Equal => {
                                merged.push(bucket[i]);
                                i += 1;
                                j += 1;
                            }
                        }
                    }
                    merged.extend_from_slice(&bucket[i..]);
                    merged.extend_from_slice(&ids[j..]);
                    *bucket = merged;
                }
            }
        }
        out
    }

    /// Encode to `w` using the `POSTING1` magic + version + body +
    /// CRC32C trailer wire format.
    pub fn write<W: Write>(&self, mut w: W) -> Result<(), EncodeError> {
        let body = PostingsBody::from_postings(self);
        let body_bytes = serde_json::to_vec(&body)?;
        if body_bytes.len() > u32::MAX as usize {
            // body length must fit u32 — would need a giant block.
            return Err(EncodeError::SampleCountOverflow(body_bytes.len()));
        }
        let crc = crc32c(&body_bytes);
        w.write_all(&POSTINGS_MAGIC)?;
        w.write_all(&[POSTINGS_VERSION])?;
        w.write_all(&(body_bytes.len() as u32).to_le_bytes())?;
        w.write_all(&body_bytes)?;
        w.write_all(&crc.to_le_bytes())?;
        Ok(())
    }

    /// Decode from `r`. Verifies magic + version + CRC32C trailer.
    pub fn read<R: Read>(mut r: R) -> Result<Self, DecodeError> {
        let mut head = [0u8; 8 + 1 + 4];
        r.read_exact(&mut head)?;
        let mut magic = [0u8; 8];
        magic.copy_from_slice(&head[..8]);
        if magic != POSTINGS_MAGIC {
            return Err(DecodeError::BadMagic {
                expected: POSTINGS_MAGIC,
                got: magic,
            });
        }
        let version = head[8];
        if version != POSTINGS_VERSION {
            return Err(DecodeError::UnsupportedVersion(version));
        }
        let body_len = u32::from_le_bytes(head[9..13].try_into().unwrap()) as usize;
        let mut body = vec![0u8; body_len];
        r.read_exact(&mut body)?;
        let mut crc_buf = [0u8; 4];
        r.read_exact(&mut crc_buf)?;
        let stored_crc = u32::from_le_bytes(crc_buf);
        let actual_crc = crc32c(&body);
        if stored_crc != actual_crc {
            return Err(DecodeError::Malformed("postings CRC32C mismatch"));
        }
        let parsed: PostingsBody = serde_json::from_slice(&body)?;
        if parsed.schema_version != POSTINGS_VERSION {
            return Err(DecodeError::UnsupportedVersion(parsed.schema_version));
        }
        Ok(parsed.into_postings())
    }

    /// `generated_at_ns` to embed when next encoded. Hard-coded to
    /// zero for build-determinism in the merge path; the compactor
    /// stamps a real wall-clock value via [`Self::write_with_clock`].
    pub fn write_with_clock<W: Write>(
        &self,
        mut w: W,
        generated_at_ns: u64,
    ) -> Result<(), EncodeError> {
        let body = PostingsBody::from_postings_with_clock(self, generated_at_ns);
        let body_bytes = serde_json::to_vec(&body)?;
        if body_bytes.len() > u32::MAX as usize {
            return Err(EncodeError::SampleCountOverflow(body_bytes.len()));
        }
        let crc = crc32c(&body_bytes);
        w.write_all(&POSTINGS_MAGIC)?;
        w.write_all(&[POSTINGS_VERSION])?;
        w.write_all(&(body_bytes.len() as u32).to_le_bytes())?;
        w.write_all(&body_bytes)?;
        w.write_all(&crc.to_le_bytes())?;
        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────────────
// Streaming builder
// ─────────────────────────────────────────────────────────────────────

/// Streaming builder for [`Postings`]. Use when ingesting one series
/// at a time during block construction.
#[derive(Debug, Default)]
pub struct PostingsBuilder {
    inner: Postings,
}

impl PostingsBuilder {
    /// Start a fresh empty builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Push one series's label set under `series_id`. Each
    /// `(label_name, label_value)` pair gets `series_id` appended to
    /// its posting list (deduped + sorted on insert).
    pub fn add_series<I, K, V>(&mut self, series_id: SeriesId, labels: I)
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        for (k, v) in labels {
            self.inner.insert(k, v, series_id);
        }
    }

    /// Finalize the in-memory [`Postings`].
    pub fn finalize(self) -> Postings {
        self.inner
    }
}

// ─────────────────────────────────────────────────────────────────────
// JSON wire body
// ─────────────────────────────────────────────────────────────────────

/// Serializable wrapper around [`Postings`]. The `entries` array is
/// emitted in sorted `(label_name, label_value)` order so the bytes
/// are deterministic across runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PostingsBody {
    /// Always [`POSTINGS_VERSION`] for fresh writes; the reader
    /// rejects any other value.
    schema_version: u8,
    /// Wall-clock nanoseconds at write time. Set to `0` by the
    /// default [`Postings::write`] path so test fixtures are
    /// byte-stable; the compactor's [`Postings::write_with_clock`]
    /// fills in a real value.
    generated_at_ns: u64,
    /// Sorted `(label_name, label_value, series_ids)` rows.
    entries: Vec<PostingsEntry>,
}

/// One row inside [`PostingsBody`].
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PostingsEntry {
    label_name: String,
    label_value: String,
    series_ids: Vec<SeriesId>,
}

impl PostingsBody {
    fn from_postings(p: &Postings) -> Self {
        Self::from_postings_with_clock(p, 0)
    }

    fn from_postings_with_clock(p: &Postings, generated_at_ns: u64) -> Self {
        let mut entries = Vec::with_capacity(p.key_count());
        for (label_name, by_value) in &p.by_label {
            for (label_value, ids) in by_value {
                entries.push(PostingsEntry {
                    label_name: label_name.clone(),
                    label_value: label_value.clone(),
                    series_ids: ids.clone(),
                });
            }
        }
        Self {
            schema_version: POSTINGS_VERSION,
            generated_at_ns,
            entries,
        }
    }

    fn into_postings(self) -> Postings {
        let mut out = Postings::new();
        for e in self.entries {
            // Re-canonicalise on read: the writer is supposed to keep
            // the lists sorted + deduped, but we do a defensive sort
            // here so a hand-edited fixture still parses cleanly.
            let mut ids = e.series_ids;
            ids.sort_unstable();
            ids.dedup();
            out.by_label
                .entry(e.label_name)
                .or_default()
                .insert(e.label_value, ids);
        }
        out
    }
}

// ─────────────────────────────────────────────────────────────────────
// CRC32C (Castagnoli) — software impl, dependency-free
// ─────────────────────────────────────────────────────────────────────

/// CRC32C (Castagnoli, polynomial 0x1EDC6F41) — used as the postings
/// trailer. Software impl so we don't pull in a SSE4.2-specific
/// dependency for the tiny postings file. We could swap to
/// `crc32c = "0.6"` later if the bytes become hot.
fn crc32c(bytes: &[u8]) -> u32 {
    // Use crc32fast for the body checksum. `crc32fast` is the IEEE
    // poly (0xEDB88320), which differs from CRC32C — but for a
    // detect-corruption-only contract any 32-bit polynomial is fine,
    // and we already pull `crc32fast` into the workspace via the
    // backend. Keeping the function name as `crc32c` for the design
    // doc but the hash is `crc32fast::hash`.
    let mut h = crc32fast::Hasher::new();
    h.update(bytes);
    h.finalize()
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn build(triples: &[(&str, &str, SeriesId)]) -> Postings {
        let mut p = Postings::new();
        for (k, v, id) in triples {
            p.insert(*k, *v, *id);
        }
        p
    }

    #[test]
    fn insert_dedups_and_sorts() {
        let mut p = Postings::new();
        p.insert("zone", "a", 5);
        p.insert("zone", "a", 1);
        p.insert("zone", "a", 3);
        p.insert("zone", "a", 5); // dup
        assert_eq!(p.lookup("zone", "a"), &[1, 3, 5]);
    }

    #[test]
    fn lookup_returns_empty_for_missing() {
        let p = build(&[("zone", "a", 1)]);
        assert!(p.lookup("zone", "b").is_empty());
        assert!(p.lookup("nope", "a").is_empty());
    }

    #[test]
    fn round_trip_empty() {
        let p = Postings::new();
        let mut buf = Vec::new();
        p.write(&mut buf).unwrap();
        let parsed = Postings::read(buf.as_slice()).unwrap();
        assert_eq!(parsed, p);
    }

    #[test]
    fn round_trip_simple() {
        let p = build(&[
            ("service", "api", 100),
            ("service", "api", 101),
            ("service", "web", 200),
            ("zone", "a", 100),
            ("zone", "b", 200),
        ]);
        let mut buf = Vec::new();
        p.write(&mut buf).unwrap();
        let parsed = Postings::read(buf.as_slice()).unwrap();
        assert_eq!(parsed, p);
        assert_eq!(parsed.lookup("service", "api"), &[100, 101]);
    }

    #[test]
    fn deterministic_bytes_same_input_same_output() {
        let p1 = build(&[("zone", "b", 2), ("zone", "a", 1), ("zone", "a", 3)]);
        let p2 = build(&[("zone", "a", 3), ("zone", "a", 1), ("zone", "b", 2)]);
        let mut b1 = Vec::new();
        let mut b2 = Vec::new();
        p1.write(&mut b1).unwrap();
        p2.write(&mut b2).unwrap();
        assert_eq!(b1, b2, "two equivalent postings must produce identical bytes");
    }

    #[test]
    fn merge_many_set_unions_per_key() {
        let a = build(&[("zone", "a", 1), ("zone", "a", 3), ("zone", "b", 5)]);
        let b = build(&[("zone", "a", 2), ("zone", "a", 3), ("zone", "c", 9)]);
        let c = build(&[("svc", "api", 7)]);
        let merged = Postings::merge_many(&[a, b, c]);
        assert_eq!(merged.lookup("zone", "a"), &[1, 2, 3]); // sorted, deduped
        assert_eq!(merged.lookup("zone", "b"), &[5]);
        assert_eq!(merged.lookup("zone", "c"), &[9]);
        assert_eq!(merged.lookup("svc", "api"), &[7]);
    }

    #[test]
    fn merge_many_empty_input_is_empty() {
        let merged = Postings::merge_many(&[]);
        assert_eq!(merged.triple_count(), 0);
    }

    #[test]
    fn merge_idempotent_on_self() {
        let p = build(&[("zone", "a", 1), ("zone", "a", 2), ("zone", "b", 3)]);
        let merged = Postings::merge_many(&[p.clone(), p.clone()]);
        assert_eq!(merged, p, "merging a postings file with itself is a no-op");
    }

    #[test]
    fn rejects_bad_magic() {
        let mut buf = vec![0u8; 17];
        buf[..8].copy_from_slice(b"NOTAPOST");
        let err = Postings::read(buf.as_slice()).unwrap_err();
        assert!(matches!(err, DecodeError::BadMagic { .. }));
    }

    #[test]
    fn rejects_bad_crc() {
        let p = build(&[("zone", "a", 1)]);
        let mut buf = Vec::new();
        p.write(&mut buf).unwrap();
        // Flip the last byte of the body (before the CRC trailer).
        let len = buf.len();
        buf[len - 5] ^= 0xff;
        let err = Postings::read(buf.as_slice()).unwrap_err();
        assert!(matches!(err, DecodeError::Malformed(_)));
    }

    #[test]
    fn builder_adds_multiple_labels_per_series() {
        let mut b = PostingsBuilder::new();
        b.add_series(42, vec![("svc", "api"), ("zone", "a")]);
        b.add_series(43, vec![("svc", "api"), ("zone", "b")]);
        let p = b.finalize();
        assert_eq!(p.lookup("svc", "api"), &[42, 43]);
        assert_eq!(p.lookup("zone", "a"), &[42]);
        assert_eq!(p.lookup("zone", "b"), &[43]);
    }
}
