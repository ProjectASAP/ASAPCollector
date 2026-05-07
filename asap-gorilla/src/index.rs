//! Per-hour-bucket `index.json` catalog of cold-store chunks.
//!
//! The Telegraf-side `gorilla_s3` output writes one or more
//! `<tenant>/<metric>/YYYY/MM/DD/HH/part-NNNNNN.gor` chunks per hour.
//! A sibling `<tenant>/<metric>/YYYY/MM/DD/HH/index.json` enumerates
//! them so the backend `GorillaQueryEngine` can prune by time / label
//! without listing the bucket. This module owns the on-wire format
//! for that index file.

use std::io::{Read, Write};

use serde::{Deserialize, Serialize};

use crate::error::{DecodeError, EncodeError};

/// Schema version of [`IndexFile`]. Bump if the JSON layout changes
/// in a backwards-incompatible way.
///
/// **mvp/v5**: still `1`. The `byte_offset` / `byte_length` /
/// `object_key` extension on [`IndexEntry`] is purely additive — old
/// chunks (no compactor merge) keep all three at their defaults
/// (`object_key = key`, `byte_offset = 0`, `byte_length = size_bytes`)
/// and load fine on both old and new readers. See module docs for
/// the contract.
pub const INDEX_SCHEMA_VERSION: u8 = 1;

/// One entry in the per-hour [`IndexFile`].
///
/// ### mvp/v5 — chunk-manifest extension for compactor-merged blocks
///
/// The compactor produces ONE big `block-NNNN-MMMM.gor` object that
/// is the literal byte concatenation of N source `part-*.gor` files.
/// Each source chunk remains an atomic, individually-decodable
/// Gorilla chunk inside the merged object. The new manifest fields
/// record where each chunk lives:
///
/// * `object_key`  → the merged-object's key (e.g. `block-0000-0006.gor`)
/// * `byte_offset` → offset of this chunk within the merged object
/// * `byte_length` → length of this chunk's bytes inside the merged object
///
/// The query engine issues `Range: bytes=START-END` partial reads
/// against `object_key` to fetch exactly the chunk it wants.
///
/// **Backward compat**: `object_key` is `Option<String>` and
/// `byte_offset` / `byte_length` are `Option<u64> / Option<u32>`. Old
/// fixtures parse with all three at `None` and resolve to "fetch
/// whole object at `key`". The `effective_*` helpers below pick the
/// right value regardless of producer.
///
/// ### v7 — agent-side schema compatibility
///
/// The Telegraf-side / OTel `gorillas3processor` writes JSON with
/// slightly different field names:
///
/// | Agent field          | Backend field |
/// |----------------------|---------------|
/// | `object`             | `key`         |
/// | `start_ts_nano`      | (lower half of `time_range`) |
/// | `end_ts_nano`        | (upper half of `time_range`) |
/// | `point_count`        | `sample_count` |
/// | `series_count`       | (ignored — agent writes one chunk per series) |
/// | `written_at_unix_nano` | (ignored — purely informational) |
///
/// v7 implements a custom `Deserialize` that accepts both shapes,
/// so backend reads of agent-produced index.json files work
/// unchanged. Existing backend-produced index.json files keep
/// roundtripping bit-for-bit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IndexEntry {
    /// Full S3 key for this *chunk* (legacy field). For pre-compactor
    /// blocks this is the key of the chunk's own object. For compactor-
    /// merged blocks it points at the chunk within the merged file —
    /// the [`Self::object_key`] field then carries the merged file's
    /// key, and [`Self::byte_offset`] / [`Self::byte_length`] mark the
    /// partial-read window. Readers that don't consult the new fields
    /// will read the whole merged file by mistake — see
    /// [`Self::effective_object_key`] for the correct lookup.
    pub key: String,
    /// `(start_ts_ns, end_ts_ns)` covered by the chunk.
    pub time_range: (u64, u64),
    /// Number of samples in the chunk.
    pub sample_count: u32,
    /// 64-bit hash of the canonical sorted label set, for prune-by-
    /// label-equality without fetching the chunk.
    pub label_hash: u64,
    /// Size of the on-S3 object in bytes (pre-compactor) **or** of
    /// this chunk's slice inside a merged block (post-compactor).
    pub size_bytes: u32,
    /// **mvp/v5**: the S3 key of the merged-block object that
    /// physically holds this chunk. `None` ⇒ this chunk is its own
    /// S3 object at [`Self::key`] (pre-compactor layout).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_key: Option<String>,
    /// **mvp/v5**: byte offset of this chunk inside the merged-block
    /// object identified by [`Self::object_key`]. `None` ⇒ start of
    /// object (pre-compactor layout).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub byte_offset: Option<u64>,
    /// **mvp/v5**: byte length of this chunk's slice inside the
    /// merged-block object. `None` ⇒ whole object (pre-compactor
    /// layout).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub byte_length: Option<u64>,
}

// v7: hand-rolled `Deserialize` for [`IndexEntry`] that accepts
// BOTH the backend's historical field set AND the agent-side
// `gorillas3processor`'s shape (different field names for the
// same logical fields). See struct doc above for the mapping.
impl<'de> serde::Deserialize<'de> for IndexEntry {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        use serde::de::Error as _;

        // A permissive raw form. All fields are `Option<...>`; the
        // post-decode pass picks the right ones to populate the
        // canonical struct. `serde_json::Value` for `time_range`
        // because either an explicit `[s, e]` pair OR the
        // `start_ts_nano`/`end_ts_nano` siblings can supply it.
        #[derive(Deserialize)]
        struct Raw {
            // Backend canonical names.
            key: Option<String>,
            time_range: Option<(u64, u64)>,
            sample_count: Option<u32>,
            label_hash: Option<u64>,
            size_bytes: Option<u32>,
            object_key: Option<String>,
            byte_offset: Option<u64>,
            byte_length: Option<u64>,
            // Agent-side aliases.
            object: Option<String>,
            start_ts_nano: Option<u64>,
            end_ts_nano: Option<u64>,
            point_count: Option<u32>,
        }

        let raw = Raw::deserialize(d)?;
        let key = raw
            .key
            .or(raw.object)
            .ok_or_else(|| D::Error::missing_field("key"))?;
        let time_range = raw.time_range.or_else(|| {
            match (raw.start_ts_nano, raw.end_ts_nano) {
                (Some(s), Some(e)) => Some((s, e)),
                _ => None,
            }
        });
        let time_range = time_range
            .ok_or_else(|| D::Error::missing_field("time_range or start_ts_nano+end_ts_nano"))?;
        let sample_count = raw
            .sample_count
            .or(raw.point_count)
            .ok_or_else(|| D::Error::missing_field("sample_count"))?;
        // `label_hash` defaults to 0 for agent-produced entries
        // (single-series chunks; the agent's optional `label_hash`
        // field is also accepted by `Raw::label_hash`).
        let label_hash = raw.label_hash.unwrap_or(0);
        let size_bytes = raw.size_bytes.unwrap_or(0);
        Ok(IndexEntry {
            key,
            time_range,
            sample_count,
            label_hash,
            size_bytes,
            object_key: raw.object_key,
            byte_offset: raw.byte_offset,
            byte_length: raw.byte_length,
        })
    }
}

impl IndexEntry {
    /// S3 key the engine should issue a GET / Range-GET against to
    /// read this chunk's bytes. Falls back to [`Self::key`] if the
    /// chunk is its own object (pre-compactor layout).
    pub fn effective_object_key(&self) -> &str {
        self.object_key.as_deref().unwrap_or(&self.key)
    }

    /// `(byte_offset, byte_length)` window inside
    /// [`Self::effective_object_key`] that holds this chunk's bytes.
    /// Returns `None` if the entry has no offset/length pair AND no
    /// `size_bytes` info — in that case the caller should GET the
    /// full object. For pre-compactor entries (no `byte_offset`),
    /// returns `Some((0, size_bytes))` so the partial-read path can
    /// be unconditional.
    pub fn effective_byte_range(&self) -> Option<(u64, u64)> {
        match (self.byte_offset, self.byte_length) {
            (Some(off), Some(len)) => Some((off, len)),
            // Pre-compactor: chunk is its own object, range starts at
            // 0 with the recorded size_bytes.
            _ if self.size_bytes > 0 => Some((0, self.size_bytes as u64)),
            _ => None,
        }
    }
}

/// Per-hour-bucket catalog of [`IndexEntry`]s. Encoded as JSON for
/// human-debuggability (matches the Telegraf-side index.json contract).
///
/// ### v7 — agent-side schema compatibility
///
/// The agent-side `gorillas3processor` writes the top-level JSON as
/// `{"version": 1, "tenant": "...", "metric": "...", "entries": [...]}`
/// — `version` instead of `schema_version`, plus extra `tenant` /
/// `metric` siblings. The backend's `read` accepts either spelling
/// via the custom [`Deserialize`] below; `tenant` and `metric` are
/// captured so the engine can surface them in diagnostics but are
/// not load-bearing on the read path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IndexFile {
    /// On-wire schema version. Always [`INDEX_SCHEMA_VERSION`] for
    /// freshly built indexes; a [`DecodeError::UnsupportedIndexVersion`]
    /// is returned when reading any other value.
    pub schema_version: u8,
    /// Wall-clock nanoseconds-since-Unix-epoch at which the index
    /// file was written.
    pub generated_at_ns: u64,
    /// One row per chunk. The Phase 1 cold-engine writes them in
    /// time-ascending order; the prune iterators do not assume that.
    pub entries: Vec<IndexEntry>,
}

impl<'de> serde::Deserialize<'de> for IndexFile {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw {
            // Backend canonical names.
            schema_version: Option<u8>,
            generated_at_ns: Option<u64>,
            entries: Vec<IndexEntry>,
            // Agent-side aliases (kept here for forwards compat —
            // we deliberately ignore tenant / metric on read since
            // the engine already knows which (tenant, metric) it
            // requested, and we want IndexFile's wire shape to stay
            // small).
            #[serde(default)]
            version: Option<u8>,
            #[serde(default, rename = "tenant")]
            _tenant: Option<String>,
            #[serde(default, rename = "metric")]
            _metric: Option<String>,
        }

        let raw = Raw::deserialize(d)?;
        let schema_version = raw
            .schema_version
            .or(raw.version)
            .unwrap_or(INDEX_SCHEMA_VERSION);
        // Agent-side index.json doesn't carry a top-level
        // `generated_at_ns`. We default to the latest entry's
        // `written_at_unix_nano` if the JSON happens to carry it —
        // but since we ignore agent fields per-entry beyond what
        // the canonical IndexEntry needs, fall back to 0 here. The
        // backend's prune iterators do not consult this field.
        let generated_at_ns = raw.generated_at_ns.unwrap_or(0);
        Ok(IndexFile {
            schema_version,
            generated_at_ns,
            entries: raw.entries,
        })
    }
}

impl IndexFile {
    /// Build an empty index pinned to the current schema version.
    pub fn new(generated_at_ns: u64) -> Self {
        Self {
            schema_version: INDEX_SCHEMA_VERSION,
            generated_at_ns,
            entries: Vec::new(),
        }
    }

    /// Read a JSON-encoded index from `r`.
    pub fn read<R: Read>(mut r: R) -> Result<Self, DecodeError> {
        let mut buf = Vec::new();
        r.read_to_end(&mut buf)?;
        let me: Self = serde_json::from_slice(&buf)?;
        if me.schema_version != INDEX_SCHEMA_VERSION {
            return Err(DecodeError::UnsupportedIndexVersion(me.schema_version));
        }
        Ok(me)
    }

    /// Write a pretty-printed JSON encoding of the index to `w`.
    pub fn write<W: Write>(&self, mut w: W) -> Result<(), EncodeError> {
        let bytes = serde_json::to_vec_pretty(self)?;
        w.write_all(&bytes)?;
        Ok(())
    }

    /// Iterate the entries that overlap `range = (start_ns, end_ns)`.
    /// Both endpoints are inclusive. Two intervals overlap when
    /// `entry.start <= range.end && entry.end >= range.start`.
    pub fn prune_by_time(&self, range: (u64, u64)) -> impl Iterator<Item = &IndexEntry> {
        let (qs, qe) = range;
        self.entries
            .iter()
            .filter(move |e| e.time_range.0 <= qe && e.time_range.1 >= qs)
    }

    /// Iterate the entries whose [`IndexEntry::label_hash`] equals
    /// `hash`. Used by the backend to pre-filter chunks for an
    /// exact-equality matcher (e.g. `instance="i-12345"`).
    pub fn prune_by_label_hash(&self, hash: u64) -> impl Iterator<Item = &IndexEntry> {
        self.entries.iter().filter(move |e| e.label_hash == hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// v7: backend can read a JSON index produced by the agent's
    /// `gorillas3processor`. The agent uses `version` instead of
    /// `schema_version`, plus `tenant`/`metric` siblings; per-entry
    /// it uses `object` + `start_ts_nano`/`end_ts_nano` +
    /// `point_count` instead of `key` + `time_range` +
    /// `sample_count`.
    #[test]
    fn read_accepts_agent_side_index_json_shape_v7() {
        // Verbatim shape produced by the agent's
        // opentelemetry-collector-contrib-patch/processor/
        // gorillas3processor/s3_sink.go indexFile + indexEntry
        // structs.
        let agent_json = r#"{
            "version": 1,
            "tenant": "default",
            "metric": "http_freshness_probe_archive",
            "entries": [
                {
                    "object": "part-1778127177-000000.gor",
                    "start_ts_nano": 1778127132419130620,
                    "end_ts_nano": 1778127177986083641,
                    "series_count": 1,
                    "point_count": 20,
                    "size_bytes": 391,
                    "written_at_unix_nano": 1778127178654862770
                },
                {
                    "object": "part-1778127237-000000.gor",
                    "start_ts_nano": 1778127192629541252,
                    "end_ts_nano": 1778127237947382534,
                    "series_count": 1,
                    "point_count": 20,
                    "size_bytes": 389,
                    "written_at_unix_nano": 1778127238725051510
                }
            ]
        }"#;
        let parsed = IndexFile::read(agent_json.as_bytes())
            .expect("must accept agent-side wire shape");
        assert_eq!(parsed.schema_version, INDEX_SCHEMA_VERSION);
        assert_eq!(parsed.entries.len(), 2);
        assert_eq!(parsed.entries[0].key, "part-1778127177-000000.gor");
        assert_eq!(
            parsed.entries[0].time_range,
            (1778127132419130620, 1778127177986083641)
        );
        assert_eq!(parsed.entries[0].sample_count, 20);
        assert_eq!(parsed.entries[0].size_bytes, 391);
        assert_eq!(parsed.entries[0].label_hash, 0);
        assert_eq!(parsed.entries[1].key, "part-1778127237-000000.gor");
    }

    /// Backend's canonical wire shape still roundtrips bit-for-bit
    /// with the v7 custom Deserialize.
    #[test]
    fn read_accepts_backend_side_index_json_shape() {
        let backend_json = r#"{
            "schema_version": 1,
            "generated_at_ns": 1700000000000000000,
            "entries": [
                {
                    "key": "part-A.gor",
                    "time_range": [1700000000000000000, 1700000060000000000],
                    "sample_count": 60,
                    "label_hash": 12345,
                    "size_bytes": 500
                }
            ]
        }"#;
        let parsed = IndexFile::read(backend_json.as_bytes())
            .expect("must accept backend wire shape");
        assert_eq!(parsed.schema_version, INDEX_SCHEMA_VERSION);
        assert_eq!(parsed.generated_at_ns, 1700000000000000000);
        assert_eq!(parsed.entries[0].key, "part-A.gor");
        assert_eq!(parsed.entries[0].sample_count, 60);
        assert_eq!(parsed.entries[0].label_hash, 12345);
    }

    /// Mixing fields — the parser prefers backend canonical names
    /// when both are present.
    #[test]
    fn agent_canonical_preference_when_both_set() {
        let json = r#"{
            "schema_version": 1,
            "generated_at_ns": 0,
            "entries": [
                {
                    "key": "canonical.gor",
                    "object": "agent-name.gor",
                    "time_range": [10, 20],
                    "start_ts_nano": 99,
                    "end_ts_nano": 100,
                    "sample_count": 5,
                    "point_count": 999,
                    "label_hash": 7,
                    "size_bytes": 0
                }
            ]
        }"#;
        let parsed = IndexFile::read(json.as_bytes()).expect("parse");
        assert_eq!(parsed.entries[0].key, "canonical.gor");
        assert_eq!(parsed.entries[0].time_range, (10, 20));
        assert_eq!(parsed.entries[0].sample_count, 5);
    }
}
