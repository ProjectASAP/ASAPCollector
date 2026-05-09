//! Tests for the per-hour `index.json` catalog.

use std::io::Cursor;

use asap_gorilla::{IndexEntry, IndexFile};

fn entry(key: &str, start: u64, end: u64, label_hash: u64) -> IndexEntry {
    IndexEntry {
        key: key.to_string(),
        time_range: (start, end),
        sample_count: ((end - start) / 1_000_000_000) as u32,
        label_hash,
        size_bytes: 1024,
        // Pre-compactor entries leave the new fields unset; the
        // Option<…>'s `None` round-trips as a missing JSON key.
        object_key: None,
        byte_offset: None,
        byte_length: None,
    }
}

#[test]
fn index_write_read_roundtrip() {
    let mut idx = IndexFile::new(1_700_000_000_000_000_000);
    idx.entries.push(entry(
        "tenant/metric/2026/05/06/00/part-000000.gor",
        1_700_000_000_000_000_000,
        1_700_000_060_000_000_000,
        0xdead_beef_cafe_babe,
    ));
    idx.entries.push(entry(
        "tenant/metric/2026/05/06/00/part-000001.gor",
        1_700_000_060_000_000_000,
        1_700_000_120_000_000_000,
        0x1234_5678_9abc_def0,
    ));

    let mut buf = Vec::new();
    idx.write(&mut buf).expect("write");
    let cur = Cursor::new(&buf);
    let parsed = IndexFile::read(cur).expect("read");
    assert_eq!(parsed, idx);
}

#[test]
fn index_prune_by_time_range() {
    let mut idx = IndexFile::new(0);
    idx.entries.push(entry("a", 100, 200, 0));
    idx.entries.push(entry("b", 300, 400, 0));
    idx.entries.push(entry("c", 500, 600, 0));
    idx.entries.push(entry("d", 250, 350, 0));

    // Query covering 320..380 — should hit "b" and "d" only.
    let hits: Vec<_> = idx.prune_by_time((320, 380)).map(|e| e.key.as_str()).collect();
    assert_eq!(hits, vec!["b", "d"]);

    // Query touching only the first entry's tail.
    let hits: Vec<_> = idx.prune_by_time((150, 175)).map(|e| e.key.as_str()).collect();
    assert_eq!(hits, vec!["a"]);

    // Query past everything.
    let hit_count = idx.prune_by_time((10_000, 20_000)).count();
    assert_eq!(hit_count, 0);

    // Query covering everything.
    let hits: Vec<_> = idx.prune_by_time((0, u64::MAX)).map(|e| e.key.as_str()).collect();
    assert_eq!(hits, vec!["a", "b", "c", "d"]);
}

#[test]
fn index_prune_by_label_hash() {
    let mut idx = IndexFile::new(0);
    idx.entries.push(entry("a", 100, 200, 0xaaaa));
    idx.entries.push(entry("b", 300, 400, 0xbbbb));
    idx.entries.push(entry("c", 500, 600, 0xaaaa));

    let hits: Vec<_> = idx
        .prune_by_label_hash(0xaaaa)
        .map(|e| e.key.as_str())
        .collect();
    assert_eq!(hits, vec!["a", "c"]);

    let hits = idx.prune_by_label_hash(0xcccc).count();
    assert_eq!(hits, 0);
}

#[test]
fn entry_with_compactor_extension_round_trips() {
    // Compactor-merged-block layout: object_key + byte_offset +
    // byte_length all set; `key` keeps the legacy chunk identifier.
    let e = IndexEntry {
        key: "chunk-A".to_string(),
        time_range: (1_700_000_000_000_000_000, 1_700_000_060_000_000_000),
        sample_count: 60,
        label_hash: 0xfeed_face_dead_beef,
        size_bytes: 512,
        object_key: Some("day=2026-05-06/block-0000-0006.gor".to_string()),
        byte_offset: Some(0),
        byte_length: Some(512),
    };
    let mut idx = IndexFile::new(0);
    idx.entries.push(e.clone());
    let mut buf = Vec::new();
    idx.write(&mut buf).unwrap();
    let parsed = IndexFile::read(buf.as_slice()).unwrap();
    assert_eq!(parsed.entries[0], e);

    // Helper accessors return the merged-object key + range.
    assert_eq!(parsed.entries[0].effective_object_key(), "day=2026-05-06/block-0000-0006.gor");
    assert_eq!(parsed.entries[0].effective_byte_range(), Some((0, 512)));
}

#[test]
fn legacy_entry_falls_back_to_self_key_and_size() {
    // Pre-compactor layout: `object_key`/`byte_offset`/`byte_length`
    // all `None`. The helpers must fall back to the chunk's own
    // `key` + `(0, size_bytes)` so legacy fixtures still resolve.
    let e = entry("legacy-chunk", 100_000_000, 200_000_000, 0x1234);
    let mut idx = IndexFile::new(0);
    idx.entries.push(e.clone());
    let mut buf = Vec::new();
    idx.write(&mut buf).unwrap();
    let parsed = IndexFile::read(buf.as_slice()).unwrap();
    assert_eq!(parsed.entries[0], e);
    assert_eq!(parsed.entries[0].effective_object_key(), "legacy-chunk");
    // size_bytes==1024 → range is (0, 1024).
    assert_eq!(parsed.entries[0].effective_byte_range(), Some((0, 1024)));
}

#[test]
fn legacy_json_without_new_fields_decodes() {
    // A pre-mvp/v5 fixture that doesn't even include the new keys
    // must still parse — proves the additive change doesn't break
    // already-written index.json blobs.
    let legacy = serde_json::json!({
        "schema_version": 1,
        "generated_at_ns": 0,
        "entries": [{
            "key": "tenant/m/2026/05/06/12/part-0.gor",
            "time_range": [100_000_000_000_u64, 200_000_000_000_u64],
            "sample_count": 60,
            "label_hash": 12345,
            "size_bytes": 4096
        }]
    });
    let bytes = serde_json::to_vec(&legacy).unwrap();
    let parsed = IndexFile::read(&bytes[..]).unwrap();
    assert_eq!(parsed.entries.len(), 1);
    assert!(parsed.entries[0].object_key.is_none());
    assert!(parsed.entries[0].byte_offset.is_none());
    assert!(parsed.entries[0].byte_length.is_none());
    assert_eq!(parsed.entries[0].effective_object_key(),
        "tenant/m/2026/05/06/12/part-0.gor");
    assert_eq!(parsed.entries[0].effective_byte_range(), Some((0, 4096)));
}

#[test]
fn index_rejects_unsupported_schema_version() {
    let bogus = serde_json::json!({
        "schema_version": 99,
        "generated_at_ns": 0,
        "entries": []
    });
    let bytes = serde_json::to_vec(&bogus).unwrap();
    let err = IndexFile::read(&bytes[..]).unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("99"), "unexpected error message: {msg}");
}
