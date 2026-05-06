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
