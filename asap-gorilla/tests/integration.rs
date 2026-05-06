//! End-to-end test that exercises encoder + decoder + index file
//! together — i.e. the same flow the cold-engine ingest path will
//! hit when it writes a chunk and updates the per-hour index.

use asap_gorilla::{GorillaDecoder, GorillaEncoder, IndexEntry, IndexFile};

fn label_hash(labels: &[(String, String)]) -> u64 {
    // The real cold-engine will plug in a stable hasher (e.g.
    // xxhash). For this test we only need a deterministic stand-in.
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    for (k, v) in labels {
        k.hash(&mut h);
        v.hash(&mut h);
    }
    h.finish()
}

#[test]
fn encode_then_index_then_decode() {
    let labels = vec![("instance".to_string(), "i-int".to_string())];
    let samples: Vec<(u64, f64)> = (0..256u64)
        .map(|i| (1_700_000_000_000_000_000 + i * 1_000_000_000, i as f64))
        .collect();

    // 1. Encode the chunk.
    let mut enc = GorillaEncoder::new("integ_metric", labels.clone());
    for (t, v) in &samples {
        enc.append(*t, *v);
    }
    let chunk_bytes = enc.finalize().expect("encode");

    // 2. Catalog it inside an IndexFile.
    let mut idx = IndexFile::new(samples.last().unwrap().0);
    idx.entries.push(IndexEntry {
        key: "tenant/integ_metric/2026/05/06/00/part-000000.gor".to_string(),
        time_range: (samples.first().unwrap().0, samples.last().unwrap().0),
        sample_count: samples.len() as u32,
        label_hash: label_hash(&labels),
        size_bytes: chunk_bytes.len() as u32,
    });

    // 3. Round-trip the index.
    let mut idx_bytes = Vec::new();
    idx.write(&mut idx_bytes).unwrap();
    let parsed = IndexFile::read(&idx_bytes[..]).unwrap();
    assert_eq!(parsed, idx);

    // 4. Use the index to "find" the chunk and decode it.
    let want_range = (
        samples.first().unwrap().0 + 50_000_000_000,
        samples.first().unwrap().0 + 100_000_000_000,
    );
    let hits: Vec<_> = parsed.prune_by_time(want_range).collect();
    assert_eq!(hits.len(), 1, "expected the chunk to overlap the query window");
    assert_eq!(hits[0].sample_count, samples.len() as u32);

    // 5. Decode the chunk and assert sample fidelity.
    let dec = GorillaDecoder::from_reader(&chunk_bytes[..]).unwrap();
    let h = dec.header().unwrap();
    assert_eq!(h.metric, "integ_metric");
    assert_eq!(h.sample_count as usize, samples.len());
    assert_eq!(
        h.time_range,
        (samples.first().unwrap().0, samples.last().unwrap().0)
    );
    let decoded: Vec<_> = dec.samples().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(decoded, samples);
}

#[test]
fn multi_series_block_round_trip() {
    // Phase 1 default writes single-series blocks, but the format
    // permits multi-series objects (the Go side packs many per S3
    // upload). Verify both encoder and streaming decoder handle it.
    let mut enc = GorillaEncoder::new(
        "metric_a",
        vec![("kind".to_string(), "first".to_string())],
    );
    for i in 0..32u64 {
        enc.append(1_700_000_000_000_000_000 + i * 1_000_000_000, i as f64);
    }
    let mut enc = enc.with_series(
        "metric_b",
        vec![("kind".to_string(), "second".to_string())],
    );
    for i in 0..16u64 {
        enc.append(1_700_000_500_000_000_000 + i * 2_000_000_000, (i as f64) * 0.5);
    }
    let bytes = enc.finalize().unwrap();

    let mut dec = GorillaDecoder::from_reader(&bytes[..]).unwrap();
    let h = dec.header().unwrap();
    assert_eq!(h.metric, "metric_a");
    assert_eq!(h.sample_count, 32);
    let s1: Vec<_> = dec.samples().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(s1.len(), 32);

    assert!(dec.next_series().unwrap());
    let h = dec.header().unwrap();
    assert_eq!(h.metric, "metric_b");
    assert_eq!(h.sample_count, 16);
    let s2: Vec<_> = dec.samples().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(s2.len(), 16);

    assert!(!dec.next_series().unwrap());
}

#[test]
fn header_only_scan_via_headers_helper() {
    let mut enc = GorillaEncoder::new("a", vec![]);
    enc.append(1_000, 1.0);
    enc.append(2_000, 2.0);
    let mut enc = enc.with_series("b", vec![]);
    enc.append(3_000, 3.0);
    let bytes = enc.finalize().unwrap();

    let headers = GorillaDecoder::headers(&bytes[..]).unwrap();
    assert_eq!(headers.len(), 2);
    assert_eq!(headers[0].metric, "a");
    assert_eq!(headers[0].sample_count, 2);
    assert_eq!(headers[1].metric, "b");
    assert_eq!(headers[1].sample_count, 1);
}
