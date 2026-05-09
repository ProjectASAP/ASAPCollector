//! Integration tests for the postings module — round-trip + merge +
//! determinism. The unit tests inside `src/postings.rs` cover the
//! basic API; this file pins the contracts that downstream callers
//! (the compactor, the backend GorillaQueryEngine) rely on.

use asap_gorilla::{Postings, PostingsBuilder};

fn build_block_postings(prefix: &str, n_series: u64) -> Postings {
    let mut b = PostingsBuilder::new();
    for i in 0..n_series {
        let zone = if i % 2 == 0 { "a" } else { "b" };
        let role = if i % 3 == 0 { "leader" } else { "follower" };
        b.add_series(
            i,
            vec![
                ("service", prefix),
                ("zone", zone),
                ("role", role),
            ],
        );
    }
    b.finalize()
}

#[test]
fn round_trip_realistic_postings() {
    let p = build_block_postings("api", 1000);
    let mut buf = Vec::new();
    p.write(&mut buf).expect("encode");
    let parsed = Postings::read(buf.as_slice()).expect("decode");
    assert_eq!(parsed, p);
    assert_eq!(parsed.lookup("service", "api").len(), 1000);
    assert_eq!(parsed.lookup("zone", "a").len(), 500);
    assert_eq!(parsed.lookup("role", "leader").len(), 334);
}

#[test]
fn merge_six_block_postings_is_set_union() {
    // Six "hour" postings, each covering its own series-id range.
    // Compactor merges these into one "day" postings; we assert the
    // result is exactly the set union per (label_name, label_value).
    let mut sources = Vec::new();
    for h in 0u64..6 {
        let mut b = PostingsBuilder::new();
        for sid in (h * 1000)..((h + 1) * 1000) {
            b.add_series(sid, vec![("service", "api"), ("zone", "a")]);
        }
        sources.push(b.finalize());
    }
    let merged = Postings::merge_many(&sources);

    // Set-union: 6 × 1000 distinct series_ids, all under (service=api).
    assert_eq!(merged.lookup("service", "api").len(), 6000);
    assert_eq!(merged.lookup("zone", "a").len(), 6000);
    let api_ids = merged.lookup("service", "api");
    // Sorted ascending, no duplicates.
    for w in api_ids.windows(2) {
        assert!(w[0] < w[1], "merged postings must be sorted strictly ascending");
    }
}

#[test]
fn merge_idempotent_when_inputs_overlap() {
    // Three sources with overlapping series_ids — the union shouldn't
    // multiply duplicates.
    let mut a = PostingsBuilder::new();
    a.add_series(1, vec![("zone", "a")]);
    a.add_series(2, vec![("zone", "a")]);
    a.add_series(3, vec![("zone", "a")]);

    let mut b = PostingsBuilder::new();
    b.add_series(2, vec![("zone", "a")]); // duplicate of `a`
    b.add_series(3, vec![("zone", "a")]); // duplicate of `a`
    b.add_series(4, vec![("zone", "a")]);

    let mut c = PostingsBuilder::new();
    c.add_series(5, vec![("zone", "a")]);

    let merged =
        Postings::merge_many(&[a.finalize(), b.finalize(), c.finalize()]);
    assert_eq!(merged.lookup("zone", "a"), &[1, 2, 3, 4, 5]);
}

#[test]
fn determinism_same_inputs_same_bytes() {
    // Two postings built from the same triples in different orders
    // must serialize to identical bytes.
    let a = build_block_postings("svc", 200);
    let b = build_block_postings("svc", 200);

    let mut ba = Vec::new();
    let mut bb = Vec::new();
    a.write(&mut ba).unwrap();
    b.write(&mut bb).unwrap();
    assert_eq!(ba, bb, "deterministic encoding");
}

#[test]
fn merged_postings_serialize_round_trip() {
    // Compactor's final write step: merge then encode then decode.
    let s1 = build_block_postings("api", 100);
    let s2 = build_block_postings("web", 50);
    let merged = Postings::merge_many(&[s1, s2]);

    let mut buf = Vec::new();
    merged.write(&mut buf).unwrap();
    let parsed = Postings::read(buf.as_slice()).unwrap();
    assert_eq!(parsed, merged);
}
