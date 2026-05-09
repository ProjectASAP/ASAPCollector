//! Round-trip tests: encode samples, decode them, assert sample-equal.
//!
//! These cover the contract `decode(encode(samples)) == samples` for
//! the bit patterns that exercise every branch of the Gorilla encoder
//! state machine.

use std::time::Instant;

use asap_gorilla::{GorillaDecoder, GorillaEncoder};

fn round_trip(samples: &[(u64, f64)]) -> Vec<(u64, f64)> {
    let mut enc = GorillaEncoder::new(
        "test_metric",
        vec![("instance".to_string(), "i-rt".to_string())],
    );
    for (t, v) in samples {
        enc.append(*t, *v);
    }
    let bytes = enc.finalize().expect("encode");
    let dec = GorillaDecoder::from_reader(&bytes[..]).expect("decoder header");
    let h = dec.header().expect("series header present");
    assert_eq!(h.metric, "test_metric");
    assert_eq!(h.sample_count as usize, samples.len());
    dec.samples()
        .collect::<Result<Vec<_>, _>>()
        .expect("decode samples")
}

fn assert_bit_eq(want: &[(u64, f64)], got: &[(u64, f64)]) {
    assert_eq!(got.len(), want.len(), "sample count mismatch");
    for (i, (w, g)) in want.iter().zip(got.iter()).enumerate() {
        assert_eq!(g.0, w.0, "ts mismatch at index {i}");
        assert_eq!(
            g.1.to_bits(),
            w.1.to_bits(),
            "value bits mismatch at index {i}: want={:?} got={:?}",
            w.1,
            g.1
        );
    }
}

#[test]
fn roundtrip_single_sample() {
    let samples = vec![(1_700_000_000_000_000_000, 1.5)];
    let got = round_trip(&samples);
    assert_bit_eq(&samples, &got);
}

#[test]
fn roundtrip_100_samples() {
    let mut samples = Vec::with_capacity(100);
    let base = 1_700_000_000_000_000_000u64;
    for i in 0..100u64 {
        samples.push((base + i * 1_000_000_000, i as f64 * 0.25));
    }
    let got = round_trip(&samples);
    assert_bit_eq(&samples, &got);
}

#[test]
fn roundtrip_5000_samples_realistic() {
    // Mimic a 5,000-point gauge sampled at 1-second intervals with
    // small drift — this is what a real 1h cold-store chunk looks
    // like for a Prometheus-style 1s scrape interval × 1.4h coverage.
    let mut samples = Vec::with_capacity(5000);
    let base = 1_700_000_000_000_000_000u64;
    let mut v = 0.5_f64;
    for i in 0..5000u64 {
        // 1s ± 5ms jitter, deterministic.
        let jitter = ((i.wrapping_mul(2654435761)) % 11) as i64 - 5;
        let ts = base
            .wrapping_add(i.wrapping_mul(1_000_000_000))
            .wrapping_add((jitter * 1_000_000) as u64);
        // A drifty sine-like walk that won't hit every-sample-equal.
        v += ((i as f64) * 0.0001).sin() * 0.01;
        samples.push((ts, v));
    }
    let got = round_trip(&samples);
    assert_bit_eq(&samples, &got);
}

#[test]
fn roundtrip_with_repeated_values() {
    // Pure constant: Gorilla should emit one bit per sample for
    // values, two bits for the timestamp delta-of-delta=0 path.
    let mut samples = Vec::new();
    let base = 1_700_000_000_000_000_000u64;
    for i in 0..1024u64 {
        samples.push((base + i * 1_000_000_000, 42.0));
    }
    let got = round_trip(&samples);
    assert_bit_eq(&samples, &got);

    // Sanity: encoded body should be much smaller than 16B/sample.
    let mut enc = GorillaEncoder::new("constant", Vec::new());
    for (t, v) in &samples {
        enc.append(*t, *v);
    }
    let bytes = enc.finalize().unwrap();
    assert!(
        bytes.len() < samples.len() * 16 / 4,
        "expected significant compression on constant-value series, got {} bytes for {} samples",
        bytes.len(),
        samples.len()
    );
}

#[test]
fn roundtrip_with_irregular_intervals() {
    // Hand-picked intervals that exercise every dd-bucket: 0, ±63
    // (fits in 7), ±255 (fits in 9), ±2047 (fits in 12), and the
    // 64-bit fallback.
    let base = 1_700_000_000_000_000_000u64;
    let intervals_ns: &[i64] = &[
        1_000_000_000,
        1_000_000_000, // dd = 0
        1_000_000_063, // dd = +63 -> 7-bit
        999_999_953,   // dd back to small
        1_000_000_255, // dd = +302 -> 9-bit window
        999_998_000,   // dd swing -> larger
        1_000_002_047, // dd ~ 4047 -> 12-bit window
        1_999_999_000, // huge swing -> 64-bit
        1_000_000_000,
        1_000_000_000,
    ];
    let mut samples = Vec::new();
    let mut t = base;
    for (i, dt) in intervals_ns.iter().enumerate() {
        t = t.wrapping_add(*dt as u64);
        samples.push((t, (i as f64).sin()));
    }
    let got = round_trip(&samples);
    assert_bit_eq(&samples, &got);
}

#[test]
fn roundtrip_extreme_values() {
    // NaN, +Inf, -Inf, denormals, +/- zero, max finite, min positive.
    let base = 1_700_000_000_000_000_000u64;
    let weird: &[f64] = &[
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        0.0,
        -0.0,
        f64::MIN_POSITIVE,
        -f64::MIN_POSITIVE,
        f64::MAX,
        -f64::MAX,
        1e-300,
        1e300,
        f64::EPSILON,
    ];
    let samples: Vec<(u64, f64)> = weird
        .iter()
        .enumerate()
        .map(|(i, v)| (base + (i as u64) * 1_000_000_000, *v))
        .collect();
    let got = round_trip(&samples);
    // Bit-exact equality (NaN compares unequal under `==` but the
    // `to_bits()` round-trip is the contract that matters here).
    assert_bit_eq(&samples, &got);
}

#[test]
fn roundtrip_unsorted_input_is_sorted_internally() {
    // The Go side `sort.Slice`s before encoding. We mirror that, so
    // out-of-order input still round-trips, but the decoded order is
    // ts-ascending.
    let base = 1_700_000_000_000_000_000u64;
    let unsorted = vec![
        (base + 3_000_000_000, 3.0),
        (base + 1_000_000_000, 1.0),
        (base + 2_000_000_000, 2.0),
    ];
    let got = round_trip(&unsorted);
    let mut want = unsorted.clone();
    want.sort_by_key(|p| p.0);
    assert_bit_eq(&want, &got);
}

#[test]
fn smoke_bench_encode_decode_rates() {
    // Cheap smoke bench — just prints encode/decode rates so the CI
    // log records a ballpark. Asserts a generous lower bound to catch
    // catastrophic regressions.
    let n = 100_000usize;
    let mut samples = Vec::with_capacity(n);
    let base = 1_700_000_000_000_000_000u64;
    let mut v = 0.0f64;
    for i in 0..n as u64 {
        v += 0.001;
        samples.push((base + i * 1_000_000_000, v));
    }

    let enc_start = Instant::now();
    let mut enc = GorillaEncoder::new("bench", Vec::new());
    for (t, x) in &samples {
        enc.append(*t, *x);
    }
    let bytes = enc.finalize().unwrap();
    let enc_elapsed = enc_start.elapsed();
    let enc_rate = n as f64 / enc_elapsed.as_secs_f64();

    let dec_start = Instant::now();
    let dec = GorillaDecoder::from_reader(&bytes[..]).unwrap();
    let count = dec.samples().count();
    let dec_elapsed = dec_start.elapsed();
    let dec_rate = count as f64 / dec_elapsed.as_secs_f64();

    eprintln!(
        "asap-gorilla bench: n={n}, encoded={} bytes ({:.2} bytes/sample), \
         encode={:.2}M pts/s, decode={:.2}M pts/s",
        bytes.len(),
        bytes.len() as f64 / n as f64,
        enc_rate / 1e6,
        dec_rate / 1e6,
    );

    assert!(enc_rate > 1e6, "encode rate too low: {enc_rate:.0}");
    assert!(dec_rate > 1e6, "decode rate too low: {dec_rate:.0}");
}
