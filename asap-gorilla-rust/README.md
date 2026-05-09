# asap-gorilla-rust

`asap-gorilla-rust` is the canonical Rust implementation of the
`GORILLA1` block format. Its Go sibling is
[`asap-gorilla-go`](../asap-gorilla-go/), which runtime edge collectors
and processors use directly instead of carrying their own Gorilla encoder
copies. Bytes written by either side are interchangeable — that is the
byte-parity contract this crate is built around.

The Cargo package name remains `asap-gorilla` for existing downstream
path dependencies; the repository path is `asap-gorilla-rust`.

This is **Phase 1** of the Gorilla-S3-cold-engine: the encoder, the
decoder, and the per-hour `index.json` catalog. Subsequent phases wire
the crate into `ASAPQuery-backend`'s `GorillaQueryEngine` and the
controller's cold-store-aware planner.

## Block format (`GORILLA1`)

All multi-byte integers are little-endian.

| offset | size | field          | description                            |
|-------:|-----:|----------------|----------------------------------------|
| 0      | 8    | magic          | ASCII `"GORILLA1"`                     |
| 8      | 1    | version        | block-format version (`1`)             |
| 9      | 4    | series_count   | number of per-series chunks            |
| 13     | …    | chunks         | back-to-back encoded series chunks     |

Each per-series chunk:

| size      | field          | description                                            |
|----------:|----------------|--------------------------------------------------------|
| 2         | meta_len       | length of the JSON metadata blob (`u16`)               |
| meta_len  | meta_json      | `SeriesMeta` JSON                                      |
| 4         | point_count    | number of `(ts, value)` samples (`u32`)                |
| 8         | first_ts       | first timestamp, raw `int64` cast to `u64`             |
| 8         | first_val_bits | `f64::to_bits` of the first value                      |
| 4         | ts_bits_len    | length **in bits** of the timestamp body (`u32`)       |
| …         | ts_bits        | byte-padded delta-of-delta timestamp bit stream        |
| 4         | val_bits_len   | length **in bits** of the value body (`u32`)           |
| …         | val_bits       | byte-padded XOR-encoded value bit stream               |

`SeriesMeta` is JSON with the field set Go's `gorillaprocessor.seriesMeta`
emits:

```json
{
  "metric_name": "node_cpu_seconds_total",
  "attributes": {"instance": "i-1", "mode": "user"},
  "start_ts": 1700000000000000000,
  "end_ts":   1700000060000000000,
  "point_count": 60
}
```

The `attributes` map is serialized with sorted keys on the Rust side
(see "Byte-parity caveat" below).

## Index file (`index.json`)

The per-hour bucket carries a sibling `index.json` listing every chunk
in that hour. Layout:

```json
{
  "schema_version": 1,
  "generated_at_ns": 1700000003600000000,
  "entries": [
    {
      "key": "tenant/metric/2026/05/06/00/part-000000.gor",
      "time_range": [1700000000000000000, 1700000060000000000],
      "sample_count": 60,
      "label_hash": 16045690984833335023,
      "size_bytes": 1024
    }
  ]
}
```

`prune_by_time` and `prune_by_label_hash` give the backend cheap
chunk-level pruning before it issues range reads.

## Quick start

```rust
use asap_gorilla::{GorillaDecoder, GorillaEncoder};

let mut enc = GorillaEncoder::new(
    "node_cpu_seconds_total",
    vec![
        ("instance".to_string(), "i-1".to_string()),
        ("mode".to_string(), "user".to_string()),
    ],
);
enc.append(1_700_000_000_000_000_000, 0.5);
enc.append(1_700_000_001_000_000_000, 0.6);
let bytes = enc.finalize().unwrap();

let dec = GorillaDecoder::from_reader(&bytes[..]).unwrap();
let header = dec.header().unwrap();
let samples: Vec<_> = dec.samples().collect::<Result<Vec<_>, _>>().unwrap();
```

## Byte-parity caveat

The Go implementation JSON-encodes its label set from a Go
`map[string]string`, whose iteration order is randomized. That makes its
`meta_json` blob (and therefore its block bytes) non-deterministic even
for identical inputs. `asap-gorilla-rust` deliberately strengthens the
contract by serializing labels through a `BTreeMap`, so its output is
deterministic and byte-stable.

Consequence: byte-identical round-trip against a Go-produced fixture
requires either (a) the Go fixturegen sorts keys before marshaling, or
(b) we compare via the parsed `SeriesMeta` struct rather than the raw
bytes. The cross-language tests under `tests/byte_compat.rs` activate
once the Go fixturegen lands (see `tests/golden/regen.sh`).

## Testing

```sh
cargo test --release -p asap-gorilla
cargo clippy --release -p asap-gorilla --all-targets -- -D warnings
cargo doc --no-deps -p asap-gorilla
```

`tests/round_trip.rs` covers single-sample, 100-sample, 5000-sample
realistic, repeated-value, irregular-interval, and extreme-value
(NaN/Inf/denormal) round-trips, plus a smoke bench printing
encode/decode rates. `tests/byte_compat.rs` covers self-byte-round-trip
and (when the fixture is present) cross-language byte-parity.
`tests/index_file.rs` covers the index JSON round-trip plus
prune-by-time and prune-by-label-hash. `tests/integration.rs` ties
encoder + index + decoder together.
