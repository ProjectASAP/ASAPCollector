# asap-gorilla-go

Go library for ASAP's lossless archive (cold-tier) metric encoding. It is
imported by both sides of the wire: the edge agent / runtime processors
(`asapedgeprocessor`, the gateway finalizer) produce the formats here, and the
backend `gorilla-merger` consumes them. There is no single "GORILLA1" object
format — the package is a set of focused codecs and block builders described
below.

Module path: `github.com/ProjectASAP/asap-gorilla-go`. Three packages:

| Package | Role |
|---|---|
| `gorilla` (root) | XOR-chunk fragments (edge transport), the streaming Prometheus-TSDB block builder, and the fragment→TSDB-block finalizer. |
| `intchunk` | Best-of-N, all-lossless raw value chunk codec (the cold-tier value codec). |
| `coldpart` | `ASAPCC1` decode-on-read cold "Part" container built on `intchunk`. |

---

## `intchunk` — best-of-N lossless value chunk codec

A standalone codec that encodes one series' `(timestamp int64, value float64)`
samples for a block. Several lossless codecs compete per block; the encoder
emits whichever produces the fewest bytes. All candidates are bit-exact
lossless.

The five value codecs (`CodecTag`):

| Tag | Codec | Notes |
|---|---|---|
| 0 | `GORILLA_XOR` | Lossless `float64` via `prometheus/tsdb/chunkenc` XOR (XOR == Gorilla). Always valid; the fallback for true high-precision floats. |
| 1 | `INT_FOR_DELTA` | `float→int64` via a decimal scale exponent, frame-of-reference (subtract base), first-order delta, single fixed-width bit-pack. For gauges. |
| 2 | `INT_FOR_DOD` | Same scale+FOR but delta-of-delta, fixed width. For monotonic counters. |
| 3 | `INT_FOR_DELTA_VARINT` | FOR+delta with each residual as a zigzag varint. Wins on skewed residual distributions where a single fixed width over-pays. |
| 4 | `INT_FOR_DOD_VARINT` | FOR delta-of-delta with zigzag-varint residuals. |

Key properties:

- **Timestamps** are always encoded as `t0 + delta-of-delta` zigzag varints,
  independent of the value codec, so a chunk is self-describing. (Overflow-safe
  at int64 extremes: the wraparound subtraction on encode is exactly inverted by
  the wraparound addition on decode.)
- **Decimal-exactness guard** (`tryScaleToInt64`): an `INT_*` candidate is only
  ever produced when `float→int64→float` round-trips BIT-EXACTLY at the chosen
  decimal scale (probing exponents 0..15). True high-precision floats fall back
  to `GORILLA_XOR`. NaN/Inf always fall back. This avoids the `lib/decimal`
  precision trap that silently introduces ~1e-12 error.
- **Overflow chunk-cut**: when a residual would exceed `maxResidualWidth` (56
  bits), the encoder ends the chunk and re-bases on a fresh frame, so `Encode`
  may return more than one chunk for one block.

Public surface:

```go
res, err := intchunk.Encode(samples)        // EncodeResult{Tag, Chunks [][]byte, Bytes}
samples, err := intchunk.DecodeChunk(chunk)  // decode one self-contained chunk
samples, err := intchunk.DecodeChunks(chunks)// decode a concatenation of chunks
tag, err := intchunk.PeekTag(chunk)          // codec tag without full decode
```

`intchunk.Sample{T int64, V float64}` is the sample type.

---

## `coldpart` — `ASAPCC1` decode-on-read cold Part

A `Part` bundles many series, each stored as one or more `intchunk` value
chunks, behind a label index + symbol table so a reader can answer
`Series(matchers, mint, maxt)` WITHOUT decoding any chunk body — chunk bodies
are decoded lazily only for the series that match.

On-disk layout (`ASAPCC1`, little-endian unless noted uvarint):

```text
[part header]  magic "ASAPCC1" | u8 version | i64 block_start_ms |
               i64 block_end_ms | uvarint series_count
[chunks]       per-series intchunk value chunks, concatenated
[index]        per series (sorted by labels): label refs into the symbol table,
               u64 chunk_off, u32 chunk_len, uvarint chunk_count,
               per-chunk byte lengths, i64 min_ts, i64 max_ts
[symbol table] deduped label strings; the index references them by ordinal
[footer]       u64 index_off | u64 index_len | u64 symtab_off | u32 crc32c
```

The `crc32c` (Castagnoli) covers every byte before the crc field and is verified
by `OpenPart`, so a corrupt object is rejected before any chunk is decoded.

Public surface:

```go
err := coldpart.WritePart(w, blockStartMs, blockEndMs, series, coldpart.Options{})
part, err := coldpart.OpenPart(b)            // validate header/version/crc + parse index; no chunk decode
n := part.NumSeries()                        // indexed series count (no decode)
lbls := part.SeriesLabels()                  // label sets of every series (no chunk decode)
hits, err := part.Series(matchers, mintMs, maxtMs) // decode-on-read; []SeriesData
ok := coldpart.MatchesAll(ls, matchers)      // Prometheus-matcher AND semantics
```

`Series()` filters by inclusive `[min_ts,max_ts]` time-window overlap AND applies
every `*labels.Matcher` with AND semantics, so `=`, `!=`, `=~`, `!~` behave
exactly as Prometheus selectors do.

---

## `gorilla` (root) — fragments, TSDB block builders

### Fragments (edge transport)

A `Fragment` is the transport unit produced by resource-constrained edge agents.
Its `Data` is a Prometheus XOR chunk payload (not raw samples). XOR is the only
defined fragment encoding.

- `MarshalFragment` / `UnmarshalFragment` — base64-wrapped JSON, for carrying a
  fragment inside an OTLP attribute.
- `EncodeFragmentBatch` / `DecodeFragmentBatch` — the compact binary `ASAPFRG1`
  wire frame (no JSON, no base64) carrying a batch of fragments between the edge
  cold tier and the backend merger. Labels are emitted in sorted name order so
  the frame is deterministic. Transient edge bookkeeping
  (`OOODropCount`/`FragmentULID`/`WatermarkTime`) is intentionally dropped on
  this path.

`ASAPFRG1` frame layout:

```text
magic "ASAPFRG1" (8B) | version u8 | fragment_count uvarint
repeat fragment_count:
  metric_name   : uvarint len + bytes
  label_count   : uvarint
  repeat: name (uvarint len + bytes), value (uvarint len + bytes)  # sorted by name
  min_time_ms   : zigzag varint
  max_time_ms   : zigzag varint
  sample_count  : uvarint
  encoding      : u8 (0 = XOR / chunkenc.EncXOR)
  source        : uvarint len + bytes
  chunk_len     : uvarint
  chunk_bytes   : [chunk_len]   # the raw chunkenc XOR chunk
```

### `StreamingFragmentEncoder`

Turns raw samples into encoded XOR fragments. It bounds out-of-order memory with
an event-time watermark (`ReorderGrace`) and never keeps a full flush window of
raw samples. Under rotating high cardinality it evicts idle, fully-shipped series
to bound retained state to the active working set (`IdleEvict`; defaults to
`3*ReorderGrace`, negative to disable).

```go
enc := gorilla.NewStreamingFragmentEncoder(gorilla.StreamingFragmentOptions{...})
enc.AddSample(sample)            // raw (metric, attrs, time, value)
frags, err := enc.Drain(force)   // watermark-safe fragments (force flushes all)
enc.ActiveSeries(); enc.DroppedSamples(); enc.EvictedSeries()
```

Each flushed fragment's `OOODropCount` is a PER-FRAGMENT DELTA (drops since the
last flush), so a consumer summing it across fragments reconstructs the true
total exactly.

`DecodeFragmentSamples` is the inverse of the per-series XOR encoding: it
recovers a fragment's raw `(ts, value)` samples (e.g. so the `intchunk` cold-part
path can archive the same data the XOR ship path uses).

### `StreamingTSDBBlockBuilder`

Writes Prometheus TSDB XOR chunks as samples become watermark-safe, then writes
`index` and `meta.json` at finalize, producing a standard Prometheus block
(`<ulid>/chunks/*`, `<ulid>/index`, `<ulid>/meta.json`) as a `TSDBBlockArtifact`.

```go
b, err := gorilla.NewStreamingTSDBBlockBuilder(gorilla.StreamingTSDBOptions{...})
b.AddSample(sample)                     // or b.AddSampleKeyed(key, sample)
b.DrainWatermark(t)
art, err := b.Finalize(ctx)             // *TSDBBlockArtifact
```

`SeriesKey` / `AddSampleKeyed` let a fused upstream (`asap_edge`) build the
canonical series key once (for sharding + cold dispatch) and share it; key parity
with the unkeyed path is guaranteed by sharing the same key code.

### `FragmentBlockFinalizer`

Consumes edge fragments (decoded XOR chunks) and writes a Prometheus TSDB block.
Intended to run in a gateway/backend collector, not the edge. It sorts and
de-overlaps each series' chunks, counts out-of-order drops (summing each
fragment's `OOODropCount` delta plus any chunk-overlap drops it detects itself),
and produces a `TSDBBlockArtifact`.

```go
f, err := gorilla.NewFragmentBlockFinalizer(gorilla.FragmentBlockOptions{...})
f.AddFragment(fragment)
art, err := f.Finalize(ctx)             // *TSDBBlockArtifact (nil if no samples)
```

`FragmentLabels` builds the Prometheus label set for a cold series exactly as the
finalizer does, so an alternate cold producer (e.g. the `intchunk`/`coldpart`
path) builds byte-identical label sets and a series archived via either format is
queried under the same identity.

---

## Wire formats at a glance

| Format | Producer → consumer | Description |
|---|---|---|
| JSON fragment (base64) | edge → OTLP attribute | `MarshalFragment`/`UnmarshalFragment` |
| `ASAPFRG1` | edge cold tier → backend merger | compact binary fragment batch |
| `ASAPCC1` | edge → backend merger | `coldpart` decode-on-read cold Part |
| Prometheus TSDB block | block builders → object storage / Thanos | standard `chunks/`, `index`, `meta.json` |
