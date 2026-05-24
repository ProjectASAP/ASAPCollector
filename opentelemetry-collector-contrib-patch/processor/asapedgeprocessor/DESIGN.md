# asap_edge — fused, sharded edge aggregation (design)

Issue #46 follow-up. Status: in progress on `feat/asap-edge-fused`.

## 1. Motivation

Profiling the 1-sketch + gorilla agent (30K series @ 100 Hz) found three structural costs in the previous topology (`routing connector → 6 per-family pipelines, each [gorillas3, <aggregator>]`):

1. **Redundant parse + key.** The cold processor (`gorillas3`) and the warm aggregator each independently re-iterated the same data points, re-converted `dp.Attributes()`, and re-built the series key. ~2× decode+key per sample (`getAttrMap`+`appendSeriesKey` on the cold side, `AttributesToKeyValues`+`buildSeriesKey` on the warm side).
2. **No parallelism.** Every processor takes a processor-wide mutex in `ConsumeMetrics`, so concurrent OTLP `Export` goroutines serialize on one lock — the agent could not exceed ~1 core for ingestion (measured: <100% CPU regardless of load; only GC ran on other cores).
3. **A pathological sum path.** The counter's sum-by-zone aggregation ran through contrib `metricstransform`, whose `aggregateutil.dataPointHashKey` builds a grouping key per data point via `atts.AsRaw()` + `ts.String()` + `json.Marshal` (reflection) — **~17% of agent CPU**, the single most expensive thing in the agent.

## 2. Architecture

Replace routing connector + per-family pipelines + per-pipeline `gorillas3` + `metricstransform` with **one processor**:

```
otlp → cumulativetodelta → asap_edge → otlp/backend
```

`asap_edge` internally:

- **One decode + key pass.** Each datapoint's attributes are decoded once into a shared form; the series key is built once and dispatched to both the cold builder and the metric's warm aggregator. (Cold and warm key *formats* differ — cold = gorilla `metricName+attrs+extLabels`; warm = precompute `AggID|labels` filtered by `aggregate_by` — so both are built from the one shared decode rather than re-decoding.)
- **Key-hash sharding.** `shard_count` (default 4) independent shards, each owning `{mutex, cold builder, warm aggregators}`. A datapoint goes to `shard = hash(seriesKey) % N`. Concurrent `Export` goroutines on distinct series ingest on distinct cores → real multi-core parallelism. Sum is associative, so per-shard partials merge at flush.
- **Cold tier (per shard).** A `StreamingFragmentEncoder` fed via `AddSample`. On flush each shard `Drain`s its compact Gorilla-XOR **chunk fragments**; all shards' fragments are batched into the shared `ASAPFRG1` binary frame (`EncodeFragmentBatch`), gzipped, and **POSTed to a `/ingest` endpoint** (the backend merger). The edge does **no** index build, no TSDB-block finalize, no 1h cut / S3 PUT — it only emits XOR-chunk fragments, leaving the block/index build to the backend merger.
- **Warm tier (per shard).**
  - **sum** — `asap_edge`'s own `map[groupKey]→{sum,count}` (group key = `aggregate_by`, e.g. `[zone]`). No precompute, no sketch envelope. Flush merges per-shard partials by group key → emits the **same zone-keyed delta Sum metric** `metricstransform` emits today (backend unchanged).
  - **sketches** (ddsketch/kll/hll/cms/cs) — precompute `Precompute` instances fed via keyed-observe (no internal re-key), emitting the existing sketch envelopes.
- **Staggered (round-robin) flush.** Flushing all `shard_count` shards on one `window_duration` ticker builds + releases the N shards' state in lockstep → one synchronized memory/CPU burst per window (a +N×-amplitude sawtooth). Instead, the flush ticker fires every `window_duration / shard_count`; on tick `k` only shard `k % shard_count`'s **cold fragments + sketches** are drained/shipped. Each shard still flushes once per `window_duration`, but the N shards are phase-shifted by `window_duration / shard_count`, so their sawtooths interleave → the aggregate memory swing drops toward `1/shard_count` of the synchronized peak and the CPU burst splits into N smaller offset ones. The **cross-shard sum** stays a **unified flush** on the `window_duration`-aligned cadence (merge ALL shards' partials + emit + reset, once per window) — sum state is tiny so it's not the memory driver, and a unified flush keeps the backend's per-group delta total per window byte/semantically unchanged. Guard: `shard_count <= 1` or `window_duration <= 0` falls back to the single all-shard `flushAll`. Shutdown always does a final `flushAll` (all shards + sum) so no un-flushed shard is lost. Transient flush buffers (the per-ship `gzip.Writer` and its ~4MB flate state) are pooled via `sync.Pool` + `Reset` to kill the per-flush allocation/GC spike.

## 3. Shared-key seam (the crux)

To parse+key once, the cold builder and the warm aggregators must accept a **pre-built key**:

- gorilla: `AddSampleKeyed(key, sample)` → `getSeriesKeyed` (skips `appendSeriesKey`). **Invariant:** `key` byte-identical to `appendSeriesKey(metricName, attrs)`. (done, tested)
- precompute: `ObserveKeyed(key, obs)` → `windowState.observeKeyed` (skips `buildSeriesKey`). **Invariant:** `key` byte-identical to `cfg.SeriesKeyFor(obs)`.

`asap_edge` derives both keys from the shared decode using the same `appendSeriesKey` / `SeriesKey` logic, so keyed and legacy admits land on the same series.

## 4. Two tracks

- **Track 1 — agent (`ASAPCollector`):** this processor + keyed APIs + config rewrite. Ships cold blocks to the existing merger `/ingest` (no backend dependency to land).
- **Track 2 — backend (`asapquery-backend`):** move the merger's cut/merge into the Rust `gorilla_object_store` storage engine. It currently *reads* S3 only; the read path was already designed for compactor-merged blocks (`IndexEntry.object_key/byte_offset/byte_length`, `Postings::merge_many`) but the **write/compactor side was never built**. Add: `ObjectStore` `put_object`/`get_object_range`/`list`/`delete_object`; a `compactor.rs` that byte-concatenates whole `GORILLA1` objects of a 1h window into one block + rebuilds `IndexFile` (with offsets) + `Postings`; Range support in `read_chunk`; and a query path that **unions the open (pre-flush) window with the S3 cut blocks**. Then retire the Go `gorilla-head-merger`.

## 5. Parity (keep backend output the same)

- **sum** → byte/semantically identical zone-keyed **delta** Sum metric (`http_requests_total`), as `metricstransform aggregate_labels label_set:[zone] aggregation_type:sum` produces today.
- **sketches** → identical sketch envelopes.
- **cold** → identical GORILLA1 blocks; the read/query API of `gorilla_object_store` is unchanged for callers.

## 6. Risks

1. **`label_hash` parity** (Track 2) — no shared hash impl between the Go agent writer and the Rust backend; a mismatch silently mis-prunes postings. Highest correctness risk.
2. **`read_chunk` Range** (Track 2) — adding Range reads without regressing the whole-object path; touches the live query path.
3. **keyed-observe / AddSampleKeyed key format** (Track 1) — the pre-built key must match the legacy key byte-for-byte; guarded by equivalence tests.
4. **write-then-delete atomicity** (Track 2) — mirror the Go meta/index-last readiness ordering; consider verify-read before pruning sources.
5. **byte-copy only** — never decode/re-encode in the cut (SeriesMeta attribute key order differs: Go `map` vs Rust `BTreeMap`).

## 7. Implementation order

1. ✅ gorilla `AddSampleKeyed` (+ test)
2. precompute `ObserveKeyed`
3. sum aggregator (per-shard map + flush → zone-keyed delta Sum)
4. wire `asap_edge` shards: single decode/key pass, cold via `AddSampleKeyed`+`merger_sink`, warm via keyed-observe; `flushAll` (merge sum partials, forward)
5. `go.mod` + register `asap_edge` in the OCB manifest; build the full binary
6. rewrite b6 config to the single-pipeline all-family topology
7. Track 2: Rust `compactor.rs` + `ObjectStore` extensions + Range read + open∪S3; retire Go merger
8. build + parity (sum-by-zone, quantile, cold incl. open window) + re-profile (expect metricstransform 17% gone, double parse/key gone, CPU scales past one core)
