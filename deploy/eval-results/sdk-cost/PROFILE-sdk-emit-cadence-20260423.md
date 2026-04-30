# SDK emit-cadence profiling — per-sample vs batched raw-buffer

_Ran: 2026-04-23._

Targeted follow-up to the encoding-axis finding. Question: at
the same ground-truth raw-sample rate, does it cost more or
less to emit each sample immediately vs buffer them and emit
in a batch once per longer window?

All four configurations use `AggregationRawBuffer`, per-series
emit rate = 10 Hz (one `Counter.Add` per 100 ms per series),
the native fake-exporter binary running against the live N=1
stack via OTLP gRPC. Only two knobs vary:

| Config | `W` (PeriodicReader interval) | Cardinality | Per-tick data-point volume |
|---|---:|---:|---:|
| **A-100ms × 10**  | 100 ms | 10 | 1 sample × 10 series = 10 dp / tick |
| **A-100ms × 100** | 100 ms | 100 | 1 × 100 = 100 dp / tick |
| **B-10s × 10**    | 10 s | 10 | 100 × 10 = 1 000 dp / tick |
| **B-10s × 100**   | 10 s | 100 | 100 × 100 = 10 000 dp / tick |

Aggregate wire data-point rate is the same at matching
cardinality (A and B both do `card × 10` dp/s average) — only
the flush cadence changes.

Captured: 15 s CPU profile + heap snapshot via
`http://127.0.0.1:6060/debug/pprof/*` on the fake-exporter
binary (pprof listener gated on the new `EXPORTER_PPROF_ADDR`
env var).

## Results

### CPU spent during the 15 s profile window

| Config | Total CPU samples | % of one core |
|---|---:|---:|
| A-100ms × 10 | 460 ms | 3.07 % |
| A-100ms × 100 | 1.54 s | 10.20 % |
| B-10s × 10 | 200 ms | 1.32 % |
| B-10s × 100 | 870 ms | 5.75 % |

At matching cardinality, **A (per-tick) uses ~2 × the CPU of
B (batched)**. The delta isn't data-volume — same number of
data points moves through the pipeline — it's the per-flush
overhead Config A pays ×100 more often (one flush per 100 ms
vs one per 10 s).

CPU scales with cardinality roughly 3–4 × between card=10 and
card=100, not 10 ×; the fixed per-flush cost amortises over
larger batches.

### RSS evolution

| Config | Initial RSS | RSS after 15 s profile | Δ |
|---|---:|---:|---:|
| A-100ms × 10 | 29 MiB | 29 MiB | ~0 |
| A-100ms × 100 | 32 MiB | 35 MiB | +3 MiB |
| B-10s × 10 | 15 MiB | 25 MiB | +10 MiB |
| B-10s × 100 | 17 MiB | 56 MiB | **+39 MiB** |

Config A keeps RSS ~flat (every tick empties the buffer, GC
reclaims immediately). Config B's RSS grows during the
buffering window as samples accumulate; at card=100 the first
flush allocates ~39 MiB transiently, matching the heap top
(below).

### Where the CPU goes — `pprof -top -cum` hot paths

**Config A (`W=100 ms`), card=100 — dominated by gRPC export path**:

```
PeriodicReader.collectAndExport   54 %
otlpmetricgrpc.Export              54 %
grpc.ClientConn.Invoke             53 %
otlp/collector/metrics/v1.Export   53 %
```

Every 100 ms the SDK spins up flate/gzip compression, walks
the aggregator, invokes `ClientConn.Invoke`. These costs
aren't amortisation-friendly — roughly the same fixed overhead
per flush regardless of payload size.

**Config B (`W=10 s`), card=100 — dominated by GC + protobuf marshalling**:

```
runtime.gcBgMarkWorker                  30 %
runtime.gcDrain                         30 %
PeriodicReader.collectAndExport         26 %
otlpmetricgrpc.Export                  ~25 %
```

Burst behaviour: most of the 10 s the goroutines are idle
buffering; then at the flush tick the SDK synchronously walks
100 × 100 = 10 000 buffered tuples, marshals them into OTLP
protobuf, and ships. GC chases the burst of `transform.*`
allocations.

### Where the memory goes — `pprof -top inuse_space`

**Config B-10s × 100 heap** — 12.02 MiB total; the biggest
allocators are all in
`otel/exporters/otlp/otlpmetricgrpc/internal/transform/*`:

```
7.50 MiB  transform.Value         (21 %)
6.00 MiB  transform.KeyValue      (17 %)
2.00 MiB  transform.DataPoints    ( 6 %)
```

These are the per-flush protobuf serialisation intermediates
for the 10 000-data-point batch. The raw-buffer aggregator's
own `(ts, attrs, value)` slice adds on top. Size scales
linearly with `cardinality × samples_per_window`.

**Config A heap** stays around 5–7 MiB, mostly
`compress/flate.NewWriter` (gzip state) plus small runtime
allocations. No sample buffer to hold.

## Bottleneck summary

| Config | CPU bottleneck | Memory bottleneck |
|---|---|---|
| **A (W=100 ms)** | gRPC export path — `ClientConn.Invoke` + flate writer init, once per 100 ms flush. Scales with flush frequency, not data volume. | None; flat RSS. |
| **B (W=10 s), low card** | Almost nothing — 1.3 % of one core, mostly idle. | Low; bounded by `card × samples_per_window × ~80 B`. |
| **B (W=10 s), high card** | GC (~30 % of CPU samples) chasing the burst of protobuf-transform allocations at flush. | Flush-time spike: RSS more than triples as `transform.*` builds the big payload. |

### Net takeaway

- **Per-tick emit (Config A)** trades steady CPU for flat
  RSS. The CPU floor is the fixed per-flush gRPC + compression
  cost × flush frequency. Pays badly at short `W` because the
  overhead doesn't amortise.
- **Batched raw-buffer (Config B)** trades CPU for bursty
  memory. Cheap while buffering, but the flush tick is a
  synchronous burst that marshals the whole window's worth of
  data points, triggers GC, and can more than triple RSS.
- Neither scales linearly in the direction you'd naively
  expect. Config A's CPU scales with `1/W` (flush rate), not
  cardinality. Config B's memory scales with
  `cardinality × samples_per_window`, not with aggregate data
  rate.

Design implication: small `W` + modest cardinality is the
cheap corner. Large `W` + large cardinality is the dangerous
corner — trades modest average CPU for bursty memory pressure
that a container's memory cgroup will clip.

## Reproducing

```bash
export EXPORTER_TARGET=localhost:14317
export EXPORTER_SDK_AGG=raw-buffer
export EXPORTER_SDK_WINDOW=100ms       # or 10s
export EXPORTER_CARDINALITY=10         # or 100
export EXPORTER_FREQ_HZ=10
export EXPORTER_PPROF_ADDR=127.0.0.1:6060

./fake-exporter &
PID=$!

# warm up, then capture
sleep 5
curl -sS "http://127.0.0.1:6060/debug/pprof/profile?seconds=15" \
  -o /tmp/sdk.cpu.pb
curl -sS "http://127.0.0.1:6060/debug/pprof/heap" \
  -o /tmp/sdk.heap.pb

kill $PID

# analyse
go tool pprof -top -cum -nodecount=10 /tmp/sdk.cpu.pb
go tool pprof -top -nodecount=5       /tmp/sdk.heap.pb
```

Raw `.pb` artefacts aren't committed (~7–12 KiB each). The
`EXPORTER_PPROF_ADDR` env knob landed with this doc; it's a
no-op when unset so production runs of the fake-exporter
aren't exposing a pprof listener.

## Fine-grained follow-up (2026-04-23, same day)

The first pass used `cardinality=10 or 100` at `freq_hz=10` —
small workloads where per-flush fixed costs dominated. The
user pushed for 2–3 orders of magnitude more:

| Config | `W` | Card | `freq_hz` | Aggregate raw | Per-tick dp |
|---|---:|---:|---:|---:|---:|
| A c1000 f100 | 100 ms | 1000 | 100 | 100 k events/s | 10 k |
| B c1000 f100 | 10 s | 1000 | 100 | 100 k events/s | 1 M |
| A c1000 f1000 | 100 ms | 1000 | 1000 | 1 M events/s | 100 k |
| B c1000 f1000 | 10 s | 1000 | 1000 | 1 M events/s | 10 M |

Same native-binary + live-stack + 15 s pprof harness.
`EXPORTER_MAX_BUFFER_PER_SERIES=200 000` to stop raw-buffer
overflow from dropping samples in the big Config B cells.

### Results

| Config | CPU (% of 1 core) | RSS start | RSS end |
|---|---:|---:|---:|
| A c1000 f100 | **280 %** | 658 MiB | 1.45 GiB |
| B c1000 f100 | **302 %** | 113 MiB | 2.08 GiB |
| A c1000 f1000 | **811 %** | 1.67 GiB | 3.68 GiB |
| B c1000 f1000 | **979 %** | 332 MiB | 5.95 GiB |

Three things the first pass missed:

1. **The A-vs-B CPU gap collapses at scale.** In the first pass
   (tiny workloads), A used ~2 × B's CPU because the per-flush
   gRPC/flate overhead was the dominant cost and A paid it
   100 × more often. At fine scale the per-flush overhead
   amortises over enormous per-tick batches (10 k–100 k dp), so
   it's no longer the dominant cost — A and B converge (+7 %
   at 100 k events/s; B is actually _worse_ by +21 % at 1 M
   events/s because it adds GC and buffer-management cost on
   top of the same marginal per-dp work).

2. **The hot path shifts from gRPC export to aggregator
   mutex contention.** At `freq_hz=1000` the hot frames in
   both configs are:

   ```
   go.opentelemetry.io/otel/sdk/metric/internal/aggregate
       .(*rawBufferValues[…]).measure         ~38 % cum
   internal/sync.(*Mutex).lockSlow             ~26 %
   sync.(*Mutex).Lock                          ~26 %
   main.runSynthetic.func1 (per-series goroutine) ~50 % cum
   ```

   `rawBufferValues.measure` takes `valuesMu` for the whole
   buffer on every `Add` call. With 1000 goroutines firing
   1000 Adds/s each = 1 M `Lock` acquisitions/s, lock contention
   becomes the bottleneck. **This is a real design issue in
   the patched aggregator**: a single `sync.Mutex` over the
   whole `map[attribute.Distinct]*rawBufferSeries[N]`
   serialises every producer goroutine.

   Fix options (all deferred, out of scope for this pass):
   - Shard the buffer by a hash of the attribute key
     (`[]rawBufferValues` keyed by hash % N_shards; each shard
     has its own mutex).
   - Use a `sync.Map` or a striped lock.
   - Per-series lock (move the `[]rawBufferSample` onto the
     series struct and take only that series' lock during
     `append`). The map mutation for new series is rare enough
     to stay under a global lock.

3. **Heap allocations are dominated by the OTLP transform
   path, not the raw-buffer itself.** Top heap consumers at
   `1 M events/s`:

   ```
   otlp transform.Value           —  547 MiB inuse  (25 %)
   otlp transform.KeyValue        —  445 MiB         (20 %)
   otlp transform.DataPoints      —  237 MiB inuse,  1.28 GiB cum
   rawBufferValues.measure        —  226 MiB
   aggregate.reset[DataPoint]     —  668 MiB
   ```

   For every flush, the OTLP exporter builds a protobuf
   `ResourceMetrics` representation by allocating fresh
   `KeyValue`, `Value`, `DataPoint` objects for each data
   point. At 1 M dp/tick this is hundreds of MiB of transient
   allocations per flush — the main driver of the 3.7–6 GiB
   peak RSS. Pool reuse in the upstream transform would help
   both configs; the raw-buffer aggregator itself is
   relatively disciplined.

### Updated bottleneck picture

| Workload scale | Config A (`W=100 ms`) bottleneck | Config B (`W=10 s`) bottleneck |
|---|---|---|
| Tiny (≤ 1 k events/s) | Fixed per-flush cost (gRPC/flate), paid once per 100 ms | Occasional GC spike at flush; bursty memory |
| Moderate (100 k events/s) | Marginal per-dp cost + flush amortises | Same + buffer holds 1 M dp before flush → transient 2 GiB heap |
| Fine (≥ 1 M events/s) | **Aggregator mutex contention in `rawBufferValues.measure`** dominates both configs | Same + heap pressure from 10 M-dp flush bursts (~6 GiB RSS) |

The first pass's conclusion — "small `W` + modest card is
cheap; large `W` + large card is dangerous" — still holds at
moderate scale. At fine-grained scale both configs are
expensive and run into the same mutex wall. The producer
process needs a sharded-buffer redesign before SDK-side
`raw-buffer` is usable at ≥ 1 M events/s as a paper baseline.

### Artefacts

Raw `.pb` files under `/tmp/sdk-profiles2/` on the dev box;
not committed.
