# otlpfilter — pre-decode OTLP wire-filter (coordinated update-sampling at the decode boundary)

Geometric-samples the datapoints of **warm-sketch** metrics **at the protobuf
wire level**, before the OTLP payload is decoded into pdata, and **drops the
rejected fraction** so it is never materialized (no attribute-map / value
decode). This is the "sample before (full) decode" idea from
[`docs/distributed-nitrosketch-coordinated-sampling.md`](../../docs/distributed-nitrosketch-coordinated-sampling.md)
("in-collector pre-decode shim").

Post-decode sampling (what `asap_edge` does today) can only skip the innermost
sketch-update loop (~3% of edge CPU); the dominant cost is the per-datapoint
OTLP decode + key materialization. This filter wire-skips dropped warm
datapoints **before** that decode, so the dropped fraction's decode cost is
saved too.

## What's here

| file | role | built/tested |
| --- | --- | --- |
| `state.go` | `SampleState`: atomic metric→`{P, Rows}` map, stateless consistent decisions; `SetParams`/`Upsert`/`SetP`, `keepDataPoint` | yes |
| `filter.go` | `FilterRequest` + generic `rewriteRepeated` protowire walk | yes |
| `filter_test.go` | unit tests (pdata-built OTLP round-trips through the filter) | yes (5 tests green) |
| `receiver.go` | `receiver.Metrics` component **sketch** (build tag `asap_otlp_receiver_sketch`) | shell only — needs collector receiver SDK |

The compiling + tested deliverable is the **library** (`state.go` + `filter.go`).
The receiver component is a documented shell; it is NOT OCB-built/run here.

## The protowire walk

`FilterRequest([]byte) []byte` walks the OTLP metrics tree with
`google.golang.org/protobuf/encoding/protowire` (low-level `ConsumeTag` /
`ConsumeFieldValue` / `ConsumeBytes` / `AppendTag` / `AppendBytes`) — **no full
unmarshal**:

```
ExportMetricsServiceRequest { repeated ResourceMetrics resource_metrics = 1 }
  └ ResourceMetrics       { Resource resource = 1; repeated ScopeMetrics scope_metrics = 2 }
      └ ScopeMetrics      { scope = 1; repeated Metric metrics = 2 }
          └ Metric        { string name = 1; oneof data { Gauge=5 Sum=7 Histogram=9 ExpoHist=10 Summary=11 } }
              └ <data msg> { repeated <DataPoint> data_points = 1 }
```

A single generic helper does the work at every level:

```go
func (s *SampleState) rewriteRepeated(buf []byte, targetField Number, fn func([]byte) []byte) ([]byte, bool)
```

It copies every **non-target** field verbatim (varint / fixed32 / fixed64 / other
length-delimited — bytes untouched), and for each **target** field (the repeated
length-delimited element) runs `fn` on the element bytes: `fn`'s return is
re-emitted, `nil` drops the element. Recursion is just `rewriteRepeated` calling
itself down the tree (request→rm→sm→metric→data→data_points).

Per `Metric`: read `name` (field 1) cheaply (`readMetricName`), classify via
`SampleState.paramsFor`:

- **not sampled** (name absent from the params map, or p outside (0,1)) → copy
  the whole `Metric` bytes through unchanged.
- **sampled** (warm) → find the present data-oneof variant and rewrite its
  `data_points`: for each datapoint, read `time_unix_nano` (field 3, fixed64)
  from the opaque bytes and evaluate the CONSISTENT decision
  `keep ⇔ ∃ r < Rows: ConsistentAdmit(SeedForMetric(name), timeNanos/1e6, r, P)`
  — **keep** ⇒ copy the datapoint bytes verbatim (attributes/value never
  decoded); **drop** ⇒ wire-skip (`R(x)=∅`, probability `(1−p)^d`). Zero/absent
  timestamp ⇒ keep (fail-open; the wrapper's occurrence-counter fallback
  samples such points instead).

Output is a freshly assembled, **byte-valid** `ExportMetricsServiceRequest` that
the stock `pmetric.ProtoUnmarshaler` decodes without error. The filter is
**fail-open**: on any malformed input it returns the original bytes unchanged.

## `SampleState` — the shared hook

```go
s := otlpfilter.Default()               // process-wide shared instance
// PRODUCTION writer — installed automatically by asap_edge's grant hook
// (warm_sketch.go: Engine.SetSampleGrantHook), one Upsert per accepted grant:
s.Upsert("http.server.duration", otlpfilter.SampleParams{P: 0.25, Rows: 6})
p, sampled := s.Params("http.server.duration") // read-only introspection
```

- `params atomic.Pointer[map[string]SampleParams]` — metric→`{P, Rows}`.
  Absent / P outside (0,1) ⇒ not sampled. `SetParams` swaps the whole map;
  `Upsert` does a copy-on-write single-entry update (P≥1 or ≤0 removes the
  entry; identical re-grants are no-ops). The read path never blocks.
- decisions are STATELESS (`common.ConsistentAdmit` with the canonical
  `common.SeedForMetric` seed and `occ = time_unix_nano/1e6` ms) — no sampler
  objects, no mutexes, and the collector wrappers re-derive the identical
  per-row admissions on survivors (design §3.1.1: one sampling decision,
  evaluated at any stage, idempotent under re-evaluation).

## Receiver component + OCB wiring (sketch)

`receiver.go` (build tag `asap_otlp_receiver_sketch`) sketches a
`receiver.Metrics` that wraps the **stock OTLP transport** (gRPC/HTTP unchanged)
and, on the metrics path, intercepts the **raw** export-request bytes:

```
wire bytes --FilterRequest--> thinned wire bytes --UnmarshalMetrics--> pmetric.Metrics --ConsumeMetrics-->
```

Traces/logs delegate straight to the stock handlers. To build it for real, lift
this file (minus the tag) into a small receiver module that has the collector SDK
in its `go.mod`.

OCB `builder-config.yaml` — register the receiver and swap `otlp`→`asap_otlp` in
the **metrics** pipeline only:

```yaml
receivers:
  - gomod: github.com/ProjectASAP/asap-otlp-receiver v0.0.0
    # (a thin module that imports otlpfilter + the collector receiver SDK)

service:
  pipelines:
    metrics:
      receivers: [asap_otlp]      # was: [otlp]
      processors: [asap_edge, ...]
      exporters: [...]
    traces:  { receivers: [otlp], ... }   # unchanged
    logs:    { receivers: [otlp], ... }   # unchanged
```

The shared `SampleState` is `otlpfilter.Default()`: `asap_edge`'s grant hook
(the writer, wired in `warm_sketch.go` via `Engine.SetSampleGrantHook`) and the
receiver (the reader; `NewFactory(nil)` defaults to `Default()`) reference
**one in-process map** with no extra wiring. Inject a dedicated state into
`NewFactory` only for isolated multi-pipeline builds.

## Integration contract (asap_edge + backend)

1. **The edge re-evaluates, never re-samples.** Filter and wrappers compute the
   SAME stateless decision from shared inputs (`SeedForMetric(name)`,
   `time ms`, row), so the wrapper's per-row admissions on survivors are the
   decision the filter already took — idempotent, no compounding. The filter's
   `Rows` comes from the same family config the wrapper uses
   (`wireSampleRows`); too-large is safe (wasted wire), too-small over-drops.
2. **Weighting is applied exactly once, at the wrapper.** CountSketch/CMS put
   the `1/p` weight in place (wire envelope stays EXACT — no backend rescale);
   DDSketch (d=1) keeps raw admitted counts and stamps `sample_p` via
   `SetWireSampleP`, so the backend's existing `×1/p` rescale applies there
   only.
3. **Backend needs NO changes.** Exact-wire families need no rescale by
   construction; DDSketch reuses the existing `sample_p` envelope path.
   Because the contract just sets `sample_p` on the envelope (a value the backend
   already honors), **asapquery-backend requires no change**.

## Verify

```
cd asap-precompute-go
go build ./...               # clean
go test ./otlpfilter/ -v     # 5 tests green
go vet ./otlpfilter/         # clean
```

Sample test output (deterministic seeds):

```
warm p=0.25 N=1000: kept=239 (expected ~250, dropped=761)
mixed: cold kept=1000 (=N), warm kept=311 of 1000 (p=0.30)
sampler admits=209 == survivors=209 (dropped 791 never materialized)
```

## Prototype-only caveats

- The **receiver component is a shell** (`receiver.go`, build-tagged out): it is
  not compiled into this library module and is not OCB-built or run here. The
  tested core is the wire-filter library.
- The walk handles the standard Gauge/Sum/Histogram/ExponentialHistogram/Summary
  data-oneof variants. The patched-OTLP sketch data variants (DDSketch/KLL/HLL/
  Count-Sketch/Count-Min) are single-datapoint sketch payloads, not
  per-datapoint sampling targets, and are not in scope for datapoint thinning —
  they pass through as cold unless their metric name is in the p-map (in which
  case `data_points` is thinned like any other; for sketch payloads you would
  leave them out of the p-map).
```
