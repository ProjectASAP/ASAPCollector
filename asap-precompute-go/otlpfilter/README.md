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
| `state.go` | `SampleState`: atomic metric→p map + per-metric geometric samplers; `SetP`, `admit` | yes |
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
`SampleState.admit`:

- **not sampled** (name absent from p-map, or p≥1) → copy the whole `Metric`
  bytes through unchanged (no sampler draw consumed).
- **sampled** (warm) → find the present data-oneof variant and rewrite its
  `data_points`: for each datapoint, `admit` draws one geometric decision —
  **keep** ⇒ copy the datapoint bytes verbatim (attributes/value never decoded);
  **drop** ⇒ wire-skip.

Output is a freshly assembled, **byte-valid** `ExportMetricsServiceRequest` that
the stock `pmetric.ProtoUnmarshaler` decodes without error. The filter is
**fail-open**: on any malformed input it returns the original bytes unchanged.

## `SampleState` — the shared hook

```go
s := otlpfilter.NewSampleState()       // empty => nothing sampled (passthrough)
s.SetP(map[string]float64{"http.server.duration": 0.25}) // asap_edge OnGrant writer
sampled, keep := s.admit("http.server.duration")          // lock-free reader (per datapoint)
```

- `pmap atomic.Pointer[map[string]float64]` — metric→effective `p` in (0,1].
  `1.0`/absent ⇒ not sampled. `SetP` swaps the whole map lock-free; the read path
  never blocks.
- per-metric `common.GeometricSampler` (reused from
  `sketchlib-go/common/sampling.go`), seeded **deterministically from the metric
  name** (FNV-1a) so runs are reproducible. `SetP` invalidates a sampler whose
  `p` changed so it is rebuilt lazily.
- `admit(metric) (sampled, keep)`: not-sampled ⇒ `(false, true)`; sampled ⇒
  `(true, geometricSampler.Admit())`.

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

The shared `SampleState` is injected so `asap_edge`'s `OnGrant` (the writer) and
the receiver (the reader) reference **one in-process map** (e.g. via a host
extension or a shared singleton passed to both factories).

## Integration contract (asap_edge + backend)

1. **The edge must NOT re-sample.** The receiver already applied the geometric
   skip at the wire. `asap_edge` ingests the thinned datapoints at face value
   (it must keep its own sampler at `p=1` for these metrics, or it would compound
   the sampling and double-rescale).
2. **The edge stamps `SketchEnvelope.sample_p = the receiver's p`** for the
   metric, so the backend applies the `×1/p` rescale exactly once. The receiver's
   `p` is the same value `OnGrant` wrote into `SampleState`, so the edge reads it
   from the shared grant.
3. **Backend needs NO changes for this prototype.** The `1/p` rescale already
   exists where it matters — CMS, DDSketch (Count), HLL cardinality rescale on
   `sample_p`; Count-Sketch is self-contained (insert-upweight inside the sketch).
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
