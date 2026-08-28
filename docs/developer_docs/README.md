# Developer guide

This guide describes the architecture and the public contracts used to add a new
aggregation feature, sketch family, or query mapping. It deliberately omits private
types, helper functions, and implementation choices.

## 1. Code architecture

The repository has four relevant layers:

1. **Telemetry adapters** receive OpenTelemetry metrics and normalize them into the
   precompute model.
2. **Precompute** groups observations into configured windows, applies an aggregation
   policy, and produces mergeable sketch envelopes.
3. **Control plane** distributes configuration and coordination decisions to agents.
4. **Export and storage** forwards ordinary metrics or sketch envelopes to the
   configured downstream pipeline and archive tier.

The reusable contracts live in `asap-precompute-go`; collector integrations implement
the OpenTelemetry processor factory contract and translate configuration into the
precompute configuration. Query parsing, query-to-summary mapping, sketch algebra,
and rewrite rules are owned by
[ASAPPlanner](https://github.com/ProjectASAP/ASAPPlanner); the local
[query-mapping pointer](sketch-algebra-query-mapping.md) records that ownership.

For repository setup, overlay ownership, module-scoped tests, and end-to-end
verification, see the [build, test, and patch workflow](build-test-and-patch-workflow.md).

## 2. Interfaces and definitions

These are the supported extension contracts. Inputs and outputs are data values; an
implementation may choose any private representation.

### Sketch contract

```text
Sketch.Observe(value: Observation) -> error
Sketch.Merge(other: Sketch) -> error
Sketch.Encode() -> bytes
Sketch.Reset() -> void
```

`Observation` contains the metric value, timestamp, and the series identity selected
by the aggregation policy. `Encode` returns a self-contained, versioned payload that
can be merged by a compatible consumer. `Merge` must reject incompatible sketch
families or parameters rather than silently changing the result.

Optional capability contracts expose typed queries:

```text
QuantileSketch.Quantile(q: float64) -> (value: float64, ok: bool)
CardinalitySketch.Cardinality() -> (estimate: uint64, ok: bool)
FrequencySketch.TopK(k: uint32) -> (entries: []FrequencyEntry, ok: bool)
```

`FrequencyEntry` contains a key and its estimated count. `q` is in `[0, 1]`; `k` is
positive. An unavailable result returns `ok = false`.

### Precompute contract

```text
Precompute.Observe(observation: Observation) -> error
Precompute.ObserveKeyed(key: SeriesKey, observation: Observation) -> error
Precompute.Flush(now: Timestamp) -> (envelopes: []SketchEnvelope, error)
Precompute.Update(config: PrecomputeConfig) -> error
```

`PrecomputeConfig` defines the aggregation mode, window, sketch parameters, and
resource limits. `SketchEnvelope` contains the series identity, aggregation identity,
window boundaries, encoding version, and encoded sketch payload. `Flush` returns only
complete or explicitly closed windows and must preserve their identity metadata.

### Collector integration contract

```text
NewFactory() -> ProcessorFactory
Processor.ConsumeMetrics(ctx: Context, metrics: Metrics) -> (Metrics, error)
```

The processor receives an OpenTelemetry metrics batch and returns the batch to the
next consumer, unless the feature's documented policy intentionally consumes it.
Configuration is validated when the processor is created. Runtime failures are
returned as errors; malformed input must not produce an apparently valid envelope.

These interfaces are defined so that a new feature can be tested independently,
composed with existing adapters, and consumed by another runtime without depending
on private Go types.

## 3. Adding and verifying features

### Add a sketch family

Define its merge and encoding semantics, implement the sketch and any optional
capability contract, register a public configuration name, and connect the collector
factory to the precompute factory. Verify observation, merge, reset, encoding round
trips, incompatible-input errors, empty input, and configured resource limits. Then
run the processor integration tests and confirm that the output envelope metadata is
unchanged by the new family.

### Add an aggregation mode

Define the grouping key, window-close rule, identity fields, and behavior for late or
out-of-order observations. Add it to `PrecomputeConfig`, implement the mode through
the precompute contract, and document its mergeability and resource bounds. Verify
single-series, multi-series, boundary-timestamp, empty-window, and configuration
update cases. Interpret output by checking the grouping identity and window bounds
before comparing sketch values.

### Add a query mapping

Define the query operation, required sketch capability, input parameters, and result
shape in the sketch algebra reference. Map it to the compatible capability contract
and specify the fallback when the capability is unavailable. Verify exact boundary
semantics, empty results, invalid parameters, and merge-order independence. A valid
result must identify the aggregation and time window from which it was produced.
