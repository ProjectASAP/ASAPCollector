# Developing the stored-series ID dictionary

## Code architecture

The producer-side implementation lives in the patched OpenTelemetry Go metric
exporter:

```text
metric aggregation
  -> series.Dictionary.Annotate
  -> OTLP transform
  -> Export
  -> apply SeriesAssignments / UnknownSeriesIds
  -> next Annotate
```

The modified OTLP fields are maintained under `opentelemetry-proto-patch/`.
Backend resolution is maintained in ASAPQuery-backend. Changes must be tested
as one producer/consumer protocol.

## Public behavior

`WithSeriesDictionary(enabled)` controls the feature. Enabled is the current
default. `Dictionary.Annotate` leaves a new entry unregistered, with SID zero
and attributes intact. `Dictionary.Apply` installs only Backend-returned
assignments. `Dictionary.EvictByID` clears matching cached IDs so the next
export restores attribute-carrying mode.

Do not use the dictionary's local provisional counter as wire authority. The
Backend assignment is authoritative.

## Safe changes

When adding a metric data type:

1. define a stable metric-type discriminator;
2. include it in `annotateMetric`;
3. use the same descriptor and attribute fingerprint rules;
4. retain attributes until confirmation;
5. propagate the assigned SID into the aggregation state; and
6. test assignment, ID-only export, eviction, and relearning.

When changing canonicalization, update the patched proto contract and Backend
consumer together. Provide fixtures proving old/new compatibility or explicitly
version and cold-start the dictionary. Silent key-layout changes can attach a
valid numeric ID to the wrong stored series.

## Verification

Run the focused exporter tests from the restored OpenTelemetry Go patch tree,
then run Backend SID-resolution tests and a cross-repository OTLP round trip.
Required behaviors are listed in the
[stored-series identity design](../design_docs/stored-series-identity.md#acceptance-tests).

Benchmark dictionary-on and dictionary-off modes separately when measuring
summary savings. Report label-deduplication bytes independently from sketch,
delta, gzip, or archive compression.
