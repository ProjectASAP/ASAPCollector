# ASAPCollector core algorithm: GOS collection and transmission

> Status: mixed. Sampling, sketch updates, full/delta encoding, and several GOS
> allocation/detection pieces exist; the scalar GOS knobs are not yet wired
> through a production CollectorPlan end to end.

## TL;DR

ASAPCollector's core data-plane algorithm turns a physical collection rule into
bounded local work and bounded divergence from backend state. The controller
chooses the summary, sampling/error allocation, and transmission policy. The
Collector performs deterministic admission, updates windowed raw/exact/sketch
state, detects threshold crossings at insert time, and wakes the existing
full/delta export path. The backend applies the fragments to maintain queryable
state; alerting and query readout remain backend responsibilities.

This design is the concise operational form of the GOS design reviewed in
[ASAPCollector PR #547](https://github.com/ProjectASAP/ASAPCollector/pull/547).
Detailed proofs belong in `sampling-cdm-gos-derivations.md`; this page defines
the component algorithm and its contracts.

## Inputs and outputs

Inputs from CollectorPlan/control policy:

- source metric/matchers and summarized sample value or item label;
- retained grouping/reduction and materialization fingerprint;
- raw, exact-aggregation, or sketch family and typed parameters;
- window, lateness, activation, retirement, and resource limits;
- sampling probability/seed contract where sampling is legal;
- accuracy/freshness budget and GOS scalar threshold parameters; and
- full/delta encoding, checkpoint cadence, sequence scope, and backend target.

Runtime input is an ordered stream of timestamped telemetry observations with
resource and point labels. Output is a self-describing raw/full/delta record
carrying plan/materialization identity, canonical series evidence or assigned
SID, window, producer epoch, encoding/schema, checkpoint/sequence, and payload.

## Controller-side synthesis

The expensive optimization is outside the Collector hot path:

1. ASAPPlanner selects logical summary/readout semantics and an accuracy target.
2. The physical controller binds sources, placement, grouping, panes,
   capabilities, and transport.
3. It splits the query error budget into sketch collision error, sampling
   error, and deterministic unsent-delta staleness:

   ```text
   sqrt(epsilon_sketch^2 + epsilon_sampling^2)
     + epsilon_staleness
     <= epsilon_query
   ```

4. It chooses a legal sampling rate and solves the per-cell threshold
   allocation. High-activity cells tolerate larger thresholds; cells to which
   the query is more sensitive receive smaller thresholds. Query-error and
   freshness caps bound every threshold.
5. It publishes scalar policy parameters and compatibility metadata. The edge
   reconstructs thresholds from those scalars and live activity; an `O(d*w)`
   threshold vector does not need to cross the control channel.

Sampling is allowed only for families with a valid estimator. Additive matrix
families use inverse-probability weighting; KLL compaction and HLL register-max
must not be treated as weighted additive counters.

## Collector hot-path algorithm

For every observation matching an active materialization:

```text
1. canonicalize resource/point labels
2. evaluate source matchers
3. derive retained group key and materialized-series evidence
4. assign observation to the configured window
5. deterministically decide row/item admission, if sampling is enabled
6. update the family-specific local state
7. update activity/rate statistics
8. test the changed scalar/cell/bucket/register/sketch against its threshold
9. if crossed, mark dirty and send a non-blocking wake to the flush loop
```

Admission is a pure function of shared seed, occurrence identity, and row. If
two stages accidentally recompute it, they choose the same admitted set rather
than multiplying sampling probabilities. Exactly one stage applies the
inverse-probability weight.

Series state is isolated by materialization definition, retained label values,
and window. Observations from different materializations, groups, or windows
never share an accumulator merely because their source metric matches.

## Insert-time detection and wake-on-demand flush

Threshold crossing changes when export is requested, not how the telemetry
pipeline transports it:

```text
flush loop:
  timer tick  -> flush()     # slow recovery/periodic checkpoint
  wake signal -> flush()     # prompt threshold-crossing export
```

The insert path sends a coalescing, non-blocking wake. `flush()` continues to
use the normal snapshot/delta encoder and exporter, preserving retry,
backpressure, and pipeline ownership. It must not perform an out-of-band network
write from the insert call.

## Family-specific detection

| Family | Detection unit | State after delta emission | Merge/readout consequence |
| --- | --- | --- | --- |
| Sum/exact additive accumulator | scalar delta | subtract/reset reported delta | Backend addition reconstructs total |
| Count-Min Sketch | changed matrix cell | zero reported cell delta | Backend cell-wise addition; point/top-k readout is backend-side |
| Count Sketch | changed signed matrix cell | zero reported cell delta | Backend cell-wise addition and median/readout |
| DDSketch | changed bucket count | zero reported bucket delta | Backend bucket addition and quantile readout |
| KLL | whole sketch/count trigger | emit full disjoint segment and reset | Backend merges segments; no compact order-independent cell delta is claimed |
| HLL | changed register | keep MAX state; clear dirty marker | Backend register-wise MAX; duplicate sends are idempotent |

For additive families, emitted fragments telescope to the cumulative state
regardless of asynchronous per-cell reset times. For HLL, reset would be
incorrect because MAX-merge needs the current register value. Family semantics,
not a generic byte-diff helper, determine reset and merge behavior.

## Full/delta state machine

The sender maintains per `(destination, SID, producer_epoch, window)`:

- last acknowledged full checkpoint/base;
- next sequence and dirty units;
- encoding/schema compatibility; and
- time since the last required full checkpoint.

The receiver must be able to apply every delta exactly once to its named base.
A missing base, sequence gap, incompatible materialization/schema, restart
without lineage, or replica handoff forces a full checkpoint. SID alone is not
sufficient delta lineage.

Cold-start crossings are expected: activity-dependent thresholds begin small,
allowing the backend to receive usable state early. A floor may be introduced
only as an explicit accuracy/freshness trade-off, not to make graphs quieter.

## Relationship to other components

| Component | Responsibility |
| --- | --- |
| ASAPPlanner | Select summary/readout semantics and logical error guarantee |
| Physical compiler | Allocate placement, window, sampling/threshold policy, transmission and matching BackendPlan |
| Collector plan validator | Reject unsupported family/parameters/policy before activation |
| Collection router/window manager | Match observations, build canonical groups, and isolate window state |
| Family implementation | Update, threshold test, snapshot/delta, reset/dirty behavior |
| SID dictionary/exporter | Retain identity evidence until assignment, frame records, retry and resynchronize |
| Backend ingest/store | Validate lineage and merge fragments into queryable state |
| Query engine | Execute readout/alerts against backend state; never depend on a corrupted locally reset sketch |

## Complexity and resource bounds

The normal insert path touches only the rows/cells required by the chosen
family and the unit changed by that observation. Geometric skip sampling makes
random-number work scale with admitted updates rather than candidates. The
communication/apply rate follows the activity-to-threshold ratio
`sum_j(V_j/T_j)`; periodic full checkpoints add bounded recovery overhead.

Cardinality limits, maximum windows/groups, sketch dimensions, queue capacity,
and exporter backpressure policy must be explicit in CollectorPlan or local
deployment policy. Resource pressure must not silently change sampling,
accuracy, grouping, or family parameters.

## Current status and open work

Implemented/live pieces include family sketch updates, full/delta envelopes,
sampling paths, scalar monitoring, and relevant threshold-allocation helpers.
GOS scalar derivation and edge threshold reconstruction exist in code/test
paths, but production configuration does not yet thread all required knobs into
the Collector processor; sampling/threshold coupling is therefore not an
end-to-end production claim.

Open correctness work includes anisotropic CountSketch activity under
asynchronous resets, DDSketch bucket-growth bounds, and HLL small-cardinality
behavior. These require explicit tests/guarantees before activation.

## Acceptance tests

- deterministic admission agrees across SDK/filter/Collector recomputation;
- sampling occurs once and preserves the declared estimator;
- grouping and window boundaries isolate state correctly;
- each family crosses, emits, resets/marks dirty, and merges according to the
  table above;
- wake coalescing never blocks inserts and timer fallback still flushes;
- full and complete delta sequences produce equivalent backend readout;
- duplicate, gap, restart, unknown SID, and forced checkpoint paths fail closed;
- achieved error and freshness stay within the predeclared combined budget; and
- CPU, memory, wire bytes, checkpoint bytes, and backend apply cost are measured
  separately.
