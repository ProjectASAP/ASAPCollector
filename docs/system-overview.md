# ASAP system overview

---

## 1. TL;DR

ASAP is a **continuous-monitoring observability pipeline**: instead of
scanning raw samples at query time, it keeps a live, incrementally
updated sketch of every metric at the backend, fed by a steady trickle
of small deltas from the edge. The point of the design is to make
**high-frequency, high-cardinality metrics** affordable to collect and
fast to query, without giving up freshness.

```
SDK (in-app)                agent collector (asap-otel)         backend (ASAPQuery-backend)
─────────────                ────────────────────────────         ───────────────────────────
raw samples          sampling      insert into a live       continuous     apply each delta into
generated at    ───▶  decision ──▶  sketch / aggregation ──▶ delta sync ──▶ the running sketch;
native rate           (SDK-side)    (one sketch per series)  (small,        reconstruct tumbling-
(e.g. 100ms)                                                 threshold-     window sketches; the
                                                              triggered)     newest window is open
                                                                             and queryable NOW
```

Three mechanisms, three different costs cut:

1. **SDK-side sampling** — not every raw sample is even hashed into
   the sketch. Cuts SDK CPU, SDK↔collector wire bytes, and the
   collector's own hashing/update work.
2. **Sketch-in-collector** — the agent collector maintains the sketch,
   not the backend, so the wire to the backend carries sketch *state*
   (bounded size), not the raw stream.
3. **Delta transmission, continuously** — the collector doesn't wait
   for a window to close and flush a full sketch; it streams small
   per-cell deltas the moment they cross a threshold. This is what
   buys **freshness**: the backend's current window is always
   reconstructable from what's arrived so far, not stale until a
   flush boundary.

None of this requires raw samples to ever leave the edge process for
most metrics — the backend answers from sketch state it continuously
reconstructs, not from a raw-data warehouse. See §7 for the fallback
path (exact archive tier) when a query genuinely needs raw fidelity.

---

## 2. The pipeline, one stage at a time

### SDK: sampling decision

The SDK sees every raw measurement at its native rate (can be far
higher than the metric's nominal scrape/push interval — e.g. samples
every 100ms while the "emit period" is 1s). It decides, per sample,
whether to admit it into the sketch update stream at all — a much
cheaper decision than building the sketch itself. Admitted samples
carry an inverse-probability weight so the sketch stays unbiased. The
sampling math (per-row geometric skip-sampling, why it must decide
*which sketch rows*, not raw items) is its own doc — see
[`design-gos-unified-edge-telemetry.md`](./design-gos-unified-edge-telemetry.md)
§3.

**Why at the SDK, not the collector?** Because the alternative — ship
every raw sample to the collector and sample there — still pays the
serialization/transmission cost SDK-side sampling avoids. Deciding
early is strictly cheaper.

### Agent collector (`asap-otel`): sketch build + continuous sync

`asap-otel` is the **only edge runtime this doc assumes** — the sole
one actively maintained. (An earlier Rust/OTAP variant, `asap-otap`,
exists in history but is unmaintained; see
[`docs/dormant/`](./dormant/).)

Admitted samples are inserted into a per-series sketch/aggregation
(DDSketch, KLL, HLL, Count-Min, Count-Sketch, or a plain running
sum/count — see §6). The collector does **not** batch-and-flush on a
timer as its primary mechanism. Instead, every insert checks whether
the sketch's accumulated-since-last-sync state has crossed a
threshold; if so, that delta gets synced to the backend right away.
This "continuous monitoring" behavior — and the threshold math behind
it — is the subject of
[`design-gos-unified-edge-telemetry.md`](./design-gos-unified-edge-telemetry.md)
§11.

### Backend (`ASAPQuery-backend`): delta apply + tumbling reconstruction

The backend applies each incoming delta into a running merge per
series/cell. Because every family here is additively mergeable, it
doesn't matter how many small deltas arrive or when — summing them
telescopes to the correct cumulative state. The backend buckets this
running state into **tumbling windows**; the **newest window is
always open** — still accepting deltas, and still queryable, with
whatever partial state has landed so far. A query against "now" never
waits for a window-close event; it reads the freshest reconstructed
state directly. Older, closed windows are immutable and archivable.

This is the core freshness claim of the design: **staleness is bounded
by how fast deltas arrive, not by a fixed flush interval.**

---

## 3. Two shapes of query — and why they're handled differently

Not every query is "the same series, over time." ASAP has to support
two structurally different aggregation shapes, and they hit the
pipeline very differently.

### (a) Temporal — continuous monitoring over one series

`rate(m[5m])`, `quantile_over_time(...)`, a dashboard panel tracking
one series across a tumbling window. This is exactly the model in §2:
one series' sketch, continuously synced, read out of whichever
window(s) the query's time range covers. No cross-agent merge is
needed if that series lives on one collector.

### (b) Spatial — aggregation across label dimensions, at a timestamp

`sum by (region) (m)`, `topk(5, m)` — these group **across series**,
and those series may be produced by *different, independent agent
collectors* (each collector owns a partition of the fleet). Answering
this means merging sketches contributed by multiple collectors, and
whether a merge is even necessary depends on the query's `group by`
granularity — a query that keys by the same label the data is already
partitioned on may need no cross-agent merge at all; one that
collapses across that label does.

**Open question, not yet solved:** merging state from independent
collectors is only correct if their windows line up — i.e. their
notion of "the current tumbling window" must be aligned in wall-clock
time. The pipeline has no explicit clock-skew or cross-collector
window-alignment mechanism today; this is effectively an assumption
that collector clocks are synchronized closely enough to not matter,
which hasn't been stress-tested. Flagged here because it's a real
correctness dependency of the spatial query path, not because it's
handled.

---

## 4. Why this design — mapping mechanism to cost saved

| Mechanism | Cost it cuts | Where |
|---|---|---|
| SDK-side sampling | SDK CPU (fewer hashes), SDK→collector wire bytes, collector hashing/update work | SDK |
| Sketch built at the collector, not the backend | Backend ingest CPU; backend never sees the raw stream | Agent collector |
| Continuous, threshold-triggered delta sync (not periodic full-flush) | Backend→query freshness (no flush-boundary wait); wire bytes (deltas ≪ full sketch state) | Collector → backend |
| Sketch store + tumbling-window reconstruction at the backend | Query latency and query-time compute (no raw scan) | Backend |

The **freshness** win and the **cost** wins come from different
mechanisms and shouldn't be conflated: continuous delta sync is what
buys freshness; sampling and sketching are what buy cost reduction.
A design that only sketched (batch-flush per window) would be
cost-efficient but not fresh; a design that only streamed continuously
without sketching (raw continuous streaming) would be fresh but not
cheap.

---

## 5. Evaluating the win — the fair-baseline framing

A naive comparison against "what OTel/Prometheus does by default" is
not a fair baseline, and inflates ASAP's apparent advantage: standard
OTel push-based collection already **downsamples** — it picks (e.g.)
the last raw sample in each emit period and sends just that one,
discarding the rest, even when the true signal is generated far more
frequently (e.g. every 100ms against a 1s emit period). That's an
accuracy loss baked into the comparison, not a cost ASAP is actually
beating fairly.

The three pipelines to compare:

| Pipeline | What crosses the wire | Signal fidelity |
|---|---|---|
| **OTel default** | 1 sample/emit-period (e.g. 1/1s), picked from a much higher raw rate | Lossy — most raw samples never leave the SDK |
| **Fair raw baseline** | *every* raw sample, at native rate (e.g. every 100ms), to the collector or straight to a backend | Full fidelity, no sampling anywhere |
| **ASAP** | SDK-admitted samples only, inserted into a sketch at the collector, synced as deltas | Bounded-error, tunable via the sampling rate / threshold |

The claim to validate is against the **fair raw baseline**, not
against OTel-default: at comparable signal fidelity (both see the
full 100ms-rate stream), ASAP's sketch store + query engine should
answer queries at lower latency and lower serving cost than computing
the same query directly over the raw high-frequency stream. Comparing
against OTel-default instead would conflate "ASAP is cheaper" with
"OTel-default already threw away most of the data" — a different and
much weaker claim.

---

## 6. Sketch families

| Family | Answers | Merge |
|---|---|---|
| **DDSketch** | Quantiles (relative-error) | Additive |
| **KLL** | Quantiles (rank-error, tighter guarantee) | Compactor-hierarchy merge (no delta variant — always full state) |
| **HLL** | Distinct-cardinality | Register-wise max (idempotent — never "resets" a register, only clears a dirty flag) |
| **Count-Min** | Heavy-hitters / point frequency | Additive |
| **Count-Sketch** | Signed frequency estimates | Additive |
| Sum/Count | Exact aggregates | Additive (degenerate 1-cell case) |

All are typed, schema-ful entries on the wire (modified OTLP, five
extra `Metric.data` variants) — see
[`design-gos-unified-edge-telemetry.md`](./design-gos-unified-edge-telemetry.md)
for the accuracy math and §11's per-family insert-time detection rules.

---

## 7. When the sketch store isn't enough: the archive tier

Some query shapes have no summary realization by design — they need
richer partial state than any single sketch can carry, or an exact
answer (`histogram_quantile`, `absent`, vector matching, etc.). These
fail over to an **exact archive tier**: edge/gateway also write
standard Prometheus-TSDB blocks to object storage (MinIO), served
through a Thanos-compatible query path. This is a parallel path, not a
fallback that raw samples take by default — most metrics never touch
it. See [`design-archive-tier.md`](./design-archive-tier.md).

---

## 8. Controller — planning is separate from this doc

*Who* decides sketch family, parameters, and stage placement is a
separate planning-time concern (a five-layer L1→L5 pipeline, split
between the `ASAPController` library and `ASAPQuery-backend`'s
deployment-specific planner). It doesn't change the dataflow narrative
above — it just decides the sketch family/params each series uses
before any of §2 runs. Detail:
[`control-plane-design.md`](./control-plane-design.md) and
`ASAPQuery-backend/control_plane/docs/design-target-architecture.md`.

---

## 9. Cross-references

- [`design-gos-unified-edge-telemetry.md`](./design-gos-unified-edge-telemetry.md) — the mechanism and math behind §2's sampling and continuous delta sync (error bounds, threshold allocation).
- [`sampling-cdm-gos-derivations.md`](./sampling-cdm-gos-derivations.md) — per-family threshold derivations referenced above.
- [`design-archive-tier.md`](./design-archive-tier.md) — §7's exact tier.
- [`design-asap-edge-framework.md`](./design-asap-edge-framework.md) — `asap-otel` agent design.
- [`docs/dormant/`](./dormant/) — the unmaintained `asap-otap` integration design.
- [`control-plane-design.md`](./control-plane-design.md) — planning-time architecture (§8).
- [`mvp-demo-runbook.md`](./mvp-demo-runbook.md) — how to run the single-host demo end-to-end.
- [`comparison-asap-vs-databricks-pantheon-hydra.md`](./comparison-asap-vs-databricks-pantheon-hydra.md) — positioning vs. related systems.

---

## Glossary

- **Continuous monitoring** — syncing sketch deltas to the backend as
  they cross a threshold, rather than on a fixed flush timer; the
  mechanism that makes the newest tumbling window fresh.
- **Tumbling window** — a fixed, non-overlapping time bucket the
  backend reconstructs a sketch's state into; the newest one is open
  (still accepting deltas) and queryable.
- **Temporal query** — a query over one series' history (rate,
  quantile_over_time, ...); answered from that series' own windowed
  sketch, no cross-agent merge needed.
- **Spatial query** — a query that aggregates across series/labels at
  one timestamp (`sum by (...)`, `topk`); may require merging sketches
  from multiple independent agent collectors, depending on `group by`
  granularity relative to how the fleet is partitioned across
  collectors.
- **Fair raw baseline** — the evaluation baseline that ships *every*
  raw sample at native rate (no SDK sampling), used to isolate ASAP's
  win from the unrelated fact that OTel-default already downsamples.

---

*Maintainer note.* This doc is the narrative reference — what the
pipeline does and why. Mechanism-level detail (accuracy math,
threshold derivations, wire formats, per-layer planning interfaces)
belongs in the linked per-component docs, not here.
