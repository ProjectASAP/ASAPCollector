# Continuous, Arbitrary-Window Queries over Distributed Sketch Telemetry

> Design-doc / paper section: Problem, Requirements, Related Work.
> Topic: distributed sketch telemetry with continuous, arbitrary-window
> queries (edge collection → centralized analytics backend).
> Citation markers `[key]` are placeholders; full references are listed at
> the end.

## Problem Statement

We target *distributed telemetry analytics*: a large fleet of edge data
sources (services, hosts, sidecar agents) each observes a local substream of
metric samples, and a centralized backend answers analytical queries over the
*union* of all substreams. The metrics are high-cardinality (many time series
= metric name × label set) and high-frequency (many samples per series per
second). Queries are *approximate aggregations* — quantiles, distinct counts,
heavy hitters, sums — expressed over *time windows*.

The architecture is two-stage. Edges *pre-aggregate* each window of their
local data into mergeable summaries (sketches) and transmit those, not raw
samples; the backend *merges* per-edge summaries and serves queries. Today
both stages operate on *tumbling* windows of a fixed size: the edge emits one
summary per window, and the backend answers queries aligned to those same
boundaries.

**The gap we address.** Fixed, boundary-aligned tumbling windows are
restrictive for monitoring. We want the edge to emit summaries *continuously*
at a fine granularity, and the backend to answer **(i)** tumbling-window
queries (the easy case), **(ii)** *continuously* updated window queries ("the
last *T*, as of now"), and **(iii)** *arbitrary* window queries over any
`[t_start, t_end]` and any duration *T*, ideally without the edge knowing the
query windows in advance. The central tension is that finer emission improves
query flexibility and freshness but costs edge memory, edge CPU, and network
bandwidth — the three resources we must conserve.

## Requirements

We organize requirements along two axes: the *distributed edge* (resource
efficiency under cloud economics) and the *query model* (continuous, accurate,
fresh). We then add cross-cutting requirements that the two-stage,
transmit-summaries design forces upon us.

### Distributed edge: resource efficiency

Edge collection runs *co-located with production workloads* in the cloud, so
its resource use is not free overhead — it is stolen from the paying workload
and billed at cloud rates. Cloud pricing is *asymmetric* in ways that rank our
objectives more sharply than abstract big-O does. On AWS EC2 (on-demand,
Graviton, mid-2026), a memory-optimized `r7g` instance costs roughly 1.7× the
hourly price of a compute-optimized `c7g` with the same vCPU count (e.g.
`r7g.large` $0.151/hr vs `c7g.large` $0.072/hr) [awsec2pricing]: *RAM is the
expensive axis*, so a memory floor on co-located agents is doubly costly — it
both consumes the scarcer resource and pushes nodes toward the pricier
instance family. Transmission is billed separately and is frequently the
dominant *marginal* cost: internet egress is $0.09/GB (first 10 TB/mo),
cross-region $0.01–0.02/GB, and even cross-AZ traffic $0.01/GB *in each
direction* [awsdatatransfer]. These three prices — $/GB-RAM-hour,
$/vCPU-hour, $/GB-egress — are what our memory, computation, and
communication objectives respectively optimize, and the asymmetry between them
sets their priority.

**Memory efficiency.** Per-series, per-window sketches multiply:
`|series| × S_sketch`. At high cardinality a per-series quantile/cardinality
sketch (e.g. a fixed-size HLL or KLL) imposes a large, *cardinality-
proportional* memory floor on the very nodes running the application. The
floor is paid even by series of trivial cardinality, and even before any query
is asked. We require that edge memory scale with *useful information*, not with
worst-case structure size: sketches that are sparse/adaptive at low
cardinality and promote to dense only when warranted; and bounded total
footprint via cardinality caps with graceful overflow.

**Aggregation scope (per-series vs. whole-stream).** We treat the aggregation
*scope* as a first-class *requirement*, not an optional optimization, because
it changes the memory *asymptotics* rather than a constant factor. In
*per-series* scope the edge keeps one summary per series, so memory is
`|series| × S_sketch`; in *whole-stream* scope a single summary tracks the
union of all matching series, so memory is `O(S_sketch)` — independent of
cardinality. For a query that does not need per-label resolution (a global
distinct count, a fleet-wide quantile, a grand total), whole-stream is the
difference between feasible and infeasible at high cardinality (e.g. for HLL,
~16 KB × N vs ~16 KB). We therefore require that *every* family support both
scopes and that the *query* select between them (via the control plane), so
resolution is spent only where a query needs it.

**Computation efficiency.** Two cost centers, with different constraints.
*(a) The per-sample hot path* (sketch update) runs at the full ingest rate and
competes directly with the application for CPU; it must be `O(1)`,
allocation-free, and cache-friendly, so that telemetry overhead is a small,
predictable fraction of node CPU regardless of cardinality or sample rate.
*(b) The flush/emit path* (serialize, delta-encode, ship) runs once per
emission period and can be heavier, but its cost scales with the number of
live summaries and the emission frequency, so it trades directly against
freshness (below). We require that finer emission not impose super-linear
flush cost, and that update cost be independent of window size and of how many
windows the backend will later assemble.

**Communication efficiency.** Transmission is billed (egress) *and*
latency-bearing. We require that what crosses the network be (i) *summaries,
never raw samples* — the bandwidth invariant; (ii) *delta-encoded* against
state the backend can already reconstruct, so steady-state bytes track
*change* rather than absolute state; and (iii) *adaptive* — silent/empty
intervals cost nothing, and emission can be triggered by significant change
rather than a fixed clock when freshness permits. Crucially, communication
trades against both freshness (emit more often ⇒ fresher but costlier) and the
backend's window flexibility (finer temporal slices ⇒ more arbitrary windows
answerable, but more bytes). The emission granularity τ is the single knob
that sets this trade, and it also sets a hard floor: *the backend can place
window boundaries no finer than τ.*

### Continuous, accurate, fresh queries

**Continuous (standing) queries.** Monitoring queries are long-running
"standing" views, not one-shot on-demand reads [babcock2002models]. The system
maintains and continuously refreshes answers as data arrives, rather than
recomputing from scratch per request. This favors *incremental* maintenance at
the backend and *push*-style emission at the edge.

**Arbitrary and sliding windows.** Beyond fixed tumbling windows we require
*sliding* windows ("last *T* as of now", advancing continuously) and
*arbitrary* historical sub-window queries over any `[a,b]` and any *T*,
composed at the backend from the edge's fine-grained slices — without the edge
pre-committing to the query windows.

**Accuracy and its composition.** Each summary carries an error bound (ε). In
a two-stage hierarchy the key property is that *merging summaries does not
compound error*: assembling a window from *N* slices must keep error at the
base ε, independent of *N* and of the merge order. This is exactly the
*mergeable summary* guarantee [agarwal2013mergeable] and it is what makes
arbitrary-window assembly viable.

Our *headline accuracy promise is exact-to-τ*: a window query is answered by
assembling exactly the slices it covers, so the only error is the base sketch
error ε_s and window boundaries are exact up to the emission granularity τ.
This holds for any `[a,b]` aligned to τ and is the regime we optimize for.
Bounded-space windowing ([Related Work](#related-work), exponential/smooth
histograms) is a *secondary tier* for the long-retention tail where storing
every slice is infeasible: there the system trades an additive *boundary*
error ε_w for sublinear space, and ε_w *adds* to ε_s, so the two must be
budgeted jointly. The default and the core contribution is the exact-to-τ
path; the approximate-boundary tier is an opt-in for horizons that cannot
afford full slice retention.

**Freshness / timeliness.** We care about the end-to-end lag between when a
sample is *generated* at the source and when it is *reflected* in an accurate
backend answer. This staleness is dominated by the emission period τ plus the
backend's late-data grace, and it trades directly against communication cost.
Freshness should be an explicit, tunable SLO, not an accident of the window
size — and, crucially, *decoupled* from the query window size: a query over a
1-hour window should still see data within τ of "now," not be an hour stale.
This decoupling of freshness from window size is a central goal of the
continuous design.

**Exactly-once slice application (idempotent ingest).** This is a
*correctness* requirement unique to continuous, delta-based, additive
aggregation, and it is easy to overlook. Summary merge is associative but
*not idempotent*: re-applying the same additive slice (sum, CMS/Count-Sketch
cells, DDSketch buckets) double-counts, and even register-max families
mis-attribute a slice applied to the wrong time bucket. An at-least-once
edge→backend channel — the realistic assumption under retries, reconnects, and
failover — will therefore corrupt aggregates unless the backend applies each
slice *exactly once*. We require either sequence-numbered slices with backend
deduplication, or content-addressed idempotent slice identity, so that
retransmission is safe. Because this constrains what a "delta" may be (it must
be independently identifiable and replay-safe), it shapes the edge emission
format directly and cannot be bolted on after the fact.

### Cross-cutting requirements (forced by the design)

- **Query-driven configuration (elasticity).** The set of queries determines
  what each edge must compute: sketch family, parameters, window/emission
  policy, and per-series vs. whole-stream scope. A control plane must
  (re)configure edges at runtime — without restart or data loss — as queries
  arrive and change.

- **Out-of-order, late, and clock-skewed data.** Edges have independent
  clocks; samples and slices can arrive late or reordered. Window assembly
  must tolerate late slices landing in past time buckets within a bounded
  grace, with defined semantics for what falls outside it.

- **Retention vs. space.** Storing every fine slice for a long horizon is
  often infeasible. We require bounded backend storage via multi-resolution
  roll-up (fine-recent, coarse-old) and/or bounded-space sliding-window
  structures, with an explicit, documented loss of *resolution* (roll-up) or
  *accuracy* (approximate boundaries) on the long tail.

- **Query expressiveness across families.** The aggregations we support —
  quantiles (DDSketch/KLL), distinct counts (HLL), heavy hitters/frequencies
  (CMS/Count Sketch), sums — split into *linear/subtractable* families (sum,
  CMS, Count Sketch, DDSketch bucket counts), for which arbitrary windows
  reduce to a prefix-sum *difference*, and *non-linear* families (HLL's
  register max, KLL's selection structure), for which windows require
  *merging* slices. The system must serve both, dispatching the window
  mechanism by family.

## Related Work

Our setting sits at the intersection of three lines of work: continuous
distributed monitoring (the edge→backend channel), the sliding-window
streaming model (bounded-space "last *T*"), and arbitrary sub-window / range
aggregation (any `[a,b]`). Mergeable summaries are the connective tissue that
lets the three compose.

### Continuous distributed monitoring

The *continuous distributed monitoring* model [cormode2013survey] formalizes
our exact channel: *k* sites each see a substream, a coordinator must
continuously hold `f(⋃ stream_i)` within ε while minimizing total
communication. The theory of *distributed functional monitoring*
[cormode2008functional] bounds how much a site must communicate to keep a
function tracked, and shows that for many functions communication is far
sublinear in the stream — the formal basis for "emit on significant change,
not every update." For *non-linear* functions, *geometric monitoring*
[sharfman2006geometric] decomposes a global threshold condition into *local*
constraints each site checks independently, escalating only on violation; this
is the template for an edge deciding *locally* whether its slice matters
globally. Closest to our "delta" idea, sketch-plus-prediction tracking
[cormode2005sketching] has each site hold a sketch and a shared prediction
model, transmitting only when its true sketch diverges from what the
coordinator can already reconstruct — an I-frame/P-frame discipline for
sketches; the quantile specialization [cormode2005holistic] tracks holistic
aggregates the same way. This line optimizes tracking of a *single current
value*; it deliberately discards the temporal decomposition, so on its own it
does *not* answer arbitrary historical windows — motivating the next two
lines.

### Sliding-window model

The sliding-window model maintains an aggregate over the last *N* items / last
*T* time, forgetting older data. The foundational result — the Exponential
Histogram (EH) [datar2002sliding] — maintains counts/sums within ε in
`O((1/ε) log² N)` bits and proves a *matching lower bound*: exact
sliding-window sums require `Ω((1/ε) log² N)` bits. This is the result that
forces our "exact-to-τ *or* bounded-space" fork. Waves [gibbons2002distributed]
port sliding-window counting to the distributed setting. *Smooth histograms*
[braverman2007smooth] generalize EH to the broad class of "smooth" functions
(Lp norms, frequency moments, many sketchable quantities), maintaining
`O((1/ε) log N)` sketch instances at chosen window-start points so any suffix
is covered — the principal theoretical tool for bounded-space sliding windows
*over sketches*. Deterministic schemes target frequent items and quantiles
specifically [arasu2004quantiles, lee2006significant]. Most directly reusable
are sketch-specific window variants: the ECM-sketch [papapetrou2012ecm]
replaces each Count-Min cell with an Exponential Histogram, giving distributed
sliding-window range-frequency queries; and Sliding HyperLogLog
[chabchoub2010sliding] stores recent `(timestamp, ρ)` per register to
recompute the max over any suffix — confronting head-on that HLL's register
max is *not* invertible.

### Arbitrary sub-window / range aggregation

Answering *any* `[a,b]`, not just a suffix of now, pushes from "sliding-window
structure" to "range-aggregate over time-partitioned partials." The *panes*
technique [li2005nopane] chops time into fixed slices, partial-aggregates
each, and answers any window as a merge of covered slices. General *stream
slicing* — Cutty [carbone2016cutty] and especially Scotty [traub2021scotty] —
manages many concurrent windows of different sizes over out-of-order streams
with watermarks, and is the closest existing *system* to "arbitrary window
sizes, answered continuously." To make the merges cheap, incremental
sliding-window aggregation provides associative-combine data structures:
FlatFAT [tangwongsan2015flatfat] is a balanced tree over slices with
`O(log n)` update and *arbitrary `[a,b]` range queries*; DABA
[tangwongsan2017daba] gives `O(1)` worst-case for the suffix case; and FiBA
[tangwongsan2019fiba] extends to *out-of-order* inserts with arbitrary window
queries. All require only that the aggregation be an associative monoid —
which sketch-merge is — so they apply to our non-linear families directly. As
an alternative to hard windows, *time-decayed* aggregation [cohen2003timedecay,
cormode2009forwarddecay] weights data by age, often a better and far cheaper
model for monitoring dashboards. For the *linear* families, the classical
persistent prefix-sum / Fenwick lineage collapses arbitrary `[a,b]` to a
single *subtraction* of two cumulative summaries.

### Mergeable summaries: the connective tissue

What makes the above compose safely is *mergeability* [agarwal2013mergeable]:
KLL [karnin2016kll], HyperLogLog [flajolet2007hyperloglog, ertl2017new],
Count-Min [cormode2005cm], Count Sketch [charikar2002frequent], and DDSketch
[masson2019ddsketch] can each be merged in *any* tree shape without
compounding error. This licenses assembling a window from `O(log n)`
slice-merges with error fixed at the base ε, regardless of how the range
structure groups slices — the property without which arbitrary-window assembly
over sketches would be unsound.

### Positioning

Prior work supplies the pieces but not the combination we need. Continuous
monitoring optimizes the channel but tracks a single current value, discarding
the temporal decomposition needed for historical windows. Sliding-window
theory bounds space for "last *T*" but is suffix-oriented and largely
single-node or count-centric. Stream-slicing systems manage arbitrary windows
but are designed for raw/SQL aggregates within one engine, not for
*distributed, bandwidth-billed, sketch* telemetry with a per-family
linear/non-linear split. Our contribution is a two-stage design that (i) emits
fine, continuous, delta-compressed *temporal slice-sketches* from
resource-constrained edges under cloud economics, and (ii) serves tumbling,
sliding, and arbitrary-window queries at the backend by dispatching, *per
sketch family*, between prefix-difference (linear) and tree-based range-merge
(non-linear) over a multi-resolution slice store — with mergeability
guaranteeing non-compounding error across the hierarchy.

## References

Citation keys above are placeholders; fill in full bibliographic entries when
binding into the paper.

| Key | Reference |
| --- | --- |
| `awsec2pricing` | Amazon EC2 On-Demand Pricing, <https://aws.amazon.com/ec2/pricing/on-demand/> (r7g vs c7g per-vCPU price; accessed 2026-06). |
| `awsdatatransfer` | Amazon EC2 / VPC Data Transfer Pricing, <https://aws.amazon.com/vpc/pricing/> and <https://aws.amazon.com/ec2/pricing/on-demand/> (egress, cross-AZ, cross-region; accessed 2026-06). |
| `babcock2002models` | Babcock et al., Models and Issues in Data Stream Systems, PODS 2002. |
| `cormode2013survey` | Cormode, The Continuous Distributed Monitoring Model, SIGMOD Record 2013. |
| `cormode2008functional` | Cormode, Muthukrishnan, Yi, Algorithms for Distributed Functional Monitoring, SODA 2008 / ACM TALG 2011. |
| `sharfman2006geometric` | Sharfman, Schuster, Keren, A Geometric Approach to Monitoring Threshold Functions over Distributed Data Streams, SIGMOD 2006. |
| `cormode2005sketching` | Cormode, Garofalakis, Sketching Streams Through the Net, VLDB 2005. |
| `cormode2005holistic` | Cormode et al., Holistic Aggregates in a Networked World (quantile tracking), SIGMOD 2005. |
| `datar2002sliding` | Datar, Gionis, Indyk, Motwani, Maintaining Stream Statistics over Sliding Windows, SODA / SICOMP 2002 (Exponential Histograms). |
| `gibbons2002distributed` | Gibbons, Tirthapura, Distributed Streams Algorithms for Sliding Windows, SPAA 2002. |
| `braverman2007smooth` | Braverman, Ostrovsky, Smooth Histograms for Sliding Windows, FOCS 2007. |
| `arasu2004quantiles` | Arasu, Manku, Approximate Counts and Quantiles over Sliding Windows, PODS 2004. |
| `lee2006significant` | Lee, Ting, Maintaining Significant Stream Statistics over Sliding Windows, SODA 2006. |
| `papapetrou2012ecm` | Papapetrou, Garofalakis, Deligiannakis, Sketch-based Querying of Distributed Sliding-Window Data Streams (ECM-sketch), VLDB 2012. |
| `chabchoub2010sliding` | Chabchoub, Hébrail, Sliding HyperLogLog, 2010. |
| `li2005nopane` | Li et al., No Pane, No Gain, SIGMOD Record 2005. |
| `carbone2016cutty` | Carbone et al., Cutty, CIKM 2016. |
| `traub2021scotty` | Traub et al., Scotty: Efficient Window Aggregation for Out-of-Order Stream Processing, ICDE 2018 / ACM TODS 2021. |
| `tangwongsan2015flatfat` | Tangwongsan et al., General Incremental Sliding-Window Aggregation (FlatFAT), VLDB 2015. |
| `tangwongsan2017daba` | Tangwongsan et al., Low-Latency Sliding-Window Aggregation in Worst-Case Constant Time (DABA), DEBS 2017. |
| `tangwongsan2019fiba` | Tangwongsan et al., Optimal and General Out-of-Order Sliding-Window Aggregation (FiBA), VLDB 2019. |
| `cohen2003timedecay` | Cohen, Strauss, Maintaining Time-Decaying Stream Aggregates, PODS 2003. |
| `cormode2009forwarddecay` | Cormode et al., Forward Decay, ICDE 2009. |
| `agarwal2013mergeable` | Agarwal et al., Mergeable Summaries, PODS 2012 / ACM TODS 2013. |
| `karnin2016kll` | Karnin, Lang, Liberty, Optimal Quantile Approximation in Streams (KLL), FOCS 2016. |
| `flajolet2007hyperloglog` | Flajolet et al., HyperLogLog, AofA 2007. |
| `ertl2017new` | Ertl, New Cardinality Estimation Algorithms for HyperLogLog Sketches, arXiv:1702.01284. |
| `cormode2005cm` | Cormode, Muthukrishnan, Count-Min Sketch, J. Algorithms 2005. |
| `charikar2002frequent` | Charikar, Chen, Farach-Colton, Finding Frequent Items in Data Streams (Count Sketch), ICALP 2002. |
| `masson2019ddsketch` | Masson, Rim, Lee, DDSketch, VLDB 2019. |
