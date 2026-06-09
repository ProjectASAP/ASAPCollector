# Continuous, Arbitrary-Window Queries over Distributed Sketch Telemetry

> **Design-doc / paper section** — Problem, Requirements, Related Work.
> Distributed sketch telemetry with continuous, arbitrary-window queries
> (edge collection → centralized analytics backend).
> Citation markers like `[cormode2013survey]` are placeholders; full
> references are in the [table at the end](#references).

---

## TL;DR

- **Setting.** A fleet of edge agents each summarize a local metric substream
  into mergeable sketches and ship them to a central backend that answers
  approximate aggregate queries (quantiles, distinct counts, heavy hitters,
  sums) over time windows.
- **Today.** Both sides use *fixed tumbling windows*: the edge emits one
  summary per window; the backend answers only on those boundaries.
- **Goal.** Let the edge emit *continuously* at a fine granularity `τ`, and let
  the backend answer **tumbling**, **sliding ("last T")**, and **arbitrary
  `[a,b]`** window queries — without the edge knowing the query windows ahead
  of time.
- **Core tension.** Finer emission → more query flexibility + fresher data,
  but more edge memory, edge CPU, and network bandwidth. The emission period
  `τ` is the one knob that trades all of these, and it sets a hard floor:
  **the backend can never place a window boundary finer than `τ`.**
- **Key design split.** *Linear* sketches (sum, CMS, Count-Sketch, DDSketch)
  answer any window by a **prefix-sum subtraction** (`O(1)`); *non-linear*
  ones (HLL, KLL) require **merging slices**. The system serves both,
  dispatched per family.

---

## 1. Problem Statement

We target **distributed telemetry analytics**: a large fleet of edge data
sources (services, hosts, sidecar agents) each observes a local substream of
metric samples, and a centralized backend answers analytical queries over the
**union** of all substreams.

The workload is demanding on two dimensions:

| Property | Meaning |
| --- | --- |
| **High cardinality** | Many time series (metric name × label set). |
| **High frequency** | Many samples per series per second. |

Queries are **approximate aggregations** — quantiles, distinct counts, heavy
hitters, sums — expressed over **time windows**.

**Architecture (two-stage).**

1. **Edge** — pre-aggregates each window of local data into mergeable summaries
   (sketches) and transmits *those*, never raw samples.
2. **Backend** — merges per-edge summaries and serves queries.

Today both stages use **tumbling windows of a fixed size**: the edge emits one
summary per window, and the backend answers queries aligned to those same
boundaries.

### The gap we address

Fixed, boundary-aligned tumbling windows are too rigid for monitoring. We want:

- the **edge** to emit summaries *continuously*, at fine granularity; and
- the **backend** to answer three query shapes:

  1. **Tumbling** windows (the easy case).
  2. **Continuous / sliding** windows — "the last `T`, as of now," advancing
     continuously.
  3. **Arbitrary** windows — any `[t_start, t_end]`, any duration `T` —
     ideally without the edge knowing the query windows in advance.

> **Central tension.** Finer emission improves query flexibility and freshness
> but costs **edge memory, edge CPU, and network bandwidth** — the three
> resources we must conserve.

---

## 2. Requirements

Two axes plus a set of cross-cutting requirements the two-stage design forces
on us:

- **[Distributed edge](#21-distributed-edge-resource-efficiency)** — resource
  efficiency under cloud economics.
- **[Query model](#22-continuous-accurate-fresh-queries)** — continuous,
  accurate, fresh.
- **[Cross-cutting](#23-cross-cutting-requirements)** — forced by the design.

### 2.1 Distributed edge: resource efficiency

Edge collection runs **co-located with production workloads**, so its resource
use isn't free overhead — it's stolen from the paying workload and billed at
cloud rates. And cloud pricing is **asymmetric**, which ranks our objectives
more sharply than abstract big-O does:

| Resource | Representative AWS price (on-demand, Graviton, mid-2026) | Implication |
| --- | --- | --- |
| **RAM** | `r7g` ≈ **1.7×** the hourly cost of `c7g` at equal vCPU (`r7g.large` $0.151/hr vs `c7g.large` $0.072/hr) [awsec2pricing] | RAM is the *expensive axis*; a memory floor is doubly costly (scarcer resource **and** pushes nodes to a pricier family). |
| **Egress** | Internet $0.09/GB (first 10 TB/mo); cross-region $0.01–0.02/GB; **cross-AZ $0.01/GB each direction** [awsdatatransfer] | Transmission is billed separately and is often the dominant *marginal* cost. |
| **CPU** | (baseline `c7g`) | The cheaper axis — but still stolen from the workload on the hot path. |

These three prices — **$/GB-RAM-hour, $/vCPU-hour, $/GB-egress** — are exactly
what our memory, computation, and communication objectives optimize, and the
asymmetry between them sets their priority.

#### Memory efficiency

Per-series, per-window sketches multiply: `|series| × S_sketch`. At high
cardinality, a fixed-size per-series sketch (e.g. HLL or KLL) imposes a large
**cardinality-proportional memory floor** on the very nodes running the
application — paid even by trivial-cardinality series, and even before any
query is asked.

> **Requirement.** Edge memory should scale with *useful information*, not
> worst-case structure size:
> - sketches that are **sparse/adaptive** at low cardinality and promote to
>   dense only when warranted; and
> - **bounded total footprint** via cardinality caps with graceful overflow.

#### Aggregation scope (per-series vs. whole-stream)

Scope is a **first-class requirement, not an optimization**, because it changes
the memory *asymptotics*, not just a constant:

| Scope | What it keeps | Memory | Use when |
| --- | --- | --- | --- |
| **Per-series** | one summary per series | `|series| × S_sketch` | the query needs per-label resolution |
| **Whole-stream** | one summary over the union of all matching series | `O(S_sketch)` — *independent of cardinality* | global distinct count, fleet-wide quantile, grand total |

For HLL this is the difference between **~16 KB × N** and **~16 KB** — i.e.
between infeasible and feasible at high cardinality.

> **Requirement.** *Every* family must support both scopes, and the **query**
> (via the control plane) selects between them, so resolution is spent only
> where a query needs it.

#### Computation efficiency

Two cost centers with *opposite* constraints:

| Path | When it runs | Constraint |
| --- | --- | --- |
| **Hot path** (per-sample sketch update) | at full ingest rate, competing with the app for CPU | must be `O(1)`, allocation-free, cache-friendly — overhead a small, predictable fraction of node CPU regardless of cardinality or sample rate |
| **Flush path** (serialize, delta-encode, ship) | once per emission period | may be heavier, but scales with `live_summaries × emission_frequency`, so it trades against freshness |

> **Requirement.** Finer emission must not impose super-linear flush cost, and
> update cost must be **independent of window size** and of how many windows
> the backend will later assemble.

#### Communication efficiency

Transmission is billed (egress) **and** latency-bearing. What crosses the
network must be:

1. **Summaries, never raw samples** — the bandwidth invariant.
2. **Delta-encoded** against state the backend can already reconstruct — so
   steady-state bytes track *change*, not absolute state.
3. **Adaptive** — silent/empty intervals cost nothing; emission can be
   triggered by significant change rather than a fixed clock when freshness
   permits.

> **The `τ` knob.** Communication trades against both **freshness** (emit more
> often ⇒ fresher but costlier) and the backend's **window flexibility** (finer
> slices ⇒ more arbitrary windows answerable, but more bytes). The emission
> granularity `τ` is the single knob that sets this trade — and it sets a hard
> floor: *the backend can place window boundaries no finer than `τ`.*

### 2.2 Continuous, accurate, fresh queries

#### Continuous (standing) queries

Monitoring queries are long-running **"standing" views**, not one-shot
on-demand reads [babcock2002models]. The system continuously refreshes answers
as data arrives rather than recomputing per request — favoring **incremental**
maintenance at the backend and **push**-style emission at the edge.

#### Arbitrary and sliding windows

Beyond fixed tumbling windows we require:

- **Sliding** windows — "last `T` as of now," advancing continuously; and
- **Arbitrary** historical sub-windows over any `[a,b]` and any `T`, composed
  at the backend from the edge's fine slices — **without** the edge
  pre-committing to query windows.

#### Accuracy and its composition

Each summary carries an error bound `ε`. The property that makes a two-stage
hierarchy work:

> **Merging summaries does not compound error.** Assembling a window from `N`
> slices keeps error at the base `ε`, independent of `N` and of merge order —
> the *mergeable summary* guarantee [agarwal2013mergeable].

**Headline promise: exact-to-`τ`.** A window query is answered by assembling
exactly the slices it covers, so the only error is the base sketch error `ε_s`
and boundaries are exact up to `τ`. This holds for any `[a,b]` aligned to `τ`
and is the regime we optimize for.

**Secondary tier: bounded-space.** For the long-retention tail where storing
every slice is infeasible, bounded-space windowing (exponential/smooth
histograms — see [Related Work](#3-related-work)) trades an additive *boundary*
error `ε_w` for sublinear space. Then `ε_w` **adds** to `ε_s`, so the two must
be budgeted jointly. This tier is opt-in; the exact-to-`τ` path is the default
and the core contribution.

#### Freshness / timeliness

We care about end-to-end lag: when a sample is **generated** at the source vs.
when it is **reflected** in an accurate backend answer. Staleness ≈ emission
period `τ` + the backend's late-data grace, and it trades directly against
communication cost.

> **Requirement.** Freshness is an explicit, tunable SLO — and **decoupled
> from the query window size**. A query over a 1-hour window should still see
> data within `τ` of "now," not be an hour stale. This decoupling is a central
> goal of the continuous design.

#### Exactly-once slice application (idempotent ingest)

A **correctness** requirement unique to continuous, delta-based, additive
aggregation — and easy to overlook:

> Summary merge is associative but **not idempotent**. Re-applying the same
> additive slice (sum, CMS/Count-Sketch cells, DDSketch buckets)
> double-counts; even register-max families mis-attribute a slice applied to
> the wrong time bucket.

An at-least-once edge→backend channel — realistic under retries, reconnects,
and failover — will corrupt aggregates unless the backend applies each slice
**exactly once**.

> **Requirement.** Either sequence-numbered slices with backend dedup, or
> content-addressed idempotent slice identity, so retransmission is safe.
> Because this constrains what a "delta" may be (independently identifiable,
> replay-safe), it **shapes the edge emission format directly** and cannot be
> bolted on later.

### 2.3 Cross-cutting requirements

| Requirement | What it demands |
| --- | --- |
| **Query-driven configuration (elasticity)** | The queries determine each edge's sketch family, parameters, window/emission policy, and per-series vs. whole-stream scope. A control plane must (re)configure edges at runtime — no restart, no data loss — as queries change. |
| **Out-of-order / late / clock-skewed data** | Edges have independent clocks; samples and slices arrive late or reordered. Window assembly must tolerate late slices landing in past buckets within a bounded grace, with defined semantics for what falls outside it. |
| **Retention vs. space** | Storing every fine slice for a long horizon is often infeasible. Need bounded storage via multi-resolution roll-up (fine-recent, coarse-old) and/or bounded-space sliding-window structures — with an **explicit, documented** loss of *resolution* (roll-up) or *accuracy* (approximate boundaries) on the long tail. |
| **Query expressiveness across families** | Must serve quantiles (DDSketch/KLL), distinct counts (HLL), heavy hitters (CMS/Count-Sketch), sums — see the linear vs. non-linear split below. |

**Linear vs. non-linear families** — the split that drives the windowing
mechanism:

| Class | Families | Arbitrary-window mechanism | Cost |
| --- | --- | --- | --- |
| **Linear / subtractable** | sum, CMS, Count-Sketch, DDSketch (bucket counts) | prefix-sum **difference** of two cumulative summaries | `O(1)` |
| **Non-linear** | HLL (register max), KLL (selection structure) | **merge** of the covered slices | `O(log n)` via a tree |

> **Requirement.** The system must serve both, **dispatching the window
> mechanism by family**.

---

## 3. Related Work

Our setting sits at the intersection of three lines of work, with **mergeable
summaries** as the connective tissue:

| Line | Question it answers | For us |
| --- | --- | --- |
| [Continuous distributed monitoring](#31-continuous-distributed-monitoring) | how to track `f(union)` cheaply over a fleet | designs the edge→backend channel |
| [Sliding-window model](#32-sliding-window-model) | bounded-space "last `T`" | the long-retention tier + a key lower bound |
| [Arbitrary sub-window / range aggregation](#33-arbitrary-sub-window--range-aggregation) | any `[a,b]`, not just a suffix | the backend store + query structure |
| [Mergeable summaries](#34-mergeable-summaries-the-connective-tissue) | why composing the above is sound | non-compounding error guarantee |

### 3.1 Continuous distributed monitoring

The **continuous distributed monitoring** model [cormode2013survey] formalizes
our exact channel: `k` sites each see a substream; a coordinator must
continuously hold `f(⋃ stream_i)` within `ε` while minimizing total
communication.

- **Distributed functional monitoring** [cormode2008functional] bounds how much
  a site must communicate to keep a function tracked, and shows it is far
  sublinear in the stream for many functions — the formal basis for *"emit on
  significant change, not every update."*
- **Geometric monitoring** [sharfman2006geometric] handles *non-linear*
  functions by decomposing a global threshold into *local* constraints each
  site checks independently, escalating only on violation — the template for an
  edge deciding *locally* whether its slice matters globally.
- **Sketch + prediction tracking** [cormode2005sketching] (closest to our
  "delta" idea): each site holds a sketch and a shared prediction model, and
  transmits only when its true sketch diverges from what the coordinator can
  already reconstruct — an **I-frame/P-frame** discipline for sketches. The
  quantile specialization is [cormode2005holistic].

> **Limitation for us.** This line optimizes tracking a *single current value*
> and deliberately discards the temporal decomposition — so on its own it does
> **not** answer arbitrary historical windows. That motivates the next two
> lines.

### 3.2 Sliding-window model

Maintains an aggregate over the last `N` items / last `T` time, forgetting
older data.

- **Exponential Histograms (EH)** [datar2002sliding] — the foundation:
  counts/sums within `ε` in `O((1/ε) log² N)` bits, **with a matching lower
  bound** (exact sliding-window sums need `Ω((1/ε) log² N)` bits). This is the
  result that forces our *"exact-to-`τ` **or** bounded-space"* fork.
- **Waves** [gibbons2002distributed] — sliding-window counting in the
  *distributed* setting.
- **Smooth histograms** [braverman2007smooth] — generalize EH to "smooth"
  functions (Lp norms, frequency moments, many sketchable quantities), keeping
  `O((1/ε) log N)` sketch instances at chosen window-start points so any suffix
  is covered. The principal theoretical tool for bounded-space sliding windows
  *over sketches*.
- **Deterministic frequent-items / quantiles** [arasu2004quantiles,
  lee2006significant].
- **Sketch-specific window variants** (most directly reusable):
  - **ECM-sketch** [papapetrou2012ecm] — Count-Min where each cell is an
    Exponential Histogram ⇒ distributed sliding-window range-frequency queries.
  - **Sliding HyperLogLog** [chabchoub2010sliding] — stores recent
    `(timestamp, ρ)` per register to recompute the max over any suffix,
    confronting head-on that HLL's register max is **not** invertible.

### 3.3 Arbitrary sub-window / range aggregation

Answering *any* `[a,b]` pushes from "sliding-window structure" to
"range-aggregate over time-partitioned partials."

- **Panes** [li2005nopane] — chop time into fixed slices, partial-aggregate
  each, answer any window as a merge of covered slices. (Origin of our
  `τ`-slice idea.)
- **Stream slicing** — **Cutty** [carbone2016cutty] and especially **Scotty**
  [traub2021scotty] — manage many concurrent windows of different sizes over
  out-of-order streams with watermarks. Scotty is the closest existing
  *system* to "arbitrary window sizes, answered continuously."
- **Incremental aggregation structures** (make the merges cheap; need only an
  associative monoid, which sketch-merge is):

  | Structure | Strength |
  | --- | --- |
  | **FlatFAT** [tangwongsan2015flatfat] | balanced tree, `O(log n)` update, **arbitrary `[a,b]` range queries** |
  | **DABA** [tangwongsan2017daba] | `O(1)` worst-case for the suffix ("last `T`") case |
  | **FiBA** [tangwongsan2019fiba] | **out-of-order** inserts + arbitrary window queries |

- **Time-decay** [cohen2003timedecay, cormode2009forwarddecay] — weight data by
  age instead of hard windows; often a better and far cheaper model for
  dashboards.
- **Persistent prefix-sum / Fenwick** — for the *linear* families, collapses
  arbitrary `[a,b]` to a single **subtraction** of two cumulative summaries.

### 3.4 Mergeable summaries: the connective tissue

What makes all of the above compose safely is **mergeability**
[agarwal2013mergeable]: KLL [karnin2016kll], HyperLogLog
[flajolet2007hyperloglog, ertl2017new], Count-Min [cormode2005cm], Count-Sketch
[charikar2002frequent], and DDSketch [masson2019ddsketch] can each be merged in
*any* tree shape without compounding error. This licenses assembling a window
from `O(log n)` slice-merges with error fixed at the base `ε`, regardless of
how the range structure groups slices — without which arbitrary-window assembly
over sketches would be unsound.

### 3.5 Positioning

Prior work supplies the pieces but not the combination we need:

| Prior line | What it gives | What it lacks for us |
| --- | --- | --- |
| Continuous monitoring | cheap channel tracking | tracks one current value; no temporal decomposition for history |
| Sliding-window theory | bounded-space "last `T`" | suffix-oriented; largely single-node or count-centric |
| Stream-slicing systems | arbitrary windows, many sizes | built for raw/SQL aggregates in one engine — not distributed, bandwidth-billed *sketch* telemetry with a linear/non-linear split |

> **Our contribution.** A two-stage design that **(i)** emits fine,
> continuous, delta-compressed *temporal slice-sketches* from
> resource-constrained edges under cloud economics, and **(ii)** serves
> tumbling, sliding, and arbitrary-window queries at the backend by
> dispatching, *per sketch family*, between **prefix-difference** (linear) and
> **tree-based range-merge** (non-linear) over a multi-resolution slice store —
> with mergeability guaranteeing non-compounding error across the hierarchy.

---

## References

Citation keys are placeholders; fill in full bibliographic entries when binding
into the paper. Each title links directly to the paper (DOI where available,
else the official PDF / arXiv).

- `awsec2pricing` — Amazon, [EC2 On-Demand Pricing](https://aws.amazon.com/ec2/pricing/on-demand/) (r7g vs c7g per-vCPU price; accessed 2026-06).
- `awsdatatransfer` — Amazon, [EC2 / VPC Data Transfer Pricing](https://aws.amazon.com/vpc/pricing/) (egress, cross-AZ, cross-region; accessed 2026-06).
- `babcock2002models` — Babcock et al., [Models and Issues in Data Stream Systems](https://doi.org/10.1145/543613.543615), PODS 2002.
- `cormode2013survey` — Cormode, [The Continuous Distributed Monitoring Model](https://doi.org/10.1145/2481528.2481530), SIGMOD Record 2013.
- `cormode2008functional` — Cormode, Muthukrishnan, Yi, [Algorithms for Distributed Functional Monitoring](https://doi.org/10.1145/1921659.1921667), SODA 2008 / ACM TALG 2011.
- `sharfman2006geometric` — Sharfman, Schuster, Keren, [A Geometric Approach to Monitoring Threshold Functions over Distributed Data Streams](https://doi.org/10.1145/1142473.1142508), SIGMOD 2006.
- `cormode2005sketching` — Cormode, Garofalakis, [Sketching Streams Through the Net](http://www.vldb.org/archives/website/2005/program/paper/tue/p13-cormode.pdf), VLDB 2005.
- `cormode2005holistic` — Cormode et al., [Holistic Aggregates in a Networked World](https://doi.org/10.1145/1066157.1066161) (quantile tracking), SIGMOD 2005.
- `datar2002sliding` — Datar, Gionis, Indyk, Motwani, [Maintaining Stream Statistics over Sliding Windows](https://doi.org/10.1137/S0097539701398363), SODA / SICOMP 2002 (Exponential Histograms).
- `gibbons2002distributed` — Gibbons, Tirthapura, [Distributed Streams Algorithms for Sliding Windows](https://doi.org/10.1145/564870.564880), SPAA 2002.
- `braverman2007smooth` — Braverman, Ostrovsky, [Smooth Histograms for Sliding Windows](https://doi.org/10.1109/FOCS.2007.55), FOCS 2007.
- `arasu2004quantiles` — Arasu, Manku, [Approximate Counts and Quantiles over Sliding Windows](https://doi.org/10.1145/1055558.1055598), PODS 2004.
- `lee2006significant` — Lee, Ting, [Maintaining Significant Stream Statistics over Sliding Windows](https://dl.acm.org/doi/10.5555/1109557.1109636), SODA 2006.
- `papapetrou2012ecm` — Papapetrou, Garofalakis, Deligiannakis, [Sketch-based Querying of Distributed Sliding-Window Data Streams](https://doi.org/10.14778/2336664.2336672) (ECM-sketch), VLDB 2012.
- `chabchoub2010sliding` — Chabchoub, Hébrail, [Sliding HyperLogLog](https://doi.org/10.1109/ICDMW.2010.18), ICDM Workshops 2010.
- `li2005nopane` — Li et al., [No Pane, No Gain](https://doi.org/10.1145/1058150.1058158), SIGMOD Record 2005.
- `carbone2016cutty` — Carbone et al., [Cutty: Aggregate Sharing for User-Defined Windows](https://doi.org/10.1145/2983323.2983807), CIKM 2016.
- `traub2021scotty` — Traub et al., [Scotty: Efficient Window Aggregation for Out-of-Order Stream Processing](https://doi.org/10.1145/3433675), ICDE 2018 / ACM TODS 2021.
- `tangwongsan2015flatfat` — Tangwongsan et al., [General Incremental Sliding-Window Aggregation](https://doi.org/10.14778/2752939.2752940) (FlatFAT), VLDB 2015.
- `tangwongsan2017daba` — Tangwongsan et al., [Low-Latency Sliding-Window Aggregation in Worst-Case Constant Time](https://doi.org/10.1145/3093742.3093925) (DABA), DEBS 2017.
- `tangwongsan2019fiba` — Tangwongsan et al., [Optimal and General Out-of-Order Sliding-Window Aggregation](https://doi.org/10.14778/3339490.3339499) (FiBA), VLDB 2019.
- `cohen2003timedecay` — Cohen, Strauss, [Maintaining Time-Decaying Stream Aggregates](https://doi.org/10.1145/773153.773175), PODS 2003.
- `cormode2009forwarddecay` — Cormode et al., [Forward Decay](https://doi.org/10.1109/ICDE.2009.65), ICDE 2009.
- `agarwal2013mergeable` — Agarwal et al., [Mergeable Summaries](https://doi.org/10.1145/2500128), PODS 2012 / ACM TODS 2013.
- `karnin2016kll` — Karnin, Lang, Liberty, [Optimal Quantile Approximation in Streams](https://doi.org/10.1109/FOCS.2016.17) (KLL), FOCS 2016.
- `flajolet2007hyperloglog` — Flajolet et al., [HyperLogLog](https://doi.org/10.46298/dmtcs.3545), AofA 2007.
- `ertl2017new` — Ertl, [New Cardinality Estimation Algorithms for HyperLogLog Sketches](https://arxiv.org/abs/1702.01284), arXiv:1702.01284.
- `cormode2005cm` — Cormode, Muthukrishnan, [An Improved Data Stream Summary: The Count-Min Sketch and its Applications](https://doi.org/10.1016/j.jalgor.2003.12.001), J. Algorithms 2005.
- `charikar2002frequent` — Charikar, Chen, Farach-Colton, [Finding Frequent Items in Data Streams](https://doi.org/10.1007/3-540-45465-9_59) (Count Sketch), ICALP 2002.
- `masson2019ddsketch` — Masson, Rim, Lee, [DDSketch](https://doi.org/10.14778/3352063.3352135), VLDB 2019.
