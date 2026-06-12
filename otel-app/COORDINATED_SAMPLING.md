# Producer coordinated warm-sampling

This branch adds three producer (`otel-app`) capabilities used by the
Google-cluster warm-sampling accuracy + coordination evaluation
(`datasets_eval/google_cluster/`).

## 1. `-warm-sample-p` on the replayed warm metric

`-warm-sample-p <p>` admits each warm-gauge datapoint with probability `p`
before it enters the SDK aggregation; the dropped `(1-p)` points are never
sketched, exported, or sent. It already applied to the synthetic
`<metric>_latency_ms`; this branch also applies it to the **trace-replay**
path (`-trace-file`), so the real Google-cluster trace can be thinned at the
producer. Because DDSketch quantiles are rank-preserving, thinning preserves
quantile shape with no `1/p` rescale.

## 2. `-trace-metric-name` + `raw-buffer`/`dd-full` on replay

`-trace-metric-name <name>` overrides the replay gauge name (default
`<metric>_trace`) so the replayed series lands under a name a DDSketch
streaming-config aggregation + the query suite reference
(`google_cluster_2019_cpu_rate`). Combined with `-agg dd-full` and
`-sdk-projection -` (drop all labels) every replayed series merges into one
cell-wide DDSketch — the shape `quantile_over_time(q, metric[30s])` queries.
`-agg raw-buffer` is also supported for the no-aggregation arm.

## 3. CDM coordinated `-warm-sample-p` (coordinator-driven, not static)

With `-coordinator-url <host:port>` the producer becomes a **CDM edge**
(`sample_controller.go`): it imports the nested transport module
`asap-precompute-go/monitor/grpcclient` driving a `monitor.NewEngine`. Each
window it reports its observed warm-metric rate (engine `obsCount`) to the
data-plane monitor coordinator, and its `OnGrant` handler stores the granted
`SampleP`. At each window boundary (never mid-window) it reads
`Engine.GrantedSampleP(aggID)` and sets the live warm-sample-p to that value.

Flags: `-coordinator-url`, `-monitor-agg-id` (must match the coordinator's
`monitors:` entry `agg_id`), `-edge-id` (defaults to `-producer-id`).

The monitored additive value climbs a small fixed increment per candidate so
per-window values stay below a modest coordinator `tau` (grant regime) yet
cross the per-round slack so a Report fires every window — that Report carries
the edge's rate, which the coordinator's `AllocateSampleRates`
(`p_i ∝ √(f_i/rate_i)` plus the ε-coupling floor `1/(1+ε²·rate)`) coordinates
over. With ≥2 edges of skewed rates the hot edge is granted a small `p` and
the quiet edge keeps `p=1`.

### Observed end-to-end (two edges, skewed rates)

    coordinator: monitors: [{agg_id:1, tau:70000, epsilon:0.05, window_ms:30000}]
    HOT  edge (rate ~50000/win): applied granted warm-sample-p 0.0065 (was 1.0)
                                 then 0.0113 as the rate estimate refined
    QUIET edge (rate ~500/win):  kept p=1.0 (never sampled down)

A single edge ⇒ `p=1` by design (no rate vector to coordinate over).

## Build

    cd otel-app && go build -o /tmp/otel-app .
