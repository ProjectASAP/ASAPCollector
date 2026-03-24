# Control Plane Design: DataCollector Controller

## Overview

The controller is the "brain" that observes the query workload and drives configuration across the entire pipeline — from agent OTel collectors to SimpleStore precomputation — to minimize bandwidth and latency while meeting accuracy SLAs.

## System Architecture

```
                     ┌──────────────────────────────────┐
                     │      User Query Registry          │
                     │      (ASAPQuery workload)         │
                     └────────────────┬─────────────────┘
                                      │ query analysis
                     ┌────────────────▼─────────────────┐
                     │           CONTROLLER              │
                     │   • Query Analyzer                │
                     │   • Cost Model                    │
                     │   • Plan Generator                │
                     │     (asap-planner-rs)             │
                     └──┬──────────┬──────────┬────────┬─┘
                OpAMP   │   OpAMP  │   OpAMP  │        │ precompute API
                        │          │          │        │
          ┌─────────────▼─┐  ┌─────▼───┐  ┌──▼──────┐ ┌▼──────────────────┐
          │ Agent OTel    │  │ Gateway │  │ Backend │ │ ASAPQuery Precomp │
          │ Collector     │  │         │  │Collector│ │ Engine            │
          │ • sketch type │  │         │  │         │ │                   │
          │ • aggregate_by│  │         │  │         │ │                   │
          │ • window_dur  │  │         │  │         │ │                   │
          │ • raw/sketch  │  │         │  │         │ │                   │
          └───────┬───────┘  └────▲────┘  └────▲────┘ └─────────▲─────────┘
                  │               │             │                │
                  └──── OTLP ─────┘             │                │
                                  └─── OTLP ────┘                │
                                                └── sketches ────┘
                                                                  │
                                                                  ▼
                                                            SimpleStore
```

---

## Decision Space

The controller makes **four coupled decisions** per metric × query workload.

### 1. Agent Collector: Sketch vs. Raw

| Condition | Decision |
|---|---|
| Query needs only coarse aggregation (e.g., p99 across all hosts) | Sketch at agent, `aggregate_by: []` |
| Query needs per-dimension breakdown (e.g., p99 per `host.name`) | Sketch at agent, `aggregate_by: [host.name]` |
| Query requires exact values, point queries (e.g., `metric{host="h1"}` at a specific timestamp), or anomaly detection over individual samples | Raw samples |
| Bandwidth budget is tight, accuracy SLA is loose | Sketch, larger window |
| Low-latency SLA where sketch window would delay | Raw or `mode: batch` |

### 2. Agent Collector: Sketch Type Selection

| Query type | Sketch |
|---|---|
| Quantile (p50, p99, ...) | DDSketch (bounded error) or KLL (simpler, compact) |
| Cardinality (count distinct) | HLL |
| Frequency / heavy hitters | CountSketch or Count-Min Sketch |
| Multiple query types on same metric | Parallel sketch processors (one per type) |

DDSketch is preferred for quantiles when accuracy SLA is strict (it guarantees relative error); KLL when compactness matters more.

### 3. Dimension Aggregation Strategy

The key tension: **higher `aggregate_by` granularity = more sketches = more bandwidth, but finer query capability**. The controller decides the minimal set of dimensions to preserve.

```
User query: quantile_over_time(0.99, latency{service="web"}[5m]) by (host)
  → aggregate_by: [host.name, service.name]  (need both to filter + group)
  → window_duration: 5m                      (matches query window)

User query: quantile_over_time(0.99, latency[1h])  (per series, no grouping clause)
  → aggregate_by: [all label dims]           (one sketch per time series — finest granularity)
  → window_duration: 1h or batch + merge at backend
```

### 4. Time Window Strategy

| Mode | When to use |
|---|---|
| `window` | Query window is known in advance, long aggregation (1m–1h) |
| `batch` | Fine-grained streaming; gateway/backend merges later |
| Backend-side window | Agent uses `batch`, backend collector accumulates into larger window |

---

## Controller Architecture

### Directory Layout

```
controller/
├── cmd/
│   └── controller/main.go          # gRPC server + OpAMP server
├── internal/
│   ├── analyzer/
│   │   └── query_analyzer.go       # parse PromQL/ASAPQuery workload → QueryWorkload
│   ├── planner/
│   │   ├── planner.go              # calls asap-planner-rs (subprocess or FFI)
│   │   ├── cost_model.go           # bandwidth/CPU/accuracy estimates from benchmark data
│   │   └── rules.go                # rule-based fallback (sketch type selection)
│   ├── config/
│   │   ├── agent_config.go         # build agent OTel YAML from CollectionPlan
│   │   ├── backend_config.go       # build backend collector YAML
│   │   └── precompute_config.go    # build ASAPQuery precompute job spec
│   ├── opamp/
│   │   └── server.go               # OpAMP server, pushes configs to collectors
│   ├── monitor/
│   │   └── feedback.go             # scrape collector metrics, close feedback loop
│   └── store/
│       └── plan_store.go           # persist current plan, support diff/rollback
├── proto/
│   └── controller.proto            # QueryWorkload, CollectionPlan, PlanStatus RPCs
└── config.yaml                     # controller own config (OpAMP listen addr, etc.)
```

### Core Data Model

```go
// Input: what queries need
type QueryWorkload struct {
    MetricName    string
    LabelFilters  map[string]string   // e.g. service="web"
    GroupByLabels []string            // e.g. ["host.name"]
    Aggregations  []AggType           // Quantile, Cardinality, Frequency
    TimeWindow    time.Duration       // e.g. 5m
    RepeatEvery   time.Duration       // query repetition rate
    AccuracySLA   float64             // e.g. 0.01 = 1% relative error
    LatencySLA    time.Duration       // max tolerable staleness
}

// Output: what the controller decides
type CollectionPlan struct {
    AgentConfig   AgentCollectorConfig
    GatewayConfig GatewayCollectorConfig  // nil = passthrough only
    BackendConfig BackendCollectorConfig
    Precompute    []PrecomputeJob
    ValidUntil    time.Time               // re-plan after this
}

type AgentCollectorConfig struct {
    OutputMode     OutputMode    // Raw | Sketch
    SketchType     SketchType    // DDSketch | KLL | HLL | CountSketch | CountMinSketch
    SketchParams   SketchParams  // type-specific (k, rows, cols, accuracy)
    AggregateBy    []string      // dimension keys to preserve
    LabelMatchers  []string      // which series to include
    WindowDuration time.Duration
    Mode           ProcessorMode // Batch | Window
    TransmitSketch bool
}

type PrecomputeJob struct {
    QueryExpr    string        // PromQL/ASAP query expression
    Granularity  time.Duration // how often to materialize
    SketchSource SketchSource  // which backend collector output feeds this
    StorePath    string        // SimpleStore key pattern
}
```

---

## Planning Algorithm

```
Input: QueryWorkload W

1. CLASSIFY aggregation type(s) needed:
   → Quantile?    → candidate sketches: [DDSketch, KLL]
   → Cardinality? → [HLL]
   → Frequency?   → [CountSketch, CountMinSketch]

2. DETERMINE minimum label dimensions:
   dims = union(W.GroupByLabels, keys(W.LabelFilters))
   → These must be preserved in aggregate_by

3. SELECT sketch type using cost model:
   for each candidate sketch:
     estimate bandwidth(sketch, dims, cardinality, W.TimeWindow)
     estimate accuracy(sketch, params, W.AccuracySLA)
     estimate cpu(sketch, rate)         ← from benchmark table
     estimate memory(sketch, dims, cardinality, W.WindowDuration)  ← sketch size in-memory while accumulating
   → pick lowest bandwidth that meets AccuracySLA

4. SELECT window strategy:
   if W.LatencySLA >= W.TimeWindow:
     mode = Window, window_duration = W.TimeWindow
   else:
     mode = Batch (collector merges on query)

5. BUILD CollectionPlan:
   AgentConfig  = {SketchType, AggregateBy=dims, Mode, WindowDuration, params}
   BackendConfig = {MergeFrom=AgentConfig.SketchType, GroupBy=dims}
   Precompute   = [{QueryExpr=W.Query, Granularity=W.RepeatEvery, ...}]
     ↳ only if RepeatEvery < LatencySLA (benefit from caching)

6. PUSH via OpAMP to all agents, gateway, backend
7. REGISTER precompute jobs with ASAPQuery
8. MONITOR: if actual_accuracy < AccuracySLA → re-plan with stricter params
```

---

## Config Push: OpAMP (Open Agent Management Protocol) Integration

OpAMP is an open protocol (defined by the OpenTelemetry project) for remotely managing telemetry agents over a persistent WebSocket or HTTP connection. The server pushes new config to agents; agents reload without restart and report back health and current config hash.

### OpAMP Server — the Controller

- The controller runs the OpAMP server
- It listens for incoming WebSocket connections from collectors
- It pushes `RemoteConfig` messages (new YAML configs) whenever the plan changes

### OpAMP Clients — the Collectors

- Every OTel collector instance (agent, gateway, backend) runs the `opampextension`, which is the OpAMP client
- On startup, each collector connects to the controller's OpAMP server address
- It receives config updates and reloads its pipeline, then reports back health/status

```
┌─────────────────────────────────┐
│  CONTROLLER (Rust)              │
│  ┌───────────────────────────┐  │
│  │  OpAMP Server             │  │
│  │  (WebSocket listener)     │  │
│  └────────┬────────┬─────────┘  │
└───────────┼────────┼────────────┘
            │        │  persistent WebSocket connections
            ▼        ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ Agent OTel   │  │   Gateway    │  │   Backend    │
│ Collector    │  │  Collector   │  │  Collector   │
│ ┌──────────┐ │  │ ┌──────────┐ │  │ ┌──────────┐ │
│ │  opamp   │ │  │ │  opamp   │ │  │ │  opamp   │ │
│ │extension │ │  │ │extension │ │  │ │extension │ │
│ │(client)  │ │  │ │(client)  │ │  │ │(client)  │ │
│ └──────────┘ │  │ └──────────┘ │  │ └──────────┘ │
└──────────────┘  └──────────────┘  └──────────────┘
```

Each collector's config only needs to point at the controller's address — everything else (sketch type, `aggregate_by`, window, etc.) is pushed dynamically at runtime:

```yaml
extensions:
  opamp:
    server:
      ws:
        endpoint: ws://controller:4320/v1/opamp
```

### Generated Config Example

The controller generates YAML dynamically from the plan:

```yaml
# Generated agent config for: DDSketch, aggregate_by=[host.name], window=5m
processors:
  ddsketch:
    mode: window
    window_duration: 5m
    relative_accuracy: 0.01
    quantiles: [0.5, 0.9, 0.99]
    aggregate_by: [host.name]
    label_matchers: ["service.name=~web.*"]
    transmit_sketch: true
    drop_original: true
```

---

## ASAPQuery Precompute Integration

The controller calls ASAPQuery's precompute engine API to register/deregister materialization jobs:

```
POST /api/v1/precompute/jobs
{
  "query": "quantile_over_time(0.99, benchmark_latency{service='web'}[5m])",
  "granularity": "5m",
  "source": "backend_collector:4317",
  "sketch_type": "ddsketch",
  "store_path": "precomputed/latency/p99/service=web/5m"
}
```

The precompute engine:
1. Pulls sketches from backend collector output on arrival
2. Runs the query against the sketch
3. Writes result to SimpleStore

Subsequent identical user queries hit SimpleStore directly, bypassing recomputation.

---

## Implementation Phases

The controller is implemented in **Rust**, enabling direct in-process integration with `asap-planner-rs` (no subprocess boundary) and future integration with ASAPQuery's query planner. The OpAMP server is implemented from the [OpAMP spec](https://github.com/open-telemetry/opamp-spec) over WebSocket (`tokio-tungstenite`) — a one-time cost given no mature Rust server library exists yet.

### Phase 1 — Static Planner (rule-based, no ML)

1. Define `controller.proto` — `QueryWorkload`, `CollectionPlan`, `PlanStatus` RPCs (`tonic`)
2. Implement `query_analyzer.rs` — parse ASAPQuery workload into `QueryWorkload`
3. Implement `rules.rs` — deterministic sketch selection (type from aggregation type, dims from query)
4. Implement `agent_config.rs` / `backend_config.rs` — YAML generation from plan (`serde_yaml`)
5. Implement OpAMP server from spec over WebSocket (`tokio-tungstenite`, protobuf messages)
6. Wire up: on new query workload → plan → push configs

### Phase 2 — Cost Model

7. Build `cost_model.rs` with benchmark data table (from e2e benchmark results)
8. Score plans by bandwidth × CPU × memory × accuracy — pick Pareto-optimal
9. Add `ValidUntil` expiry and periodic re-planning

### Phase 3 — Feedback Loop

10. `feedback.rs` — scrape collector Prometheus metrics (sketch size, CPU) via `/metrics`
11. Compare actual vs. predicted accuracy (via ASAPQuery error metrics)
12. Trigger re-plan if SLA is violated or bandwidth is wasted

### Phase 4 — ASAPQuery Precompute Integration

13. Implement precompute job registration API client
14. Add precompute scheduling logic (which queries benefit from caching)
15. Add SimpleStore write path for materialized results

---

## Key Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Config push protocol | OpAMP | Already included in all collector builds; standard OTel remote management |
| Plan format | YAML (collector native) | No translation layer; controller generates the exact YAML collectors accept |
| Implementation language | Rust | Direct in-process integration with `asap-planner-rs`; future integration with ASAPQuery query planner |
| OpAMP server | Implemented from spec (`tokio-tungstenite`) | No mature Rust OpAMP server library; one-time implementation cost |
| Planner core | `asap-planner-rs` (in-process) | Already in development (PR #174); no subprocess boundary in Rust |
| Re-planning trigger | Time-based + SLA violation | Simple and predictable; avoids oscillation |
| Sketch mergeability | Guaranteed by sketchlib-go | DDSketch, KLL, HLL all support merge — enables hierarchical aggregation |
| Fallback | Raw samples always valid | If planner is down or uncertain, default to raw (safe) |

---

## Out of Scope (for now)

- ML-based workload prediction
- Cross-metric correlation (joint sketch types)
- Auto-scaling the number of collector replicas
- Query rewriting (controller configures collection only, not query semantics)
