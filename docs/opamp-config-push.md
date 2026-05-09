# OpAMP Config Push: Controller → Supervisor → Collector

**Status**: implemented and working end-to-end as of [#133](https://github.com/ProjectASAP/DataCollector/pull/133) (commit `4b196e1`, "feat: OpAMP Supervisor config — push-based config from controller to collector"). This document captures the architecture, the restart semantics, and the test surface so future contributors don't have to re-derive it from the commit history.

This is the **control plane** side of the pipeline. The data plane — sketch bytes flowing from collector processors to the ASAPQuery-backend ingest path over modified OTLP — is covered in [`docs/pipeline-query-catalog.md`](pipeline-query-catalog.md).

---

## 1. Three-process topology

```
┌────────────────────┐
│     Controller     │        (Rust, long-running)
│   asap-controller  │
│                    │
│  ┌──────────────┐  │
│  │ OpAMP Server │◀─┼── WebSocket /v1/opamp  (default :4320)
│  │ (axum)       │  │
│  └──────┬───────┘  │
│         │ push()   │
│         ▼          │
│    replanner       │        (SLA-violation loop, REST /api/v1/plan)
└────────────────────┘
            ▲
            │ persistent WebSocket
            │ (agent-initiated)
            │
┌───────────┼────────────────────────────────────────────┐
│           ▼                                             │
│ ┌──────────────────┐                                    │
│ │ OpAMP Supervisor │   (Go, github.com/open-telemetry/  │
│ │  (binary)        │    opentelemetry-collector/cmd/    │
│ │                  │    opampsupervisor)                │
│ └──────┬───────────┘                                    │
│        │                                                │
│        │ fork/exec + SIGTERM on config change           │
│        ▼                                                │
│ ┌──────────────────┐                                    │
│ │  asap-otel       │  (the unified collector binary —   │
│ │  collector       │   every sketch processor compiled  │
│ │                  │   in: countmin, ddsketch, kll,     │
│ │                  │   hll, countsketch, serf, gorilla) │
│ │  ┌────────────┐  │                                    │
│ │  │ opamp-     │  │                                    │
│ │  │ extension  │  │  (reports health + config_hash     │
│ │  │            │◀─┼── back to controller on the        │
│ │  └────────────┘  │    supervisor's socket)            │
│ └──────────────────┘                                    │
│                                                         │
│                    one collector host                   │
└─────────────────────────────────────────────────────────┘
```

The supervisor and the collector are **two separate OS processes** on the same host. The supervisor owns the long-lived WebSocket connection to the controller and the lifecycle of the child collector.

---

## 2. Push call graph

### 2a. Controller side (Rust)

**File**: `controller/src/opamp/mod.rs`

| Symbol | Line | Role |
|---|---|---|
| `OpampServer` | 77–101 | Axum handler + connected-agent registry |
| `OpampServer::ws_handler` | 121–144 | HTTP `GET /v1/opamp` → WebSocket upgrade, calls `handle_socket` |
| `OpampServer::push` | 147 | Send a `RemoteConfig` to one specific agent by `agent_id` |
| `OpampServer::push_to_role` | 162 | Broadcast a `RemoteConfig` to all agents matching a role (`AgentRole::Agent`, `AgentRole::Backend`, etc.) |
| `OpampServer::push_all` | 156 | Broadcast to every connected agent |

**Triggers** (all in `controller/src/main.rs`):

1. **`POST /api/v1/plan`** — external HTTP entry point that accepts a new plan payload. The handler (lines 310–404) computes agent and backend configs, then calls:
   - `opamp.push_to_role(AgentRole::Agent, agent_config)` — lines 337–340
   - `opamp.push_to_role(AgentRole::Backend, backend_config)` — lines 350–353

2. **On-connect callback** (lines 133–159) — fires when a new agent's WebSocket arrives. Looks up the agent's registered metrics, calls `replanner.push_config_to_agent(agent_id)` which pulls the current plan for those metrics and pushes it. This is how a newly-started supervisor bootstraps its first config.

3. **Replanner** (`controller/src/replan.rs:123–178`):
   - `replan_metric(metric)` — invoked when a metric's accuracy or latency SLA is violated. Recomputes the plan for that metric and pushes to every agent registered as a producer for it.
   - `handle_violation(agent_id)` — the SLA-breach entry point wired to the monitoring loop.

### 2b. Supervisor side (Go binary)

**Bootstrap config**: `opentelemetry-collector-contrib/cmd/asap-otel-opamp/supervisor-config.yaml`

```yaml
server:
  endpoint: ws://localhost:4320/v1/opamp
  tls:
    insecure: true
agent:
  executable: ./asap-otel         # path to the collector binary
  description:
    non_identifying_attributes:
      role: agent                 # consumed by controller's push_to_role()
```

The supervisor:

1. Opens a WebSocket to the controller and sends an `AgentToServer` registration message including the `non_identifying_attributes.role` value (so the controller knows whether to route `push_to_role(AgentRole::Agent, ...)` to this supervisor).
2. Waits for a `ServerToAgent` message with a populated `remote_config` field.
3. On receipt, merges the received YAML with the bootstrap `config-with-opamp.yaml`, writes the merged content to `effective.yaml` in its working directory, and sends a `SIGTERM` (or platform equivalent) to the running collector child process.
4. Forks a new collector child with the new `effective.yaml`.

### 2c. Collector side (Go binary)

The collector binary (e.g. `asap-otel` built from `opentelemetry-collector-contrib/cmd/asap-otel`) runs the stock `opampextension`. Its role is **reporting**, not applying — the extension sends `AgentToServer` messages with:

- Current `config_hash` (so the controller can confirm the push landed)
- Agent health status (the supervisor's restart loop proves the new config is loadable)

The extension does **not** hot-reload the collector pipeline. All pipeline changes happen via supervisor restart.

---

## 3. Why supervisor restart, not in-process hot-reload?

The OpenTelemetry collector's pipeline is constructed at startup from the parsed YAML — processor factories build concrete `Processor` instances into a pinned DAG, and the runtime has no graceful "swap the DAG under live traffic" primitive. A config change that adds, removes, or retypes a processor cannot be applied by mutating the running pipeline; it requires re-running the factory graph.

Two approaches exist in principle:

- **In-process hot-reload**: pause the pipeline, rebuild the processor graph, resume. Requires every processor to support clean shutdown and state handoff. Not supported upstream as of the current collector release.
- **Supervisor restart**: kill the child, start a new child with the new config. Loses a few seconds of in-flight traffic but requires no processor-level cooperation.

DataCollector uses supervisor restart (option 2) because it's the only pattern that works with stock collector processors today. The commit that introduced this (`4b196e1`) confirmed the chain works end-to-end on a sketch config change, including the collector restart observably picking up a new KLL processor config.

The downside is **in-flight loss**: any sketch window currently being assembled on the old collector is lost when the child exits. This is acceptable for Phase 1 because:

1. Config changes are rare (SLA-driven replans, not per-query)
2. The backend's precompute engine tolerates gaps via late-data-policy fallback (see `pipeline-query-catalog.md` §5.2)
3. A restart is observable to the controller via the `opampextension`'s health reports, so consecutive restarts trip a circuit-breaker rather than looping

If in-process hot-reload becomes available upstream, the supervisor layer can be removed and the `opampextension` wired to apply configs directly. The controller push API doesn't change — only the agent-side apply mechanism does.

---

## 4. Sketch-type capability matching

Commit `4b196e1`'s testing originally surfaced this as an open issue: the controller had to know which sketch processors a given collector binary supported, because pushing a KLL config to a per-sketch builder (e.g. an old `countminsketchcol` that only had countmin compiled in) would crash the restarted collector on config load.

The cleanup that consolidated all per-sketch builder dirs into the single unified `asap-otel` binary (cleanup PR #363) collapsed this problem: every supervisor advertising `role: agent` now runs `asap-otel`, and `asap-otel` compiles in every sketch processor. The controller's `push_to_role` only needs to ensure the YAML it pushes uses processor names the unified builder registered (`countminsketchprocessor`, `ddsketchprocessor`, `kllprocessor`, `hllprocessor`, `countsketchprocessor`, `serfprocessor`, `gorillaprocessor`, …), which it does by construction — these are the same names the controller's emit table uses when generating configs.

If a future deployment ever ships a stripped-down collector with a subset of processors, the supervisor registration can be extended to include a `processors_available: [...]` list in `non_identifying_attributes`, and `push_to_role` can filter by that list before sending. Not needed today.

---

## 5. Test surface

### 5a. What's tested today

**File**: `controller/src/main.rs` lines 991–1238. Three Rust integration tests that use a mock WebSocket client to decode `ServerToAgent` protobufs and assert on their contents:

1. **`agent_receives_config_on_connect_via_workload_registry`** (992–1110) — agent connects, on-connect callback triggers `push_config_to_agent`, mock client verifies the pushed YAML matches the expected plan for the agent's registered metrics.
2. **`replan_pushes_only_to_registered_agent`** (1114–1184) — `replan_metric(metric)` is called, mock clients confirm only the producer for that metric receives the push (not unrelated agents).
3. **`generated_agent_yaml_contains_opamp_extension`** (1188–1238) — static YAML validation: `extensions.opamp.server.ws.endpoint` is set, `service.extensions` includes `"opamp"`, etc.

All three are fast (seconds), hermetic (no real supervisor, no real collector), and run as part of `cargo test -p controller`.

### 5b. What's NOT tested today

- **Real supervisor binary → controller → supervisor loop**. The existing tests use mock WebSocket clients; they do not spawn `opampsupervisor` or a real collector binary.
- **End-to-end restart observability**. The assertion that the child collector actually restarts and picks up the new config was done manually in PR #133 and is not part of the automated suite.

### 5c. Proposed supervisor integration test (follow-up)

A stand-alone integration test file `controller/tests/opamp_supervisor_integration_test.rs` marked `#[ignore]` by default (so it doesn't run in `cargo test` but can be invoked with `cargo test -- --ignored`):

1. Start the controller HTTP + OpAMP servers on ephemeral ports
2. `exec` the `opampsupervisor` binary with a temp-dir `supervisor-config.yaml` pointing at the controller's port
3. `exec` a stub child collector that just writes its argv + config path to a known temp file and sleeps
4. `POST /api/v1/plan` with a known sketch config
5. Poll the temp file until it reflects the new config (child was re-execed)
6. Assert the new config hash matches what the controller pushed

The `#[ignore]` gate is important because the test depends on an external binary (`opampsupervisor` from `open-telemetry/opentelemetry-collector`) being installed and on filesystem/process primitives that are flaky on some CI runners. Marking it `#[ignore]` keeps it available for manual verification without blocking the fast suite.

**This follow-up is tracked but not shipping in PR F** — PR F ships the architecture doc + the control-plane-design.md correction, and the proposed test layout above. The test itself can land independently once the opampsupervisor binary is pinned in CI.

---

## 6. Quick reference — file:line index

| Thing | Path | Line |
|---|---|---|
| OpAMP server struct | `controller/src/opamp/mod.rs` | 77–101 |
| WebSocket handler | `controller/src/opamp/mod.rs` | 121–144 |
| Push unicast | `controller/src/opamp/mod.rs` | 147 |
| Push to role | `controller/src/opamp/mod.rs` | 162 |
| `ServerToAgent` remote_config build | `controller/src/opamp/mod.rs` | 256–283 |
| Plan push REST handler | `controller/src/main.rs` | 310–404 |
| On-connect callback | `controller/src/main.rs` | 133–159 |
| Replanner push | `controller/src/replan.rs` | 123–178 |
| Supervisor bootstrap config | `opentelemetry-collector-contrib/cmd/asap-otel-opamp/supervisor-config.yaml` | — |
| Collector-with-opamp config | `opentelemetry-collector-contrib/cmd/asap-otel-opamp/config-with-opamp.yaml` | — |
| Existing mock-client tests | `controller/src/main.rs` | 991–1238 |
| Original supervisor e2e commit | `git show 4b196e1` | — |
