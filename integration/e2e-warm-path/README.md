# integration/e2e-warm-path

End-to-end integration test for the ASAP warm-path sketch pipeline:

```
fake-driver  ─OTLP─▶  asap-otel + ddsketchprocessor (window 10s)
                          │  emits typed DDSketchDataPoint
                          ▼
                       data plane (asap/data-plane:dev)
                          │  warm-tier OTLP receiver +
                          │  DDSketchAccumulator
                          ▼
                       PromQL HTTP /api/v1/query
                       { data_source: sketch_warm_tier,
                         result: p99 within ε=0.01 }
```

This is the warm-path e2e gate: it validates the full
edge → backend → PromQL round-trip for the DDSketch quantile family
and pins the criterion-④ accuracy bound (DDSketch relative-accuracy
ε=0.01) end-to-end.

## Layout

```
integration/e2e-warm-path/
├── e2e_test.go                  # main Go test — TestWarmPathE2E
├── golden/
│   └── input_samples.json       # 100 deterministic latency samples
├── docker-compose/
│   └── e2e-overlay.yml          # ephemeral stack (separate compose
│                                #   project + 29xxx port range)
├── configs/
│   └── asap-otel-agent-warm.yaml  # agent config: ddsketch window 10s
├── go.mod                       # std-lib only
└── README.md
```

## How the assertion works

The test pushes the 100-sample fixture (uniform 1..100 ms latency)
via OTLP/HTTP+JSON to the asap-otel agent. The DDSketch processor
runs in `mode: window` with a 10s flush interval, emits a typed
`DDSketchDataPoint` named `http_requests_total_latency_ms_quantile`,
and forwards it to the backend's warm-tier OTLP receiver. The backend
ingests the sketch into its `DDSketchAccumulator` and answers
`quantile_over_time(0.99, http_requests_total_latency_ms[30s])` from
the merged sketch, stamping the response with
`data_source: sketch_warm_tier`.

The test asserts:

1. HTTP 200, response shape valid (`status: success`, `data` +
   `data.resultType` populated).
2. `data_source` reports the warm-tier engine. Canonical name is
   `sketch_warm_tier`; the test tolerates name drift (e.g. an
   `_ddsketch` suffix) but logs the deviation.
3. The returned p99 estimate satisfies the DDSketch relative-accuracy
   bound: `|estimate - exact| / exact <= 0.01`. For the 1..100 ramp
   the exact p99 is 99.0, so the estimate must land in [98.01, 99.99].

## Run

### Default (no docker, fixture-only)

```
cd integration/e2e-warm-path
go test -v ./...
```

The live e2e test (`TestWarmPathE2E`) is gated on `WARM_E2E_LIVE=1`
and SKIPs in this mode; `TestGoldenInputFixtureWellFormed` still runs
and pins the shape of the input fixture.

### Live e2e (requires docker, sweep idle)

```
WARM_E2E_LIVE=1 go test -v -run TestWarmPathE2E ./...
```

The live test:

1. Brings up `asap-otel-warm + backend-warm` via
   `docker compose -p asap-warm-e2e -f docker-compose/e2e-overlay.yml up -d`.
2. Pushes the 100-sample fixture via OTLP/HTTP to `127.0.0.1:24318/v1/metrics`.
3. Waits 15s for the ddsketch window flush + backend ingest.
4. Issues `quantile_over_time(0.99, http_requests_total_latency_ms[30s])`
   against `http://localhost:29191/api/v1/query`.
5. Asserts response shape, data_source marker, and p99 within ε=0.01.
6. Tears the stack down with `down -v`.

End-to-end runtime is under 60s (10s window + 15s slack + ~30s for
docker up/down).

### Coordination with the host-wide sweep

Same defensive posture as the cold-path test: separate compose
project (`asap-warm-e2e`) and separate port range (29191) so it
does NOT collide on ports — but the live test SKIPs when the host
sweep is alive (`WARM_E2E_SWEEP_PID=<pid>`). Override with
`WARM_E2E_FORCE=1`; disable the busy-check entirely with
`WARM_E2E_SWEEP_PID=0` (or `=none`).

## Constraints

- The compose stack pulls `asap/data-plane:dev` from the local
  Docker daemon — assume it's already built.
- This is a **focused** warm-path gate, not a copy of the MVP demo:
  no MinIO, no Thanos, no fake-exporter, no
  mvp-multi-stage.yml/mvp-workload.yaml fixtures.
- The agent's DDSketch processor uses `delta_transmission: false`
  intentionally — full-state per window keeps the backend's first
  ingest deterministic and avoids dependence on between-window
  diffing logic that is harder to reason about in a 60s test budget.
