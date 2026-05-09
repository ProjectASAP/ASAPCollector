# integration/e2e-cold-path

End-to-end integration test for the ASAP cold-path pipeline:

```
fake-driver  ─OTLP─▶  asap-otel + gorillas3processor (gateway_raw)
                          │  Prometheus TSDB block builder
                          ▼
                       MinIO   (<ulid>/chunks/000001 + index + meta.json)
                          │
                          ▼
                       Thanos store-gateway / backend cold-path
```

This is the cold-path e2e gate. After PR #359 the
`gorillas3processor` runtime emits Prometheus TSDB blocks (not the
legacy `GORILLA1` magic-prefixed XOR-delta chunks). The legacy in-test
decoder + cross-language Rust subtest were retired with the rename
from `integration/gorilla_s3_e2e/` to this directory.

## Layout

```
integration/e2e-cold-path/
├── e2e_test.go                  # main Go test — TestColdPathEnd2End
├── golden/
│   └── input_samples.json       # 100 deterministic samples
├── docker-compose/
│   └── e2e-overlay.yml          # ephemeral stack (separate compose
│                                #   project + 29xxx port range)
├── go.mod                       # depends on prometheus/prometheus
├── Makefile                     # `make test` / `make test-live`
└── README.md
```

## How the assertion works

The runtime side of the cold path is the `gorillas3processor` in
`gateway_raw` role + `delivery_mode=durable_raw`. On each
`window_interval` tick it finalizes a Prometheus TSDB block to
S3/MinIO with three files under `<tsdb_bucket>/<ULID>/`:

- `chunks/000001` — XOR-encoded sample chunks
- `index` — Prometheus inverted index
- `meta.json` — block metadata (uploaded LAST, so Thanos doesn't see
  half-written blocks)

The test:

1. Lists every `meta.json` marker in the `asap-gorilla` MinIO bucket
   via `mc find ... --name 'meta.json'`. Each match identifies one
   finalized block ULID.
2. Downloads each block's three files into a per-block temp dir.
3. Opens the block with `prometheus/tsdb`'s `tsdb.OpenBlock(nil, dir,
   nil, nil)`, builds a `BlockQuerier` over the full time range, and
   selects every series matching `__name__=~.+`.
4. Iterates the resulting `SeriesSet`, flattens to (label_set, ts,
   value) tuples, and asserts the multi-set of values equals the
   input fixture.

## Run

### Default (no docker, fixture-only)

```
cd integration/e2e-cold-path
go test -v ./...
```

The live e2e test (`TestColdPathEnd2End`) is gated on
`COLD_E2E_LIVE=1` and SKIPs in this mode; the
`TestGoldenInputFixtureWellFormed` test still runs and pins the
shape of the input fixture.

### Live e2e (requires docker, sweep idle)

```
COLD_E2E_LIVE=1 go test -v -run TestColdPathEnd2End ./...
```

The live test:

1. Brings up `minio-e2e + asap-otel-e2e` via
   `docker compose -p asap-cold-e2e -f docker-compose/e2e-overlay.yml up -d`.
2. Pushes the 100-sample fixture via OTLP/HTTP to `127.0.0.1:24318/v1/metrics`.
3. Waits 75s for the gorillas3processor's 60s window to flush.
4. Runs `HappyPath` and `MultiBlockRange` subtests against the live stack.
5. Tears the stack down with `down -v`.

### Coordination with the host-wide sweep

The host runs a long sweep (`run_e2e_sweep.sh`) that owns the default
compose project. The e2e test takes the **defensive** posture:

- The overlay uses a separate compose project name (`asap-cold-e2e`)
  and a separate port range (29xxx) so it would NOT collide on
  ports — but
- Docker memory + CPU + network bandwidth are still shared with the
  sweep. If the sweep PID (default `2862786`, override via
  `COLD_E2E_SWEEP_PID=<pid>`) is alive, the live test SKIPs unless
  forced via `COLD_E2E_FORCE=1`.
- Disable the busy-check entirely with `COLD_E2E_SWEEP_PID=0` (or
  `=none`) — useful in CI where there's no sweep concept.

## Subtests

| Subtest                            | What it asserts                                                                                          |
| ---------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `HappyPath`                        | MinIO contains ≥1 finalized block (a `meta.json` exists); the block decodes back to the input value-set. |
| `MultiBlockRange`                  | If the fixture spread crossed a window boundary, ≥2 blocks land; their union covers the full input.     |
| `TestGoldenInputFixtureWellFormed` | (always runs, no docker) The fixture's `expected_sum` invariant matches `sum(values)`.                   |

## Notes

- The bucket name `asap-gorilla` matches the legacy MVP demo.
- Block download uses `mc cat` inside a transient `minio/mc:latest`
  container; no S3 SDK in `go.mod`.
- OTLP push uses raw HTTP/JSON to the agent's `/v1/metrics` endpoint;
  no OTLP client in `go.mod`.
- The PromQL accuracy / data-source-marker subtests from the legacy
  Gorilla-S3 e2e are NOT carried over: TSDB is upstream Prometheus's
  native format, not an ASAP-specific wire format. PromQL response
  shape is now exercised by the warm-path e2e + ASAPQuery-backend's
  own tests.
