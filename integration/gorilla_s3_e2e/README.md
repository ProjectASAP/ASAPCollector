# Phase 6 — Gorilla-S3 cold-engine end-to-end test

End-to-end integration test for the full pipeline:

```
fake-driver  ─OTLP─▶  asap-otel+gorillas3processor
                          │  encode (Gorilla XOR-delta)
                          ▼
                       MinIO   ─list/get─▶  GorillaS3ColdStore
                                                  │  decode
                                                  ▼
                                         GorillaQueryEngine
                                          exact PromQL
                                                  │
                                                  ▼
                                          backend HTTP /api/v1/query
                                          { accuracy: ε=0,
                                            data_source: gorilla_archive }
```

This is the FINAL phase of the
[Gorilla-S3 cold-engine](../../docs/design-gorilla-s3-cold-engine.md) work.
It validates the byte-format compatibility contract that ties together the
Phase-1 Rust crate (`asap-gorilla`), the Phase-2 Go processor
(`gorillas3processor`), the Phase-3 backend cold-store, and the
Phase-4/5 backend query engine + capability router.

## Layout

```
integration/gorilla_s3_e2e/
├── e2e_test.go                  # main Go test — TestGorillaS3End2End
├── gorilla_decoder.go           # in-test decoder; mirrors the
│                                #   GORILLA1 byte format directly
├── gorilla_decoder_test.go      # round-trip sanity tests for the
│                                #   in-test decoder (no docker)
├── golden/
│   └── input_samples.json       # 100 deterministic samples
├── docker-compose/
│   └── e2e-overlay.yml          # ephemeral stack (separate compose
│                                #   project + 29xxx port range)
├── go.mod                       # std-lib only
├── Makefile                     # `make test` / `make test-live`
└── README.md
```

## Run

### Default (no docker, fixture-only)

```
cd integration/gorilla_s3_e2e
go test -v ./...
```

This runs the decoder round-trip + fixture sanity tests. The live e2e test
(`TestGorillaS3End2End`) is gated on `GORILLA_E2E_LIVE=1` and SKIPs in this
mode; the rest of the test surface still pins the byte format and the input
fixture.

### Live e2e (requires docker, sweep idle)

```
GORILLA_E2E_LIVE=1 go test -v -run TestGorillaS3End2End ./...
```

The live test:

1. Brings up `minio-e2e + asap-otel-e2e + backend-e2e` via
   `docker compose -p asap-gorilla-e2e -f docker-compose/e2e-overlay.yml up -d`.
2. Pushes the 100-sample fixture via OTLP/HTTP to `127.0.0.1:24318/v1/metrics`.
3. Waits 75s for the gorillas3processor's 60s window to flush.
4. Runs five subtests (HappyPath, MultiChunkRange, AccuracyExact,
   DataSourceMarker, CrossLanguageByteCompat).
5. Tears the stack down with `down -v`.

### Coordination with the host-wide sweep

The host runs a long sweep (`run_e2e_sweep.sh`) that owns the default
compose project. The e2e test takes the **defensive** posture (option B in
the PR brief):

* The overlay uses a separate compose project name (`asap-gorilla-e2e`)
  and a separate port range (29xxx) so it would NOT collide on ports —
  but
* Docker memory + CPU + network bandwidth are still shared with the
  sweep. If the sweep PID (default `2862786`, override via
  `GORILLA_E2E_SWEEP_PID=<pid>`) is alive, the live test SKIPs unless
  forced via `GORILLA_E2E_FORCE=1`.
* Disable the busy-check entirely with `GORILLA_E2E_SWEEP_PID=0` (or
  `=none`) — useful in CI where there's no sweep concept.

## Subtests

| Subtest                       | What it asserts                                                                                                                                                       |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `HappyPath`                   | MinIO contains ≥1 chunk under `asap-gorilla/default/<metric>/...`; the in-test Go decoder reads the chunk back to the input value-set (multi-set equality).            |
| `MultiChunkRange`             | If the fixture spread crossed a 60s window boundary, ≥2 chunks land; their union covers the full input set. If only 1 chunk lands, single-chunk path is exercised.    |
| `AccuracyExact`               | Backend PromQL response carries `kind=Exact, ε=0, δ=0` infos. SKIPS with a Phase-6 follow-up note when the backend HTTP server doesn't yet wire `EngineRouter`.        |
| `DataSourceMarker`            | Backend PromQL response carries `data_source: gorilla_archive`. Same Phase-6 follow-up SKIP behavior.                                                                  |
| `CrossLanguageByteCompat`     | Go-encoded chunk is decoded by the Rust `asap-gorilla` crate via `cargo test --test byte_compat`. This is the byte-format-parity contract.                            |

Plus three docker-free subtests that always run:

| Subtest                                                | Purpose                                                                                                            |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------ |
| `TestGoldenInputFixtureWellFormed`                     | The 100-sample fixture's `expected_sum` invariant matches `sum(values)` (catches a typo in the JSON).               |
| `TestGorillaDecoder_HeaderRejectsBadMagic`             | Decoder rejects a bad magic prefix with a clear error.                                                              |
| `TestGorillaDecoder_HeaderRejectsBadVersion`           | Decoder rejects an unsupported version byte.                                                                        |
| `TestGorillaDecoder_RoundTripSingleSeriesSinglePoint`  | Header-only path (no bit-stream entries) round-trips.                                                               |
| `TestGorillaDecoder_RoundTripFlatSequence`             | Constant-delta TS + constant-value (bucket-0 + value-unchanged paths) round-trip.                                   |
| `TestGorillaDecoder_RoundTripVaryingValues`            | 100-sample 1..N ramp (mirrors the live fixture) round-trips and sums to 5050.                                       |

## Phase-6 follow-ups surfaced by writing this test

1. **Backend HTTP server still wires `Arc<SimpleEngine>` directly.** The
   Phase-5 [`EngineRouter`](https://github.com/ProjectASAP/ASAPQuery-backend/blob/main/asap-query-engine/src/engines/router.rs)
   is built but not yet consumed by
   `asap-query-engine/src/drivers/query/servers/http.rs`. Until that wiring
   lands, the `AccuracyExact` and `DataSourceMarker` subtests SKIP with a
   note rather than fail. Tracking item: ASAPQuery-backend follow-up
   "wire EngineRouter into HTTP server".
2. **`gorillas3processor` README header byte-count typo.** The README says
   `[4] "GORILLA1"` but `encoder.go` writes 8 bytes. Filed for a doc fix
   in a follow-up PR (the byte format is correct, just the doc).
3. **Cross-language test consumes `byte_compat` via `cargo test`.** The
   `asap-gorilla` `byte_compat` integration test currently asserts on its
   own fixtures; this PR's e2e passes `GORILLA_E2E_CHUNK_PATH` for the
   live chunk. A small `byte_compat` enhancement to assert that env-var-
   pointed chunk decodes successfully would close the loop. Filed as
   Phase-6 follow-up.

## Notes

* `go.mod` is intentionally **std-lib only**. The byte format pin lives
  in `gorilla_decoder.go`, not in a transitive import that could drift on
  a dep bump.
* Chunk download uses `mc` inside a transient `minio/mc:latest` container;
  no S3 SDK in `go.mod`.
* OTLP push uses raw HTTP/JSON to the agent's `/v1/metrics` endpoint;
  no OTLP client in `go.mod`.
