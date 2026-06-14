# Cold-path E2E validation (edge cold-ship decouple)

Branch: `fix/edge-cold-ship-decouple`
Date: 2026-06-14
Machine constraint as scoped by the task: docker/compose Thanos+MinIO stack
"cannot run here". (In practice `docker` and a standalone `minio` binary turned
out to be available on this host, so the object-store leg was also exercised
live — see step 4 below. The compose-orchestrated multi-service stack
— data-plane, thanos-query, store-gateway wiring — remains the docker-gated part.)

## TL;DR

- The edge cold-fragment shipper is **already decoupled** from the control
  channel in the current code: shipping is gated **solely** on `cold.enabled`.
  There was no control-channel gate to remove in the Go source.
- This PR **pins that decoupling as an invariant** (a regression test +
  a load-bearing comment at the start-up site), and **validates the cold path
  end-to-end** against the runnable `gorilla-merger`, including a live
  MinIO object-store PUT.

## 1. Where cold shipping is (not) gated — investigation result

Searched `processor.go`, `cold_fragment_shipper.go`, `cold_spool.go`,
`flush.go`, `ingest.go`, `config.go`, `config_validate.go`,
`control_plane.go`. Findings:

- Cold ingest (`ingest.go consumeMetric`) adds samples to the per-shard cold
  encoder whenever the metric is not `tier: warm`; no control-channel check.
- Cold flush + ship (`flush.go flushAll` / `flushShardWarmCold`) run under
  `if p.coldEnabled { ... shipFragments ... }`; no control-channel check.
- `processor.go newProcessor` builds the shipper + ship worker under
  `if p.coldEnabled`; `Start()` calls `p.shipWorker.start()` unconditionally;
  `startControlPlane()` is a **separate**, independently-gated path.
- `grep` for any conjunction of {`ctrl`, `ControlChannel`, `control`} with
  {`cold`, `ship`, `frag`} in non-test Go returns **nothing**.

Conclusion: cold shipping and the control channel are independent. The control
channel only applies `precompute.PrecomputeConfigSet` updates to live sketch
aggregators (coordinated sampling); it does not touch the cold tier.

The static deploy configs confirm the intended decoupling already works:
`deploy/mvp-multinode/configs/asap/asap-otel-agent-asapedge.yaml` sets
`cold.enabled: true` + `cold.ship_endpoint: …/ingest/gorilla` with
`control_channel` DISABLED, and the code ships under exactly that config.

## 2. The "fix" (minimal, honest)

Because the decoupling already holds, the change is **defensive**, not a
behavior change:

- Added `TestColdShipsWithControlChannelDisabled` — drives the processor through
  its real lifecycle (`Start` → `ConsumeMetrics` → `flushAll` → async ship) with
  `cold.enabled: true` + `control_channel.enabled: false`, asserts `ctrlChan ==
  nil`, and verifies gzipped ASAPFRG1 fragments reach an `httptest` sink and
  decode back to the ingested series. A regression that re-couples shipping to
  the control channel (e.g. starting the ship worker only when `ctrlChan != nil`)
  fails this test.
- Added a load-bearing INVARIANT comment at the `p.shipWorker.start()` site in
  `processor.go Start()` documenting the decoupling and naming the test.

## 3. Test — name & status

`TestColdShipsWithControlChannelDisabled`
(`opentelemetry-collector-contrib-patch/processor/asapedgeprocessor/cold_ship_decouple_test.go`)
— **PASS**. Full package `go test ./...` for `asapedgeprocessor` — **PASS**
(2.0s). Run with `GOPRIVATE='github.com/ProjectASAP/*' GOFLAGS=-mod=mod`, after
`git submodule update --init opentelemetry-collector` +
`bash restore_otel_collector_patches.sh` to satisfy the vendored-pdata replace.

## 4. End-to-end against the gorilla-merger — VALIDATED LIVE

The merger (`ASAPQuery-backend/gorilla-merger`) was built
(`GOPRIVATE='github.com/ProjectASAP/*' go build ./cmd/gorilla-merger`) and run
as a live server. A throwaway driver built cold ASAPFRG1 fragments with the
**same `asap-gorilla-go` streaming encoder the edge uses**, gzip-POSTed them to
`/ingest/gorilla`, then queried them back over the merger's Thanos StoreAPI
(gRPC).

Validated, observed directly:

1. **Ingest** — `POST /ingest/gorilla` → `200`. Fragment: 1 fragment,
   30 samples, XOR encoding, `node_cpu_seconds_total{core="0",mode="user"}`.
2. **Pending block** — merger flushed the closed window into a pending L1 block
   (`built pending block from closed window … series=1`).
3. **Queryable via StoreAPI** — gRPC `Series` returned the series with all
   **30 samples**, XOR chunks decoded back to the ingested values; external
   labels merged onto the series.
4. **Compaction** — pending L1 → shipped L2 block (`compact blocks … runs=1`).
5. **Object-store PUT (live MinIO)** — with `-external-labels` set, the shipper
   uploaded the L2 block to a real MinIO bucket:
   `shipper uploaded blocks uploaded=1`. Verified in the bucket:
   `01KV3R7PDKVG8DJCA4XK2CVE2F/{chunks/000001, index, meta.json}`,
   `meta.json` → `numSamples: 30, numSeries: 1`,
   `thanos.labels: {cluster: asap-mvp, merger: m1, tier: cold}`.

   Note: an initial run with empty external labels failed the upload with
   "empty external labels are not allowed for Thanos block" — a Thanos block
   requirement, not a pipeline defect; setting `-external-labels` completed the
   PUT. (The real edge sets `cold.external_labels`, e.g. `cluster: asap-mvp`.)

Also green: `gorilla-merger` `internal/merger` + `internal/coldchunk` test
suites (`go test ./internal/...`), which include the in-process
ingest→flush→pending→StoreAPI round-trip (`TestCustomStoreSeriesRoundTrip`).

## 5. What remains docker-gated (NOT validated here)

- The compose-orchestrated multi-service stack as one unit: the asap-otel **edge
  collector** binary pushing live to the merger, `data-plane`, and the
  **thanos-query → store-gateway → S3** read path resolving a cold query into a
  rendered answer across services.
- The full **Fig 7 cold-arm** evaluation run (warm-eviction → cold-fallback →
  answer-from-archive latency/recall numbers) end to end across the deployed
  topology.
- gorilla-merger CPU/IO/PUT **throughput** numbers under load (a separate
  benchmark PR), per the prior eval note.

What this PR de-risks for those: the edge→merger→pending→StoreAPI→compaction
→object-store PUT chain is proven on this machine; the remaining gap is purely
the cross-service compose wiring and the store-gateway read leg.
