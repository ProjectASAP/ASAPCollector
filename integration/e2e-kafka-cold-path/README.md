# Kafka-bridged cold-path end-to-end test

End-to-end integration test for the `durable_fragment` delivery mode of
the `gorillas3processor` — the cold-path composition where:

1. The agent runs `gorillas3 role=agent, delivery_mode=durable_fragment,
   drop_original=true`. Raw OTLP samples are encoded into XOR-delta
   fragment metrics; the raw stream is dropped.
2. Fragment metrics traverse Kafka via the upstream `kafkaexporter` →
   topic `asap.gorilla.fragments` → `kafkareceiver` (durable network
   bridge).
3. The gateway runs `gorillas3 role=gateway_fragment,
   delivery_mode=durable_fragment`. Fragments are reassembled into
   per-window Prometheus TSDB blocks and PUT to MinIO bucket
   `asap-gorilla-tsdb`.

```
test driver  ──OTLP/HTTP──▶  agent-kafka  ──Kafka──▶  gateway-kafka  ──S3──▶  MinIO
                                                                                │
                                                                                ▼
                                                          test driver lists / downloads
                                                          each <ulid>/{chunks/000001,
                                                          index, meta.json}, opens via
                                                          prometheus/tsdb, asserts byte-
                                                          equal vs the input fixture
```

## Layout

```
integration/e2e-kafka-cold-path/
├── README.md
├── e2e_test.go              # TestKafkaColdPathE2E + TestGoldenInputFixtureWellFormed
├── golden/
│   └── input_samples.json   # 100 deterministic samples (copied verbatim from gorilla_s3_e2e)
└── go.mod                   # github.com/prometheus/prometheus only
```

## Run

### Default (no docker, fixture-only)

```
cd integration/e2e-kafka-cold-path
go test -v ./...
```

This runs `TestGoldenInputFixtureWellFormed` only; the live test
SKIPs unless `KAFKA_E2E_LIVE=1` is set.

### Live e2e (requires docker)

```
KAFKA_E2E_LIVE=1 go test -v -run TestKafkaColdPathE2E ./...
```

The live test:

1. Brings up `base.yml + mvp-kafka-cold-path.yml` on compose project
   `asap-kafka-cold` (`docker compose -p asap-kafka-cold ... up -d`).
2. Pushes 100 deterministic samples to the agent's OTLP/HTTP receiver
   via a transient `curlimages/curl` sidecar attached to the project
   network.
3. Sleeps ~90s for window flush + Kafka transit + gateway finalize → S3.
   Override via `KAFKA_E2E_FLUSH_WAIT=2m` if the host is slow.
4. Lists all `<ulid>/` prefixes in MinIO bucket `asap-gorilla-tsdb`
   via a transient `minio/mc:latest` sidecar.
5. For each block, downloads `chunks/000001 + index + meta.json` to
   a temp dir and opens it via `prometheus/tsdb.OpenBlock` →
   `tsdb.NewBlockQuerier`.
6. Streams every (label_set, ts, value) tuple where `__name__` matches
   the fixture metric name.
7. Asserts the read-back set is a SUPERSET of the input set, byte-
   equal on (label_set, timestamp_ms, value).
8. Tears the stack down with `docker compose down -v`.

### Tunables

| Env var                    | Default | Description                                                                |
| -------------------------- | ------- | -------------------------------------------------------------------------- |
| `KAFKA_E2E_LIVE`           | unset   | `1` = run the docker path; otherwise SKIP (CI-friendly default).           |
| `KAFKA_E2E_FLUSH_WAIT`     | `90s`   | How long to wait between OTLP push and S3 listing.                         |

## Host port assignments

The overlay exposes exactly one host port: `34317:4317` (agent OTLP
gRPC). This port range is chosen to avoid collision with:

* `mvp-multi-stage.yml` (19xxx),
* `gorilla_s3_e2e/docker-compose/e2e-overlay.yml` (29xxx),
* `base.yml` gateway/backend (14317/14318/19091).

MinIO is reused from `base.yml` and is reachable from the test driver
on host port 19000 (the test only ever talks to it via `mc` inside a
transient container, which uses the docker-internal `minio:9000`).

## Notes

* `go.mod` has only `github.com/prometheus/prometheus` for the TSDB
  block reader; everything else is std-lib. Mirrors the dep-minimal
  posture of `integration/gorilla_s3_e2e/`.
* A sibling `integration/e2e-cold-path/` test on a parallel branch is
  concurrently being written with a similar TSDB-block reader. Post-
  merge, the two should consolidate their reader helpers; this PR's
  worktree was isolated, so the duplication is intentional.
