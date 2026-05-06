# Eval instrumentation notes — what each sweep CSV column means

_Last updated: 2026-05-05 (paper blocker #3 closeout)._

This is the column-by-column "what is this number, where does it
come from, and why" reference for `deploy/eval-results/sweep-*.csv`
(the file format produced by
`deploy/scripts/measure-baseline.py` and chained together by
`deploy/scripts/run-baseline-sweep.sh` /
`deploy/scripts/run_e2e_sweep.sh`).

The next person to add a column or interpret one in a paper figure
should be able to land on this doc and understand the source of
truth without re-deriving it from comments scattered through
`measure-baseline.py`.

## Column map

Columns are emitted in the order shown in the CSV header. The
"source" column says which signal feeds it; the "fallback" column
is what `measure-baseline.py` consults when the primary path is
NaN. "Stack tier" is which container the signal originates from.

| Column | Source (primary) | Fallback (paper blocker #3) | Stack tier | Unit / scale |
| --- | --- | --- | --- | --- |
| `baseline` | `--baseline` CLI arg (e.g. `b3-delta`) | — | — | label |
| `scale` | `--scale` CLI arg (e.g. `N1`, `N10`) | — | — | label |
| `rate` | `--rate` CLI arg | — | — | events/s (workload knob) |
| `cardinality` | `--cardinality` CLI arg | — | — | distinct series (workload knob) |
| `producer_cpu_cores` | `docker stats` CPU% / 100 on `--producer-container` | — | producer (`fake-exporter`) | cores |
| `producer_rss_mib` | `docker stats` mem on `--producer-container` | — | producer | MiB |
| `producer_bytes_out_per_s` | `docker stats` net tx delta on `--producer-container`, divided by `--bytes-sample-window` | — | producer | bytes/s on the wire |
| `agent_cpu_cores` | `rate(otelcol_process_cpu_seconds_total{job="agents"})` | — | agent | cores |
| `agent_rss_mib` | `otelcol_process_memory_rss_bytes{job="agents"}` / 1MiB | — | agent | MiB |
| `agent_in_kib_per_s` | `rate(otelcol_asapcollector_processor_input_bytes_total{job="agents"})` / 1024 | `docker stats` net rx avg across `docker-compose-agent-*` / 1024 | agent | KiB/s |
| `agent_out_kib_per_s` | `rate(otelcol_asapcollector_processor_output_bytes_total{job="agents"})` / 1024 | `docker stats` net tx avg across `docker-compose-agent-*` / 1024 | agent | KiB/s |
| `agent_points_per_s` | `rate(otelcol_receiver_accepted_metric_points_total{job="agents"})` | — | agent | points/s |
| `gateway_cpu_cores` | `rate(otelcol_process_cpu_seconds_total{job="gateway"})` (with v0.108 fallback) | — | gateway | cores |
| `gateway_rss_mib` | `otelcol_process_memory_rss_bytes{job="gateway"}` / 1MiB | — | gateway | MiB |
| `gateway_points_per_s` | `rate(otelcol_receiver_accepted_metric_points_total{job="gateway"})` (with v0.108 fallback) | — | gateway | points/s |
| `gateway_out_series_per_s` | `rate(otelcol_exporter_sent_metric_points_total{job="gateway"})` (with v0.108 fallback) | — | gateway | series/s |
| `backend_cpu_pct` | `docker stats` CPU% on `docker-compose-backend-1` | — | backend | percent of one core |
| `backend_rss_mib` | `docker stats` mem on `docker-compose-backend-1` | — | backend | MiB |
| `backend_samples_per_s` | `rate(asap_ingest_samples_total)` (backend `:9091/metrics`) | `rate(otelcol_exporter_sent_metric_points_total{exporter=~"otlp.*backend.*",job="gateway"})` | backend (or gateway when fallback) | samples/s |
| `backend_query_p99_ms` | `1000 * histogram_quantile(0.99, rate(asap_query_duration_seconds_bucket))` (backend `:9091/metrics`) | `--replay-jsonl PATH` → p99 of successful `duration_ms` rows in client JSONL | backend (server-side) or replay client (client-side) | ms |

## Caveats — do not paper over these

### `agent_in_kib_per_s` / `agent_out_kib_per_s`: in-process bytes vs wire bytes

The patched-processor counter
(`otelcol_asapcollector_processor_*_bytes_total`) measures the
**OTLP protobuf MessageSize of the in-process `pmetric.Metrics`
batch as it crosses the processor boundary**, computed by the
`pmetric.ProtoMarshaler` in
`opentelemetry-collector-patch/processor/selfmonitor/selfmonitor.go`.
That's not the same number as bytes-on-the-wire:

  * It excludes gRPC framing, HTTP/2 headers, and the OTLP
    request envelope.
  * For sketch payloads (DDSketch / HLL / etc.), the in-process
    bytes include the typed proto envelope's full state — the
    same payload that goes on the wire — so the two measures
    agree to within ~5%.
  * For raw / Gorilla / Serf baselines that have no patched
    processor, no in-process counter fires at all. The fallback
    is `docker stats` net rx/tx on the agent container, which
    IS the on-the-wire rate.

When comparing across baselines (the bandwidth claim in
`docs/paper-outline.md` claim #1), prefer the `docker stats`
fallback as the apples-to-apples ground truth — note this
explicitly in the figure caption. The patched-processor counter
is the right signal for "bytes the sketch processor saw"; the
wire bytes are the right signal for "bytes the bandwidth budget
spent."

### `backend_samples_per_s`: backend ingest vs gateway egress

Today's `asap/query-backend:dev` image does NOT expose
`asap_ingest_samples_total` to Prometheus. The `:9091/metrics`
surface only contains query-side counters
(`asap_query_duration_seconds`, `asap_query_requests_total`).
The fallback uses
`otelcol_exporter_sent_metric_points_total{exporter=~"otlp.*backend.*",job="gateway"}`,
which counts the metric points the gateway forwarded to the
backend over OTLP. Modulo dropped batches (a small number
tracked elsewhere), gateway-egress equals backend-ingress, so
this is the right proxy.

For raw / Gorilla / Serf baselines that drop the OTLP forward
(`drop_original: true` in the agent yaml), the gateway is
literally not receiving anything from the agent — so this
fallback returns 0, which is correct: those baselines write
their compressed output to a local container directory, not to
the backend. The bandwidth signal for those baselines lives in
`agent_*_kib_per_s` (docker-stats fallback).

The proper fix is a backend-side counter — that's tracked as a
follow-up in the ASAPQuery-backend repo. Fallback is good
enough for the paper.

### `backend_query_p99_ms`: server-side vs client-side p99

`measure-baseline.py` chooses the client-side fallback whenever
`--replay-jsonl PATH` is provided, because:

  * The Prometheus path dies with the stack
    (`docker compose down -v` between cells in
    `run_e2e_sweep.sh`), so the histogram is gone before the
    next cell can inspect it. The replay JSONL persists on the
    host filesystem.
  * Client-side latency is what the caller actually
    experienced, including any network / OTLP serialization
    delay — which IS what the paper claim ("query latency
    competitive with raw") is about.

The numbers will not be identical: client-side adds the
local-loopback HTTP round-trip (~0.1-0.5ms on the dev box).
For the paper figure, document which side the number came from
in the caption. `run_e2e_sweep.sh` always passes
`--replay-jsonl`, so e2e-sweep CSVs are always client-side p99.
`run-baseline-sweep.sh` is client-side only when
`DRIVE_QUERIES=1` is set.

### `asap_query_duration_seconds` only fires on serviced queries

Empty-but-running stack → the histogram has 0 buckets. So the
primary-path PromQL returns NaN for any cell where no queries
were issued during the soak. That's not a bug, that's the
metric's contract; just make sure the sweep driver issues some
queries before reading the column. `run_e2e_sweep.sh` does this
by default (it runs `promql_replay.py` for the full soak).
`run-baseline-sweep.sh` did NOT prior to 2026-05-05; the
`DRIVE_QUERIES=1` opt-in flag added in this paper-blocker-#3
work fills the gap.

## Signal coverage by baseline (post paper-blocker-#3)

| Baseline | `agent_*_kib_per_s` | `gateway_*` | `backend_samples_per_s` | `backend_query_p99_ms` |
| --- | --- | --- | --- | --- |
| `b0a-raw-stream` | docker stats fallback | direct | gateway-fallback | client-side (when DRIVE_QUERIES) |
| `b0b-raw-batched` | docker stats fallback | direct | gateway-fallback | client-side (when DRIVE_QUERIES) |
| `b1-serf` | docker stats fallback | direct (zero — drop_original) | gateway-fallback (zero) | client-side (no warm answers — cold path) |
| `b2-full` | direct (sketch processor) | direct | direct (when ingest counter exists) or gateway-fallback | direct (when histogram is fed) |
| `b3-delta` | direct (sketch processor) | direct | direct or gateway-fallback | direct or client-side |
| `b4-tunable` | direct (sketch processor) | direct | direct or gateway-fallback | direct or client-side |
| `b5-gorilla` | docker stats fallback | direct (zero — drop_original) | gateway-fallback (zero) | client-side (no warm answers — cold path) |

Note that B1/B5 baselines are **expected** to show ~0
gateway-egress and 0 backend-samples: their pipeline is
`drop_original: true`, so the wire path to backend is empty by
design — the bandwidth signal lands in `agent_*_kib_per_s`
(docker-stats fallback) or in compressed-blob-on-disk metrics
that aren't in the CSV today.

## Adding a new column

1. Define the source. Prefer Prometheus over `docker stats`
   (Prom is rate-aware; docker stats requires the two-sample
   pass).
2. If the source isn't universal across baselines, add a
   fallback to `FALLBACK_QUERIES` in
   `deploy/scripts/measure-baseline.py` (or in `main()` for
   non-Prom fallbacks). Don't silently let the column NaN —
   one of the bandwidth-claim figures was unreproducible for a
   week because of exactly that.
3. Add a row to the column map above. Mention any unit subtlety
   in the caveats section.
4. Smoke-test by running
   `deploy/scripts/run-baseline-sweep.sh DRIVE_QUERIES=1` and
   confirming the column has no NaN for any baseline.
