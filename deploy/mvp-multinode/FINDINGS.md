# MVP multinode — accuracy & bandwidth findings (2026-05)

Consolidated results from the end-to-end MVP demo validation: agent (asap-otel)
→ asap-query backend, compared against raw-telemetry baselines, on the 4-node
cluster (`run_demo.sh`). Two axes were measured: **query accuracy** (asap sketch
tier vs VictoriaMetrics raw) and **wire bandwidth** (asap aggregation vs a sweep
of compression codecs).

## 1. Query accuracy (asap sketch tier vs VictoriaMetrics raw baseline)

Apples-to-apples requires: (a) per-series sketches on the latency metric (drop
`grouping_labels` so PromQL per-series semantics match raw), (b) matched metric
names (`add_metric_suffixes: false` on PRW; VM OTLP naming), (c) a deterministic
producer seed (`-seed`) so all arms see identical sample sequences.

| Query | asap rel-err vs baseline | Notes |
|---|---|---|
| `count(http_requests_total)` | 0.00% | HLL exact on this cardinality |
| `quantile_over_time(0.5, latency[5m])` | ~0.6% | DDSketch median |
| `quantile_over_time(0.99, latency[5m])` | ~8% | DDSketch tail (ε + temporal-window variance) |
| `max by (zone) (quantile_over_time(0.99, ...))` | ~8% | typed outer-agg fold (ASAPQuery-backend #297) |
| `sum by (zone) (http_requests_total)` | correct (4 zones) | after counter-fn fix #303/#304 |
| `sum by (zone) (rate(...[5m]))` | ~0.3% | after counter-fn fix |
| `topk(5, sum by (zone) (rate(...)))` | tracks rate | |

**Counter-function semantics** (ASAPQuery-backend #301/#300, fixed in #303/#304):
the `ExactAgg(Sum)` dispatch originally collapsed `sum`/`sum_over_time`/`increase`/
`rate` into one path, ignoring the function name → `rate` was 64% off. The fix
type-tracks the counter-function so each honors its PromQL semantic. `rate`
dropped 64% → 0.26%. `sum_over_time(counter[r])` now capability-misses to archive
(honest refusal — it's a quadratic-weighted cumulative sum that per-window deltas
can't cheaply reconstruct) rather than fabricating a wrong number.

DDSketch vs KLL experiment (latency metric): DDSketch wins on the max-by-zone
aggregation pattern (~9% vs ~20% rel-err) — KLL's per-series sample-reservoir
variance amplifies through the max-fold; DDSketch's bucket-midpoint estimates are
steadier. KLL retained on `request_size_bytes` for five-sketch coverage.

## 2. Wire bandwidth — aggregation vs compression

The headline question: does asap's edge sketching/aggregation reduce bandwidth?
Investigation found the dominant cost was NOT what early hypotheses assumed:

- **#400 multi-family fan-out** — wrong; routing was already one-family-per-metric
  (SET-pruning generalization landed for correctness anyway, ASAPQuery-backend #305).
- **per-series latency DDSketch (10k states)** — only ~1 Mbps.
- **raw_passthrough of the `http_requests_total` counter** — the real driver.
  The Sum-role metric was shipped raw (10k series continuously) and aggregated
  centrally. Edge-aggregating it to 4 zone-sums (ASAPCollector #404 + controller
  emit #307) cut backend ingress 63% (12.85 → 4.73 Mbps).
- **transport confound** — baselines used PRW (Snappy-compressed) while asap used
  uncompressed OTLP. Equalizing transport (matched-codec sweep #405 + backend
  gzip-accept #308) isolated aggregation from compression.

### Final sweep — compressed wire bytes leaving the edge

| Arm | Strategy | Wire (Mbps) | vs raw |
|---|---|---:|---:|
| b0 | raw OTLP, no codec | 18.59 | 1× |
| b3 | raw, **serf-XOR** (lossless) | 9.1 | ~2× |
| b2 | raw, **Snappy** (PRW) | 3.03 | ~6× |
| b1 | raw, **gzip** | 1.50 | ~12× |
| asap | **aggregated**, no codec | 0.98 | ~19× |
| asap-gzip | **aggregated + gzip** | 0.137 | ~136× |

(asap measured on backend ingress; b3 serf measured on the agent→serf-gateway
compressed hop. All represent compressed wire bytes shipped from the edge.)

**Conclusions:**
1. **Aggregation alone beats every compression-only baseline** (asap 0.98 < gzip
   1.50 < Snappy 3.03 < serf 9.1). Reducing 10k raw series → 4 zone-sums wins more
   than any lossless codec on the raw stream.
2. **Aggregation + compression compound** — asap-gzip (0.137) is the decisive
   winner, ~11× better than the best codec-only baseline and ~136× over raw.
3. **serf-XOR is the worst compressor here (~2×)** — Gorilla/serf XOR needs
   temporal smoothness (consecutive values similar → many XOR leading zeros). The
   workload is dominated by log-normal *random* latency (10k series), uncorrelated
   sample-to-sample, so XOR barely compresses; `max_diff:0.0` (lossless) also
   disables serf's best-approximation trick. serf would shine on smooth
   sensor-style data, not random latency. gzip/Snappy exploit structural/label
   repetition serf-XOR can't.

**Caveat — asap total NIC vs ingest wire:** asap also runs the fused `asap_edge`
cold tier, Gorilla-XOR-encoding the full raw stream and shipping `ASAPFRG1`
fragments to the gorilla-merger (which archives to colocated MinIO; ~3.57 Mbps) —
archival the baselines don't do. That's separate from the ingest-wire metric and
shouldn't be charged against the aggregation comparison.

## 3. Serf arm: real wire codec (not raw passthrough)

The original b3 ran `serfprocessor` (`drop_original:false`) — XOR-compressed a
blob to **local disk** while forwarding **raw** PRW to VM, so the measured wire was
raw PRW, never serf. Rebuilt (ASAPCollector #406) as a true codec: agent
`serfhttp` exporter (XOR-compress) → POST → node1 gateway `serfhttp` receiver
(decompress) → otlphttp → VM. No PRW. Required adding `serfexporter` +
`serfreceiver` to `builder-config.yaml` (only `serfprocessor` was compiled in).
Round-trip verified lossless (4 zones, 12-14 sig-digit floats preserved).

## 4. Known remaining gaps

- **ASAPCollector#381** — the deployed agent runs its static config, NOT the
  controller's OpAMP push (config mounted read-only). Controller-emit changes
  (e.g. the #307 edge-aggregation plan) don't take runtime effect until this lands;
  the static configs were edited directly to demonstrate.
- **`sum_over_time(counter[r])` without `by`** — ungrouped `_over_time` has no
  controller plan; capability-misses. Not a marquee query.
- **asap+Snappy** — tonic backend only decodes gzip/zstd at the gRPC layer; true
  Snappy parity would need a tower-layer Snappy decompressor (PRW gets Snappy free
  because it's application-level over HTTP). gzip already beats Snappy here, so low
  priority.

## 5. Reproduce

```bash
cd /mydata/ASAPCollector/deploy/mvp-multinode/scripts
bash run_demo.sh all          # b0..b3 + asap + asap-gzip, aggregate report
# per-arm: bash run_demo.sh arm <name>
```
Images must be post-#406 on all nodes (`docker save | ssh nodeN docker load`).
Accuracy probe script + NIC RX measurement notes are in the run results dir.
