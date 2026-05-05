# matched-accuracy quantile-sketch head-to-head

Measures the **wire / CPU / memory cost** of every quantile algorithm we care
about when each one is configured to land at roughly the same accuracy target
(~1% relative error at p99). Runs entirely in-process; no Docker, no
collector, no network.

## algorithms

| algorithm        | configuration         | source                                    |
|------------------|-----------------------|-------------------------------------------|
| `ddsketch_a0.01` | α = 0.01              | `sketchlib-go/sketches/DDSketch`          |
| `kll_k200`       | k = 200, m = 8        | `sketchlib-go/sketches/KLL`               |
| `tdigest_c100`   | compression = 100     | `github.com/caio/go-tdigest` (see note)   |
| `hdr_sigfig3`    | range 1..1e7, 3 sf    | `github.com/HdrHistogram/hdrhistogram-go` |
| `linhist_b100`   | 100 linear buckets    | inline (`linHist` in `main.go`)           |
| `raw_samples`    | float64 array         | inline                                    |
| ~~Gorilla~~      | skipped               | see "what got skipped" below              |

### note on the t-digest module

The task spec asked for `github.com/influxdata/tdigest`. That module is **not
present in this user's `GOMODCACHE`** (`/data2/zeying/go-mod`), and the
sandbox cannot fetch it from the network. `github.com/caio/go-tdigest` v3.1.0
**is** cached (it's a transitive dep of `telegraf-patch`), is pure Go,
MIT-licensed, and implements the identical algorithm. Substituted with a
comment in `main.go`. To switch to influxdata/tdigest later:

```diff
-import tdigest "github.com/caio/go-tdigest"
+import "github.com/influxdata/tdigest"
```

…and replace the `td.Add` / `td.Quantile` / `td.AsBytes` calls with
influxdata's equivalents (`Add(value, weight)`, `Quantile(q)`, no built-in
binary marshaller — you'd ship the centroid array yourself).

### what got skipped

* **Gorilla.** Gorilla is **a (timestamp, value) lossless compressor,
  not a quantile sketch.**

  In a matched-p99-error table Gorilla either has to store every sample (in
  which case its wire size is necessarily larger than DDSketch / KLL by
  construction) or have a quantile estimator hand-rolled on top — at which
  point the comparison is no longer "Gorilla". A separate "lossless
  time-series" track is the right home for it; left as a `// TODO` in
  `main.go`.

## what gets measured

For every algorithm:

| field               | how                                                       |
|---------------------|-----------------------------------------------------------|
| `wire_bytes`        | `SerializeToBytes` (sketchlib), `AsBytes` (t-digest), zero-copy `Export().Counts` × 8 + 32-byte header (HDR), inline encoder (linhist), `len(samples)*8` (raw) |
| `insert_ns_per_op`  | wall-clock `time.Now()` / `time.Since` over the whole stream, divided by N |
| `heap_delta_bytes`  | `runtime.ReadMemStats` HeapAlloc, after − before, with a `runtime.GC()` before the start sample |
| `p50_est` / `p99_est` | algorithm's own quantile query                          |
| `p50_truth` / `p99_truth` | exact percentile from the sorted input              |
| `p99_rel_err`       | `|p99_est − p99_truth| / p99_truth`                       |

## running

### smoke run (one skew, 100k samples — what's used for CI)

```bash
cd otel_collector_benchmark/matched_accuracy
go build ./...
go run ./ --skew 1.5 --n 100000
```

A single CSV is emitted at `results/matched_accuracy.csv` plus a markdown
table on stdout.

### full sweep (three skews, 1M samples each)

From the parent `otel_collector_benchmark/` directory:

```bash
./bench_matched_accuracy.sh
```

This runs skews `{1.01, 1.5, 2.5}` back-to-back, appending all rows into
`matched_accuracy/results/matched_accuracy.csv`. A 1M-sample stream takes
about 60 seconds for the slowest algorithm (t-digest); the whole sweep is
~3-5 minutes on a laptop.

### one skew only (full size)

```bash
./bench_matched_accuracy.sh --skew 1.5 --n 1000000
```

## flags

| flag         | default                          | meaning                                    |
|--------------|----------------------------------|--------------------------------------------|
| `--skew`     | 1.5                              | Zipf parameter `s` (must be > 1.0)         |
| `--n`        | 1_000_000                        | stream length                              |
| `--seed`     | 0xC0FFEE                         | PRNG seed (fixed for reproducibility)      |
| `--out`      | `results/matched_accuracy.csv`   | CSV destination                            |
| `--append`   | false                            | append rather than overwrite (used by the shell driver to combine multiple skews) |

## output schema

```
skew, algorithm, samples,
p50_est, p50_truth,
p99_est, p99_truth, p99_rel_err,
wire_bytes, insert_ns_per_op, heap_delta_bytes
```

## sample smoke-run output (skew = 1.5, n = 100k)

```
| algorithm       | p99 est | p99 truth | p99 rel-err | wire bytes | ns/op | heap Δ bytes |
|---              |    ---: |      ---: |        ---: |       ---: |  ---: |         ---: |
| ddsketch_a0.01  | 4493.02 |   4497.00 |      0.0009 |       1153 |  32.7 |        24920 |
| kll_k200        | 4643.00 |   4497.00 |      0.0325 |       1075 |  98.3 |       228840 |
| tdigest_c100    | 4503.53 |   4497.00 |      0.0015 |       4064 | 479.6 |        21912 |
| hdr_sigfig3     | 4499.00 |   4497.00 |      0.0004 |     122912 |   5.3 |       123008 |
| linhist_b100    | 4700.52 |   4497.00 |      0.0453 |        848 |   3.4 |          976 |
| raw_samples     | 4497.00 |   4497.00 |      0.0000 |     800000 |   0.6 |       802816 |
```

Note that on this run `kll_k200` and `linhist_b100` did not hit the ~1% p99
target — KLL's worst-case error scales as O(1/k · log(1/δ)) and at k=200 you
get a 90th-pct error of ~1.3%, but the realised p99 error on a single
high-tail Zipf draw can be larger and varies run-to-run because **the KLL
implementation in `sketchlib-go` seeds its compactor coin from
`time.Now().UnixNano()`** — see `kll.go:newCoin`. The Zipf input stream is
fully reproducible (we pass `--seed`), but KLL's internal randomness is not.
For the paper table we'll either:

1. add a deterministic-coin constructor to `sketchlib-go/sketches/KLL` and
   use it here (preferred), or
2. average each KLL row over N=10 seeds and report mean ± stddev, or
3. bump k to ~400-500 so even the worst draw clears 1%.

The `ddsketch` / `hdr` / `tdigest` / `raw` / `linhist` rows are deterministic
given a fixed input seed — their wire/CPU/heap numbers are stable.
