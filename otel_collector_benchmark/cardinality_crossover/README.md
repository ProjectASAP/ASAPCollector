# Cardinality Crossover Sweep (CMS / CountSketch vs. raw)

This micro-benchmark identifies the **series-cardinality break-even point**
at which a fixed-size sketch becomes smaller than transmitting the raw
`(series_id, value)` tuples. It produces the data needed for the paper's
"sketch wins above N=X" chart.

## What this experiment shows

A CountSketch / CountMinSketch matrix has a **constant** size determined by
its `(rows, cols)` dimensions, independent of how many distinct series are
fed into it. Raw transmission, by contrast, costs **O(N)** bytes per series.
Therefore:

- At **small N**, the fixed sketch matrix is *bigger* than raw.
- At **large N**, the sketch is *smaller* than raw — by orders of magnitude.

The sweep records, for each `N ∈ {100, 1k, 10k, 100k, 1M, 5M}`:

| column                  | meaning                                                                 |
| ----------------------- | ----------------------------------------------------------------------- |
| `n_series`              | distinct series count fed into the sketch                              |
| `sketch_type`           | `CountSketch` or `CountMinSketch`                                       |
| `dim_label`             | `default` (5 × 2048) or `narrowed` (3 × 512)                            |
| `rows`, `cols`          | matrix dimensions                                                       |
| `sketch_bytes`          | bytes produced by `sketch.SerializeToBytes()`                          |
| `raw_bytes_min`         | `N × 16` — lower bound: 8B series_id + 8B value                         |
| `raw_bytes_realistic`   | `N × (16 + 32)` — adds an OTLP envelope per series                      |
| `ratio_min`             | `sketch_bytes / raw_bytes_min`                                          |
| `ratio_realistic`       | `sketch_bytes / raw_bytes_realistic`                                    |
| `top_k_relative_error`  | mean rel. error of sketch's estimate for the 10 heaviest ground-truth values |
| `insert_seconds`        | wall time of the insert loop (sanity check)                            |

A `ratio_*` value greater than 1 means the sketch is bigger than raw at
that `N`; less than 1 means the sketch wins. The top-K relative error
column is reported alongside so the chart can show that the sketch is
still useful in the regime where it wins on bytes.

The **narrowed variant** (3 × 512) shows the cost-vs-accuracy trade-off:
the matrix shrinks ~6.7× but top-K error rises.

## Tuple generation

- `series_id` is the iteration index `i ∈ [0, N)` (so each tuple has a
  distinct series_id).
- `value` is drawn from a Zipf distribution `s = 1.5, v = 1`, `imax = 2^20`.
  Heavy values appear many times — exactly the regime where a sketch is
  asked to find heavy hitters.
- The seed is fixed (`42`) so the sweep is deterministic.

The sketch keys on `value` (encoded as 8-byte little-endian via
`common.FromU64`). This is the realistic deployment scenario where many
series share the same heavy bucket and the sketch summarises that
distribution.

## Dimensional choices

- Spec asked for `cols = 2000`. `sketchlib-go`'s CountSketch requires
  `cols` to be a power of two (it derives the column index by bitmask on
  the precomputed hash, see
  `sketchlib-go/sketches/CountSketch/CountSketch.go`). We round up to
  `2048`, which is also the `DefaultCols` constant in that file. The
  collector processor's countminsketch factory uses `cols=1024` by
  default — we keep `2048` here for an apples-to-apples comparison
  between CMS and CountSketch (same matrix shape, same byte budget).
- Hash function: `common.Hash64` from sketchlib-go (xxh3-based). We do
  not roll our own — we want sketch behaviour to match what the
  in-collector processor produces.
- Insert path: `Insert(*common.SketchInput)` for both sketches, fed by
  `common.FromU64(value)` so that the input is normalised the same way
  it would be inside the collector pipeline.

## Build

```
cd otel_collector_benchmark/cardinality_crossover
go build ./...
```

The module's `go.mod` uses a `replace` directive that points
`github.com/ProjectASAP/sketchlib-go` at the sibling checkout
(`../../../sketchlib-go`).

## Run

### Smoke run (seconds, just N=10k)

```
go run . -smoke
```

or via the wrapper:

```
../bench_cardinality_crossover.sh --smoke
```

Output: `results/cardinality_crossover_smoke.csv`. Use this to confirm
the binary builds and writes a CSV before committing to the full sweep.

### Full sweep

```
go run .
```

or:

```
../bench_cardinality_crossover.sh
```

Output: `results/cardinality_crossover.csv`.

### Expected runtime

| N        | per-N work (insert + serialize) approx |
| -------- | -------------------------------------- |
| 100      | < 1 ms                                 |
| 1 000    | ~1 ms                                  |
| 10 000   | ~5 ms                                  |
| 100 000  | ~50 ms                                 |
| 1 000 000| ~0.5 – 1 s                             |
| 5 000 000| ~3 – 8 s                               |

Times are per (sketch_type × dim_label) combination, of which there are
four. The full sweep finishes on the order of **30 s – 1 min** on a
modern desktop. The 5M tier dominates the wall-clock; if you only need
the crossover region you can edit `ns` in `main.go`.

## Output

`results/cardinality_crossover.csv`. Each row is one (N, sketch_type,
dim_label) triple. The CSV is suitable as direct input to `pandas.read_csv`
or any plotting tool. Suggested chart: `sketch_bytes` and
`raw_bytes_realistic` on the y-axis (log scale) vs. `n_series` on the
x-axis (log scale), with a vertical line where the curves cross (the
"break-even" cardinality). Plot the four `(sketch_type, dim_label)`
combinations as separate series.
