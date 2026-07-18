# NitroSketch SDK-to-collector end-to-end tradeoff (corrected)

Real trace: DEBS 2022 Grand Challenge trading data, first 300000 rows, 5493 distinct tickers, CountSketch rows=4, point-query target = top-20 heaviest tickers by true frequency.

**Corrects PR #544**, which benchmarked `CountSketchWrapper.WithSampleP` — collector-side coordinated sampling (the collector receives 100% of the wire traffic; only sketch-compute is saved). This eval instead drives the REAL NitroSketch-style path end to end: SDK-side admission (`common.GeometricSampler`, the same type `rowSampledSketchValues.measure()` uses) decides row admission BEFORE serialization; a fully-skipped occurrence is never serialized, sent, or processed by the collector at all. Every stage below is real production code: the SDK-side sampler, `pmetric.ProtoMarshaler`/`ProtoUnmarshaler` (the actual OTLP wire codec), and a real `*asapEdgeProcessor.ConsumeMetrics`.

Baseline (sample_p=1.0, width=2048): mean relative error = 0.0687

## End-to-end CPU cost (rows=4, width=2048 — SDK admit + OTLP marshal/unmarshal + real ConsumeMetrics)

| sample_p | occurrences sent (of 300000) | send fraction | total ns | ns/occurrence (over ALL, incl. skipped) | vs p=1.0 |
|---|---|---|---|---|---|
| 1.0000 | 300000 | 1.0000 | 1957127693 | 6523.8 | 1.000x |
| 0.7500 | 298794 | 0.9960 | 1814036691 | 6046.8 | 0.927x |
| 0.5000 | 280925 | 0.9364 | 1447792901 | 4826.0 | 0.740x |
| 0.2500 | 204914 | 0.6830 | 955699101 | 3185.7 | 0.488x |
| 0.1250 | 123761 | 0.4125 | 986928307 | 3289.8 | 0.504x |
| 0.0625 | 68629 | 0.2288 | 476074648 | 1586.9 | 0.243x |

## Memory needed to hold accuracy constant

Minimum width (from {[512 1024 2048 4096 8192 16384]}) whose mean relative error <= the p=1.0/w=2048 baseline (0.0687):

| sample_p | min width for baseline accuracy | memory ratio vs w=2048 | achieved rel_err |
|---|---|---|---|
| 1.0000 | 2048 | 1.00x | 0.0687 |
| 0.7500 | 4096 | 2.00x | 0.0324 |
| 0.5000 | 4096 | 2.00x | 0.0413 |
| 0.2500 | 4096 | 2.00x | 0.0511 |
| 0.1250 | 4096 | 2.00x | 0.0619 |
| 0.0625 | not reached within swept range (up to 16384) | - | 0.0959 at w=16384 |

## Full accuracy grid (mean relative error, top-20 heavy hitters)

| sample_p \ width | 512 | 1024 | 2048 | 4096 | 8192 | 16384 |
|---|---|---|---|---|---|---|
| 1.0000 | 0.1880 | 0.1750 | 0.0687 | 0.0286 | 0.0119 | 0.0254 |
| 0.7500 | 0.1858 | 0.1744 | 0.0748 | 0.0324 | 0.0226 | 0.0355 |
| 0.5000 | 0.1911 | 0.1689 | 0.0694 | 0.0413 | 0.0219 | 0.0386 |
| 0.2500 | 0.2094 | 0.1715 | 0.1004 | 0.0511 | 0.0449 | 0.0606 |
| 0.1250 | 0.1855 | 0.1731 | 0.0915 | 0.0619 | 0.0572 | 0.0724 |
| 0.0625 | 0.2482 | 0.1815 | 0.1293 | 0.0968 | 0.0842 | 0.0959 |

## Methodology and honest caveats

- **This isolates the SAMPLING mechanism's own effect, not "row-sampled export vs traditional periodic sketch-state export."** The RowSampledSketch wire model sends ONE message per admitted occurrence (see the transform doc: "a row-sampled point carries no sketch STATE at all"), which is architecturally a different (and at sample_p=1.0, COSTLIER per-window) wire model than a plain CountSketch aggregation that accumulates in memory and exports one periodic full/delta sketch-state message regardless of occurrence count. Comparing sample_p=1.0 vs sample_p<1.0 WITHIN the row-sampled path (as this eval does) is the correct comparison for measuring what sampling itself buys you; comparing the row-sampled path against the traditional per-window sketch export is a separate, larger question this eval does not address.
- All admitted occurrences from one dataset pass are batched into ONE OTLP message, matching how a real SDK export cycle amortizes fixed per-message overhead (resource/scope wrapping, protobuf framing) across many points. A real deployment would flush periodically in smaller batches, not one batch of the whole dataset — this changes how OFTEN the fixed overhead is paid, not the marginal per-point cost this eval reports.
- The SDK-side sampler uses the SAME fixed constant seed sketchlib-go's own `countSketchSampleSeed`/`rowSampledSketchValues.targetFor` convention uses in production ("seeds its own RNG from the supplied seed so producers are reproducible") — deterministic across runs by design, not an eval artifact. Whether sharing one seed across every series of a given policy/AggID could correlate admission timing across series with similar arrival patterns has not been analyzed here; tracked separately.
- The accuracy grid is not perfectly monotonic in width even at sample_p=1.0 (every width sees the IDENTICAL 300k observations at p=1.0, since Admit() always returns true and draws no RNG) — e.g. width=16384 shows slightly WORSE mean relative error than width=8192. This is genuine CountSketch row-hash noise (the sketch's own internal hash function differs by width, so the SAME 20 heavy-hitter keys can land in a marginally less favorable collision pattern at a larger width by chance) rather than a sampling effect — read the coarse trend, not individual grid cells, same caveat as PR #544's own single-seed note.
