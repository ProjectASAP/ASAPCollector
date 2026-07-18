# NitroSketch-style sampling: CPU reduction vs memory tradeoff

Real trace: DEBS 2022 Grand Challenge trading data, first 2000000 rows, 5493 distinct tickers, CountSketch rows=4, point-query target = top-20 heaviest tickers by true frequency.

Baseline (sample_p=1.0, width=2048): mean relative error = 0.0092

## CPU cost per insert (rows=4, width=2048 — insert cost is O(rows), not O(width))

| sample_p | ns/insert | vs p=1.0 |
|---|---|---|
| 1.0000 | 313.2 | 1.000x |
| 0.7500 | 474.7 | 1.516x |
| 0.5000 | 427.4 | 1.365x |
| 0.2500 | 295.7 | 0.944x |
| 0.1250 | 202.7 | 0.647x |
| 0.0625 | 124.2 | 0.397x |

## Memory needed to hold accuracy constant

Minimum width (from {[512 1024 2048 4096 8192 16384]}) whose mean relative error <= the p=1.0/w=2048 baseline (0.0092):

| sample_p | min width for baseline accuracy | memory ratio vs w=2048 | achieved rel_err |
|---|---|---|---|
| 1.0000 | 2048 | 1.00x | 0.0092 |
| 0.7500 | 4096 | 2.00x | 0.0088 |
| 0.5000 | 8192 | 4.00x | 0.0077 |
| 0.2500 | not reached within swept range (up to 16384) | - | 0.0098 at w=16384 |
| 0.1250 | 8192 | 4.00x | 0.0079 |
| 0.0625 | not reached within swept range (up to 16384) | - | 0.0131 at w=16384 |

## Full accuracy grid (mean relative error, top-20 heavy hitters)

| sample_p \ width | 512 | 1024 | 2048 | 4096 | 8192 | 16384 |
|---|---|---|---|---|---|---|
| 1.0000 | 0.0503 | 0.0230 | 0.0092 | 0.0074 | 0.0036 | 0.0011 |
| 0.7500 | 0.0488 | 0.0241 | 0.0094 | 0.0088 | 0.0058 | 0.0037 |
| 0.5000 | 0.0488 | 0.0241 | 0.0093 | 0.0108 | 0.0077 | 0.0054 |
| 0.2500 | 0.0511 | 0.0245 | 0.0138 | 0.0143 | 0.0128 | 0.0098 |
| 0.1250 | 0.0446 | 0.0235 | 0.0134 | 0.0123 | 0.0079 | 0.0104 |
| 0.0625 | 0.0573 | 0.0252 | 0.0199 | 0.0164 | 0.0155 | 0.0131 |

## Methodology and honest caveats

- **The geometric sampler uses a FIXED constant seed** (`countSketchSampleSeed`, production code, not eval-specific) — every (sample_p, width) cell here is a single deterministic draw, not an average over independent trials. The accuracy grid's small non-monotonic blips (e.g. a lower sample_p occasionally showing marginally better error than a higher one at the same width) are single-seed noise, not a real effect — read the coarse trend (lower sample_p needs more memory), not individual cells, as the finding. Re-running over multiple seeds and averaging would tighten this if a precise multiplier is needed.
- **CPU only nets a REAL reduction below roughly sample_p<=0.25 in this measurement — sample_p=0.75 and 0.5 are actually SLOWER than unsampled (p=1.0).** Verified this is not a measurement-order artifact (re-ran with the sample_p sweep reversed; the raw ns/insert per p barely moved). The geometric sampler itself has a real fixed per-item bookkeeping cost (gap countdown + row-admission decision) that must be paid on EVERY item regardless of whether it's ultimately admitted; at high sample_p most items ARE admitted, so you pay full hash+insert cost PLUS that bookkeeping overhead — strictly worse than the unsampled path. Net savings only appear once the skip fraction is large enough to outweigh that fixed cost. This is the opposite of the naive assumption that any sample_p<1 saves CPU proportionally — it does not, until sample_p drops low enough.
- Point-query target is a fixed top-20 heavy-hitter set by TRUE frequency; a workload with a flatter frequency distribution (fewer, less extreme heavy hitters) would likely need proportionally MORE memory to hold the same accuracy under sampling, since the relative sampling noise matters more for lower-frequency items.
