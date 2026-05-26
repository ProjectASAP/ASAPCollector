# ASAP cost model + AWS dollar-cost simulator

A standalone evaluation simulator that estimates the **per-component ×
per-resource** cost of the MVP-multinode arms and prices them in **USD/month**
using a configurable AWS price book. It answers: *what does ProjectASAP cost on
AWS vs the raw-telemetry baselines (b0/b1/b2/b3), and where does the money go?*

## Why standalone (not the control_plane optimizer cost model)

`ASAPQuery-backend/control_plane/src/optimizer/cost/` already has a cost model,
and we reuse its **concepts** (the `CloudPricing` price-book idea in `tco.rs`,
the per-family `benchmark_table` sketch-state sizes in `mod.rs`). But that code
is a **planner-internal heuristic**: `score_with()` ranks sketch *candidates*
for one query (abstract "dollars", monotonic-not-calibrated), and `tco.rs` is a
2-way *before/after* (Grafana-Cloud raw vs sketch) TCO. Neither models the
actual deployed component set (VM, Thanos query/store/compact, MinIO, gorilla-
merger, data-plane), the six run_demo arms, the cold-format PUT-count economics,
or calibrates to the measured `FINDINGS.md` bandwidth. This simulator is an
**eval artifact** — it lives next to the run_demo harness, is calibrated to the
same runs, and is meant to be edited by whoever runs the demo. Bolting it into
the Rust planner would couple a measurement/eval tool to the request path and
force the planner's abstract-dollar contract onto a concrete AWS price book.

## Run

No build step, stdlib-only (Python 3.9+). `tabulate` and `pytest` are optional;
a plain-text fallback renders tables and a stdlib test runner works without
pytest.

```bash
cd deploy/mvp-multinode            # the package is cost_model/ under here

# Full MVP arm comparison: $/arm + per-component breakdown + tradeoff
python3 -m cost_model.simulator

# One arm, full per-component × per-resource + $ breakdown
python3 -m cost_model.simulator --arm asap
python3 -m cost_model.simulator --arm b0

# Config sweeps
python3 -m cost_model.simulator --sweep cold       # cold-format PUT/storage $
python3 -m cost_model.simulator --sweep sampling   # sampling-p edge-CPU $

# Knob overrides
python3 -m cost_model.simulator --cold-format fragment
python3 -m cost_model.simulator --no-batch          # pre-#442 tiny-part PUTs
python3 -m cost_model.simulator --sample-p 0.5
python3 -m cost_model.simulator --retention-days 90
python3 -m cost_model.simulator --internet-egress   # charge wire at internet rate
python3 -m cost_model.simulator --workload mvp-5sketch

# Reproduce the FINDINGS bandwidth numbers (asserts % error)
python3 -m cost_model.validate

# Tests
python3 -m cost_model.test_cost_model      # stdlib runner
python3 -m pytest cost_model/              # if pytest installed
```

## Inputs / outputs

**Inputs** (`workloads.py`):
- `Workload` — the data-generation operating point, **shared by all arms**:
  `series`, `sample_hz`, `metrics` (name + sketch family + aggregate_by + tier
  + sample_p), `group_cardinality`, `retention_days`, `queries_per_sec`. The
  `mvp` preset mirrors `topology.env` + `configs/asap/mvp-workload.yaml`.
- `ASAPConfig` — ASAP-only knobs: `cold_format` (`intchunk`|`fragment`),
  `cold_part_batched` (#442), `window_duration_sec`, `default_sample_p`.
- `pricing.Pricing` — the editable AWS price book (below).

**Outputs**:
- a **ResourceTable** — per component × per resource physical quantities
  (`cpu_vcpu`, `mem_gb`, `egress_gb_mo`, `ebs_gb`, `s3_storage_gb`,
  `s3_put_per_mo`, `s3_get_per_mo`),
- a **CostTable** — each priced in USD/month (compute / storage / requests /
  egress), totalled per component and per arm,
- an **arm comparison** with total $/mo, $/GB-ingested, the dominant cost term
  per arm, and the ASAP-vs-b0..b3 delta.

## Components (match run_demo.sh containers)

| arm | components |
|---|---|
| b0/b1/b2 | `raw_agent`, `vm` |
| b3 | `raw_agent`, `serf_gateway`, `vm` |
| asap, asap-gzip | `edge_agent`, `data_plane`, `gorilla_merger`, `minio`, `thanos_query`, `thanos_store_gateway`, `thanos_compact`, `controller` |

Producers (the metric source, identical across arms) are excluded — they cancel
out of the comparison.

## AWS price book (`pricing.py` — single editable module)

Region **us-east-1**, on-demand, Linux, list price 2026-05. Edit `Pricing` or
pass an override into `simulate()` to re-price.

| resource | rate | basis |
|---|---|---|
| EC2 compute | **$0.0264/vCPU-hr + $0.0054/GB-RAM-hr** | m6i.large ($0.096/hr, 2 vCPU, 8 GiB) split ~55% compute / 45% memory — the resource-decomposed view (Fargate-style) so each component is charged for the vCPU-fraction + RAM it uses |
| S3 storage | $0.023/GB-month | Standard, first 50 TB |
| S3 PUT | **$0.005 / 1,000** | drives the cold object-count cost |
| S3 GET | **$0.0004 / 1,000** | store-gateway block discovery + query reads |
| EBS gp3 | $0.08/GB-month | raw TSDB (VM) disk + ASAP warm-tier disk |
| data egress | $0.09/GB internet, **$0.01/GB cross-AZ** | edge→backend wire; defaults to cross-AZ (realistic multi-AZ deploy) |

Instance assumptions: general-purpose m6i.large reference; the edge agent could
run on burstable `t3.medium` and the VM/store-gateway on memory-optimised
`r6i` — alternates in `ALT_INSTANCE_RATES`. The decomposed $/vCPU-hr + $/GB-hr
avoids whole-instance rounding so a 0.1-vCPU controller isn't charged a whole box.

## Calibration: measured vs assumed (`coefficients.py`)

Every constant is tagged `MEASURED` / `DERIVED` / `DESIGN` / `ASSUMED` with an
inline citation. Sources: `FINDINGS.md` (wire sweep), the
`results/mvp-multinode-20260510-065843/container-summary-*.csv` (per-container
CPU%/mem), the holistic-compression design doc + ASAPCollector #442 (cold codec
ratios), and processor `config.go` defaults.

**MEASURED (calibration ground truth):**
- Per-arm wire Mbps (b0 18.59 … asap 0.98 … asap-gzip 0.137) — `validate.py`
  reproduces these at **0.00% error** at the MVP operating point.
- Per-component CPU vCPU + memory at the 10k-series point (edge_agent 2.385
  vCPU vs raw_agent 0.255; data_plane 0.123; vm 0.63; minio/thanos/controller).
- Cold raw stream ~3.57 Mbps (FINDINGS caveat).

**DESIGN (doc / source defaults):** intchunk 2.33× vs Gorilla-XOR; sketch-state
sizes (DDSketch 4 KB, HLL 16 KB, …); window 60 s, gorillas3 window_interval 5 s,
block_duration 60 s, shard_count 4; raw TSDB ~1.3 B/sample on-disk.

**ASSUMED — pending live measurements in progress on the cluster** (each carries
a `# TODO(live)` in `coefficients.py`; refine these first):
- `SAMPLING_CPU_EXPONENT = 0.6` — realized edge-CPU reduction at sampling p
  (the p=0.5 sampling-bench will fix this).
- `ASAP_WIRE_HZ_EXPONENT = 0.15` — how asap wire grows with scrape Hz.
- `gorilla_merger` CPU/mem — the split-out merger container isn't in the 0510
  run (cold ingest was colocated in the agent then).
- `THANOS_GETS_PER_BLOCK_SYNC = 3`, `THANOS_COMPACT_REWRITE_FACTOR`,
  CPU/mem idle-fractions — coarse; refine from MinIO access logs + container
  metrics on the next full run.

## Headline result (MVP workload, us-east-1, 30-day retention)

```
arm        wire Mbps   TOTAL $/mo   dominant term
b0          18.59       131.9        egress  (raw ships everything)
b1           1.50        76.6        storage (full raw TSDB on EBS)
b2           3.03        81.5        storage
b3           9.10       101.9        storage
asap         0.98        67.4        compute (edge aggregation CPU)
asap-gzip    0.14        64.7        compute
```

ASAP is **~2× cheaper than raw b0** and beats every baseline. The tradeoff is
explicit: ASAP spends **more on edge EC2 CPU** ($52/mo, ~77% of its bill — the
cost of aggregating at the edge) but **saves big on egress** (0.98 vs 18.59
Mbps → $3 vs $60/mo) **and storage** (compact S3 cold $11 vs full-raw EBS TSDB
$54/mo). The cold-format sweep shows intchunk+batched (#442) at ~190k PUTs/mo
($1/mo) vs the pre-#442 tiny-part path at ~11M PUTs/mo ($57/mo) — a ~60× request
saving — and ~2.33× smaller stored bytes than the Gorilla fragment path.
