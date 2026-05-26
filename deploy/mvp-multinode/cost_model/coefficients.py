"""Calibration constants for the ASAP cost model.

Every coefficient is tagged with its provenance:

    MEASURED   — read off a real MVP-multinode run (file cited inline).
    DERIVED    — computed from a MEASURED number + workload arithmetic.
    DESIGN     — taken from a design doc / processor default / source const.
    ASSUMED    — a placeholder pending a live measurement that is in progress
                 on the cluster. These are the ones to refine first; each
                 carries a `# TODO(live)` so they are greppable.

Primary calibration sources
---------------------------
[FINDINGS]   deploy/mvp-multinode/FINDINGS.md  (2026-05) — the per-arm wire
             bandwidth sweep on the 10k-series / 10 Hz / 2-metric workload.
[CSUMMARY]   deploy/mvp-multinode/results/mvp-multinode-20260510-065843/
             container-summary-{asap,b0,b1}.csv — measured per-container
             cpu_mean_perc + mem_mean_mib over a full run.
[HOLISTIC]   ASAPQuery-backend docs/design/holistic-edge-backend-compression.md
             — cold INT-chunk codec ratios (intchunk ~2.33x vs Gorilla-XOR).
[PR442]      ASAPCollector commit 894cdab (#442) — cold intchunk part
             accumulation per block_duration; the value-codec / part-overhead
             numbers in its message.
[CONFIG]     deploy/mvp-multinode/configs/asap/* + processor config.go defaults
             (window 60s, gorillas3 window_interval 5s, block_duration 60s,
             shard_count 4, KLL k=200, DDSketch alpha=0.01, HLL p=14).

NOTE on the wire model: rather than reconstruct OTLP byte sizes from first
principles (which is sensitive to label encoding details we don't fully
control), the wire bandwidth per arm is calibrated *directly* to the
[FINDINGS] sweep and then scaled by the workload's raw-sample rate relative to
the FINDINGS calibration point. validate.py asserts the round-trip.
"""

from __future__ import annotations

# ─────────────────────────────────────────────────────────────────────────────
# 0. The FINDINGS calibration operating point.
#    The bandwidth sweep (#404–#407) ran the 2-metric workload
#    (http_requests_total + http_requests_total_latency_ms) at this point.
# ─────────────────────────────────────────────────────────────────────────────

# MEASURED [FINDINGS §2 conclusions + topology.env workload sizing]:
# 5 producers/node x 2 agent-nodes x 1000 series = 10_000 total series, 10 Hz.
CAL_SERIES = 10_000              # MEASURED [FINDINGS "10k raw series"]
CAL_SAMPLE_HZ = 10.0             # DESIGN  [workload.yaml header / runbook: EXPORTER_FREQ_HZ=10]
CAL_METRICS = 2                  # DESIGN  [FINDINGS: http_requests_total + latency]
# Raw samples/sec at the calibration point = series * Hz * metrics. This is the
# per-DEPLOYMENT raw throughput that ALL the measured coefficients (wire, CPU,
# memory) were observed at, so it is the single denominator every scaling ratio
# divides by. Workload.raw_samples_per_sec uses the same series*Hz*metrics form,
# so at the MVP preset the ratio is exactly 1.0 and the model reproduces every
# measured coefficient verbatim.
CAL_RAW_SAMPLES_PER_SEC = CAL_SERIES * CAL_SAMPLE_HZ * CAL_METRICS   # DERIVED = 200_000

# ─────────────────────────────────────────────────────────────────────────────
# 1. WIRE BANDWIDTH per arm at the calibration point, in MEGABITS/sec.
#    These ARE the FINDINGS headline numbers; validate.py reproduces them.
#    Mbps == compressed wire bytes leaving the edge (backend/gateway ingress).
# ─────────────────────────────────────────────────────────────────────────────

WIRE_MBPS_AT_CAL = {
    "b0":        18.59,   # MEASURED [FINDINGS] raw OTLP, no codec
    "b1":         1.50,   # MEASURED [FINDINGS] raw OTLP + gzip
    "b2":         3.03,   # MEASURED [FINDINGS] raw PRW + Snappy
    "b3":         9.10,   # MEASURED [FINDINGS] raw + serf-XOR wire codec
    "asap":       0.98,   # MEASURED [FINDINGS] edge-aggregated sketches, no codec
    "asap-gzip":  0.137,  # MEASURED [FINDINGS] edge-aggregated + gzip
}

# How wire scales off the calibration point. The raw arms (b0..b3) scale ~linearly
# with raw sample rate (series x Hz). The asap arms ship per-window aggregated
# sketch state + edge zone-sums, so their wire scales with the *aggregated*
# output, which is dominated by sketch-state-per-series-group rather than raw
# sample count. We model asap wire as scaling with series count (sketch states
# in flight) and only weakly with Hz (more samples fold into the same window
# state). See model.wire_bytes_per_sec for the exact form.
ASAP_WIRE_HZ_EXPONENT = 0.15     # ASSUMED  # TODO(live): fit from a Hz sweep.
                                 # 0 => wire flat in Hz (pure window state);
                                 # 1 => wire linear in Hz (like raw). The edge
                                 # window folds samples, so it's near-flat; a
                                 # small positive exponent captures residual
                                 # delta-transmission growth.

# ─────────────────────────────────────────────────────────────────────────────
# 2. COLD ARCHIVE (asap only) — the raw stream written to object storage.
# ─────────────────────────────────────────────────────────────────────────────

# MEASURED [FINDINGS §2 caveat]: "asap also runs the gorillas3 cold-tier,
# writing the full raw stream to colocated MinIO (~3.57 Mbps)".
COLD_RAW_MBPS_AT_CAL = 3.57      # MEASURED [FINDINGS caveat]

# Cold codec compression of the raw stream into stored object bytes.
# Gorilla-XOR is the deployed default cold encoder (fragment path).
# The ~3.57 Mbps figure is the *encoded* (Gorilla-XOR fragment) stream already
# (it's measured as MinIO write traffic), so the Gorilla path stores at 1.0x of
# that. The intchunk path stores SMALLER per the codec ratio below.
GORILLA_XOR_STORE_RATIO = 1.0    # MEASURED [FINDINGS caveat is already encoded]
# HOLISTIC §intro + PR442 message: intchunk value codec ~2.33x aggregate vs
# Gorilla-XOR (i.e. intchunk stores at ~1/2.33 of Gorilla bytes), once parts are
# batched per block (PR442). Pre-batching, tiny parts were ~3x LARGER.
INTCHUNK_AGG_RATIO_VS_GORILLA = 2.33   # DESIGN [HOLISTIC / PR442]
INTCHUNK_STORE_RATIO = GORILLA_XOR_STORE_RATIO / INTCHUNK_AGG_RATIO_VS_GORILLA  # DERIVED ~0.429

# ─────────────────────────────────────────────────────────────────────────────
# 3. S3 / object-store REQUEST counts — the part that drives request $.
#    Cadences are DESIGN values from the deployed configs (config.go).
# ─────────────────────────────────────────────────────────────────────────────

# Cold-tier flush/seal cadence. PR442: the intchunk path now seals + POSTs ONE
# part once the buffered span reaches cold.block_duration (default 60s). Pre-#442
# it POSTed one part per drain (~window_interval 5s => ~12x more, and at the
# per-shard granularity even more). The gorilla fragment path ships per window.
COLD_BLOCK_DURATION_SEC = 60.0   # DESIGN [config.go: BlockDuration default = WindowDuration]
COLD_WINDOW_INTERVAL_SEC = 5.0   # DESIGN [agent yaml gorillas3.window_interval: 5s]
SHARD_COUNT = 4                  # DESIGN [config.go: ShardCount default 4]

# One PUT per (shard x seal). Parts are per-tenant/per-block/per-shard objects.
# intchunk (post-#442): one part per block_duration per shard.
# fragment/legacy intchunk (pre-#442): one part per window_interval per shard.
# Tiny-part pre-#442 behaviour is modelled via COLD_WINDOW_INTERVAL_SEC so the
# ~60x PUT reduction is visible when you flip cold_part_batched off.
COLD_PUT_OBJECTS_PER_SEAL = SHARD_COUNT   # DESIGN — one S3 object per shard per seal

# Thanos store-gateway GETs: it syncs blocks every sync_block_duration and reads
# index+chunks on query. Modeled as a steady GET rate driven by block count.
THANOS_SYNC_BLOCK_DURATION_SEC = 30.0     # DESIGN [run_demo.sh --sync-block-duration=30s]
# GETs per sync per block (meta.json + index + a few chunk reads). Coarse.
THANOS_GETS_PER_BLOCK_SYNC = 3.0          # ASSUMED  # TODO(live): count from MinIO access log.

# Thanos compact downsamples + merges blocks. It rewrites objects: model as a
# PUT amplification on the stored block volume.
THANOS_COMPACT_REWRITE_FACTOR = 1.0       # ASSUMED  # TODO(live): from compactor metrics.

# ─────────────────────────────────────────────────────────────────────────────
# 3b. RAW-ARM TSDB on-disk storage (b0..b3 keep the full raw stream on EBS).
#     VictoriaMetrics / Prometheus compress samples to ~1.3 B/sample on-disk
#     (Gorilla double-delta ts + XOR values; VM is a bit tighter than Prom).
#     This is the "VM storage" cost ASAP avoids by aggregating + shipping
#     sketches (and archiving only the compact cold stream to cheap S3).
# ─────────────────────────────────────────────────────────────────────────────
RAW_TSDB_BYTES_PER_SAMPLE = 1.3           # DESIGN [VictoriaMetrics/Prometheus
                                          # on-disk compressed sample size; VM
                                          # docs cite ~0.4-1.7 B/sample, ~1.3 typical]
# ASAP's warm tier keeps only the current/recent sketch window state on EBS
# (the durable warm-sketch tier), NOT the full history — history goes to S3
# cold. Model the warm EBS footprint as a small multiple of one window's state.
ASAP_WARM_EBS_WINDOWS_RESIDENT = 4.0      # ASSUMED  # TODO(live): warm-tier disk RSS.

# ─────────────────────────────────────────────────────────────────────────────
# 4. COMPUTE — CPU (vCPU-fraction) per component, calibrated at CAL_SERIES.
#    Read off container-summary cpu_mean_perc (100% == 1 vCPU). [CSUMMARY]
#    CPU is modeled as scaling linearly with raw sample throughput
#    (series x Hz x metrics) for the per-sample processing components, with a
#    fixed idle baseline.
# ─────────────────────────────────────────────────────────────────────────────

# vCPU at the calibration point (cpu_mean_perc / 100). Two agent nodes are
# summed where the component is replicated; we report per-deployment totals.
CPU_VCPU_AT_CAL = {
    # asap arm. edge_agent + raw_agent are DEPLOYMENT TOTALS (both agent nodes
    # summed), since the deployment runs agent-a + agent-b.
    "edge_agent":            2.385,  # MEASURED [CSUMMARY asap: agent-a 107.7% + agent-b 130.8% => 2.385 vCPU]
    "data_plane":            0.123,  # MEASURED [CSUMMARY asap: asap-backend 12.3%]
    "controller":            0.002,  # MEASURED [CSUMMARY asap: asap-controller 0.2%]
    "minio":                 0.037,  # MEASURED [CSUMMARY asap: asap-minio 3.7%]
    "thanos_store_gateway":  0.017,  # MEASURED [CSUMMARY asap: 1.7%]
    "thanos_compact":        0.001,  # MEASURED [CSUMMARY asap: ~0.0%, floored]
    "thanos_query":          0.001,  # MEASURED [CSUMMARY asap: ~0.0%, floored]
    # gorilla-merger runs the cold ingest/StoreAPI; in the 0510 run the cold
    # ingest was colocated in the agent (gorillas3 processor) so there is no
    # standalone merger container line. Charge the merger as the backend-side
    # cold ingest cost. ASSUMED until the split-out merger container is measured.
    "gorilla_merger":        0.05,   # ASSUMED  # TODO(live): split-out merger CPU.
    # raw baselines: VM/Prometheus sink + raw agent (raw_agent = both nodes summed)
    "raw_agent":             0.255,  # MEASURED [CSUMMARY b0: agent-a 12.7% + agent-b 12.8% => 0.255 vCPU]
    "vm":                    0.63,   # MEASURED [CSUMMARY b0: asap-prometheus(VM) 63.0%]
    "serf_gateway":          0.025,  # MEASURED [CSUMMARY asap-gateway 2.5% as a proxy] # TODO(live): measure b3 serf-gw
}

# Idle baseline fraction of the calibration CPU that does NOT scale with
# throughput (process overhead, GC, scrape loops). The remainder scales with
# raw sample rate. 0.0 => fully throughput-proportional.
CPU_IDLE_FRACTION = {
    "edge_agent":            0.10,   # ASSUMED  # TODO(live): from a cardinality sweep.
    "data_plane":            0.30,   # ASSUMED
    "vm":                    0.15,   # ASSUMED
    "raw_agent":             0.20,   # ASSUMED
    "minio":                 0.20,   # ASSUMED
    "gorilla_merger":        0.20,   # ASSUMED
    "serf_gateway":          0.20,   # ASSUMED
    "thanos_store_gateway":  0.50,   # ASSUMED (mostly sync overhead)
    "thanos_compact":        0.50,   # ASSUMED
    "thanos_query":          0.50,   # ASSUMED
    "controller":            0.90,   # ASSUMED (control-plane, near-flat)
}

# Sampling probability p (per-metric sample_p knob, #441) reduces the per-sample
# work on the warm sketch processors (CMS/HLL). Edge-agent throughput-scaled CPU
# multiplies by this exponent of p. p=1 => no reduction. The cold archive and
# the routing/batch front still see every sample, so this is < 1 but > 0.
SAMPLING_CPU_EXPONENT = 0.6      # ASSUMED  # TODO(live): the sampling-bench at p=0.5
                                 # is measuring the realized CPU reduction.

# ─────────────────────────────────────────────────────────────────────────────
# 5. MEMORY (GB) per component at the calibration point. [CSUMMARY mem_mean_mib]
#    Memory scales with in-flight window state (series x sketch-state-size) plus
#    a fixed baseline.
# ─────────────────────────────────────────────────────────────────────────────

MIB = 1.0 / 1024.0   # MiB -> GiB

MEM_GB_AT_CAL = {
    # edge_agent + raw_agent are DEPLOYMENT TOTALS (both agent nodes summed).
    "edge_agent":           173.4 * MIB,   # MEASURED [CSUMMARY asap: agent-a 78.2 + agent-b 95.2 => 173.4 MiB]
    "data_plane":            33.7 * MIB,   # MEASURED [CSUMMARY asap: asap-backend]
    "controller":             4.6 * MIB,   # MEASURED [CSUMMARY]
    "minio":                219.8 * MIB,   # MEASURED [CSUMMARY asap]
    "thanos_store_gateway":  30.9 * MIB,   # MEASURED [CSUMMARY]
    "thanos_compact":        22.9 * MIB,   # MEASURED [CSUMMARY]
    "thanos_query":           8.0 * MIB,   # MEASURED [CSUMMARY ~0, floored to a sane idle]
    "gorilla_merger":        40.0 * MIB,   # ASSUMED  # TODO(live): split-out merger RSS.
    "raw_agent":            150.8 * MIB,   # MEASURED [CSUMMARY b0: agent-a 76.4 + agent-b 74.4 => 150.8 MiB]
    "vm":                   105.0 * MIB,   # MEASURED [CSUMMARY b0: asap-prometheus(VM) 104.8 MiB]
    "serf_gateway":          66.0 * MIB,   # MEASURED [CSUMMARY asap-gateway as proxy] # TODO(live)
}

MEM_IDLE_FRACTION = {
    # fraction of measured memory that is fixed (heap floor) vs series-scaled.
    "edge_agent":            0.30,   # ASSUMED
    "data_plane":            0.40,   # ASSUMED
    "vm":                    0.30,   # ASSUMED
    "raw_agent":             0.60,   # ASSUMED
    "minio":                 0.80,   # ASSUMED (mostly server baseline)
    "gorilla_merger":        0.40,   # ASSUMED
    "serf_gateway":          0.60,   # ASSUMED
    "thanos_store_gateway":  0.60,   # ASSUMED
    "thanos_compact":        0.80,   # ASSUMED
    "thanos_query":          0.80,   # ASSUMED
    "controller":            0.95,   # ASSUMED
}

# ─────────────────────────────────────────────────────────────────────────────
# 6. Per-sketch-family state size (bytes) — for the warm-tier memory + the
#    asap wire decomposition. From the control_plane optimizer benchmark table
#    (ASAPQuery-backend optimizer/cost/mod.rs benchmark_table, 2026-03-15) and
#    processor defaults. base_memory_bytes per sketch instance.
# ─────────────────────────────────────────────────────────────────────────────

SKETCH_STATE_BYTES = {
    "ddsketch":       4_096,    # DESIGN [optimizer benchmark_table: DDSketch base_memory_bytes]
    "kll":            2_048,    # DESIGN [optimizer: KLL]  (k=200)
    "hll":           16_384,    # DESIGN [optimizer: HLL precision=14 -> 16 KB]
    "countsketch":   40_960,    # DESIGN [optimizer: CountSketch]
    "countminsketch":40_960,    # DESIGN [optimizer: CountMinSketch]
    "sum":               64,    # DESIGN — a Sum role emits one f64 + labels per group
}

# Default conversion: how many seconds in a 30-day month / hours.
SECONDS_PER_MONTH = 86_400.0 * 30.0
HOURS_PER_MONTH = 24.0 * 30.0
BYTES_PER_GB = 1_000_000_000.0    # decimal GB (matches AWS billing + tco.rs)
BITS_PER_BYTE = 8.0
MBIT = 1_000_000.0                # bits per megabit (Mbps is decimal)
