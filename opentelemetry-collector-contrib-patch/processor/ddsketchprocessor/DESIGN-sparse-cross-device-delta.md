# Sparse Cross-Device Delta (Paper §4.3 — OctoSketch-style)

## Problem

When N agents each send full sketches to the backend:
- Total bandwidth = N × sketch_size
- Most buckets are zero (each agent sees a subset of the global key space)
- Redundant: unchanged buckets are re-transmitted every window

## Design (OctoSketch-inspired)

### Agent side: sparse delta transmission

Each agent maintains per-sketch state:
```
last_sent[bucket_idx] → count
```

On each window flush:
1. Compare `current_sketch[i]` vs `last_sent[i]` for each bucket
2. Only emit buckets where `|current - last_sent| >= threshold`
3. Encode as sparse map: `{bucket_idx: delta_count, ...}`
4. Update `last_sent[i] = current_sketch[i]` for emitted buckets

### Backend side: reconstruction

Backend maintains per-agent accumulator:
```
accumulated[agent_id][bucket_idx] → count
```

On receiving sparse delta from agent:
1. Apply: `accumulated[agent_id][i] += delta[i]` for each received bucket
2. Global sketch: `global[i] = sum(accumulated[agent_id][i] for all agents)`

### New config fields

```yaml
processors:
  ddsketch:
    delta_transmission: true
    # NEW: cross-device sparse mode
    sparse_delta: true
    sparse_threshold: 1        # min bucket change to include in delta
```

### Bandwidth savings

For a DDSketch with 2048 buckets, 100 agents, typical 10% fill rate:
- Full: 100 × 2048 × 8B = 1.6 MB/window
- Sparse delta: 100 × (2048 × 0.1) × 12B = 245 KB/window (6.5× reduction)

For CountMinSketch with 2048×5 cells, most cells zero per device:
- Full: 100 × 2048 × 5 × 8B = 8.2 MB/window
- Sparse: 100 × (2048 × 5 × 0.05) × 12B = 615 KB/window (13× reduction)

### Wire format

Sparse delta encoded as OTLP gauge with attributes:
```
metric_name: "sketch_sparse_delta"
attributes:
  agent_id: "agent-42"
  sketch_type: "ddsketch"
  window_start: 1712000000
data_points:
  - value: 3.0    # delta count
    attributes: { bucket: 42 }
  - value: 1.0
    attributes: { bucket: 107 }
```

### Go implementation changes

**Agent processor** (`ddsketchprocessor/processor.go`):
1. Add `lastEmitted map[int]float64` state per series
2. On flush: compute sparse delta, encode as OTLP gauge
3. Gating: only active when `sparse_delta: true`

**Backend merge processor** (`ddsketchmergeprocessor` — new):
1. Maintain per-agent accumulator
2. On receiving sparse delta: apply increments
3. On query: compute global sketch from accumulators

### Testing

1. Unit: verify sparse delta + reconstruction = full sketch
2. Benchmark: measure bandwidth reduction at 10/50/100 agents
3. Accuracy: verify zero reconstruction error for additive sketches
