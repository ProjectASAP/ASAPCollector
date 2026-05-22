// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ddsketchprocessor

import (
	"go.opentelemetry.io/collector/pdata/pmetric"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	"github.com/ProjectASAP/asap-precompute-go/sketches"
)

// getOrCreate fetches the per-metric Precompute or builds one from
// the current Config translated via toPrecomputeConfig.
//
// proc may be nil — Batch-mode flushes (ProcessBatch) call this with
// a fresh batch map and a non-nil proc, but unit tests construct a
// throwaway map via the public test helper. Both paths keep working.
func getOrCreate(batch map[string]precompute.Precompute, metricName string, cfg *Config, proc *ddsketchProcessor) precompute.Precompute {
	if pc, ok := batch[metricName]; ok {
		return pc
	}
	pc := precompute.New(toPrecomputeConfig(cfg, metricName), func() precompute.Sketch {
		// Draw from the processor's cross-window sketch pool when
		// available; the pool's New makes a fresh wrapper at the
		// configured relative_accuracy. The throwaway-batch path
		// (proc==nil, used by tests) always makes a fresh sketch.
		if proc != nil {
			return proc.sketchPool.Get().(precompute.Sketch)
		}
		return sketches.NewDDSketchWrapper(cfg.RelativeAccuracy)
	}, sketches.DDSketchObserver{})
	if proc != nil {
		// Recycle each series' sketch back to the pool once its
		// envelope is serialized at flush; Reset() does the in-place
		// Clear that keeps the bucket-store capacity for reuse.
		pc.SetSketchSink(func(s precompute.Sketch) {
			s.Reset()
			proc.sketchPool.Put(s)
		})
		// Wire the per-Observe latency histogram if the shim has one.
		// Closes Phase 2.11B gap #3 — every observation routed through
		// this Precompute now contributes to asap_processor_observe_seconds
		// on the deployed /metrics endpoint.
		if proc.observeLatency != nil {
			pc.SetLatencyObserver(proc.recordObserveLatency)
		}
	}
	batch[metricName] = pc
	return pc
}

// toPrecomputeConfig translates the legacy Config into the runtime's
// PrecomputeConfig. The runtime always emits envelopes
// (TransmitSketch=true); quantile materialization happens in
// appendQuantileMetrics when the legacy config asked for it.
func toPrecomputeConfig(cfg *Config, metricName string) *precompute.PrecomputeConfig {
	mode := precompute.Batch
	var window precompute.WindowSpec
	if cfg.Mode == ModeWindow {
		mode, window.Size = precompute.Tumbling, cfg.WindowDuration
	}
	return &precompute.PrecomputeConfig{
		AggID:             precompute.AggId(seriesNameHash(metricName)),
		SketchType:        precompute.SketchTypeDDSketch,
		Mode:              mode,
		Window:            window,
		AggregateBy:       cfg.AggregateBy,
		TransmitSketch:    true,
		DeltaTransmission: cfg.DeltaTransmission,
		DeltaThreshold:    cfg.DeltaThreshold,
		Encoding:          precompute.EncodingProtoFull,
		// Refactor-2026-05: precompute config carries the raw input name.
		MetricName:        metricName,
		Temporality:       int32(pmetric.AggregationTemporalityDelta),
	}
}

// seriesNameHash derives a stable per-metric AggID via FNV-1a 64.
// Opaque integer; downstream consumers don't depend on this value.
func seriesNameHash(name string) uint64 {
	var h uint64 = 1469598103934665603
	for i := 0; i < len(name); i++ {
		h ^= uint64(name[i])
		h *= 1099511628211
	}
	return h
}

// matchesLegacyMatchers replicates the legacy ddsketchProcessor's
// matchesMatchers semantics on a host-neutral []KeyValue. Filtering
// happens BEFORE allocating the per-metric Precompute so a fully
// filtered batch never spawns one.
func (p *ddsketchProcessor) matchesLegacyMatchers(labels []precompute.KeyValue) bool {
	if len(p.cfg.LabelMatchers) == 0 {
		return true
	}
	for _, m := range p.cfg.LabelMatchers {
		hit := false
		for i := range labels {
			if labels[i].Key == m.Key {
				if labels[i].Value != m.Value {
					return false
				}
				hit = true
				break
			}
		}
		if !hit {
			return false
		}
	}
	return true
}
