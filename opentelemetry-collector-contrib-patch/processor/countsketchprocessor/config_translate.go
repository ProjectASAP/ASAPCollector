// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package countsketchprocessor

import (
	"math"

	"go.opentelemetry.io/collector/pdata/pmetric"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// outputMetricName is the fixed metric name the legacy processor
// stamped on every flushed CountSketch envelope. Hard-coded here (not
// derived per-input-metric) to preserve byte-parity with the legacy
// emit path — see ADR-0002 §"Behavior preservation" and the parity
// harness's CountSketch sketchDescriptor in
// integration/parity/harness/runtime.go.
const outputMetricName = "countsketch_partition"

// toPrecomputeConfig translates the legacy Config into the runtime's
// PrecomputeConfig. The runtime always emits envelopes
// (TransmitSketch=true on the runtime side); the shim's downstream
// emit path inspects the original Config.TransmitSketch to decide
// whether to write typed CountSketch DPs or fall back to legacy
// Gauge summaries — see flushToMetrics in shim_helpers.go.
//
// Three non-default flags are flipped specifically for CountSketch
// byte-parity (verified by integration/parity/harness/runtime.go):
//
//   - GlobalAggregation=true when AggregateBy is empty: legacy
//     buildPartitionKey returns the literal "global" for empty
//     AggregateBy, collapsing every observation into one shared
//     sketch. The runtime models that exactly.
//   - OmitResourceAttrs=true: legacy series-key construction never
//     puts resource attrs in the key; resource attrs only appear as
//     a fallback when buildPartitionKey looks up an AggregateBy key
//     missing from dp-attrs. The shim's observe path merges
//     resource attrs into dp-labels before Observe so AggregateBy
//     lookups still find them, and the runtime then strips resource
//     attrs from the emitted envelope per OmitResourceAttrs=true.
//   - EmitWindowStats=true: legacy stamped sample_count and
//     window_duration_seconds onto every emitted DP. Routing them
//     through the envelope's Labels at flush time lets the OTel
//     adapter reproduce them via KeyValuesToAttributes naturally.
func toPrecomputeConfig(cfg *Config) *precompute.PrecomputeConfig {
	pcfg := &precompute.PrecomputeConfig{
		AggID:             precompute.AggId(uint64(precompute.SketchTypeCountSketch)),
		SketchType:        precompute.SketchTypeCountSketch,
		Mode:              precompute.Tumbling,
		Window:            precompute.WindowSpec{Size: cfg.WindowDuration},
		Matchers:          toRuntimeMatchers(cfg.LabelMatchers),
		AggregateBy:       append([]string(nil), cfg.AggregateBy...),
		MetricName:        outputMetricName,
		TransmitSketch:    true,
		DeltaTransmission: cfg.DeltaTransmission,
		DeltaThreshold:    uint64(math.Ceil(cfg.DeltaThreshold)),
		Encoding:          mapEncoding(cfg.Encoding),
		Temporality:       int32(pmetric.AggregationTemporalityDelta),
		GlobalAggregation: len(cfg.AggregateBy) == 0,
		OmitResourceAttrs: true,
		EmitWindowStats:   true,
	}
	return pcfg
}

// configDimensions mirrors the legacy newConfiguredCountSketch sizing.
// Exposed at file scope so the shim and tests build sketches with the
// exact dimensions the legacy processor used.
func configDimensions(cfg *Config) (rows, cols int) {
	rows = int(math.Ceil(math.Log(1 / cfg.Delta)))
	if rows < 1 {
		rows = 1
	}
	cols = int(math.Ceil(1 / (cfg.Epsilon * cfg.Epsilon)))
	if cols < 2 {
		cols = 2
	}
	cols = nextPowerOfTwo(cols)
	return rows, cols
}

func nextPowerOfTwo(n int) int {
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}

func toRuntimeMatchers(in []LabelMatcher) []precompute.LabelMatcher {
	if len(in) == 0 {
		return nil
	}
	out := make([]precompute.LabelMatcher, 0, len(in))
	for _, m := range in {
		out = append(out, precompute.LabelMatcher{Name: m.Key, Value: m.Value})
	}
	return out
}

func mapEncoding(e SketchEncoding) precompute.Encoding {
	if e == EncodingMsgpack {
		return precompute.EncodingMsgpack
	}
	return precompute.EncodingProtoFull
}
