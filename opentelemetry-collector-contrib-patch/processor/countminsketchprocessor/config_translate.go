// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package countminsketchprocessor

import (
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// toPrecomputeConfig translates the legacy Config to the host-neutral
// PrecomputeConfig the shim hands to precompute.New. The translation
// pins the Phase-2 parity flags the legacy CMS processor's series key
// and emit shape require:
//
//   - OmitResourceAttrs=true: the legacy seriesKey is
//     `metricName + "::" + encodeKey(dpAttrs)`; resource attrs are
//     NOT in the key, and the emit appends a freshly-created
//     ResourceMetrics with an empty Resource. Without this flag the
//     runtime would key by (resource, dp-labels) and surface
//     non-empty ResourceLabels on the envelope, breaking byte-parity.
//
//   - GlobalAggregation=false: CMS emits one envelope per (metric,
//     dp-attrs) series — never collapsed to a single global series
//     (that's CountSketch's batch-mode behavior).
//
//   - EmitWindowStats=false: CMS does NOT carry sample_count /
//     window_duration_seconds inside the envelope's Labels list. The
//     legacy emit stamps `sample_count` onto the typed
//     CountMinSketchDataPoint via dp.SetSampleCount(...) — an OTel
//     pmetric typed field, not a label. The shim's encode path
//     restores it from envelope.Count.
//
// MetricName is set to cfg.MetricName per legacy semantics: the CMS
// processor emits one configured metric name (e.g. "countmin_sketch")
// regardless of the input metric's name. This is the opposite of KLL,
// where the output name derives from the input name. The shim's
// encode path uses this as the output metric name.
func (c *Config) toPrecomputeConfig(metricName string) *precompute.PrecomputeConfig {
	matchers := make([]precompute.LabelMatcher, 0, len(c.LabelMatchers)+1)
	if metricName != "" {
		// One Precompute per input metric; pin the metric-name matcher
		// so observations from other metrics fed through the same
		// instance are filtered. (The shim only routes matching
		// observations here today, but the matcher makes the contract
		// explicit and preserves the invariant when the runtime is
		// shared with control-channel-driven callers in 2.10.)
		matchers = append(matchers, precompute.LabelMatcher{Value: metricName})
	}
	for _, m := range c.LabelMatchers {
		matchers = append(matchers, precompute.LabelMatcher{
			Name:  m.Key,
			Value: m.Value,
		})
	}
	mode := precompute.Batch
	winSize := time.Duration(0) // Batch: zero size triggers always-flushable rotate
	if c.Mode == ModeWindow {
		mode = precompute.Tumbling
		winSize = c.WindowDuration
	}
	enc := precompute.EncodingProtoFull
	if c.Encoding == EncodingMsgpack {
		enc = precompute.EncodingMsgpack
	}
	return &precompute.PrecomputeConfig{
		SketchType: precompute.SketchTypeCountMinSketch,
		Mode:       mode,
		Window:     precompute.WindowSpec{Size: winSize},
		Matchers:   matchers,
		AggregateBy: append([]string(nil), c.AggregateBy...),
		TransmitSketch: c.TransmitSketch,
		// Delta transmission is post-processed in the shim, not the
		// runtime. See cmsProcessor.applyDeltaTransmission and the
		// note on cmsSketchWrapper.ComputeDeltaAgainst for why. We
		// always ask the runtime for full snapshots and convert to
		// delta against a shim-owned per-series prev cache.
		DeltaTransmission: false,
		Encoding:          enc,
		Temporality:       1, // delta — matches legacy SetAggregationTemporality(Delta)
		MetricName:        c.MetricName,
		OmitResourceAttrs: true,
		GlobalAggregation: false,
		EmitWindowStats:   false,
	}
}
