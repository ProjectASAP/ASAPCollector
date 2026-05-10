// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// File shim_helpers.go houses the OTel-side glue the shim needs but
// the host-neutral runtime cannot provide:
//
//   - observeInto: walk md, merge resource-attrs into dp-attrs (so
//     AggregateBy lookups find resource keys), and feed each
//     observation into the single Precompute. The metric name is
//     stashed on ObservationValue.Bytes so the SketchObserver can
//     call UpdateString with the legacy key (metric.Name()).
//   - flushToMetrics: tick the Precompute, then either build typed
//     CountSketchDataPoints (TransmitSketch=true, the new wire
//     format) or fall back to a legacy Gauge per partition key
//     (TransmitSketch=false). The Gauge fallback is required because
//     existing dashboards keyed by `countsketch_partition` as a
//     scalar series still rely on it.
//   - stampDPMetadata: fill the typed CountSketchDataPoint's
//     Dimension / Epsilon / Delta / Encoding fields the host-neutral
//     adapter doesn't know about.
//   - partitionKeyFromEnvelope: rebuild the legacy
//     `key=value;key=value;` partition-key string from the
//     envelope's Labels.

package countsketchprocessor

import (
	"sort"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// observeInto walks md and feeds one host-neutral Observation per
// data point into the Precompute. Resource attributes are merged
// into the data-point label set before observe so AggregateBy
// lookups find resource-level keys (mirroring the legacy
// buildPartitionKey's "look up dpAttrs first, then resourceAttrs"
// fallback). Sketch-typed inputs are surfaced as KindEnvelope so
// Precompute.ObserveEnvelope merges them in place — the runtime
// never expands envelopes to scalar samples.
func (p *countSketchProcessor) observeInto(md pmetric.Metrics) error {
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		resourceAttrs := rm.Resource().Attributes()
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				if err := p.observeMetric(resourceAttrs, ms.At(k)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// observeMetric pushes one Observation per data point. Scalar
// metrics produce KindFloat observations carrying the metric name
// in ObservationValue.Bytes (so the SketchObserver invokes
// UpdateString with the legacy key). Sketch-typed metrics produce
// KindEnvelope observations with the payload bytes preserved.
func (p *countSketchProcessor) observeMetric(resourceAttrs pcommon.Map, m pmetric.Metric) error {
	name := m.Name()
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		dps := m.Gauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			if err := p.observeFloat(name, resourceAttrs, dp.Attributes(), dp.Timestamp(), numberValue(dp)); err != nil {
				return err
			}
		}
	case pmetric.MetricTypeSum:
		dps := m.Sum().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			if err := p.observeFloat(name, resourceAttrs, dp.Attributes(), dp.Timestamp(), numberValue(dp)); err != nil {
				return err
			}
		}
	case pmetric.MetricTypeHistogram:
		dps := m.Histogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			if err := p.observeFloat(name, resourceAttrs, dp.Attributes(), dp.Timestamp(), float64(dp.Count())); err != nil {
				return err
			}
		}
	case pmetric.MetricTypeCountSketch:
		dps := m.CountSketch().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			env := &precompute.SketchEnvelope{
				SchemaVersion: 1,
				SketchType:    precompute.SketchTypeCountSketch,
				Labels:        mergedLabels(resourceAttrs, dp.Attributes()),
				WindowStartMs: uint64(dp.StartTimestamp() / 1_000_000),
				WindowEndMs:   uint64(dp.Timestamp() / 1_000_000),
				Encoding:      countSketchEncodingToHostNeutral(dp.Encoding()),
				Payload:       copyBytes(dp.Sketch()),
			}
			obs := precompute.Observation{
				TimestampMs: uint64(dp.Timestamp() / 1_000_000),
				Metric:      name,
				Labels:      env.Labels,
				Value:       precompute.EnvelopeValue(env),
			}
			if err := p.pc.Observe(&obs); err != nil {
				return err
			}
		}
	}
	return nil
}

// observeFloat is the scalar-input fast path. Metric name travels in
// ObservationValue.Bytes as a side-channel so the SketchObserver can
// call UpdateString(metricName, value) with the same key the legacy
// processor used.
func (p *countSketchProcessor) observeFloat(name string, resourceAttrs, dpAttrs pcommon.Map, ts pcommon.Timestamp, val float64) error {
	obs := precompute.Observation{
		TimestampMs: uint64(ts / 1_000_000),
		Metric:      name,
		Labels:      mergedLabels(resourceAttrs, dpAttrs),
		Value: precompute.ObservationValue{
			Kind:  precompute.KindFloat,
			Float: val,
			Bytes: []byte(name),
		},
	}
	return p.pc.Observe(&obs)
}

// flushToMetrics drains the Precompute and converts the closed
// envelopes into pmetric.Metrics. TransmitSketch=true takes the
// otel.Encode path then stamps typed-DP fields the host-neutral
// adapter doesn't know about (Dimension / Epsilon / Delta / Encoding).
// TransmitSketch=false falls back to the legacy Gauge-per-partition
// emission so existing dashboards keep working.
//
// Force-drain semantics: legacy emitWindowAndReset rotated regardless
// of wall-clock; Precompute.Drain is the runtime's dedicated entry
// point for that contract. Drain replaces the previous
// pseudo-timestamp Tick(1<<62-1) workaround.
func (p *countSketchProcessor) flushToMetrics() pmetric.Metrics {
	envs := p.pc.Drain()
	if len(envs) == 0 {
		return pmetric.NewMetrics()
	}
	if p.config.TransmitSketch {
		return p.encodeSketchMetrics(envs)
	}
	return p.encodeGaugeMetrics(envs)
}

// encodeSketchMetrics goes through the runtime's OTel adapter then
// stamps the legacy typed-DP fields the host-neutral encoder doesn't
// populate (Dimension / Epsilon / Delta / Encoding).
func (p *countSketchProcessor) encodeSketchMetrics(envs []*precompute.SketchEnvelope) pmetric.Metrics {
	encoded, err := p.adapter.Encode(envs)
	if err != nil {
		if p.logger != nil {
			p.logger.Error("countsketchprocessor: encode failed", zap.Error(err))
		}
		return pmetric.NewMetrics()
	}
	md, ok := encoded.(pmetric.Metrics)
	if !ok {
		return pmetric.NewMetrics()
	}
	p.stampDPMetadata(md, envs)
	return md
}

// stampDPMetadata walks the encoded pmetric output in encode-order
// (groupOrder by ResourceLabels, envelopes within group preserved)
// and stamps the CountSketch parent's AggregationTemporality plus
// Rows/Cols (sketch matrix dimensions, sent ONCE per Metric emit
// instead of duplicated per DataPoint as of refactor-2026-05) and
// stamps each DP's Encoding tag.
//
// Refactor-2026-05: per-DP `dimension`, `epsilon`, `delta` are
// removed from CountSketchDataPoint:
//   - dimension was a legacy partition-key descriptor, now replaced
//     by the attribute set on the DP (group-by labels);
//   - epsilon/delta are derivable from rows/cols on the parent
//     container, so they no longer ride per-DP.
func (p *countSketchProcessor) stampDPMetadata(md pmetric.Metrics, envs []*precompute.SketchEnvelope) {
	rows, cols := configDimensions(p.config)
	idx := 0
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len() && idx < len(envs); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len() && idx < len(envs); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len() && idx < len(envs); k++ {
				m := ms.At(k)
				if m.Type() != pmetric.MetricTypeCountSketch {
					idx++
					continue
				}
				cs := m.CountSketch()
				cs.SetAggregationTemporality(pmetric.AggregationTemporality(envs[idx].AggregationTemporality))
				cs.SetRows(int32(rows))
				cs.SetCols(int32(cols))
				dps := cs.DataPoints()
				for l := 0; l < dps.Len() && idx < len(envs); l++ {
					dp := dps.At(l)
					env := envs[idx]
					dp.SetEncoding(hostNeutralToTypedEncoding(env.Encoding, p.config.Encoding))
					idx++
				}
			}
		}
	}
}

// encodeGaugeMetrics is the non-transmit emission path. The legacy
// processor produced one Gauge metric named "countsketch_partition"
// per partition with attributes (partition_key, sample_count, epsilon,
// delta, window_duration_seconds) and the sample count as the gauge
// value. Existing dashboards consume this as a scalar series.
func (p *countSketchProcessor) encodeGaugeMetrics(envs []*precompute.SketchEnvelope) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("otelcol/countsketch")
	for _, env := range envs {
		m := sm.Metrics().AppendEmpty()
		// Refactor-2026-05: prefer input metric name from the
		// envelope; fall back to outputMetricName only if the
		// envelope arrived without a name (shouldn't happen in
		// practice).
		name := env.MetricName
		if name == "" {
			name = outputMetricName
		}
		m.SetName(name)
		m.SetUnit("1")
		gauge := m.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.Timestamp(env.WindowEndMs * 1_000_000))
		dp.Attributes().PutStr("partition_key", partitionKeyFromEnvelope(env, p.config))
		dp.Attributes().PutInt("sample_count", int64(env.Count))
		dp.Attributes().PutDouble("epsilon", p.config.Epsilon)
		dp.Attributes().PutDouble("delta", p.config.Delta)
		dp.Attributes().PutInt(
			"window_duration_seconds",
			int64(p.config.WindowDuration.Seconds()),
		)
		dp.SetDoubleValue(float64(env.Count))
	}
	return md
}

// partitionKeyFromEnvelope reconstructs the legacy buildPartitionKey
// output from the envelope's Labels. With GlobalAggregation the
// runtime strips Labels entirely; we hard-code "global" to match
// buildPartitionKey's empty-AggregateBy branch. Otherwise we render
// the AggregateBy-projected labels in the same `key=value;` form the
// legacy builder produced.
func partitionKeyFromEnvelope(env *precompute.SketchEnvelope, cfg *Config) string {
	if len(cfg.AggregateBy) == 0 {
		return "global"
	}
	keep := make(map[string]struct{}, len(cfg.AggregateBy))
	for _, k := range cfg.AggregateBy {
		keep[k] = struct{}{}
	}
	keys := make([]string, 0, len(cfg.AggregateBy))
	values := make(map[string]string, len(cfg.AggregateBy))
	for _, kv := range env.Labels {
		if _, ok := keep[kv.Key]; !ok {
			continue
		}
		if _, seen := values[kv.Key]; !seen {
			keys = append(keys, kv.Key)
		}
		values[kv.Key] = kv.Value
	}
	sort.Strings(keys)
	var sb strings.Builder
	for _, k := range keys {
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(values[k])
		sb.WriteByte(';')
	}
	return sb.String()
}

// mergedLabels combines resource and DP attribute sets into one
// host-neutral KeyValue slice, sorted by key. DP entries take
// precedence on duplicate keys (mirroring the legacy
// buildPartitionKey's "look up dpAttrs first, then resourceAttrs"
// fallback semantics).
func mergedLabels(resourceAttrs, dpAttrs pcommon.Map) []precompute.KeyValue {
	if resourceAttrs.Len() == 0 && dpAttrs.Len() == 0 {
		return nil
	}
	merged := make(map[string]string, resourceAttrs.Len()+dpAttrs.Len())
	resourceAttrs.Range(func(k string, v pcommon.Value) bool {
		merged[k] = v.AsString()
		return true
	})
	dpAttrs.Range(func(k string, v pcommon.Value) bool {
		merged[k] = v.AsString()
		return true
	})
	keys := make([]string, 0, len(merged))
	for k := range merged {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]precompute.KeyValue, 0, len(keys))
	for _, k := range keys {
		out = append(out, precompute.KeyValue{Key: k, Value: merged[k]})
	}
	return out
}

// numberValue extracts a float from a NumberDataPoint regardless of
// its int/double tag.
func numberValue(dp pmetric.NumberDataPoint) float64 {
	if dp.ValueType() == pmetric.NumberDataPointValueTypeInt {
		return float64(dp.IntValue())
	}
	return dp.DoubleValue()
}

// copyBytes returns a defensive copy of b so the host-neutral
// envelope doesn't alias pmetric's storage.
func copyBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// countSketchEncodingToHostNeutral mirrors the otel adapter's
// encoding-mapping helper. Inlined here to keep the shim's import
// surface narrow.
func countSketchEncodingToHostNeutral(e pmetric.CountSketchEncoding) precompute.Encoding {
	switch e {
	case pmetric.CountSketchEncodingProto:
		return precompute.EncodingProtoFull
	case pmetric.CountSketchEncodingDelta:
		return precompute.EncodingProtoDelta
	case pmetric.CountSketchEncodingMsgpack:
		return precompute.EncodingMsgpack
	}
	return precompute.EncodingProtoFull
}

// hostNeutralToTypedEncoding maps the runtime's host-neutral encoding
// tag back to the typed pmetric.CountSketchEncoding enum the legacy
// processor wrote. The runtime's own encode helper always writes
// Proto; we override here so PROTO_DELTA frames are visible to
// downstream consumers (the backend's modified-OTLP router and the
// existing delta-transmission tests both dispatch on this enum).
func hostNeutralToTypedEncoding(e precompute.Encoding, cfgEnc SketchEncoding) pmetric.CountSketchEncoding {
	switch e {
	case precompute.EncodingProtoDelta:
		return pmetric.CountSketchEncodingDelta
	case precompute.EncodingMsgpack:
		return pmetric.CountSketchEncodingMsgpack
	}
	if cfgEnc == EncodingMsgpack {
		return pmetric.CountSketchEncodingMsgpack
	}
	return pmetric.CountSketchEncodingProto
}
