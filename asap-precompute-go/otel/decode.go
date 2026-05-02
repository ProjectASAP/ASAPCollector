package otel

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// Decode converts a pmetric.Metrics into a flat slice of host-neutral
// Observations. Walks ResourceMetrics → ScopeMetrics → Metric and
// produces one Observation per data point.
//
// Sketch-typed inputs (DDSketch / KLLSketch / HLLSketch / CountSketch
// / CountMinSketch) are surfaced as ObservationValueKind=KindEnvelope
// with the original payload bytes preserved verbatim — the runtime
// then routes them through Precompute.ObserveEnvelope, never
// expanding them to scalar samples (design-doc §5.2 bandwidth
// invariant).
//
// Gauge / Sum data points become KindFloat observations. The Adapter
// honors cfg.ReadAsInt for typed integer reads (matching the KLL
// processor's `is_int` knob); when ReadAsInt is true, IntValue() is
// read and converted to float for the Float field.
func Decode(md pmetric.Metrics, cfg *AdapterConfig) ([]precompute.Observation, error) {
	if md.ResourceMetrics().Len() == 0 {
		return nil, nil
	}
	out := make([]precompute.Observation, 0, md.DataPointCount())
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		resourceLabels := AttributesToKeyValues(rm.Resource().Attributes())
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			ms := sm.Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				if err := decodeMetric(m, resourceLabels, cfg, &out); err != nil {
					return nil, err
				}
			}
		}
	}
	return out, nil
}

// decodeMetric appends Observations for a single Metric into out.
func decodeMetric(
	m pmetric.Metric,
	resourceLabels []precompute.KeyValue,
	cfg *AdapterConfig,
	out *[]precompute.Observation,
) error {
	name := m.Name()
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		dps := m.Gauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			val, ok := readNumberValue(dp, cfg)
			if !ok {
				continue
			}
			*out = append(*out, precompute.Observation{
				TimestampMs:    timestampMs(dp.Timestamp()),
				Metric:         name,
				ResourceLabels: resourceLabels,
				Labels:         AttributesToKeyValues(dp.Attributes()),
				Value:          precompute.FloatValue(val),
			})
		}
	case pmetric.MetricTypeSum:
		dps := m.Sum().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			val, ok := readNumberValue(dp, cfg)
			if !ok {
				continue
			}
			*out = append(*out, precompute.Observation{
				TimestampMs:    timestampMs(dp.Timestamp()),
				Metric:         name,
				ResourceLabels: resourceLabels,
				Labels:         AttributesToKeyValues(dp.Attributes()),
				Value:          precompute.FloatValue(val),
			})
		}
	case pmetric.MetricTypeDDSketch:
		dps := m.DDSketch().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			env := &precompute.SketchEnvelope{
				SchemaVersion:  1,
				SketchType:     precompute.SketchTypeDDSketch,
				ResourceLabels: resourceLabels,
				Labels:         AttributesToKeyValues(dp.Attributes()),
				WindowStartMs:  timestampMs(dp.StartTimestamp()),
				WindowEndMs:    timestampMs(dp.Timestamp()),
				Encoding:       ddSketchEncodingToHostNeutral(dp.Encoding()),
				Payload:        copyBytes(dp.Sketch()),
			}
			*out = append(*out, precompute.Observation{
				TimestampMs:    timestampMs(dp.Timestamp()),
				Metric:         name,
				ResourceLabels: resourceLabels,
				Labels:         env.Labels,
				Value:          precompute.EnvelopeValue(env),
			})
		}
	case pmetric.MetricTypeKLLSketch:
		dps := m.KLLSketch().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			env := &precompute.SketchEnvelope{
				SchemaVersion:  1,
				SketchType:     precompute.SketchTypeKLLSketch,
				ResourceLabels: resourceLabels,
				Labels:         AttributesToKeyValues(dp.Attributes()),
				WindowStartMs:  timestampMs(dp.StartTimestamp()),
				WindowEndMs:    timestampMs(dp.Timestamp()),
				Encoding:       kllSketchEncodingToHostNeutral(dp.Encoding()),
				Payload:        copyBytes(dp.Sketch()),
			}
			*out = append(*out, precompute.Observation{
				TimestampMs:    timestampMs(dp.Timestamp()),
				Metric:         name,
				ResourceLabels: resourceLabels,
				Labels:         env.Labels,
				Value:          precompute.EnvelopeValue(env),
			})
		}
	case pmetric.MetricTypeHLLSketch:
		dps := m.HLLSketch().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			env := &precompute.SketchEnvelope{
				SchemaVersion:  1,
				SketchType:     precompute.SketchTypeHLLSketch,
				ResourceLabels: resourceLabels,
				Labels:         AttributesToKeyValues(dp.Attributes()),
				WindowStartMs:  timestampMs(dp.StartTimestamp()),
				WindowEndMs:    timestampMs(dp.Timestamp()),
				Encoding:       hllSketchEncodingToHostNeutral(dp.Encoding()),
				Payload:        copyBytes(dp.Sketch()),
			}
			*out = append(*out, precompute.Observation{
				TimestampMs:    timestampMs(dp.Timestamp()),
				Metric:         name,
				ResourceLabels: resourceLabels,
				Labels:         env.Labels,
				Value:          precompute.EnvelopeValue(env),
			})
		}
	case pmetric.MetricTypeCountSketch:
		dps := m.CountSketch().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			env := &precompute.SketchEnvelope{
				SchemaVersion:  1,
				SketchType:     precompute.SketchTypeCountSketch,
				ResourceLabels: resourceLabels,
				Labels:         AttributesToKeyValues(dp.Attributes()),
				WindowStartMs:  timestampMs(dp.StartTimestamp()),
				WindowEndMs:    timestampMs(dp.Timestamp()),
				Encoding:       countSketchEncodingToHostNeutral(dp.Encoding()),
				Payload:        copyBytes(dp.Sketch()),
			}
			*out = append(*out, precompute.Observation{
				TimestampMs:    timestampMs(dp.Timestamp()),
				Metric:         name,
				ResourceLabels: resourceLabels,
				Labels:         env.Labels,
				Value:          precompute.EnvelopeValue(env),
			})
		}
	case pmetric.MetricTypeCountMinSketch:
		dps := m.CountMinSketch().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			env := &precompute.SketchEnvelope{
				SchemaVersion:  1,
				SketchType:     precompute.SketchTypeCountMinSketch,
				ResourceLabels: resourceLabels,
				Labels:         AttributesToKeyValues(dp.Attributes()),
				WindowStartMs:  timestampMs(dp.StartTimestamp()),
				WindowEndMs:    timestampMs(dp.Timestamp()),
				Encoding:       countMinSketchEncodingToHostNeutral(dp.Encoding()),
				Payload:        copyBytes(dp.Sketch()),
			}
			*out = append(*out, precompute.Observation{
				TimestampMs:    timestampMs(dp.Timestamp()),
				Metric:         name,
				ResourceLabels: resourceLabels,
				Labels:         env.Labels,
				Value:          precompute.EnvelopeValue(env),
			})
		}
	default:
		// Histogram / ExponentialHistogram / Summary aren't part of the
		// ASAP sketch surface today; ignore silently. A future
		// extension would add an explicit "drop reason" telemetry
		// counter, but that's out of scope for Phase 2.
		return nil
	}
	return nil
}

// readNumberValue extracts the float value from a NumberDataPoint
// honoring cfg.ReadAsInt. Returns (val, true) if a value was
// readable; (0, false) for empty data points.
func readNumberValue(dp pmetric.NumberDataPoint, cfg *AdapterConfig) (float64, bool) {
	if cfg != nil && cfg.ReadAsInt {
		if dp.ValueType() == pmetric.NumberDataPointValueTypeInt {
			return float64(dp.IntValue()), true
		}
		// Fall back to double when ReadAsInt is set but the data
		// point carries a double; the existing KLL processor logs
		// a warning here, but the runtime is logger-free.
		if dp.ValueType() == pmetric.NumberDataPointValueTypeDouble {
			return dp.DoubleValue(), true
		}
		return 0, false
	}
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeDouble:
		return dp.DoubleValue(), true
	case pmetric.NumberDataPointValueTypeInt:
		return float64(dp.IntValue()), true
	}
	return 0, false
}

// timestampMs converts a pcommon.Timestamp (nanoseconds since epoch)
// to milliseconds.
func timestampMs(t pcommon.Timestamp) uint64 {
	return uint64(t / 1_000_000)
}

// copyBytes returns a defensive copy of b so the host-neutral
// envelope doesn't alias the host's pmetric storage.
func copyBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// ddSketchEncodingToHostNeutral maps the OTel-typed DDSketchEncoding
// enum to the host-neutral precompute.Encoding.
func ddSketchEncodingToHostNeutral(e pmetric.DDSketchEncoding) precompute.Encoding {
	switch e {
	case pmetric.DDSketchEncodingProto:
		return precompute.EncodingProtoFull
	case pmetric.DDSketchEncodingProtoDelta:
		return precompute.EncodingProtoDelta
	case pmetric.DDSketchEncodingMsgpack, pmetric.DDSketchEncodingMsgpackDelta:
		return precompute.EncodingMsgpack
	}
	return precompute.EncodingUnspecified
}

// kllSketchEncodingToHostNeutral mirrors the DDSketch helper for KLL.
func kllSketchEncodingToHostNeutral(e pmetric.KLLSketchEncoding) precompute.Encoding {
	switch e {
	case pmetric.KLLSketchEncodingProto:
		return precompute.EncodingProtoFull
	case pmetric.KLLSketchEncodingMsgpack, pmetric.KLLSketchEncodingMsgpackDelta:
		return precompute.EncodingMsgpack
	}
	return precompute.EncodingUnspecified
}

// hllSketchEncodingToHostNeutral mirrors the DDSketch helper for HLL.
func hllSketchEncodingToHostNeutral(e pmetric.HLLSketchEncoding) precompute.Encoding {
	switch e {
	case pmetric.HLLSketchEncodingProto:
		return precompute.EncodingProtoFull
	case pmetric.HLLSketchEncodingDelta:
		return precompute.EncodingProtoDelta
	case pmetric.HLLSketchEncodingMsgpack, pmetric.HLLSketchEncodingMsgpackDelta:
		return precompute.EncodingMsgpack
	}
	return precompute.EncodingUnspecified
}

// countSketchEncodingToHostNeutral mirrors the DDSketch helper for
// CountSketch.
func countSketchEncodingToHostNeutral(e pmetric.CountSketchEncoding) precompute.Encoding {
	switch e {
	case pmetric.CountSketchEncodingProto:
		return precompute.EncodingProtoFull
	}
	return precompute.EncodingUnspecified
}

// countMinSketchEncodingToHostNeutral mirrors the DDSketch helper for
// CountMin.
func countMinSketchEncodingToHostNeutral(e pmetric.CountMinSketchEncoding) precompute.Encoding {
	switch e {
	case pmetric.CountMinSketchEncodingProto:
		return precompute.EncodingProtoFull
	}
	return precompute.EncodingUnspecified
}
