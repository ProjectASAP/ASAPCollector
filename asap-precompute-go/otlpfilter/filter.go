package otlpfilter

import (
	"google.golang.org/protobuf/encoding/protowire"
)

// OTLP metrics protobuf field numbers (from
// opentelemetry/proto/collector/metrics/v1/metrics_service.proto and
// opentelemetry/proto/metrics/v1/metrics.proto). Only the fields the walk needs
// are named; every other field is copied through verbatim.
const (
	// ExportMetricsServiceRequest
	fieldResourceMetrics = 1 // repeated ResourceMetrics

	// ResourceMetrics
	fieldRMScopeMetrics = 2 // repeated ScopeMetrics
	// (field 1 = Resource, field 3 = schema_url — copied verbatim)

	// ScopeMetrics
	fieldSMMetrics = 2 // repeated Metric
	// (field 1 = scope, field 3 = schema_url — copied verbatim)

	// Metric
	fieldMetricName = 1 // string name
	// oneof data — each variant is a length-delimited sub-message holding the
	// data_points list. We rewrite whichever variant is present.
	fieldMetricGauge     = 5
	fieldMetricSum       = 7
	fieldMetricHistogram = 9
	fieldMetricExpoHist  = 10
	fieldMetricSummary   = 11

	// Gauge / Sum / Histogram / ExponentialHistogram / Summary
	fieldDataPoints = 1 // repeated <DataPoint>
)

// FilterRequest takes a marshalled OTLP ExportMetricsServiceRequest and returns
// a new, byte-valid ExportMetricsServiceRequest in which the datapoints of
// warm-sketch (sampled) metrics have been geometrically thinned at the wire
// level. Cold metrics (absent from the p-map or p>=1) and all non-target fields
// are copied through verbatim — never decoded. Kept datapoints are copied
// opaque, so their attribute maps and values are never materialized inside the
// filter.
//
// On any malformed input the original bytes are returned unchanged (fail-open):
// the wire-filter must never corrupt a payload the downstream decoder could
// otherwise have read.
func (s *SampleState) FilterRequest(otlpBytes []byte) []byte {
	out, ok := s.rewriteRepeated(otlpBytes, fieldResourceMetrics, s.rewriteResourceMetrics)
	if !ok {
		return otlpBytes
	}
	return out
}

// rewriteResourceMetrics rewrites one ResourceMetrics message: descend into its
// repeated ScopeMetrics (field 2), copy resource/schema_url verbatim.
func (s *SampleState) rewriteResourceMetrics(rm []byte) []byte {
	out, ok := s.rewriteRepeated(rm, fieldRMScopeMetrics, s.rewriteScopeMetrics)
	if !ok {
		return rm
	}
	return out
}

// rewriteScopeMetrics rewrites one ScopeMetrics message: descend into its
// repeated Metric (field 2), copy scope/schema_url verbatim.
func (s *SampleState) rewriteScopeMetrics(sm []byte) []byte {
	out, ok := s.rewriteRepeated(sm, fieldSMMetrics, s.rewriteMetric)
	if !ok {
		return sm
	}
	return out
}

// rewriteMetric rewrites one Metric message. It first reads the metric name
// (field 1) cheaply, classifies via admit, and:
//   - if NOT sampled (cold / p>=1): returns the metric bytes unchanged.
//   - if sampled (warm): locates the present data-oneof variant (gauge/sum/...)
//     and rewrites that sub-message's repeated data_points (field 1), keeping
//     each datapoint iff the per-metric geometric sampler admits it.
func (s *SampleState) rewriteMetric(metric []byte) []byte {
	name, ok := readMetricName(metric)
	if !ok {
		return metric // unparseable name; pass through.
	}
	sampled, _ := s.admit(name)
	if !sampled {
		// Cold / raw / p>=1: copy the whole Metric through unchanged. We do NOT
		// consume a sampler admission for cold metrics.
		return metric
	}

	// Warm metric: rewrite the data-oneof variant that is present. Exactly one
	// of the data fields is set in a well-formed Metric; we rewrite whichever we
	// encounter and copy everything else (name, description, unit, metadata)
	// verbatim. The per-datapoint admission for THIS metric is drawn inside
	// dropOrKeepDataPoint via the metric name captured here.
	out := make([]byte, 0, len(metric))
	b := metric
	for len(b) > 0 {
		num, typ, tagLen := protowire.ConsumeTag(b)
		if tagLen < 0 {
			return metric // malformed; fail-open on whole metric.
		}
		valLen := protowire.ConsumeFieldValue(num, typ, b[tagLen:])
		if valLen < 0 {
			return metric
		}
		fieldLen := tagLen + valLen
		field := b[:fieldLen]

		if typ == protowire.BytesType && isDataField(num) {
			// This is the data-oneof sub-message; rewrite its data_points.
			dataMsg, _ := protowire.ConsumeBytes(b[tagLen:])
			newData, dok := s.rewriteRepeated(dataMsg, fieldDataPoints, func(dp []byte) []byte {
				return s.keepOrDropDataPoint(name, dp)
			})
			if !dok {
				// Malformed data sub-message: copy verbatim.
				out = append(out, field...)
			} else {
				out = protowire.AppendTag(out, num, protowire.BytesType)
				out = protowire.AppendBytes(out, newData)
			}
		} else {
			// name / description / unit / metadata / unknown: verbatim.
			out = append(out, field...)
		}
		b = b[fieldLen:]
	}
	return out
}

// keepOrDropDataPoint draws one geometric admission for `name` and returns the
// datapoint bytes verbatim (keep) or nil (drop). The datapoint is NEVER
// decoded: a kept datapoint is copied opaque, preserving its attributes/value
// exactly; a dropped datapoint is wire-skipped and never materialized.
func (s *SampleState) keepOrDropDataPoint(name string, dp []byte) []byte {
	_, keep := s.admit(name)
	if !keep {
		return nil
	}
	return dp
}

// readMetricName extracts the Metric.name (field 1, string) without decoding any
// other field. Returns ("", false) only on a malformed metric; a Metric with no
// name field yields ("", true) and is treated as a (nameless) cold metric.
func readMetricName(metric []byte) (string, bool) {
	b := metric
	for len(b) > 0 {
		num, typ, tagLen := protowire.ConsumeTag(b)
		if tagLen < 0 {
			return "", false
		}
		if num == fieldMetricName && typ == protowire.BytesType {
			v, n := protowire.ConsumeBytes(b[tagLen:])
			if n < 0 {
				return "", false
			}
			return string(v), true
		}
		valLen := protowire.ConsumeFieldValue(num, typ, b[tagLen:])
		if valLen < 0 {
			return "", false
		}
		b = b[tagLen+valLen:]
	}
	return "", true
}

// isDataField reports whether a Metric field number is one of the data-oneof
// variants (gauge/sum/histogram/exponential_histogram/summary), all of which
// are length-delimited sub-messages carrying repeated data_points = 1.
func isDataField(num protowire.Number) bool {
	switch num {
	case fieldMetricGauge, fieldMetricSum, fieldMetricHistogram,
		fieldMetricExpoHist, fieldMetricSummary:
		return true
	default:
		return false
	}
}

// rewriteRepeated walks a protobuf message `buf` and rewrites the
// length-delimited (wiretype-2) elements of field number `targetField` by
// running `fn` on each element's bytes:
//   - fn returns the (possibly identical) replacement bytes => re-emitted as a
//     length-delimited targetField element.
//   - fn returns nil => the element is dropped (wire-skipped).
//
// Every other field — non-target fields, and target-field occurrences that are
// NOT wiretype-2 (defensive) — is copied through verbatim, including varint,
// fixed32, fixed64 and other length-delimited fields. The second return value
// is false iff `buf` is malformed (in which case the caller should fail-open and
// keep the original bytes).
func (s *SampleState) rewriteRepeated(buf []byte, targetField protowire.Number, fn func([]byte) []byte) ([]byte, bool) {
	out := make([]byte, 0, len(buf))
	b := buf
	for len(b) > 0 {
		num, typ, tagLen := protowire.ConsumeTag(b)
		if tagLen < 0 {
			return nil, false
		}
		valLen := protowire.ConsumeFieldValue(num, typ, b[tagLen:])
		if valLen < 0 {
			return nil, false
		}
		fieldLen := tagLen + valLen

		if num == targetField && typ == protowire.BytesType {
			elem, n := protowire.ConsumeBytes(b[tagLen:])
			if n < 0 {
				return nil, false
			}
			rewritten := fn(elem)
			if rewritten != nil {
				out = protowire.AppendTag(out, num, protowire.BytesType)
				out = protowire.AppendBytes(out, rewritten)
			}
			// rewritten == nil => drop (emit nothing).
		} else {
			// Non-target field (any wiretype) copied verbatim.
			out = append(out, b[:fieldLen]...)
		}
		b = b[fieldLen:]
	}
	return out, true
}
