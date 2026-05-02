package otel

import (
	"fmt"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// Encode converts a slice of host-neutral SketchEnvelopes into a
// pmetric.Metrics suitable for handoff to OTel's downstream pipeline.
//
// Envelopes are grouped by ResourceLabels equality. Each unique
// ResourceLabels group produces one ResourceMetrics; inside it, a
// single ScopeMetrics holds one Metric per envelope. Metric naming
// follows AdapterConfig.metricNameFor: cfg.MetricName overrides
// entirely; otherwise the envelope's MetricName (set by the runtime
// from PrecomputeConfig.MetricName at flush time) plus
// cfg.MetricSuffix is used. If neither is set the encode path
// falls back to "asap.agg_<id>" so downstream consumers always see
// a non-empty name.
//
// Envelopes carry an explicit MetricName field as of step 2.4b —
// the labels list no longer contains the "_asap_metric_name"
// side-channel key. Likewise SketchEnvelope.Count and
// SketchEnvelope.AggregationTemporality are typed fields (not
// labels) and the encode path reads them directly when populating
// the output data point's count / temporality where the chosen
// pmetric data variant supports them.
//
// Phase 2 limitation: only TransmitSketch=true is supported here.
// Quantile-output mode (TransmitSketch=false → emit gauge per
// quantile) is deferred to Phase-2.5 because Encode does not have
// a Sketch instance to query — the envelope's Payload is bytes,
// not a reconstructed sketch. See the spec's "Phase-2.5 follow-up"
// note.
func Encode(envelopes []*precompute.SketchEnvelope, cfg *AdapterConfig) (pmetric.Metrics, error) {
	md := pmetric.NewMetrics()
	if len(envelopes) == 0 {
		return md, nil
	}

	// Group by ResourceLabels (sorted-by-key from the runtime, so
	// equal slices have equal canonical-string forms).
	groupOrder := make([]string, 0)
	groups := make(map[string][]*precompute.SketchEnvelope)
	for _, env := range envelopes {
		if env == nil {
			continue
		}
		key := canonicalLabelsKey(env.ResourceLabels)
		if _, ok := groups[key]; !ok {
			groupOrder = append(groupOrder, key)
		}
		groups[key] = append(groups[key], env)
	}

	for _, gkey := range groupOrder {
		group := groups[gkey]
		if len(group) == 0 {
			continue
		}
		rm := md.ResourceMetrics().AppendEmpty()
		KeyValuesToAttributes(group[0].ResourceLabels, rm.Resource().Attributes())
		sm := rm.ScopeMetrics().AppendEmpty()
		sm.Scope().SetName(cfg.scopeNameOrDefault())
		for _, env := range group {
			if err := writeMetric(sm.Metrics().AppendEmpty(), env, cfg); err != nil {
				return md, err
			}
		}
	}
	return md, nil
}

// writeMetric materializes one SketchEnvelope into a single
// pmetric.Metric. The metric's data variant matches the envelope's
// SketchType; payload bytes are copied verbatim. Encoding is mapped
// from the host-neutral precompute.Encoding to the OTel-typed
// per-sketch encoding enum.
func writeMetric(out pmetric.Metric, env *precompute.SketchEnvelope, cfg *AdapterConfig) error {
	out.SetName(metricNameFor(env, cfg))

	// Convert WindowStart / WindowEnd to pcommon.Timestamp (ns).
	startTs := pcommon.Timestamp(env.WindowStartMs * 1_000_000)
	endTs := pcommon.Timestamp(env.WindowEndMs * 1_000_000)

	switch env.SketchType {
	case precompute.SketchTypeDDSketch:
		dst := out.SetEmptyDDSketch()
		dp := dst.DataPoints().AppendEmpty()
		KeyValuesToAttributes(env.Labels, dp.Attributes())
		dp.SetStartTimestamp(startTs)
		dp.SetTimestamp(endTs)
		dp.SetSketch(env.Payload)
		dp.SetEncoding(hostNeutralToDDSketchEncoding(env.Encoding))
	case precompute.SketchTypeKLLSketch:
		dst := out.SetEmptyKLLSketch()
		dp := dst.DataPoints().AppendEmpty()
		KeyValuesToAttributes(env.Labels, dp.Attributes())
		dp.SetStartTimestamp(startTs)
		dp.SetTimestamp(endTs)
		dp.SetSketch(env.Payload)
		dp.SetEncoding(hostNeutralToKLLSketchEncoding(env.Encoding))
	case precompute.SketchTypeHLLSketch:
		dst := out.SetEmptyHLLSketch()
		dp := dst.DataPoints().AppendEmpty()
		KeyValuesToAttributes(env.Labels, dp.Attributes())
		dp.SetStartTimestamp(startTs)
		dp.SetTimestamp(endTs)
		dp.SetSketch(env.Payload)
		dp.SetEncoding(hostNeutralToHLLSketchEncoding(env.Encoding))
	case precompute.SketchTypeCountSketch:
		dst := out.SetEmptyCountSketch()
		dp := dst.DataPoints().AppendEmpty()
		KeyValuesToAttributes(env.Labels, dp.Attributes())
		dp.SetStartTimestamp(startTs)
		dp.SetTimestamp(endTs)
		dp.SetSketch(env.Payload)
		dp.SetEncoding(hostNeutralToCountSketchEncoding(env.Encoding))
	case precompute.SketchTypeCountMinSketch:
		dst := out.SetEmptyCountMinSketch()
		dp := dst.DataPoints().AppendEmpty()
		KeyValuesToAttributes(env.Labels, dp.Attributes())
		dp.SetStartTimestamp(startTs)
		dp.SetTimestamp(endTs)
		dp.SetSketch(env.Payload)
		dp.SetEncoding(hostNeutralToCountMinSketchEncoding(env.Encoding))
	default:
		return fmt.Errorf("otel.Encode: unsupported sketch type %s", env.SketchType)
	}
	return nil
}

// metricNameFor picks the output Metric.Name. cfg.MetricName overrides
// entirely; otherwise the envelope's typed MetricName field (set by
// the runtime from PrecomputeConfig.MetricName at flush time) plus
// cfg.MetricSuffix is used. If neither is set, a stable placeholder
// "asap.agg_<id>" is used so downstream consumers always see a
// non-empty name.
//
// As of step 2.4b the envelope carries MetricName as a typed field,
// so this function no longer searches Labels for the legacy
// "_asap_metric_name" side-channel key.
func metricNameFor(env *precompute.SketchEnvelope, cfg *AdapterConfig) string {
	if cfg != nil && cfg.MetricName != "" {
		return cfg.MetricName
	}
	base := env.MetricName
	if base == "" {
		base = fmt.Sprintf("asap.agg_%d", env.AggID)
	}
	if cfg != nil {
		base += cfg.MetricSuffix
	}
	return base
}

// canonicalLabelsKey produces a stable string for grouping envelopes
// by ResourceLabels. Assumes the labels are already sorted by key
// (which the runtime guarantees via AttributesToKeyValues).
func canonicalLabelsKey(kvs []precompute.KeyValue) string {
	if len(kvs) == 0 {
		return ""
	}
	var b strings.Builder
	for _, kv := range kvs {
		b.WriteString(kv.Key)
		b.WriteByte('=')
		b.WriteString(kv.Value)
		b.WriteByte(';')
	}
	return b.String()
}

// hostNeutralToDDSketchEncoding maps precompute.Encoding back to the
// OTel-typed DDSketch encoding enum.
func hostNeutralToDDSketchEncoding(e precompute.Encoding) pmetric.DDSketchEncoding {
	switch e {
	case precompute.EncodingProtoFull:
		return pmetric.DDSketchEncodingProto
	case precompute.EncodingProtoDelta:
		return pmetric.DDSketchEncodingProtoDelta
	case precompute.EncodingMsgpack:
		return pmetric.DDSketchEncodingMsgpack
	}
	return pmetric.DDSketchEncodingProto
}

func hostNeutralToKLLSketchEncoding(e precompute.Encoding) pmetric.KLLSketchEncoding {
	switch e {
	case precompute.EncodingMsgpack:
		return pmetric.KLLSketchEncodingMsgpack
	}
	return pmetric.KLLSketchEncodingProto
}

func hostNeutralToHLLSketchEncoding(e precompute.Encoding) pmetric.HLLSketchEncoding {
	switch e {
	case precompute.EncodingProtoDelta:
		return pmetric.HLLSketchEncodingDelta
	case precompute.EncodingMsgpack:
		return pmetric.HLLSketchEncodingMsgpack
	}
	return pmetric.HLLSketchEncodingProto
}

func hostNeutralToCountSketchEncoding(e precompute.Encoding) pmetric.CountSketchEncoding {
	return pmetric.CountSketchEncodingProto
}

func hostNeutralToCountMinSketchEncoding(e precompute.Encoding) pmetric.CountMinSketchEncoding {
	return pmetric.CountMinSketchEncodingProto
}
