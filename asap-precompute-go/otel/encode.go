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
// entirely; otherwise the envelope's metric-name (carried in the
// last KeyValue with key "_asap_metric_name", set by the runtime
// when wrapping back) plus cfg.MetricSuffix is used. Phase 2 keeps
// metric naming simple — the runtime currently does NOT thread an
// explicit Metric name through SketchEnvelope (see TODO below);
// the encode path falls back to the envelope's AggID stringified
// as a placeholder name.
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
// entirely; otherwise the envelope's Labels are searched for the
// well-known runtime-supplied "_asap_metric_name" key — that's how
// the upstream Precompute threads the original metric name back into
// the encode boundary. If neither is set, a stable placeholder
// "asap.agg_<id>" is used so downstream consumers always see a
// non-empty name.
//
// TODO(phase-2-followup): SketchEnvelope should carry an explicit
// MetricName field; threading it through Labels is a Phase-2-bootstrap
// shortcut so the existing OTel processors can preserve their
// per-config MetricSuffix behavior on the encode path.
func metricNameFor(env *precompute.SketchEnvelope, cfg *AdapterConfig) string {
	if cfg != nil && cfg.MetricName != "" {
		return cfg.MetricName
	}
	base := lookupLabel(env.Labels, "_asap_metric_name")
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

// lookupLabel returns the value for the given key in kvs, or the
// empty string if missing.
func lookupLabel(kvs []precompute.KeyValue, key string) string {
	for _, kv := range kvs {
		if kv.Key == key {
			return kv.Value
		}
	}
	return ""
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
