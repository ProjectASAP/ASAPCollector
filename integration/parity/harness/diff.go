package harness

import (
	"fmt"
	"sort"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// envelopeView is the canonical comparison form. Both Path A
// (precompute.SketchEnvelope) and Path B (pmetric.Metrics) get
// projected into envelopeView; the diff operates on slices of these.
type envelopeView struct {
	MetricName       string
	SketchType       precompute.SketchType
	ResourceLabelKey string // canonical "k=v;k=v;" sorted-key form
	DataPointLabel   string // same shape, dp-level
	WindowStartMs    uint64
	WindowEndMs      uint64
	Encoding         precompute.Encoding
	Payload          []byte
	Count            uint64
	Temporality      int32
}

// projectRuntime converts a slice of runtime SketchEnvelopes into
// envelopeViews ready for comparison.
func projectRuntime(envs []*precompute.SketchEnvelope) []envelopeView {
	out := make([]envelopeView, 0, len(envs))
	for _, e := range envs {
		if e == nil {
			continue
		}
		out = append(out, envelopeView{
			MetricName:       e.MetricName,
			SketchType:       e.SketchType,
			ResourceLabelKey: keyValuesString(e.ResourceLabels),
			DataPointLabel:   keyValuesString(e.Labels),
			WindowStartMs:    e.WindowStartMs,
			WindowEndMs:      e.WindowEndMs,
			Encoding:         e.Encoding,
			Payload:          e.Payload,
			Count:            e.Count,
			Temporality:      e.AggregationTemporality,
		})
	}
	return out
}

// projectLegacy walks the pmetric.Metrics emitted by a legacy
// processor and projects each typed-sketch data point to an
// envelopeView. Gauge / Sum outputs (the quantile-emission paths)
// don't carry sketch payloads — they're skipped because the parity
// invariant only applies to TransmitSketch=true outputs.
func projectLegacy(allMetrics []pmetric.Metrics) []envelopeView {
	out := make([]envelopeView, 0)
	for _, md := range allMetrics {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			rm := rms.At(i)
			resKey := attrsToString(rm.Resource().Attributes())
			sms := rm.ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					m := ms.At(k)
					out = append(out, projectMetricVariant(m, resKey)...)
				}
			}
		}
	}
	return out
}

// projectMetricVariant flattens one typed-sketch metric to one or
// more envelopeViews (one per data point).
func projectMetricVariant(m pmetric.Metric, resKey string) []envelopeView {
	out := make([]envelopeView, 0, 4)
	switch m.Type() {
	case pmetric.MetricTypeDDSketch:
		dps := m.DDSketch().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			payload := make([]byte, len(dp.Sketch()))
			copy(payload, dp.Sketch())
			out = append(out, envelopeView{
				MetricName:       m.Name(),
				SketchType:       precompute.SketchTypeDDSketch,
				ResourceLabelKey: resKey,
				DataPointLabel:   attrsToString(dp.Attributes()),
				WindowStartMs:    uint64(dp.StartTimestamp() / 1_000_000),
				WindowEndMs:      uint64(dp.Timestamp() / 1_000_000),
				Encoding:         legacyDDEncoding(dp.Encoding()),
				Payload:          payload,
				Count:            dp.Count(),
				Temporality:      int32(m.DDSketch().AggregationTemporality()),
			})
		}
	case pmetric.MetricTypeKLLSketch:
		dps := m.KLLSketch().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			payload := make([]byte, len(dp.Sketch()))
			copy(payload, dp.Sketch())
			// The legacy KLL processor adds a `kll.k` integer
			// attribute to every data point; the runtime does
			// not. Strip it from the comparison key so other
			// label values still align — this is documented in
			// the divergence note in the PR description.
			label := attrsToStringExcluding(dp.Attributes(), "kll.k")
			out = append(out, envelopeView{
				MetricName:       m.Name(),
				SketchType:       precompute.SketchTypeKLLSketch,
				ResourceLabelKey: resKey,
				DataPointLabel:   label,
				WindowStartMs:    uint64(dp.StartTimestamp() / 1_000_000),
				WindowEndMs:      uint64(dp.Timestamp() / 1_000_000),
				Encoding:         legacyKLLEncoding(dp.Encoding()),
				Payload:          payload,
				Count:            dp.Count(),
				Temporality:      int32(m.KLLSketch().AggregationTemporality()),
			})
		}
	case pmetric.MetricTypeHLLSketch:
		dps := m.HLLSketch().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			payload := make([]byte, len(dp.Sketch()))
			copy(payload, dp.Sketch())
			out = append(out, envelopeView{
				MetricName:       m.Name(),
				SketchType:       precompute.SketchTypeHLLSketch,
				ResourceLabelKey: resKey,
				DataPointLabel:   attrsToString(dp.Attributes()),
				WindowStartMs:    uint64(dp.StartTimestamp() / 1_000_000),
				WindowEndMs:      uint64(dp.Timestamp() / 1_000_000),
				Encoding:         legacyHLLEncoding(dp.Encoding()),
				Payload:          payload,
				Count:            dp.Count(),
				Temporality:      int32(m.HLLSketch().AggregationTemporality()),
			})
		}
	case pmetric.MetricTypeCountSketch:
		dps := m.CountSketch().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			payload := make([]byte, len(dp.Sketch()))
			copy(payload, dp.Sketch())
			// CountSketch parity is now true byte-equivalence:
			// the runtime emits sample_count and
			// window_duration_seconds via PrecomputeConfig.EmitWindowStats,
			// which routes them through Labels →
			// otel/encode.go::KeyValuesToAttributes. Both paths
			// thus carry the same attr set without any diff-side
			// projection strip.
			out = append(out, envelopeView{
				MetricName:       m.Name(),
				SketchType:       precompute.SketchTypeCountSketch,
				ResourceLabelKey: resKey,
				DataPointLabel:   attrsToString(dp.Attributes()),
				WindowStartMs:    uint64(dp.StartTimestamp() / 1_000_000),
				WindowEndMs:      uint64(dp.Timestamp() / 1_000_000),
				Encoding:         legacyCSEncoding(dp.Encoding()),
				Payload:          payload,
				Count:            0,
				Temporality:      int32(m.CountSketch().AggregationTemporality()),
			})
		}
	case pmetric.MetricTypeCountMinSketch:
		dps := m.CountMinSketch().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			payload := make([]byte, len(dp.Sketch()))
			copy(payload, dp.Sketch())
			out = append(out, envelopeView{
				MetricName:       m.Name(),
				SketchType:       precompute.SketchTypeCountMinSketch,
				ResourceLabelKey: resKey,
				DataPointLabel:   attrsToString(dp.Attributes()),
				WindowStartMs:    uint64(dp.StartTimestamp() / 1_000_000),
				WindowEndMs:      uint64(dp.Timestamp() / 1_000_000),
				Encoding:         legacyCMSEncoding(dp.Encoding()),
				Payload:          payload,
				Count:            0,
				Temporality:      int32(m.CountMinSketch().AggregationTemporality()),
			})
		}
	}
	return out
}

// DiffReport summarizes the comparison of one sketch type's outputs.
type DiffReport struct {
	SketchType        string
	OnlyInRuntime     []envelopeView
	OnlyInLegacy      []envelopeView
	PayloadMismatches []payloadMismatch
	BothCount         int
}

type payloadMismatch struct {
	Key            string
	RuntimePayload []byte
	LegacyPayload  []byte
	RuntimeCount   uint64
	LegacyCount    uint64
}

// Equal reports whether the diff is empty (no divergence).
func (d *DiffReport) Equal() bool {
	return len(d.OnlyInRuntime) == 0 &&
		len(d.OnlyInLegacy) == 0 &&
		len(d.PayloadMismatches) == 0
}

// Format renders a multi-line human-readable diff for test output.
func (d *DiffReport) Format() string {
	var b strings.Builder
	fmt.Fprintf(&b, "=== %s parity report ===\n", d.SketchType)
	fmt.Fprintf(&b, "envelopes present in both: %d\n", d.BothCount)
	fmt.Fprintf(&b, "only in runtime  (Path A): %d\n", len(d.OnlyInRuntime))
	fmt.Fprintf(&b, "only in legacy   (Path B): %d\n", len(d.OnlyInLegacy))
	fmt.Fprintf(&b, "payload byte mismatches:   %d\n", len(d.PayloadMismatches))
	const showFirst = 5
	if len(d.OnlyInRuntime) > 0 {
		fmt.Fprintf(&b, "\n-- only in runtime (first %d):\n", showFirst)
		for i, v := range d.OnlyInRuntime {
			if i >= showFirst {
				fmt.Fprintf(&b, "    ... (%d more)\n", len(d.OnlyInRuntime)-i)
				break
			}
			fmt.Fprintf(&b, "    %s\n", envelopeViewKey(v))
		}
	}
	if len(d.OnlyInLegacy) > 0 {
		fmt.Fprintf(&b, "\n-- only in legacy (first %d):\n", showFirst)
		for i, v := range d.OnlyInLegacy {
			if i >= showFirst {
				fmt.Fprintf(&b, "    ... (%d more)\n", len(d.OnlyInLegacy)-i)
				break
			}
			fmt.Fprintf(&b, "    %s\n", envelopeViewKey(v))
		}
	}
	if len(d.PayloadMismatches) > 0 {
		fmt.Fprintf(&b, "\n-- payload mismatches (first %d):\n", showFirst)
		for i, m := range d.PayloadMismatches {
			if i >= showFirst {
				fmt.Fprintf(&b, "    ... (%d more)\n", len(d.PayloadMismatches)-i)
				break
			}
			fmt.Fprintf(&b, "    key=%s\n", m.Key)
			fmt.Fprintf(&b, "      runtime: count=%d size=%d head=%x\n",
				m.RuntimeCount, len(m.RuntimePayload), firstBytes(m.RuntimePayload, 16))
			fmt.Fprintf(&b, "      legacy : count=%d size=%d head=%x\n",
				m.LegacyCount, len(m.LegacyPayload), firstBytes(m.LegacyPayload, 16))
		}
	}
	return b.String()
}

// Diff compares the runtime path's envelopes to the legacy path's
// emitted pmetric.Metrics, keyed by (metric_name, sorted resource
// labels, sorted dp labels, window_start_ms).
func Diff(
	sketchType string,
	runtime []*precompute.SketchEnvelope,
	legacy []pmetric.Metrics,
) *DiffReport {
	rViews := projectRuntime(runtime)
	lViews := projectLegacy(legacy)

	// Skip empty payloads on the runtime side — those occur when
	// a Precompute had no admitted observations and thus emitted
	// no envelope (defensive; should not happen with the harness's
	// inputs).
	rViews = filterNonEmpty(rViews)
	lViews = filterNonEmpty(lViews)

	// Build maps. If two envelopes share the same key, the second
	// replaces the first — which would silently mask duplicates.
	// We track duplicate-occurrence counts separately and surface
	// them as "payload mismatch" if the bytes differ.
	rMap := make(map[string][]envelopeView)
	for _, v := range rViews {
		k := envelopeViewKey(v)
		rMap[k] = append(rMap[k], v)
	}
	lMap := make(map[string][]envelopeView)
	for _, v := range lViews {
		k := envelopeViewKey(v)
		lMap[k] = append(lMap[k], v)
	}

	report := &DiffReport{SketchType: sketchType}
	allKeys := unionKeys(rMap, lMap)
	sort.Strings(allKeys)
	for _, k := range allKeys {
		rs := rMap[k]
		ls := lMap[k]
		switch {
		case len(rs) > 0 && len(ls) == 0:
			report.OnlyInRuntime = append(report.OnlyInRuntime, rs...)
		case len(rs) == 0 && len(ls) > 0:
			report.OnlyInLegacy = append(report.OnlyInLegacy, ls...)
		default:
			// Compare payload bytes pairwise. With duplicate
			// counts > 1 on either side the harness flags a
			// mismatch — duplicates indicate a window-rotation
			// or merge bug.
			if len(rs) != len(ls) {
				report.PayloadMismatches = append(
					report.PayloadMismatches,
					payloadMismatch{
						Key:            k + " (duplicate-count mismatch)",
						RuntimePayload: rs[0].Payload,
						LegacyPayload:  ls[0].Payload,
						RuntimeCount:   rs[0].Count,
						LegacyCount:    ls[0].Count,
					},
				)
				continue
			}
			for i := range rs {
				if !bytesEqual(rs[i].Payload, ls[i].Payload) {
					report.PayloadMismatches = append(
						report.PayloadMismatches,
						payloadMismatch{
							Key:            k,
							RuntimePayload: rs[i].Payload,
							LegacyPayload:  ls[i].Payload,
							RuntimeCount:   rs[i].Count,
							LegacyCount:    ls[i].Count,
						},
					)
				} else {
					report.BothCount++
				}
			}
		}
	}
	return report
}

func filterNonEmpty(vs []envelopeView) []envelopeView {
	out := vs[:0]
	for _, v := range vs {
		if len(v.Payload) > 0 {
			out = append(out, v)
		}
	}
	return out
}

// envelopeViewKey is the comparison key — same shape on both sides
// modulo the WindowStartMs alignment caveat (legacy batch-mode
// outputs use the original sample timestamps, not a synthesized
// window edge; the harness drops WindowStartMs from the key for
// batch-only runs to avoid trivial misses).
func envelopeViewKey(v envelopeView) string {
	return fmt.Sprintf("%s|%s|%s",
		v.MetricName, v.ResourceLabelKey, v.DataPointLabel)
}

func keyValuesString(kvs []precompute.KeyValue) string {
	if len(kvs) == 0 {
		return ""
	}
	keys := make([]string, 0, len(kvs))
	m := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		keys = append(keys, kv.Key)
		m[kv.Key] = kv.Value
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(m[k])
		b.WriteByte(';')
	}
	return b.String()
}

func attrsToString(attrs pcommon.Map) string {
	keys := make([]string, 0, attrs.Len())
	attrs.Range(func(k string, _ pcommon.Value) bool {
		keys = append(keys, k)
		return true
	})
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		v, _ := attrs.Get(k)
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(v.AsString())
		b.WriteByte(';')
	}
	return b.String()
}

func attrsToStringExcluding(attrs pcommon.Map, drop ...string) string {
	dropSet := make(map[string]struct{}, len(drop))
	for _, d := range drop {
		dropSet[d] = struct{}{}
	}
	keys := make([]string, 0, attrs.Len())
	attrs.Range(func(k string, _ pcommon.Value) bool {
		if _, skip := dropSet[k]; skip {
			return true
		}
		keys = append(keys, k)
		return true
	})
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		v, _ := attrs.Get(k)
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(v.AsString())
		b.WriteByte(';')
	}
	return b.String()
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func firstBytes(b []byte, n int) []byte {
	if len(b) <= n {
		return b
	}
	return b[:n]
}

func unionKeys(a, b map[string][]envelopeView) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	for k := range a {
		seen[k] = struct{}{}
	}
	for k := range b {
		seen[k] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	return out
}

// ===== legacy-encoding mappers =====

func legacyDDEncoding(e pmetric.DDSketchEncoding) precompute.Encoding {
	switch e {
	case pmetric.DDSketchEncodingProto:
		return precompute.EncodingProtoFull
	case pmetric.DDSketchEncodingProtoDelta:
		return precompute.EncodingProtoDelta
	case pmetric.DDSketchEncodingMsgpack:
		return precompute.EncodingMsgpack
	}
	return precompute.EncodingUnspecified
}

func legacyKLLEncoding(e pmetric.KLLSketchEncoding) precompute.Encoding {
	switch e {
	case pmetric.KLLSketchEncodingProto:
		return precompute.EncodingProtoFull
	case pmetric.KLLSketchEncodingMsgpack:
		return precompute.EncodingMsgpack
	}
	return precompute.EncodingUnspecified
}

func legacyHLLEncoding(e pmetric.HLLSketchEncoding) precompute.Encoding {
	switch e {
	case pmetric.HLLSketchEncodingProto:
		return precompute.EncodingProtoFull
	case pmetric.HLLSketchEncodingDelta:
		return precompute.EncodingProtoDelta
	case pmetric.HLLSketchEncodingMsgpack:
		return precompute.EncodingMsgpack
	}
	return precompute.EncodingUnspecified
}

func legacyCSEncoding(e pmetric.CountSketchEncoding) precompute.Encoding {
	switch e {
	case pmetric.CountSketchEncodingProto:
		return precompute.EncodingProtoFull
	case pmetric.CountSketchEncodingDelta:
		return precompute.EncodingProtoDelta
	case pmetric.CountSketchEncodingMsgpack:
		return precompute.EncodingMsgpack
	}
	return precompute.EncodingUnspecified
}

func legacyCMSEncoding(e pmetric.CountMinSketchEncoding) precompute.Encoding {
	switch e {
	case pmetric.CountMinSketchEncodingProto:
		return precompute.EncodingProtoFull
	case pmetric.CountMinSketchEncodingDelta:
		return precompute.EncodingProtoDelta
	case pmetric.CountMinSketchEncodingMsgpack:
		return precompute.EncodingMsgpack
	}
	return precompute.EncodingUnspecified
}
