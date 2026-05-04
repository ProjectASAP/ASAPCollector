// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// File shim_helpers.go houses the OTel-side glue the shim needs but
// the host-neutral runtime cannot provide:
//
//   - patchEnvelopeMetadata: copy dp.Count + dp.AggregationTemporality
//     from input DDSketch dps onto the corresponding envelope-valued
//     observations (otel.Decode skips these typed fields because
//     they're not on the SketchEnvelope wire format).
//   - stampDPMetadata: write Count + Temporality back out from
//     envelopes onto the encoded data points (otel.Encode does not
//     propagate these typed fields), and clear the runtime's
//     synthetic scope name so mergeAppend can fold sketches into the
//     input's empty-scope SM.
//   - mergeAppend: fold the encoded output into md by appending
//     metrics into a matching existing (resource × scope) bucket so
//     the legacy "input + appended sketch metrics" shape is
//     preserved.
//   - appendSketchMetrics / appendQuantileMetrics: per-mode emission.

package ddsketchprocessor

import (
	"errors"
	"sort"

	envpb "github.com/ProjectASAP/sketchlib-go/proto/sketch_envelope"
	ddsketch "github.com/ProjectASAP/sketchlib-go/sketches/DDSketch"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	otelpre "github.com/ProjectASAP/asap-precompute-go/otel"
)

// decodeDDSketchEnvelope unwraps a SerializePortable envelope into a
// reconstructed *ddsketch.DDSketch. The shim's quantile-emission path
// uses this to query Quantile(q) on the inbound payload — the canonical
// wrapper in asap-precompute-go/sketches keeps its decoder unexported.
func decodeDDSketchEnvelope(b []byte) (*ddsketch.DDSketch, error) {
	var env envpb.SketchEnvelope
	if err := proto.Unmarshal(b, &env); err != nil {
		return nil, err
	}
	st := env.GetDdsketch()
	if st == nil {
		return nil, errors.New("envelope did not carry DDSketchState")
	}
	return ddsketch.NewFromState(st)
}

// observeInto walks md and feeds each (matched, decoded) observation
// into the per-metric Precompute, allocating fresh on first sight.
// otel.Decode preserves Encoded payload bytes but not Count or
// Temporality (those aren't on the SketchEnvelope wire format), so
// patchEnvelopeMetadata stamps them in afterwards from the original
// DDSketch data points — the legacy processor preserved them on
// output and existing tests expect them.
func (p *ddsketchProcessor) observeInto(md pmetric.Metrics, batch map[string]precompute.Precompute) error {
	obs, err := otelpre.Decode(md, &otelpre.AdapterConfig{})
	if err != nil {
		return err
	}
	patchEnvelopeMetadata(md, obs)
	for i := range obs {
		if !p.matchesLegacyMatchers(obs[i].Labels) {
			continue
		}
		pc := getOrCreate(batch, obs[i].Metric, p.cfg)
		if err := pc.Observe(&obs[i]); err != nil && p.logger != nil {
			p.logger.Debug("precompute observe", zap.Error(err))
		}
	}
	return nil
}

// flushToMetrics drains every Precompute in batch and produces a
// pmetric.Metrics carrying either DDSketch envelopes (when
// TransmitSketch=true) or Gauge-quantile metrics. Drops each entry
// from batch after flushing so batch-mode discards cleanly and
// window-mode reuses slots for the next window.
//
// Force-drain semantics: legacy flushWindow rotated regardless of
// wall-clock; Precompute.Drain is the runtime's dedicated entry
// point for that contract. Drain replaces the previous
// pseudo-timestamp Tick(1<<62-1) workaround.
func (p *ddsketchProcessor) flushToMetrics(batch map[string]precompute.Precompute) pmetric.Metrics {
	out := pmetric.NewMetrics()
	if len(batch) == 0 {
		return out
	}
	names := make([]string, 0, len(batch))
	for name := range batch {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		envs := batch[name].Drain()
		delete(batch, name)
		if len(envs) == 0 {
			continue
		}
		if p.cfg.TransmitSketch {
			p.appendSketchMetrics(out, envs, name)
		} else {
			p.appendQuantileMetrics(out, envs, name)
		}
	}
	return out
}

// patchEnvelopeMetadata stamps Count + Temporality from md's DDSketch
// data points onto obs's envelope-valued observations. Walks md and
// obs in lockstep matching otel.Decode's traversal.
func patchEnvelopeMetadata(md pmetric.Metrics, obs []precompute.Observation) {
	idx := 0
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				switch m.Type() {
				case pmetric.MetricTypeGauge:
					idx += m.Gauge().DataPoints().Len()
				case pmetric.MetricTypeSum:
					idx += m.Sum().DataPoints().Len()
				case pmetric.MetricTypeDDSketch:
					dps := m.DDSketch().DataPoints()
					temp := int32(m.DDSketch().AggregationTemporality())
					for n := 0; n < dps.Len(); n++ {
						if idx < len(obs) && obs[idx].Value.Envelope != nil {
							obs[idx].Value.Envelope.Count = dps.At(n).Count()
							obs[idx].Value.Envelope.AggregationTemporality = temp
						}
						idx++
					}
				}
			}
		}
	}
}

// stampDPMetadata walks encoded in encode-order (RM groupOrder by
// ResourceLabels, envelopes in slice order within each group) and
// copies Count + Temporality from envs onto each DDSketch data point.
// Also strips the runtime scope name so mergeAppend folds sketches
// into the input's empty-scope SM.
func stampDPMetadata(encoded pmetric.Metrics, envs []*precompute.SketchEnvelope) {
	idx := 0
	rms := encoded.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sms.At(j).Scope().SetName("")
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				if m.Type() != pmetric.MetricTypeDDSketch || idx >= len(envs) {
					continue
				}
				env := envs[idx]
				idx++
				dst := m.DDSketch()
				dst.SetAggregationTemporality(pmetric.AggregationTemporality(env.AggregationTemporality))
				if dps := dst.DataPoints(); dps.Len() > 0 {
					dps.At(0).SetCount(env.Count)
				}
			}
		}
	}
}

// appendSketchMetrics encodes envs as DDSketch-typed metrics and
// merges them into out, in place where possible.
func (p *ddsketchProcessor) appendSketchMetrics(out pmetric.Metrics, envs []*precompute.SketchEnvelope, inputName string) {
	for _, env := range envs {
		env.MetricName = inputName + p.cfg.MetricSuffix
	}
	encoded, err := otelpre.Encode(envs, &otelpre.AdapterConfig{})
	if err != nil {
		if p.logger != nil {
			p.logger.Error("encode sketch envelopes", zap.Error(err))
		}
		return
	}
	stampDPMetadata(encoded, envs)
	mergeAppend(out, encoded)
}

// appendQuantileMetrics decodes each envelope back to a DDSketch and
// emits one Gauge metric (per group of envelopes sharing a resource)
// with one data point per (series, quantile). Mirrors the legacy
// buildQuantileMetric path.
func (p *ddsketchProcessor) appendQuantileMetrics(out pmetric.Metrics, envs []*precompute.SketchEnvelope, inputName string) {
	if len(envs) == 0 {
		return
	}
	rm := out.ResourceMetrics().AppendEmpty()
	otelpre.KeyValuesToAttributes(envs[0].ResourceLabels, rm.Resource().Attributes())
	sm := rm.ScopeMetrics().AppendEmpty()
	metric := sm.Metrics().AppendEmpty()
	metric.SetName(inputName + p.cfg.MetricSuffix)
	dps := metric.SetEmptyGauge().DataPoints()
	for _, env := range envs {
		sk, err := decodeDDSketchEnvelope(env.Payload)
		if err != nil || sk == nil {
			continue
		}
		startTs := pcommon.Timestamp(env.WindowStartMs * 1_000_000)
		endTs := pcommon.Timestamp(env.WindowEndMs * 1_000_000)
		for _, q := range p.cfg.Quantiles {
			val, ok := sk.Quantile(q)
			if !ok {
				continue
			}
			dp := dps.AppendEmpty()
			otelpre.KeyValuesToAttributes(env.Labels, dp.Attributes())
			dp.Attributes().PutDouble("ddsketch.quantile", q)
			dp.SetStartTimestamp(startTs)
			dp.SetTimestamp(endTs)
			dp.SetDoubleValue(val)
		}
	}
	if dps.Len() == 0 {
		out.ResourceMetrics().RemoveIf(func(_ pmetric.ResourceMetrics) bool { return true })
	}
}

// mergeAppend folds src into dst, preferring to append metrics into
// an existing matching (resource × scope) bucket so the output keeps
// the legacy processor's in-place "input + appended sketch metrics"
// shape (same RM count, same SM count, just more metrics inside).
// Falls back to appending a fresh RM/SM when no match exists.
func mergeAppend(dst, src pmetric.Metrics) {
	srcRMs := src.ResourceMetrics()
	for i := 0; i < srcRMs.Len(); i++ {
		srcRM := srcRMs.At(i)
		dstRM, ok := findMatchingResource(dst, srcRM.Resource())
		if !ok {
			srcRM.CopyTo(dst.ResourceMetrics().AppendEmpty())
			continue
		}
		srcSMs := srcRM.ScopeMetrics()
		for j := 0; j < srcSMs.Len(); j++ {
			srcSM := srcSMs.At(j)
			dstSM, smOK := findMatchingScope(dstRM, srcSM.Scope())
			if !smOK {
				srcSM.CopyTo(dstRM.ScopeMetrics().AppendEmpty())
				continue
			}
			srcMetrics := srcSM.Metrics()
			for k := 0; k < srcMetrics.Len(); k++ {
				srcMetrics.At(k).CopyTo(dstSM.Metrics().AppendEmpty())
			}
		}
	}
}

func findMatchingResource(dst pmetric.Metrics, res pcommon.Resource) (pmetric.ResourceMetrics, bool) {
	want := res.Attributes()
	rms := dst.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		if attrsEqual(want, rms.At(i).Resource().Attributes()) {
			return rms.At(i), true
		}
	}
	return pmetric.ResourceMetrics{}, false
}

func findMatchingScope(rm pmetric.ResourceMetrics, scope pcommon.InstrumentationScope) (pmetric.ScopeMetrics, bool) {
	sms := rm.ScopeMetrics()
	for i := 0; i < sms.Len(); i++ {
		got := sms.At(i).Scope()
		if got.Name() == scope.Name() && got.Version() == scope.Version() {
			return sms.At(i), true
		}
	}
	return pmetric.ScopeMetrics{}, false
}

func attrsEqual(a, b pcommon.Map) bool {
	if a.Len() != b.Len() {
		return false
	}
	equal := true
	a.Range(func(k string, v pcommon.Value) bool {
		bv, ok := b.Get(k)
		if !ok || bv.AsString() != v.AsString() {
			equal = false
			return false
		}
		return true
	})
	return equal
}
