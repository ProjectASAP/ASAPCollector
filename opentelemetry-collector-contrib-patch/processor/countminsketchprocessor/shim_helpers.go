// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// File shim_helpers.go houses the OTel-side glue the shim needs but
// the host-neutral runtime cannot provide:
//
//   - observeAll: walk md, route observations through the per-metric
//     Precompute, hashing the encoded data-point attribute set into
//     a KindBytes value the sketches.CMSObserver can consume.
//
//   - encodeEnvelopes: materialize emitted envelopes into the legacy
//     pmetric output shape (typed CountMinSketchDataPoint with
//     SampleCount / Rows / Cols / Sketch / Encoding fields, or a
//     Gauge fallback when TransmitSketch=false).
//
//   - The runtime's otel.Encode supports CountMinSketch envelopes but
//     stamps neither SampleCount nor Rows / Cols (those aren't on
//     the SketchEnvelope wire format), and always defaults to
//     EncodingProto. We own the encode here so legacy attribute
//     parity holds and msgpack / delta encodings flow through.

package countminsketchprocessor

import (
	"sort"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	otelpre "github.com/ProjectASAP/asap-precompute-go/otel"
)

// observeAll walks md and feeds each observation through its
// per-metric Precompute, lazily allocating one on first sight.
//
// For Gauge / Sum data points the legacy CMS processor hashes the
// encoded data-point attribute set (NOT the numeric value) — the
// sketch counts series cardinality, not samples-by-value. We
// substitute the OTel adapter's KindFloat observation with a
// KindBytes one carrying the encoded-attrs bytes so sketches.CMSObserver
// can hash it identically. Envelope-valued observations (typed
// CountMinSketch input) flow through unmodified — the runtime routes
// them through ObserveEnvelope and merges the inbound state.
func (p *cmsProcessor) observeAll(md pmetric.Metrics) error {
	obs, err := otelpre.Decode(md, &otelpre.AdapterConfig{})
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range obs {
		if obs[i].Value.Kind != precompute.KindEnvelope {
			// Replace the float observation with a hashable bytes
			// observation matching the legacy emit's hash input
			// (encodeAttributesAsKey on the data-point attrs).
			obs[i].Value = precompute.BytesValue(
				[]byte(precompute.AttributesKey(obs[i].Labels, nil)),
			)
		}
		if err := p.precomputeForLocked(obs[i].Metric).Observe(&obs[i]); err != nil && p.logger != nil {
			p.logger.Debug("countminsketchprocessor: observe", zap.Error(err))
		}
	}
	return nil
}

// drainAndEncode rotates every per-metric window unconditionally
// and synthesizes one pmetric.Metrics carrying the emitted typed
// CMS metrics (or Gauge fallbacks). Used by both the batch path
// (per ConsumeMetrics) and the window-mode tick goroutine (per
// FlushWindow). Drain is the right primitive — legacy
// emitWindowAndReset rotated regardless of wall-clock, the ticker
// fires once per WindowDuration so every fire wants to flush, and
// the shutdown branches in the goroutine need to capture mid-window
// state that Tick(time.Now()) would silently drop.
func (p *cmsProcessor) drainAndEncode() pmetric.Metrics {
	out := pmetric.NewMetrics()
	p.mu.Lock()
	pcs := make(map[string]precompute.Precompute, len(p.pcByName))
	for n, pp := range p.pcByName {
		pcs[n] = pp
	}
	p.mu.Unlock()
	if len(pcs) == 0 {
		return out
	}
	// Sort by metric name for stable output ordering across runs.
	names := make([]string, 0, len(pcs))
	for name := range pcs {
		names = append(names, name)
	}
	sort.Strings(names)

	var sm pmetric.ScopeMetrics
	var smInit bool
	for _, name := range names {
		envs := pcs[name].Drain()
		if len(envs) == 0 {
			continue
		}
		if !smInit {
			sm = out.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
			sm.Scope().SetName("otelcol/windowed-countmin")
			smInit = true
		}
		p.encodeEnvelopes(sm.Metrics(), envs)
	}
	return out
}

// encodeEnvelopes writes envs into metrics. TransmitSketch=true emits
// a typed CountMinSketchDataPoint (the legacy emit-side shape that
// the backend's modified-OTLP CMS decoder expects); TransmitSketch=
// false emits a Gauge with sample_count attribute (the legacy
// "monitoring-only" fallback for dashboards reading countmin as a
// scalar series).
//
// Multiple envelopes sharing the same MetricName collapse into one
// Metric (CMS uses a single configured output name regardless of
// input metric, per the legacy `cfg.MetricName`).
func (p *cmsProcessor) encodeEnvelopes(metrics pmetric.MetricSlice, envs []*precompute.SketchEnvelope) {
	if p.cfg.TransmitSketch {
		p.encodeTypedSketch(metrics, envs)
		return
	}
	p.encodeGauge(metrics, envs)
}

// encodeTypedSketch emits one CountMinSketch metric per distinct
// MetricName (legacy behavior) and stamps SampleCount / Rows / Cols /
// Sketch / Encoding onto each typed data point. The Encoding enum
// translation honors the wrapper's wire format choice (msgpack vs
// proto) and the runtime's full-vs-delta decision.
func (p *cmsProcessor) encodeTypedSketch(metrics pmetric.MetricSlice, envs []*precompute.SketchEnvelope) {
	byName := make(map[string]pmetric.Metric)
	for _, env := range envs {
		if env == nil || len(env.Payload) == 0 {
			continue
		}
		name := env.MetricName
		if name == "" {
			name = p.cfg.MetricName
		}
		m, ok := byName[name]
		if !ok {
			m = metrics.AppendEmpty()
			m.SetName(name)
			m.SetUnit("1")
			parent := m.SetEmptyCountMinSketch()
			parent.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
			// Refactor-2026-05: rows/cols are sketch-instance config —
			// constant across all DPs in this Metric — so they lift to
			// the parent CountMinSketch container, sent ONCE per emit
			// instead of duplicated per DataPoint.
			parent.SetRows(int32(p.cfg.Rows))
			parent.SetCols(int32(p.cfg.Columns))
			byName[name] = m
		}
		dp := m.CountMinSketch().DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.Timestamp(env.WindowEndMs * 1_000_000))
		labelsToAttrs(env.Labels, dp.Attributes())
		// Refactor-2026-05: per-DP sample_count is removed; it is
		// derivable from the sketch row sums at the receiver.
		dp.SetSketch(env.Payload)
		dp.SetEncoding(cmsEncodingFor(env.Encoding, p.cfg.Encoding))
	}
}

// encodeGauge emits a Gauge fallback the legacy processor produces
// when TransmitSketch=false. The DP attributes carry rows, cols, and
// sample_count for monitoring dashboards; the value is the
// sample_count as a double (preserved verbatim from the legacy emit).
func (p *cmsProcessor) encodeGauge(metrics pmetric.MetricSlice, envs []*precompute.SketchEnvelope) {
	byName := make(map[string]pmetric.Metric)
	for _, env := range envs {
		if env == nil {
			continue
		}
		name := env.MetricName
		if name == "" {
			name = p.cfg.MetricName
		}
		m, ok := byName[name]
		if !ok {
			m = metrics.AppendEmpty()
			m.SetName(name)
			m.SetUnit("1")
			m.SetEmptyGauge()
			byName[name] = m
		}
		dp := m.Gauge().DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.Timestamp(env.WindowEndMs * 1_000_000))
		labelsToAttrs(env.Labels, dp.Attributes())
		dp.Attributes().PutInt("rows", int64(p.cfg.Rows))
		dp.Attributes().PutInt("cols", int64(p.cfg.Columns))
		dp.Attributes().PutInt("sample_count", int64(env.Count))
		dp.SetDoubleValue(float64(env.Count))
	}
}

// cmsEncodingFor maps the runtime's host-neutral Encoding plus the
// shim's configured wire-format pick to the OTel-typed enum the
// backend's modified-OTLP decoder dispatches on. The runtime emits
// EncodingProtoDelta when the wrapper's ComputeDeltaAgainst returns
// a delta; EncodingProtoFull otherwise. Msgpack is selected via the
// shim's Config.Encoding knob (it never appears as an envelope
// Encoding because the runtime doesn't model msgpack-delta — the
// legacy processor falls back to proto-delta when delta+msgpack is
// configured).
func cmsEncodingFor(env precompute.Encoding, cfgEnc SketchEncoding) pmetric.CountMinSketchEncoding {
	switch env {
	case precompute.EncodingProtoDelta:
		return pmetric.CountMinSketchEncodingDelta
	case precompute.EncodingMsgpack:
		return pmetric.CountMinSketchEncodingMsgpack
	}
	if cfgEnc == EncodingMsgpack {
		return pmetric.CountMinSketchEncodingMsgpack
	}
	return pmetric.CountMinSketchEncodingProto
}

// labelsToAttrs copies host-neutral KeyValues into a pcommon.Map.
func labelsToAttrs(kvs []precompute.KeyValue, dst pcommon.Map) {
	for _, kv := range kvs {
		dst.PutStr(kv.Key, kv.Value)
	}
}
