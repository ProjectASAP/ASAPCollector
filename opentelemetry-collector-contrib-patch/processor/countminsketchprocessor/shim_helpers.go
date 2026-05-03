// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// File shim_helpers.go houses the OTel-side glue the shim needs but
// the host-neutral runtime cannot provide:
//
//   - observeAll: walk md, route observations through the per-metric
//     Precompute, hashing the encoded data-point attribute set into
//     a KindBytes value the cmsSketchObserver can consume.
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

	cms "github.com/ProjectASAP/sketchlib-go/sketches/CountMinSketch"
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
// KindBytes one carrying the encoded-attrs bytes so cmsSketchObserver
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

// tickAndEncode rotates every per-metric window and synthesizes one
// pmetric.Metrics carrying the emitted typed CMS metrics (or Gauge
// fallbacks). Used by both the batch path (per ConsumeMetrics) and
// the window-mode tick goroutine (per FlushWindow).
//
// flushAll=true forces every Precompute to drain regardless of
// wall-clock; passing a far-future timestamp in nowMs is the
// canonical way to do so. Batch and window paths both pass the
// equivalent.
func (p *cmsProcessor) tickAndEncode(nowMs uint64) pmetric.Metrics {
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
		envs := pcs[name].Tick(nowMs)
		if len(envs) == 0 {
			continue
		}
		// Post-process delta transmission: rewrite envelope payloads
		// from full proto to sparse delta where Config asks for it,
		// using the shim-owned per-series prev cache.
		p.applyDeltaTransmission(envs)
		if !smInit {
			sm = out.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
			sm.Scope().SetName("otelcol/windowed-countmin")
			smInit = true
		}
		p.encodeEnvelopes(sm.Metrics(), envs)
	}
	return out
}

// applyDeltaTransmission rewrites envelope payloads to sparse deltas
// when Config.DeltaTransmission=true, using a per-series snapshot
// cache held at the shim layer. Mirrors the legacy CMS processor's
// "always refresh snapshot after every emit" semantics:
//
//   - On the first emit per series key the prev cache is empty;
//     the envelope payload stays as a full proto state and the
//     wire encoding stays at PROTO_FULL. The current full state is
//     stored as the new prev.
//   - On subsequent emits the prev is decoded, a delta is computed
//     against the current state, and the envelope payload is
//     replaced with the proto-encoded delta. The wire encoding is
//     flipped to PROTO_DELTA. The prev is then refreshed to the
//     current full state for the next window.
//
// MUST run after the runtime's Tick (so envs hold the runtime's
// freshly-emitted full snapshots) and before the encode path (so
// encodeEnvelopes sees the rewritten payloads). Msgpack-encoded
// envelopes are skipped — sketchlib-go has no msgpack-delta path,
// matching the legacy fallback to proto-full when
// encoding=msgpack + delta=true.
func (p *cmsProcessor) applyDeltaTransmission(envs []*precompute.SketchEnvelope) {
	if !p.cfg.DeltaTransmission {
		return
	}
	threshold := p.cfg.DeltaThreshold
	if threshold <= 0 {
		threshold = 1.0
	}
	for _, env := range envs {
		if env == nil || len(env.Payload) == 0 || env.Encoding == precompute.EncodingMsgpack {
			continue
		}
		key := p.deltaKey(env)
		current, err := cms.DeserializeCountMinSketchFromProtoBytes(env.Payload)
		if err != nil {
			// Defensive: bad runtime payload — leave as full.
			continue
		}
		// Always refresh the prev to current full state at end of
		// loop iteration; matches the legacy processor's
		// snapshot-update-after-every-emit invariant. Capture the
		// full state once up front so encode/serialize errors below
		// don't desync the cache.
		fullBytes, fErr := current.SerializeProtoBytesFO()
		if fErr != nil {
			continue
		}
		p.snapshotsMu.Lock()
		prevBytes, hasPrev := p.snapshots[key]
		p.snapshots[key] = fullBytes
		p.snapshotsMu.Unlock()
		if !hasPrev {
			// First emit for this key — leave as PROTO_FULL and
			// seed the cache (already done above).
			continue
		}
		prevSk, err := cms.DeserializeCountMinSketchFromProtoBytes(prevBytes)
		if err != nil {
			continue
		}
		delta, err := cms.ComputeDelta(prevSk, current, threshold)
		if err != nil {
			continue
		}
		payload, err := cms.SerializeDelta(delta)
		if err != nil {
			continue
		}
		env.Payload = payload
		env.Encoding = precompute.EncodingProtoDelta
	}
}

// deltaKey builds the per-series key used to index the shim's prev
// snapshot cache. Composing MetricName + Labels matches the legacy
// processor's `metricName + "::" + encodeKey(dpAttrs)` aggregation
// key (which never included resource attrs — the shim sets
// OmitResourceAttrs=true so envelopes carry empty ResourceLabels).
func (p *cmsProcessor) deltaKey(env *precompute.SketchEnvelope) string {
	return env.MetricName + "::" + precompute.AttributesKey(env.Labels, nil)
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
			m.SetEmptyCountMinSketch().SetAggregationTemporality(
				pmetric.AggregationTemporalityDelta)
			byName[name] = m
		}
		dp := m.CountMinSketch().DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.Timestamp(env.WindowEndMs * 1_000_000))
		labelsToAttrs(env.Labels, dp.Attributes())
		dp.SetSampleCount(env.Count)
		dp.SetRows(int32(p.cfg.Rows))
		dp.SetCols(int32(p.cfg.Columns))
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
