// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ddsketchprocessor

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	ddsketch "github.com/ProjectASAP/sketchlib-go/sketches/DDSketch"
	ddpb "github.com/ProjectASAP/sketchlib-go/proto/ddsketch"
	envpb "github.com/ProjectASAP/sketchlib-go/proto/sketch_envelope"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor/selfmonitor"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type ddsketchProcessor struct {
	cfg          *Config
	logger       *zap.Logger
	nextConsumer consumer.Metrics
	monitor      *selfmonitor.Monitor

	// window mode state
	mu            sync.Mutex
	windowStore   map[string]*resourceWindow // keyed by resource attributes
	stopCh        chan struct{}
	doneCh        chan struct{}
	windowStarted atomic.Bool // true once the window goroutine is running

	// snapshots maps "<metricName>::<attrKey>" → proto-serialized snapshot,
	// updated each flush. Used to compute delta payloads when
	// cfg.DeltaTransmission=true.
	snapshotsMu sync.Mutex
	snapshots   map[string][]byte // proto-marshalled sketchpb.DDSketch

	// inboundSnapshots tracks the last full proto payload per series key received
	// from upstream. Used to reconstruct the current sketch when upstream sends
	// DDSketchEncodingProtoDelta payloads.
	inboundMu        sync.Mutex
	inboundSnapshots map[string][]byte // proto-marshalled sketchpb.DDSketch
}

type resourceWindow struct {
	resource pcommon.Resource
	scopes   map[string]*scopeWindow // key: scope name + ":" + version
}

type scopeWindow struct {
	scope   pcommon.InstrumentationScope
	metrics map[string]*metricWindow // metric name -> window
}

type metricWindow struct {
	name        string
	description string
	unit        string
	series      map[string]*sketchSeries // attr key -> aggregated series
	// temporality captures AggregationTemporality from DDSketch inputs so that
	// flushWindow can preserve it on emitted DDSketch metrics.
	temporality pmetric.AggregationTemporality
}

func newProcessor(cfg *Config, logger *zap.Logger, next consumer.Metrics) *ddsketchProcessor {
	return &ddsketchProcessor{
		cfg:              cfg,
		logger:           logger,
		nextConsumer:     next,
		windowStore:      make(map[string]*resourceWindow),
		snapshots:        make(map[string][]byte),
		inboundSnapshots: make(map[string][]byte),
		stopCh:           make(chan struct{}),
		doneCh:           make(chan struct{}),
	}
}

// Capabilities implements processor.Metrics.
func (p *ddsketchProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

// Start implements processor.Metrics.
func (p *ddsketchProcessor) Start(ctx context.Context, _ component.Host) error {
	if p.cfg.Mode != ModeWindow {
		return nil
	}

	ticker := time.NewTicker(p.cfg.WindowDuration)

	go func() {
		p.windowStarted.Store(true) // set only after goroutine is running so Shutdown never waits on doneCh before it is closed
		defer func() {
			ticker.Stop()
			close(p.doneCh)
		}()

		for {
			select {
			case <-ctx.Done():
				// Best-effort final flush on context cancellation.
				_ = p.flushWindow(context.Background())
				return
			case <-p.stopCh:
				// Final flush before shutdown.
				_ = p.flushWindow(context.Background())
				return
			case <-ticker.C:
				// Periodic flush.
				_ = p.flushWindow(context.Background())
			}
		}
	}()

	return nil
}

// Shutdown implements processor.Metrics.
func (p *ddsketchProcessor) Shutdown(ctx context.Context) error {
	defer p.shutdownMonitor()

	// Only wait if the window goroutine was actually started; avoids blocking
	// forever when Start was never called.
	if p.cfg.Mode != ModeWindow || !p.windowStarted.Load() {
		return nil
	}

	// Signal the goroutine and wait for it to finish (or context cancellation).
	close(p.stopCh)

	select {
	case <-p.doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ConsumeMetrics implements processor.Metrics.
func (p *ddsketchProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	p.recordInput(ctx, md)

	switch p.cfg.Mode {
	case ModeBatch:
		if _, err := p.processBatch(ctx, md); err != nil {
			return err
		}
		p.recordOutput(ctx, md)
		return p.nextConsumer.ConsumeMetrics(ctx, md)
	case ModeWindow:
		p.accumulateIntoWindow(md)
		// Forward inputs unchanged so this processor can chain with
		// other windowed sketch processors in a single pipeline
		// (`processors: [ddsketch, hll, batch]`). Without this, the
		// next processor never sees the raw inputs — it only sees
		// this processor's tick-emitted typed sketches, which it
		// treats as foreign types and drops. Matches what
		// countminsketchprocessor / countsketchprocessor already do
		// in their ModeWindow paths. Add a `filter` processor at the
		// end of the pipeline if you want to drop the raw inputs.
		return p.nextConsumer.ConsumeMetrics(ctx, md)
	default:
		// Should not happen due to config validation, but be defensive.
		if p.logger != nil {
			p.logger.Error("ddsketchprocessor: unknown mode, dropping metrics", zap.Any("mode", p.cfg.Mode))
		}
		return nil
	}
}

// processBatch applies the existing per-batch aggregation logic (batch mode).
func (p *ddsketchProcessor) processBatch(_ context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			p.processScopeMetrics(sms.At(j))
		}
	}
	return md, nil
}

func (p *ddsketchProcessor) processScopeMetrics(sm pmetric.ScopeMetrics) {
	metrics := sm.Metrics()
	originalLen := metrics.Len()
	for i := 0; i < originalLen; i++ {
		metric := metrics.At(i)
		if newMetric, ok := p.buildMetric(metric); ok {
			newMetric.CopyTo(metrics.AppendEmpty())
		}
	}
}

type sketchSeries struct {
	attrs  pcommon.Map
	sketch *ddsketch.DDSketch
	count  uint64
	start  pcommon.Timestamp
	end    pcommon.Timestamp
	flags  pmetric.DataPointFlags
}

func (p *ddsketchProcessor) buildMetric(src pmetric.Metric) (pmetric.Metric, bool) {
	var series map[string]*sketchSeries
	switch src.Type() {
	case pmetric.MetricTypeDDSketch:
		series = p.consumeDDSketchDataPoints(src.DDSketch().DataPoints())
	case pmetric.MetricTypeGauge:
		series = p.consumeGaugeDataPoints(src.Gauge().DataPoints())
	default:
		return pmetric.Metric{}, false
	}
	if len(series) == 0 {
		return pmetric.Metric{}, false
	}

	if p.cfg.TransmitSketch {
		return p.buildMergedSketchMetric(src, series)
	}
	return p.buildQuantileMetric(src, series)
}

func (p *ddsketchProcessor) buildMergedSketchMetric(src pmetric.Metric, series map[string]*sketchSeries) (pmetric.Metric, bool) {
	out := pmetric.NewMetric()
	out.SetName(src.Name() + p.cfg.MetricSuffix)
	out.SetDescription("DDSketch summary for " + src.Name())
	out.SetUnit(src.Unit())

	dst := out.SetEmptyDDSketch()
	if src.Type() == pmetric.MetricTypeDDSketch {
		dst.SetAggregationTemporality(src.DDSketch().AggregationTemporality())
	} else {
		dst.SetAggregationTemporality(pmetric.AggregationTemporalityUnspecified)
	}

	dps := dst.DataPoints()
	for attrKey, s := range series {
		if s.sketch == nil {
			continue
		}

		var payload []byte
		var err error
		var encoding string

		if p.cfg.DeltaTransmission {
			snapKey := src.Name() + "::" + attrKey
			p.snapshotsMu.Lock()
			snapPayload, hasSnap := p.snapshots[snapKey]
			p.snapshotsMu.Unlock()

			if hasSnap {
				payload, err = computeDDSketchDelta(snapPayload, s.sketch, p.cfg.DeltaThreshold)
				encoding = "proto_delta"
			} else {
				payload, err = serializeDDSketch(s.sketch)
				encoding = "proto_full"
			}

			// Update snapshot.
			newSnap, snapErr := serializeDDSketch(s.sketch)
			if snapErr == nil {
				p.snapshotsMu.Lock()
				p.snapshots[snapKey] = newSnap
				p.snapshotsMu.Unlock()
			}
		} else {
			payload, err = serializeDDSketch(s.sketch)
			encoding = "proto_full"
		}

		if err != nil {
			if p.logger != nil {
				p.logger.Error("failed to serialize DDSketch", zap.Error(err))
			}
			continue
		}

		dp := dps.AppendEmpty()
		s.attrs.CopyTo(dp.Attributes())
		dp.SetStartTimestamp(s.start)
		dp.SetTimestamp(s.end)
		dp.SetCount(s.count)
		// Set the TYPED encoding to match the actual payload — the
		// backend's modified-OTLP ingest dispatches on this enum, not
		// on the `ddsketch.encoding` string attribute. With
		// `delta_transmission: true` the second window's payload is
		// a `DDSketchDelta` proto, not a `DDSketchState`, and the
		// backend's `decode_modified_otlp_sketch_bytes` (PROTO path)
		// would silently fail to parse it as a state envelope. The
		// PROTO_DELTA branch hands the bytes to
		// `apply_modified_otlp_delta_bytes` which knows how to merge
		// against the per-series snapshot.
		switch encoding {
		case "proto_delta":
			dp.SetEncoding(pmetric.DDSketchEncodingProtoDelta)
		default:
			// "proto_full" and any unexpected fallback.
			dp.SetEncoding(pmetric.DDSketchEncodingProto)
		}
		dp.SetSketch(payload)
		dp.SetFlags(s.flags)
		// NB: do NOT add a `ddsketch.encoding` string attribute here.
		// The typed `Encoding()` field above is the source of truth.
		// Adding the encoding as an attribute makes consecutive
		// frames (proto_full → proto_delta → proto_delta) carry
		// different attribute sets and therefore different
		// `series_key`s on the backend, which keys its per-series
		// snapshot cache by attribute set. Result: the delta frame
		// looks up the cache with `…,ddsketch.encoding=proto_delta`
		// and never finds the snapshot stored under
		// `…,ddsketch.encoding=proto_full`, so every delta drops as
		// "delta-sketch arrived before any base snapshot".
	}

	if dps.Len() == 0 {
		return pmetric.Metric{}, false
	}
	return out, true
}

func (p *ddsketchProcessor) buildQuantileMetric(src pmetric.Metric, series map[string]*sketchSeries) (pmetric.Metric, bool) {
	out := pmetric.NewMetric()
	out.SetName(src.Name() + p.cfg.MetricSuffix)
	out.SetDescription("DDSketch quantiles for " + src.Name())
	out.SetUnit(src.Unit())

	dst := out.SetEmptyGauge()
	dps := dst.DataPoints()
	for _, s := range series {
		if s.sketch == nil {
			continue
		}
		for _, q := range p.cfg.Quantiles {
			val, ok := s.sketch.Quantile(q)
			if !ok {
				// sketchlib-go returns (0, false) for empty sketch
				// or an out-of-range quantile; skip silently to
				// avoid log spam when a window contains no
				// observations.
				continue
			}
			dp := dps.AppendEmpty()
			s.attrs.CopyTo(dp.Attributes())
			dp.Attributes().PutDouble("ddsketch.quantile", q)
			dp.SetStartTimestamp(s.start)
			dp.SetTimestamp(s.end)
			dp.SetDoubleValue(val)
			dp.SetFlags(s.flags)
		}
	}
	if dps.Len() == 0 {
		return pmetric.Metric{}, false
	}
	return out, true
}

func (p *ddsketchProcessor) consumeDDSketchDataPoints(dps pmetric.DDSketchDataPointSlice) map[string]*sketchSeries {
	if dps.Len() == 0 {
		return nil
	}
	result := make(map[string]*sketchSeries)
	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		if !p.matchesMatchers(dp.Attributes()) {
			continue
		}
		attrKey := attributesKey(dp.Attributes())
		sk, err := p.decodeDDSketchDataPoint(attrKey, dp)
		if err != nil {
			if p.logger != nil {
				p.logger.Error("failed to decode DDSketch payload", zap.Error(err))
			}
			continue
		}
		if sk == nil {
			continue // delta with no snapshot yet
		}

		key := p.seriesKey(dp.Attributes())
		series := result[key]
		if series == nil {
			series = p.newSeriesFrom(dp.Attributes(), dp.StartTimestamp(), dp.Timestamp())
			result[key] = series
		} else {
			series.updateWindow(dp.StartTimestamp(), dp.Timestamp())
		}
		series.merge(sk, dp, p.logger)
	}
	return result
}

func (p *ddsketchProcessor) consumeGaugeDataPoints(dps pmetric.NumberDataPointSlice) map[string]*sketchSeries {
	if dps.Len() == 0 {
		return nil
	}
	result := make(map[string]*sketchSeries)
	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		if !p.matchesMatchers(dp.Attributes()) {
			continue
		}
		key := p.seriesKey(dp.Attributes())
		series := result[key]
		if series == nil {
			series = p.newSeriesFrom(dp.Attributes(), dp.StartTimestamp(), dp.Timestamp())
			result[key] = series
		} else {
			series.updateWindow(dp.StartTimestamp(), dp.Timestamp())
		}
		sk, err := p.ensureSketch(series)
		if err != nil {
			if p.logger != nil {
				p.logger.Error("failed to create DDSketch for gauge", zap.Error(err))
			}
			continue
		}
		switch dp.ValueType() {
		case pmetric.NumberDataPointValueTypeDouble:
			sk.Update(dp.DoubleValue())
		case pmetric.NumberDataPointValueTypeInt:
			sk.Update(float64(dp.IntValue()))
		default:
			if p.logger != nil {
				p.logger.Error("unsupported gauge data point type", zap.Any("type", dp.ValueType()))
			}
			continue
		}
		series.count++
		series.flags |= dp.Flags()
	}
	return result
}

// decodeDDSketchDataPoint deserializes an inbound DDSketch data
// point's `sketch` bytes back to a `*ddsketch.DDSketch` we can
// merge into the local window. The wire format is the canonical
// sketchlib-go `SketchEnvelope{DDSketchState}`, byte-compatible
// with the backend's Rust decoder.
//
// Delta encoding (`DDSketchEncodingProtoDelta`) is not yet
// supported on this side — sketchlib-go's `DDSketchDelta` is
// only generated in the Rust backend's `asap_otel_proto` crate.
// Until the Go-side delta encoder lands (mirroring the Rust one
// in `asap-query-engine/src/.../dd_sketch_accumulator.rs`),
// callers should use `delta_transmission: false`. With delta
// off, every payload carries the full sketch state.
func (p *ddsketchProcessor) decodeDDSketchDataPoint(seriesKey string, dp pmetric.DDSketchDataPoint) (*ddsketch.DDSketch, error) {
	data := dp.Sketch()
	if len(data) == 0 {
		return nil, fmt.Errorf("empty DDSketch payload")
	}

	switch dp.Encoding() {
	case pmetric.DDSketchEncodingProtoDelta:
		return nil, fmt.Errorf("DDSketchEncodingProtoDelta inbound decode not implemented for sketchlib-go wire format yet — agent-side delta requires the same generator the backend uses (`asap_otel_proto::sketchlib::v1::DdSketchDelta`); set `delta_transmission: false` on the upstream emitter")

	default: // DDSketchEncodingProto or unspecified
		if dp.Encoding() != pmetric.DDSketchEncodingProto && dp.Encoding() != pmetric.DDSketchEncodingUnspecified {
			return nil, fmt.Errorf("unsupported DDSketch encoding %v", dp.Encoding())
		}
		var env envpb.SketchEnvelope
		if err := proto.Unmarshal(data, &env); err != nil {
			// Fall back to bare DDSketchState for senders that
			// skip the envelope wrapper (e.g. unit tests).
			var bareState ddpb.DDSketchState
			if err2 := proto.Unmarshal(data, &bareState); err2 != nil {
				return nil, fmt.Errorf("unmarshal DDSketch envelope: %w (bare-state fallback also failed: %v)", err, err2)
			}
			sk, err := ddsketch.NewFromState(&bareState)
			if err != nil {
				return nil, fmt.Errorf("NewFromState (bare): %w", err)
			}
			p.cacheInboundSnapshot(seriesKey, data)
			return sk, nil
		}
		// SketchEnvelope's `sketch_state` is a oneof; the generated
		// `GetDdsketch()` accessor returns nil if any other variant
		// was sent (defensive — the upstream is supposed to emit a
		// DDSketch state since this is a DDSketchDataPoint).
		ddState := env.GetDdsketch()
		if ddState == nil {
			return nil, fmt.Errorf("DDSketch SketchEnvelope did not carry a DDSketchState variant")
		}
		sk, err := ddsketch.NewFromState(ddState)
		if err != nil {
			return nil, fmt.Errorf("NewFromState (envelope): %w", err)
		}
		p.cacheInboundSnapshot(seriesKey, data)
		return sk, nil
	}
}

func (p *ddsketchProcessor) cacheInboundSnapshot(seriesKey string, data []byte) {
	p.inboundMu.Lock()
	if p.inboundSnapshots == nil {
		p.inboundSnapshots = make(map[string][]byte)
	}
	// Store a defensive copy — `data` aliases pmetric storage that
	// can be mutated when the next pdata batch is reused.
	cp := make([]byte, len(data))
	copy(cp, data)
	p.inboundSnapshots[seriesKey] = cp
	p.inboundMu.Unlock()
}

func newSketchSeries(attrs pcommon.Map, start, ts pcommon.Timestamp) *sketchSeries {
	attrCopy := pcommon.NewMap()
	attrs.CopyTo(attrCopy)
	return &sketchSeries{
		attrs: attrCopy,
		start: start,
		end:   ts,
	}
}

func (s *sketchSeries) updateWindow(start, ts pcommon.Timestamp) {
	if s.start == 0 || (start != 0 && start < s.start) {
		s.start = start
	}
	if ts > s.end {
		s.end = ts
	}
}

func (s *sketchSeries) merge(sk *ddsketch.DDSketch, dp pmetric.DDSketchDataPoint, logger *zap.Logger) {
	if sk == nil {
		return
	}
	if s.sketch == nil {
		s.sketch = sk
	} else if err := s.sketch.Merge(sk); err != nil {
		if logger != nil {
			logger.Error("failed to merge DDSketch", zap.Error(err))
		}
		return
	}

	s.count += dp.Count()
	s.flags |= dp.Flags()
	s.updateWindow(dp.StartTimestamp(), dp.Timestamp())
}

func (p *ddsketchProcessor) ensureSketch(s *sketchSeries) (*ddsketch.DDSketch, error) {
	if s.sketch != nil {
		return s.sketch, nil
	}
	// sketchlib-go's NewDDSketch panics on alpha out of (0,1); guard
	// here so we surface a clean error instead.
	if !(p.cfg.RelativeAccuracy > 0 && p.cfg.RelativeAccuracy < 1) {
		return nil, fmt.Errorf("ddsketch relative_accuracy %v out of range (0,1)", p.cfg.RelativeAccuracy)
	}
	s.sketch = ddsketch.NewDDSketch(p.cfg.RelativeAccuracy)
	return s.sketch, nil
}

// serializeDDSketch produces the canonical wire format the backend's
// `DDSketchAccumulator::from_sketchlib_proto_bytes` expects: a
// proto-marshalled `SketchEnvelope` carrying a `DDSketchState`. This
// is byte-compatible with the sketch decoded by
// `asap_sketchlib::proto::sketchlib::SketchEnvelope`.
func serializeDDSketch(sk *ddsketch.DDSketch) ([]byte, error) {
	if sk == nil {
		return nil, nil
	}
	env, err := sk.SerializePortable()
	if err != nil {
		return nil, fmt.Errorf("SerializePortable: %w", err)
	}
	return proto.Marshal(env)
}

// computeDDSketchDelta computes a sparse delta between the previous
// flush's snapshot payload and the current sketch. The result is
// `proto.Marshal(DDSketchDelta)` from `sketches/DDSketch/delta.go`,
// byte-for-byte compatible with the backend's
// `apply_modified_otlp_delta_bytes` → `DDSketchAccumulator.apply_proto_delta_bytes`
// path (Rust's `asap_otel_proto::sketchlib::v1::DdSketchDelta`).
//
// `snapPayload` is the previously emitted full-state envelope
// (output of `serializeDDSketch`). We deserialize it into a
// `*DDSketch` so sketchlib-go's `ComputeDelta` can iterate both
// stores' bucket counts.
func computeDDSketchDelta(snapPayload []byte, current *ddsketch.DDSketch, threshold uint64) ([]byte, error) {
	var env envpb.SketchEnvelope
	if err := proto.Unmarshal(snapPayload, &env); err != nil {
		return nil, fmt.Errorf("computeDDSketchDelta: unmarshal snapshot envelope: %w", err)
	}
	snapState := env.GetDdsketch()
	if snapState == nil {
		return nil, fmt.Errorf("computeDDSketchDelta: snapshot envelope did not carry a DDSketchState variant")
	}
	snapshot, err := ddsketch.NewFromState(snapState)
	if err != nil {
		return nil, fmt.Errorf("computeDDSketchDelta: NewFromState(snapshot): %w", err)
	}
	return ddsketch.ComputeDelta(snapshot, current, threshold)
}

func attributesKey(attrs pcommon.Map) string {
	keys := make([]string, 0, attrs.Len())
	attrs.Range(func(k string, _ pcommon.Value) bool {
		keys = append(keys, k)
		return true
	})
	sort.Strings(keys)
	b := strings.Builder{}
	for _, k := range keys {
		v, ok := attrs.Get(k)
		if !ok {
			continue
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(v.AsString())
		b.WriteByte(';')
	}
	return b.String()
}

// matchesMatchers returns true if attrs satisfies all configured LabelMatchers.
func (p *ddsketchProcessor) matchesMatchers(attrs pcommon.Map) bool {
	for _, m := range p.cfg.LabelMatchers {
		v, ok := attrs.Get(m.Key)
		if !ok || v.AsString() != m.Value {
			return false
		}
	}
	return true
}

// seriesKey returns the map key used to locate a series in the window/batch store.
// When AggregateBy is configured, only those label values form the key (cross-series
// aggregation). Otherwise the full attribute set is used (per-series, default).
func (p *ddsketchProcessor) seriesKey(attrs pcommon.Map) string {
	if len(p.cfg.AggregateBy) == 0 {
		return attributesKey(attrs)
	}
	b := strings.Builder{}
	for _, k := range p.cfg.AggregateBy { // already sorted by validate
		v, ok := attrs.Get(k)
		if !ok {
			continue
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(v.AsString())
		b.WriteByte(';')
	}
	return b.String()
}

// seriesAttrs builds the attribute map to store on a new series entry.
// When AggregateBy is configured, only those labels are included in the output.
// Otherwise a full copy of attrs is returned.
func (p *ddsketchProcessor) seriesAttrs(attrs pcommon.Map) pcommon.Map {
	out := pcommon.NewMap()
	if len(p.cfg.AggregateBy) == 0 {
		attrs.CopyTo(out)
		return out
	}
	for _, k := range p.cfg.AggregateBy {
		if v, ok := attrs.Get(k); ok {
			out.PutStr(k, v.AsString())
		}
	}
	return out
}

// newSeriesFrom creates a sketchSeries with the appropriate attribute set for this processor.
func (p *ddsketchProcessor) newSeriesFrom(attrs pcommon.Map, start, ts pcommon.Timestamp) *sketchSeries {
	return &sketchSeries{
		attrs: p.seriesAttrs(attrs),
		start: start,
		end:   ts,
	}
}

// accumulateIntoWindow aggregates incoming samples into sketches across a tumbling window.
func (p *ddsketchProcessor) accumulateIntoWindow(md pmetric.Metrics) {
	rms := md.ResourceMetrics()
	if rms.Len() == 0 {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)

		resKey := attributesKey(rm.Resource().Attributes())
		rw, ok := p.windowStore[resKey]
		if !ok {
			rw = &resourceWindow{
				resource: pcommon.NewResource(),
				scopes:   make(map[string]*scopeWindow),
			}
			rm.Resource().CopyTo(rw.resource)
			p.windowStore[resKey] = rw
		}

		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			scope := sm.Scope()
			scopeKey := scope.Name() + ":" + scope.Version()

			sw, ok := rw.scopes[scopeKey]
			if !ok {
				sw = &scopeWindow{
					scope:   pcommon.NewInstrumentationScope(),
					metrics: make(map[string]*metricWindow),
				}
				scope.CopyTo(sw.scope)
				rw.scopes[scopeKey] = sw
			}

			metrics := sm.Metrics()
			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)

				switch metric.Type() {
				case pmetric.MetricTypeGauge:
					p.accumulateGaugeMetric(sw, metric)
				case pmetric.MetricTypeDDSketch:
					p.accumulateDDSketchMetric(sw, metric)
				default:
					continue
				}
			}
		}
	}
}

func (p *ddsketchProcessor) getOrCreateMetricWindow(sw *scopeWindow, metric pmetric.Metric) *metricWindow {
	name := metric.Name()
	mw, ok := sw.metrics[name]
	if !ok {
		mw = &metricWindow{
			name:        name,
			description: metric.Description(),
			unit:        metric.Unit(),
			series:      make(map[string]*sketchSeries),
		}
		sw.metrics[name] = mw
	}
	return mw
}

func (p *ddsketchProcessor) accumulateGaugeMetric(sw *scopeWindow, metric pmetric.Metric) {
	mw := p.getOrCreateMetricWindow(sw, metric)

	dps := metric.Gauge().DataPoints()
	for l := 0; l < dps.Len(); l++ {
		dp := dps.At(l)
		if !p.matchesMatchers(dp.Attributes()) {
			continue
		}
		attrKey := p.seriesKey(dp.Attributes())
		series := mw.series[attrKey]
		if series == nil {
			series = p.newSeriesFrom(dp.Attributes(), dp.StartTimestamp(), dp.Timestamp())
			mw.series[attrKey] = series
		} else {
			series.updateWindow(dp.StartTimestamp(), dp.Timestamp())
		}

		sk, err := p.ensureSketch(series)
		if err != nil {
			if p.logger != nil {
				p.logger.Error("failed to create DDSketch for gauge in window mode", zap.Error(err))
			}
			continue
		}

		switch dp.ValueType() {
		case pmetric.NumberDataPointValueTypeDouble:
			sk.Update(dp.DoubleValue())
		case pmetric.NumberDataPointValueTypeInt:
			sk.Update(float64(dp.IntValue()))
		default:
			if p.logger != nil {
				p.logger.Error("unsupported gauge data point type in window mode", zap.Any("type", dp.ValueType()))
			}
			continue
		}

		series.count++
		series.flags |= dp.Flags()
	}
}

func (p *ddsketchProcessor) accumulateDDSketchMetric(sw *scopeWindow, metric pmetric.Metric) {
	mw := p.getOrCreateMetricWindow(sw, metric)

	// Preserve the original temporality; take the first non-Unspecified value seen.
	if mw.temporality == pmetric.AggregationTemporalityUnspecified {
		mw.temporality = metric.DDSketch().AggregationTemporality()
	}

	dps := metric.DDSketch().DataPoints()
	for l := 0; l < dps.Len(); l++ {
		dp := dps.At(l)
		if !p.matchesMatchers(dp.Attributes()) {
			continue
		}
		attrKey := p.seriesKey(dp.Attributes())
		series := mw.series[attrKey]
		if series == nil {
			series = p.newSeriesFrom(dp.Attributes(), dp.StartTimestamp(), dp.Timestamp())
			mw.series[attrKey] = series
		} else {
			series.updateWindow(dp.StartTimestamp(), dp.Timestamp())
		}

		sk, err := p.decodeDDSketchDataPoint(mw.name+"::"+attributesKey(dp.Attributes()), dp)
		if err != nil {
			if p.logger != nil {
				p.logger.Error("failed to decode DDSketch payload in window mode", zap.Error(err))
			}
			continue
		}
		if sk == nil {
			continue // delta with no snapshot yet
		}

		series.merge(sk, dp, p.logger)
	}
}

// flushWindow turns the accumulated raw samples into output metrics and forwards them.
func (p *ddsketchProcessor) flushWindow(ctx context.Context) error {
	p.mu.Lock()
	if len(p.windowStore) == 0 {
		p.mu.Unlock()
		return nil
	}

	snapshot := p.windowStore
	p.windowStore = make(map[string]*resourceWindow)
	p.mu.Unlock()

	out := pmetric.NewMetrics()
	rms := out.ResourceMetrics()

	for _, rw := range snapshot {
		rm := rms.AppendEmpty()
		rw.resource.CopyTo(rm.Resource())

		sms := rm.ScopeMetrics()
		for _, sw := range rw.scopes {
			sm := sms.AppendEmpty()
			sw.scope.CopyTo(sm.Scope())

			dstMetrics := sm.Metrics()
			for _, mw := range sw.metrics {
				tmp := pmetric.NewMetric()
				tmp.SetName(mw.name)
				tmp.SetDescription(mw.description)
				tmp.SetUnit(mw.unit)
				// Restore temporality on the template so buildMergedSketchMetric
				// can propagate it to the emitted DDSketch metric.
				if p.cfg.TransmitSketch {
					tmp.SetEmptyDDSketch().SetAggregationTemporality(mw.temporality)
				}

				var (
					outMetric pmetric.Metric
					ok        bool
				)
				if p.cfg.TransmitSketch {
					outMetric, ok = p.buildMergedSketchMetric(tmp, mw.series)
				} else {
					outMetric, ok = p.buildQuantileMetric(tmp, mw.series)
				}
				if !ok {
					continue
				}
				outMetric.CopyTo(dstMetrics.AppendEmpty())
			}
		}
	}

	if out.ResourceMetrics().Len() == 0 {
		return nil
	}
	p.recordOutput(ctx, out)
	return p.nextConsumer.ConsumeMetrics(ctx, out)
}

func (p *ddsketchProcessor) enableSelfMonitoring(settings component.TelemetrySettings, processorID string) {
	monitor, err := selfmonitor.New(settings, processorID, Type.String(), p.activeSeriesCount)
	if err != nil {
		if p.logger != nil {
			p.logger.Warn("ddsketchprocessor: failed to initialize self-monitoring", zap.Error(err))
		}
		return
	}
	p.monitor = monitor
}

func (p *ddsketchProcessor) shutdownMonitor() {
	if p.monitor != nil {
		p.monitor.Shutdown()
	}
}

func (p *ddsketchProcessor) recordInput(ctx context.Context, md pmetric.Metrics) {
	if p.monitor != nil {
		p.monitor.RecordInput(ctx, md)
	}
}

func (p *ddsketchProcessor) recordOutput(ctx context.Context, md pmetric.Metrics) {
	if p.monitor != nil {
		p.monitor.RecordOutput(ctx, md)
	}
}

func (p *ddsketchProcessor) activeSeriesCount() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	var total int64
	for _, rw := range p.windowStore {
		for _, sw := range rw.scopes {
			for _, mw := range sw.metrics {
				total += int64(len(mw.series))
			}
		}
	}
	return total
}
