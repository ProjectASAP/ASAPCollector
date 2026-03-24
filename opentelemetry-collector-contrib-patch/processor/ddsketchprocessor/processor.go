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

	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/DataDog/sketches-go/ddsketch/pb/sketchpb"
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
		return nil
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
		dp.SetEncoding(pmetric.DDSketchEncodingProto)
		dp.SetSketch(payload)
		dp.SetFlags(s.flags)
		if p.cfg.DeltaTransmission {
			dp.Attributes().PutStr("ddsketch.encoding", encoding)
		}
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
			val, err := s.sketch.GetValueAtQuantile(q)
			if err != nil {
				if p.logger != nil {
					p.logger.Error("failed to evaluate DDSketch quantile", zap.Float64("quantile", q), zap.Error(err))
				}
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
			sk.Add(dp.DoubleValue())
		case pmetric.NumberDataPointValueTypeInt:
			sk.Add(float64(dp.IntValue()))
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

func (p *ddsketchProcessor) decodeDDSketchDataPoint(seriesKey string, dp pmetric.DDSketchDataPoint) (*ddsketch.DDSketch, error) {
	data := dp.Sketch()
	if len(data) == 0 {
		return nil, fmt.Errorf("empty DDSketch payload")
	}

	switch dp.Encoding() {
	case pmetric.DDSketchEncodingProtoDelta:
		p.inboundMu.Lock()
		snapPayload, hasSnap := p.inboundSnapshots[seriesKey]
		p.inboundMu.Unlock()
		if !hasSnap || snapPayload == nil {
			// No snapshot to apply delta against; skip this data point.
			return nil, nil
		}
		// Reconstruct: unmarshal snapshot, apply delta bucket counts.
		var snapPb sketchpb.DDSketch
		if err := proto.Unmarshal(snapPayload, &snapPb); err != nil {
			return nil, fmt.Errorf("unmarshal DDSketch snapshot: %w", err)
		}
		var deltaPb sketchpb.DDSketch
		if err := proto.Unmarshal(data, &deltaPb); err != nil {
			return nil, fmt.Errorf("unmarshal DDSketch delta: %w", err)
		}
		reconstructed := applyDDSketchDelta(&snapPb, &deltaPb)
		reconstructedBytes, err := proto.Marshal(reconstructed)
		if err != nil {
			return nil, fmt.Errorf("marshal reconstructed DDSketch: %w", err)
		}
		p.inboundMu.Lock()
		if p.inboundSnapshots == nil {
			p.inboundSnapshots = make(map[string][]byte)
		}
		p.inboundSnapshots[seriesKey] = reconstructedBytes
		p.inboundMu.Unlock()
		return ddsketch.FromProto(reconstructed)

	default: // DDSketchEncodingProto or unspecified
		if dp.Encoding() != pmetric.DDSketchEncodingProto && dp.Encoding() != pmetric.DDSketchEncodingUnspecified {
			return nil, fmt.Errorf("unsupported DDSketch encoding %v", dp.Encoding())
		}
		var pb sketchpb.DDSketch
		if err := proto.Unmarshal(data, &pb); err != nil {
			return nil, fmt.Errorf("unmarshal DDSketch: %w", err)
		}
		// Store full snapshot for future delta reconstruction.
		p.inboundMu.Lock()
		if p.inboundSnapshots == nil {
			p.inboundSnapshots = make(map[string][]byte)
		}
		p.inboundSnapshots[seriesKey] = data
		p.inboundMu.Unlock()
		return ddsketch.FromProto(&pb)
	}
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
	} else if err := s.sketch.MergeWith(sk); err != nil {
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
	sk, err := ddsketch.NewDefaultDDSketch(p.cfg.RelativeAccuracy)
	if err != nil {
		return nil, err
	}
	s.sketch = sk
	return sk, nil
}

func serializeDDSketch(sk *ddsketch.DDSketch) ([]byte, error) {
	if sk == nil {
		return nil, nil
	}
	return proto.Marshal(sk.ToProto())
}

// computeDDSketchDelta computes a sparse delta between a snapshot proto payload
// and the current sketch. Buckets are included when |Δcount| ≥ threshold.
// Returns proto-marshalled sketchpb.DDSketch bytes with only changed buckets.
func computeDDSketchDelta(snapPayload []byte, current *ddsketch.DDSketch, threshold uint64) ([]byte, error) {
	var snap sketchpb.DDSketch
	if err := proto.Unmarshal(snapPayload, &snap); err != nil {
		// Can't parse snapshot; fall back to full serialization.
		return serializeDDSketch(current)
	}

	curr := current.ToProto()
	delta := &sketchpb.DDSketch{
		Mapping:   curr.Mapping,
		ZeroCount: curr.ZeroCount - snap.ZeroCount,
	}

	// Compute sparse delta for positive/negative bucket stores.
	delta.PositiveValues = storeDelta(snap.PositiveValues, curr.PositiveValues, float64(threshold))
	delta.NegativeValues = storeDelta(snap.NegativeValues, curr.NegativeValues, float64(threshold))

	return proto.Marshal(delta)
}

// storeDelta returns a sparse Store containing only buckets where |Δcount| ≥ threshold.
func storeDelta(snap, curr *sketchpb.Store, threshold float64) *sketchpb.Store {
	if curr == nil {
		return nil
	}

	// Merge contiguous encoding into a single map for easy diffing.
	snapCounts := storeToMap(snap)
	currCounts := storeToMap(curr)

	out := &sketchpb.Store{BinCounts: make(map[int32]float64)}
	for idx, cnt := range currCounts {
		delta := cnt - snapCounts[idx]
		if delta >= threshold || delta <= -threshold {
			out.BinCounts[idx] = delta
		}
	}
	if len(out.BinCounts) == 0 {
		return nil
	}
	return out
}

// applyDDSketchDelta reconstructs a full DDSketch proto from a snapshot and a
// sparse delta (produced by computeDDSketchDelta / ddSketchDeltaPayload).
func applyDDSketchDelta(snap, delta *sketchpb.DDSketch) *sketchpb.DDSketch {
	out := &sketchpb.DDSketch{
		Mapping:   snap.Mapping,
		ZeroCount: snap.ZeroCount + delta.ZeroCount,
	}
	out.PositiveValues = applyDDStore(snap.PositiveValues, delta.PositiveValues)
	out.NegativeValues = applyDDStore(snap.NegativeValues, delta.NegativeValues)
	return out
}

// applyDDStore adds delta bucket counts onto snapshot bucket counts.
func applyDDStore(snap, delta *sketchpb.Store) *sketchpb.Store {
	base := storeToMap(snap) // existing helper in the file
	changes := storeToMap(delta)
	out := &sketchpb.Store{BinCounts: make(map[int32]float64)}
	for idx, cnt := range base {
		out.BinCounts[idx] = cnt
	}
	for idx, d := range changes {
		out.BinCounts[idx] += d
	}
	if len(out.BinCounts) == 0 {
		return nil
	}
	return out
}

// storeToMap converts a sketchpb.Store into a flat index→count map.
func storeToMap(s *sketchpb.Store) map[int32]float64 {
	m := make(map[int32]float64)
	if s == nil {
		return m
	}
	for idx, cnt := range s.BinCounts {
		m[idx] += cnt
	}
	for i, cnt := range s.ContiguousBinCounts {
		idx := s.ContiguousBinIndexOffset + int32(i)
		m[idx] += cnt
	}
	return m
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
			sk.Add(dp.DoubleValue())
		case pmetric.NumberDataPointValueTypeInt:
			sk.Add(float64(dp.IntValue()))
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
