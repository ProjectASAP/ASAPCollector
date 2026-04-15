package hllprocessor

import (
	"context"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	hll "github.com/ProjectASAP/sketchlib-go/sketches/HLL"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor/selfmonitor"
	"go.uber.org/zap"
)

// builderPool recycles strings.Builder instances to avoid per-call heap
// allocations in the hot attributesKey path.
var builderPool = sync.Pool{New: func() any { return new(strings.Builder) }}

// inboundMergeHLL merges a received HLLSketch data point into dst.
// For HLLSketchEncodingProto payloads the sketch bytes are deserialized and
// merged directly. For HLLSketchEncodingDelta payloads the register delta is
// applied to the last known snapshot to reconstruct the current full state,
// which is then merged into dst. The inbound snapshot is updated on each call.
func (p *hllProcessor) inboundMergeHLL(seriesKey string, dp pmetric.HLLSketchDataPoint, dst *hll.HyperLogLog) error {
	payload := dp.Sketch()
	if len(payload) == 0 {
		return nil
	}

	switch dp.Encoding() {
	case pmetric.HLLSketchEncodingDelta:
		p.inboundMu.Lock()
		snap, hasSnap := p.inboundSnapshots[seriesKey]
		p.inboundMu.Unlock()
		if !hasSnap || snap == nil {
			// No snapshot to apply delta against; skip.
			return nil
		}
		// Clone the snapshot and apply the delta to get the current full state.
		reconstructed := cloneHLL(snap)
		if reconstructed == nil {
			return nil
		}
		deltaMsg, err := hll.DeserializeRegisterDelta(payload)
		if err != nil {
			return err
		}
		hll.ApplyRegisterDelta(reconstructed, deltaMsg)
		// Update inbound snapshot to the reconstructed current state.
		p.inboundMu.Lock()
		p.inboundSnapshots[seriesKey] = cloneHLL(reconstructed)
		p.inboundMu.Unlock()
		return dst.Merge(reconstructed)

	default: // HLLSketchEncodingProto or unspecified
		src, err := hll.DeserializeHyperLogLogFromProtoBytes(payload)
		if err != nil {
			return err
		}
		// Store full snapshot for future delta reconstruction.
		p.inboundMu.Lock()
		p.inboundSnapshots[seriesKey] = cloneHLL(src)
		p.inboundMu.Unlock()
		return dst.Merge(src)
	}
}

// mergeSketchBytes deserializes a serialized HLL sketch and merges it into dst.
// Returns dst unchanged (with an error) if deserialization fails.
func mergeSketchBytes(dst *hll.HyperLogLog, payload []byte) error {
	src, err := hll.DeserializeHyperLogLogFromProtoBytes(payload)
	if err != nil {
		return err
	}
	return dst.Merge(src)
}

type hllProcessor struct {
	cfg          *Config
	logger       *zap.Logger
	nextConsumer consumer.Metrics
	monitor      *selfmonitor.Monitor

	mu            sync.Mutex
	windowStore   map[string]*resourceWindow
	stopCh        chan struct{}
	doneCh        chan struct{}
	windowStarted atomic.Bool

	// seriesPool recycles hllSeries structs (and their underlying HLL sketch
	// register arrays) across window flushes to reduce GC pressure in
	// high-cardinality deployments.
	seriesPool sync.Pool

	// snapshots maps "<metricName>::<attrKey>" → cloned HLL, updated each flush.
	// Used to compute register deltas when cfg.DeltaTransmission=true.
	snapshots   map[string]*hll.HyperLogLog
	snapshotsMu sync.Mutex

	// inboundSnapshots tracks the last reconstructed full HLL per series key
	// received from upstream (e.g., SDK). Used to apply register deltas when
	// the upstream sends HLLSketchEncodingDelta payloads.
	inboundMu        sync.Mutex
	inboundSnapshots map[string]*hll.HyperLogLog
}

type resourceWindow struct {
	resource pcommon.Resource
	scopes   map[string]*scopeWindow
}

type scopeWindow struct {
	scope   pcommon.InstrumentationScope
	metrics map[string]*metricWindow
}

type metricWindow struct {
	name        string
	description string
	unit        string
	series      map[string]*hllSeries
}

type hllSeries struct {
	attrs  pcommon.Map
	sketch *hll.HyperLogLog
}

func newProcessor(cfg *Config, logger *zap.Logger, next consumer.Metrics) *hllProcessor {
	p := &hllProcessor{
		cfg:              cfg,
		logger:           logger,
		nextConsumer:     next,
		windowStore:      make(map[string]*resourceWindow),
		snapshots:        make(map[string]*hll.HyperLogLog),
		inboundSnapshots: make(map[string]*hll.HyperLogLog),
		stopCh:           make(chan struct{}),
		doneCh:           make(chan struct{}),
	}
	p.seriesPool.New = func() any { return new(hllSeries) }
	return p
}

func (p *hllProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *hllProcessor) Start(ctx context.Context, _ component.Host) error {
	if p.cfg.Mode != ModeWindow {
		return nil
	}
	ticker := time.NewTicker(p.cfg.WindowDuration)
	p.windowStarted.Store(true)
	go func() {
		defer func() {
			ticker.Stop()
			close(p.doneCh)
		}()
		for {
			select {
			case <-ctx.Done():
				_ = p.flushWindow(context.Background())
				return
			case <-p.stopCh:
				_ = p.flushWindow(context.Background())
				return
			case <-ticker.C:
				_ = p.flushWindow(context.Background())
			}
		}
	}()
	return nil
}

func (p *hllProcessor) Shutdown(ctx context.Context) error {
	defer p.shutdownMonitor()

	if p.cfg.Mode != ModeWindow || !p.windowStarted.Load() {
		return nil
	}
	close(p.stopCh)
	select {
	case <-p.doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *hllProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	p.recordInput(ctx, md)

	switch p.cfg.Mode {
	case ModeBatch:
		if err := p.processBatch(md); err != nil {
			return err
		}
		p.recordOutput(ctx, md)
		return p.nextConsumer.ConsumeMetrics(ctx, md)
	case ModeWindow:
		p.accumulateIntoWindow(md)
		return nil
	default:
		if p.logger != nil {
			p.logger.Error("hllprocessor: unknown mode, dropping metrics", zap.Any("mode", p.cfg.Mode))
		}
		return nil
	}
}

// processBatch aggregates gauge and HLLSketch data points within a batch into
// HLL sketches and appends a cardinality gauge metric to the resource metrics slice.
func (p *hllProcessor) processBatch(md pmetric.Metrics) error {
	type batchSeries struct {
		name   string
		unit   string
		attrs  pcommon.Map
		sketch *hll.HyperLogLog
		count  uint64
	}
	batched := make(map[string]*batchSeries)

	getOrCreate := func(name, unit string, attrs pcommon.Map) *batchSeries {
		key := name + "::" + p.seriesKey(attrs)
		bs := batched[key]
		if bs == nil {
			bs = &batchSeries{
				name:   name,
				unit:   unit,
				attrs:  p.seriesAttrs(attrs),
				sketch: hll.NewHyperLogLog(),
			}
			batched[key] = bs
		}
		return bs
	}

	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			metrics := sms.At(j).Metrics()
			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)
				switch metric.Type() {
				case pmetric.MetricTypeGauge:
					dps := metric.Gauge().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						if !p.matchesMatchers(dp.Attributes()) {
							continue
						}
						bs := getOrCreate(metric.Name(), metric.Unit(), dp.Attributes())
						bs.sketch.InsertValue(dp.DoubleValue())
						bs.count++
					}
				case pmetric.MetricTypeHLLSketch:
					dps := metric.HLLSketch().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						if !p.matchesMatchers(dp.Attributes()) {
							continue
						}
						bs := getOrCreate(metric.Name(), metric.Unit(), dp.Attributes())
						bs.count += dp.Count()
						inboundKey := metric.Name() + "::" + p.seriesKey(dp.Attributes())
						if err := p.inboundMergeHLL(inboundKey, dp, bs.sketch); err != nil && p.logger != nil {
							p.logger.Error("hllprocessor: failed to merge inbound HLLSketch", zap.Error(err))
						}
					}
				}
			}
		}
	}

	if len(batched) == 0 {
		return nil
	}

	scope := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	scope.Scope().SetName("otelcol/hllprocessor")
	now := pcommon.NewTimestampFromTime(time.Now())

	if p.cfg.TransmitSketch {
		// Group all batch series into a single HLLSketch metric per unique name.
		hllMetrics := make(map[string]pmetric.Metric)
		for _, bs := range batched {
			if bs.sketch == nil {
				continue
			}
			metricName := p.cardinalityMetricName(bs.name)
			m, ok := hllMetrics[metricName]
			if !ok {
				m = scope.Metrics().AppendEmpty()
				m.SetName(metricName)
				m.SetUnit(bs.unit)
				m.SetEmptyHLLSketch().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
				hllMetrics[metricName] = m
			}
			payload, err := bs.sketch.SerializeProtoBytes()
			if err != nil {
				if p.logger != nil {
					p.logger.Error("hllprocessor: failed to serialize sketch", zap.Error(err))
				}
				continue
			}
			dp := m.HLLSketch().DataPoints().AppendEmpty()
			bs.attrs.CopyTo(dp.Attributes())
			dp.SetTimestamp(now)
			dp.SetCount(bs.count)
			dp.SetCardinality(uint64(bs.sketch.EstimateCardinality()))
			dp.SetSketch(payload)
			dp.SetEncoding(pmetric.HLLSketchEncodingProto)
			dp.SetPrecision(uint32(hll.HLLPrecision))
		}
	} else {
		for _, bs := range batched {
			if bs.sketch == nil {
				continue
			}
			metricName := p.cardinalityMetricName(bs.name)
			m := findOrCreateGaugeMetric(scope.Metrics(), metricName, bs.unit)
			dp := m.Gauge().DataPoints().AppendEmpty()
			bs.attrs.CopyTo(dp.Attributes())
			dp.SetTimestamp(now)
			dp.SetDoubleValue(float64(bs.sketch.EstimateCardinality()))
		}
	}
	return nil
}

func attributesKey(attrs pcommon.Map) string {
	keys := make([]string, 0, attrs.Len())
	attrs.Range(func(k string, _ pcommon.Value) bool {
		keys = append(keys, k)
		return true
	})
	sort.Strings(keys)
	b := builderPool.Get().(*strings.Builder)
	b.Reset()
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
	s := b.String()
	builderPool.Put(b)
	return s
}

// matchesMatchers returns true if attrs satisfies all configured LabelMatchers.
func (p *hllProcessor) matchesMatchers(attrs pcommon.Map) bool {
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
func (p *hllProcessor) seriesKey(attrs pcommon.Map) string {
	if len(p.cfg.AggregateBy) == 0 {
		return attributesKey(attrs)
	}
	b := builderPool.Get().(*strings.Builder)
	b.Reset()
	for _, k := range p.cfg.AggregateBy { // already sorted by Validate
		v, ok := attrs.Get(k)
		if !ok {
			continue
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(v.AsString())
		b.WriteByte(';')
	}
	key := b.String()
	builderPool.Put(b)
	return key
}

// seriesAttrs returns the attribute map to store on a new series entry.
// When AggregateBy is configured, only those labels are included in the output.
// Otherwise a full copy of attrs is returned.
func (p *hllProcessor) seriesAttrs(attrs pcommon.Map) pcommon.Map {
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

func (p *hllProcessor) accumulateIntoWindow(md pmetric.Metrics) {
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
				case pmetric.MetricTypeHLLSketch:
					p.accumulateHLLSketchMetric(sw, metric)
				}
			}
		}
	}
}

func (p *hllProcessor) getOrCreateMetricWindow(sw *scopeWindow, metric pmetric.Metric) *metricWindow {
	name := metric.Name()
	mw, ok := sw.metrics[name]
	if !ok {
		mw = &metricWindow{
			name:        name,
			description: metric.Description(),
			unit:        metric.Unit(),
			series:      make(map[string]*hllSeries),
		}
		sw.metrics[name] = mw
	}
	return mw
}

func (p *hllProcessor) accumulateGaugeMetric(sw *scopeWindow, metric pmetric.Metric) {
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
			series = p.seriesPool.Get().(*hllSeries)
			series.attrs = p.seriesAttrs(dp.Attributes())
			if series.sketch != nil {
				series.sketch.Reset()
			} else {
				series.sketch = hll.NewHyperLogLog()
			}
			mw.series[attrKey] = series
		}
		series.sketch.InsertValue(dp.DoubleValue())
	}
}

// accumulateHLLSketchMetric merges pre-aggregated HLLSketch data points (from
// the SDK pre-aggregation path) into the window store.
func (p *hllProcessor) accumulateHLLSketchMetric(sw *scopeWindow, metric pmetric.Metric) {
	mw := p.getOrCreateMetricWindow(sw, metric)
	dps := metric.HLLSketch().DataPoints()
	for l := 0; l < dps.Len(); l++ {
		dp := dps.At(l)
		if !p.matchesMatchers(dp.Attributes()) {
			continue
		}
		attrKey := p.seriesKey(dp.Attributes())
		series := mw.series[attrKey]
		if series == nil {
			series = p.seriesPool.Get().(*hllSeries)
			series.attrs = p.seriesAttrs(dp.Attributes())
			if series.sketch != nil {
				series.sketch.Reset()
			} else {
				series.sketch = hll.NewHyperLogLog()
			}
			mw.series[attrKey] = series
		}
		inboundKey := mw.name + "::" + attrKey
		if err := p.inboundMergeHLL(inboundKey, dp, series.sketch); err != nil && p.logger != nil {
			p.logger.Error("hllprocessor: failed to merge inbound HLLSketch", zap.Error(err))
		}
	}
}

func (p *hllProcessor) flushWindow(ctx context.Context) error {
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
				metricName := p.cardinalityMetricName(mw.name)
				now := pcommon.NewTimestampFromTime(time.Now())

				if p.cfg.TransmitSketch {
					var (
						m       pmetric.Metric
						created bool
					)
					for _, series := range mw.series {
						if series.sketch == nil {
							series.attrs = pcommon.Map{}
							p.seriesPool.Put(series)
							continue
						}
						if !created {
							m = dstMetrics.AppendEmpty()
							m.SetName(metricName)
							m.SetDescription(mw.description)
							m.SetUnit(mw.unit)
							m.SetEmptyHLLSketch().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
							created = true
						}
						snapKey := mw.name + "::" + p.seriesKey(series.attrs)
						if p.cfg.DeltaTransmission {
							p.snapshotsMu.Lock()
							snap, hasSnap := p.snapshots[snapKey]
							p.snapshotsMu.Unlock()

							if hasSnap && snap != nil {
								deltaMsg := hll.ComputeRegisterDelta(snap, series.sketch)
								payload, err := hll.SerializeRegisterDelta(deltaMsg)
								if err != nil {
									if p.logger != nil {
										p.logger.Error("hllprocessor: failed to serialize delta", zap.Error(err))
									}
								} else {
									dp := m.HLLSketch().DataPoints().AppendEmpty()
									series.attrs.CopyTo(dp.Attributes())
									dp.SetTimestamp(now)
									dp.SetCount(0)
									dp.SetCardinality(uint64(series.sketch.EstimateCardinality()))
									dp.SetSketch(payload)
									dp.SetEncoding(pmetric.HLLSketchEncodingDelta)
									dp.SetPrecision(uint32(hll.HLLPrecision))
								}
							} else {
								payload, err := series.sketch.SerializeProtoBytes()
								if err != nil {
									if p.logger != nil {
										p.logger.Error("hllprocessor: failed to serialize sketch", zap.Error(err))
									}
								} else {
									dp := m.HLLSketch().DataPoints().AppendEmpty()
									series.attrs.CopyTo(dp.Attributes())
									dp.SetTimestamp(now)
									dp.SetCount(0)
									dp.SetCardinality(uint64(series.sketch.EstimateCardinality()))
									dp.SetSketch(payload)
									dp.SetEncoding(pmetric.HLLSketchEncodingProto)
									dp.SetPrecision(uint32(hll.HLLPrecision))
								}
							}
							newSnap := cloneHLL(series.sketch)
							p.snapshotsMu.Lock()
							p.snapshots[snapKey] = newSnap
							p.snapshotsMu.Unlock()
						} else {
							payload, err := series.sketch.SerializeProtoBytes()
							if err != nil {
								if p.logger != nil {
									p.logger.Error("hllprocessor: failed to serialize sketch", zap.Error(err))
								}
							} else {
								dp := m.HLLSketch().DataPoints().AppendEmpty()
								series.attrs.CopyTo(dp.Attributes())
								dp.SetTimestamp(now)
								dp.SetCount(0)
								dp.SetCardinality(uint64(series.sketch.EstimateCardinality()))
								dp.SetSketch(payload)
								dp.SetEncoding(pmetric.HLLSketchEncodingProto)
								dp.SetPrecision(uint32(hll.HLLPrecision))
							}
						}
						series.attrs = pcommon.Map{}
						p.seriesPool.Put(series)
					}
					continue
				}

				// Emit cardinality estimate per series.
				var dps []struct {
					attrs pcommon.Map
					val   float64
				}
				for _, series := range mw.series {
					if series.sketch == nil {
						series.attrs = pcommon.Map{}
						p.seriesPool.Put(series)
						continue
					}
					dps = append(dps, struct {
						attrs pcommon.Map
						val   float64
					}{series.attrs, float64(series.sketch.EstimateCardinality())})
					series.attrs = pcommon.Map{}
					p.seriesPool.Put(series)
				}
				if len(dps) == 0 {
					continue
				}
				m := dstMetrics.AppendEmpty()
				m.SetName(metricName)
				m.SetDescription(mw.description)
				m.SetUnit(mw.unit)
				g := m.SetEmptyGauge()
				for _, d := range dps {
					dp := g.DataPoints().AppendEmpty()
					d.attrs.CopyTo(dp.Attributes())
					dp.SetTimestamp(now)
					dp.SetDoubleValue(d.val)
				}
			}
		}
	}

	if out.ResourceMetrics().Len() == 0 {
		return nil
	}
	p.recordOutput(ctx, out)
	return p.nextConsumer.ConsumeMetrics(ctx, out)
}

func (p *hllProcessor) enableSelfMonitoring(settings component.TelemetrySettings, processorID string) {
	monitor, err := selfmonitor.New(settings, processorID, "HLL", p.activeSeriesCount)
	if err != nil {
		if p.logger != nil {
			p.logger.Warn("hllprocessor: failed to initialize self-monitoring", zap.Error(err))
		}
		return
	}
	p.monitor = monitor
}

func (p *hllProcessor) shutdownMonitor() {
	if p.monitor != nil {
		p.monitor.Shutdown()
	}
}

func (p *hllProcessor) recordInput(ctx context.Context, md pmetric.Metrics) {
	if p.monitor != nil {
		p.monitor.RecordInput(ctx, md)
	}
}

func (p *hllProcessor) recordOutput(ctx context.Context, md pmetric.Metrics) {
	if p.monitor != nil {
		p.monitor.RecordOutput(ctx, md)
	}
}

func (p *hllProcessor) activeSeriesCount() int64 {
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

func findOrCreateGaugeMetric(metrics pmetric.MetricSlice, name, unit string) pmetric.Metric {
	for idx := 0; idx < metrics.Len(); idx++ {
		if metrics.At(idx).Name() == name {
			return metrics.At(idx)
		}
	}
	m := metrics.AppendEmpty()
	m.SetName(name)
	m.SetUnit(unit)
	m.SetEmptyGauge()
	return m
}

// cloneHLL returns a deep copy of h suitable for use as a delta snapshot.
func cloneHLL(h *hll.HyperLogLog) *hll.HyperLogLog {
	data, err := h.SerializeProtoBytes()
	if err != nil {
		return nil
	}
	clone, err := hll.DeserializeHyperLogLogFromProtoBytes(data)
	if err != nil {
		return nil
	}
	return clone
}

func (p *hllProcessor) cardinalityMetricName(base string) string {
	if p.cfg.MetricSuffix != "" {
		return base + p.cfg.MetricSuffix
	}
	return base + "_hll_cardinality"
}
