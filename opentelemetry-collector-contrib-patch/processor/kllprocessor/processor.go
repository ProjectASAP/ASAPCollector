package kllprocessor

import (
	"context"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	kll "github.com/ProjectASAP/sketchlib-go/sketches/KLL"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor/selfmonitor"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// builderPool recycles strings.Builder instances to avoid per-call heap
// allocations in the hot attributesKey path.
var builderPool = sync.Pool{New: func() any { return new(strings.Builder) }}

type kllProcessor struct {
	cfg          *Config
	logger       *zap.Logger
	nextConsumer consumer.Metrics
	monitor      *selfmonitor.Monitor

	mu            sync.Mutex
	windowStore   map[string]*resourceWindow
	stopCh        chan struct{}
	doneCh        chan struct{}
	windowStarted atomic.Bool // true once the window goroutine is running

	// seriesPool recycles kllSeries structs (and their underlying KLL sketch
	// compactor arrays) across window flushes to reduce GC pressure in
	// high-cardinality deployments.
	seriesPool sync.Pool
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
	series      map[string]*kllSeries
}

type kllSeries struct {
	attrs  pcommon.Map
	sketch *kll.KLLSketch
}

func newProcessor(cfg *Config, logger *zap.Logger, next consumer.Metrics) *kllProcessor {
	p := &kllProcessor{
		cfg:          cfg,
		logger:       logger,
		nextConsumer: next,
		windowStore:  make(map[string]*resourceWindow),
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
	}
	p.seriesPool.New = func() any { return new(kllSeries) }
	return p
}

func (p *kllProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *kllProcessor) Start(ctx context.Context, _ component.Host) error {
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

func (p *kllProcessor) Shutdown(ctx context.Context) error {
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

func (p *kllProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
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
		// Forward inputs unchanged so this processor can chain with
		// other windowed sketch processors in a single pipeline
		// (`processors: [kll, hll, batch]`). Without this, the next
		// processor never sees the raw inputs — it only sees this
		// processor's tick-emitted typed sketches, which it treats
		// as foreign types and drops. Matches what
		// countminsketchprocessor / countsketchprocessor already do.
		return p.nextConsumer.ConsumeMetrics(ctx, md)
	default:
		if p.logger != nil {
			p.logger.Error("kllprocessor: unknown mode, dropping metrics", zap.Any("mode", p.cfg.Mode))
		}
		return nil
	}
}

// processBatch builds per-batch KLL sketches from gauge and KLLSketch data points,
// appends quantile metrics to md (raw inputs preserved).
func (p *kllProcessor) processBatch(md pmetric.Metrics) error {
	type batchSeries struct {
		name   string
		unit   string
		attrs  pcommon.Map
		sketch *kll.KLLSketch
	}
	batched := make(map[string]*batchSeries) // key = metricName + "::" + attributesKey(attrs)

	getOrCreate := func(name, unit string, attrs pcommon.Map) *batchSeries {
		key := name + "::" + p.seriesKey(attrs)
		bs := batched[key]
		if bs == nil {
			bs = &batchSeries{name: name, unit: unit, attrs: p.seriesAttrs(attrs), sketch: newKLLSketch(p.cfg)}
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
						var val float64
						if p.cfg.ReadAsInt {
							val = float64(dp.IntValue())
						} else {
							val = dp.DoubleValue()
						}
						bs := getOrCreate(metric.Name(), metric.Unit(), dp.Attributes())
						if bs.sketch != nil {
							bs.sketch.Update(val)
						}
					}
				case pmetric.MetricTypeKLLSketch:
					dps := metric.KLLSketch().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						if !p.matchesMatchers(dp.Attributes()) {
							continue
						}
						bs := getOrCreate(metric.Name(), metric.Unit(), dp.Attributes())
						if bs.sketch != nil && len(dp.Sketch()) > 0 {
							incoming, err := kll.DeserializeKLLSketchFromProtoBytes(dp.Sketch())
							if err == nil {
								_ = bs.sketch.Merge(incoming)
							} else if p.logger != nil {
								p.logger.Error("kllprocessor: failed to deserialize KLLSketch dp", zap.Error(err))
							}
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
	scope.Scope().SetName("otelcol/kllprocessor")
	now := pcommon.NewTimestampFromTime(time.Now())

	for _, bs := range batched {
		if bs.sketch == nil || bs.sketch.GetSize() == 0 {
			continue
		}
		if p.cfg.TransmitSketch {
			// Typed KLLSketchDataPoint emission — what
			// ASAPQuery-backend's modified-OTLP sketch router
			// consumes as `Metric.data = KLLSketch{...}`.
			// Before this change the processor emitted a Gauge
			// with the sketch payload stuffed into a
			// `kll.sketch_payload` byte attribute, which the
			// backend router never recognized as a sketch
			// variant.
			m := findOrCreateKLLSketchMetric(
				scope.Metrics(), p.sketchMetricName(bs.name), bs.unit)
			if err := appendTypedKLLSketchDataPoint(
				m, bs.attrs, bs.sketch, now, p.cfg.K, p.logger,
			); err != nil && p.logger != nil {
				p.logger.Error("kllprocessor: failed to serialize sketch", zap.Error(err))
			}
			continue
		}
		cdf := bs.sketch.CDF()
		for _, q := range p.cfg.Quantiles {
			suffix, ok := p.cfg.suffixes[q]
			if !ok {
				continue
			}
			metricName := bs.name + suffix
			m := findOrCreateGaugeMetric(scope.Metrics(), metricName, bs.unit)
			dp := m.Gauge().DataPoints().AppendEmpty()
			bs.attrs.CopyTo(dp.Attributes())
			dp.SetTimestamp(now)
			dp.SetDoubleValue(cdf.Query(q))
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
func (p *kllProcessor) matchesMatchers(attrs pcommon.Map) bool {
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
func (p *kllProcessor) seriesKey(attrs pcommon.Map) string {
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
func (p *kllProcessor) seriesAttrs(attrs pcommon.Map) pcommon.Map {
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

func (p *kllProcessor) accumulateIntoWindow(md pmetric.Metrics) {
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
				case pmetric.MetricTypeKLLSketch:
					p.accumulateKLLSketchMetric(sw, metric)
				}
			}
		}
	}
}

func (p *kllProcessor) getOrCreateMetricWindow(sw *scopeWindow, metric pmetric.Metric) *metricWindow {
	name := metric.Name()
	mw, ok := sw.metrics[name]
	if !ok {
		mw = &metricWindow{
			name:        name,
			description: metric.Description(),
			unit:        metric.Unit(),
			series:      make(map[string]*kllSeries),
		}
		sw.metrics[name] = mw
	}
	return mw
}

func (p *kllProcessor) accumulateGaugeMetric(sw *scopeWindow, metric pmetric.Metric) {
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
			series = p.seriesPool.Get().(*kllSeries)
			series.attrs = p.seriesAttrs(dp.Attributes())
			if series.sketch != nil {
				series.sketch.Reset()
			} else {
				series.sketch = newKLLSketch(p.cfg)
			}
			mw.series[attrKey] = series
		}
		var val float64
		if p.cfg.ReadAsInt {
			val = float64(dp.IntValue())
		} else {
			val = dp.DoubleValue()
		}
		if series.sketch != nil {
			series.sketch.Update(val)
		}
	}
}

// accumulateKLLSketchMetric merges pre-aggregated KLLSketch data points (from the
// SDK pre-aggregation path) into the window store.
func (p *kllProcessor) accumulateKLLSketchMetric(sw *scopeWindow, metric pmetric.Metric) {
	mw := p.getOrCreateMetricWindow(sw, metric)
	dps := metric.KLLSketch().DataPoints()
	for l := 0; l < dps.Len(); l++ {
		dp := dps.At(l)
		if !p.matchesMatchers(dp.Attributes()) {
			continue
		}
		attrKey := p.seriesKey(dp.Attributes())
		series := mw.series[attrKey]
		if series == nil {
			series = p.seriesPool.Get().(*kllSeries)
			series.attrs = p.seriesAttrs(dp.Attributes())
			if series.sketch != nil {
				series.sketch.Reset()
			} else {
				series.sketch = newKLLSketch(p.cfg)
			}
			mw.series[attrKey] = series
		}
		if series.sketch != nil && len(dp.Sketch()) > 0 {
			incoming, err := kll.DeserializeKLLSketchFromProtoBytes(dp.Sketch())
			if err == nil {
				_ = series.sketch.Merge(incoming)
			} else if p.logger != nil {
				p.logger.Error("kllprocessor: failed to merge KLLSketch dp", zap.Error(err))
			}
		}
	}
}

func (p *kllProcessor) flushWindow(ctx context.Context) error {
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
				if p.cfg.TransmitSketch {
					now := pcommon.NewTimestampFromTime(time.Now())
					var (
						m       pmetric.Metric
						created bool
					)
					for _, series := range mw.series {
						if series.sketch == nil || series.sketch.GetSize() == 0 {
							series.attrs = pcommon.Map{}
							p.seriesPool.Put(series)
							continue
						}
						if !created {
							m = dstMetrics.AppendEmpty()
							m.SetName(p.sketchMetricName(mw.name))
							m.SetDescription(mw.description)
							m.SetUnit(mw.unit)
							// Typed KLLSketchDataPoint emission,
							// matching the batch path above.
							m.SetEmptyKLLSketch().SetAggregationTemporality(
								pmetric.AggregationTemporalityDelta)
							created = true
						}
						if err := appendTypedKLLSketchDataPoint(
							m, series.attrs, series.sketch, now, p.cfg.K, p.logger,
						); err != nil && p.logger != nil {
							p.logger.Error("kllprocessor: failed to serialize sketch", zap.Error(err))
						}
						series.attrs = pcommon.Map{}
						p.seriesPool.Put(series)
					}
					continue
				}
				for _, q := range p.cfg.Quantiles {
					suffix, ok := p.cfg.suffixes[q]
					if !ok {
						continue
					}
					metricName := mw.name + suffix
					if p.cfg.MetricSuffix != "" {
						metricName = mw.name + p.cfg.MetricSuffix + suffix
					}
					now := pcommon.NewTimestampFromTime(time.Now())
					var dps []struct {
						attrs pcommon.Map
						val   float64
					}
					for _, series := range mw.series {
						if series.sketch == nil || series.sketch.GetSize() == 0 {
							continue
						}
						dps = append(dps, struct {
							attrs pcommon.Map
							val   float64
						}{series.attrs, series.sketch.CDF().Query(q)})
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
				// Return series to pool after all quantiles have been processed.
				for _, series := range mw.series {
					series.attrs = pcommon.Map{}
					p.seriesPool.Put(series)
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

func (p *kllProcessor) enableSelfMonitoring(settings component.TelemetrySettings, processorID string) {
	monitor, err := selfmonitor.New(settings, processorID, "KLL", p.activeSeriesCount)
	if err != nil {
		if p.logger != nil {
			p.logger.Warn("kllprocessor: failed to initialize self-monitoring", zap.Error(err))
		}
		return
	}
	p.monitor = monitor
}

func (p *kllProcessor) shutdownMonitor() {
	if p.monitor != nil {
		p.monitor.Shutdown()
	}
}

func (p *kllProcessor) recordInput(ctx context.Context, md pmetric.Metrics) {
	if p.monitor != nil {
		p.monitor.RecordInput(ctx, md)
	}
}

func (p *kllProcessor) recordOutput(ctx context.Context, md pmetric.Metrics) {
	if p.monitor != nil {
		p.monitor.RecordOutput(ctx, md)
	}
}

func (p *kllProcessor) activeSeriesCount() int64 {
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

// findOrCreateGaugeMetric is retained for the non-TransmitSketch
// quantile-emission path, which still exports gauge-shaped scalar
// quantile series.
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

// findOrCreateKLLSketchMetric finds an existing typed KLLSketch metric
// with the given name in `metrics` or appends a new empty one. Used by
// the TransmitSketch batch path, which groups multiple sketch series
// under the same metric name.
func findOrCreateKLLSketchMetric(metrics pmetric.MetricSlice, name, unit string) pmetric.Metric {
	for idx := 0; idx < metrics.Len(); idx++ {
		if metrics.At(idx).Name() == name &&
			metrics.At(idx).Type() == pmetric.MetricTypeKLLSketch {
			return metrics.At(idx)
		}
	}
	m := metrics.AppendEmpty()
	m.SetName(name)
	m.SetUnit(unit)
	m.SetEmptyKLLSketch().SetAggregationTemporality(
		pmetric.AggregationTemporalityDelta)
	return m
}

// appendTypedKLLSketchDataPoint serializes the KLL sketch and writes
// it into a typed `KLLSketchDataPoint` on the given metric (which
// must already be `MetricTypeKLLSketch`). Replaces the earlier
// Gauge-with-`kll.sketch_payload`-byte-attribute shape, which
// ASAPQuery-backend's modified-OTLP decoder never recognized as a
// sketch variant.
func appendTypedKLLSketchDataPoint(
	metric pmetric.Metric,
	attrs pcommon.Map,
	sketch *kll.KLLSketch,
	ts pcommon.Timestamp,
	k int,
	logger *zap.Logger,
) error {
	payload, err := serializeKLLSketch(sketch)
	if err != nil {
		return err
	}
	dp := metric.KLLSketch().DataPoints().AppendEmpty()
	attrs.CopyTo(dp.Attributes())
	// Keep `kll.k` on attributes for operator visibility — the
	// typed DP has no dedicated field for it and the backend's
	// Rust side derives k from the payload anyway.
	dp.Attributes().PutInt("kll.k", int64(k))
	dp.SetTimestamp(ts)
	dp.SetCount(uint64(sketch.Count()))
	dp.SetSketch(payload)
	dp.SetEncoding(pmetric.KLLSketchEncodingProto)
	// Sum / Min / Max fields are part of the KLLSketchDataPoint
	// spec but the sketchlib-go KLLSketch type does not track them
	// (KLL is quantile-only; sum/min/max are carried alongside
	// the sketch for convenience in the proto shape). Leave them
	// at zero — the backend's decoder tolerates missing values.
	_ = logger
	return nil
}

func serializeKLLSketch(sketch *kll.KLLSketch) ([]byte, error) {
	if sketch == nil {
		return nil, nil
	}
	env, err := sketch.SerializePortable()
	if err != nil {
		return nil, err
	}
	return proto.Marshal(env)
}

func (p *kllProcessor) sketchMetricName(base string) string {
	if p.cfg.MetricSuffix != "" {
		return base + p.cfg.MetricSuffix
	}
	return base + "_kll"
}

// newKLLSketch builds a fresh KLLSketch honoring cfg.Seed.
//
// When cfg.Seed is nil (the default and the production path), the
// time-seeded constructor is used — preserving today's behavior.
// When cfg.Seed is set (parity harness / deterministic-replay), the
// seeded constructor is used so two processors fed identical input
// produce byte-identical sketch state.
func newKLLSketch(cfg *Config) *kll.KLLSketch {
	if cfg.Seed != nil {
		sketch, err := kll.NewKLLSketchWithSeed(cfg.K, *cfg.Seed)
		if err != nil {
			return nil
		}
		return sketch
	}
	sketch, err := kll.NewKLLSketch(cfg.K)
	if err != nil {
		return nil
	}
	return sketch
}
