package kllprocessor

import (
	"context"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	KLL "github.com/zzylol/go-kll"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type kllProcessor struct {
	cfg          *Config
	logger       *zap.Logger
	nextConsumer consumer.Metrics

	mu            sync.Mutex
	windowStore   map[string]*resourceWindow
	stopCh        chan struct{}
	doneCh        chan struct{}
	windowStarted atomic.Bool // true once the window goroutine is running
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
	sketch *KLL.Sketch
}

func newProcessor(cfg *Config, logger *zap.Logger, next consumer.Metrics) *kllProcessor {
	return &kllProcessor{
		cfg:          cfg,
		logger:       logger,
		nextConsumer: next,
		windowStore:  make(map[string]*resourceWindow),
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
	}
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
	switch p.cfg.Mode {
	case ModeBatch:
		if err := p.processBatch(md); err != nil {
			return err
		}
		return p.nextConsumer.ConsumeMetrics(ctx, md)
	case ModeWindow:
		p.accumulateIntoWindow(md)
		return nil
	default:
		if p.logger != nil {
			p.logger.Error("kllprocessor: unknown mode, dropping metrics", zap.Any("mode", p.cfg.Mode))
		}
		return nil
	}
}

// processBatch builds per-batch KLL sketches from gauge data points, appends quantile metrics to md (raw inputs preserved).
func (p *kllProcessor) processBatch(md pmetric.Metrics) error {
	type batchSeries struct {
		name   string
		unit   string
		attrs  pcommon.Map
		sketch *KLL.Sketch
	}
	batched := make(map[string]*batchSeries) // key = metricName + "::" + attributesKey(attrs)

	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			metrics := sms.At(j).Metrics()
			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)
				if metric.Type() != pmetric.MetricTypeGauge {
					continue
				}
				dps := metric.Gauge().DataPoints()
				for l := 0; l < dps.Len(); l++ {
					dp := dps.At(l)
					var val float64
					if p.cfg.ReadAsInt {
						val = float64(dp.IntValue())
					} else {
						val = dp.DoubleValue()
					}
					attrKey := attributesKey(dp.Attributes())
					key := metric.Name() + "::" + attrKey
					bs := batched[key]
					if bs == nil {
						attrCopy := pcommon.NewMap()
						dp.Attributes().CopyTo(attrCopy)
						bs = &batchSeries{
							name:   metric.Name(),
							unit:   metric.Unit(),
							attrs:  attrCopy,
							sketch: KLL.New(p.cfg.K),
						}
						batched[key] = bs
					}
					bs.sketch.Update(val)
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
		if bs.sketch.GetSize() == 0 {
			continue
		}
		cdf := bs.sketch.CDF()
		for _, q := range p.cfg.Quantiles {
			suffix, ok := p.cfg.suffixes[q]
			if !ok {
				continue
			}
			metricName := bs.name + suffix
			ms := scope.Metrics()
			var m pmetric.Metric
			var found bool
			for idx := 0; idx < ms.Len(); idx++ {
				if ms.At(idx).Name() == metricName {
					m = ms.At(idx)
					found = true
					break
				}
			}
			if !found {
				m = scope.Metrics().AppendEmpty()
				m.SetName(metricName)
				m.SetUnit(bs.unit)
				m.SetEmptyGauge()
			}
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
	var b strings.Builder
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
				if metric.Type() != pmetric.MetricTypeGauge {
					continue
				}
				p.accumulateGaugeMetric(sw, metric)
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
			series:     make(map[string]*kllSeries),
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
		attrKey := attributesKey(dp.Attributes())
		series := mw.series[attrKey]
		if series == nil {
			attrCopy := pcommon.NewMap()
			dp.Attributes().CopyTo(attrCopy)
			series = &kllSeries{
				attrs:  attrCopy,
				sketch: KLL.New(p.cfg.K),
			}
			mw.series[attrKey] = series
		}
		var val float64
		if p.cfg.ReadAsInt {
			val = float64(dp.IntValue())
		} else {
			val = dp.DoubleValue()
		}
		series.sketch.Update(val)
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
			}
		}
	}

	if out.ResourceMetrics().Len() == 0 {
		return nil
	}
	return p.nextConsumer.ConsumeMetrics(ctx, out)
}
