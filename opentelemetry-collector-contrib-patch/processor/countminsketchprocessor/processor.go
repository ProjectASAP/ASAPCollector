// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package countminsketchprocessor

import (
	"context"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ProjectASAP/sketchlib-go/common"
	cms "github.com/ProjectASAP/sketchlib-go/sketches/CountMinSketch"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// builderPool recycles strings.Builder instances used in the hot
// encodeAttributesAsKey path (called on every data point).
var builderPool = sync.Pool{New: func() any { return new(strings.Builder) }}


//
// ─────────────────────────────────────────────────────────────
// Window-level state
// ─────────────────────────────────────────────────────────────
//

type windowSketch struct {
	cms         *cms.CountMinSketch
	attrs       pcommon.Map // output attributes for this sketch group
	mu          sync.Mutex
	sampleCount uint64
}

//
// ─────────────────────────────────────────────────────────────
// Processor definition
// ─────────────────────────────────────────────────────────────
//

type windowedCountMinSketchProcessor struct {
	cfg    *Config
	logger *zap.Logger

	nextConsumer consumer.Metrics

	activeWindowSketches map[string]*windowSketch
	mu                   sync.RWMutex

	stopCh        chan struct{}
	doneCh        chan struct{}
	windowStarted atomic.Bool

	// windowSketchPool recycles windowSketch structs (and their underlying CMS
	// arrays via Reset()) across window flushes to reduce GC pressure.
	windowSketchPool sync.Pool
}

func newProcessor(
	cfg *Config,
	next consumer.Metrics,
	logger *zap.Logger,
) *windowedCountMinSketchProcessor {
	p := &windowedCountMinSketchProcessor{
		cfg:                  cfg,
		logger:               logger,
		nextConsumer:         next,
		activeWindowSketches: make(map[string]*windowSketch),
		stopCh:               make(chan struct{}),
		doneCh:               make(chan struct{}),
	}
	p.windowSketchPool.New = func() any { return new(windowSketch) }
	return p
}

//
// ─────────────────────────────────────────────────────────────
// Lifecycle methods
// ─────────────────────────────────────────────────────────────
//

func (p *windowedCountMinSketchProcessor) Start(
	ctx context.Context,
	host component.Host,
) error {
	p.logger.Info(
		"Starting Count-Min Sketch processor",
		zap.String("mode", string(p.cfg.Mode)),
		zap.Duration("window_duration", p.cfg.WindowDuration),
	)

	// Batch mode does not require a background ticker; we flush per-batch.
	if p.cfg.Mode != ModeWindow {
		return nil
	}

	// Window mode: start background window loop if a positive window is configured.
	if p.cfg.WindowDuration <= 0 {
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
				p.emitWindowAndReset()
				return
			case <-p.stopCh:
				p.emitWindowAndReset()
				return
			case <-ticker.C:
				p.emitWindowAndReset()
			}
		}
	}()

	return nil
}

func (p *windowedCountMinSketchProcessor) Shutdown(
	ctx context.Context,
) error {
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

func (p *windowedCountMinSketchProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

//
// ─────────────────────────────────────────────────────────────
// Ingestion phase
// ─────────────────────────────────────────────────────────────
//

func (p *windowedCountMinSketchProcessor) ConsumeMetrics(
	ctx context.Context,
	md pmetric.Metrics,
) (pmetric.Metrics, error) {

	switch p.cfg.Mode {
	case ModeBatch:
		return p.consumeBatch(md), nil
	case ModeWindow:
		p.accumulateIntoWindow(md)
		if !p.cfg.DropOriginal {
			return md, nil
		}
		return pmetric.NewMetrics(), nil
	default:
		if p.logger != nil {
			p.logger.Error("countminsketchprocessor: unknown mode, dropping metrics", zap.Any("mode", p.cfg.Mode))
		}
		return pmetric.NewMetrics(), nil
	}
}

// accumulateIntoWindow ingests metrics into the active window sketches (window mode).
func (p *windowedCountMinSketchProcessor) accumulateIntoWindow(md pmetric.Metrics) {
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			metrics := sms.At(j).Metrics()
			for k := 0; k < metrics.Len(); k++ {
				p.ingestMetric(metrics.At(k))
			}
		}
	}
}

// consumeBatch aggregates a single batch into sketches and returns output metrics
// according to DropOriginal semantics (batch mode).
func (p *windowedCountMinSketchProcessor) consumeBatch(md pmetric.Metrics) pmetric.Metrics {
	// Reuse the window-style accumulation for this batch, then reset.
	p.accumulateIntoWindow(md)

	sketches := p.buildWindowMetricsAndReset()
	if sketches.ResourceMetrics().Len() == 0 {
		if p.cfg.DropOriginal {
			return pmetric.NewMetrics()
		}
		return md
	}

	if p.cfg.DropOriginal {
		return sketches
	}

	// Expansion mode: keep originals and append sketch summaries.
	out := pmetric.NewMetrics()
	md.ResourceMetrics().MoveAndAppendTo(out.ResourceMetrics())
	sketches.ResourceMetrics().MoveAndAppendTo(out.ResourceMetrics())
	return out
}

func (p *windowedCountMinSketchProcessor) ingestMetric(
	metric pmetric.Metric,
) {
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		dps := metric.Gauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			if !p.matchesMatchers(dp.Attributes()) {
				continue
			}
			p.updateWindowSketch(metric.Name(), dp)
		}
	case pmetric.MetricTypeSum:
		dps := metric.Sum().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			if !p.matchesMatchers(dp.Attributes()) {
				continue
			}
			p.updateWindowSketch(metric.Name(), dp)
		}
	case pmetric.MetricTypeCountMinSketch:
		// Pre-aggregated path: deserialize and merge each incoming sketch into
		// the corresponding per-aggregation-key window sketch.
		dps := metric.CountMinSketch().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			if !p.matchesMatchers(dp.Attributes()) {
				continue
			}
			if len(dp.Sketch()) == 0 {
				continue
			}
			incoming, err := deserializeCMS(dp.Sketch())
			if err != nil {
				p.logger.Error("countminsketchprocessor: failed to deserialize CountMinSketch dp", zap.Error(err))
				continue
			}
			aggregationKey := p.seriesKey(metric.Name(), dp.Attributes())
			p.mergeWindowSketch(aggregationKey, dp.Attributes(), incoming)
		}
	}
}

// matchesMatchers returns true if attrs satisfies all configured LabelMatchers.
func (p *windowedCountMinSketchProcessor) matchesMatchers(attrs pcommon.Map) bool {
	for _, m := range p.cfg.LabelMatchers {
		v, ok := attrs.Get(m.Key)
		if !ok || v.AsString() != m.Value {
			return false
		}
	}
	return true
}

// seriesKey returns the map key used to locate a series in the window store.
// When AggregateBy is configured, only those label values form the key (cross-series
// aggregation). Otherwise the full attribute set is used (per-series, default).
func (p *windowedCountMinSketchProcessor) seriesKey(metricName string, attrs pcommon.Map) string {
	return metricName + "::" + p.encodeKey(attrs)
}

// encodeKey builds a stable string from the labels that form the grouping key.
// When AggregateBy is set, only those keys are used; otherwise all attributes.
func (p *windowedCountMinSketchProcessor) encodeKey(attrs pcommon.Map) string {
	if len(p.cfg.AggregateBy) > 0 {
		sb := builderPool.Get().(*strings.Builder)
		sb.Reset()
		for _, k := range p.cfg.AggregateBy { // already sorted by Validate
			v, ok := attrs.Get(k)
			if !ok {
				continue
			}
			sb.WriteString(k)
			sb.WriteString("=")
			sb.WriteString(v.AsString())
			sb.WriteString(";")
		}
		s := sb.String()
		builderPool.Put(sb)
		return s
	}
	return encodeAttributesAsKey(attrs)
}

// seriesAttrs returns the attribute map to store on a new sketch group.
// When AggregateBy is configured, only those labels are included in the output.
// Otherwise a full copy of attrs is returned.
func (p *windowedCountMinSketchProcessor) seriesAttrs(attrs pcommon.Map) pcommon.Map {
	out := pcommon.NewMap()
	if len(p.cfg.AggregateBy) > 0 {
		for _, k := range p.cfg.AggregateBy {
			if v, ok := attrs.Get(k); ok {
				out.PutStr(k, v.AsString())
			}
		}
		return out
	}
	attrs.CopyTo(out)
	return out
}

// mergeWindowSketch merges an incoming pre-aggregated CMS into the per-key window store.
func (p *windowedCountMinSketchProcessor) mergeWindowSketch(aggregationKey string, attrs pcommon.Map, incoming *cms.CountMinSketch) {
	p.mu.RLock()
	ws, exists := p.activeWindowSketches[aggregationKey]
	p.mu.RUnlock()

	if !exists {
		p.mu.Lock()
		ws, exists = p.activeWindowSketches[aggregationKey]
		if !exists {
			ws = p.windowSketchPool.Get().(*windowSketch)
			if ws.cms != nil && ws.cms.Rows == incoming.Rows && ws.cms.Cols == incoming.Cols {
				ws.cms.Reset()
			} else {
				newCMS, err := cms.NewCountMinSketch(incoming.Rows, incoming.Cols)
				if err != nil {
					p.logger.Error("Failed to create CMS for merge", zap.Error(err))
					p.windowSketchPool.Put(ws)
					p.mu.Unlock()
					return
				}
				ws.cms = newCMS
			}
			ws.attrs = p.seriesAttrs(attrs)
			ws.sampleCount = 0
			p.activeWindowSketches[aggregationKey] = ws
		}
		p.mu.Unlock()
	}

	ws.mu.Lock()
	defer ws.mu.Unlock()
	if err := ws.cms.Merge(incoming); err != nil {
		p.logger.Error("Failed to merge CMS", zap.Error(err))
	}
}

func (p *windowedCountMinSketchProcessor) updateWindowSketch(
	metricName string,
	dp pmetric.NumberDataPoint,
) {
	aggregationKey := p.seriesKey(metricName, dp.Attributes())

	p.mu.RLock()
	ws, exists := p.activeWindowSketches[aggregationKey]
	p.mu.RUnlock()

	if !exists {
		p.mu.Lock()
		ws, exists = p.activeWindowSketches[aggregationKey]
		if !exists {
			ws = p.windowSketchPool.Get().(*windowSketch)
			if ws.cms != nil && ws.cms.Rows == p.cfg.Rows && ws.cms.Cols == p.cfg.Columns {
				ws.cms.Reset()
			} else {
				newCMS, err := cms.NewCountMinSketch(p.cfg.Rows, p.cfg.Columns)
				if err != nil {
					p.logger.Error("Failed to create CMS", zap.Error(err))
					p.windowSketchPool.Put(ws)
					p.mu.Unlock()
					return
				}
				ws.cms = newCMS
			}
			ws.attrs = p.seriesAttrs(dp.Attributes())
			ws.sampleCount = 0
			p.activeWindowSketches[aggregationKey] = ws
		}
		p.mu.Unlock()
	}

	// Update Sketch
	ws.mu.Lock()
	defer ws.mu.Unlock()

	// GENERATE SKETCH INPUT
	// We convert the attributes into string keys, then convert them to SketchInput.
	// SketchInput will calculate the hash using xxhash internally.
	flowKey := encodeAttributesAsKey(dp.Attributes())
	input := common.FromString(flowKey)

	// INSERT INTO SKETCH
	// New API: InsertWithHash(hash uint64)
	// Note: the library currently performs frequency increment (+1).
	// Value metric (float/int) is currently not used as a weight.
	ws.cms.InsertWithHash(input.Hash)

	ws.sampleCount++
}

//
// ─────────────────────────────────────────────────────────────
// Emission phase (tumbling window boundary)
// ─────────────────────────────────────────────────────────────
//

// buildWindowMetricsAndReset snapshots the current window sketches, resets the
// state, and returns a Metrics payload containing sketch summaries.
func (p *windowedCountMinSketchProcessor) buildWindowMetricsAndReset() pmetric.Metrics {
	p.mu.Lock()
	if len(p.activeWindowSketches) == 0 {
		p.mu.Unlock()
		return pmetric.NewMetrics()
	}

	// Snapshot and reset
	windowSnapshot := p.activeWindowSketches
	p.activeWindowSketches = make(map[string]*windowSketch)
	p.mu.Unlock()

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("otelcol/windowed-countmin")

	now := pcommon.NewTimestampFromTime(time.Now())

	for _, ws := range windowSnapshot {
		ws.mu.Lock()
		rows := ws.cms.Rows
		cols := ws.cms.Cols
		sampleCount := ws.sampleCount
		outputAttrs := ws.attrs
		payload, err := serializeCMS(ws.cms)
		ws.mu.Unlock()
		p.windowSketchPool.Put(ws)
		if err != nil {
			p.logger.Error("Failed to serialize CMS", zap.Error(err))
			continue
		}

		m := sm.Metrics().AppendEmpty()
		m.SetName(p.cfg.MetricName)
		m.SetUnit("1")

		gauge := m.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetTimestamp(now)

		outputAttrs.CopyTo(dp.Attributes())
		dp.Attributes().PutInt("rows", int64(rows))
		dp.Attributes().PutInt("cols", int64(cols))
		dp.Attributes().PutInt("sample_count", int64(sampleCount))
		if p.cfg.TransmitSketch {
			dp.Attributes().PutEmptyBytes("sketch_payload").FromRaw(payload)
		} else {
			dp.SetDoubleValue(float64(sampleCount))
		}
	}

	return md
}

func (p *windowedCountMinSketchProcessor) emitWindowAndReset() {
	md := p.buildWindowMetricsAndReset()
	if md.ResourceMetrics().Len() == 0 {
		return
	}

	if err := p.nextConsumer.ConsumeMetrics(context.Background(), md); err != nil {
		p.logger.Error("Failed to emit windowed CMS", zap.Error(err))
	}
}

//
// ─────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────
//

func serializeCMS(s *cms.CountMinSketch) ([]byte, error) {
	return s.SerializeToBytes()
}

func deserializeCMS(data []byte) (*cms.CountMinSketch, error) {
	return cms.DeserializeCountMinSketchFromBytes(data)
}

func buildAggregationKey(
	metricName string,
	attrs pcommon.Map,
) string {
	return metricName + "::" + encodeAttributesAsKey(attrs)
}

func encodeAttributesAsKey(
	attrs pcommon.Map,
) string {
	var keys []string
	attrs.Range(func(k string, _ pcommon.Value) bool {
		keys = append(keys, k)
		return true
	})
	sort.Strings(keys)

	sb := builderPool.Get().(*strings.Builder)
	sb.Reset()
	for _, k := range keys {
		v, _ := attrs.Get(k)
		sb.WriteString(k)
		sb.WriteString("=")
		sb.WriteString(v.AsString())
		sb.WriteString(";")
	}
	s := sb.String()
	builderPool.Put(sb)
	return s
}
