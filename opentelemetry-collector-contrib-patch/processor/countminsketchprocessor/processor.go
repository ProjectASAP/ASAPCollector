// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package countminsketchprocessor

import (
	"bytes"
	"context"
	"encoding/gob"
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

//
// ─────────────────────────────────────────────────────────────
// Window-level state
// ─────────────────────────────────────────────────────────────
//

type windowSketch struct {
	cms         *cms.CountMinSketch
	mu          sync.Mutex
	sampleCount uint64
}

// countMinSketchSnapshot is a serializable DTO.
// NOTE: Seed field is removed as new lib manages seeds internally.
type countMinSketchSnapshot struct {
	Rows  int
	Cols  int
	Count [][]float64
	Sum   [][]float64
	Sum2  [][]float64
	L1    []float64
	L2    []float64
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
}

func newProcessor(
	cfg *Config,
	next consumer.Metrics,
	logger *zap.Logger,
) *windowedCountMinSketchProcessor {
	return &windowedCountMinSketchProcessor{
		cfg:                  cfg,
		logger:               logger,
		nextConsumer:         next,
		activeWindowSketches: make(map[string]*windowSketch),
		stopCh:               make(chan struct{}),
		doneCh:               make(chan struct{}),
	}
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
		zap.Duration("window_interval", p.cfg.WindowInterval),
	)

	// Batch mode does not require a background ticker; we flush per-batch.
	if p.cfg.Mode != ModeWindow {
		return nil
	}

	// Window mode: start background window loop if a positive window is configured.
	if p.cfg.WindowInterval <= 0 {
		return nil
	}

	ticker := time.NewTicker(p.cfg.WindowInterval)
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
			p.updateWindowSketch(metric.Name(), dps.At(i))
		}
	case pmetric.MetricTypeSum:
		dps := metric.Sum().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			p.updateWindowSketch(metric.Name(), dps.At(i))
		}
	case pmetric.MetricTypeCountMinSketch:
		// Pre-aggregated path: deserialize and merge each incoming sketch into
		// the corresponding per-aggregation-key window sketch.
		dps := metric.CountMinSketch().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			if len(dp.Sketch()) == 0 {
				continue
			}
			incoming, err := deserializeCMS(dp.Sketch())
			if err != nil {
				p.logger.Error("countminsketchprocessor: failed to deserialize CountMinSketch dp", zap.Error(err))
				continue
			}
			aggregationKey := buildAggregationKey(metric.Name(), dp.Attributes())
			p.mergeWindowSketch(aggregationKey, incoming)
		}
	}
}

// mergeWindowSketch merges an incoming pre-aggregated CMS into the per-key window store.
func (p *windowedCountMinSketchProcessor) mergeWindowSketch(aggregationKey string, incoming *cms.CountMinSketch) {
	p.mu.RLock()
	ws, exists := p.activeWindowSketches[aggregationKey]
	p.mu.RUnlock()

	if !exists {
		p.mu.Lock()
		ws, exists = p.activeWindowSketches[aggregationKey]
		if !exists {
			newCMS, err := cms.NewCountMinSketch(incoming.Rows, incoming.Cols)
			if err != nil {
				p.logger.Error("Failed to create CMS for merge", zap.Error(err))
				p.mu.Unlock()
				return
			}
			ws = &windowSketch{cms: newCMS}
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
	aggregationKey := buildAggregationKey(metricName, dp.Attributes())

	p.mu.RLock()
	ws, exists := p.activeWindowSketches[aggregationKey]
	p.mu.RUnlock()

	if !exists {
		p.mu.Lock()
		ws, exists = p.activeWindowSketches[aggregationKey]
		if !exists {
			// Using new API constructor (without seed)
			newCMS, err := cms.NewCountMinSketch(
				p.cfg.Rows,
				p.cfg.Columns,
			)
			if err != nil {
				p.logger.Error("Failed to create CMS", zap.Error(err))
				p.mu.Unlock()
				return
			}

			ws = &windowSketch{cms: newCMS}
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

	for aggregationKey, ws := range windowSnapshot {
		ws.mu.Lock()
		rows := ws.cms.Rows
		cols := ws.cms.Cols
		sampleCount := ws.sampleCount
		payload, err := serializeCMS(ws.cms)
		ws.mu.Unlock()
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

		dp.Attributes().PutStr("aggregation_key", aggregationKey)
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

func serializeCMS(
	s *cms.CountMinSketch,
) ([]byte, error) {

	snapshot := countMinSketchSnapshot{
		Rows:  s.Rows,
		Cols:  s.Cols,
		Count: s.Count,
		Sum:   s.Sum,
		Sum2:  s.Sum2,
		L1:    s.L1,
		L2:    s.L2,
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(snapshot); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// deserializeCMS reconstructs a CountMinSketch from the gob-encoded snapshot
// format used by both the SDK aggregate and this processor's serializeCMS.
func deserializeCMS(data []byte) (*cms.CountMinSketch, error) {
	var snap countMinSketchSnapshot
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&snap); err != nil {
		return nil, err
	}
	s, err := cms.NewCountMinSketch(snap.Rows, snap.Cols)
	if err != nil {
		return nil, err
	}
	for r := 0; r < snap.Rows && r < len(snap.Count); r++ {
		for c := 0; c < snap.Cols && c < len(snap.Count[r]); c++ {
			s.Count[r][c] = snap.Count[r][c]
			s.Sum[r][c] = snap.Sum[r][c]
			s.Sum2[r][c] = snap.Sum2[r][c]
		}
	}
	for r := 0; r < snap.Rows && r < len(snap.L1); r++ {
		s.L1[r] = snap.L1[r]
		s.L2[r] = snap.L2[r]
	}
	return s, nil
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

	var sb strings.Builder
	for _, k := range keys {
		v, _ := attrs.Get(k)
		sb.WriteString(k)
		sb.WriteString("=")
		sb.WriteString(v.AsString())
		sb.WriteString(";")
	}
	return sb.String()
}
