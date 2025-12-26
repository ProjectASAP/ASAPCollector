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
	"time"

	cms "github.com/approx-telemetry/sketchlib-go/CountMinSketch"

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

// windowSketch represents the Count-Min Sketch state
// for a single aggregation key within ONE tumbling window.
type windowSketch struct {
	cms         *cms.CountMinSketch
	mu          sync.Mutex // Fine-grained lock per sketch
	sampleCount uint64
}

// countMinSketchSnapshot is a serializable DTO
// used to export CMS state via OTLP.
type countMinSketchSnapshot struct {
	Rows  int
	Cols  int
	Seed1 []uint32
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

// windowedCountMinSketchProcessor implements a
// processing-time tumbling window CMS processor.
type windowedCountMinSketchProcessor struct {
	cfg    *Config
	logger *zap.Logger

	// Downstream consumer for emitting aggregated metrics
	nextConsumer consumer.Metrics

	// Active sketches for the CURRENT window only
	activeWindowSketches map[string]*windowSketch
	mu                   sync.RWMutex

	// Window lifecycle control
	ticker *time.Ticker
	done   chan struct{}
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
		done:                 make(chan struct{}),
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
		"Starting windowed Count-Min Sketch processor",
		zap.Duration("window_interval", p.cfg.WindowInterval),
	)

	p.ticker = time.NewTicker(p.cfg.WindowInterval)

	go func() {
		for {
			select {
			case <-p.ticker.C:
				p.emitWindowAndReset()
			case <-p.done:
				return
			}
		}
	}()

	return nil
}

func (p *windowedCountMinSketchProcessor) Shutdown(
	ctx context.Context,
) error {
	if p.ticker != nil {
		p.ticker.Stop()
	}
	close(p.done)
	return nil
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

	if !p.cfg.DropOriginal {
		return md, nil
	}

	return pmetric.NewMetrics(), nil
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
	}
}

func (p *windowedCountMinSketchProcessor) updateWindowSketch(
	metricName string,
	dp pmetric.NumberDataPoint,
) {
	aggregationKey := buildAggregationKey(metricName, dp.Attributes())

	// Fast path: read lock
	p.mu.RLock()
	ws, exists := p.activeWindowSketches[aggregationKey]
	p.mu.RUnlock()

	// Create sketch if needed
	if !exists {
		p.mu.Lock()
		ws, exists = p.activeWindowSketches[aggregationKey]
		if !exists {
			seed := uint32(p.cfg.Seed)
			seeds := []uint32{seed, seed + 1, seed + 2, seed + 3, seed + 4}

			newCMS, err := cms.NewCountMinSketch(
				p.cfg.Rows,
				p.cfg.Columns,
				seeds,
			)
			if err != nil {
				p.logger.Error("Failed to create CMS", zap.Error(err))
				p.mu.Unlock()
				return
			}

			ws = &windowSketch{cms: &newCMS}
			p.activeWindowSketches[aggregationKey] = ws
		}
		p.mu.Unlock()
	}

	// Update sketch (fine-grained lock)
	ws.mu.Lock()

	value := dp.DoubleValue()
	if dp.ValueType() == pmetric.NumberDataPointValueTypeInt {
		value = float64(dp.IntValue())
	}

	flowKey := encodeAttributesAsKey(dp.Attributes())
	ws.cms.CMProcessing(flowKey, value)
	ws.sampleCount++

	ws.mu.Unlock()
}

//
// ─────────────────────────────────────────────────────────────
// Emission phase (tumbling window boundary)
// ─────────────────────────────────────────────────────────────
//

func (p *windowedCountMinSketchProcessor) emitWindowAndReset() {
	p.mu.Lock()
	if len(p.activeWindowSketches) == 0 {
		p.mu.Unlock()
		return
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
		m := sm.Metrics().AppendEmpty()
		m.SetName(p.cfg.MetricName)
		m.SetUnit("1")

		gauge := m.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetTimestamp(now)

		dp.Attributes().PutStr("aggregation_key", aggregationKey)
		dp.Attributes().PutInt("rows", int64(ws.cms.Rows))
		dp.Attributes().PutInt("cols", int64(ws.cms.Cols))
		dp.Attributes().PutInt("sample_count", int64(ws.sampleCount))

		payload, err := serializeCMS(ws.cms)
		if err != nil {
			p.logger.Error("Failed to serialize CMS", zap.Error(err))
			continue
		}

		dp.Attributes().PutEmptyBytes("sketch_payload").FromRaw(payload)
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
		Seed1: s.Seed1,
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
