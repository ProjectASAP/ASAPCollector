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

	// Import library baru
	"github.com/approx-telemetry/sketchlib-go/common"
	cms "github.com/approx-telemetry/sketchlib-go/sketches/CountMinSketch"

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
		"Starting windowed Count-Min Sketch processor (New API)",
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

	// 1. Dapatkan Lock Read
	p.mu.RLock()
	ws, exists := p.activeWindowSketches[aggregationKey]
	p.mu.RUnlock()

	// 2. Init Sketch jika belum ada
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

	// 3. Update Sketch
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
