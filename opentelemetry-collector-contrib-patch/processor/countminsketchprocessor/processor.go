// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package countminsketchprocessor

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	cms "github.com/approx-telemetry/sketchlib-go/CountMinSketch"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// lockedSketch wraps the CMS with its own Mutex.
// This allows worker A to write to Sketch A, while worker B writes to Sketch B
// in parallel without waiting for each other (fine-grained locking).
type lockedSketch struct {
	sketch *cms.CountMinSketch
	mu     sync.Mutex
}

type countMinProcessor struct {
	cfg    *Config
	logger *zap.Logger

	// State management
	// Value is now a POINTER to lockedSketch
	sketches map[string]*lockedSketch

	// Global Mutex only protects read/write access to the MAP (p.sketches),
	// it does NOT protect the sketch update process.
	mu sync.RWMutex
}

func newProcessor(cfg *Config, logger *zap.Logger) *countMinProcessor {
	return &countMinProcessor{
		cfg:      cfg,
		logger:   logger,
		sketches: make(map[string]*lockedSketch),
	}
}

// serializableSketch is a DTO Struct for serialization (excluding the Hasher interface)
type serializableSketch struct {
	Rows  int
	Cols  int
	Seed1 []uint32
	Count [][]float64
	Sum   [][]float64
	Sum2  [][]float64
	L1    []float64
	L2    []float64
}

func (p *countMinProcessor) processMetrics(_ context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	// OPTIMIZATION: REMOVED Global Lock (p.mu.Lock()) from here!
	// This allows OTel workers to run in parallel.

	// 1. Ingestion Phase
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			metrics := sms.At(j).Metrics()
			for k := 0; k < metrics.Len(); k++ {
				p.consumeMetric(metrics.At(k))
			}
		}
	}

	// 2. Emission Phase
	if err := p.appendSketchesToMetrics(md); err != nil {
		p.logger.Error("failed to append sketches", zap.Error(err))
	}

	return md, nil
}

func (p *countMinProcessor) consumeMetric(metric pmetric.Metric) {
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		dps := metric.Gauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			p.processDataPoint(metric.Name(), dps.At(i))
		}
	case pmetric.MetricTypeSum:
		dps := metric.Sum().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			p.processDataPoint(metric.Name(), dps.At(i))
		}
	}
}

func (p *countMinProcessor) processDataPoint(metricName string, dp pmetric.NumberDataPoint) {
	// 1. Calculate Key (Heavy Computation, but safe without lock)
	groupKey := buildGroupKey(metricName, dp.Attributes())

	// 2. Access Map (Needs Global Lock only briefly)
	p.mu.RLock()
	ls, exists := p.sketches[groupKey]
	p.mu.RUnlock()

	if !exists {
		// Upgrade to Write Lock because we need to create a new sketch
		p.mu.Lock()
		// Double-check locking in case another goroutine created it during lock transition
		ls, exists = p.sketches[groupKey]
		if !exists {
			seedVal := uint32(p.cfg.Seed)
			seeds := []uint32{seedVal, seedVal + 1, seedVal + 2, seedVal + 3, seedVal + 4}

			newSketch, err := cms.NewCountMinSketch(p.cfg.Rows, p.cfg.Columns, seeds)
			if err != nil {
				p.logger.Error("failed to initialize CMS", zap.Error(err))
				p.mu.Unlock()
				return
			}

			ls = &lockedSketch{
				sketch: &newSketch,
			}
			p.sketches[groupKey] = ls
		}
		p.mu.Unlock()
	}

	// 3. Update Sketch (Needs Local Lock)
	// This only blocks other workers writing to the SAME sketch.
	// Workers writing to different sketches can proceed concurrently.
	flowKey := attributesToString(dp.Attributes())
	value := dp.DoubleValue()
	if dp.ValueType() == pmetric.NumberDataPointValueTypeInt {
		value = float64(dp.IntValue())
	}

	ls.mu.Lock()
	ls.sketch.CMProcessing(flowKey, value)
	ls.mu.Unlock()
}

func (p *countMinProcessor) appendSketchesToMetrics(md pmetric.Metrics) error {
	// For map iteration, we need a global Read Lock
	p.mu.RLock()
	if len(p.sketches) == 0 {
		p.mu.RUnlock()
		return nil
	}

	// Copy sketch references so we can release the Global Lock faster
	// (Snapshotting strategy)
	type snapshotItem struct {
		key string
		ls  *lockedSketch
	}
	items := make([]snapshotItem, 0, len(p.sketches))
	for k, v := range p.sketches {
		items = append(items, snapshotItem{key: k, ls: v})
	}
	p.mu.RUnlock() // RELEASE GLOBAL LOCK

	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("otelcol/countminprocessor")
	now := pcommon.NewTimestampFromTime(time.Now())

	for _, item := range items {
		// Lock Local Sketch during serialization so data doesn't change mid-process
		item.ls.mu.Lock()

		// Metric creation logic
		m := sm.Metrics().AppendEmpty()
		m.SetName(p.cfg.MetricName)
		m.SetUnit("1")

		gauge := m.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetTimestamp(now)
		dp.Attributes().PutStr("aggregation_key", item.key)

		// Serialize
		payload, err := serializeSketch(item.ls.sketch)

		// Metadata
		rows := int64(item.ls.sketch.Row())
		cols := int64(item.ls.sketch.Col())

		item.ls.mu.Unlock() // RELEASE LOCAL LOCK

		if err != nil {
			return fmt.Errorf("serialize error: %w", err)
		}

		dp.Attributes().PutEmptyBytes("sketch_payload").FromRaw(payload)
		dp.Attributes().PutInt("rows", rows)
		dp.Attributes().PutInt("cols", cols)
	}
	return nil
}

func serializeSketch(s *cms.CountMinSketch) ([]byte, error) {
	// Using Public Fields (Getters are no longer mandatory since fields are public)
	snapshot := serializableSketch{
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

// --- Helper Functions ---

func buildGroupKey(name string, attrs pcommon.Map) string {
	return name + "::" + attributesToString(attrs)
}

func attributesToString(attrs pcommon.Map) string {
	var sb strings.Builder
	var keys []string
	attrs.Range(func(k string, _ pcommon.Value) bool {
		keys = append(keys, k)
		return true
	})
	sort.Strings(keys)

	for _, k := range keys {
		v, _ := attrs.Get(k)
		sb.WriteString(k)
		sb.WriteString("=")
		sb.WriteString(v.AsString())
		sb.WriteString(";")
	}
	return sb.String()
}
