// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package countminsketchmergeprocessor

import (
	"context"
	"sync"

	cms "github.com/ProjectASAP/sketchlib-go/sketches/CountMinSketch"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type countMinSketchMergeProcessor struct {
	cfg    *Config
	logger *zap.Logger
	next   consumer.Metrics

	mu           sync.Mutex
	accumulators map[string]*cms.CountMinSketch // keyed by aggregation_key
}

func newProcessor(cfg *Config, logger *zap.Logger, next consumer.Metrics) *countMinSketchMergeProcessor {
	return &countMinSketchMergeProcessor{
		cfg:          cfg,
		logger:       logger,
		next:         next,
		accumulators: make(map[string]*cms.CountMinSketch),
	}
}

func (p *countMinSketchMergeProcessor) Start(_ context.Context, _ component.Host) error {
	return nil
}

func (p *countMinSketchMergeProcessor) Shutdown(_ context.Context) error {
	return nil
}

func (p *countMinSketchMergeProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *countMinSketchMergeProcessor) processMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	metricName := p.cfg.MetricName
	if metricName == "" {
		metricName = "countmin_sketch"
	}

	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			metrics := sms.At(j).Metrics()
			for k := 0; k < metrics.Len(); k++ {
				m := metrics.At(k)
				if m.Name() != metricName {
					continue
				}
				if m.Type() != pmetric.MetricTypeGauge {
					continue
				}
				dps := m.Gauge().DataPoints()
				for l := 0; l < dps.Len(); l++ {
					dp := dps.At(l)
					p.mergeDataPoint(dp)
				}
			}
		}
	}

	return md, nil
}

func (p *countMinSketchMergeProcessor) mergeDataPoint(dp pmetric.NumberDataPoint) {
	aggKey, ok := dp.Attributes().Get("aggregation_key")
	if !ok {
		return
	}
	key := aggKey.Str()

	payloadVal, ok := dp.Attributes().Get("sketch_payload")
	if !ok {
		return
	}
	payload := payloadVal.Bytes().AsRaw()
	if len(payload) == 0 {
		return
	}

	encodingVal, _ := dp.Attributes().Get("encoding")
	encoding := encodingVal.Str()

	p.mu.Lock()
	defer p.mu.Unlock()

	switch encoding {
	case "proto_delta":
		acc, exists := p.accumulators[key]
		if !exists {
			p.logger.Warn("countminsketchmergeprocessor: received delta for unknown key; dropping",
				zap.String("key", key))
			return
		}
		if err := cms.ApplyDelta(acc, payload); err != nil {
			p.logger.Error("countminsketchmergeprocessor: ApplyDelta failed", zap.Error(err))
		}
	default:
		// proto_full or no encoding: deserialize and replace
		sketch, err := cms.DeserializeCountMinSketchFromProtoBytes(payload)
		if err != nil {
			p.logger.Error("countminsketchmergeprocessor: failed to deserialize full sketch", zap.Error(err))
			return
		}
		p.accumulators[key] = sketch
	}
}

// GetAccumulator returns the current accumulated sketch for the given key.
// Used by downstream processors or exporters to read the merged state.
func (p *countMinSketchMergeProcessor) GetAccumulator(key string) (*cms.CountMinSketch, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	acc, ok := p.accumulators[key]
	return acc, ok
}
