// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package countsketchmergeprocessor

import (
	"context"
	"sync"

	cs "github.com/ProjectASAP/sketchlib-go/sketches/CountSketch"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type countSketchMergeProcessor struct {
	cfg    *Config
	logger *zap.Logger
	next   consumer.Metrics

	mu           sync.Mutex
	accumulators map[string]*cs.CountSketch // keyed by partition_key
}

func newProcessor(cfg *Config, logger *zap.Logger, next consumer.Metrics) *countSketchMergeProcessor {
	return &countSketchMergeProcessor{
		cfg:          cfg,
		logger:       logger,
		next:         next,
		accumulators: make(map[string]*cs.CountSketch),
	}
}

func (p *countSketchMergeProcessor) Start(_ context.Context, _ component.Host) error {
	return nil
}

func (p *countSketchMergeProcessor) Shutdown(_ context.Context) error {
	return nil
}

func (p *countSketchMergeProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *countSketchMergeProcessor) processMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	metricName := p.cfg.MetricName
	if metricName == "" {
		metricName = "countsketch_partition"
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

func (p *countSketchMergeProcessor) mergeDataPoint(dp pmetric.NumberDataPoint) {
	partKeyVal, ok := dp.Attributes().Get("partition_key")
	if !ok {
		return
	}
	key := partKeyVal.Str()

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
			p.logger.Warn("countsketchmergeprocessor: received delta for unknown partition; dropping",
				zap.String("key", key))
			return
		}
		if err := cs.ApplyDelta(acc, payload); err != nil {
			p.logger.Error("countsketchmergeprocessor: ApplyDelta failed", zap.Error(err))
		}
	default:
		// proto_full or no encoding: deserialize and replace
		sketch, err := cs.DeserializeCountSketchFromBytes(payload)
		if err != nil {
			p.logger.Error("countsketchmergeprocessor: failed to deserialize full sketch", zap.Error(err))
			return
		}
		p.accumulators[key] = sketch
	}
}

// GetAccumulator returns the current accumulated sketch for the given partition key.
func (p *countSketchMergeProcessor) GetAccumulator(key string) (*cs.CountSketch, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	acc, ok := p.accumulators[key]
	return acc, ok
}
