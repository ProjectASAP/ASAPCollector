// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kllmergeprocessor

import (
	"context"
	"sort"
	"strings"
	"sync"

	kll "github.com/ProjectASAP/sketchlib-go/sketches/KLL"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type kllMergeProcessor struct {
	cfg    *Config
	logger *zap.Logger
	next   consumer.Metrics

	mu           sync.Mutex
	accumulators map[string]*kll.KLLSketch // keyed by attribute-set fingerprint
}

func newProcessor(cfg *Config, logger *zap.Logger, next consumer.Metrics) *kllMergeProcessor {
	return &kllMergeProcessor{
		cfg:          cfg,
		logger:       logger,
		next:         next,
		accumulators: make(map[string]*kll.KLLSketch),
	}
}

func (p *kllMergeProcessor) Start(_ context.Context, _ component.Host) error {
	return nil
}

func (p *kllMergeProcessor) Shutdown(_ context.Context) error {
	return nil
}

func (p *kllMergeProcessor) Capabilities() consumer.Capabilities {
	// Pass-through: we do not mutate md, only update internal accumulators.
	return consumer.Capabilities{MutatesData: false}
}

func (p *kllMergeProcessor) processMetrics(_ context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	metricName := p.cfg.MetricName
	if metricName == "" {
		metricName = "kll_sketch"
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
				if m.Type() != pmetric.MetricTypeKLLSketch {
					continue
				}
				dps := m.KLLSketch().DataPoints()
				for l := 0; l < dps.Len(); l++ {
					p.mergeDataPoint(dps.At(l))
				}
			}
		}
	}

	return md, nil
}

func (p *kllMergeProcessor) mergeDataPoint(dp pmetric.KLLSketchDataPoint) {
	payload := dp.Sketch()
	if len(payload) == 0 {
		return
	}
	key := attrSetKey(dp.Attributes())

	p.mu.Lock()
	defer p.mu.Unlock()

	switch dp.Encoding() {
	case pmetric.KLLSketchEncodingProto, pmetric.KLLSketchEncodingUnspecified:
		// Full snapshot: deserialize and replace.
		sketch, err := kll.DeserializeKLLSketchFromProtoBytes(payload)
		if err != nil {
			p.logger.Error("kllmergeprocessor: failed to deserialize KLL sketch", zap.Error(err))
			return
		}
		p.accumulators[key] = sketch
	default:
		// KLL has no shared msgpack/delta wire format end-to-end (see
		// pdata kllsketch_encoding.go); treat anything else as a
		// configuration mismatch and warn.
		p.logger.Warn("kllmergeprocessor: unsupported KLL encoding; dropping data point",
			zap.String("encoding", dp.Encoding().String()),
			zap.String("key", key))
	}
}

// GetAccumulator returns the current accumulated sketch for the given
// attribute-set key.
func (p *kllMergeProcessor) GetAccumulator(key string) (*kll.KLLSketch, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	acc, ok := p.accumulators[key]
	return acc, ok
}

// attrSetKey produces a stable string key for a pdata attribute map by
// sorting attribute names and joining `name=value` pairs. The exact
// format is private to this processor — it is only used as a map key
// and round-trip serialization is not required.
func attrSetKey(attrs pcommon.Map) string {
	if attrs.Len() == 0 {
		return ""
	}
	keys := make([]string, 0, attrs.Len())
	values := make(map[string]string, attrs.Len())
	attrs.Range(func(k string, v pcommon.Value) bool {
		keys = append(keys, k)
		values[k] = v.AsString()
		return true
	})
	sort.Strings(keys)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(values[k])
	}
	return b.String()
}
