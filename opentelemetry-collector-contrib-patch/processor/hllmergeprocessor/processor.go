// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package hllmergeprocessor

import (
	"context"
	"sort"
	"strings"
	"sync"

	hll "github.com/ProjectASAP/sketchlib-go/sketches/HLL"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type hllMergeProcessor struct {
	cfg    *Config
	logger *zap.Logger
	next   consumer.Metrics

	mu           sync.Mutex
	accumulators map[string]*hll.HyperLogLog // keyed by attribute-set fingerprint
}

func newProcessor(cfg *Config, logger *zap.Logger, next consumer.Metrics) *hllMergeProcessor {
	return &hllMergeProcessor{
		cfg:          cfg,
		logger:       logger,
		next:         next,
		accumulators: make(map[string]*hll.HyperLogLog),
	}
}

func (p *hllMergeProcessor) Start(_ context.Context, _ component.Host) error {
	return nil
}

func (p *hllMergeProcessor) Shutdown(_ context.Context) error {
	return nil
}

func (p *hllMergeProcessor) Capabilities() consumer.Capabilities {
	// Pass-through: we do not mutate md, only update internal accumulators.
	return consumer.Capabilities{MutatesData: false}
}

func (p *hllMergeProcessor) processMetrics(_ context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	metricName := p.cfg.MetricName
	if metricName == "" {
		metricName = "hll_sketch"
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
				if m.Type() != pmetric.MetricTypeHLLSketch {
					continue
				}
				dps := m.HLLSketch().DataPoints()
				for l := 0; l < dps.Len(); l++ {
					p.mergeDataPoint(dps.At(l))
				}
			}
		}
	}

	return md, nil
}

func (p *hllMergeProcessor) mergeDataPoint(dp pmetric.HLLSketchDataPoint) {
	payload := dp.Sketch()
	if len(payload) == 0 {
		return
	}
	key := attrSetKey(dp.Attributes())

	p.mu.Lock()
	defer p.mu.Unlock()

	switch dp.Encoding() {
	case pmetric.HLLSketchEncodingProto, pmetric.HLLSketchEncodingUnspecified:
		// Full snapshot via sketchlib-go's portable proto encoding.
		sketch, err := hll.DeserializeHyperLogLogFromProtoBytes(payload)
		if err != nil {
			p.logger.Error("hllmergeprocessor: failed to deserialize HLL sketch", zap.Error(err))
			return
		}
		p.accumulators[key] = sketch
	case pmetric.HLLSketchEncodingDelta:
		acc, exists := p.accumulators[key]
		if !exists {
			p.logger.Warn("hllmergeprocessor: received delta for unknown key; dropping",
				zap.String("key", key))
			return
		}
		delta, err := hll.DeserializeRegisterDelta(payload)
		if err != nil {
			p.logger.Error("hllmergeprocessor: DeserializeRegisterDelta failed", zap.Error(err))
			return
		}
		hll.ApplyRegisterDelta(acc, delta)
	case pmetric.HLLSketchEncodingMsgpack:
		sketch, err := hll.DeserializeMsgpack(payload)
		if err != nil {
			p.logger.Error("hllmergeprocessor: DeserializeMsgpack failed", zap.Error(err))
			return
		}
		p.accumulators[key] = sketch
	default:
		// MsgpackDelta is reserved-but-unsupported (see pdata
		// hllsketch_encoding.go). Anything else: warn and drop.
		p.logger.Warn("hllmergeprocessor: unsupported HLL encoding; dropping data point",
			zap.String("encoding", dp.Encoding().String()),
			zap.String("key", key))
	}
}

// GetAccumulator returns the current accumulated sketch for the given
// attribute-set key.
func (p *hllMergeProcessor) GetAccumulator(key string) (*hll.HyperLogLog, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	acc, ok := p.accumulators[key]
	return acc, ok
}

// attrSetKey produces a stable string key for a pdata attribute map by
// sorting attribute names and joining `name=value` pairs.
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
