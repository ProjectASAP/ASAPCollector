// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ddsketchmergeprocessor

import (
	"context"
	"sort"
	"strings"
	"sync"

	ddsketch "github.com/ProjectASAP/sketchlib-go/sketches/DDSketch"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type ddsketchMergeProcessor struct {
	cfg    *Config
	logger *zap.Logger
	next   consumer.Metrics

	mu           sync.Mutex
	accumulators map[string]*ddsketch.DDSketch // keyed by attribute-set fingerprint
}

func newProcessor(cfg *Config, logger *zap.Logger, next consumer.Metrics) *ddsketchMergeProcessor {
	return &ddsketchMergeProcessor{
		cfg:          cfg,
		logger:       logger,
		next:         next,
		accumulators: make(map[string]*ddsketch.DDSketch),
	}
}

func (p *ddsketchMergeProcessor) Start(_ context.Context, _ component.Host) error {
	return nil
}

func (p *ddsketchMergeProcessor) Shutdown(_ context.Context) error {
	return nil
}

func (p *ddsketchMergeProcessor) Capabilities() consumer.Capabilities {
	// Pass-through: we do not mutate md, only update internal accumulators.
	return consumer.Capabilities{MutatesData: false}
}

func (p *ddsketchMergeProcessor) processMetrics(_ context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	metricName := p.cfg.MetricName
	if metricName == "" {
		metricName = "ddsketch"
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
				if m.Type() != pmetric.MetricTypeDDSketch {
					continue
				}
				dps := m.DDSketch().DataPoints()
				for l := 0; l < dps.Len(); l++ {
					p.mergeDataPoint(dps.At(l))
				}
			}
		}
	}

	return md, nil
}

func (p *ddsketchMergeProcessor) mergeDataPoint(dp pmetric.DDSketchDataPoint) {
	payload := dp.Sketch()
	if len(payload) == 0 {
		return
	}
	key := attrSetKey(dp.Attributes())

	p.mu.Lock()
	defer p.mu.Unlock()

	switch dp.Encoding() {
	case pmetric.DDSketchEncodingProto, pmetric.DDSketchEncodingUnspecified:
		// Full snapshot via sketchlib-go's portable proto-state encoding.
		sketch, err := ddsketch.NewFromStateProtoBytes(payload)
		if err != nil {
			p.logger.Error("ddsketchmergeprocessor: failed to deserialize DDSketch state", zap.Error(err))
			return
		}
		p.accumulators[key] = sketch
	case pmetric.DDSketchEncodingProtoDelta:
		acc, exists := p.accumulators[key]
		if !exists {
			p.logger.Warn("ddsketchmergeprocessor: received delta for unknown key; dropping",
				zap.String("key", key))
			return
		}
		if err := ddsketch.ApplyDelta(acc, payload); err != nil {
			p.logger.Error("ddsketchmergeprocessor: ApplyDelta failed", zap.Error(err))
			return
		}
	case pmetric.DDSketchEncodingMsgpack:
		sketch, err := ddsketch.DeserializeMsgpack(payload)
		if err != nil {
			p.logger.Error("ddsketchmergeprocessor: DeserializeMsgpack failed", zap.Error(err))
			return
		}
		p.accumulators[key] = sketch
	default:
		// MsgpackDelta is reserved-but-unsupported (see pdata
		// ddsketch_encoding.go). Anything else is unknown: warn and drop.
		p.logger.Warn("ddsketchmergeprocessor: unsupported DDSketch encoding; dropping data point",
			zap.String("encoding", dp.Encoding().String()),
			zap.String("key", key))
	}
}

// GetAccumulator returns the current accumulated sketch for the given
// attribute-set key.
func (p *ddsketchMergeProcessor) GetAccumulator(key string) (*ddsketch.DDSketch, bool) {
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
