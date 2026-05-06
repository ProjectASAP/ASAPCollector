// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package gorillas3processor

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type gorillaS3Processor struct {
	cfg          *Config
	logger       *zap.Logger
	nextConsumer consumer.Metrics

	window *windowState
	sink   chunkSink

	ticker  *time.Ticker
	done    chan struct{}
	wg      sync.WaitGroup
	monitor *monitor
}

func newProcessor(cfg *Config, next consumer.Metrics, logger *zap.Logger, sink chunkSink) *gorillaS3Processor {
	return &gorillaS3Processor{
		cfg:          cfg,
		logger:       logger,
		nextConsumer: next,
		window:       newWindowState(),
		sink:         sink,
		done:         make(chan struct{}),
	}
}

// Start initializes the sink (if not pre-injected) and launches the
// tumbling-window flush goroutine.
func (p *gorillaS3Processor) Start(ctx context.Context, _ component.Host) error {
	if p.sink == nil {
		s, err := newS3Sink(p.cfg)
		if err != nil {
			return err
		}
		p.sink = s
	}
	p.logger.Info("Starting gorillas3 processor",
		zap.Duration("window_interval", p.cfg.WindowInterval),
		zap.String("bucket", p.cfg.Bucket),
		zap.String("endpoint", p.cfg.Endpoint),
		zap.String("prefix_template", p.cfg.PrefixTemplate),
		zap.Bool("drop_original", p.cfg.DropOriginal),
	)
	p.ticker = time.NewTicker(p.cfg.WindowInterval)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			select {
			case <-p.ticker.C:
				p.flushWindow(context.Background())
			case <-p.done:
				return
			}
		}
	}()
	return nil
}

// Shutdown stops the ticker, drains in-flight buffers and closes the sink.
func (p *gorillaS3Processor) Shutdown(ctx context.Context) error {
	if p.ticker != nil {
		p.ticker.Stop()
	}
	close(p.done)
	p.wg.Wait()
	p.flushWindow(ctx)
	if p.monitor != nil {
		p.monitor.shutdown()
	}
	if p.sink != nil {
		_ = p.sink.Close()
	}
	return nil
}

// Capabilities reports whether this processor mutates the data passed in.
// We may swap the metrics out (drop_original) but we do not mutate the
// caller's pmetric.Metrics in place — return MutatesData=true defensively
// because we still touch attributes via Range and may emit a fresh Metrics.
func (p *gorillaS3Processor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

// ConsumeMetrics ingests gauge/sum data points into the window buffer,
// then forwards downstream (or drops, per config).
func (p *gorillaS3Processor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	if p.monitor != nil {
		p.monitor.recordInput(ctx, md)
	}
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
	if p.cfg.DropOriginal {
		empty := pmetric.NewMetrics()
		if p.monitor != nil {
			p.monitor.recordOutput(ctx, empty)
		}
		return empty, nil
	}
	if p.monitor != nil {
		p.monitor.recordOutput(ctx, md)
	}
	return md, nil
}

func (p *gorillaS3Processor) ingestMetric(m pmetric.Metric) {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		dps := m.Gauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			v, ok := numberValue(dp)
			if !ok {
				continue
			}
			p.window.add(m.Name(), dp.Attributes(), dp.Timestamp().AsTime(), v)
		}
	case pmetric.MetricTypeSum:
		dps := m.Sum().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			v, ok := numberValue(dp)
			if !ok {
				continue
			}
			p.window.add(m.Name(), dp.Attributes(), dp.Timestamp().AsTime(), v)
		}
	}
}

func numberValue(dp pmetric.NumberDataPoint) (float64, bool) {
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeDouble:
		return dp.DoubleValue(), true
	case pmetric.NumberDataPointValueTypeInt:
		return float64(dp.IntValue()), true
	}
	return 0, false
}

// flushWindow encodes the buffered window into chunks and sends them
// to the sink. Errors per-chunk are logged but do not abort the flush.
func (p *gorillaS3Processor) flushWindow(ctx context.Context) {
	snapshot, _, latest := p.window.snapshot()
	if len(snapshot) == 0 {
		return
	}
	chunks, err := buildChunks(snapshot, p.cfg.MaxObjectBytes)
	if err != nil {
		p.logger.Error("gorillas3: build chunks failed", zap.Error(err))
		return
	}
	blockTime := latest
	if blockTime.IsZero() {
		blockTime = time.Now().UTC()
	}
	for idx, c := range chunks {
		prefix := renderPrefix(p.cfg.PrefixTemplate, p.cfg.Tenant, c.metricName, blockTime)
		key := buildObjectKey(prefix, blockTime, idx)
		hints := chunkHints{
			Tenant:      p.cfg.Tenant,
			MetricName:  c.metricName,
			StartTSNano: c.startTS,
			EndTSNano:   c.endTS,
			SeriesCount: c.seriesCount,
			PointCount:  c.pointCount,
			SizeBytes:   len(c.data),
			IndexPrefix: prefix,
		}
		if err := p.sink.PutChunk(ctx, key, c.data, hints); err != nil {
			p.logger.Error("gorillas3: PutChunk failed",
				zap.String("key", key),
				zap.Int("series", c.seriesCount),
				zap.Int("points", c.pointCount),
				zap.Error(err),
			)
			if p.monitor != nil {
				p.monitor.putFailure(ctx)
			}
			continue
		}
		if p.monitor != nil {
			p.monitor.chunkWritten(ctx, len(c.data), c.pointCount)
		}
		p.logger.Info("gorillas3: chunk written",
			zap.String("key", key),
			zap.String("metric", c.metricName),
			zap.Int("series", c.seriesCount),
			zap.Int("points", c.pointCount),
			zap.Int("bytes", len(c.data)),
		)
	}
}

// activeSeries is exposed to selfmonitor as the gauge callback.
func (p *gorillaS3Processor) activeSeries() int64 {
	return p.window.activeSeries()
}
