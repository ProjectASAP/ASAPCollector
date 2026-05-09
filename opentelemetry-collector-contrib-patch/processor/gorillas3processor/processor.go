// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package gorillas3processor

import (
	"context"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
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

	// mvp/step2.1: lazily-constructed Prometheus TSDB block builder.
	// Nil when block_format=asap; non-nil when prometheus_tsdb|both.
	tsdbBuilder *tsdbBlockBuilder

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
	if p.cfg.EmitTSDB() && p.tsdbBuilder == nil {
		p.tsdbBuilder = newTSDBBlockBuilder(
			p.cfg.TSDBBlockDuration,
			p.cfg.TSDBExternalLabels,
			zapToSlog(p.logger),
		)
	}
	p.logger.Info("Starting gorillas3 processor",
		zap.Duration("window_interval", p.cfg.WindowInterval),
		zap.String("bucket", p.cfg.Bucket),
		zap.String("tsdb_bucket", p.cfg.TSDBBucket),
		zap.String("endpoint", p.cfg.Endpoint),
		zap.String("prefix_template", p.cfg.PrefixTemplate),
		zap.Bool("drop_original", p.cfg.DropOriginal),
		zap.String("block_format", string(p.cfg.BlockFormat)),
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
//
// mvp/step2.1: dispatches to one or both of:
//   - the legacy ASAP GORILLA1 chunk + index.json + postings-v1.json
//     layout (block_format=asap or both)
//   - a Prometheus TSDB block under <tsdb-bucket>/<ulid>/ (block_format
//     =prometheus_tsdb or both)
//
// Both paths consume the same in-memory snapshot. The ASAP path adapts
// into asap-gorilla-go, and the TSDB path makes its own defensive copy
// before sorting, so neither path mutates the other's point order.
func (p *gorillaS3Processor) flushWindow(ctx context.Context) {
	snapshot, _, latest := p.window.snapshot()
	if len(snapshot) == 0 {
		return
	}
	blockTime := latest
	if blockTime.IsZero() {
		blockTime = time.Now().UTC()
	}
	if p.cfg.EmitASAP() {
		p.flushASAP(ctx, snapshot, blockTime)
	}
	if p.cfg.EmitTSDB() {
		p.flushTSDB(ctx, snapshot)
	}
}

// flushASAP emits the legacy GORILLA1 chunk + index.json + postings-v1.json
// triplet for the supplied window. Behaviour is byte-identical to the
// pre-step2.1 flushWindow.
func (p *gorillaS3Processor) flushASAP(ctx context.Context, snapshot map[seriesKey]*seriesBuffer, blockTime time.Time) {
	chunks, err := buildChunks(snapshot, p.cfg.MaxObjectBytes)
	if err != nil {
		p.logger.Error("gorillas3: build chunks failed", zap.Error(err))
		return
	}
	// mvp/v5: precompute postings once for the whole window. The
	// agent emits ONE `postings-v1.json` per (metric, hour-bucket)
	// because all chunks landing under the same prefix share the
	// same postings file. We bucket by prefix so multi-metric
	// windows still get their own postings sidecars.
	postingsByPrefix := make(map[string]map[seriesKey]*seriesBuffer)
	for sk, buf := range snapshot {
		if buf == nil || len(buf.points) == 0 {
			continue
		}
		prefix := renderPrefix(p.cfg.PrefixTemplate, p.cfg.Tenant, sk.metricName, blockTime)
		bucket, ok := postingsByPrefix[prefix]
		if !ok {
			bucket = make(map[seriesKey]*seriesBuffer)
			postingsByPrefix[prefix] = bucket
		}
		bucket[sk] = buf
	}

	for idx, c := range chunks {
		prefix := renderPrefix(p.cfg.PrefixTemplate, p.cfg.Tenant, c.metricName, blockTime)
		key := buildObjectKey(prefix, blockTime, idx)
		// canonical hash of the (metric, sorted-attrs) tuple — same
		// hash buildPostings uses for the per-series id, so the
		// backend can join postings → index entries on label_hash.
		// For multi-series chunks this is not single-valued; we
		// surface 0 ("multi-series") in that case so the backend
		// only short-circuits on single-series chunks.
		var labelHash uint64
		if c.seriesCount == 1 {
			for sk, buf := range snapshot {
				if sk.metricName == c.metricName && buf != nil {
					labelHash = canonicalLabelHash(sk.metricName, buf.attributes)
					break
				}
			}
		}
		hints := chunkHints{
			Tenant:      p.cfg.Tenant,
			MetricName:  c.metricName,
			StartTSNano: c.startTS,
			EndTSNano:   c.endTS,
			SeriesCount: c.seriesCount,
			PointCount:  c.pointCount,
			SizeBytes:   len(c.data),
			IndexPrefix: prefix,
			LabelHash:   labelHash,
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

	// mvp/v5: emit `postings-v1.json` per prefix bucket.
	for prefix, bucket := range postingsByPrefix {
		body, err := buildPostings(bucket, time.Now().UnixNano())
		if err != nil {
			p.logger.Error("gorillas3: buildPostings failed",
				zap.String("prefix", prefix),
				zap.Error(err))
			continue
		}
		postingsKey := prefix + "postings-v1.json"
		if err := p.sink.PutPostings(ctx, postingsKey, body); err != nil {
			p.logger.Error("gorillas3: PutPostings failed",
				zap.String("key", postingsKey),
				zap.Error(err))
			continue
		}
		p.logger.Info("gorillas3: postings written",
			zap.String("key", postingsKey),
			zap.Int("bytes", len(body)),
			zap.Int("series", len(bucket)))
	}
}

// flushTSDB builds a Prometheus TSDB block from the supplied window
// snapshot and uploads its files to the TSDB bucket. Errors are
// logged; the caller continues. mvp/step2.1.
func (p *gorillaS3Processor) flushTSDB(ctx context.Context, snapshot map[seriesKey]*seriesBuffer) {
	if p.tsdbBuilder == nil {
		// Tests call flushWindow without going through Start; fall
		// back to constructing a builder lazily.
		p.tsdbBuilder = newTSDBBlockBuilder(
			p.cfg.TSDBBlockDuration,
			p.cfg.TSDBExternalLabels,
			zapToSlog(p.logger),
		)
	}
	artifact, err := p.tsdbBuilder.build(ctx, snapshot)
	if err != nil {
		p.logger.Error("gorillas3: tsdb block build failed", zap.Error(err))
		if p.monitor != nil {
			p.monitor.putFailure(ctx)
		}
		return
	}
	if artifact == nil {
		return
	}
	// mvp/issue46: surface OOB-dropped sample counts on every flush.
	// We warn (not error) so operators see drift but the agent stays
	// up; the counter feeds the same metric exporter the rest of the
	// processor counters use.
	if artifact.NumOOBDropped > 0 {
		p.logger.Warn("gorillas3: tsdb out-of-bounds samples dropped",
			zap.Uint64("dropped", artifact.NumOOBDropped),
			zap.Uint64("appended", artifact.NumSamples),
		)
		if p.monitor != nil {
			p.monitor.tsdbOOBDropped(ctx, artifact.NumOOBDropped)
		}
	}
	if artifact.ULID == (ulid.ULID{}) {
		// Pure drop-only artifact (every sample tripped OOB and
		// no block was produced). Counter already incremented
		// above; nothing to upload.
		return
	}
	ulidStr := artifact.ULID.String()
	if err := p.sink.PutTSDBBlock(ctx, ulidStr, artifact.Files); err != nil {
		p.logger.Error("gorillas3: tsdb block upload failed",
			zap.String("block", ulidStr),
			zap.Uint64("series", artifact.NumSeries),
			zap.Uint64("samples", artifact.NumSamples),
			zap.Error(err),
		)
		if p.monitor != nil {
			p.monitor.putFailure(ctx)
		}
		return
	}
	var totalBytes int
	for _, b := range artifact.Files {
		totalBytes += len(b)
	}
	if p.monitor != nil {
		// Reuse chunkWritten as the most apt counter — a TSDB
		// block is the moral equivalent of a flushed chunk in
		// the new layout. Step 2.x can split if we need
		// distinct counters.
		p.monitor.chunkWritten(ctx, totalBytes, int(artifact.NumSamples))
	}
	p.logger.Info("gorillas3: tsdb block written",
		zap.String("block", ulidStr),
		zap.Int64("min_time_ms", artifact.MinTime),
		zap.Int64("max_time_ms", artifact.MaxTime),
		zap.Uint64("series", artifact.NumSeries),
		zap.Uint64("samples", artifact.NumSamples),
		zap.Int("bytes", totalBytes),
		zap.Int("files", len(artifact.Files)),
	)
}

// activeSeries is exposed to selfmonitor as the gauge callback.
func (p *gorillaS3Processor) activeSeries() int64 {
	return p.window.activeSeries()
}
