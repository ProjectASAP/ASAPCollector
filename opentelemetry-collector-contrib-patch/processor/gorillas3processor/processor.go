// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package gorillas3processor

import (
	"context"
	"sync"
	"time"

	gorilla "github.com/ProjectASAP/asap-gorilla-go"
	"github.com/oklog/ulid/v2"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type gorillaS3Processor struct {
	cfg          *Config
	logger       *zap.Logger
	nextConsumer consumer.Metrics

	mu sync.Mutex

	sink chunkSink

	rawBuilder        *gorilla.StreamingTSDBBlockBuilder
	fragmentEncoder   *gorilla.StreamingFragmentEncoder
	fragmentFinalizer *gorilla.FragmentBlockFinalizer

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
		sink:         sink,
		done:         make(chan struct{}),
	}
}

// Start initializes the sink (if not pre-injected) and launches the
// tumbling-window flush goroutine.
func (p *gorillaS3Processor) Start(ctx context.Context, _ component.Host) error {
	if p.cfg.Role != ProcessorRoleAgent && p.sink == nil {
		if p.cfg.ShipEndpoint != "" {
			s, err := newMergerSink(p.cfg)
			if err != nil {
				return err
			}
			p.sink = s
		} else {
			s, err := newS3Sink(p.cfg)
			if err != nil {
				return err
			}
			p.sink = s
		}
	}
	p.mu.Lock()
	err := p.ensureRoleStateLocked()
	p.mu.Unlock()
	if err != nil {
		return err
	}
	p.logger.Info("Starting gorillas3 processor",
		zap.String("role", string(p.cfg.Role)),
		zap.String("delivery_mode", string(p.cfg.DeliveryMode)),
		zap.Duration("window_interval", p.cfg.WindowInterval),
		zap.String("tsdb_bucket", p.cfg.TSDBBucket),
		zap.String("endpoint", p.cfg.Endpoint),
		zap.Bool("drop_original", p.cfg.DropOriginal),
		zap.Duration("reorder_grace", p.cfg.TSDBReorderGrace),
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

	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.ensureRoleStateLocked(); err != nil {
		return md, err
	}

	var out pmetric.Metrics
	var err error
	switch p.cfg.Role {
	case ProcessorRoleAgent:
		out, err = p.consumeAgentLocked(md)
	case ProcessorRoleGatewayFragment:
		out, err = p.consumeGatewayFragmentLocked(md)
	default:
		out, err = p.consumeGatewayRawLocked(md)
	}
	if err != nil {
		return md, err
	}
	if p.monitor != nil {
		p.monitor.recordOutput(ctx, out)
	}
	return out, nil
}

func (p *gorillaS3Processor) consumeGatewayRawLocked(md pmetric.Metrics) (pmetric.Metrics, error) {
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			metrics := sms.At(j).Metrics()
			for k := 0; k < metrics.Len(); k++ {
				if err := p.ingestRawMetricLocked(metrics.At(k)); err != nil {
					return md, err
				}
			}
		}
	}
	if p.cfg.DropOriginal {
		return pmetric.NewMetrics(), nil
	}
	return md, nil
}

func (p *gorillaS3Processor) consumeAgentLocked(md pmetric.Metrics) (pmetric.Metrics, error) {
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			metrics := sms.At(j).Metrics()
			for k := 0; k < metrics.Len(); k++ {
				if err := p.ingestFragmentMetricLocked(metrics.At(k)); err != nil {
					return md, err
				}
			}
		}
	}
	fragments, err := p.fragmentEncoder.Drain(false)
	if err != nil {
		return md, err
	}
	fragMD, err := fragmentsToMetrics(fragments)
	if err != nil {
		return md, err
	}
	if !p.cfg.DropOriginal && len(fragments) == 0 {
		return md, nil
	}
	return fragMD, nil
}

func (p *gorillaS3Processor) consumeGatewayFragmentLocked(md pmetric.Metrics) (pmetric.Metrics, error) {
	fragments, err := extractFragments(md)
	if err != nil {
		return md, err
	}
	for _, fragment := range fragments {
		if err := p.fragmentFinalizer.AddFragment(fragment); err != nil {
			return md, err
		}
	}
	if p.cfg.DropOriginal {
		return pmetric.NewMetrics(), nil
	}
	return md, nil
}

func (p *gorillaS3Processor) ingestRawMetricLocked(m pmetric.Metric) error {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		dps := m.Gauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			v, ok := numberValue(dp)
			if !ok {
				continue
			}
			if err := p.rawBuilder.AddSample(gorilla.TSDBSample{
				MetricName: m.Name(),
				Attributes: attributesToMap(dp.Attributes()),
				Timestamp:  dp.Timestamp().AsTime(),
				Value:      v,
			}); err != nil {
				return err
			}
		}
	case pmetric.MetricTypeSum:
		dps := m.Sum().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			v, ok := numberValue(dp)
			if !ok {
				continue
			}
			if err := p.rawBuilder.AddSample(gorilla.TSDBSample{
				MetricName: m.Name(),
				Attributes: attributesToMap(dp.Attributes()),
				Timestamp:  dp.Timestamp().AsTime(),
				Value:      v,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *gorillaS3Processor) ingestFragmentMetricLocked(m pmetric.Metric) error {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		dps := m.Gauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			v, ok := numberValue(dp)
			if !ok {
				continue
			}
			if err := p.fragmentEncoder.AddSample(gorilla.TSDBSample{
				MetricName: m.Name(),
				Attributes: attributesToMap(dp.Attributes()),
				Timestamp:  dp.Timestamp().AsTime(),
				Value:      v,
			}); err != nil {
				return err
			}
		}
	case pmetric.MetricTypeSum:
		dps := m.Sum().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			v, ok := numberValue(dp)
			if !ok {
				continue
			}
			if err := p.fragmentEncoder.AddSample(gorilla.TSDBSample{
				MetricName: m.Name(),
				Attributes: attributesToMap(dp.Attributes()),
				Timestamp:  dp.Timestamp().AsTime(),
				Value:      v,
			}); err != nil {
				return err
			}
		}
	}
	return nil
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

// flushWindow finalizes the current streaming Prometheus TSDB block and uploads
// it to the Thanos bucket. The next incoming sample starts a new builder.
func (p *gorillaS3Processor) flushWindow(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.ensureRoleStateLocked(); err != nil {
		p.logger.Error("gorillas3: role state init failed", zap.Error(err))
		return
	}

	switch p.cfg.Role {
	case ProcessorRoleAgent:
		p.flushAgentLocked(ctx)
	case ProcessorRoleGatewayFragment:
		p.flushGatewayFragmentLocked(ctx)
	default:
		p.flushGatewayRawLocked(ctx)
	}
}

func (p *gorillaS3Processor) flushAgentLocked(ctx context.Context) {
	fragments, err := p.fragmentEncoder.Drain(true)
	if err != nil {
		p.logger.Error("gorillas3: fragment drain failed", zap.Error(err))
		return
	}
	if len(fragments) == 0 || p.nextConsumer == nil {
		return
	}
	md, err := fragmentsToMetrics(fragments)
	if err != nil {
		p.logger.Error("gorillas3: fragment metric build failed", zap.Error(err))
		return
	}
	if err := p.nextConsumer.ConsumeMetrics(ctx, md); err != nil {
		p.logger.Error("gorillas3: fragment forward failed", zap.Error(err))
	}
}

func (p *gorillaS3Processor) flushGatewayRawLocked(ctx context.Context) {
	builder := p.rawBuilder
	p.rawBuilder = nil
	artifact, err := builder.Finalize(ctx)
	if err != nil {
		p.logger.Error("gorillas3: tsdb block build failed", zap.Error(err))
		if p.monitor != nil {
			p.monitor.putFailure(ctx)
		}
		return
	}
	p.uploadArtifact(ctx, artifact)
}

func (p *gorillaS3Processor) flushGatewayFragmentLocked(ctx context.Context) {
	finalizer := p.fragmentFinalizer
	p.fragmentFinalizer = nil
	artifact, err := finalizer.Finalize(ctx)
	if err != nil {
		p.logger.Error("gorillas3: tsdb block build failed", zap.Error(err))
		if p.monitor != nil {
			p.monitor.putFailure(ctx)
		}
		return
	}
	p.uploadArtifact(ctx, artifact)
}

func (p *gorillaS3Processor) uploadArtifact(ctx context.Context, artifact *gorilla.TSDBBlockArtifact) {
	if artifact == nil {
		return
	}
	// mvp/issue46: surface OOB-dropped sample counts on every flush.
	// We warn (not error) so operators see drift but the agent stays
	// up; the counter feeds the same metric exporter the rest of the
	// processor counters use.
	if artifact.NumOOODropped > 0 {
		p.logger.Warn("gorillas3: tsdb out-of-bounds samples dropped",
			zap.Uint64("dropped", artifact.NumOOODropped),
			zap.Uint64("appended", artifact.NumSamples),
		)
		if p.monitor != nil {
			p.monitor.tsdbOOBDropped(ctx, artifact.NumOOODropped)
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
	p.mu.Lock()
	defer p.mu.Unlock()
	switch p.cfg.Role {
	case ProcessorRoleAgent:
		if p.fragmentEncoder == nil {
			return 0
		}
		return int64(p.fragmentEncoder.ActiveSeries())
	case ProcessorRoleGatewayFragment:
		if p.fragmentFinalizer == nil {
			return 0
		}
		// The shared finalizer intentionally does not expose mutable internals;
		// this role reports zero until we add a dedicated gauge.
		return 0
	default:
		if p.rawBuilder == nil {
			return 0
		}
		return int64(p.rawBuilder.ActiveSeries())
	}
}

// ensureRoleStateLocked lazily initializes the role-specific builder/encoder.
// Callers MUST hold p.mu — Go mutexes are not reentrant, and the consume/flush
// paths rely on the init step being part of the SAME critical section as the
// subsequent read/mutation of p.rawBuilder/p.fragmentEncoder/p.fragmentFinalizer
// (see flushGatewayRawLocked/flushGatewayFragmentLocked which nil those out).
// Splitting init and use into two critical sections re-introduces the TOCTOU
// race that produced nil-pointer dereferences in AddSample.
func (p *gorillaS3Processor) ensureRoleStateLocked() error {
	switch p.cfg.Role {
	case ProcessorRoleAgent:
		if p.fragmentEncoder == nil {
			p.fragmentEncoder = gorilla.NewStreamingFragmentEncoder(gorilla.StreamingFragmentOptions{
				ReorderGrace:    p.cfg.TSDBReorderGrace,
				SamplesPerChunk: p.cfg.FragmentSamplesPerChunk,
				Source:          p.cfg.SourceID,
			})
		}
	case ProcessorRoleGatewayFragment:
		if p.fragmentFinalizer == nil {
			finalizer, err := gorilla.NewFragmentBlockFinalizer(gorilla.FragmentBlockOptions{
				ExternalLabels: p.cfg.TSDBExternalLabels,
			})
			if err != nil {
				return err
			}
			p.fragmentFinalizer = finalizer
		}
	default:
		if p.rawBuilder == nil {
			builder, err := gorilla.NewStreamingTSDBBlockBuilder(gorilla.StreamingTSDBOptions{
				ReorderGrace:   p.cfg.TSDBReorderGrace,
				ExternalLabels: p.cfg.TSDBExternalLabels,
			})
			if err != nil {
				return err
			}
			p.rawBuilder = builder
		}
	}
	return nil
}

func fragmentsToMetrics(fragments []gorilla.Fragment) (pmetric.Metrics, error) {
	md := pmetric.NewMetrics()
	if len(fragments) == 0 {
		return md, nil
	}
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName(gorilla.FragmentMetricName)
	g := m.SetEmptyGauge()
	for _, fragment := range fragments {
		payload, err := gorilla.MarshalFragment(fragment)
		if err != nil {
			return md, err
		}
		dp := g.DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.UnixMilli(fragment.MaxTime)))
		dp.SetIntValue(int64(fragment.Count))
		dp.Attributes().PutStr(gorilla.FragmentPayloadAttribute, payload)
		if fragment.FragmentULID != "" {
			dp.Attributes().PutStr("fragment_ulid", fragment.FragmentULID)
		}
		if fragment.Source != "" {
			dp.Attributes().PutStr("source_id", fragment.Source)
		}
		dp.Attributes().PutStr("metric_name", fragment.MetricName)
	}
	return md, nil
}

func extractFragments(md pmetric.Metrics) ([]gorilla.Fragment, error) {
	var fragments []gorilla.Fragment
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			metrics := sms.At(j).Metrics()
			for k := 0; k < metrics.Len(); k++ {
				m := metrics.At(k)
				if m.Name() != gorilla.FragmentMetricName {
					continue
				}
				switch m.Type() {
				case pmetric.MetricTypeGauge:
					dps := m.Gauge().DataPoints()
					for i := 0; i < dps.Len(); i++ {
						fragment, ok, err := fragmentFromAttributes(dps.At(i).Attributes())
						if err != nil {
							return nil, err
						}
						if ok {
							fragments = append(fragments, fragment)
						}
					}
				case pmetric.MetricTypeSum:
					dps := m.Sum().DataPoints()
					for i := 0; i < dps.Len(); i++ {
						fragment, ok, err := fragmentFromAttributes(dps.At(i).Attributes())
						if err != nil {
							return nil, err
						}
						if ok {
							fragments = append(fragments, fragment)
						}
					}
				}
			}
		}
	}
	return fragments, nil
}

func fragmentFromAttributes(attrs pcommon.Map) (gorilla.Fragment, bool, error) {
	v, ok := attrs.Get(gorilla.FragmentPayloadAttribute)
	payload := v.AsString()
	if !ok || payload == "" {
		return gorilla.Fragment{}, false, nil
	}
	fragment, err := gorilla.UnmarshalFragment(payload)
	if err != nil {
		return gorilla.Fragment{}, false, err
	}
	return fragment, true, nil
}

func attributesToMap(attrs pcommon.Map) map[string]string {
	m := make(map[string]string, attrs.Len())
	attrs.Range(func(k string, v pcommon.Value) bool {
		m[k] = v.AsString()
		return true
	})
	return m
}
