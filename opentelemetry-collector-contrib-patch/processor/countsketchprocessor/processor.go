package countsketchprocessor

import (
	"context"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	countsketch "github.com/ProjectASAP/sketchlib-go/sketches/CountSketch"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// builderPool recycles strings.Builder instances used in buildPartitionKey.
var builderPool = sync.Pool{New: func() any { return new(strings.Builder) }}

type windowSketch struct {
	cs          *countsketch.CountSketch
	mu          sync.Mutex
	sampleCount uint64
}

type countSketchProcessor struct {
	logger *zap.Logger
	next   consumer.Metrics
	config *Config
	mode   InputMode

	mu                   sync.RWMutex
	activeWindowSketches map[string]*windowSketch

	windowSketchPool sync.Pool

	// snapshots holds one CS clone per partition key, updated every window
	// flush. Used to compute sparse delta payloads when DeltaTransmission=true.
	snapshots   map[string]*countsketch.CountSketch
	snapshotsMu sync.Mutex

	stopCh        chan struct{}
	doneCh        chan struct{}
	windowStarted atomic.Bool
}

func newConfiguredCountSketch(cfg *Config) (*countsketch.CountSketch, error) {
	rows := int(math.Ceil(math.Log(1 / cfg.Delta)))
	if rows < 1 {
		rows = 1
	}

	cols := int(math.Ceil(1 / (cfg.Epsilon * cfg.Epsilon)))
	if cols < 2 {
		cols = 2
	}
	cols = nextPowerOfTwo(cols)

	return countsketch.NewCountSketch(rows, cols)
}

func nextPowerOfTwo(n int) int {
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}

func newProcessor(logger *zap.Logger, cfg *Config, next consumer.Metrics) *countSketchProcessor {
	mode := cfg.Mode
	if mode == "" {
		mode = ModeBatch
	}

	p := &countSketchProcessor{
		logger:               logger,
		next:                 next,
		config:               cfg,
		mode:                 mode,
		activeWindowSketches: make(map[string]*windowSketch),
		snapshots:            make(map[string]*countsketch.CountSketch),
		stopCh:               make(chan struct{}),
		doneCh:               make(chan struct{}),
	}
	p.windowSketchPool.New = func() any { return new(windowSketch) }
	return p
}

func (p *countSketchProcessor) Start(ctx context.Context, host component.Host) error {
	p.logger.Info("Starting Count Sketch Processor",
		zap.Float64("epsilon", p.config.Epsilon),
		zap.Float64("delta", p.config.Delta),
		zap.Duration("window_duration", p.config.WindowDuration),
		zap.String("mode", string(p.mode)),
		zap.Strings("aggregate_by", p.config.AggregateBy),
	)

	if p.mode == ModeBatch {
		return nil
	}

	if p.config.WindowDuration <= 0 {
		return nil
	}

	ticker := time.NewTicker(p.config.WindowDuration)
	p.windowStarted.Store(true)
	go p.startWindowLoop(ctx, ticker)

	return nil
}

func (p *countSketchProcessor) Shutdown(ctx context.Context) error {
	if p.mode != ModeWindow || !p.windowStarted.Load() {
		return nil
	}

	close(p.stopCh)

	select {
	case <-p.doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *countSketchProcessor) processMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	switch p.mode {
	case ModeBatch:
		return p.consumeBatch(md), nil
	case ModeWindow:
		p.accumulateIntoWindow(md)
		if !p.config.DropOriginal {
			return md, nil
		}
		return pmetric.NewMetrics(), nil
	default:
		p.logger.Error("countsketchprocessor: unknown mode, dropping metrics", zap.Any("mode", p.mode))
		return pmetric.NewMetrics(), nil
	}
}

func (p *countSketchProcessor) accumulateIntoWindow(md pmetric.Metrics) {
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		resourceAttrs := rm.Resource().Attributes()
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			metrics := sms.At(j).Metrics()
			for k := 0; k < metrics.Len(); k++ {
				p.ingestMetric(resourceAttrs, metrics.At(k))
			}
		}
	}
}

func (p *countSketchProcessor) consumeBatch(md pmetric.Metrics) pmetric.Metrics {
	p.accumulateIntoWindow(md)

	sketches := p.buildWindowMetricsAndReset()
	if sketches.ResourceMetrics().Len() == 0 {
		if p.config.DropOriginal {
			return pmetric.NewMetrics()
		}
		return md
	}

	if p.config.DropOriginal {
		return sketches
	}

	// Expansion mode: keep originals and append sketch summaries.
	out := pmetric.NewMetrics()
	md.ResourceMetrics().MoveAndAppendTo(out.ResourceMetrics())
	sketches.ResourceMetrics().MoveAndAppendTo(out.ResourceMetrics())
	return out
}

// matchesMatchers returns true if attrs satisfies all configured LabelMatchers.
func (p *countSketchProcessor) matchesMatchers(attrs pcommon.Map) bool {
	for _, m := range p.config.LabelMatchers {
		v, ok := attrs.Get(m.Key)
		if !ok || v.AsString() != m.Value {
			return false
		}
	}
	return true
}

func (p *countSketchProcessor) ingestMetric(resourceAttrs pcommon.Map, metric pmetric.Metric) {
	metricName := metric.Name()
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		dps := metric.Gauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			if !p.matchesMatchers(dp.Attributes()) {
				continue
			}
			pk := buildPartitionKey(resourceAttrs, dp.Attributes(), p.config.AggregateBy)
			p.updateWindowSketch(pk, metricName, dpValue(dp))
		}
	case pmetric.MetricTypeSum:
		dps := metric.Sum().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			if !p.matchesMatchers(dp.Attributes()) {
				continue
			}
			pk := buildPartitionKey(resourceAttrs, dp.Attributes(), p.config.AggregateBy)
			p.updateWindowSketch(pk, metricName, dpValue(dp))
		}
	case pmetric.MetricTypeHistogram:
		dps := metric.Histogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			if !p.matchesMatchers(dp.Attributes()) {
				continue
			}
			pk := buildPartitionKey(resourceAttrs, dp.Attributes(), p.config.AggregateBy)
			p.updateWindowSketch(pk, metricName, float64(dp.Count()))
		}
	case pmetric.MetricTypeCountSketch:
		// Pre-aggregated path: count each dp as one observation.
		dps := metric.CountSketch().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			if !p.matchesMatchers(dp.Attributes()) {
				continue
			}
			pk := buildPartitionKey(resourceAttrs, dp.Attributes(), p.config.AggregateBy)
			p.updateWindowSketch(pk, metricName, 1.0)
		}
	}
}

func dpValue(dp pmetric.NumberDataPoint) float64 {
	if dp.ValueType() == pmetric.NumberDataPointValueTypeInt {
		return float64(dp.IntValue())
	}
	return dp.DoubleValue()
}

func (p *countSketchProcessor) updateWindowSketch(partitionKey, itemKey string, value float64) {
	p.mu.RLock()
	ws, exists := p.activeWindowSketches[partitionKey]
	p.mu.RUnlock()

	if !exists {
		p.mu.Lock()
		ws, exists = p.activeWindowSketches[partitionKey]
		if !exists {
			ws = p.windowSketchPool.Get().(*windowSketch)
			if ws.cs != nil {
				ws.cs.Reset()
			} else {
				cs, err := newConfiguredCountSketch(p.config)
				if err != nil {
					p.logger.Error("Failed to create CountSketch", zap.Error(err))
					p.windowSketchPool.Put(ws)
					p.mu.Unlock()
					return
				}
				ws.cs = cs
			}
			ws.sampleCount = 0
			p.activeWindowSketches[partitionKey] = ws
		}
		p.mu.Unlock()
	}

	ws.mu.Lock()
	ws.cs.UpdateString(itemKey, value)
	ws.sampleCount++
	ws.mu.Unlock()
}

func (p *countSketchProcessor) startWindowLoop(ctx context.Context, ticker *time.Ticker) {
	defer func() {
		ticker.Stop()
		close(p.doneCh)
	}()

	for {
		select {
		case <-ctx.Done():
			p.emitWindowAndReset()
			return
		case <-p.stopCh:
			p.emitWindowAndReset()
			return
		case <-ticker.C:
			p.emitWindowAndReset()
		}
	}
}

func (p *countSketchProcessor) buildWindowMetricsAndReset() pmetric.Metrics {
	p.mu.Lock()
	if len(p.activeWindowSketches) == 0 {
		p.mu.Unlock()
		return pmetric.NewMetrics()
	}

	snapshot := p.activeWindowSketches
	p.activeWindowSketches = make(map[string]*windowSketch)
	p.mu.Unlock()

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("otelcol/countsketch")

	now := pcommon.NewTimestampFromTime(time.Now())

	for partitionKey, ws := range snapshot {
		ws.mu.Lock()
		sampleCount := ws.sampleCount

		var payload []byte
		var encoding string
		var serErr error

		if p.config.TransmitSketch && ws.cs != nil {
			if p.config.DeltaTransmission {
				p.snapshotsMu.Lock()
				snap, hasSnap := p.snapshots[partitionKey]
				p.snapshotsMu.Unlock()

				if hasSnap {
					payload, serErr = countsketch.ComputeDelta(snap, ws.cs, p.config.DeltaThreshold)
					encoding = "proto_delta"
				} else {
					payload, serErr = ws.cs.SerializeToBytes()
					encoding = "proto_full"
				}

				newSnap := cloneCS(ws.cs)
				p.snapshotsMu.Lock()
				p.snapshots[partitionKey] = newSnap
				p.snapshotsMu.Unlock()
			} else {
				payload, serErr = ws.cs.SerializeToBytes()
				encoding = "proto_full"
			}
		}

		ws.mu.Unlock()

		if ws.cs != nil {
			ws.cs.Reset()
		}
		p.windowSketchPool.Put(ws)

		if p.config.TransmitSketch && serErr != nil {
			p.logger.Error("Failed to serialize CountSketch", zap.Error(serErr))
			continue
		}

		m := sm.Metrics().AppendEmpty()
		m.SetName("countsketch_partition")
		m.SetUnit("1")

		gauge := m.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetTimestamp(now)
		dp.Attributes().PutStr("partition_key", partitionKey)
		dp.Attributes().PutInt("sample_count", int64(sampleCount))
		dp.Attributes().PutDouble("epsilon", p.config.Epsilon)
		dp.Attributes().PutDouble("delta", p.config.Delta)
		dp.Attributes().PutInt("window_duration_seconds", int64(p.config.WindowDuration.Seconds()))
		if p.config.TransmitSketch {
			dp.Attributes().PutStr("encoding", encoding)
			dp.Attributes().PutEmptyBytes("sketch_payload").FromRaw(payload)
		}
		dp.SetDoubleValue(float64(sampleCount))
	}

	return md
}

func (p *countSketchProcessor) emitWindowAndReset() {
	md := p.buildWindowMetricsAndReset()
	if md.ResourceMetrics().Len() == 0 {
		return
	}

	if err := p.next.ConsumeMetrics(context.Background(), md); err != nil {
		p.logger.Error("Failed to emit countsketch partition metrics", zap.Error(err))
	}
}

// buildPartitionKey encodes selected attributes as a stable partition key.
// Each key is looked up in dpAttrs first, then resourceAttrs as a fallback.
// Returns "global" when aggregateBy is empty (single undivided partition).
func buildPartitionKey(resourceAttrs, dpAttrs pcommon.Map, aggregateBy []string) string {
	if len(aggregateBy) == 0 {
		return "global"
	}

	keys := make([]string, len(aggregateBy))
	copy(keys, aggregateBy)
	sort.Strings(keys)

	sb := builderPool.Get().(*strings.Builder)
	sb.Reset()
	for _, k := range keys {
		var val pcommon.Value
		var ok bool
		val, ok = dpAttrs.Get(k)
		if !ok {
			val, ok = resourceAttrs.Get(k)
		}
		if ok {
			sb.WriteString(k)
			sb.WriteString("=")
			sb.WriteString(val.AsString())
			sb.WriteString(";")
		}
	}
	s := sb.String()
	builderPool.Put(sb)
	return s
}

// cloneCS returns a deep copy of cs suitable for use as a delta snapshot.
func cloneCS(cs *countsketch.CountSketch) *countsketch.CountSketch {
	data, err := cs.SerializeToBytes()
	if err != nil {
		return nil
	}
	clone, err := countsketch.DeserializeCountSketchFromBytes(data)
	if err != nil {
		return nil
	}
	return clone
}
