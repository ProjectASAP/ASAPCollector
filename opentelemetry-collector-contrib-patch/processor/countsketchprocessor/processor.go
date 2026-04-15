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
	"go.opentelemetry.io/collector/processor/selfmonitor"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// builderPool recycles strings.Builder instances used in buildPartitionKey.
var builderPool = sync.Pool{New: func() any { return new(strings.Builder) }}

type windowSketch struct {
	cs          *countsketch.CountSketch
	mu          sync.Mutex
	sampleCount uint64
}

type countSketchProcessor struct {
	logger  *zap.Logger
	next    consumer.Metrics
	config  *Config
	mode    InputMode
	monitor *selfmonitor.Monitor

	mu                   sync.RWMutex
	activeWindowSketches map[string]*windowSketch

	windowSketchPool sync.Pool

	// snapshots holds one CS clone per partition key, updated every window
	// flush. Used to compute sparse delta payloads when DeltaTransmission=true.
	snapshots   map[string]*countsketch.CountSketch
	snapshotsMu sync.Mutex

	// inboundSnapshots tracks the last reconstructed full CS per series key
	// received from upstream. Used to apply sparse deltas when upstream sends
	// CountSketchEncodingDelta payloads.
	inboundMu        sync.Mutex
	inboundSnapshots map[string]*countsketch.CountSketch

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
		inboundSnapshots:     make(map[string]*countsketch.CountSketch),
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
	defer p.shutdownMonitor()

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
	p.recordInput(ctx, md)

	switch p.mode {
	case ModeBatch:
		out := p.consumeBatch(md)
		p.recordOutput(ctx, out)
		return out, nil
	case ModeWindow:
		p.accumulateIntoWindow(md)
		if !p.config.DropOriginal {
			p.recordOutput(ctx, md)
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
		// Pre-aggregated path: deserialize (or reconstruct from delta) the incoming
		// CountSketch and merge it into the running window sketch.
		dps := metric.CountSketch().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			if !p.matchesMatchers(dp.Attributes()) {
				continue
			}
			pk := buildPartitionKey(resourceAttrs, dp.Attributes(), p.config.AggregateBy)
			if len(dp.Sketch()) == 0 {
				// No sketch payload — treat as a raw sample (backwards compat).
				p.updateWindowSketch(pk, metricName, 1.0)
				continue
			}
			incoming, err := p.inboundDecodeCS(pk, dp)
			if err != nil {
				p.logger.Error("countsketchprocessor: failed to decode inbound CountSketch", zap.Error(err))
				continue
			}
			if incoming == nil {
				continue // delta arrived before any full snapshot
			}
			p.mergeWindowCS(pk, incoming)
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
				// Delta transmission is proto-only — msgpack delta
				// is tracked as a follow-up once sketchlib-go
				// grows `apply_delta` semantics.
				p.snapshotsMu.Lock()
				snap, hasSnap := p.snapshots[partitionKey]
				p.snapshotsMu.Unlock()

				if hasSnap {
					deltaMsg, deltaErr := countsketch.ComputeDelta(snap, ws.cs, p.config.DeltaThreshold)
					if deltaErr == nil {
						payload, serErr = countsketch.SerializeDelta(deltaMsg)
					} else {
						serErr = deltaErr
					}
					encoding = "proto_delta"
				} else {
					payload, serErr = serializeCountSketch(ws.cs)
					encoding = "proto_full"
				}

				newSnap := cloneCS(ws.cs)
				p.snapshotsMu.Lock()
				p.snapshots[partitionKey] = newSnap
				p.snapshotsMu.Unlock()
			} else {
				// Non-delta path — choose between proto and msgpack
				// wire formats based on the config's Encoding field.
				switch p.config.Encoding {
				case EncodingMsgpack:
					payload, serErr = ws.cs.SerializeMsgpack()
					encoding = "msgpack_full"
				default:
					payload, serErr = serializeCountSketch(ws.cs)
					encoding = "proto_full"
				}
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

		if p.config.TransmitSketch {
			// Typed CountSketchDataPoint emission — what
			// ASAPQuery-backend's modified-OTLP sketch router
			// consumes as `Metric.data = CountSketch{...}`.
			// Before this change the processor emitted a Gauge
			// with the sketch payload stuffed into a
			// `sketch_payload` byte attribute, which the backend
			// router never recognized as a sketch variant —
			// sketch bytes were lost on the wire for any
			// consumer that tried to decode them as typed.
			csMetric := m.SetEmptyCountSketch()
			csMetric.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
			dp := csMetric.DataPoints().AppendEmpty()
			dp.SetTimestamp(now)
			// The processor's `partition_key` is the natural
			// match for CountSketch's `dimension` field (both
			// identify which sub-population the sketch covers).
			dp.SetDimension(partitionKey)
			dp.SetEpsilon(p.config.Epsilon)
			dp.SetDelta(p.config.Delta)
			dp.SetSketch(payload)
			switch encoding {
			case "proto_delta":
				dp.SetEncoding(pmetric.CountSketchEncodingDelta)
			case "msgpack_full":
				dp.SetEncoding(pmetric.CountSketchEncodingMsgpack)
			default:
				// "proto_full" and any unexpected fallback.
				dp.SetEncoding(pmetric.CountSketchEncodingProto)
			}
			// Fields the typed DP doesn't have dedicated setters
			// for still go on the attribute map. `sample_count`
			// and `window_duration_seconds` are observability
			// hints the backend does not use for routing.
			dp.Attributes().PutInt("sample_count", int64(sampleCount))
			dp.Attributes().PutInt(
				"window_duration_seconds",
				int64(p.config.WindowDuration.Seconds()),
			)
		} else {
			// Non-transmit mode: caller only wants the
			// per-partition sample count for monitoring, not
			// the sketch bytes. Keep the legacy Gauge emission
			// so existing dashboards that read
			// `countsketch_partition` as a scalar series
			// continue to work.
			gauge := m.SetEmptyGauge()
			dp := gauge.DataPoints().AppendEmpty()
			dp.SetTimestamp(now)
			dp.Attributes().PutStr("partition_key", partitionKey)
			dp.Attributes().PutInt("sample_count", int64(sampleCount))
			dp.Attributes().PutDouble("epsilon", p.config.Epsilon)
			dp.Attributes().PutDouble("delta", p.config.Delta)
			dp.Attributes().PutInt(
				"window_duration_seconds",
				int64(p.config.WindowDuration.Seconds()),
			)
			dp.SetDoubleValue(float64(sampleCount))
		}
	}

	return md
}

func (p *countSketchProcessor) emitWindowAndReset() {
	md := p.buildWindowMetricsAndReset()
	if md.ResourceMetrics().Len() == 0 {
		return
	}

	p.recordOutput(context.Background(), md)
	if err := p.next.ConsumeMetrics(context.Background(), md); err != nil {
		p.logger.Error("Failed to emit countsketch partition metrics", zap.Error(err))
	}
}

func (p *countSketchProcessor) enableSelfMonitoring(settings component.TelemetrySettings, processorID string) {
	monitor, err := selfmonitor.New(settings, processorID, typeStr.String(), p.activeSeriesCount)
	if err != nil {
		if p.logger != nil {
			p.logger.Warn("countsketchprocessor: failed to initialize self-monitoring", zap.Error(err))
		}
		return
	}
	p.monitor = monitor
}

func (p *countSketchProcessor) shutdownMonitor() {
	if p.monitor != nil {
		p.monitor.Shutdown()
	}
}

func (p *countSketchProcessor) recordInput(ctx context.Context, md pmetric.Metrics) {
	if p.monitor != nil {
		p.monitor.RecordInput(ctx, md)
	}
}

func (p *countSketchProcessor) recordOutput(ctx context.Context, md pmetric.Metrics) {
	if p.monitor != nil {
		p.monitor.RecordOutput(ctx, md)
	}
}

func (p *countSketchProcessor) activeSeriesCount() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return int64(len(p.activeWindowSketches))
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

// inboundDecodeCS decodes an incoming CountSketch data point, handling both full
// (Gob-encoded) and sparse-delta payloads. For delta payloads it applies the delta
// onto the last stored inbound snapshot to reconstruct the current full state.
// Returns (nil, nil) when a delta arrives before any full snapshot.
func (p *countSketchProcessor) inboundDecodeCS(partitionKey string, dp pmetric.CountSketchDataPoint) (*countsketch.CountSketch, error) {
	payload := dp.Sketch()

	switch dp.Encoding() {
	case pmetric.CountSketchEncodingDelta:
		p.inboundMu.Lock()
		snap, hasSnap := p.inboundSnapshots[partitionKey]
		p.inboundMu.Unlock()
		if !hasSnap || snap == nil {
			return nil, nil
		}
		reconstructed := cloneCS(snap)
		if reconstructed == nil {
			return nil, nil
		}
		deltaMsg, err := countsketch.DeserializeDelta(payload)
		if err != nil {
			return nil, err
		}
		countsketch.ApplyDelta(reconstructed, deltaMsg)
		p.inboundMu.Lock()
		p.inboundSnapshots[partitionKey] = cloneCS(reconstructed)
		p.inboundMu.Unlock()
		return reconstructed, nil

	default: // CountSketchEncodingProto or unspecified
		decoded, err := countsketch.DeserializeCountSketchFromBytes(payload)
		if err != nil {
			return nil, err
		}
		p.inboundMu.Lock()
		p.inboundSnapshots[partitionKey] = cloneCS(decoded)
		p.inboundMu.Unlock()
		return decoded, nil
	}
}

// mergeWindowCS merges an incoming pre-aggregated CountSketch into the per-key
// window store, creating the window sketch if it does not yet exist.
func (p *countSketchProcessor) mergeWindowCS(partitionKey string, incoming *countsketch.CountSketch) {
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
					p.logger.Error("countsketchprocessor: failed to create CS for merge", zap.Error(err))
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
	defer ws.mu.Unlock()
	if err := ws.cs.Merge(incoming); err != nil {
		p.logger.Error("countsketchprocessor: failed to merge CountSketch", zap.Error(err))
	}
	ws.sampleCount++
}

func serializeCountSketch(s *countsketch.CountSketch) ([]byte, error) {
	if s == nil {
		return nil, nil
	}
	env, err := s.SerializePortable()
	if err != nil {
		return nil, err
	}
	return proto.Marshal(env)
}

// cloneCS returns a deep copy of cs suitable for use as a delta snapshot.
func cloneCS(cs *countsketch.CountSketch) *countsketch.CountSketch {
	data, err := cs.SerializeProtoBytes()
	if err != nil {
		return nil
	}
	clone, err := countsketch.DeserializeCountSketchFromProtoBytes(data)
	if err != nil {
		return nil
	}
	return clone
}
