package countsketchprocessor

import (
	"context"
	"math"
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

type countSketchProcessor struct {
	logger *zap.Logger
	next   consumer.Metrics

	config *Config

	mode InputMode

	// To prevent race conditions between
	// processMetrics (Write) and flushSketches (Reset)
	mutex sync.Mutex

	rowSketch *countsketch.CountSketch // Tracks Metric Names
	colSketch *countsketch.CountSketch // Tracks Host Names

	// sketchPool recycles CountSketch objects across flushes via Reset(),
	// avoiding re-allocation of the underlying hash/count arrays every window.
	sketchPool sync.Pool

	stopCh        chan struct{}
	doneCh        chan struct{}
	windowStarted atomic.Bool // true once the window goroutine is running
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
		// Preserve legacy behavior when mode is not set explicitly.
		mode = ModeWindow
	}

	rowS, errRow := newConfiguredCountSketch(cfg)
	if errRow != nil {
		logger.Error("Failed to init row sketch", zap.Error(errRow))
	}

	colS, errCol := newConfiguredCountSketch(cfg)
	if errCol != nil {
		logger.Error("Failed to init col sketch", zap.Error(errCol))
	}

	p := &countSketchProcessor{
		logger: logger,
		next:   next,
		config: cfg,
		mode:   mode,

		rowSketch: rowS,
		colSketch: colS,
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
	// New returns nil so Get() returns nil when pool is empty;
	// callers allocate fresh sketches in that case.
	p.sketchPool.New = func() any { return (*countsketch.CountSketch)(nil) }
	return p
}

func (p *countSketchProcessor) Start(ctx context.Context, host component.Host) error {
	p.logger.Info("Starting Count Sketch Processor",
		zap.Float64("epsilon", p.config.Epsilon),
		zap.Float64("delta", p.config.Delta),
		zap.Duration("window", p.config.WindowSize),
		zap.String("mode", string(p.mode)),
	)
	if p.config.TransmitSketch {
		p.logger.Warn("countsketchprocessor: transmit_sketch=true is not yet backed by a serialized sketch payload; emitting metric-form summaries")
	}

	// In batch mode we flush per ConsumeMetrics call and do not need a ticker.
	if p.mode == ModeBatch {
		return nil
	}

	// Window mode: start background window loop if a positive window is configured.
	if p.config.WindowSize <= 0 {
		return nil
	}

	ticker := time.NewTicker(p.config.WindowSize)
	p.windowStarted.Store(true)
	go p.startWindowLoop(ctx, ticker)

	return nil
}

func (p *countSketchProcessor) Shutdown(ctx context.Context) error {
	// Only wait if the window goroutine was actually started; avoids blocking
	// forever when Start was never called.
	if p.mode != ModeWindow || !p.windowStarted.Load() {
		return nil
	}

	// Signal the goroutine and wait for it to finish (or context cancellation).
	close(p.stopCh)

	select {
	case <-p.doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *countSketchProcessor) processMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	p.mutex.Lock()

	if p.rowSketch == nil || p.colSketch == nil {
		p.mutex.Unlock()
		return md, nil
	}

	rm := md.ResourceMetrics()
	for i := 0; i < rm.Len(); i++ {
		resourceMetric := rm.At(i)

		hostKey := "unknown-host"
		// TODO: Might need to change the key based on the input
		if v, ok := resourceMetric.Resource().Attributes().Get("host.name"); ok {
			hostKey = v.Str()
		}

		sms := resourceMetric.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			metrics := sms.At(j).Metrics()

			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)

				rowKey := metric.Name()

				var value float64

				switch metric.Type() {
				case pmetric.MetricTypeGauge:
					dps := metric.Gauge().DataPoints()
					value = sumPoints(dps)

				case pmetric.MetricTypeSum:
					dps := metric.Sum().DataPoints()
					value = sumPoints(dps)

				case pmetric.MetricTypeHistogram:
					dps := metric.Histogram().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						value += float64(dps.At(l).Count())
					}

				case pmetric.MetricTypeCountSketch:
					// Pre-aggregated path: each dp represents one series window.
					// Count each dp as one observation for the row/col frequency sketches.
					value += float64(metric.CountSketch().DataPoints().Len())
				}

				// For Debugging
				// p.logger.Info("Sketch Update",
				// 	zap.String("host", hostKey),
				// 	zap.String("metric", rowKey),
				// 	zap.Float64("value", value),
				// )

				p.rowSketch.UpdateString(rowKey, value)
				p.colSketch.UpdateString(hostKey, value)
			}
		}
	}

	p.mutex.Unlock()

	// In batch mode, flush immediately after processing this batch.
	if p.mode == ModeBatch {
		p.flushBatch()
	}

	// If drop_original is true, return empty metrics (sketches will be emitted in flushSketches)
	if p.config.DropOriginal {
		return pmetric.NewMetrics(), nil
	}

	return md, nil
}

func sumPoints(dps pmetric.NumberDataPointSlice) float64 {
	var total float64
	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		if dp.ValueType() == pmetric.NumberDataPointValueTypeInt {
			total += float64(dp.IntValue())
		} else {
			total += dp.DoubleValue()
		}
	}
	return total
}

func (p *countSketchProcessor) startWindowLoop(ctx context.Context, ticker *time.Ticker) {
	defer func() {
		ticker.Stop()

		p.mutex.Lock()
		p.rowSketch = nil
		p.colSketch = nil

		p.mutex.Unlock()

		close(p.doneCh)
	}()

	for {
		select {
		case <-ctx.Done():
			p.flushSketches()
			return
		case <-p.stopCh:
			p.flushSketches()
			return

		case <-ticker.C:
			p.flushSketches()
		}
	}
}

// flushBatch snapshots the current sketches, resets them, and emits summary
// metrics immediately. It is used when the processor runs in batch mode.
func (p *countSketchProcessor) flushBatch() {
	p.mutex.Lock()

	// Snapshot sketches before resetting.
	rowSnapshot := p.rowSketch
	colSnapshot := p.colSketch

	// Get reusable sketches from pool, or allocate fresh ones.
	var err error
	rowNext, _ := p.sketchPool.Get().(*countsketch.CountSketch)
	if rowNext == nil {
		rowNext, err = newConfiguredCountSketch(p.config)
		if err != nil {
			p.logger.Error("Failed to reset row sketch", zap.Error(err))
		}
	}
	colNext, _ := p.sketchPool.Get().(*countsketch.CountSketch)
	if colNext == nil {
		colNext, err = newConfiguredCountSketch(p.config)
		if err != nil {
			p.logger.Error("Failed to reset col sketch", zap.Error(err))
		}
	}
	p.rowSketch = rowNext
	p.colSketch = colNext

	p.mutex.Unlock()

	// Emit sketch metrics if we have snapshots.
	if rowSnapshot != nil || colSnapshot != nil {
		p.emitSketches(rowSnapshot, colSnapshot)
	}

	// Return old sketches to pool after emission.
	if rowSnapshot != nil {
		rowSnapshot.Reset()
		p.sketchPool.Put(rowSnapshot)
	}
	if colSnapshot != nil {
		colSnapshot.Reset()
		p.sketchPool.Put(colSnapshot)
	}
}

func (p *countSketchProcessor) flushSketches() {
	p.mutex.Lock()

	// Snapshot sketches before resetting
	rowSnapshot := p.rowSketch
	colSnapshot := p.colSketch

	// Get reusable sketches from pool, or allocate fresh ones.
	var err error
	rowNext, _ := p.sketchPool.Get().(*countsketch.CountSketch)
	if rowNext == nil {
		rowNext, err = newConfiguredCountSketch(p.config)
		if err != nil {
			p.logger.Error("Failed to reset row sketch", zap.Error(err))
		}
	}
	colNext, _ := p.sketchPool.Get().(*countsketch.CountSketch)
	if colNext == nil {
		colNext, err = newConfiguredCountSketch(p.config)
		if err != nil {
			p.logger.Error("Failed to reset col sketch", zap.Error(err))
		}
	}
	p.rowSketch = rowNext
	p.colSketch = colNext

	p.mutex.Unlock()

	// Emit sketch metrics if we have snapshots
	if rowSnapshot != nil || colSnapshot != nil {
		p.emitSketches(rowSnapshot, colSnapshot)
	}

	// Return old sketches to pool after emission.
	if rowSnapshot != nil {
		rowSnapshot.Reset()
		p.sketchPool.Put(rowSnapshot)
	}
	if colSnapshot != nil {
		colSnapshot.Reset()
		p.sketchPool.Put(colSnapshot)
	}
}

func (p *countSketchProcessor) emitSketches(rowSketch, colSketch *countsketch.CountSketch) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("otelcol/countsketch")

	now := pcommon.NewTimestampFromTime(time.Now())

	// Emit row sketch (metric names)
	if rowSketch != nil {
		m := sm.Metrics().AppendEmpty()
		m.SetName("countsketch_row")
		m.SetUnit("1")

		gauge := m.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetTimestamp(now)

		dp.Attributes().PutStr("sketch_type", "row")
		dp.Attributes().PutStr("sketch_dimension", "metric_names")
		dp.Attributes().PutDouble("epsilon", p.config.Epsilon)
		dp.Attributes().PutDouble("delta", p.config.Delta)
		dp.Attributes().PutInt("window_size_seconds", int64(p.config.WindowSize.Seconds()))
		// Note: sketchlib-go CountSketch doesn't expose serialization methods
		// For benchmarking, we emit metadata. Full serialization can be added later if needed.
	}

	// Emit col sketch (host names)
	if colSketch != nil {
		m := sm.Metrics().AppendEmpty()
		m.SetName("countsketch_col")
		m.SetUnit("1")

		gauge := m.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetTimestamp(now)

		dp.Attributes().PutStr("sketch_type", "col")
		dp.Attributes().PutStr("sketch_dimension", "host_names")
		dp.Attributes().PutDouble("epsilon", p.config.Epsilon)
		dp.Attributes().PutDouble("delta", p.config.Delta)
		dp.Attributes().PutInt("window_size_seconds", int64(p.config.WindowSize.Seconds()))
	}

	if err := p.next.ConsumeMetrics(context.Background(), md); err != nil {
		p.logger.Error("Failed to emit countsketch metrics", zap.Error(err))
	}
}
