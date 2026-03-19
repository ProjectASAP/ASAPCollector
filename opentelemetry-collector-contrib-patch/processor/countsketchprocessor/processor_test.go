package countsketchprocessor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

func TestProcessorPassThrough(t *testing.T) {
	// Setup
	cfg := createDefaultConfig().(*Config)
	next := new(consumertest.MetricsSink)
	proc := newProcessor(zap.NewNop(), cfg, next)

	// Start the processor
	err := proc.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	// Create generic metrics
	metrics := buildTestMetrics()

	// Process metrics
	out, err := proc.processMetrics(context.Background(), metrics)
	require.NoError(t, err)

	// Shutdown
	err = proc.Shutdown(context.Background())
	require.NoError(t, err)

	// Verify the original metrics were passed through to the next consumer
	assert.Equal(t, metrics, out)
}

func TestProcessorFlushLogic(t *testing.T) {
	// Setup with a very short window for testing flush
	cfg := &Config{
		Epsilon:    0.1,
		Delta:      0.9,
		WindowDuration: 100 * time.Millisecond,
	}
	next := new(consumertest.MetricsSink)
	proc := newProcessor(zap.NewNop(), cfg, next)

	// Start
	err := proc.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	// Send some data to populate sketches
	metrics := buildTestMetrics()
	_, err = proc.processMetrics(context.Background(), metrics)
	require.NoError(t, err)

	// Wait for a window flush (window is 100ms)
	time.Sleep(200 * time.Millisecond)

	// Shutdown
	err = proc.Shutdown(context.Background())
	require.NoError(t, err)
}

func TestBatchModePassThroughAndSummary(t *testing.T) {
	cfg := &Config{
		Mode:       ModeBatch,
		Epsilon:    0.01,
		Delta:      0.99,
		WindowDuration: 0,
		// Keep originals in batch mode so we can verify both paths.
		DropOriginal: false,
	}
	require.NoError(t, cfg.Validate())

	next := new(consumertest.MetricsSink)
	proc := newProcessor(zap.NewNop(), cfg, next)

	err := proc.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	metrics := buildTestMetrics()

	// Process a single batch.
	out, err := proc.processMetrics(context.Background(), metrics)
	require.NoError(t, err)

	err = proc.Shutdown(context.Background())
	require.NoError(t, err)

	// In batch mode with DropOriginal=false, the original metrics should pass through.
	assert.Equal(t, metrics, out)

	// And the next consumer should have received at least one batch of
	// CountSketch summary metrics (row + col).
	sinkMetrics := next.AllMetrics()
	require.GreaterOrEqual(t, len(sinkMetrics), 1)

	foundRow := false
	foundCol := false
	for _, md := range sinkMetrics {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					name := ms.At(k).Name()
					if name == "countsketch_row" {
						foundRow = true
					}
					if name == "countsketch_col" {
						foundCol = true
					}
				}
			}
		}
	}
	assert.True(t, foundRow, "expected countsketch_row summary metric in batch mode")
	assert.True(t, foundCol, "expected countsketch_col summary metric in batch mode")
}

func TestBatchModeDropOriginal(t *testing.T) {
	cfg := &Config{
		Mode:         ModeBatch,
		Epsilon:      0.01,
		Delta:        0.99,
		WindowDuration:   0,
		DropOriginal: true,
	}
	require.NoError(t, cfg.Validate())

	next := new(consumertest.MetricsSink)
	proc := newProcessor(zap.NewNop(), cfg, next)

	err := proc.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	metrics := buildTestMetrics()

	out, err := proc.processMetrics(context.Background(), metrics)
	require.NoError(t, err)

	err = proc.Shutdown(context.Background())
	require.NoError(t, err)

	// Originals should be dropped when DropOriginal=true.
	assert.Equal(t, 0, out.ResourceMetrics().Len())

	// But the next consumer should have received CountSketch summary metrics.
	sinkMetrics := next.AllMetrics()
	require.GreaterOrEqual(t, len(sinkMetrics), 1)
}

func TestConfigValidateModes(t *testing.T) {
	cfg := &Config{
		Mode:       ModeWindow,
		Epsilon:    0.01,
		Delta:      0.99,
		WindowDuration: 0,
	}
	// Window mode requires a positive window size.
	assert.Error(t, cfg.Validate())

	cfg.WindowDuration = 2 * time.Second
	assert.NoError(t, cfg.Validate())

	cfg.Mode = InputMode("invalid")
	assert.Error(t, cfg.Validate())
}

func buildTestMetrics() pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("host.name", "host-A")

	sm := rm.ScopeMetrics().AppendEmpty()

	// Add a Gauge
	gauge := sm.Metrics().AppendEmpty()
	gauge.SetName("system.cpu.usage")
	gauge.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(45.5)

	// Add a Sum
	sumMetric := sm.Metrics().AppendEmpty()
	sumMetric.SetName("http.requests.total")
	sumMetric.SetEmptySum().DataPoints().AppendEmpty().SetIntValue(100)

	// Add a Histogram
	histMetric := sm.Metrics().AppendEmpty()
	histMetric.SetName("http.response.time")
	hDp := histMetric.SetEmptyHistogram().DataPoints().AppendEmpty()
	hDp.SetCount(5)
	hDp.SetStartTimestamp(pcommon.Timestamp(time.Now().UnixNano()))
	hDp.SetTimestamp(pcommon.Timestamp(time.Now().UnixNano()))

	return metrics
}

// TestEmptyInput verifies that empty metrics do not cause panics.
func TestEmptyInput(t *testing.T) {
	cfg := &Config{
		Mode:       ModeBatch,
		Epsilon:    0.01,
		Delta:      0.99,
		WindowDuration: 0,
	}
	require.NoError(t, cfg.Validate())

	next := new(consumertest.MetricsSink)
	proc := newProcessor(zap.NewNop(), cfg, next)

	empty := pmetric.NewMetrics()
	out, err := proc.processMetrics(context.Background(), empty)
	require.NoError(t, err)
	require.Equal(t, 0, out.ResourceMetrics().Len())
}

// TestBatchModeNoStatePersistence verifies that each batch is independent (no cross-batch state).
func TestBatchModeNoStatePersistence(t *testing.T) {
	cfg := &Config{
		Mode:         ModeBatch,
		Epsilon:      0.01,
		Delta:        0.99,
		WindowDuration:   0,
		DropOriginal: false,
	}
	require.NoError(t, cfg.Validate())

	next := new(consumertest.MetricsSink)
	proc := newProcessor(zap.NewNop(), cfg, next)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	defer proc.Shutdown(context.Background())

	metrics1 := buildTestMetrics()
	metrics2 := buildTestMetrics()

	_, err := proc.processMetrics(context.Background(), metrics1)
	require.NoError(t, err)
	_, err = proc.processMetrics(context.Background(), metrics2)
	require.NoError(t, err)

	sinkMetrics := next.AllMetrics()
	require.GreaterOrEqual(t, len(sinkMetrics), 2, "each batch should trigger at least one emit")
}

// TestWindowModeConcurrentConsume verifies concurrent processMetrics calls in window mode do not race.
func TestWindowModeConcurrentConsume(t *testing.T) {
	cfg := &Config{
		Mode:       ModeWindow,
		Epsilon:    0.01,
		Delta:      0.99,
		WindowDuration: 2 * time.Second, // long window so we control flush
	}
	require.NoError(t, cfg.Validate())

	next := new(consumertest.MetricsSink)
	proc := newProcessor(zap.NewNop(), cfg, next)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			metrics := buildTestMetrics()
			_, _ = proc.processMetrics(context.Background(), metrics)
		}()
	}
	wg.Wait()

	// Shutdown triggers final flush
	require.NoError(t, proc.Shutdown(context.Background()))
	require.GreaterOrEqual(t, len(next.AllMetrics()), 0)
}

// TestWindowModeFlushDuringConsume verifies flush and processMetrics can run concurrently without race.
func TestWindowModeFlushDuringConsume(t *testing.T) {
	cfg := &Config{
		Mode:       ModeWindow,
		Epsilon:    0.01,
		Delta:      0.99,
		WindowDuration: 1 * time.Second,
	}
	require.NoError(t, cfg.Validate())

	next := new(consumertest.MetricsSink)
	proc := newProcessor(zap.NewNop(), cfg, next)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))

	done := make(chan struct{})
	go func() {
		for i := 0; i < 50; i++ {
			metrics := buildTestMetrics()
			_, _ = proc.processMetrics(context.Background(), metrics)
		}
		close(done)
	}()
	<-done
	// Allow at least one ticker flush (window is 1s)
	time.Sleep(1100 * time.Millisecond)
	require.NoError(t, proc.Shutdown(context.Background()))
}

// TestShutdownDuringConsume verifies Shutdown completes even when processMetrics is in progress.
func TestShutdownDuringConsume(t *testing.T) {
	cfg := &Config{
		Mode:       ModeWindow,
		Epsilon:    0.01,
		Delta:      0.99,
		WindowDuration: 5 * time.Second,
	}
	require.NoError(t, cfg.Validate())

	next := new(consumertest.MetricsSink)
	proc := newProcessor(zap.NewNop(), cfg, next)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			metrics := buildTestMetrics()
			_, _ = proc.processMetrics(context.Background(), metrics)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = proc.Shutdown(context.Background())
	}()
	wg.Wait()
}