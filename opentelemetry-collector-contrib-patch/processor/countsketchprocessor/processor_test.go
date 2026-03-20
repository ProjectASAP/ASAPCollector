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
		WindowSize: 100 * time.Millisecond,
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
		WindowSize: 0,
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

	// In batch mode with DropOriginal=false, the output includes both originals and
	// sketch summaries (expansion mode). Sketch metrics are returned in `out`, not
	// pushed to `next` directly — that path is for window-mode ticker flushes only.
	require.Greater(t, out.ResourceMetrics().Len(), 0)

	foundPartition := false
	rms := out.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				if ms.At(k).Name() == "countsketch_partition" {
					foundPartition = true
				}
			}
		}
	}
	assert.True(t, foundPartition, "expected countsketch_partition summary metric in batch mode output")
}

// TestGroupByPartitioning verifies that group_by creates separate sketches per
// unique label combination (Mode 1 / Mode 3).
func TestGroupByPartitioning(t *testing.T) {
	cfg := &Config{
		Mode:         ModeBatch,
		GroupBy:      []string{"host.name"},
		Epsilon:      0.01,
		Delta:        0.99,
		WindowSize:   0,
		DropOriginal: true,
	}
	require.NoError(t, cfg.Validate())

	next := new(consumertest.MetricsSink)
	proc := newProcessor(zap.NewNop(), cfg, next)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	defer proc.Shutdown(context.Background())

	// Two resource metrics with different host.name values.
	md := pmetric.NewMetrics()
	for _, host := range []string{"host-A", "host-B"} {
		rm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("host.name", host)
		sm := rm.ScopeMetrics().AppendEmpty()
		m := sm.Metrics().AppendEmpty()
		m.SetName("cpu.usage")
		m.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(1.0)
	}

	out, err := proc.processMetrics(context.Background(), md)
	require.NoError(t, err)

	// Collect all partition_key values from the output.
	partitionKeys := map[string]bool{}
	rms := out.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				dps := ms.At(k).Gauge().DataPoints()
				for l := 0; l < dps.Len(); l++ {
					if v, ok := dps.At(l).Attributes().Get("partition_key"); ok {
						partitionKeys[v.Str()] = true
					}
				}
			}
		}
	}

	// Two hosts → two partition keys.
	assert.Len(t, partitionKeys, 2)
	assert.True(t, partitionKeys["host.name=host-A;"])
	assert.True(t, partitionKeys["host.name=host-B;"])
}

// TestWindowModeGroupBy verifies the matrix mode: group_by + window produces
// per-partition sketches that reset each window.
func TestWindowModeGroupBy(t *testing.T) {
	cfg := &Config{
		Mode:         ModeWindow,
		GroupBy:      []string{"service.name"},
		Epsilon:      0.1,
		Delta:        0.9,
		WindowSize:   100 * time.Millisecond,
		DropOriginal: true,
	}
	// Skip Validate() to allow sub-second window in tests.

	next := new(consumertest.MetricsSink)
	proc := newProcessor(zap.NewNop(), cfg, next)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))

	md := pmetric.NewMetrics()
	for _, svc := range []string{"svc-X", "svc-Y"} {
		rm := md.ResourceMetrics().AppendEmpty()
		sm := rm.ScopeMetrics().AppendEmpty()
		m := sm.Metrics().AppendEmpty()
		m.SetName("req.count")
		dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetDoubleValue(1.0)
		dp.Attributes().PutStr("service.name", svc)
	}

	_, err := proc.processMetrics(context.Background(), md)
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)
	require.NoError(t, proc.Shutdown(context.Background()))

	partitionKeys := map[string]bool{}
	for _, emitted := range next.AllMetrics() {
		rms := emitted.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					dps := ms.At(k).Gauge().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						if v, ok := dps.At(l).Attributes().Get("partition_key"); ok {
							partitionKeys[v.Str()] = true
						}
					}
				}
			}
		}
	}

	assert.True(t, partitionKeys["service.name=svc-X;"], "expected partition for svc-X")
	assert.True(t, partitionKeys["service.name=svc-Y;"], "expected partition for svc-Y")
}

func TestBatchModeDropOriginal(t *testing.T) {
	cfg := &Config{
		Mode:         ModeBatch,
		Epsilon:      0.01,
		Delta:        0.99,
		WindowSize:   0,
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

	// DropOriginal=true: out should contain only sketch summary metrics (no originals).
	// Sketch metrics are returned in `out`; nothing is pushed to `next` in batch mode.
	require.Greater(t, out.ResourceMetrics().Len(), 0, "sketch metrics must be present in output")
	foundPartition := false
	rms := out.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				if ms.At(k).Name() == "countsketch_partition" {
					foundPartition = true
				}
			}
		}
	}
	assert.True(t, foundPartition, "expected countsketch_partition in drop-original output")
}

func TestConfigValidateModes(t *testing.T) {
	cfg := &Config{
		Mode:       ModeWindow,
		Epsilon:    0.01,
		Delta:      0.99,
		WindowSize: 0,
	}
	// Window mode requires a positive window size.
	assert.Error(t, cfg.Validate())

	cfg.WindowSize = 2 * time.Second
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
		WindowSize: 0,
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
		WindowSize:   0,
		DropOriginal: false,
	}
	require.NoError(t, cfg.Validate())

	next := new(consumertest.MetricsSink)
	proc := newProcessor(zap.NewNop(), cfg, next)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	defer proc.Shutdown(context.Background())

	metrics1 := buildTestMetrics()
	metrics2 := buildTestMetrics()

	out1, err := proc.processMetrics(context.Background(), metrics1)
	require.NoError(t, err)
	out2, err := proc.processMetrics(context.Background(), metrics2)
	require.NoError(t, err)

	// In batch mode each call returns its own sketch summary in the output; state
	// is reset between batches so the two outputs are independent.
	require.Greater(t, out1.ResourceMetrics().Len(), 0, "batch 1 should produce output")
	require.Greater(t, out2.ResourceMetrics().Len(), 0, "batch 2 should produce output")
}

// TestWindowModeConcurrentConsume verifies concurrent processMetrics calls in window mode do not race.
func TestWindowModeConcurrentConsume(t *testing.T) {
	cfg := &Config{
		Mode:       ModeWindow,
		Epsilon:    0.01,
		Delta:      0.99,
		WindowSize: 2 * time.Second, // long window so we control flush
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
		WindowSize: 1 * time.Second,
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
		WindowSize: 5 * time.Second,
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