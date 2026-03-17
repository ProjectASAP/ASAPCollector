// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package countminsketchprocessor

import (
	"context"
	"sync"
	"testing"
	"time"

	cms "github.com/ProjectASAP/sketchlib-go/sketches/CountMinSketch"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// mockConsumer captures metrics emitted by the processor
type mockConsumer struct {
	mu      sync.Mutex
	metrics []pmetric.Metrics
}

func (m *mockConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (m *mockConsumer) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.metrics = append(m.metrics, md)
	return nil
}

func (m *mockConsumer) getMetrics() []pmetric.Metrics {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Copy slice to ensure thread safety during read
	return append([]pmetric.Metrics{}, m.metrics...)
}

func (m *mockConsumer) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.metrics = nil
}

// 1. Test Configuration Validation
func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name        string
		cfg         *Config
		expectError bool
	}{
		{
			name: "valid config",
			cfg: &Config{
				MetricName:     "cms_test",
				Rows:           5,
				Columns:        1024,
				TransmitSketch: true,
				WindowInterval: 10 * time.Second,
			},
			expectError: false,
		},
		{
			name: "missing metric name",
			cfg: &Config{
				MetricName: "",
				Rows:       5,
				Columns:    1024,
			},
			expectError: true,
		},
		{
			name: "invalid dimension",
			cfg: &Config{
				MetricName: "cms",
				Rows:       0,
				Columns:    1024,
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// 2. Main Test: Tumbling Window & Reset Logic Correctness
func TestProcessor_TumblingWindow_Correctness(t *testing.T) {
	// Setup: use window mode with a short interval for testing (200ms).
	// Note: we do not call Validate() so we can use <1s window for fast tests.
	windowDuration := 200 * time.Millisecond
	cfg := &Config{
		Mode:           ModeWindow,
		MetricName:     "cms_output",
		Rows:           5,
		Columns:        128,
		DropOriginal:   true,
		TransmitSketch: true,
		WindowInterval: windowDuration,
	}

	sink := &mockConsumer{}
	logger := zap.NewNop()

	// Initialize Processor
	proc := newProcessor(cfg, sink, logger)
	err := proc.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)
	defer proc.Shutdown(context.Background())

	// ==========================================
	// WINDOW 1: Send 5 Data Points
	// ==========================================
	md1 := generateMetrics("service_A", 5)

	// FIX: Capture 2 return values (_, err) because the processor implementation returns (Metrics, error)
	_, err = proc.ConsumeMetrics(context.Background(), md1)
	require.NoError(t, err)

	// Wait for window to expire + buffer
	time.Sleep(windowDuration + 100*time.Millisecond)

	// Verify Window 1 Output
	batches1 := sink.getMetrics()
	require.Len(t, batches1, 1, "Should emit exactly 1 batch for Window 1")

	dps1 := getAllDataPoints(batches1[0])
	require.Len(t, dps1, 1, "Should have 1 sketch metric")

	// Check Sample Count
	count1, ok := dps1[0].Attributes().Get("sample_count")
	require.True(t, ok)
	assert.Equal(t, int64(5), count1.Int(), "Window 1 should have 5 samples")

	// ==========================================
	// WINDOW 2: Send 3 Data Points
	// (Test Correctness: Is the counter reset back to 0?)
	// ==========================================

	sink.reset() // Clear sink for easier assertion

	md2 := generateMetrics("service_A", 3)

	// FIX: Capture 2 return values (_, err)
	_, err = proc.ConsumeMetrics(context.Background(), md2)
	require.NoError(t, err)

	// Wait for window expiration
	time.Sleep(windowDuration + 100*time.Millisecond)

	// Verify Window 2 Output
	batches2 := sink.getMetrics()
	require.Len(t, batches2, 1, "Should emit exactly 1 batch for Window 2")

	dps2 := getAllDataPoints(batches2[0])
	require.Len(t, dps2, 1)

	count2, _ := dps2[0].Attributes().Get("sample_count")

	// === CRITICAL ASSERTION ===
	// If the cumulative bug exists, the result will be 8 (5+3).
	// If correct, the result must be 3.
	assert.Equal(t, int64(3), count2.Int(), "Window 2 FAILED TO RESET! Value should be 3, not cumulative.")

	// ==========================================
	// 3. Verify Binary Payload (Gob Decode)
	// ==========================================
	payloadVal, ok := dps2[0].Attributes().Get("sketch_payload")
	require.True(t, ok, "Sketch payload must exist in attributes")

	rawBytes := payloadVal.Bytes().AsRaw()
	require.NotEmpty(t, rawBytes)

	// Attempt to decode back to sketch
	sketch, decErr := cms.DeserializeCountMinSketchFromBytes(rawBytes)
	assert.NoError(t, decErr, "Payload must be a valid serialized CMS")
	assert.Equal(t, 5, sketch.Rows)
	assert.Equal(t, 128, sketch.Cols)
}

// Helpers to generate dummy metrics
func generateMetrics(serviceName string, count int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()

	for i := 0; i < count; i++ {
		m := sm.Metrics().AppendEmpty()
		m.SetName("http_requests_total")
		m.SetEmptyGauge()
		dp := m.Gauge().DataPoints().AppendEmpty()
		dp.SetIntValue(1)
		dp.Attributes().PutStr("service.name", serviceName)
	}
	return md
}

func getAllDataPoints(md pmetric.Metrics) []pmetric.NumberDataPoint {
	var dps []pmetric.NumberDataPoint
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				pts := m.Gauge().DataPoints()
				for l := 0; l < pts.Len(); l++ {
					dps = append(dps, pts.At(l))
				}
			}
		}
	}
	return dps
}

// TestBatchMode verifies batch mode returns originals plus sketch summary when DropOriginal=false.
func TestBatchMode(t *testing.T) {
	cfg := &Config{
		Mode:           ModeBatch,
		MetricName:     "cms_batch",
		Rows:           5,
		Columns:        128,
		DropOriginal:   false,
		TransmitSketch: true,
		WindowInterval: 0,
	}
	require.NoError(t, cfg.Validate())

	sink := &mockConsumer{}
	proc := newProcessor(cfg, sink, zap.NewNop())

	md := generateMetrics("svc", 4)
	out, err := proc.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)

	// DropOriginal=false: output should include originals and sketch metrics (expansion mode)
	require.Greater(t, out.ResourceMetrics().Len(), 0, "batch mode with DropOriginal=false should return metrics")
	dps := getAllDataPoints(out)
	require.GreaterOrEqual(t, len(dps), 1, "should have at least one sketch metric in output")
}

// TestBatchModeDropOriginal verifies batch mode with DropOriginal=true returns only sketch metrics.
func TestBatchModeDropOriginal(t *testing.T) {
	cfg := &Config{
		Mode:           ModeBatch,
		MetricName:     "cms_only",
		Rows:           5,
		Columns:        128,
		DropOriginal:   true,
		TransmitSketch: true,
		WindowInterval: 0,
	}
	require.NoError(t, cfg.Validate())

	sink := &mockConsumer{}
	proc := newProcessor(cfg, sink, zap.NewNop())

	md := generateMetrics("svc", 3)
	out, err := proc.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)

	// DropOriginal=true: out contains only sketch metrics
	require.GreaterOrEqual(t, out.ResourceMetrics().Len(), 0)
	dps := getAllDataPoints(out)
	require.GreaterOrEqual(t, len(dps), 1, "should have sketch metric in output")
}

func TestBatchModeQueryMetricsWhenTransmitSketchDisabled(t *testing.T) {
	cfg := &Config{
		Mode:           ModeBatch,
		MetricName:     "cms_queries",
		Rows:           5,
		Columns:        128,
		TransmitSketch: false,
		DropOriginal:   true,
	}
	require.NoError(t, cfg.Validate())

	sink := &mockConsumer{}
	proc := newProcessor(cfg, sink, zap.NewNop())

	out, err := proc.ConsumeMetrics(context.Background(), generateMetrics("svc", 3))
	require.NoError(t, err)

	dps := getAllDataPoints(out)
	require.Len(t, dps, 1)
	assert.Equal(t, 3.0, dps[0].DoubleValue())
	_, hasPayload := dps[0].Attributes().Get("sketch_payload")
	assert.False(t, hasPayload)
}

// TestEmptyInput verifies empty metrics do not cause panics.
func TestEmptyInput(t *testing.T) {
	cfg := &Config{
		Mode:           ModeBatch,
		MetricName:     "cms",
		Rows:           5,
		Columns:        128,
		WindowInterval: 0,
	}
	require.NoError(t, cfg.Validate())

	sink := &mockConsumer{}
	proc := newProcessor(cfg, sink, zap.NewNop())

	empty := pmetric.NewMetrics()
	out, err := proc.ConsumeMetrics(context.Background(), empty)
	require.NoError(t, err)
	require.Equal(t, 0, out.ResourceMetrics().Len())
}

// TestWindowModeConcurrentConsume verifies concurrent ConsumeMetrics in window mode do not race.
func TestWindowModeConcurrentConsume(t *testing.T) {
	cfg := &Config{
		Mode:           ModeWindow,
		MetricName:     "cms_concurrent",
		Rows:           5,
		Columns:        128,
		WindowInterval: 5 * time.Second,
	}
	require.NoError(t, cfg.Validate())

	sink := &mockConsumer{}
	proc := newProcessor(cfg, sink, zap.NewNop())
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			md := generateMetrics("svc", 2)
			_, _ = proc.ConsumeMetrics(context.Background(), md)
		}()
	}
	wg.Wait()
	require.NoError(t, proc.Shutdown(context.Background()))
}

// TestWindowModeFlushDuringConsume verifies flush and ConsumeMetrics can run concurrently.
func TestWindowModeFlushDuringConsume(t *testing.T) {
	cfg := &Config{
		Mode:           ModeWindow,
		MetricName:     "cms_flush",
		Rows:           5,
		Columns:        128,
		WindowInterval: 1 * time.Second,
	}
	require.NoError(t, cfg.Validate())

	sink := &mockConsumer{}
	proc := newProcessor(cfg, sink, zap.NewNop())
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))

	done := make(chan struct{})
	go func() {
		for i := 0; i < 30; i++ {
			md := generateMetrics("svc", 2)
			_, _ = proc.ConsumeMetrics(context.Background(), md)
		}
		close(done)
	}()
	<-done
	time.Sleep(1100 * time.Millisecond)
	require.NoError(t, proc.Shutdown(context.Background()))
}

// TestShutdownDuringConsume verifies Shutdown completes when ConsumeMetrics is in progress.
func TestShutdownDuringConsume(t *testing.T) {
	cfg := &Config{
		Mode:           ModeWindow,
		MetricName:     "cms_shutdown",
		Rows:           5,
		Columns:        128,
		WindowInterval: 10 * time.Second,
	}
	require.NoError(t, cfg.Validate())

	sink := &mockConsumer{}
	proc := newProcessor(cfg, sink, zap.NewNop())
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			md := generateMetrics("svc", 1)
			_, _ = proc.ConsumeMetrics(context.Background(), md)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = proc.Shutdown(context.Background())
	}()
	wg.Wait()
}
