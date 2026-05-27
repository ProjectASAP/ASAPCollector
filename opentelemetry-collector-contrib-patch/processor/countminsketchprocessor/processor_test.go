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
	"go.opentelemetry.io/collector/pdata/pcommon"
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
				WindowDuration: 10 * time.Second,
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
		WindowDuration: windowDuration,
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

	// Attempt to decode back to sketch. Emit path uses
	// SerializeProtoBytesFO (proto envelope, FrequencyOnly); the
	// matching deserializer is DeserializeCountMinSketchFromProtoBytes.
	sketch, decErr := cms.DeserializeCountMinSketchFromProtoBytes(rawBytes)
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

// cmsTestDataPoint is a test-only adapter that flattens either a
// `CountMinSketchDataPoint` (the typed emission path) or a `NumberDataPoint`
// (the legacy Gauge path kept for non-TransmitSketch mode) into a single
// `pcommon.Map` so existing test assertions that read `sketch_payload`,
// `encoding`, `sample_count`, `rows`, `cols` via `Attributes().Get(...)`
// continue to compile without site-by-site rewrites.
//
// `doubleValue` is only populated for the non-TransmitSketch Gauge path —
// `TestBatchModeQueryMetricsWhenTransmitSketchDisabled` is the single
// test that reads it. For the typed-DP path it stays 0 and is unused.
type cmsTestDataPoint struct {
	attributes  pcommon.Map
	doubleValue float64
}

func (d cmsTestDataPoint) Attributes() pcommon.Map { return d.attributes }
func (d cmsTestDataPoint) DoubleValue() float64    { return d.doubleValue }

// encodingToLegacyString mirrors the strings the processor used to
// write into the `encoding` attribute, so tests that compare against
// "proto_full" / "proto_delta" continue to work.
func encodingToLegacyString(enc pmetric.CountMinSketchEncoding) string {
	switch enc {
	case pmetric.CountMinSketchEncodingProto:
		return "proto_full"
	case pmetric.CountMinSketchEncodingDelta:
		return "proto_delta"
	}
	return "unknown"
}

// sampleCountFromPayload reconstructs the per-DP sample count from the
// serialized CountMin sketch payload. Refactor-2026-05 removed the
// per-DP sample_count field; the count is recoverable from the sketch
// itself: for an unweighted CountMin every row observes all N insertions,
// so the L1 norm of any single row (here the sum of row 0's counters)
// equals the sample count. Returns 0 for payloads that aren't a full
// CountMin state (e.g. proto_delta), which the sample_count assertions
// never read.
func sampleCountFromPayload(payload []byte) int64 {
	sketch, err := cms.DeserializeCountMinSketchFromProtoBytes(payload)
	if err != nil || sketch == nil || sketch.Rows == 0 || len(sketch.Count) == 0 {
		return 0
	}
	var total float64
	for _, c := range sketch.Count[0] {
		total += c
	}
	return int64(total)
}

// getAllDataPoints walks the output metrics and returns a flat slice of
// test adapters, one per sketch data point (typed or legacy Gauge). When
// the input metric is a typed `CountMinSketch`, its fields (sketch bytes,
// encoding enum, sample_count, rows, cols) are synthesized into the
// adapter's attribute map under their legacy string keys so the rest of
// the test file can keep reading them with `Attributes().Get(...)`.
func getAllDataPoints(md pmetric.Metrics) []cmsTestDataPoint {
	var dps []cmsTestDataPoint
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				switch m.Type() {
				case pmetric.MetricTypeCountMinSketch:
					parent := m.CountMinSketch()
					pts := parent.DataPoints()
					for l := 0; l < pts.Len(); l++ {
						dp := pts.At(l)
						attrs := pcommon.NewMap()
						dp.Attributes().CopyTo(attrs)
						// Inject the typed-DP fields under their
						// legacy attribute names so existing tests
						// keep working.
						//
						// Refactor-2026-05: rows/cols moved off the DP onto
						// the parent CountMinSketch container (sent once per
						// emit), and per-DP sample_count was removed entirely
						// (recoverable from the sketch payload). We reconstruct
						// all three from the new API here so the legacy-keyed
						// assertions stay meaningful.
						attrs.PutEmptyBytes("sketch_payload").FromRaw(dp.Sketch())
						attrs.PutStr("encoding", encodingToLegacyString(dp.Encoding()))
						attrs.PutInt("rows", int64(parent.Rows()))
						attrs.PutInt("cols", int64(parent.Cols()))
						attrs.PutInt("sample_count", sampleCountFromPayload(dp.Sketch()))
						dps = append(dps, cmsTestDataPoint{attributes: attrs})
					}
				case pmetric.MetricTypeGauge:
					pts := m.Gauge().DataPoints()
					for l := 0; l < pts.Len(); l++ {
						dp := pts.At(l)
						attrs := pcommon.NewMap()
						dp.Attributes().CopyTo(attrs)
						dps = append(dps, cmsTestDataPoint{
							attributes:  attrs,
							doubleValue: dp.DoubleValue(),
						})
					}
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
		WindowDuration: 0,
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
		WindowDuration: 0,
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
		WindowDuration: 0,
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
		WindowDuration: 5 * time.Second,
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
		WindowDuration: 1 * time.Second,
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
		WindowDuration: 10 * time.Second,
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

// TestAggregateBy verifies that two series sharing the same aggregate_by label values
// are merged into a single sketch output data point.
func TestAggregateBy(t *testing.T) {
	cfg := &Config{
		Mode:           ModeBatch,
		MetricName:     "cms_agg",
		Rows:           5,
		Columns:        128,
		TransmitSketch: true,
		DropOriginal:   true,
		AggregateBy:    []string{"service.name"},
	}
	require.NoError(t, cfg.Validate())

	sink := &mockConsumer{}
	proc := newProcessor(cfg, sink, zap.NewNop())

	// Two data points: same service.name but different host labels.
	// With aggregate_by=["service.name"] they should collapse to one sketch.
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("http_requests_total")
	m.SetEmptyGauge()

	dp1 := m.Gauge().DataPoints().AppendEmpty()
	dp1.SetIntValue(1)
	dp1.Attributes().PutStr("service.name", "frontend")
	dp1.Attributes().PutStr("host", "host-A")

	dp2 := m.Gauge().DataPoints().AppendEmpty()
	dp2.SetIntValue(1)
	dp2.Attributes().PutStr("service.name", "frontend")
	dp2.Attributes().PutStr("host", "host-B")

	out, err := proc.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)

	dps := getAllDataPoints(out)
	// Both data points share service.name="frontend" → merged into exactly one sketch.
	assert.Len(t, dps, 1, "two series with same aggregate_by key should produce one output data point")

	// Output attribute should carry only the aggregate_by label.
	svcVal, ok := dps[0].Attributes().Get("service.name")
	assert.True(t, ok)
	assert.Equal(t, "frontend", svcVal.Str())
	_, hasHost := dps[0].Attributes().Get("host")
	assert.False(t, hasHost, "non-aggregate_by labels should be dropped from output")
}

// TestLabelMatchers verifies that data points not matching label_matchers are excluded.
func TestLabelMatchers(t *testing.T) {
	cfg := &Config{
		Mode:           ModeBatch,
		MetricName:     "cms_filtered",
		Rows:           5,
		Columns:        128,
		TransmitSketch: false,
		DropOriginal:   true,
		LabelMatchers:  []LabelMatcher{{Key: "env", Value: "prod"}},
	}
	require.NoError(t, cfg.Validate())

	sink := &mockConsumer{}
	proc := newProcessor(cfg, sink, zap.NewNop())

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("http_requests_total")
	m.SetEmptyGauge()

	// This point matches the matcher.
	dpProd := m.Gauge().DataPoints().AppendEmpty()
	dpProd.SetIntValue(1)
	dpProd.Attributes().PutStr("env", "prod")

	// This point does not match.
	dpDev := m.Gauge().DataPoints().AppendEmpty()
	dpDev.SetIntValue(1)
	dpDev.Attributes().PutStr("env", "dev")

	out, err := proc.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)

	dps := getAllDataPoints(out)
	require.Len(t, dps, 1)
	// sample_count should reflect only the 1 matching point.
	count, ok := dps[0].Attributes().Get("sample_count")
	require.True(t, ok)
	assert.Equal(t, int64(1), count.Int())
}

// TestRoundTripIngestProtoSketch proves that an inbound CountMinSketch
// payload produced by this processor's emit path (proto-encoded
// SketchEnvelope via `serializeCMS`/`SerializeProtoBytesFO`) is accepted
// by the same processor's ingest path. Pre-fix the ingest path called
// `cms.DeserializeCountMinSketchFromBytes` (gob), which silently rejected
// the proto bytes that production traffic carries; this test would have
// caught that asymmetry.
func TestRoundTripIngestProtoSketch(t *testing.T) {
	cfg := &Config{
		Mode:           ModeBatch,
		MetricName:     "cms_roundtrip",
		Rows:           5,
		Columns:        128,
		TransmitSketch: true,
		DropOriginal:   true,
	}
	require.NoError(t, cfg.Validate())

	// Build a source CMS sketch with N inserts at known hashes and
	// serialize it via the exact emit-side path.
	src, err := cms.NewCountMinSketch(cfg.Rows, cfg.Columns)
	require.NoError(t, err)
	const numInserts = 7
	for i := 0; i < numInserts; i++ {
		src.InsertWithHash(uint64(i + 1))
	}
	payload, err := serializeCMS(src)
	require.NoError(t, err)
	require.NotEmpty(t, payload)

	// Sanity: the matching proto deserializer round-trips the bytes
	// back to the same dimensions and per-hash frequency estimates.
	roundTripped, err := deserializeCMS(payload)
	require.NoError(t, err)
	assert.Equal(t, src.Rows, roundTripped.Rows)
	assert.Equal(t, src.Cols, roundTripped.Cols)
	for i := 0; i < numInserts; i++ {
		assert.Equal(t,
			src.FastEstimateWithHash(uint64(i+1)),
			roundTripped.FastEstimateWithHash(uint64(i+1)),
			"per-hash estimate mismatch after direct proto round-trip")
	}

	// Feed the proto-encoded sketch back through the processor as a
	// typed CountMinSketch input. The processor must (a) accept the
	// proto bytes on ingest, (b) merge the inbound sketch into a fresh
	// batch sketch, and (c) emit a typed CountMinSketchDataPoint whose
	// payload decodes to the same per-hash estimates as the source.
	sink := &mockConsumer{}
	proc := newProcessor(cfg, sink, zap.NewNop())

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	metric := sm.Metrics().AppendEmpty()
	metric.SetName("cms_roundtrip")
	// Refactor-2026-05: rows/cols moved onto the parent CountMinSketch
	// container (sent once per emit) and per-DP sample_count was removed
	// (recoverable from the payload). The sketch bytes still carry the
	// dimensions, which is what the decode path reads.
	parent := metric.SetEmptyCountMinSketch()
	parent.SetRows(int32(cfg.Rows))
	parent.SetCols(int32(cfg.Columns))
	in := parent.DataPoints().AppendEmpty()
	in.SetSketch(payload)
	in.SetEncoding(pmetric.CountMinSketchEncodingProto)

	out, err := proc.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)

	dps := getAllDataPoints(out)
	require.Len(t, dps, 1, "expected one emitted CMS sketch metric")

	encoding, ok := dps[0].Attributes().Get("encoding")
	require.True(t, ok)
	assert.Equal(t, "proto_full", encoding.Str())

	payloadVal, ok := dps[0].Attributes().Get("sketch_payload")
	require.True(t, ok, "sketch_payload must be present on emitted DP")
	mergedBytes := payloadVal.Bytes().AsRaw()
	require.NotEmpty(t, mergedBytes)

	mergedOut, err := cms.DeserializeCountMinSketchFromProtoBytes(mergedBytes)
	require.NoError(t, err)
	assert.Equal(t, cfg.Rows, mergedOut.Rows)
	assert.Equal(t, cfg.Columns, mergedOut.Cols)
	// Merging the inbound sketch into a fresh empty batch sketch yields
	// per-hash estimates equal to the source's.
	for i := 0; i < numInserts; i++ {
		assert.Equal(t,
			src.FastEstimateWithHash(uint64(i+1)),
			mergedOut.FastEstimateWithHash(uint64(i+1)),
			"per-hash estimate mismatch after full ingest→emit round-trip")
	}
}
