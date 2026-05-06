// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package gorillas3processor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap/zaptest"
)

// mockSink captures every PutChunk for assertion.
type mockSink struct {
	mu     sync.Mutex
	chunks []mockChunk
	fail   bool
}

type mockChunk struct {
	key   string
	data  []byte
	hints chunkHints
}

func (m *mockSink) PutChunk(ctx context.Context, key string, data []byte, hints chunkHints) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail {
		return errors.New("mock sink: induced failure")
	}
	m.chunks = append(m.chunks, mockChunk{key: key, data: append([]byte(nil), data...), hints: hints})
	return nil
}

func (m *mockSink) Close() error { return nil }

func (m *mockSink) chunkCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.chunks)
}

func buildTestMetrics(metricName string, n int, baseTime time.Time) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName(metricName)
	g := m.SetEmptyGauge()
	for i := 0; i < n; i++ {
		dp := g.DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.NewTimestampFromTime(baseTime.Add(time.Duration(i) * time.Second)))
		dp.SetDoubleValue(float64(i) * 1.5)
		dp.Attributes().PutStr("host", "test-host")
	}
	return md
}

func mkProcessor(t *testing.T, cfg *Config, sink chunkSink) *gorillaS3Processor {
	t.Helper()
	require.NoError(t, cfg.Validate())
	return newProcessor(cfg, nil, zaptest.NewLogger(t), sink)
}

func TestConsumeMetrics_DropOriginal(t *testing.T) {
	cfg := &Config{Bucket: "b", WindowInterval: time.Hour, DropOriginal: true}
	sink := &mockSink{}
	p := mkProcessor(t, cfg, sink)

	md := buildTestMetrics("cpu.usage", 5, time.Now())
	out, err := p.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)
	assert.Equal(t, 0, out.ResourceMetrics().Len(), "drop_original=true should emit empty metrics")
	// Buffered though.
	assert.Equal(t, int64(1), p.activeSeries())
}

func TestConsumeMetrics_PassThrough(t *testing.T) {
	cfg := &Config{Bucket: "b", WindowInterval: time.Hour, DropOriginal: false}
	sink := &mockSink{}
	p := mkProcessor(t, cfg, sink)

	md := buildTestMetrics("cpu.usage", 5, time.Now())
	out, err := p.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)
	require.Equal(t, 1, out.ResourceMetrics().Len())
	dps := out.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints()
	assert.Equal(t, 5, dps.Len())
}

func TestFlushWindow_WritesChunkOnTick(t *testing.T) {
	cfg := &Config{Bucket: "b", WindowInterval: time.Hour, DropOriginal: true, Tenant: "tnt"}
	sink := &mockSink{}
	p := mkProcessor(t, cfg, sink)

	base := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
	md := buildTestMetrics("cpu.usage", 10, base)
	_, err := p.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)

	p.flushWindow(context.Background())

	require.Equal(t, 1, sink.chunkCount())
	c := sink.chunks[0]
	assert.Contains(t, c.key, "tnt/cpu.usage/2026/05/06/")
	assert.Contains(t, c.key, ".gor")
	assert.Equal(t, "cpu.usage", c.hints.MetricName)
	assert.Equal(t, "tnt", c.hints.Tenant)
	assert.Equal(t, 1, c.hints.SeriesCount)
	assert.Equal(t, 10, c.hints.PointCount)
	// Decode the chunk to validate body.
	got := decodeChunk(t, c.data)
	require.Len(t, got, 1)
	assert.Equal(t, "cpu.usage", got[0].meta.MetricName)
	assert.Equal(t, 10, got[0].meta.PointCount)
	// After flush, series buffer must be empty.
	assert.Equal(t, int64(0), p.activeSeries())
}

func TestFlushWindow_PutFailureLogged(t *testing.T) {
	cfg := &Config{Bucket: "b", WindowInterval: time.Hour, DropOriginal: true}
	sink := &mockSink{fail: true}
	p := mkProcessor(t, cfg, sink)

	base := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
	md := buildTestMetrics("cpu.usage", 5, base)
	_, err := p.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)
	p.flushWindow(context.Background())
	// No chunk recorded since sink failed.
	assert.Equal(t, 0, sink.chunkCount())
}

func TestFlushWindow_EmptyNoOp(t *testing.T) {
	cfg := &Config{Bucket: "b", WindowInterval: time.Hour}
	sink := &mockSink{}
	p := mkProcessor(t, cfg, sink)
	p.flushWindow(context.Background())
	assert.Equal(t, 0, sink.chunkCount())
}

func TestShutdown_DrainsBufferedSamples(t *testing.T) {
	cfg := &Config{Bucket: "b", WindowInterval: time.Hour, DropOriginal: true}
	sink := &mockSink{}
	p := mkProcessor(t, cfg, sink)

	base := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
	md := buildTestMetrics("cpu.usage", 4, base)
	_, err := p.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)

	// Manually start the loop so shutdown can stop it cleanly.
	require.NoError(t, p.Start(context.Background(), nil))
	require.NoError(t, p.Shutdown(context.Background()))
	assert.Equal(t, 1, sink.chunkCount(), "shutdown should drain buffered points")
}

func TestConsumeMetrics_SumType(t *testing.T) {
	cfg := &Config{Bucket: "b", WindowInterval: time.Hour, DropOriginal: true}
	sink := &mockSink{}
	p := mkProcessor(t, cfg, sink)

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("http.requests")
	s := m.SetEmptySum()
	for i := 0; i < 3; i++ {
		dp := s.DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now().Add(time.Duration(i) * time.Second)))
		dp.SetIntValue(int64(i * 100))
	}
	_, err := p.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)
	assert.Equal(t, int64(1), p.activeSeries())
	p.flushWindow(context.Background())
	require.Equal(t, 1, sink.chunkCount())
	got := decodeChunk(t, sink.chunks[0].data)
	require.Len(t, got, 1)
	assert.Equal(t, []float64{0, 100, 200}, got[0].vals)
}

func TestConsumeMetrics_HistogramSilentlyIgnored(t *testing.T) {
	// Only Gauge / Sum are encoded; histogram-style metrics must be skipped.
	cfg := &Config{Bucket: "b", WindowInterval: time.Hour, DropOriginal: true}
	sink := &mockSink{}
	p := mkProcessor(t, cfg, sink)

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("latency")
	h := m.SetEmptyHistogram()
	dp := h.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	_, err := p.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)
	assert.Equal(t, int64(0), p.activeSeries())
}

func TestFactory_DefaultConfig(t *testing.T) {
	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	assert.Equal(t, 60*time.Second, cfg.WindowInterval)
	assert.True(t, cfg.DropOriginal)
	assert.Equal(t, defaultPrefixTemplate, cfg.PrefixTemplate)
}

func TestFactory_TypeRegistered(t *testing.T) {
	f := NewFactory()
	assert.Equal(t, Type, f.Type())
}
