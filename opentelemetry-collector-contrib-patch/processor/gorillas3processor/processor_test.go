// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package gorillas3processor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap/zaptest"
)

// mockSink captures every PutTSDBBlock for assertion.
type mockSink struct {
	mu         sync.Mutex
	tsdbBlocks []mockTSDBBlock
	fail       bool
}

// mvp/step2.1: Prometheus TSDB block capture.
type mockTSDBBlock struct {
	ulid  string
	files map[string][]byte
}

func (m *mockSink) PutTSDBBlock(ctx context.Context, blockULID string, files map[string][]byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail {
		return errors.New("mock sink: induced failure")
	}
	clone := make(map[string][]byte, len(files))
	for k, v := range files {
		clone[k] = append([]byte(nil), v...)
	}
	m.tsdbBlocks = append(m.tsdbBlocks, mockTSDBBlock{ulid: blockULID, files: clone})
	return nil
}

func (m *mockSink) Close() error { return nil }

func (m *mockSink) tsdbBlockCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.tsdbBlocks)
}

func writeMockTSDBBlock(t *testing.T, block mockTSDBBlock) string {
	t.Helper()
	root := t.TempDir()
	for k, body := range block.files {
		full := filepath.Join(root, filepath.FromSlash(k))
		require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o755))
		require.NoError(t, os.WriteFile(full, body, 0o644))
	}
	return filepath.Join(root, block.ulid)
}

type rtSeries struct {
	labels  labels.Labels
	samples []rtSample
}

type rtSample struct {
	t int64
	v float64
}

// readAllSamples opens a Prometheus TSDB block on disk and returns
// the round-trip view of every (label, ts, value) tuple. Used by
// tests that finalize a TSDB block via the live processor and want
// to assert the canonical reader can re-derive the input.
func readAllSamples(t *testing.T, block *tsdb.Block) []rtSeries {
	t.Helper()
	q, err := tsdb.NewBlockQuerier(block, block.MinTime(), block.MaxTime())
	require.NoError(t, err)
	defer q.Close()

	var out []rtSeries
	ss := q.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".+"))
	for ss.Next() {
		s := ss.At()
		ls := s.Labels()
		var samples []rtSample
		it := s.Iterator(nil)
		for it.Next() == chunkenc.ValFloat {
			ts, val := it.At()
			samples = append(samples, rtSample{t: ts, v: val})
		}
		require.NoError(t, it.Err())
		out = append(out, rtSeries{labels: ls, samples: samples})
	}
	require.NoError(t, ss.Err())
	return out
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
	cfg := &Config{TSDBBucket: "b", WindowInterval: time.Hour, DropOriginal: true}
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
	cfg := &Config{TSDBBucket: "b", WindowInterval: time.Hour, DropOriginal: false}
	sink := &mockSink{}
	p := mkProcessor(t, cfg, sink)

	md := buildTestMetrics("cpu.usage", 5, time.Now())
	out, err := p.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)
	require.Equal(t, 1, out.ResourceMetrics().Len())
	dps := out.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints()
	assert.Equal(t, 5, dps.Len())
}

func TestFlushWindow_WritesTSDBBlockOnTick(t *testing.T) {
	cfg := &Config{TSDBBucket: "b", WindowInterval: time.Hour, DropOriginal: true, Tenant: "tnt"}
	sink := &mockSink{}
	p := mkProcessor(t, cfg, sink)

	base := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
	md := buildTestMetrics("cpu.usage", 10, base)
	_, err := p.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)

	p.flushWindow(context.Background())

	require.Equal(t, 1, sink.tsdbBlockCount())
	block := sink.tsdbBlocks[0]
	require.NotEmpty(t, block.files)
	blockDir := writeMockTSDBBlock(t, block)
	opened, err := tsdb.OpenBlock(nil, blockDir, chunkenc.NewPool(), nil)
	require.NoError(t, err)
	defer opened.Close()

	got := readAllSamples(t, opened)
	require.Len(t, got, 1)
	assert.Equal(t, "cpu.usage", got[0].labels.Get(labels.MetricName))
	assert.Equal(t, 10, len(got[0].samples))
	assert.Equal(t, int64(0), p.activeSeries())
}

func TestFlushWindow_PutFailureLogged(t *testing.T) {
	cfg := &Config{TSDBBucket: "b", WindowInterval: time.Hour, DropOriginal: true}
	sink := &mockSink{fail: true}
	p := mkProcessor(t, cfg, sink)

	base := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
	md := buildTestMetrics("cpu.usage", 5, base)
	_, err := p.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)
	p.flushWindow(context.Background())
	assert.Equal(t, 0, sink.tsdbBlockCount())
}

func TestFlushWindow_EmptyNoOp(t *testing.T) {
	cfg := &Config{TSDBBucket: "b", WindowInterval: time.Hour}
	sink := &mockSink{}
	p := mkProcessor(t, cfg, sink)
	p.flushWindow(context.Background())
	assert.Equal(t, 0, sink.tsdbBlockCount())
}

func TestShutdown_DrainsBufferedSamples(t *testing.T) {
	cfg := &Config{TSDBBucket: "b", WindowInterval: time.Hour, DropOriginal: true}
	sink := &mockSink{}
	p := mkProcessor(t, cfg, sink)

	base := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
	md := buildTestMetrics("cpu.usage", 4, base)
	_, err := p.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)

	// Manually start the loop so shutdown can stop it cleanly.
	require.NoError(t, p.Start(context.Background(), nil))
	require.NoError(t, p.Shutdown(context.Background()))
	assert.Equal(t, 1, sink.tsdbBlockCount(), "shutdown should drain buffered points")
}

func TestConsumeMetrics_SumType(t *testing.T) {
	cfg := &Config{TSDBBucket: "b", WindowInterval: time.Hour, DropOriginal: true}
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
	require.Equal(t, 1, sink.tsdbBlockCount())
	blockDir := writeMockTSDBBlock(t, sink.tsdbBlocks[0])
	opened, err := tsdb.OpenBlock(nil, blockDir, chunkenc.NewPool(), nil)
	require.NoError(t, err)
	defer opened.Close()
	got := readAllSamples(t, opened)
	require.Len(t, got, 1)
	require.Len(t, got[0].samples, 3)
	assert.Equal(t, []float64{0, 100, 200}, []float64{got[0].samples[0].v, got[0].samples[1].v, got[0].samples[2].v})
}

func TestConsumeMetrics_HistogramSilentlyIgnored(t *testing.T) {
	// Only Gauge / Sum are encoded; histogram-style metrics must be skipped.
	cfg := &Config{TSDBBucket: "b", WindowInterval: time.Hour, DropOriginal: true}
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

func TestAgentRole_EmitsEncodedFragments(t *testing.T) {
	cfg := &Config{
		Role:                    ProcessorRoleAgent,
		DeliveryMode:            DeliveryModeBestEffort,
		WindowInterval:          time.Hour,
		DropOriginal:            true,
		TSDBReorderGrace:        time.Nanosecond,
		FragmentSamplesPerChunk: 2,
		SourceID:                "edge-a",
	}
	p := mkProcessor(t, cfg, nil)

	base := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
	out, err := p.ConsumeMetrics(context.Background(), buildTestMetrics("cpu.usage", 2, base))
	require.NoError(t, err)

	fragments, err := extractFragments(out)
	require.NoError(t, err)
	require.Len(t, fragments, 1)
	assert.Equal(t, "cpu.usage", fragments[0].MetricName)
	assert.Equal(t, "edge-a", fragments[0].Source)
	assert.Equal(t, 2, fragments[0].Count)
	assert.NotEmpty(t, fragments[0].Data)
}

func TestGatewayFragmentRole_FinalizesFragmentsToTSDBBlock(t *testing.T) {
	edgeCfg := &Config{
		Role:                    ProcessorRoleAgent,
		DeliveryMode:            DeliveryModeBestEffort,
		WindowInterval:          time.Hour,
		DropOriginal:            true,
		TSDBReorderGrace:        time.Nanosecond,
		FragmentSamplesPerChunk: 2,
		SourceID:                "edge-a",
	}
	edge := mkProcessor(t, edgeCfg, nil)

	base := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
	fragmentMetrics, err := edge.ConsumeMetrics(context.Background(), buildTestMetrics("cpu.usage", 4, base))
	require.NoError(t, err)
	fragments, err := extractFragments(fragmentMetrics)
	require.NoError(t, err)
	require.Len(t, fragments, 2)

	gatewayCfg := &Config{
		Role:           ProcessorRoleGatewayFragment,
		DeliveryMode:   DeliveryModeBestEffort,
		TSDBBucket:     "asap-tsdb",
		WindowInterval: time.Hour,
		DropOriginal:   true,
	}
	sink := &mockSink{}
	gateway := mkProcessor(t, gatewayCfg, sink)

	_, err = gateway.ConsumeMetrics(context.Background(), fragmentMetrics)
	require.NoError(t, err)
	gateway.flushWindow(context.Background())

	require.Equal(t, 1, sink.tsdbBlockCount())
	blockDir := writeMockTSDBBlock(t, sink.tsdbBlocks[0])
	opened, err := tsdb.OpenBlock(nil, blockDir, chunkenc.NewPool(), nil)
	require.NoError(t, err)
	defer opened.Close()

	got := readAllSamples(t, opened)
	require.Len(t, got, 1)
	assert.Equal(t, "cpu.usage", got[0].labels.Get(labels.MetricName))
	require.Len(t, got[0].samples, 4)
}

func TestFactory_DefaultConfig(t *testing.T) {
	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	assert.Equal(t, 60*time.Second, cfg.WindowInterval)
	assert.True(t, cfg.DropOriginal)
	assert.Equal(t, defaultPrefixTemplate, cfg.PrefixTemplate)
	assert.Equal(t, ProcessorRoleGatewayRaw, cfg.Role)
	assert.Equal(t, DeliveryModeDurableRaw, cfg.DeliveryMode)
}

func TestFactory_TypeRegistered(t *testing.T) {
	f := NewFactory()
	assert.Equal(t, Type, f.Type())
}
