// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package gorillaprocessor

import (
	"context"
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap/zaptest"
)

// buildTestMetrics creates a pmetric.Metrics with n gauge data points.
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

func TestConsumeMetrics_PassThrough(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &Config{
		WindowInterval: time.Hour, // large window so no auto-flush
		LocalDir:       tmpDir,
		DropOriginal:   false,
	}
	_ = cfg.Validate()
	logger := zaptest.NewLogger(t)
	proc := newProcessor(cfg, nil, logger)

	md := buildTestMetrics("cpu.usage", 5, time.Now())
	result, err := proc.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)
	assert.Equal(t, 5, result.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().Len())
}

func TestConsumeMetrics_DropOriginal(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &Config{
		WindowInterval: time.Hour,
		LocalDir:       tmpDir,
		DropOriginal:   true,
	}
	_ = cfg.Validate()
	logger := zaptest.NewLogger(t)
	proc := newProcessor(cfg, nil, logger)

	md := buildTestMetrics("cpu.usage", 5, time.Now())
	result, err := proc.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)
	assert.Equal(t, 0, result.ResourceMetrics().Len())
}

func TestFlushWindow_LocalFile(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &Config{
		WindowInterval: time.Hour,
		LocalDir:       tmpDir,
	}
	_ = cfg.Validate()
	logger := zaptest.NewLogger(t)
	proc := newProcessor(cfg, nil, logger)

	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	md := buildTestMetrics("cpu.usage", 10, baseTime)
	_, err := proc.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)

	// Verify points were buffered
	assert.Equal(t, 1, len(proc.series))
	for _, buf := range proc.series {
		assert.Equal(t, 10, len(buf.points))
	}

	// Trigger flush
	proc.flushWindow()

	// Series should be empty after flush
	assert.Equal(t, 0, len(proc.series))

	// Find the local file
	files, err := filepath.Glob(filepath.Join(tmpDir, "*.gorilla"))
	require.NoError(t, err)
	require.Equal(t, 1, len(files))

	// Parse GORILLA1 header
	data, err := os.ReadFile(files[0])
	require.NoError(t, err)
	require.True(t, len(data) > 13, "file too small")
	assert.Equal(t, "GORILLA1", string(data[:8]))
	assert.Equal(t, byte(1), data[8]) // version
	seriesCount := binary.LittleEndian.Uint32(data[9:13])
	assert.Equal(t, uint32(1), seriesCount)
}

func TestFlushWindow_BinaryRoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &Config{
		WindowInterval: time.Hour,
		LocalDir:       tmpDir,
	}
	_ = cfg.Validate()
	logger := zaptest.NewLogger(t)
	proc := newProcessor(cfg, nil, logger)

	n := 100
	baseTime := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	md := buildTestMetrics("mem.used", n, baseTime)
	_, err := proc.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)

	proc.flushWindow()

	files, err := filepath.Glob(filepath.Join(tmpDir, "*.gorilla"))
	require.NoError(t, err)
	require.Equal(t, 1, len(files))

	data, err := os.ReadFile(files[0])
	require.NoError(t, err)

	// Skip header (13 bytes)
	offset := 13

	// Read metadata length
	metaLen := binary.LittleEndian.Uint16(data[offset : offset+2])
	offset += 2
	require.True(t, int(metaLen) > 0)
	offset += int(metaLen) // skip JSON metadata

	// Read point count
	pointCount := binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4
	assert.Equal(t, uint32(n), pointCount)

	// Read first timestamp and value
	firstTS := int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
	offset += 8
	firstValBits := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	// Read timestamp bits
	tsBitsLen := binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4
	tsBytesLen := (tsBitsLen + 7) / 8
	tsBits := data[offset : offset+int(tsBytesLen)]
	offset += int(tsBytesLen)

	// Read value bits
	valBitsLen := binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4
	valBytesLen := (valBitsLen + 7) / 8
	valBits := data[offset : offset+int(valBytesLen)]

	// Decode and verify round-trip
	dts, ok := decodeTimestamps(firstTS, n, tsBits, tsBitsLen)
	require.True(t, ok)
	dvals, ok := decodeValues(firstValBits, n, valBits, valBitsLen)
	require.True(t, ok)

	for i := 0; i < n; i++ {
		expectedTS := baseTime.Add(time.Duration(i) * time.Second).UnixNano()
		assert.Equal(t, expectedTS, dts[i], "timestamp mismatch at %d", i)
		expectedVal := float64(i) * 1.5
		assert.Equal(t, math.Float64bits(expectedVal), math.Float64bits(dvals[i]),
			"value mismatch at %d", i)
	}
}

func TestFlushWindow_MultipleSeries(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &Config{
		WindowInterval: time.Hour,
		LocalDir:       tmpDir,
	}
	_ = cfg.Validate()
	logger := zaptest.NewLogger(t)
	proc := newProcessor(cfg, nil, logger)

	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	md1 := buildTestMetrics("cpu.usage", 5, baseTime)
	md2 := buildTestMetrics("mem.used", 3, baseTime)
	_, err := proc.ConsumeMetrics(context.Background(), md1)
	require.NoError(t, err)
	_, err = proc.ConsumeMetrics(context.Background(), md2)
	require.NoError(t, err)

	assert.Equal(t, 2, len(proc.series))

	proc.flushWindow()

	files, err := filepath.Glob(filepath.Join(tmpDir, "*.gorilla"))
	require.NoError(t, err)
	require.Equal(t, 1, len(files))

	data, err := os.ReadFile(files[0])
	require.NoError(t, err)
	seriesCount := binary.LittleEndian.Uint32(data[9:13])
	assert.Equal(t, uint32(2), seriesCount)
}

func TestFlushWindow_EmptyNoFile(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &Config{
		WindowInterval: time.Hour,
		LocalDir:       tmpDir,
	}
	_ = cfg.Validate()
	logger := zaptest.NewLogger(t)
	proc := newProcessor(cfg, nil, logger)

	// Flush with no data should not create any files
	proc.flushWindow()

	files, err := filepath.Glob(filepath.Join(tmpDir, "*.gorilla"))
	require.NoError(t, err)
	assert.Equal(t, 0, len(files))
}

func TestConsumeMetrics_SumType(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &Config{
		WindowInterval: time.Hour,
		LocalDir:       tmpDir,
	}
	_ = cfg.Validate()
	logger := zaptest.NewLogger(t)
	proc := newProcessor(cfg, nil, logger)

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("http.requests")
	s := m.SetEmptySum()
	for i := 0; i < 5; i++ {
		dp := s.DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now().Add(time.Duration(i) * time.Second)))
		dp.SetIntValue(int64(i * 100))
	}

	_, err := proc.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)
	assert.Equal(t, 1, len(proc.series))
	for _, buf := range proc.series {
		assert.Equal(t, 5, len(buf.points))
	}
}

func TestShutdown_FlushesRemaining(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &Config{
		WindowInterval: time.Hour,
		LocalDir:       tmpDir,
	}
	_ = cfg.Validate()
	logger := zaptest.NewLogger(t)
	proc := newProcessor(cfg, nil, logger)
	proc.done = make(chan struct{})

	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	md := buildTestMetrics("cpu.usage", 5, baseTime)
	_, err := proc.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)

	// Shutdown should flush
	err = proc.Shutdown(context.Background())
	require.NoError(t, err)

	files, err := filepath.Glob(filepath.Join(tmpDir, "*.gorilla"))
	require.NoError(t, err)
	assert.Equal(t, 1, len(files))
}

func TestBuildObjectKey_WithPrefix(t *testing.T) {
	blockEnd := time.Date(2025, 3, 15, 10, 30, 0, 0, time.UTC)
	key := buildObjectKey("metrics/%Y/%m/%d/", "", blockEnd, 0)
	assert.Contains(t, key, "metrics/2025/03/15/")
	assert.Contains(t, key, ".gorilla")
}

func TestBuildObjectKey_FixedName(t *testing.T) {
	key := buildObjectKey("metrics/", "fixed.gorilla", time.Now(), 0)
	assert.Equal(t, "fixed.gorilla", key)
}

func TestFlushWindow_S3FilesWithPrefix(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &Config{
		WindowInterval: time.Hour,
		S3Files: S3FilesConfig{
			MountPath: tmpDir,
			Prefix:    "data/%Y/%m/%d/",
		},
	}
	_ = cfg.Validate()
	logger := zaptest.NewLogger(t)
	proc := newProcessor(cfg, nil, logger)

	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	md := buildTestMetrics("cpu.usage", 10, baseTime)
	_, err := proc.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)

	proc.flushWindow()

	// Files should appear in the date-partitioned subdirectory
	pattern := filepath.Join(tmpDir, "data", "2025", "01", "01", "*.gorilla")
	files, err := filepath.Glob(pattern)
	require.NoError(t, err)
	require.Equal(t, 1, len(files))

	// Verify GORILLA1 header
	data, err := os.ReadFile(files[0])
	require.NoError(t, err)
	require.True(t, len(data) > 13, "file too small")
	assert.Equal(t, "GORILLA1", string(data[:8]))
	seriesCount := binary.LittleEndian.Uint32(data[9:13])
	assert.Equal(t, uint32(1), seriesCount)
}

func TestFormatPrefix(t *testing.T) {
	ts := time.Date(2025, 6, 15, 14, 30, 45, 0, time.UTC)
	result := formatPrefix("data/%Y/%m/%d/%H/", ts)
	assert.Equal(t, "data/2025/06/15/14/", result)
}
