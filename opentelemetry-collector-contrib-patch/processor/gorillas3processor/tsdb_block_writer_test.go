// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// mvp/step2.1: Prometheus TSDB block-format writer tests.
//
// The round-trip test is the load-bearing assertion: it writes a
// block via tsdbBlockBuilder, materialises the in-memory artifact
// to a temp directory in the canonical Prometheus layout, and reads
// it back via Prometheus' own `tsdb.OpenBlock`. If the canonical
// reader can re-derive every (label, ts, value) tuple we wrote in,
// the byte format is correct by construction — Step 2.3's Thanos
// store-gateway will be able to do the same.

package gorillas3processor

import (
	"context"
	"os"
	"path/filepath"
	"sort"
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

// writeArtifactToDir writes the in-memory artifact files back out
// under a temp dir in the canonical `<dir>/<ulid>/...` layout, so
// `tsdb.OpenBlock` can read them. Returns `<dir>/<ulid>`.
func writeArtifactToDir(t *testing.T, art *tsdbBlockArtifact) string {
	t.Helper()
	root := t.TempDir()
	for k, body := range art.Files {
		full := filepath.Join(root, filepath.FromSlash(k))
		require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o755))
		require.NoError(t, os.WriteFile(full, body, 0o644))
	}
	return filepath.Join(root, art.ULID.String())
}

// TestTSDBBlockBuilder_RoundTrip writes a small window into a
// Prometheus block, reads it back via tsdb.OpenBlock, and asserts
// that every sample matches.
func TestTSDBBlockBuilder_RoundTrip(t *testing.T) {
	base := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC).UnixNano()
	const n = 30
	pts := make([]point, n)
	for i := 0; i < n; i++ {
		pts[i] = point{ts: base + int64(i)*int64(time.Second), v: float64(i) * 1.5}
	}
	window := map[seriesKey]*seriesBuffer{
		{metricName: "cpu_usage", attributesKey: "host=h;"}: {
			attributes: map[string]string{"host": "h"},
			points:     pts,
		},
	}

	b := newTSDBBlockBuilder(60*time.Second, nil, nil)
	art, err := b.build(context.Background(), window)
	require.NoError(t, err)
	require.NotNil(t, art)
	require.NotEmpty(t, art.Files, "block writer must emit at least one file")
	// Block layout sanity.
	hasMeta, hasIndex, hasChunks := false, false, false
	for k := range art.Files {
		switch {
		case filepath.Base(k) == "meta.json":
			hasMeta = true
		case filepath.Base(k) == "index":
			hasIndex = true
		case filepath.Dir(k) == art.ULID.String()+"/chunks":
			hasChunks = true
		}
	}
	assert.True(t, hasMeta, "expected meta.json in artifact")
	assert.True(t, hasIndex, "expected index in artifact")
	assert.True(t, hasChunks, "expected chunks/000001+ in artifact")

	// Round-trip via the canonical reader.
	blockDir := writeArtifactToDir(t, art)
	block, err := tsdb.OpenBlock(nil, blockDir, chunkenc.NewPool(), nil)
	require.NoError(t, err)
	defer block.Close()

	got := readAllSamples(t, block)
	require.Len(t, got, 1, "single series in single block")

	// __name__ + host
	gotLs := got[0].labels
	assert.Equal(t, "cpu_usage", gotLs.Get(labels.MetricName))
	assert.Equal(t, "h", gotLs.Get("host"))

	require.Len(t, got[0].samples, n)
	for i, s := range got[0].samples {
		assert.Equal(t, (base+int64(i)*int64(time.Second))/int64(time.Millisecond), s.t, "ts %d", i)
		assert.InDelta(t, float64(i)*1.5, s.v, 0, "v %d", i)
	}
}

// TestTSDBBlockBuilder_MultiSeries verifies that two distinct
// label-sets produce two distinct series in the block.
func TestTSDBBlockBuilder_MultiSeries(t *testing.T) {
	base := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC).UnixNano()
	mk := func(host string, vBase float64) *seriesBuffer {
		buf := &seriesBuffer{
			attributes: map[string]string{"host": host},
			points:     make([]point, 10),
		}
		for i := 0; i < 10; i++ {
			buf.points[i] = point{
				ts: base + int64(i)*int64(time.Second),
				v:  vBase + float64(i),
			}
		}
		return buf
	}
	window := map[seriesKey]*seriesBuffer{
		{metricName: "rps", attributesKey: "host=a;"}: mk("a", 100),
		{metricName: "rps", attributesKey: "host=b;"}: mk("b", 200),
	}

	b := newTSDBBlockBuilder(60*time.Second, nil, nil)
	art, err := b.build(context.Background(), window)
	require.NoError(t, err)
	require.NotNil(t, art)

	blockDir := writeArtifactToDir(t, art)
	block, err := tsdb.OpenBlock(nil, blockDir, chunkenc.NewPool(), nil)
	require.NoError(t, err)
	defer block.Close()

	got := readAllSamples(t, block)
	require.Len(t, got, 2)
	// Sort by host attribute for stable assertions.
	sort.Slice(got, func(i, j int) bool { return got[i].labels.Get("host") < got[j].labels.Get("host") })
	assert.Equal(t, "a", got[0].labels.Get("host"))
	assert.Equal(t, "b", got[1].labels.Get("host"))
	// Spot-check first sample.
	assert.InDelta(t, 100.0, got[0].samples[0].v, 0)
	assert.InDelta(t, 200.0, got[1].samples[0].v, 0)
}

// TestTSDBBlockBuilder_ExternalLabels verifies that labels set in
// Config.TSDBExternalLabels appear on every series in the block.
// Step 2.3's backend joins on these so this is contract-load-bearing.
func TestTSDBBlockBuilder_ExternalLabels(t *testing.T) {
	base := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC).UnixNano()
	window := map[seriesKey]*seriesBuffer{
		{metricName: "m", attributesKey: ""}: {
			attributes: map[string]string{},
			points:     []point{{ts: base, v: 1}, {ts: base + int64(time.Second), v: 2}},
		},
	}
	b := newTSDBBlockBuilder(60*time.Second, map[string]string{
		"cluster": "prod",
		"replica": "a",
	}, nil)
	art, err := b.build(context.Background(), window)
	require.NoError(t, err)
	require.NotNil(t, art)

	blockDir := writeArtifactToDir(t, art)
	block, err := tsdb.OpenBlock(nil, blockDir, chunkenc.NewPool(), nil)
	require.NoError(t, err)
	defer block.Close()

	got := readAllSamples(t, block)
	require.Len(t, got, 1)
	assert.Equal(t, "prod", got[0].labels.Get("cluster"))
	assert.Equal(t, "a", got[0].labels.Get("replica"))
}

// TestTSDBBlockBuilder_EmptyWindowReturnsNil confirms the empty-window
// short-circuit. tsdb.BlockWriter would otherwise produce an empty
// ULID and we'd try to PUT three files of nothing.
func TestTSDBBlockBuilder_EmptyWindowReturnsNil(t *testing.T) {
	b := newTSDBBlockBuilder(60*time.Second, nil, nil)
	art, err := b.build(context.Background(), nil)
	require.NoError(t, err)
	assert.Nil(t, art)
}

// TestFlushWindow_BlockFormatTSDB verifies the processor flush wires
// straight through to PutTSDBBlock when block_format=prometheus_tsdb.
// No GORILLA1 chunk should land on the legacy path.
func TestFlushWindow_BlockFormatTSDB(t *testing.T) {
	cfg := &Config{
		Bucket:         "asap-gorilla",
		TSDBBucket:     "asap-tsdb",
		WindowInterval: time.Hour,
		DropOriginal:   true,
		BlockFormat:    BlockFormatPrometheusTSDB,
		Tenant:         "tnt",
	}
	sink := &mockSink{}
	p := mkProcessor(t, cfg, sink)

	base := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
	md := buildTestMetrics("cpu.usage", 8, base)
	_, err := p.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)

	p.flushWindow(context.Background())

	assert.Equal(t, 0, sink.chunkCount(), "asap chunks must NOT be emitted in prometheus_tsdb mode")
	assert.Equal(t, 0, sink.postingsCount(), "asap postings must NOT be emitted in prometheus_tsdb mode")
	require.Equal(t, 1, sink.tsdbBlockCount(), "expected one tsdb block")
	block := sink.tsdbBlocks[0]
	assert.NotEmpty(t, block.ulid)
	// Files must include meta.json + index + chunks/000001.
	require.NotEmpty(t, block.files)
	hasMeta, hasIndex, hasChunks := false, false, false
	for k := range block.files {
		base := filepath.Base(k)
		switch base {
		case "meta.json":
			hasMeta = true
		case "index":
			hasIndex = true
		case "000001":
			hasChunks = true
		}
	}
	assert.True(t, hasMeta, "block must include meta.json")
	assert.True(t, hasIndex, "block must include index")
	assert.True(t, hasChunks, "block must include chunks/000001")
}

// TestFlushWindow_BlockFormatBoth verifies that with block_format=both,
// the processor emits BOTH the legacy ASAP triplet AND a Prometheus
// TSDB block from the same window snapshot. Useful for migration.
func TestFlushWindow_BlockFormatBoth(t *testing.T) {
	cfg := &Config{
		Bucket:         "asap-gorilla",
		TSDBBucket:     "asap-tsdb",
		WindowInterval: time.Hour,
		DropOriginal:   true,
		BlockFormat:    BlockFormatBoth,
		Tenant:         "tnt",
	}
	sink := &mockSink{}
	p := mkProcessor(t, cfg, sink)

	base := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
	md := buildTestMetrics("cpu.usage", 6, base)
	_, err := p.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)

	p.flushWindow(context.Background())

	assert.Equal(t, 1, sink.chunkCount(), "block_format=both must still emit asap chunk")
	assert.Equal(t, 1, sink.postingsCount(), "block_format=both must still emit asap postings")
	assert.Equal(t, 1, sink.tsdbBlockCount(), "block_format=both must also emit tsdb block")

	// Prometheus block must round-trip via OpenBlock.
	blk := sink.tsdbBlocks[0]
	root := t.TempDir()
	for k, body := range blk.files {
		full := filepath.Join(root, filepath.FromSlash(k))
		require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o755))
		require.NoError(t, os.WriteFile(full, body, 0o644))
	}
	block, err := tsdb.OpenBlock(nil, filepath.Join(root, blk.ulid), chunkenc.NewPool(), nil)
	require.NoError(t, err)
	defer block.Close()

	got := readAllSamples(t, block)
	require.Len(t, got, 1)
	assert.Equal(t, "cpu.usage", got[0].labels.Get(labels.MetricName))
	assert.Equal(t, 6, len(got[0].samples))
}

// TestFlushWindow_BlockFormatASAPDefault asserts that the unconfigured
// default (== "asap") preserves the pre-step2.1 byte-identical
// behaviour: chunk + postings, no tsdb block.
func TestFlushWindow_BlockFormatASAPDefault(t *testing.T) {
	cfg := &Config{
		Bucket:         "asap-gorilla",
		WindowInterval: time.Hour,
		DropOriginal:   true,
		// BlockFormat unset → defaults to asap via Validate()
	}
	sink := &mockSink{}
	p := mkProcessor(t, cfg, sink)
	require.Equal(t, BlockFormatASAP, cfg.BlockFormat, "default block_format must be asap")

	base := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
	md := buildTestMetrics("cpu.usage", 4, base)
	_, err := p.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)

	p.flushWindow(context.Background())

	assert.Equal(t, 1, sink.chunkCount())
	assert.Equal(t, 1, sink.postingsCount())
	assert.Equal(t, 0, sink.tsdbBlockCount(), "default mode must NOT emit tsdb blocks")
}

// TestConfig_ValidateBlockFormat exercises the BlockFormat validation
// branches.
func TestConfig_ValidateBlockFormat(t *testing.T) {
	// Default fills in asap.
	cfg := &Config{Bucket: "b"}
	require.NoError(t, cfg.Validate())
	assert.Equal(t, BlockFormatASAP, cfg.BlockFormat)
	assert.True(t, cfg.EmitASAP())
	assert.False(t, cfg.EmitTSDB())

	// prometheus_tsdb is accepted; TSDBBucket falls back to Bucket.
	cfg = &Config{Bucket: "b", BlockFormat: BlockFormatPrometheusTSDB}
	require.NoError(t, cfg.Validate())
	assert.Equal(t, "b", cfg.TSDBBucket)
	assert.False(t, cfg.EmitASAP())
	assert.True(t, cfg.EmitTSDB())

	// both is accepted.
	cfg = &Config{Bucket: "b", TSDBBucket: "t", BlockFormat: BlockFormatBoth}
	require.NoError(t, cfg.Validate())
	assert.True(t, cfg.EmitASAP())
	assert.True(t, cfg.EmitTSDB())

	// Invalid value rejected.
	cfg = &Config{Bucket: "b", BlockFormat: BlockFormat("nonsense")}
	require.Error(t, cfg.Validate())
}

// TestTSDBBlockBuilder_RotatingCardinalityNoOOB pins the issue#46
// regression. Models the fake-exporter PR#338 `unique_users_per_min`
// rotating user_id pool: each user is "active" only during a
// non-overlapping slice of the 60s window. Before the fix, the
// Head's appender locked `minValidTime` at
// `firstAppendedSampleTs - chunkRange/2`. With random map iteration
// order over `window`, if the first-visited series held samples in
// e.g. the [40..55s] slice, the floor became 10s and any
// subsequent series with samples at t<10s tripped
// `storage.ErrOutOfBounds`.
//
// The fix flattens all samples across series and sorts globally by
// timestamp ascending, guaranteeing the first-appended sample has
// the smallest ts in the entire window. After the fix, the floor
// is `min(window) - 30s` which sits below every other sample in
// the window. We loop 50 times to make accidental success
// statistically improbable across Go's randomised map iteration.
func TestTSDBBlockBuilder_RotatingCardinalityNoOOB(t *testing.T) {
	const blockMs = int64(60_000)
	base := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC).UnixNano()

	// 8 users, each active in a non-overlapping 7s slice of the
	// 60s window: u0=[0..6s], u1=[7..13s], ..., u7=[49..55s].
	mkSeries := func(uIdx int, uid string, vBase float64) (seriesKey, *seriesBuffer) {
		const sliceWidth = 7
		startSec := uIdx * sliceWidth
		buf := &seriesBuffer{
			attributes: map[string]string{"user_id": uid, "host": "h"},
			points:     make([]point, sliceWidth),
		}
		for i := 0; i < sliceWidth; i++ {
			ts := base + int64(startSec+i)*int64(time.Second)
			buf.points[i] = point{ts: ts, v: vBase + float64(i)}
		}
		return seriesKey{metricName: "unique_users_per_min", attributesKey: "host=h;user_id=" + uid + ";"}, buf
	}

	for trial := 0; trial < 50; trial++ {
		window := make(map[seriesKey]*seriesBuffer)
		for u := 0; u < 8; u++ {
			sk, buf := mkSeries(u, "u"+string(rune('a'+u)), float64(u)*100)
			window[sk] = buf
		}
		b := newTSDBBlockBuilder(time.Duration(blockMs)*time.Millisecond, nil, nil)
		art, err := b.build(context.Background(), window)
		require.NoError(t, err, "trial %d: build must not error on rotating-cardinality input", trial)
		require.NotNil(t, art, "trial %d: artifact must not be nil", trial)
		assert.Equal(t, uint64(0), art.NumOOBDropped, "trial %d: no in-window samples should be dropped", trial)
		assert.Equal(t, uint64(8), art.NumSeries, "trial %d: expected 8 series in block", trial)
		assert.Equal(t, uint64(8*7), art.NumSamples, "trial %d: expected 56 samples in block", trial)
	}
}

// TestTSDBBlockBuilder_WideTimestampSpanNoOOB verifies that even a
// 5-minute span between the earliest and latest samples in a
// single window builds cleanly — global sort anchors the
// appender's minValidTime at the smallest ts (- chunkRange/2), so
// every later sample fits regardless of cross-series interleaving.
// This is the operator-visible contract: the agent's window
// buffer can hold whatever the upstream OTLP push schedule
// produces and we'll still emit a valid TSDB block.
func TestTSDBBlockBuilder_WideTimestampSpanNoOOB(t *testing.T) {
	const blockMs = int64(60_000)
	base := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC).UnixNano()
	wayBack := base - 5*60*int64(time.Second) // 5min before base

	window := map[seriesKey]*seriesBuffer{
		{metricName: "m", attributesKey: "host=now;"}: {
			attributes: map[string]string{"host": "now"},
			points: []point{
				{ts: base, v: 1},
				{ts: base + int64(time.Second), v: 2},
				{ts: base + 2*int64(time.Second), v: 3},
			},
		},
		{metricName: "m", attributesKey: "host=rogue;"}: {
			attributes: map[string]string{"host": "rogue"},
			points: []point{
				{ts: wayBack, v: 100},
			},
		},
	}

	b := newTSDBBlockBuilder(time.Duration(blockMs)*time.Millisecond, nil, nil)
	art, err := b.build(context.Background(), window)
	require.NoError(t, err, "build must tolerate wide-gap samples without error")
	require.NotNil(t, art, "artifact must not be nil")
	assert.Equal(t, uint64(2), art.NumSeries, "both series end up in the block")
	// Every sample fits because the rogue (oldest) is appended
	// first and anchors minValidTime at wayBack-30s.
	assert.Equal(t, uint64(0), art.NumOOBDropped, "no drops with global-sort")
	assert.Equal(t, uint64(4), art.NumSamples)
}

// TestFlushTSDB_OOBDoesNotCrashProcessor wires the rotating-cardinality
// scenario through the full processor flush path and asserts:
//  1. flushWindow returns without panic / fatal log,
//  2. the tsdb block IS uploaded (no series silently lost),
//  3. all 8 series and all 96 samples land in the block.
func TestFlushTSDB_OOBDoesNotCrashProcessor(t *testing.T) {
	cfg := &Config{
		Bucket:            "asap-gorilla",
		TSDBBucket:        "asap-tsdb",
		WindowInterval:    time.Hour,
		TSDBBlockDuration: 60 * time.Second,
		DropOriginal:      true,
		BlockFormat:       BlockFormatPrometheusTSDB,
		Tenant:            "tnt",
	}
	sink := &mockSink{}
	p := mkProcessor(t, cfg, sink)

	// Build 8 series of `unique_users_per_min`-style data, each
	// active in a non-overlapping 7s slice of the 60s window.
	// Pre-fix this triggered OOB because the appender's
	// minValidTime locked at firstAppendedTs - 30s, which (for
	// any slice starting > 30s) would reject samples in earlier
	// slices.
	base := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	const sliceWidth = 7
	for u := 0; u < 8; u++ {
		md := pmetric.NewMetrics()
		rm := md.ResourceMetrics().AppendEmpty()
		sm := rm.ScopeMetrics().AppendEmpty()
		m := sm.Metrics().AppendEmpty()
		m.SetName("unique_users_per_min")
		g := m.SetEmptyGauge()
		for i := 0; i < sliceWidth; i++ {
			dp := g.DataPoints().AppendEmpty()
			off := time.Duration(u*sliceWidth+i) * time.Second
			dp.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(off)))
			dp.SetDoubleValue(float64(u*100 + i))
			dp.Attributes().PutStr("user_id", "u"+string(rune('a'+u)))
			dp.Attributes().PutStr("host", "h")
		}
		_, err := p.ConsumeMetrics(context.Background(), md)
		require.NoError(t, err)
	}

	p.flushWindow(context.Background())
	require.Equal(t, 1, sink.tsdbBlockCount(), "expected one tsdb block")
	blk := sink.tsdbBlocks[0]
	require.NotEmpty(t, blk.files)

	// Round-trip and count.
	root := t.TempDir()
	for k, body := range blk.files {
		full := filepath.Join(root, filepath.FromSlash(k))
		require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o755))
		require.NoError(t, os.WriteFile(full, body, 0o644))
	}
	block, err := tsdb.OpenBlock(nil, filepath.Join(root, blk.ulid), chunkenc.NewPool(), nil)
	require.NoError(t, err)
	defer block.Close()
	got := readAllSamples(t, block)
	require.Len(t, got, 8, "expected 8 distinct series in the block")
	totalSamples := 0
	for _, s := range got {
		totalSamples += len(s.samples)
	}
	assert.Equal(t, 8*sliceWidth, totalSamples, "expected all 56 samples to land in the block")
}

// readAllSamples opens every series in a block and returns the
// label-set + (ts, value) decoded samples.
type rtSeries struct {
	labels  labels.Labels
	samples []rtSample
}
type rtSample struct {
	t int64
	v float64
}

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

// silenceUnused is here purely so unused imports stay honest in
// case the editor strips them. pcommon + zaptest are used by the
// shared mkProcessor / buildTestMetrics helpers in processor_test.go;
// the linker still complains if we never reference them in this
// file's transitive scope.
var _ = pcommon.NewMap
var _ = zaptest.NewLogger
