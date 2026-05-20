package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

func TestWindowOf(t *testing.T) {
	const windowMs = int64(3600_000) // 1h
	cases := []struct {
		ts   int64
		want int64
	}{
		{0, 0},
		{1, 0},
		{windowMs - 1, 0},
		{windowMs, 1},
		{windowMs + 1, 1},
		{2*windowMs - 1, 1},
		{2 * windowMs, 2},
		{5*windowMs + 123, 5},
	}
	for _, c := range cases {
		if got := windowOf(c.ts, windowMs); got != c.want {
			t.Errorf("windowOf(%d, %d) = %d, want %d", c.ts, windowMs, got, c.want)
		}
	}
}

func TestNumRetainedWindows(t *testing.T) {
	h := time.Hour
	cases := []struct {
		retention time.Duration
		window    time.Duration
		want      int64
	}{
		{0, h, 2},                 // default → at least 2 (current + previous)
		{h, h, 2},                 // exactly one window → still keep 2 for boundary bridging
		{2 * h, h, 2},             // ceil(2/1)=2
		{3 * h, h, 3},             // ceil(3/1)=3
		{90 * time.Minute, h, 2},  // ceil(1.5)=2
		{150 * time.Minute, h, 3}, // ceil(2.5)=3
		{30 * time.Minute, h, 2},  // sub-window retention floored to min 2
	}
	for _, c := range cases {
		if got := numRetainedWindows(c.retention, c.window); got != c.want {
			t.Errorf("numRetainedWindows(%s, %s) = %d, want %d", c.retention, c.window, got, c.want)
		}
	}
}

// writeTestBlock writes a minimal valid TSDB block to <dir>/<ulid>/ containing a
// single series whose one sample sits at sampleTs. Returns the block dir path.
func writeTestBlock(t *testing.T, dir string, metricName string, sampleTs int64) string {
	t.Helper()
	xc := chunkenc.NewXORChunk()
	app, err := xc.Appender()
	if err != nil {
		t.Fatalf("xor appender: %v", err)
	}
	app.Append(sampleTs, 1.0)

	se := &seriesEntry{
		lset: labels.FromStrings("__name__", metricName),
		chks: []chunks.Meta{{
			MinTime: sampleTs,
			MaxTime: sampleTs,
			Chunk:   xc,
		}},
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	uid, err := writeMergedBlock(context.Background(), dir, []*seriesEntry{se}, sampleTs, sampleTs+1)
	if err != nil {
		t.Fatalf("writeMergedBlock: %v", err)
	}
	return filepath.Join(dir, uid.String())
}

// TestTumblingBucketingAcrossBoundary is the load-bearing test: blocks whose
// start times fall in different tumbling windows must produce DISTINCT merged
// output blocks (one per window), never a single rolling block.
func TestTumblingBucketingAcrossBoundary(t *testing.T) {
	const windowMs = int64(3600_000) // 1h

	stagingRoot := t.TempDir()
	outputDir := t.TempDir()

	// Block A: sample at 0.5h → window 0.
	tsA := windowMs / 2
	srcA := writeTestBlock(t, filepath.Join(stagingRoot, "blockA"), "metric_a", tsA)
	// Block B: sample at 1.5h → window 1 (one boundary past A).
	tsB := windowMs + windowMs/2
	srcB := writeTestBlock(t, filepath.Join(stagingRoot, "blockB"), "metric_a", tsB)

	wA := windowOf(tsA, windowMs)
	wB := windowOf(tsB, windowMs)
	if wA == wB {
		t.Fatalf("test setup error: blocks expected in distinct windows, both in %d", wA)
	}

	// Merge each window's block into its own output (mimics tick()'s per-window loop).
	if err := mergeWindow(context.Background(), wA, []string{srcA}, outputDir, "", windowMs, true); err != nil {
		t.Fatalf("mergeWindow A: %v", err)
	}
	if err := mergeWindow(context.Background(), wB, []string{srcB}, outputDir, "", windowMs, false); err != nil {
		t.Fatalf("mergeWindow B: %v", err)
	}

	// There must be exactly two merged output blocks, keyed to distinct windows.
	merged := readMergedWindows(outputDir, windowMs)
	if len(merged) != 2 {
		t.Fatalf("expected 2 merged windows, got %d: %v", len(merged), merged)
	}
	if _, ok := merged[wA]; !ok {
		t.Errorf("missing merged block for window %d (block A)", wA)
	}
	if _, ok := merged[wB]; !ok {
		t.Errorf("missing merged block for window %d (block B)", wB)
	}
	if merged[wA] == merged[wB] {
		t.Errorf("windows %d and %d collapsed into the same merged block %q", wA, wB, merged[wA])
	}

	// Each merged block's time range must be clamped to its own window.
	for w, name := range merged {
		bi, err := readBlockMeta(filepath.Join(outputDir, name))
		if err != nil {
			t.Fatalf("read merged meta %s: %v", name, err)
		}
		lo, hi := w*windowMs, (w+1)*windowMs
		if bi.MinTime < lo || bi.MaxTime > hi {
			t.Errorf("merged window %d block %s range [%d,%d] escapes window [%d,%d)",
				w, name, bi.MinTime, bi.MaxTime, lo, hi)
		}
	}
}

// TestTumblingSameWindowMergesToOne verifies that multiple blocks within the
// SAME window collapse into a single merged output block (the [b1..b60]→merged
// case), and that re-merging an in-progress window replaces the prior output.
func TestTumblingSameWindowMergesToOne(t *testing.T) {
	const windowMs = int64(3600_000)

	stagingRoot := t.TempDir()
	outputDir := t.TempDir()

	// Three blocks all within window 0 (at 5m, 10m, 15m).
	src1 := writeTestBlock(t, filepath.Join(stagingRoot, "b1"), "metric_a", 5*60_000)
	src2 := writeTestBlock(t, filepath.Join(stagingRoot, "b2"), "metric_a", 10*60_000)
	src3 := writeTestBlock(t, filepath.Join(stagingRoot, "b3"), "metric_a", 15*60_000)

	// First merge with 2 blocks (window still in progress).
	if err := mergeWindow(context.Background(), 0, []string{src1, src2}, outputDir, "", windowMs, false); err != nil {
		t.Fatalf("mergeWindow first: %v", err)
	}
	merged := readMergedWindows(outputDir, windowMs)
	if len(merged) != 1 {
		t.Fatalf("after first merge expected 1 merged window, got %d", len(merged))
	}
	firstULID := merged[0]

	// Re-merge the same window with all 3 blocks; the new output must replace
	// the prior one (still exactly one merged block for window 0).
	if err := mergeWindow(context.Background(), 0, []string{src1, src2, src3}, outputDir, firstULID, windowMs, false); err != nil {
		t.Fatalf("mergeWindow re-merge: %v", err)
	}
	merged = readMergedWindows(outputDir, windowMs)
	if len(merged) != 1 {
		t.Fatalf("after re-merge expected 1 merged window, got %d: %v", len(merged), merged)
	}
	if merged[0] == firstULID {
		t.Errorf("re-merge did not produce a new block (still %q)", firstULID)
	}
	// Old block must be gone.
	if _, err := os.Stat(filepath.Join(outputDir, firstULID)); !os.IsNotExist(err) {
		t.Errorf("prior merged block %q was not removed on re-merge", firstULID)
	}
}

// TestExpireMergedWindows verifies windows older than the retention horizon are
// dropped from the output dir while in-retention windows are kept.
func TestExpireMergedWindows(t *testing.T) {
	const windowMs = int64(3600_000)
	outputDir := t.TempDir()
	stagingRoot := t.TempDir()

	// Merged blocks for windows 0, 1, 2.
	for _, w := range []int64{0, 1, 2} {
		ts := w*windowMs + windowMs/2
		src := writeTestBlock(t, filepath.Join(stagingRoot, "src", string(rune('a'+w))), "metric_a", ts)
		if err := mergeWindow(context.Background(), w, []string{src}, outputDir, "", windowMs, true); err != nil {
			t.Fatalf("mergeWindow window %d: %v", w, err)
		}
	}
	if got := len(readMergedWindows(outputDir, windowMs)); got != 3 {
		t.Fatalf("expected 3 merged windows before expiry, got %d", got)
	}

	// Retain only windows >= 1 (oldestWindow = 1): window 0 must be expired.
	expireMergedWindows(outputDir, windowMs, 1)
	merged := readMergedWindows(outputDir, windowMs)
	if len(merged) != 2 {
		t.Fatalf("expected 2 merged windows after expiry, got %d: %v", len(merged), merged)
	}
	if _, ok := merged[0]; ok {
		t.Errorf("window 0 should have been expired but is still present")
	}
	for _, w := range []int64{1, 2} {
		if _, ok := merged[w]; !ok {
			t.Errorf("window %d should be retained but is missing", w)
		}
	}
}
