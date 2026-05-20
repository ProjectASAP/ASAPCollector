package main

import (
	"archive/tar"
	"bytes"
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
// single series (one XOR chunk) at sampleTs. Models one agent per-emit block.
// Returns the block dir path.
func writeTestBlock(t *testing.T, dir, metricName string, sampleTs int64) string {
	t.Helper()
	xc := chunkenc.NewXORChunk()
	app, err := xc.Appender()
	if err != nil {
		t.Fatalf("xor appender: %v", err)
	}
	app.Append(sampleTs, 1.0)

	se := &seriesEntry{
		lset: labels.FromStrings("__name__", metricName),
		chks: []chunks.Meta{{MinTime: sampleTs, MaxTime: sampleTs, Chunk: xc}},
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

// TestBuildWindowBlockConcatenatesChunks is the load-bearing test: the per-emit
// blocks of one window must concatenate per-series into ONE block whose chunk
// count is the sum of the source chunks (append, not re-merge), clamped to the
// window's interval.
func TestBuildWindowBlockConcatenatesChunks(t *testing.T) {
	const windowMs = int64(3600_000) // 1h
	srcRoot := t.TempDir()
	dest := t.TempDir()

	// Three per-emit blocks within window 0, same series, distinct timestamps.
	src1 := writeTestBlock(t, filepath.Join(srcRoot, "b1"), "metric_a", 5*60_000)
	src2 := writeTestBlock(t, filepath.Join(srcRoot, "b2"), "metric_a", 10*60_000)
	src3 := writeTestBlock(t, filepath.Join(srcRoot, "b3"), "metric_a", 15*60_000)

	uid, nSeries, err := buildWindowBlock(context.Background(), []string{src1, src2, src3}, 0, windowMs, dest)
	if err != nil {
		t.Fatalf("buildWindowBlock: %v", err)
	}
	if nSeries != 1 {
		t.Fatalf("expected 1 series, got %d", nSeries)
	}

	bi, err := readBlockMeta(filepath.Join(dest, uid.String()))
	if err != nil {
		t.Fatalf("read cut meta: %v", err)
	}
	if bi.Compaction.Level != 2 {
		t.Errorf("cut block should be Level 2, got %d", bi.Compaction.Level)
	}
	// Re-read the cut block: its single series must carry all 3 concatenated chunks.
	got := map[string]*seriesEntry{}
	if err := readBlockSeries(context.Background(), filepath.Join(dest, uid.String()), 0, windowMs-1, got); err != nil {
		t.Fatalf("re-read cut block: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 series in cut block, got %d", len(got))
	}
	for _, se := range got {
		if len(se.chks) != 3 {
			t.Errorf("expected 3 concatenated chunks, got %d", len(se.chks))
		}
	}
	if bi.MinTime < 0 || bi.MaxTime > windowMs {
		t.Errorf("cut block range [%d,%d] escapes window [0,%d)", bi.MinTime, bi.MaxTime, windowMs)
	}
}

// TestBuildWindowBlockEmpty: source blocks entirely outside the window produce no
// block (nSeries 0) so the caller can prune them without writing output.
func TestBuildWindowBlockEmpty(t *testing.T) {
	const windowMs = int64(3600_000)
	srcRoot := t.TempDir()
	dest := t.TempDir()

	// Block sample sits in window 1, but we ask buildWindowBlock for window 0.
	src := writeTestBlock(t, filepath.Join(srcRoot, "b"), "metric_a", windowMs+60_000)
	_, nSeries, err := buildWindowBlock(context.Background(), []string{src}, 0, windowMs, dest)
	if err != nil {
		t.Fatalf("buildWindowBlock: %v", err)
	}
	if nSeries != 0 {
		t.Errorf("expected 0 series for out-of-window source, got %d", nSeries)
	}
}

// TestExtractTarRoundtrip: a block tarred (as the agent ship-to-merger sink does)
// and extracted by the merger ingest path must round-trip byte-for-byte.
func TestExtractTarRoundtrip(t *testing.T) {
	files := map[string][]byte{
		"meta.json":     []byte(`{"ulid":"01TESTULID","minTime":0,"maxTime":1,"compaction":{"level":1}}`),
		"index":         []byte("fake-index-bytes"),
		"chunks/000001": []byte("fake-chunk-bytes"),
	}
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for name, data := range files {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(data)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatalf("tar header %s: %v", name, err)
		}
		if _, err := tw.Write(data); err != nil {
			t.Fatalf("tar write %s: %v", name, err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar close: %v", err)
	}

	dest := t.TempDir()
	if err := extractTar(&buf, dest); err != nil {
		t.Fatalf("extractTar: %v", err)
	}
	for name, want := range files {
		got, err := os.ReadFile(filepath.Join(dest, filepath.FromSlash(name)))
		if err != nil {
			t.Fatalf("read extracted %s: %v", name, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("file %s mismatch: got %q want %q", name, got, want)
		}
	}
}

// TestFindBlockDir: locates meta.json whether block files are bare at root or
// under a "<ulid>/" prefix dir (gorillas3's readTSDBBlockFiles layout).
func TestFindBlockDir(t *testing.T) {
	bare := t.TempDir()
	os.WriteFile(filepath.Join(bare, "meta.json"), []byte("{}"), 0o644)
	if got, err := findBlockDir(bare); err != nil || got != bare {
		t.Errorf("bare: got (%q,%v), want (%q,nil)", got, err, bare)
	}
	root := t.TempDir()
	sub := filepath.Join(root, "01KS3MJR908CJPDDPRZ95F6NGP")
	os.MkdirAll(filepath.Join(sub, "chunks"), 0o755)
	os.WriteFile(filepath.Join(sub, "meta.json"), []byte("{}"), 0o644)
	if got, err := findBlockDir(root); err != nil || got != sub {
		t.Errorf("prefixed: got (%q,%v), want (%q,nil)", got, err, sub)
	}
	if _, err := findBlockDir(t.TempDir()); err == nil {
		t.Errorf("empty: expected error, got nil")
	}
}

// TestExtractTarRejectsTraversal: a malicious entry escaping destDir is rejected.
func TestExtractTarRejectsTraversal(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	data := []byte("pwned")
	if err := tw.WriteHeader(&tar.Header{Name: "../escape", Mode: 0o644, Size: int64(len(data)), Typeflag: tar.TypeReg}); err != nil {
		t.Fatalf("tar header: %v", err)
	}
	tw.Write(data)
	tw.Close()

	if err := extractTar(&buf, t.TempDir()); err == nil {
		t.Errorf("expected extractTar to reject path traversal, got nil error")
	}
}
