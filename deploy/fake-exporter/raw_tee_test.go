package main

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

func TestRawTee_DisabledIsNoop(t *testing.T) {
	rt := newRawTee("")
	if rt.enabled {
		t.Fatalf("empty root should disable tee")
	}
	rt.Tee("metric", 0, 1.0, []attribute.KeyValue{attribute.String("k", "v")})
	rt.Close()
}

// expectedPath returns the on-disk part path for (root, metric, ts)
// using the same algorithm as the production code, so tests don't
// drift if the format ever changes.
func expectedPath(root, metric string, tsMs int64, partName string) string {
	prefix := partPathPrefix(metric, tsMs) // "raw/<m>/YYYY/MM/DD/HH/"
	return filepath.Join(root, filepath.FromSlash(prefix), partName)
}

func TestRawTee_FormatMatchesColdStoreLayout(t *testing.T) {
	tmp := t.TempDir()
	rt := newRawTee(tmp)
	defer rt.Close()

	tsMs := time.Date(2026, 4, 30, 12, 34, 56, 0, time.UTC).UnixMilli()
	rt.Tee("http_requests_total", tsMs, 42.5,
		[]attribute.KeyValue{
			attribute.String("zone", "z0"),
			attribute.String("rack", "r1"),
		},
	)
	rt.flushAll()

	want := expectedPath(tmp, "http_requests_total", tsMs, "part-000001.jsonl")
	if _, err := os.Stat(want); err != nil {
		t.Fatalf("expected part file at %s: %v", want, err)
	}

	b, err := os.ReadFile(want)
	if err != nil {
		t.Fatal(err)
	}
	line := strings.TrimSpace(string(b))
	var got rawSample
	if err := json.Unmarshal([]byte(line), &got); err != nil {
		t.Fatalf("parse jsonl: %v\nline: %q", err, line)
	}
	if got.TsMs != tsMs {
		t.Errorf("ts_ms: got %d want %d", got.TsMs, tsMs)
	}
	if got.Value != 42.5 {
		t.Errorf("value: got %v want 42.5", got.Value)
	}
	if got.Labels["zone"] != "z0" || got.Labels["rack"] != "r1" {
		t.Errorf("labels mismatch: %#v", got.Labels)
	}
}

func TestRawTee_HourRotation(t *testing.T) {
	tmp := t.TempDir()
	rt := newRawTee(tmp)
	defer rt.Close()

	t12 := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC).UnixMilli()
	t12b := time.Date(2026, 4, 30, 12, 30, 0, 0, time.UTC).UnixMilli()
	t13 := time.Date(2026, 4, 30, 13, 0, 0, 0, time.UTC).UnixMilli()

	rt.Tee("m", t12, 1.0, nil)
	rt.Tee("m", t12b, 2.0, nil)
	if got := rt.totalRotates.Load(); got != 0 {
		t.Errorf("same-hour writes should not rotate: got %d", got)
	}

	rt.Tee("m", t13, 3.0, nil)
	if got := rt.totalRotates.Load(); got != 1 {
		t.Errorf("hour boundary should trigger one rotate: got %d", got)
	}

	rt.flushAll()

	for _, ts := range []int64{t12, t13} {
		p := expectedPath(tmp, "m", ts, "part-000001.jsonl")
		if _, err := os.Stat(p); err != nil {
			t.Errorf("missing part file at %s: %v", p, err)
		}
	}
}

func TestRawTee_PerMetricBuckets(t *testing.T) {
	tmp := t.TempDir()
	rt := newRawTee(tmp)
	defer rt.Close()

	tsMs := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC).UnixMilli()
	rt.Tee("m1", tsMs, 1, nil)
	rt.Tee("m2", tsMs, 2, nil)
	rt.flushAll()

	for _, m := range []string{"m1", "m2"} {
		p := expectedPath(tmp, m, tsMs, "part-000001.jsonl")
		if _, err := os.Stat(p); err != nil {
			t.Errorf("missing part for metric %s: %v", m, err)
		}
	}
}

func TestRawTee_ConcurrentWritesAreSerialised(t *testing.T) {
	tmp := t.TempDir()
	rt := newRawTee(tmp)
	defer rt.Close()

	const N = 1000
	var wg sync.WaitGroup
	tsBase := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC).UnixMilli()
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rt.Tee("m", tsBase+int64(i), float64(i), nil)
		}(i)
	}
	wg.Wait()
	rt.flushAll()

	if got := rt.totalSamples.Load(); got != N {
		t.Errorf("expected %d samples written, got %d", N, got)
	}

	p := expectedPath(tmp, "m", tsBase, "part-000001.jsonl")
	f, err := os.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	rows := 0
	for sc.Scan() {
		var s rawSample
		if err := json.Unmarshal(sc.Bytes(), &s); err != nil {
			t.Fatalf("malformed jsonl row %d: %v\n%s", rows, err, sc.Text())
		}
		rows++
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if rows != N {
		t.Errorf("read %d rows, want %d", rows, N)
	}
}

func TestRawTee_PartPathPrefix_KnownAnchor(t *testing.T) {
	// Pin format with an explicit anchor computed via time.Date so
	// it's robust to any tz / clock-skew on CI.
	ts := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC).UnixMilli()
	got := partPathPrefix("foo", ts)
	want := "raw/foo/2026/04/30/12/"
	if got != want {
		t.Errorf("partPathPrefix: got %q want %q", got, want)
	}

	got = partPathPrefix("bar", 0)
	want = "raw/bar/1970/01/01/00/"
	if got != want {
		t.Errorf("partPathPrefix(epoch): got %q want %q", got, want)
	}
}

func TestRawTee_InstanceIDChangesPartName(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("EXPORTER_INSTANCE_ID", "agent-7")
	rt := newRawTee(tmp)
	defer rt.Close()

	tsMs := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC).UnixMilli()
	rt.Tee("m", tsMs, 1, nil)
	rt.flushAll()

	got := expectedPath(tmp, "m", tsMs, "part-agent-7.jsonl")
	if _, err := os.Stat(got); err != nil {
		t.Errorf("expected per-instance part file at %s: %v", got, err)
	}
}

func TestRawTee_BackgroundFlush(t *testing.T) {
	if testing.Short() {
		t.Skip("background flush is wall-clock dependent")
	}
	tmp := t.TempDir()
	rt := newRawTee(tmp)
	rt.flushInterval = 100 * time.Millisecond
	defer rt.Close()
	stop := rt.startBackgroundFlush()
	defer close(stop)

	tsMs := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC).UnixMilli()
	rt.Tee("m", tsMs, 1, nil)

	time.Sleep(250 * time.Millisecond)

	p := expectedPath(tmp, "m", tsMs, "part-000001.jsonl")
	st, err := os.Stat(p)
	if err != nil {
		t.Fatalf("file missing: %v", err)
	}
	if st.Size() == 0 {
		t.Errorf("expected background flush to have written content")
	}
}
