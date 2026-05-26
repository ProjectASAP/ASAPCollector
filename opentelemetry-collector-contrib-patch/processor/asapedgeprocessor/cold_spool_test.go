package asapedgeprocessor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	gorilla "github.com/ProjectASAP/asap-gorilla-go"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

// fastShipper returns a fragmentShipper pointed at endpoint with tight retry
// timing so tests don't sleep on backoff.
func fastShipper(endpoint string) *fragmentShipper {
	s := newFragmentShipper(endpoint)
	s.maxRetries = 1
	s.backoff = time.Millisecond
	s.client.Timeout = 2 * time.Second
	return s
}

func testSpoolDir(t *testing.T) string {
	t.Helper()
	d := t.TempDir()
	return filepath.Join(d, "spool")
}

// countGzFiles returns the number of spooled (.gz) batch files in dir.
func countGzFiles(t *testing.T, dir string) int {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0
		}
		t.Fatalf("read spool dir: %v", err)
	}
	n := 0
	for _, e := range ents {
		if filepath.Ext(e.Name()) == ".gz" {
			n++
		}
	}
	return n
}

func sampleFrags(t *testing.T, metric string, n int) []gorilla.Fragment {
	t.Helper()
	enc := gorilla.NewStreamingFragmentEncoder(gorilla.StreamingFragmentOptions{Source: "edge-test"})
	base := time.Unix(1700000000, 0)
	for i := 0; i < n; i++ {
		_ = enc.AddSample(gorilla.TSDBSample{
			MetricName: metric,
			Attributes: map[string]string{"core": "0"},
			Timestamp:  base.Add(time.Duration(i) * time.Second),
			Value:      float64(i),
		})
	}
	frags, err := enc.Drain(true)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(frags) == 0 {
		t.Fatal("no fragments produced")
	}
	return frags
}

// TestSpoolOnFailureThenDrainOnRecovery: a ship failure spools the batch to
// disk; once the server recovers, a drainSpool call re-ships and deletes it.
func TestSpoolOnFailureThenDrainOnRecovery(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	var posts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		posts.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	dir := testSpoolDir(t)
	w := newShipWorker(fastShipper(srv.URL), ColdConfig{
		SpoolDir:           dir,
		SpoolMaxBytes:      1 << 20,
		ShipQueueDepth:     4,
		SpoolRetryInterval: time.Hour, // we drive drain manually
	}, zap.NewNop())
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	frags := sampleFrags(t, "cpu_seconds_total", 8)
	body, err := w.shipper.encode(frags)
	if err != nil {
		t.Fatal(err)
	}

	// Server is failing => shipOrSpool must persist the batch.
	w.shipOrSpool(context.Background(), body)
	if got := countGzFiles(t, dir); got != 1 {
		t.Fatalf("after failed ship: spool files = %d, want 1", got)
	}

	// Recover the server; drainSpool should re-ship + delete the file.
	fail.Store(false)
	w.drainSpool(context.Background())
	if got := countGzFiles(t, dir); got != 0 {
		t.Fatalf("after recovery drain: spool files = %d, want 0 (drained+deleted)", got)
	}
	if posts.Load() == 0 {
		t.Fatal("recovery drain did not POST the spooled batch")
	}
}

// TestSpoolCapEvictsOldest: writing batches past the cap drops the oldest
// spooled files (bounded disk).
func TestSpoolCapEvictsOldest(t *testing.T) {
	// Endpoint that always fails so every batch spools.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)

	dir := testSpoolDir(t)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Encode a representative batch to learn its on-disk size, then set the cap
	// to hold ~3 of them so the 4th eviction triggers.
	probe := newShipWorker(fastShipper(srv.URL), ColdConfig{SpoolDir: dir}, zap.NewNop())
	body, err := probe.shipper.encode(sampleFrags(t, "m", 8))
	if err != nil {
		t.Fatal(err)
	}
	cap := int64(len(body))*3 + int64(len(body))/2 // room for 3, not 4

	w := newShipWorker(fastShipper(srv.URL), ColdConfig{
		SpoolDir:      dir,
		SpoolMaxBytes: cap,
	}, zap.NewNop())

	var names []string
	for i := 0; i < 5; i++ {
		b, encErr := w.shipper.encode(sampleFrags(t, "m", 8))
		if encErr != nil {
			t.Fatal(encErr)
		}
		if serr := w.spool(b); serr != nil {
			t.Fatalf("spool %d: %v", i, serr)
		}
		// Track the newest file name so we can assert the oldest were evicted.
		w.mu.Lock()
		files, _ := w.listSpoolLocked()
		w.mu.Unlock()
		if len(files) > 0 {
			names = append(names, files[len(files)-1].path)
		}
		time.Sleep(2 * time.Millisecond) // ensure distinct UnixNano names
	}

	w.mu.Lock()
	files, total := w.listSpoolLocked()
	w.mu.Unlock()
	if total > cap {
		t.Fatalf("spool total %d exceeds cap %d", total, cap)
	}
	if len(files) > 3 {
		t.Fatalf("spool kept %d files, want <= 3 (cap should evict oldest)", len(files))
	}
	// The newest batch must still be present (we evict oldest, not newest).
	newest := names[len(names)-1]
	present := false
	for _, f := range files {
		if f.path == newest {
			present = true
		}
	}
	if !present {
		t.Fatalf("newest spooled batch %q was evicted; cap must drop OLDEST", newest)
	}
}

// TestFlushDoesNotBlockOnSlowShipper: with a shipper whose endpoint hangs well
// past the flush, flushAll must return promptly (the batch is handed to the
// async worker, not shipped inline).
func TestFlushDoesNotBlockOnSlowShipper(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release // hang until the test releases it
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(func() { close(release); srv.Close() })

	cap := &capMetrics{}
	dir := testSpoolDir(t)
	cfg := &Config{
		ShardCount:     2,
		WindowDuration: time.Hour,
		DropOriginal:   true,
		Cold: ColdConfig{
			Enabled:        true,
			ShipEndpoint:   srv.URL,
			ExternalLabels: map[string]string{"agent": "edge-9"},
			SpoolDir:       dir,
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	set := processor.Settings{TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()}}
	p, err := newProcessor(cfg, set, cap)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	// Shutdown with a short deadline so we don't wait on the hung server.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		_ = p.Shutdown(ctx)
	})

	md := pmetric.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("cpu_seconds_total")
	g := m.SetEmptyGauge()
	base := time.Unix(1700000000, 0)
	for i := 0; i < 10; i++ {
		dp := g.DataPoints().AppendEmpty()
		dp.Attributes().PutStr("core", "0")
		dp.SetDoubleValue(float64(i))
		dp.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(i) * time.Second)))
	}
	if err := p.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}

	done := make(chan struct{})
	start := time.Now()
	go func() {
		p.flushAll(context.Background())
		close(done)
	}()
	select {
	case <-done:
		if el := time.Since(start); el > 2*time.Second {
			t.Fatalf("flushAll took %v; should not block on the network", el)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("flushAll blocked > 2s on a hung shipper; async worker did not decouple it")
	}
}
