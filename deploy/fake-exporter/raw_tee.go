// Ground-truth tee: every app-level event is mirrored to disk in
// the §5.2 cold-store JSONL layout
// (`raw/<metric>/YYYY/MM/DD/HH/part-NNNNNN.jsonl`).
//
// Why this exists: accuracy claims for sketches need ground truth,
// and the cleanest source is the input to SDK aggregation rather
// than a parallel "raw baseline" run. Diffing two independent runs
// (b3-delta vs b0a-raw-stream) is not ground truth — it's two
// samples of a noisy process. The tee gives offline truth at the
// same workload as the sketch run.
//
// Format matches `asap-query-engine/src/drivers/query/fallback/cold_store/format.rs::RawSample`
// byte-for-byte so the same bytes are readable by `LocalFsColdStore`
// at query time.
//
// Throughput note: the writer takes a single mutex per metric per
// hour-bucket. At 20k events/s (cardinality=1000 × freq=10Hz × 2
// instruments) this is fine; at 1M events/s the mutex + JSON
// encoding becomes the bottleneck. Sweep cells beyond that will
// need shard-by-goroutine + lockless writers, tracked as a
// follow-up.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

// rawSample mirrors the on-disk wire format. Field order + tags
// must match `RawSample` in the Rust cold-store format module.
type rawSample struct {
	TsMs   int64             `json:"ts_ms"`
	Labels map[string]string `json:"labels"`
	Value  float64           `json:"value"`
}

// hourBucket is the write target for one (metric, hour). Holds an
// open file + buffered writer + json encoder. One bucket per hour
// per metric — rotation happens lazily when an event lands in a
// new hour.
type hourBucket struct {
	hourMs int64
	path   string
	f      *os.File
	w      *bufio.Writer
	enc    *json.Encoder
	count  uint64 // for diag logging
}

func (h *hourBucket) close() error {
	if h.w != nil {
		_ = h.w.Flush()
	}
	if h.f != nil {
		return h.f.Close()
	}
	return nil
}

// rawTee writes the ground-truth JSONL. One instance covers all
// metrics emitted by this fake-exporter; it switches files on
// hour-bucket rotation per metric.
//
// Disabled (no-op) when root is empty.
type rawTee struct {
	root    string
	enabled bool
	mu      sync.Mutex
	// per-metric current bucket. We only ever keep one bucket
	// open per metric — older hours are closed on rotation.
	buckets map[string]*hourBucket
	// flushInterval gates how often the buffered writer is flushed
	// to the OS. Zero disables periodic flushing (only on rotate +
	// shutdown). Default 1s — strikes a balance between durability
	// and write amplification.
	flushInterval time.Duration

	// stats exposed for log lines. Not paranoid — Add is cheap.
	totalSamples atomic.Uint64
	totalRotates atomic.Uint64
	droppedErrs  atomic.Uint64
}

// newRawTee returns a tee writing to root, or a disabled tee when
// root is empty. The disabled tee has zero overhead on every
// Tee() call (single bool check, no lock).
func newRawTee(root string) *rawTee {
	if root == "" {
		return &rawTee{enabled: false}
	}
	return &rawTee{
		root:          root,
		enabled:       true,
		buckets:       make(map[string]*hourBucket),
		flushInterval: time.Second,
	}
}

// startBackgroundFlush spawns a goroutine that flushes every
// open bucket on `flushInterval`. The caller can ignore the
// returned stop channel — it's wired only for tests.
func (t *rawTee) startBackgroundFlush() chan<- struct{} {
	stop := make(chan struct{})
	if !t.enabled || t.flushInterval == 0 {
		return stop
	}
	go func() {
		tk := time.NewTicker(t.flushInterval)
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				t.flushAll()
			}
		}
	}()
	return stop
}

// flushAll fsyncs every open bucket. Safe under concurrent writes.
func (t *rawTee) flushAll() {
	if !t.enabled {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, b := range t.buckets {
		if b.w != nil {
			_ = b.w.Flush()
		}
	}
}

// Tee writes a single sample to the ground-truth JSONL. Cheap
// (mutex + encode + buffered write). Disabled tees return
// immediately.
//
// `attrs` is converted to a sorted map[string]string to match the
// on-disk format's deterministic ordering (BTreeMap on the Rust
// side serialises sorted by key).
func (t *rawTee) Tee(metric string, tsMs int64, value float64, attrs []attribute.KeyValue) {
	if !t.enabled {
		return
	}

	labels := attrsToLabels(attrs)
	sample := rawSample{TsMs: tsMs, Labels: labels, Value: value}

	t.mu.Lock()
	defer t.mu.Unlock()

	b, err := t.bucketFor(metric, tsMs)
	if err != nil {
		t.droppedErrs.Add(1)
		// Log once per N drops to avoid logorrhea on a misconfigured root.
		if d := t.droppedErrs.Load(); d == 1 || d%10000 == 0 {
			log.Printf("rawTee: dropping sample (count=%d): %v", d, err)
		}
		return
	}

	if err := b.enc.Encode(&sample); err != nil {
		t.droppedErrs.Add(1)
		return
	}
	b.count++
	t.totalSamples.Add(1)
}

// bucketFor returns the open hour-bucket for (metric, ts), opening
// or rotating a new one as needed. Caller must hold `t.mu`.
func (t *rawTee) bucketFor(metric string, tsMs int64) (*hourBucket, error) {
	const hourMs int64 = 3_600_000
	hourStart := (tsMs / hourMs) * hourMs

	if cur, ok := t.buckets[metric]; ok {
		if cur.hourMs == hourStart {
			return cur, nil
		}
		// Rotate — close old hour before opening new.
		_ = cur.close()
		t.totalRotates.Add(1)
		delete(t.buckets, metric)
	}

	dir := filepath.Join(t.root, partPathPrefix(metric, tsMs))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", dir, err)
	}

	// part-000001.jsonl per process. We don't roll within the hour
	// — a single fake-exporter run is bounded enough that one part
	// per (metric, hour) is fine. A multi-process run would need
	// distinct part filenames; pass EXPORTER_INSTANCE_ID as a
	// suffix to avoid clobbering.
	instance := os.Getenv("EXPORTER_INSTANCE_ID")
	name := "part-000001.jsonl"
	if instance != "" {
		name = fmt.Sprintf("part-%s.jsonl", instance)
	}
	path := filepath.Join(dir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	w := bufio.NewWriterSize(f, 64*1024)
	enc := json.NewEncoder(w)
	b := &hourBucket{
		hourMs: hourStart,
		path:   path,
		f:      f,
		w:      w,
		enc:    enc,
	}
	t.buckets[metric] = b
	return b, nil
}

// Close flushes and closes every open bucket. Safe to call on a
// disabled tee.
func (t *rawTee) Close() {
	if !t.enabled {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for k, b := range t.buckets {
		_ = b.close()
		delete(t.buckets, k)
	}
	log.Printf(
		"rawTee: closed; samples=%d rotates=%d dropped=%d",
		t.totalSamples.Load(),
		t.totalRotates.Load(),
		t.droppedErrs.Load(),
	)
}

// partPathPrefix mirrors the Rust `part_path_prefix` function.
// Format: raw/<metric>/YYYY/MM/DD/HH/.
func partPathPrefix(metric string, tsMs int64) string {
	t := time.Unix(0, tsMs*int64(time.Millisecond)).UTC()
	return fmt.Sprintf(
		"raw/%s/%04d/%02d/%02d/%02d/",
		metric,
		t.Year(),
		int(t.Month()),
		t.Day(),
		t.Hour(),
	)
}

// attrsToLabels converts attribute.KeyValue slice to a label map.
// Stringifies non-string values; the Rust side stores everything
// as String.
func attrsToLabels(kvs []attribute.KeyValue) map[string]string {
	if len(kvs) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(kvs))
	keys := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		k := string(kv.Key)
		out[k] = kv.Value.Emit()
		keys = append(keys, k)
	}
	// Sort by key. json.Encode on a map already does this for
	// map[string]string in Go 1.12+, but enforce it explicitly
	// for clarity / future-proofing.
	sort.Strings(keys)
	return out
}
