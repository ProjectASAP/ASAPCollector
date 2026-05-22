package gorilla

import (
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
)

const defaultTSDBSamplesPerChunk = 120

// TSDBSample is one sample destined for a Prometheus TSDB block.
type TSDBSample struct {
	MetricName string
	Attributes map[string]string
	Timestamp  time.Time
	Value      float64
}

// TSDBBlockArtifact is one materialized Prometheus block.
type TSDBBlockArtifact struct {
	ULID          ulid.ULID
	Files         map[string][]byte
	MinTime       int64
	MaxTime       int64
	NumSeries     uint64
	NumSamples    uint64
	NumChunks     uint64
	NumOOODropped uint64
}

// StreamingTSDBBlockBuilder writes Prometheus XOR chunks as samples become
// watermark-safe, then writes index and meta.json at finalize.
//
// The builder is intended to be owned by one runtime processor for one flush
// window. AddSample may be called in arrival order; per-series out-of-order
// samples are held only for ReorderGrace before being emitted or dropped.
type StreamingTSDBBlockBuilder struct {
	mu sync.Mutex

	tmpRoot       string
	blockDir      string
	id            ulid.ULID
	reorderGrace  int64
	samplesPerChk int
	extLabels     map[string]string

	series map[string]*tsdbSeriesState

	// drainQueue is a min-heap of series keyed by their earliest pending
	// timestamp. drainWatermarkLocked pops only the series whose earliest
	// pending point is <= the watermark, instead of scanning every series in
	// `series` on every AddSample (which was O(series)-per-sample — the
	// dominant agent CPU cost, issue #46 profiling).
	drainQueue seriesDrainHeap

	maxObserved int64
	minTime     int64
	maxTime     int64

	numSamples uint64
	numChunks  uint64
	numDropped uint64
	closed     bool
}

// StreamingTSDBOptions configures a streaming block builder.
type StreamingTSDBOptions struct {
	ReorderGrace    time.Duration
	SamplesPerChunk int
	ExternalLabels  map[string]string
	TempDir         string
}

type tsdbSeriesState struct {
	key       string
	labels    labels.Labels
	pending   pointHeap
	openChunk chunkenc.Chunk
	app       chunkenc.Appender
	openMin   int64
	openMax   int64
	openCount int
	lastTs    int64
	hasLast   bool
	chunks    []chunks.Meta
	samples   uint64
}

type pendingPoint struct {
	t int64
	v float64
}

type pointHeap []pendingPoint

func (h pointHeap) Len() int           { return len(h) }
func (h pointHeap) Less(i, j int) bool { return h[i].t < h[j].t }
func (h pointHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *pointHeap) Push(x any)        { *h = append(*h, x.(pendingPoint)) }
func (h *pointHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// drainEntry pairs a series with the earliest-pending timestamp it was enqueued
// under. seriesDrainHeap is a min-heap on that timestamp so drainWatermarkLocked
// can pop only the series that actually have points ready to drain.
//
// Entries are advisory: a series may be enqueued more than once (e.g. an
// out-of-order point lowers its earliest) and entries may go stale after a
// drain. That is harmless — draining a series only pops points <= watermark, so
// re-visiting a series is idempotent. The invariant we keep is: whenever a
// series holds a pending point, the heap contains an entry with t <= that
// series' current earliest pending timestamp (enqueued on new-earliest in
// AddSample and re-enqueued after a partial drain), so no ready series is missed.
type drainEntry struct {
	t  int64
	st *tsdbSeriesState
}

type seriesDrainHeap []drainEntry

func (h seriesDrainHeap) Len() int            { return len(h) }
func (h seriesDrainHeap) Less(i, j int) bool  { return h[i].t < h[j].t }
func (h seriesDrainHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *seriesDrainHeap) Push(x any)         { *h = append(*h, x.(drainEntry)) }
func (h *seriesDrainHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// NewStreamingTSDBBlockBuilder creates a builder with an empty Prometheus block
// directory and an open chunks writer.
func NewStreamingTSDBBlockBuilder(opts StreamingTSDBOptions) (*StreamingTSDBBlockBuilder, error) {
	if opts.SamplesPerChunk <= 0 {
		opts.SamplesPerChunk = defaultTSDBSamplesPerChunk
	}
	tmpRoot, err := os.MkdirTemp(opts.TempDir, "asap-gorilla-tsdb-")
	if err != nil {
		return nil, fmt.Errorf("mkdtemp: %w", err)
	}
	id := ulid.Make()
	blockDir := filepath.Join(tmpRoot, id.String())
	return &StreamingTSDBBlockBuilder{
		tmpRoot:       tmpRoot,
		blockDir:      blockDir,
		id:            id,
		reorderGrace:  opts.ReorderGrace.Milliseconds(),
		samplesPerChk: opts.SamplesPerChunk,
		extLabels:     cloneStringMap(opts.ExternalLabels),
		series:        make(map[string]*tsdbSeriesState),
		minTime:       math.MaxInt64,
		maxTime:       math.MinInt64,
	}, nil
}

// AddSample records a sample and drains any samples now older than the
// builder's event-time watermark.
func (b *StreamingTSDBBlockBuilder) AddSample(sample TSDBSample) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return fmt.Errorf("streaming tsdb builder is closed")
	}
	tms := sample.Timestamp.UnixNano() / int64(time.Millisecond)
	if tms > b.maxObserved {
		b.maxObserved = tms
	}
	st := b.getSeries(sample.MetricName, sample.Attributes)
	heap.Push(&st.pending, pendingPoint{t: tms, v: sample.Value})
	// Enqueue the series for draining only when this point is its new earliest
	// pending timestamp (heap root). For in-order data that's just the first
	// point after each drain; for out-of-order data it's whenever a smaller
	// timestamp arrives. This keeps drainQueue ~O(series-with-pending), not
	// O(samples), and guarantees a ready series is never missed.
	if st.pending[0].t == tms {
		heap.Push(&b.drainQueue, drainEntry{t: tms, st: st})
	}
	return b.drainWatermarkLocked(b.maxObserved - b.reorderGrace)
}

// DrainWatermark emits all samples whose timestamp is <= watermark.
func (b *StreamingTSDBBlockBuilder) DrainWatermark(watermark time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return fmt.Errorf("streaming tsdb builder is closed")
	}
	return b.drainWatermarkLocked(watermark.UnixNano() / int64(time.Millisecond))
}

// ActiveSeries returns the number of series currently known by the block.
func (b *StreamingTSDBBlockBuilder) ActiveSeries() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.series)
}

// Finalize closes all open chunks, writes index and meta.json, reads block files
// into an artifact map, and removes the temporary directory.
func (b *StreamingTSDBBlockBuilder) Finalize(ctx context.Context) (*TSDBBlockArtifact, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, fmt.Errorf("streaming tsdb builder is closed")
	}
	b.closed = true
	defer func() { _ = os.RemoveAll(b.tmpRoot) }()

	if err := b.drainWatermarkLocked(math.MaxInt64); err != nil {
		return nil, err
	}
	for _, st := range b.series {
		if err := b.flushOpenChunkLocked(st); err != nil {
			return nil, err
		}
	}
	if b.numSamples == 0 {
		return nil, nil
	}
	if err := b.writeChunks(); err != nil {
		return nil, err
	}
	if err := b.writeIndex(ctx); err != nil {
		return nil, err
	}
	if err := b.writeMeta(); err != nil {
		return nil, err
	}
	files, err := readTSDBBlockFiles(b.blockDir, b.id.String())
	if err != nil {
		return nil, err
	}
	return &TSDBBlockArtifact{
		ULID:          b.id,
		Files:         files,
		MinTime:       b.minTime,
		MaxTime:       b.maxTime,
		NumSeries:     uint64(len(b.nonEmptySeries())),
		NumSamples:    b.numSamples,
		NumChunks:     b.numChunks,
		NumOOODropped: b.numDropped,
	}, nil
}

func (b *StreamingTSDBBlockBuilder) getSeries(metricName string, attrs map[string]string) *tsdbSeriesState {
	ls := b.labelsFor(metricName, attrs)
	key := ls.String()
	st := b.series[key]
	if st != nil {
		return st
	}
	st = &tsdbSeriesState{key: key, labels: ls, lastTs: math.MinInt64}
	heap.Init(&st.pending)
	b.series[key] = st
	return st
}

func (b *StreamingTSDBBlockBuilder) drainWatermarkLocked(watermark int64) error {
	// Pop only the series whose earliest pending timestamp is <= watermark
	// (drainQueue root), rather than scanning every series. Stale/duplicate
	// entries are skipped harmlessly (the inner loop is a no-op if the series
	// has nothing ready); a series with points still pending after this drain
	// is re-enqueued under its new earliest so a future watermark catches it.
	for b.drainQueue.Len() > 0 && b.drainQueue[0].t <= watermark {
		st := heap.Pop(&b.drainQueue).(drainEntry).st
		for st.pending.Len() > 0 && st.pending[0].t <= watermark {
			p := heap.Pop(&st.pending).(pendingPoint)
			if st.hasLast && p.t <= st.lastTs {
				b.numDropped++
				continue
			}
			if err := b.appendLocked(st, p); err != nil {
				return err
			}
		}
		if st.pending.Len() > 0 {
			heap.Push(&b.drainQueue, drainEntry{t: st.pending[0].t, st: st})
		}
	}
	return nil
}

func (b *StreamingTSDBBlockBuilder) appendLocked(st *tsdbSeriesState, p pendingPoint) error {
	if st.openChunk == nil {
		c := chunkenc.NewXORChunk()
		app, err := c.Appender()
		if err != nil {
			return fmt.Errorf("create xor appender: %w", err)
		}
		st.openChunk = c
		st.app = app
		st.openMin = p.t
		st.openMax = p.t
		st.openCount = 0
	}
	st.app.Append(p.t, p.v)
	st.openMax = p.t
	st.openCount++
	st.lastTs = p.t
	st.hasLast = true
	st.samples++
	b.numSamples++
	if p.t < b.minTime {
		b.minTime = p.t
	}
	if p.t > b.maxTime {
		b.maxTime = p.t
	}
	if st.openCount >= b.samplesPerChk {
		return b.flushOpenChunkLocked(st)
	}
	return nil
}

func (b *StreamingTSDBBlockBuilder) flushOpenChunkLocked(st *tsdbSeriesState) error {
	if st.openChunk == nil || st.openCount == 0 {
		return nil
	}
	st.chunks = append(st.chunks, chunks.Meta{
		Chunk:   st.openChunk,
		MinTime: st.openMin,
		MaxTime: st.openMax,
	})
	b.numChunks++
	st.openChunk = nil
	st.app = nil
	st.openCount = 0
	return nil
}

func (b *StreamingTSDBBlockBuilder) writeChunks() error {
	chunkw, err := chunks.NewWriter(filepath.Join(b.blockDir, "chunks"))
	if err != nil {
		return fmt.Errorf("create chunks writer: %w", err)
	}
	series := b.nonEmptySeries()
	sort.Slice(series, func(i, j int) bool {
		return labels.Compare(series[i].labels, series[j].labels) < 0
	})
	for _, st := range series {
		if err := chunkw.WriteChunks(st.chunks...); err != nil {
			_ = chunkw.Close()
			return fmt.Errorf("write chunks for %s: %w", st.labels.String(), err)
		}
	}
	if err := chunkw.Close(); err != nil {
		return fmt.Errorf("close chunks writer: %w", err)
	}
	return nil
}

func (b *StreamingTSDBBlockBuilder) writeIndex(ctx context.Context) error {
	w, err := index.NewWriter(ctx, filepath.Join(b.blockDir, "index"))
	if err != nil {
		return fmt.Errorf("create index writer: %w", err)
	}
	defer w.Close()

	for _, sym := range b.sortedSymbols() {
		if err := w.AddSymbol(sym); err != nil {
			return fmt.Errorf("add symbol %q: %w", sym, err)
		}
	}
	series := b.nonEmptySeries()
	sort.Slice(series, func(i, j int) bool {
		return labels.Compare(series[i].labels, series[j].labels) < 0
	})
	for i, st := range series {
		if err := w.AddSeries(storage.SeriesRef(i+1), st.labels, st.chunks...); err != nil {
			return fmt.Errorf("add series %s: %w", st.labels.String(), err)
		}
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("close index writer: %w", err)
	}
	return nil
}

func (b *StreamingTSDBBlockBuilder) writeMeta() error {
	meta := tsdb.BlockMeta{
		ULID:    b.id,
		MinTime: b.minTime,
		MaxTime: b.maxTime + 1,
		Stats: tsdb.BlockStats{
			NumSamples:      b.numSamples,
			NumFloatSamples: b.numSamples,
			NumSeries:       uint64(len(b.nonEmptySeries())),
			NumChunks:       b.numChunks,
		},
		Compaction: tsdb.BlockMetaCompaction{
			Level:   1,
			Sources: []ulid.ULID{b.id},
		},
		Version: 1,
	}
	body, err := json.MarshalIndent(meta, "", "\t")
	if err != nil {
		return fmt.Errorf("marshal meta: %w", err)
	}
	if err := os.WriteFile(filepath.Join(b.blockDir, "meta.json"), body, 0o644); err != nil {
		return fmt.Errorf("write meta.json: %w", err)
	}
	return nil
}

func (b *StreamingTSDBBlockBuilder) nonEmptySeries() []*tsdbSeriesState {
	out := make([]*tsdbSeriesState, 0, len(b.series))
	for _, st := range b.series {
		if len(st.chunks) > 0 {
			out = append(out, st)
		}
	}
	return out
}

func (b *StreamingTSDBBlockBuilder) sortedSymbols() []string {
	syms := map[string]struct{}{}
	for _, st := range b.nonEmptySeries() {
		st.labels.Range(func(l labels.Label) {
			syms[l.Name] = struct{}{}
			syms[l.Value] = struct{}{}
		})
	}
	out := make([]string, 0, len(syms))
	for s := range syms {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

func (b *StreamingTSDBBlockBuilder) labelsFor(metricName string, attrs map[string]string) labels.Labels {
	bld := labels.NewBuilder(labels.EmptyLabels())
	bld.Set(labels.MetricName, sanitizePromLabelValue(metricName))
	for k, v := range attrs {
		bld.Set(k, v)
	}
	for k, v := range b.extLabels {
		bld.Set(k, v)
	}
	return bld.Labels()
}

func sanitizePromLabelValue(v string) string {
	if v == "" {
		return "unknown"
	}
	return v
}

func readTSDBBlockFiles(dir, ulidStr string) (map[string][]byte, error) {
	out := make(map[string][]byte)
	err := filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		body, readErr := os.ReadFile(p)
		if readErr != nil {
			return readErr
		}
		rel, relErr := filepath.Rel(dir, p)
		if relErr != nil {
			return relErr
		}
		out[ulidStr+"/"+filepath.ToSlash(rel)] = body
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk block dir: %w", err)
	}
	return out, nil
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

var _ = io.Discard
