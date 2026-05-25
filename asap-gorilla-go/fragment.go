package gorilla

import (
	"container/heap"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
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

const (
	FragmentMetricName       = "asap.gorilla.fragment"
	FragmentPayloadAttribute = "asap.gorilla.fragment.payload"
)

// Fragment is a transport unit produced by resource-constrained edge agents.
// Data is a Prometheus XOR chunk payload, not raw samples.
type Fragment struct {
	MetricName    string            `json:"metric_name"`
	Attributes    map[string]string `json:"attributes,omitempty"`
	MinTime       int64             `json:"min_time_ms"`
	MaxTime       int64             `json:"max_time_ms"`
	Count         int               `json:"count"`
	Encoding      string            `json:"encoding"`
	Data          []byte            `json:"data"`
	OOODropCount  uint64            `json:"ooo_drop_count,omitempty"`
	FragmentULID  string            `json:"fragment_ulid,omitempty"`
	Source        string            `json:"source,omitempty"`
	WatermarkTime int64             `json:"watermark_time_ms,omitempty"`
}

// MarshalFragment encodes a fragment for transport inside an OTLP attribute.
func MarshalFragment(f Fragment) (string, error) {
	if f.Encoding == "" {
		f.Encoding = "xor"
	}
	body, err := json.Marshal(f)
	if err != nil {
		return "", fmt.Errorf("marshal fragment: %w", err)
	}
	return base64.StdEncoding.EncodeToString(body), nil
}

// UnmarshalFragment decodes a fragment produced by MarshalFragment.
func UnmarshalFragment(s string) (Fragment, error) {
	body, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return Fragment{}, fmt.Errorf("decode fragment payload: %w", err)
	}
	var f Fragment
	if err := json.Unmarshal(body, &f); err != nil {
		return Fragment{}, fmt.Errorf("unmarshal fragment: %w", err)
	}
	return f, nil
}

// StreamingFragmentEncoder turns raw samples into encoded XOR fragments. It
// bounds out-of-order memory with an event-time watermark and never keeps a full
// flush window of raw samples.
//
// For long-lived cold-tier backup it encodes EVERY raw series. Under rotating
// high cardinality (e.g. a metric labelled with a churning user_id) new series
// appear continuously and old ones go idle forever. To keep retained state
// bounded to the ACTIVE working set rather than the cumulative set-of-all-series,
// idle series whose state has been fully shipped are evicted (see idleEvictMs).
type StreamingFragmentEncoder struct {
	mu sync.Mutex

	reorderGrace  int64
	samplesPerChk int
	source        string
	// idleEvictMs is the idle threshold: a series with no pending un-shipped
	// points, no open chunk, and whose most recent sample is older than
	// idleEvictMs behind the encoder's max observed event-time is evicted. 0
	// disables eviction (unbounded retention). Default: 3 * reorderGrace.
	idleEvictMs int64

	series      map[string]*fragmentSeriesState
	drainQueue  fragmentDrainHeap
	evictQueue  fragmentEvictHeap
	maxObserved int64
	queued      []Fragment
	dropped     uint64
	evicted     uint64
	closed      bool
}

type fragmentSeriesState struct {
	key        string
	metricName string
	attrs      map[string]string
	pending    pointHeap

	openChunk chunkenc.Chunk
	app       chunkenc.Appender
	openMin   int64
	openMax   int64
	openCount int

	lastTs  int64
	hasLast bool

	// lastActive is the event-time (ms) of the most recent sample observed for
	// this series (set on AddSample, not only on append). It drives idle-based
	// eviction and is independent of lastTs, which advances only on append.
	lastActive int64
	// inEvictQueue avoids pushing duplicate eviction-heap entries for the same
	// series across repeated flushes (entries remain advisory and re-checked).
	inEvictQueue bool
}

type StreamingFragmentOptions struct {
	ReorderGrace    time.Duration
	SamplesPerChunk int
	Source          string
	// IdleEvict bounds retained per-series state to the active working set. A
	// series that has been fully flushed/shipped and has received no sample for
	// longer than IdleEvict (measured against the encoder's max observed
	// event-time) has its per-series state evicted. Defaults to 3*ReorderGrace
	// when unset; a negative value disables eviction.
	IdleEvict time.Duration
}

func NewStreamingFragmentEncoder(opts StreamingFragmentOptions) *StreamingFragmentEncoder {
	if opts.SamplesPerChunk <= 0 {
		opts.SamplesPerChunk = defaultTSDBSamplesPerChunk
	}
	reorderGrace := opts.ReorderGrace.Milliseconds()
	var idleEvictMs int64
	switch {
	case opts.IdleEvict < 0:
		idleEvictMs = 0 // explicitly disabled
	case opts.IdleEvict > 0:
		idleEvictMs = opts.IdleEvict.Milliseconds()
	default:
		idleEvictMs = 3 * reorderGrace // sensible default: ~3 windows of inactivity
	}
	return &StreamingFragmentEncoder{
		reorderGrace:  reorderGrace,
		samplesPerChk: opts.SamplesPerChunk,
		source:        opts.Source,
		idleEvictMs:   idleEvictMs,
		series:        make(map[string]*fragmentSeriesState),
	}
}

func (e *StreamingFragmentEncoder) AddSample(sample TSDBSample) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return fmt.Errorf("streaming fragment encoder is closed")
	}
	tms := sample.Timestamp.UnixNano() / int64(time.Millisecond)
	if tms > e.maxObserved {
		e.maxObserved = tms
	}
	st := e.getSeries(sample.MetricName, sample.Attributes)
	if tms > st.lastActive {
		st.lastActive = tms // track last-activity for idle-based eviction
	}
	heap.Push(&st.pending, pendingPoint{t: tms, v: sample.Value})
	// Enqueue for the per-sample watermark drain only when this point is the
	// series' new earliest-pending timestamp (heap root), so drainLocked pops
	// ~O(series-with-ready-points) per sample instead of scanning every series.
	// Mirrors StreamingTSDBBlockBuilder's drainQueue (issue #46 profiling).
	if st.pending[0].t == tms {
		heap.Push(&e.drainQueue, fragmentDrainEntry{t: tms, st: st})
	}
	return e.drainLocked(e.maxObserved-e.reorderGrace, false)
}

func (e *StreamingFragmentEncoder) Drain(force bool) ([]Fragment, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	watermark := e.maxObserved - e.reorderGrace
	if force {
		watermark = math.MaxInt64
	}
	if err := e.drainLocked(watermark, force); err != nil {
		return nil, err
	}
	out := e.queued
	e.queued = nil
	return out, nil
}

func (e *StreamingFragmentEncoder) ActiveSeries() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.series)
}

func (e *StreamingFragmentEncoder) DroppedSamples() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.dropped
}

// EvictedSeries reports how many idle, fully-shipped series have had their
// per-series state evicted to bound memory under rotating high cardinality.
func (e *StreamingFragmentEncoder) EvictedSeries() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.evicted
}

func (e *StreamingFragmentEncoder) getSeries(metricName string, attrs map[string]string) *fragmentSeriesState {
	key := metricName + "\xff" + canonicalAttrsKey(attrs)
	st := e.series[key]
	if st != nil {
		return st
	}
	// A re-appearing (previously evicted) series lands here and starts a fresh
	// fragment cleanly: a new state with no open chunk and an empty pending heap.
	st = &fragmentSeriesState{
		key:        key,
		metricName: metricName,
		attrs:      cloneStringMap(attrs),
		lastTs:     math.MinInt64,
		lastActive: math.MinInt64,
	}
	heap.Init(&st.pending)
	e.series[key] = st
	return st
}

// fragmentDrainEntry pairs a series with the earliest-pending timestamp it was
// enqueued under; fragmentDrainHeap is a min-heap on that timestamp so the
// per-sample watermark drain pops only series that actually have ready points.
// Entries are advisory (a series may be enqueued more than once or go stale) —
// draining a series only pops points <= watermark, so revisiting is idempotent.
// Mirrors tsdb.go's drainEntry/seriesDrainHeap.
type fragmentDrainEntry struct {
	t  int64
	st *fragmentSeriesState
}

type fragmentDrainHeap []fragmentDrainEntry

func (h fragmentDrainHeap) Len() int           { return len(h) }
func (h fragmentDrainHeap) Less(i, j int) bool { return h[i].t < h[j].t }
func (h fragmentDrainHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *fragmentDrainHeap) Push(x any)        { *h = append(*h, x.(fragmentDrainEntry)) }
func (h *fragmentDrainHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// fragmentEvictEntry pairs a fully-shipped series with the last-activity time it
// was enqueued under; fragmentEvictHeap is a min-heap on that time so the
// eviction sweep visits the most-idle series first and stops as soon as the heap
// root is still within the idle threshold (O(evicted) per sweep, not O(all)).
//
// Entries are advisory, like the drain heap: a series may receive a newer sample
// after being enqueued (raising its real lastActive above the entry's), so the
// sweep re-checks the series' current state and skips/re-enqueues stale entries
// instead of trusting the heap key. This guarantees a series that is still
// active, still has pending un-shipped points, or has an open chunk is never
// evicted.
type fragmentEvictEntry struct {
	lastActive int64
	st         *fragmentSeriesState
}

type fragmentEvictHeap []fragmentEvictEntry

func (h fragmentEvictHeap) Len() int           { return len(h) }
func (h fragmentEvictHeap) Less(i, j int) bool { return h[i].lastActive < h[j].lastActive }
func (h fragmentEvictHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *fragmentEvictHeap) Push(x any)        { *h = append(*h, x.(fragmentEvictEntry)) }
func (h *fragmentEvictHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// sweepEvictionsLocked reclaims per-series state for series that are idle AND
// fully shipped, bounding retained state to the active working set under
// rotating high cardinality. It pops the most-idle candidates from evictQueue
// and evicts a series only when ALL of these hold (correctness over thrift —
// we never drop un-shipped data nor corrupt an active series' XOR chunk):
//
//   - no pending un-shipped points (st.pending empty),
//   - no open (un-flushed) chunk (st.openChunk nil / openCount 0),
//   - idle: maxObserved - st.lastActive > idleEvictMs.
//
// A series that received a newer sample since being enqueued is re-enqueued
// under its true lastActive rather than evicted. Because the heap is ordered by
// lastActive, the sweep stops at the first candidate still within the idle
// threshold, so it costs O(evicted + re-enqueued) rather than O(all-series).
func (e *StreamingFragmentEncoder) sweepEvictionsLocked() {
	if e.idleEvictMs <= 0 {
		return
	}
	cutoff := e.maxObserved - e.idleEvictMs
	for e.evictQueue.Len() > 0 && e.evictQueue[0].lastActive <= cutoff {
		ent := heap.Pop(&e.evictQueue).(fragmentEvictEntry)
		st := ent.st
		// Stale entry: series was already evicted (and possibly re-created as a
		// distinct state object) — the live map entry, if any, is not this one.
		if e.series[st.key] != st {
			st.inEvictQueue = false
			continue
		}
		// The series became active again after this entry was enqueued; re-enqueue
		// under its current last-activity and stop draining stale-low entries.
		if st.lastActive > ent.lastActive {
			heap.Push(&e.evictQueue, fragmentEvictEntry{lastActive: st.lastActive, st: st})
			continue
		}
		// Correctness guard: never evict a series that still owns un-shipped data
		// (pending points buffered for reorder, or an open chunk not yet flushed),
		// and never evict one that is not actually idle yet.
		if st.pending.Len() > 0 || st.openChunk != nil || st.openCount > 0 {
			st.inEvictQueue = false
			continue
		}
		if e.maxObserved-st.lastActive <= e.idleEvictMs {
			st.inEvictQueue = false
			continue
		}
		st.inEvictQueue = false
		delete(e.series, st.key)
		e.evicted++
	}
}

func (e *StreamingFragmentEncoder) drainLocked(watermark int64, force bool) error {
	if force {
		// Flush path (per-window, not per-sample): drain every series' ready
		// points and flush its open chunk. O(series) is acceptable here.
		for _, st := range e.series {
			if err := e.drainSeriesLocked(st, watermark); err != nil {
				return err
			}
			if err := e.flushOpenChunkLocked(st, watermark); err != nil {
				return err
			}
		}
		e.drainQueue = e.drainQueue[:0] // all drained; reset the advisory heap
		// Now that every series' shipped state is at rest, reclaim idle ones.
		e.sweepEvictionsLocked()
		return nil
	}
	// Per-sample path: pop only the series whose earliest-pending timestamp is
	// <= watermark, instead of scanning ALL series. The old O(series)-per-sample
	// scan was ~64% of agent CPU (issue #46 profiling).
	for e.drainQueue.Len() > 0 && e.drainQueue[0].t <= watermark {
		st := heap.Pop(&e.drainQueue).(fragmentDrainEntry).st
		if err := e.drainSeriesLocked(st, watermark); err != nil {
			return err
		}
		if st.pending.Len() > 0 {
			heap.Push(&e.drainQueue, fragmentDrainEntry{t: st.pending[0].t, st: st})
		}
	}
	return nil
}

// drainSeriesLocked emits one series' pending points that are <= watermark.
func (e *StreamingFragmentEncoder) drainSeriesLocked(st *fragmentSeriesState, watermark int64) error {
	for st.pending.Len() > 0 && st.pending[0].t <= watermark {
		p := heap.Pop(&st.pending).(pendingPoint)
		if st.hasLast && p.t <= st.lastTs {
			e.dropped++
			continue
		}
		if err := e.appendLocked(st, p); err != nil {
			return err
		}
	}
	return nil
}

func (e *StreamingFragmentEncoder) appendLocked(st *fragmentSeriesState, p pendingPoint) error {
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
	if st.openCount >= e.samplesPerChk {
		return e.flushOpenChunkLocked(st, e.maxObserved-e.reorderGrace)
	}
	return nil
}

func (e *StreamingFragmentEncoder) flushOpenChunkLocked(st *fragmentSeriesState, watermark int64) error {
	if st.openChunk == nil || st.openCount == 0 {
		return nil
	}
	e.queued = append(e.queued, Fragment{
		MetricName:    st.metricName,
		Attributes:    cloneStringMap(st.attrs),
		MinTime:       st.openMin,
		MaxTime:       st.openMax,
		Count:         st.openCount,
		Encoding:      "xor",
		Data:          append([]byte(nil), st.openChunk.Bytes()...),
		OOODropCount:  e.dropped,
		FragmentULID:  ulid.Make().String(),
		Source:        e.source,
		WatermarkTime: watermark,
	})
	st.openChunk = nil
	st.app = nil
	st.openCount = 0
	// The series' open chunk has now been shipped, so it is an eviction
	// candidate once it goes idle. Enqueue it (keyed by last-activity) so the
	// next flush sweep can reclaim it in O(evicted) without scanning all series.
	// Only meaningful when eviction is enabled; the sweep re-checks current
	// state, so a stale/duplicate entry is harmless.
	if e.idleEvictMs > 0 && !st.inEvictQueue {
		st.inEvictQueue = true
		heap.Push(&e.evictQueue, fragmentEvictEntry{lastActive: st.lastActive, st: st})
	}
	return nil
}

// FragmentBlockFinalizer consumes edge fragments and writes a Prometheus TSDB
// block. This is intended to run in a gateway/backend collector, not the edge.
type FragmentBlockFinalizer struct {
	tmpRoot       string
	blockDir      string
	id            ulid.ULID
	external      map[string]string
	series        map[string]*fragmentBlockSeries
	minTime       int64
	maxTime       int64
	numSamples    uint64
	numChunks     uint64
	numOOODropped uint64
}

type FragmentBlockOptions struct {
	ExternalLabels map[string]string
	TempDir        string
}

type fragmentBlockSeries struct {
	labels  labels.Labels
	chunks  []chunks.Meta
	samples uint64
}

func NewFragmentBlockFinalizer(opts FragmentBlockOptions) (*FragmentBlockFinalizer, error) {
	tmpRoot, err := os.MkdirTemp(opts.TempDir, "asap-gorilla-finalizer-")
	if err != nil {
		return nil, fmt.Errorf("mkdtemp: %w", err)
	}
	id := ulid.Make()
	return &FragmentBlockFinalizer{
		tmpRoot:  tmpRoot,
		blockDir: filepath.Join(tmpRoot, id.String()),
		id:       id,
		external: cloneStringMap(opts.ExternalLabels),
		series:   make(map[string]*fragmentBlockSeries),
		minTime:  math.MaxInt64,
		maxTime:  math.MinInt64,
	}, nil
}

func (f *FragmentBlockFinalizer) AddFragment(fragment Fragment) error {
	if fragment.Count == 0 || len(fragment.Data) == 0 {
		return nil
	}
	chunk, err := chunkenc.FromData(chunkenc.EncXOR, fragment.Data)
	if err != nil {
		return fmt.Errorf("decode fragment chunk: %w", err)
	}
	ls := f.labelsFor(fragment.MetricName, fragment.Attributes)
	key := ls.String()
	st := f.series[key]
	if st == nil {
		st = &fragmentBlockSeries{labels: ls}
		f.series[key] = st
	}
	st.chunks = append(st.chunks, chunks.Meta{
		Chunk:   chunk,
		MinTime: fragment.MinTime,
		MaxTime: fragment.MaxTime,
	})
	st.samples += uint64(fragment.Count)
	f.numSamples += uint64(fragment.Count)
	f.numChunks++
	f.numOOODropped += fragment.OOODropCount
	if fragment.MinTime < f.minTime {
		f.minTime = fragment.MinTime
	}
	if fragment.MaxTime > f.maxTime {
		f.maxTime = fragment.MaxTime
	}
	return nil
}

func (f *FragmentBlockFinalizer) Finalize(ctx context.Context) (*TSDBBlockArtifact, error) {
	defer func() { _ = os.RemoveAll(f.tmpRoot) }()
	if f.numSamples == 0 {
		return nil, nil
	}
	for _, st := range f.series {
		sort.Slice(st.chunks, func(i, j int) bool { return st.chunks[i].MinTime < st.chunks[j].MinTime })
		dst := st.chunks[:0]
		lastMax := int64(math.MinInt64)
		for _, chk := range st.chunks {
			if chk.MinTime <= lastMax {
				f.numOOODropped++
				continue
			}
			dst = append(dst, chk)
			lastMax = chk.MaxTime
		}
		st.chunks = dst
	}
	if err := f.writeChunks(); err != nil {
		return nil, err
	}
	if err := f.writeIndex(ctx); err != nil {
		return nil, err
	}
	if err := f.writeMeta(); err != nil {
		return nil, err
	}
	files, err := readTSDBBlockFiles(f.blockDir, f.id.String())
	if err != nil {
		return nil, err
	}
	return &TSDBBlockArtifact{
		ULID:          f.id,
		Files:         files,
		MinTime:       f.minTime,
		MaxTime:       f.maxTime,
		NumSeries:     uint64(len(f.nonEmptySeries())),
		NumSamples:    f.numSamples,
		NumChunks:     f.numChunks,
		NumOOODropped: f.numOOODropped,
	}, nil
}

func (f *FragmentBlockFinalizer) writeChunks() error {
	chunkw, err := chunks.NewWriter(filepath.Join(f.blockDir, "chunks"))
	if err != nil {
		return fmt.Errorf("create chunks writer: %w", err)
	}
	for _, st := range f.sortedSeries() {
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

func (f *FragmentBlockFinalizer) writeIndex(ctx context.Context) error {
	w, err := index.NewWriter(ctx, filepath.Join(f.blockDir, "index"))
	if err != nil {
		return fmt.Errorf("create index writer: %w", err)
	}
	defer w.Close()
	for _, sym := range f.sortedSymbols() {
		if err := w.AddSymbol(sym); err != nil {
			return fmt.Errorf("add symbol %q: %w", sym, err)
		}
	}
	for i, st := range f.sortedSeries() {
		if err := w.AddSeries(storage.SeriesRef(i+1), st.labels, st.chunks...); err != nil {
			return fmt.Errorf("add series %s: %w", st.labels.String(), err)
		}
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("close index writer: %w", err)
	}
	return nil
}

func (f *FragmentBlockFinalizer) writeMeta() error {
	meta := tsdb.BlockMeta{
		ULID:    f.id,
		MinTime: f.minTime,
		MaxTime: f.maxTime + 1,
		Stats: tsdb.BlockStats{
			NumSamples:      f.numSamples,
			NumFloatSamples: f.numSamples,
			NumSeries:       uint64(len(f.nonEmptySeries())),
			NumChunks:       f.numChunks,
		},
		Compaction: tsdb.BlockMetaCompaction{Level: 1, Sources: []ulid.ULID{f.id}},
		Version:    1,
	}
	body, err := json.MarshalIndent(meta, "", "\t")
	if err != nil {
		return fmt.Errorf("marshal meta: %w", err)
	}
	if err := os.WriteFile(filepath.Join(f.blockDir, "meta.json"), body, 0o644); err != nil {
		return fmt.Errorf("write meta.json: %w", err)
	}
	return nil
}

func (f *FragmentBlockFinalizer) labelsFor(metricName string, attrs map[string]string) labels.Labels {
	bld := labels.NewBuilder(labels.EmptyLabels())
	bld.Set(labels.MetricName, sanitizePromLabelValue(metricName))
	for k, v := range attrs {
		bld.Set(k, v)
	}
	for k, v := range f.external {
		bld.Set(k, v)
	}
	return bld.Labels()
}

func (f *FragmentBlockFinalizer) nonEmptySeries() []*fragmentBlockSeries {
	out := make([]*fragmentBlockSeries, 0, len(f.series))
	for _, st := range f.series {
		if len(st.chunks) > 0 {
			out = append(out, st)
		}
	}
	return out
}

func (f *FragmentBlockFinalizer) sortedSeries() []*fragmentBlockSeries {
	series := f.nonEmptySeries()
	sort.Slice(series, func(i, j int) bool {
		return labels.Compare(series[i].labels, series[j].labels) < 0
	})
	return series
}

func (f *FragmentBlockFinalizer) sortedSymbols() []string {
	syms := map[string]struct{}{}
	for _, st := range f.nonEmptySeries() {
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

func canonicalAttrsKey(attrs map[string]string) string {
	if len(attrs) == 0 {
		return ""
	}
	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b []byte
	for _, k := range keys {
		b = append(b, k...)
		b = append(b, '=')
		b = append(b, attrs[k]...)
		b = append(b, ';')
	}
	return string(b)
}
