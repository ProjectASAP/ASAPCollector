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
type StreamingFragmentEncoder struct {
	mu sync.Mutex

	reorderGrace  int64
	samplesPerChk int
	source        string

	series      map[string]*fragmentSeriesState
	maxObserved int64
	queued      []Fragment
	dropped     uint64
	closed      bool
}

type fragmentSeriesState struct {
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
}

type StreamingFragmentOptions struct {
	ReorderGrace    time.Duration
	SamplesPerChunk int
	Source          string
}

func NewStreamingFragmentEncoder(opts StreamingFragmentOptions) *StreamingFragmentEncoder {
	if opts.SamplesPerChunk <= 0 {
		opts.SamplesPerChunk = defaultTSDBSamplesPerChunk
	}
	return &StreamingFragmentEncoder{
		reorderGrace:  opts.ReorderGrace.Milliseconds(),
		samplesPerChk: opts.SamplesPerChunk,
		source:        opts.Source,
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
	heap.Push(&st.pending, pendingPoint{t: tms, v: sample.Value})
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

func (e *StreamingFragmentEncoder) getSeries(metricName string, attrs map[string]string) *fragmentSeriesState {
	key := metricName + "\xff" + canonicalAttrsKey(attrs)
	st := e.series[key]
	if st != nil {
		return st
	}
	st = &fragmentSeriesState{
		metricName: metricName,
		attrs:      cloneStringMap(attrs),
		lastTs:     math.MinInt64,
	}
	heap.Init(&st.pending)
	e.series[key] = st
	return st
}

func (e *StreamingFragmentEncoder) drainLocked(watermark int64, force bool) error {
	for _, st := range e.series {
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
		if force {
			if err := e.flushOpenChunkLocked(st, watermark); err != nil {
				return err
			}
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
