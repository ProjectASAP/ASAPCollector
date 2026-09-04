package precompute

import (
	"errors"
	"testing"
	"time"
)

// fakeObserver is the test-side SketchObserver.
type fakeObserver struct {
	count uint64
}

func (f *fakeObserver) Observe(s Sketch, v ObservationValue) error {
	fs, ok := s.(*fakeSketch)
	if !ok {
		return errors.New("fakeObserver: not a fakeSketch")
	}
	switch v.Kind {
	case KindFloat:
		fs.state = append(fs.state, byte('f'))
	case KindHash:
		fs.state = append(fs.state, byte('h'))
	case KindBytes:
		fs.state = append(fs.state, v.Bytes...)
	case KindEnvelope:
		// no-op; envelope path is exercised by ObserveEnvelope tests.
	}
	f.count++
	return nil
}

func newFakeFactory() SketchFactory {
	return func() Sketch {
		return &fakeSketch{state: nil}
	}
}

func TestWindow_TumblingRotateAtBoundary(t *testing.T) {
	t.Parallel()
	cfg := &PrecomputeConfig{
		AggID:      1,
		SketchType: SketchTypeDDSketch,
		Mode:       Tumbling,
		Window: WindowSpec{
			Size: 10 * time.Second,
		},
	}
	w := newWindowState()
	stats := NewStats()
	obs := &fakeObserver{}
	factory := newFakeFactory()

	// Pre-populate with one observation in window 0 (start at t=0).
	o1 := &Observation{
		TimestampMs: 1_000, // 1 sec into window
		Metric:      "m",
		Labels:      []KeyValue{{Key: "k", Value: "a"}},
		Value:       FloatValue(1),
	}
	if err := w.observe(o1, cfg, factory, obs, stats); err != nil {
		t.Fatalf("observe1: %v", err)
	}

	// Rotate at the window boundary (t=10s). All series should be
	// drained; activeSeries reset.
	closed, rng := w.rotate(10_000, cfg)
	if len(closed) != 1 {
		t.Fatalf("closed: want 1 series, got %d", len(closed))
	}
	if rng[0] != 0 || rng[1] != 10_000 {
		t.Fatalf("range: want [0,10000), got %v", rng)
	}
	if w.activeSeriesCount() != 0 {
		t.Fatalf("post-rotate: want 0 active, got %d", w.activeSeriesCount())
	}

	// Observation in window 1 (t=15s): goes into the next window.
	o2 := &Observation{
		TimestampMs: 15_000,
		Metric:      "m",
		Labels:      []KeyValue{{Key: "k", Value: "a"}},
		Value:       FloatValue(2),
	}
	if err := w.observe(o2, cfg, factory, obs, stats); err != nil {
		t.Fatalf("observe2: %v", err)
	}
	if w.activeSeriesCount() != 1 {
		t.Fatalf("after o2: want 1 active, got %d", w.activeSeriesCount())
	}

	// Rotate at t=20s — second window should drain.
	closed2, rng2 := w.rotate(20_000, cfg)
	if len(closed2) != 1 {
		t.Fatalf("closed2: want 1, got %d", len(closed2))
	}
	if rng2[0] != 10_000 || rng2[1] != 20_000 {
		t.Fatalf("range2: want [10000,20000), got %v", rng2)
	}
}

func TestWindow_LateDataReturnsErrLateData(t *testing.T) {
	t.Parallel()
	cfg := &PrecomputeConfig{
		AggID:      1,
		SketchType: SketchTypeDDSketch,
		Mode:       Tumbling,
		Window: WindowSpec{
			Size:            10 * time.Second,
			AllowedLateness: 1 * time.Second,
		},
	}
	w := newWindowState()
	stats := NewStats()
	obs := &fakeObserver{}
	factory := newFakeFactory()

	// Initialize the window at t=10s (so activeStart=10000).
	first := &Observation{TimestampMs: 10_500, Metric: "m", Value: FloatValue(1)}
	if err := w.observe(first, cfg, factory, obs, stats); err != nil {
		t.Fatalf("first: %v", err)
	}

	// Late observation at t=8000ms (>1s before activeStart=10000).
	late := &Observation{TimestampMs: 8_000, Metric: "m", Value: FloatValue(1)}
	if err := w.observe(late, cfg, factory, obs, stats); !errors.Is(err, ErrLateData) {
		t.Fatalf("want ErrLateData, got %v", err)
	}
}

func TestWindow_ZeroLatenessRejectsOlderTimestamp(t *testing.T) {
	t.Parallel()
	cfg := &PrecomputeConfig{AggID: 1, SketchType: SketchTypeDDSketch, Mode: Tumbling,
		Window: WindowSpec{Size: 10 * time.Second}}
	w := newWindowState()
	if err := w.observe(&Observation{TimestampMs: 10_500, Value: FloatValue(1)}, cfg, newFakeFactory(), &fakeObserver{}, NewStats()); err != nil {
		t.Fatal(err)
	}
	if err := w.observe(&Observation{TimestampMs: 9_999, Value: FloatValue(1)}, cfg, newFakeFactory(), &fakeObserver{}, NewStats()); !errors.Is(err, ErrLateData) {
		t.Fatalf("want ErrLateData, got %v", err)
	}
}

func TestWindow_FutureTimestampNeverEntersCurrentWindow(t *testing.T) {
	t.Parallel()
	cfg := &PrecomputeConfig{AggID: 1, SketchType: SketchTypeDDSketch, Mode: Tumbling,
		Window: WindowSpec{Size: 10 * time.Second}}
	for _, keyed := range []bool{false, true} {
		w := newWindowState()
		first := &Observation{TimestampMs: 1_000, Labels: []KeyValue{{Key: "k", Value: "a"}}, Value: FloatValue(1)}
		future := &Observation{TimestampMs: 10_000, Labels: first.Labels, Value: FloatValue(2)}
		var err error
		if keyed {
			err = w.observeKeyed(cfg.SeriesKeyFor(first), first, cfg, newFakeFactory(), &fakeObserver{}, NewStats())
		} else {
			err = w.observe(first, cfg, newFakeFactory(), &fakeObserver{}, NewStats())
		}
		if err != nil {
			t.Fatal(err)
		}
		if keyed {
			err = w.observeKeyed(cfg.SeriesKeyFor(future), future, cfg, newFakeFactory(), &fakeObserver{}, NewStats())
		} else {
			err = w.observe(future, cfg, newFakeFactory(), &fakeObserver{}, NewStats())
		}
		if !errors.Is(err, ErrFutureData) {
			t.Fatalf("keyed=%v: want ErrFutureData, got %v", keyed, err)
		}
		closed, _ := w.rotate(10_000, cfg)
		if len(closed) != 1 || closed[0].Count != 1 {
			t.Fatalf("keyed=%v: future sample entered old window: %+v", keyed, closed)
		}
	}
}

func TestPrecompute_FutureTimestampRotatesQueuesAndRetries(t *testing.T) {
	cfg := &PrecomputeConfig{AggID: 1, SketchType: SketchTypeDDSketch, Mode: Tumbling,
		Window: WindowSpec{Size: 10 * time.Second}}
	p := New(cfg, newFakeFactory(), &fakeObserver{}).(*precompute)
	if err := p.Observe(&Observation{TimestampMs: 1_000, Labels: []KeyValue{{Key: "k", Value: "a"}}, Value: FloatValue(1)}); err != nil {
		t.Fatal(err)
	}
	if err := p.Observe(&Observation{TimestampMs: 11_000, Labels: []KeyValue{{Key: "k", Value: "a"}}, Value: FloatValue(2)}); err != nil {
		t.Fatal(err)
	}
	first := p.Tick(11_000)
	if len(first) != 1 || first[0].WindowStartMs != 0 || first[0].WindowEndMs != 10_000 {
		t.Fatalf("queued old window: %+v", first)
	}
	second := p.Drain()
	if len(second) != 1 || second[0].WindowStartMs != 10_000 || second[0].WindowEndMs != 20_000 {
		t.Fatalf("retried new window: %+v", second)
	}
}

func TestWindow_MaxSeriesDropsNew(t *testing.T) {
	t.Parallel()
	cfg := &PrecomputeConfig{
		AggID:      1,
		SketchType: SketchTypeDDSketch,
		Mode:       Tumbling,
		Window:     WindowSpec{Size: 10 * time.Second},
		MaxSeries:  1,
		OnOverflow: OnOverflowDrop,
	}
	w := newWindowState()
	stats := NewStats()
	obs := &fakeObserver{}
	factory := newFakeFactory()

	o1 := &Observation{TimestampMs: 100, Metric: "m", Labels: []KeyValue{{Key: "k", Value: "a"}}, Value: FloatValue(1)}
	if err := w.observe(o1, cfg, factory, obs, stats); err != nil {
		t.Fatalf("o1: %v", err)
	}
	o2 := &Observation{TimestampMs: 200, Metric: "m", Labels: []KeyValue{{Key: "k", Value: "b"}}, Value: FloatValue(1)}
	if err := w.observe(o2, cfg, factory, obs, stats); !errors.Is(err, ErrSeriesCapExceeded) {
		t.Fatalf("o2 over cap: want ErrSeriesCapExceeded, got %v", err)
	}
	if w.activeSeriesCount() != 1 {
		t.Fatalf("active: want 1, got %d", w.activeSeriesCount())
	}
}

func TestWindow_MaxSeriesEvictOldest(t *testing.T) {
	t.Parallel()
	cfg := &PrecomputeConfig{
		AggID:      1,
		SketchType: SketchTypeDDSketch,
		Mode:       Tumbling,
		Window:     WindowSpec{Size: 10 * time.Second},
		MaxSeries:  2,
		OnOverflow: OnOverflowEvictOldest,
	}
	w := newWindowState()
	stats := NewStats()
	obs := &fakeObserver{}
	factory := newFakeFactory()

	// Three series; LastSeen for "a" never updated after t=100, "b"
	// last seen at t=300, "c" arrives at t=400 — "a" is the oldest
	// and should be evicted.
	mustObs := func(o *Observation, label string) {
		t.Helper()
		if err := w.observe(o, cfg, factory, obs, stats); err != nil {
			t.Fatalf("%s: %v", label, err)
		}
	}
	mustObs(&Observation{TimestampMs: 100, Metric: "m", Labels: []KeyValue{{Key: "k", Value: "a"}}, Value: FloatValue(1)}, "a")
	mustObs(&Observation{TimestampMs: 200, Metric: "m", Labels: []KeyValue{{Key: "k", Value: "b"}}, Value: FloatValue(1)}, "b")
	mustObs(&Observation{TimestampMs: 300, Metric: "m", Labels: []KeyValue{{Key: "k", Value: "b"}}, Value: FloatValue(2)}, "b2")
	if w.activeSeriesCount() != 2 {
		t.Fatalf("pre-evict: want 2, got %d", w.activeSeriesCount())
	}
	mustObs(&Observation{TimestampMs: 400, Metric: "m", Labels: []KeyValue{{Key: "k", Value: "c"}}, Value: FloatValue(1)}, "c")
	if w.activeSeriesCount() != 2 {
		t.Fatalf("post-evict: want 2, got %d", w.activeSeriesCount())
	}
	// "a" should have been evicted; "b" and "c" remain.
	w.mu.RLock()
	defer w.mu.RUnlock()
	hasB, hasC, hasA := false, false, false
	for _, e := range w.series {
		for _, kv := range e.Labels {
			switch kv.Value {
			case "a":
				hasA = true
			case "b":
				hasB = true
			case "c":
				hasC = true
			}
		}
	}
	if hasA || !hasB || !hasC {
		t.Fatalf("survivors: hasA=%v hasB=%v hasC=%v", hasA, hasB, hasC)
	}
}

func TestWindow_ResourceLabelsPropagated(t *testing.T) {
	t.Parallel()
	cfg := &PrecomputeConfig{
		AggID:      1,
		SketchType: SketchTypeDDSketch,
		Mode:       Tumbling,
		Window:     WindowSpec{Size: 10 * time.Second},
	}
	w := newWindowState()
	stats := NewStats()
	obs := &fakeObserver{}
	factory := newFakeFactory()

	o := &Observation{
		TimestampMs:    1_000,
		Metric:         "m",
		ResourceLabels: []KeyValue{{Key: "service.name", Value: "api"}},
		Labels:         []KeyValue{{Key: "method", Value: "GET"}},
		Value:          FloatValue(1),
	}
	if err := w.observe(o, cfg, factory, obs, stats); err != nil {
		t.Fatalf("observe: %v", err)
	}
	closed, _ := w.rotate(10_000, cfg)
	if len(closed) != 1 {
		t.Fatalf("len: want 1, got %d", len(closed))
	}
	entry := closed[0]
	if len(entry.ResourceLabels) != 1 ||
		entry.ResourceLabels[0].Key != "service.name" ||
		entry.ResourceLabels[0].Value != "api" {
		t.Fatalf("resource labels: %+v", entry.ResourceLabels)
	}
	if len(entry.Labels) != 1 || entry.Labels[0].Value != "GET" {
		t.Fatalf("labels: %+v", entry.Labels)
	}
}

// slidingCfg builds a sliding config with slide=10s, size=30s ⇒ N=3 panes.
func slidingCfg() *PrecomputeConfig {
	return &PrecomputeConfig{
		AggID:      1,
		SketchType: SketchTypeDDSketch,
		Mode:       Sliding,
		Window:     WindowSpec{Size: 30 * time.Second, Slide: 10 * time.Second},
	}
}

func slidingObs(ts uint64, label string) *Observation {
	return &Observation{
		TimestampMs: ts,
		Metric:      "m",
		Labels:      []KeyValue{{Key: "k", Value: label}},
		Value:       FloatValue(1),
	}
}

// TestWindow_SlidingPaneArithmetic checks the pane-count / step helpers.
func TestWindow_SlidingPaneArithmetic(t *testing.T) {
	t.Parallel()
	cfg := slidingCfg()
	if got := panesPerWindow(cfg); got != 3 {
		t.Fatalf("panesPerWindow: want 3, got %d", got)
	}
	if got := rotationStepMs(cfg); got != 10_000 {
		t.Fatalf("rotationStepMs: want 10000 (slide), got %d", got)
	}
	if got := windowSizeMs(cfg); got != 30_000 {
		t.Fatalf("windowSizeMs: want 30000, got %d", got)
	}
	// Slide==0 degenerates to tumbling-equivalent (1 pane, step==size).
	tum := &PrecomputeConfig{Mode: Sliding, Window: WindowSpec{Size: 10 * time.Second}}
	if got := panesPerWindow(tum); got != 1 {
		t.Fatalf("panesPerWindow(no-slide): want 1, got %d", got)
	}
	if got := rotationStepMs(tum); got != 10_000 {
		t.Fatalf("rotationStepMs(no-slide): want 10000, got %d", got)
	}
}

// TestWindow_SlidingEmitsMergedWindow drives three consecutive panes and
// verifies each slide emits a merged window covering the trailing N panes,
// with the correct [start,end) range and merged observation count.
func TestWindow_SlidingEmitsMergedWindow(t *testing.T) {
	t.Parallel()
	cfg := slidingCfg()
	w := newWindowState()
	stats := NewStats()
	obs := &fakeObserver{}
	factory := newFakeFactory()

	mergedCount := func(closed []*seriesEntry) int {
		if len(closed) != 1 {
			t.Fatalf("expected exactly 1 merged series, got %d", len(closed))
		}
		fs := closed[0].Sketch.(*fakeSketch)
		return len(fs.state) // one 'f' byte per merged float observation
	}

	// Pane 0: [0,10s) — 2 observations.
	for i := 0; i < 2; i++ {
		if err := w.observe(slidingObs(1_000, "a"), cfg, factory, obs, stats); err != nil {
			t.Fatalf("observe p0: %v", err)
		}
	}
	// Close pane 0 at t=10s. Window so far = just pane 0 ⇒ 2 obs.
	closed, rng := w.rotate(10_000, cfg)
	if got := mergedCount(closed); got != 2 {
		t.Fatalf("after slide@10s: merged obs want 2, got %d", got)
	}
	if rng != [2]uint64{0, 10_000} {
		t.Fatalf("range@10s: want [0,10000), got %v", rng)
	}

	// Pane 1: [10s,20s) — 3 observations.
	for i := 0; i < 3; i++ {
		if err := w.observe(slidingObs(12_000, "a"), cfg, factory, obs, stats); err != nil {
			t.Fatalf("observe p1: %v", err)
		}
	}
	// Close pane 1 at t=20s. Window = panes 0+1 ⇒ 2+3 = 5 obs.
	closed, rng = w.rotate(20_000, cfg)
	if got := mergedCount(closed); got != 5 {
		t.Fatalf("after slide@20s: merged obs want 5, got %d", got)
	}
	if rng != [2]uint64{0, 20_000} {
		t.Fatalf("range@20s: want [0,20000), got %v", rng)
	}

	// Pane 2: [20s,30s) — 4 observations.
	for i := 0; i < 4; i++ {
		if err := w.observe(slidingObs(22_000, "a"), cfg, factory, obs, stats); err != nil {
			t.Fatalf("observe p2: %v", err)
		}
	}
	// Close pane 2 at t=30s. Window = panes 0+1+2 ⇒ 2+3+4 = 9 obs (full).
	closed, rng = w.rotate(30_000, cfg)
	if got := mergedCount(closed); got != 9 {
		t.Fatalf("after slide@30s: merged obs want 9, got %d", got)
	}
	if rng != [2]uint64{0, 30_000} {
		t.Fatalf("range@30s: want [0,30000), got %v", rng)
	}

	// Pane 3: [30s,40s) — 1 observation.
	if err := w.observe(slidingObs(32_000, "a"), cfg, factory, obs, stats); err != nil {
		t.Fatalf("observe p3: %v", err)
	}
	// Close pane 3 at t=40s. Pane 0 ages out; window = panes 1+2+3 ⇒
	// 3+4+1 = 8 obs, range = [10s,40s) (oldest retained pane is pane 1).
	closed, rng = w.rotate(40_000, cfg)
	if got := mergedCount(closed); got != 8 {
		t.Fatalf("after slide@40s: merged obs want 8 (pane0 aged out), got %d", got)
	}
	if rng != [2]uint64{10_000, 40_000} {
		t.Fatalf("range@40s: want [10000,40000), got %v", rng)
	}
}

// TestWindow_SlidingEmptySlideStillAges verifies an empty slide still
// advances the window and ages panes out (no emit when the window holds
// no data).
func TestWindow_SlidingEmptySlideStillAges(t *testing.T) {
	t.Parallel()
	cfg := slidingCfg()
	w := newWindowState()
	stats := NewStats()
	obs := &fakeObserver{}
	factory := newFakeFactory()

	// One observation in pane 0.
	if err := w.observe(slidingObs(1_000, "a"), cfg, factory, obs, stats); err != nil {
		t.Fatalf("observe: %v", err)
	}
	// Slide forward 3 empty panes after the data pane closes; pane 0
	// must eventually age out, after which a slide emits nothing.
	if closed, _ := w.rotate(10_000, cfg); len(closed) != 1 { // pane0 closes, window={p0}
		t.Fatalf("slide@10s: want 1, got %d", len(closed))
	}
	if closed, _ := w.rotate(20_000, cfg); len(closed) != 1 { // window={p0,empty}
		t.Fatalf("slide@20s: want 1, got %d", len(closed))
	}
	if closed, _ := w.rotate(30_000, cfg); len(closed) != 1 { // window={p0,empty,empty}
		t.Fatalf("slide@30s: want 1, got %d", len(closed))
	}
	// At t=40s pane 0 ages out and the window holds only empty panes.
	if closed, _ := w.rotate(40_000, cfg); len(closed) != 0 {
		t.Fatalf("slide@40s: want 0 (pane0 aged out, all empty), got %d", len(closed))
	}
}

// TestWindow_SlidingViaRuntime drives Sliding through the full Precompute
// Tick path and checks emitted envelope window ranges.
func TestWindow_SlidingViaRuntime(t *testing.T) {
	t.Parallel()
	cfg := slidingCfg()
	p := New(cfg, newFakeFactory(), &fakeObserver{})

	// Two observations in pane 0.
	for i := 0; i < 2; i++ {
		if err := p.Observe(slidingObs(1_000, "a")); err != nil {
			t.Fatalf("observe: %v", err)
		}
	}
	// Tick before the first slide boundary: nothing due.
	if out := p.Tick(5_000); len(out) != 0 {
		t.Fatalf("Tick@5s: want 0, got %d", len(out))
	}
	// Tick at the slide boundary: one merged envelope for the window.
	out := p.Tick(10_000)
	if len(out) != 1 {
		t.Fatalf("Tick@10s: want 1 envelope, got %d", len(out))
	}
	if out[0].WindowStartMs != 0 || out[0].WindowEndMs != 10_000 {
		t.Fatalf("envelope window: want [0,10000), got [%d,%d)", out[0].WindowStartMs, out[0].WindowEndMs)
	}
	st := p.Stats().Snapshot()
	if st.LastEmittedEnvelopes != 1 {
		t.Fatalf("LastEmittedEnvelopes: want 1, got %d", st.LastEmittedEnvelopes)
	}
}
