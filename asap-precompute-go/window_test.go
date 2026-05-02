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
