package precompute

import (
	"errors"
	"testing"
)

// fakeSketch is a minimal Sketch test double. Snapshot returns the
// current State bytes; ComputeDeltaAgainst returns the difference
// between current State and prev as a byte slice tagged with a
// "delta:" prefix; isFull is reported when the diff exceeds
// threshold.
type fakeSketch struct {
	state       []byte
	deltaForce  bool // when true, ComputeDeltaAgainst always returns isFull=true
	snapshotErr error
	deltaErr    error
}

func (f *fakeSketch) Snapshot() ([]byte, error) {
	if f.snapshotErr != nil {
		return nil, f.snapshotErr
	}
	cp := make([]byte, len(f.state))
	copy(cp, f.state)
	return cp, nil
}

func (f *fakeSketch) ComputeDeltaAgainst(prev []byte, threshold uint64) ([]byte, bool, error) {
	if f.deltaErr != nil {
		return nil, false, f.deltaErr
	}
	if f.deltaForce {
		full, err := f.Snapshot()
		return full, true, err
	}
	delta := make([]byte, 0, len(prev)+len(f.state))
	delta = append(delta, []byte("delta:")...)
	delta = append(delta, f.state...)
	if uint64(len(delta)) > threshold {
		full, err := f.Snapshot()
		return full, true, err
	}
	return delta, false, nil
}

func (f *fakeSketch) ApplyDelta(delta []byte) error {
	f.state = append(f.state, delta...)
	return nil
}

func (f *fakeSketch) Merge(other Sketch) error {
	o, ok := other.(*fakeSketch)
	if !ok {
		return errors.New("fakeSketch: cannot merge non-fake sketch")
	}
	f.state = append(f.state, o.state...)
	return nil
}

func (f *fakeSketch) Reset() {
	f.state = f.state[:0]
}

func TestSnapshotCache_FirstSnapshotReturnsFull(t *testing.T) {
	t.Parallel()
	c := NewSnapshotCache()
	s := &fakeSketch{state: []byte("v1")}
	payload, isFull, err := c.ComputeDelta("k1", s, 1024)
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	if !isFull {
		t.Fatal("first call: want isFull=true")
	}
	if string(payload) != "v1" {
		t.Fatalf("payload: want v1, got %q", payload)
	}
	if got := c.GetOutbound("k1"); string(got) != "v1" {
		t.Fatalf("cache stored: want v1, got %q", got)
	}
}

func TestSnapshotCache_SubsequentReturnsDelta(t *testing.T) {
	t.Parallel()
	c := NewSnapshotCache()
	s := &fakeSketch{state: []byte("v1")}
	if _, _, err := c.ComputeDelta("k1", s, 1024); err != nil {
		t.Fatal(err)
	}
	s.state = []byte("v1v2")
	payload, isFull, err := c.ComputeDelta("k1", s, 1024)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if isFull {
		t.Fatal("second: want isFull=false (delta below threshold)")
	}
	if string(payload) != "delta:v1v2" {
		t.Fatalf("payload: %q", payload)
	}
	// Always-refresh semantics: after a sub-threshold delta the
	// cached outbound advances to the current full snapshot so the
	// next delta is computed against this window, not the original
	// baseline.
	if got := c.GetOutbound("k1"); string(got) != "v1v2" {
		t.Fatalf("cache refreshed: want v1v2, got %q", got)
	}
}

// TestSnapshotCache_AlwaysRefresh exercises the "successive
// sub-threshold deltas" sequence the five legacy processors rely on:
// each delta must be computed against the immediately preceding
// window's snapshot, not against the original baseline.
func TestSnapshotCache_AlwaysRefresh(t *testing.T) {
	t.Parallel()
	c := NewSnapshotCache()
	s := &fakeSketch{state: []byte("a")}
	// Window 0: full snapshot, cache = "a".
	if _, isFull, err := c.ComputeDelta("k", s, 1024); err != nil || !isFull {
		t.Fatalf("w0: full=%v err=%v", isFull, err)
	}
	if got := c.GetOutbound("k"); string(got) != "a" {
		t.Fatalf("w0 cache: %q", got)
	}
	// Window 1: sub-threshold delta. Cache must advance to "ab".
	s.state = []byte("ab")
	if _, isFull, err := c.ComputeDelta("k", s, 1024); err != nil || isFull {
		t.Fatalf("w1: full=%v err=%v", isFull, err)
	}
	if got := c.GetOutbound("k"); string(got) != "ab" {
		t.Fatalf("w1 cache: want ab, got %q", got)
	}
	// Window 2: sub-threshold delta. Cache must advance to "abc".
	s.state = []byte("abc")
	if _, isFull, err := c.ComputeDelta("k", s, 1024); err != nil || isFull {
		t.Fatalf("w2: full=%v err=%v", isFull, err)
	}
	if got := c.GetOutbound("k"); string(got) != "abc" {
		t.Fatalf("w2 cache: want abc, got %q", got)
	}
}

func TestSnapshotCache_AboveThresholdReturnsFullAndRefreshes(t *testing.T) {
	t.Parallel()
	c := NewSnapshotCache()
	s := &fakeSketch{state: []byte("v1")}
	if _, _, err := c.ComputeDelta("k1", s, 1024); err != nil {
		t.Fatal(err)
	}
	// Force isFull on subsequent call.
	s.state = []byte("BIG")
	s.deltaForce = true
	payload, isFull, err := c.ComputeDelta("k1", s, 1024)
	if err != nil {
		t.Fatalf("forced: %v", err)
	}
	if !isFull {
		t.Fatal("forced: want isFull=true")
	}
	if string(payload) != "BIG" {
		t.Fatalf("payload: %q", payload)
	}
	// Cache must have refreshed to the new full state.
	if got := c.GetOutbound("k1"); string(got) != "BIG" {
		t.Fatalf("refreshed: want BIG, got %q", got)
	}
}

func TestSnapshotCache_InboundRoundTrip(t *testing.T) {
	t.Parallel()
	c := NewSnapshotCache()
	if got := c.GetInbound("k"); got != nil {
		t.Fatalf("miss: want nil, got %v", got)
	}
	c.CacheInbound("k", []byte("upstream"))
	if got := c.GetInbound("k"); string(got) != "upstream" {
		t.Fatalf("hit: %q", got)
	}
}

func TestSnapshotCache_Reset(t *testing.T) {
	t.Parallel()
	c := NewSnapshotCache()
	c.CacheOutbound("ko", []byte("o"))
	c.CacheInbound("ki", []byte("i"))
	if c.LenOutbound() != 1 || c.LenInbound() != 1 {
		t.Fatalf("pre: out=%d in=%d", c.LenOutbound(), c.LenInbound())
	}
	c.Reset()
	if c.LenOutbound() != 0 || c.LenInbound() != 0 {
		t.Fatalf("post: out=%d in=%d", c.LenOutbound(), c.LenInbound())
	}
}

func TestSnapshotCache_OutboundFirstTimeFlag(t *testing.T) {
	t.Parallel()
	c := NewSnapshotCache()
	if !c.CacheOutbound("k", []byte("first")) {
		t.Fatal("first call: want firstTime=true")
	}
	if c.CacheOutbound("k", []byte("second")) {
		t.Fatal("second call: want firstTime=false")
	}
}

func TestSnapshotCache_NilSketchReturnsError(t *testing.T) {
	t.Parallel()
	c := NewSnapshotCache()
	if _, _, err := c.ComputeDelta("k", nil, 1024); err == nil {
		t.Fatal("nil sketch: want error")
	}
}
