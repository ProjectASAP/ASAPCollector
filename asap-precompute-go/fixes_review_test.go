package precompute

import (
	"errors"
	"testing"
	"time"
)

// observeAt feeds one scalar observation for series label k=v at ts.
func observeAt(t *testing.T, p Precompute, k, v string, ts uint64) {
	t.Helper()
	if err := p.Observe(&Observation{
		TimestampMs: ts,
		Metric:      "m",
		Labels:      []KeyValue{{Key: k, Value: v}},
		Value:       FloatValue(1),
	}); err != nil {
		t.Fatalf("observe %s=%s@%d: %v", k, v, ts, err)
	}
}

// TestFinishRotate_PrunesVanishedSeriesCache verifies the P1-2 memory-leak
// fix: with delta transmission on, a series that appears in window 0 but NOT
// in window 1 has its snapshot-cache entry evicted after window 1's rotate,
// instead of being retained forever.
func TestFinishRotate_PrunesVanishedSeriesCache(t *testing.T) {
	t.Parallel()
	cfg := &PrecomputeConfig{
		AggID:             1,
		SketchType:        SketchTypeDDSketch,
		Mode:              Tumbling,
		Window:            WindowSpec{Size: 10 * time.Second},
		DeltaTransmission: true,
		DeltaThreshold:    1024,
	}
	p := New(cfg, newFakeFactory(), &fakeObserver{}).(*precompute)

	// Window 0: two series, "a" and "b".
	observeAt(t, p, "k", "a", 1_000)
	observeAt(t, p, "k", "b", 1_000)
	p.Tick(10_000)
	if got := p.snapshotCache.LenOutbound(); got != 2 {
		t.Fatalf("after w0: want 2 cached outbound, got %d", got)
	}

	// Window 1: only "a" reappears. "b" has vanished.
	observeAt(t, p, "k", "a", 11_000)
	p.Tick(20_000)

	// The cache must now retain ONLY "a"; "b" was pruned (P1-2). Without the
	// fix LenOutbound would stay at 2 forever.
	if got := p.snapshotCache.LenOutbound(); got != 1 {
		t.Fatalf("after w1: want 1 cached outbound (vanished series pruned), got %d", got)
	}
}

// TestFinishRotate_DroppedSerializeCounter verifies the P0-2 observability fix:
// when a series fails to serialize (Snapshot error), it is dropped (no
// envelope) AND the DroppedSerialize counter is incremented, so the loss is
// observable from the host-neutral runtime that has no logger.
func TestFinishRotate_DroppedSerializeCounter(t *testing.T) {
	t.Parallel()
	cfg := &PrecomputeConfig{
		AggID:      1,
		SketchType: SketchTypeDDSketch,
		Mode:       Tumbling,
		Window:     WindowSpec{Size: 10 * time.Second},
		// Delta off so serializeSeries takes the Snapshot() path, which the
		// fakeSketch can be made to fail via snapshotErr.
	}
	// Factory whose sketches always fail Snapshot.
	factory := func() Sketch { return &fakeSketch{snapshotErr: errors.New("boom")} }
	p := New(cfg, factory, &fakeObserver{}).(*precompute)

	observeAt(t, p, "k", "a", 1_000)
	envs := p.Tick(10_000)
	if len(envs) != 0 {
		t.Fatalf("want 0 envelopes (all failed serialize), got %d", len(envs))
	}
	snap := p.Stats().Snapshot()
	if snap.DroppedSerialize != 1 {
		t.Fatalf("DroppedSerialize: want 1, got %d", snap.DroppedSerialize)
	}
	if snap.OutputEnvelopes != 0 {
		t.Fatalf("OutputEnvelopes: want 0, got %d", snap.OutputEnvelopes)
	}
}

// TestObserveEnvelope_DeltaDoubleCountGuard verifies the P0-3 fix: two delta
// envelopes for the SAME series-key but DIFFERENT window ranges, arriving in
// one consumer window, must NOT accumulate — the second window's delta
// replaces the first (the per-series sketch is reset before the second apply).
// A same-range re-delivery still merges (idempotent retransmit path unchanged).
func TestObserveEnvelope_DeltaDoubleCountGuard(t *testing.T) {
	t.Parallel()
	cfg := &PrecomputeConfig{
		AggID:      1,
		SketchType: SketchTypeDDSketch,
		Mode:       Tumbling,
		Window:     WindowSpec{Size: 10 * time.Second},
	}
	p := New(cfg, newFakeFactory(), &fakeObserver{}).(*precompute)

	mkEnv := func(start, end uint64, payload string) *SketchEnvelope {
		return &SketchEnvelope{
			SchemaVersion: 1,
			SketchType:    SketchTypeDDSketch,
			AggID:         1,
			Labels:        []KeyValue{{Key: "k", Value: "a"}},
			WindowStartMs: start,
			WindowEndMs:   end,
			Encoding:      EncodingProtoDelta,
			Payload:       []byte(payload),
		}
	}

	// First upstream window [0,10s): fakeSketch.ApplyDelta appends payload.
	if err := p.ObserveEnvelope(mkEnv(0, 10_000, "AAA")); err != nil {
		t.Fatalf("env1: %v", err)
	}
	// Second upstream window [10s,20s) for the SAME key, BEFORE we rotate.
	// The guard must Reset the sketch first so state == "BBB", not "AAABBB".
	if err := p.ObserveEnvelope(mkEnv(10_000, 20_000, "BBB")); err != nil {
		t.Fatalf("env2: %v", err)
	}

	// Inspect the live entry's sketch state.
	key := cfg.SeriesKeyForEntry(nil, []KeyValue{{Key: "k", Value: "a"}})
	p.window.mu.RLock()
	entry := p.window.series[key]
	p.window.mu.RUnlock()
	if entry == nil {
		t.Fatal("no entry for series key")
	}
	fs := entry.Sketch.(*fakeSketch)
	if string(fs.state) != "BBB" {
		t.Fatalf("double-count guard: want state %q (second window replaces first), got %q", "BBB", string(fs.state))
	}

	// A same-range re-delivery of the second window merges (additive) as
	// before — proves we only reset on a DIFFERENT range, not every apply.
	if err := p.ObserveEnvelope(mkEnv(10_000, 20_000, "CCC")); err != nil {
		t.Fatalf("env3: %v", err)
	}
	if string(fs.state) != "BBBCCC" {
		t.Fatalf("same-range re-delivery: want %q, got %q", "BBBCCC", string(fs.state))
	}
}
