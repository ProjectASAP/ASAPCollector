package precompute

import (
	"errors"
	"testing"
	"time"
)

func framedTestEnvelope(seq uint64, kind, checkpoint, base, payload string) *SketchEnvelope {
	f := SummaryFrameIdentity{IdentityVersion: 1, PlanID: 7, PlanVersion: 2,
		Materialization: 11, SeriesIdentity: "k=a", ProducerID: "edge-a",
		ProducerEpoch: "epoch-1", Sequence: seq, Kind: kind,
		CheckpointID: checkpoint, BaseCheckpointID: base}
	env := &SketchEnvelope{SchemaVersion: 1, SketchType: SketchTypeDDSketch,
		AggID: 1, Labels: []KeyValue{{Key: "k", Value: "a"}},
		WindowStartMs: 0, WindowEndMs: 10_000, Encoding: EncodingProtoDelta,
		Payload: []byte(payload)}
	env.FrameAttributes = SealFrameAttributes(f.OTLPAttributes(), env.Payload)
	return env
}

func TestObserveEnvelopeFramedReplayIsIdempotent(t *testing.T) {
	cfg := &PrecomputeConfig{AggID: 1, SketchType: SketchTypeDDSketch, Mode: Tumbling,
		Window: WindowSpec{Size: 10 * time.Second}}
	p := New(cfg, newFakeFactory(), &fakeObserver{}).(*precompute)
	env := framedTestEnvelope(1, "full", "cp-1", "", "AAA")
	if err := p.ObserveEnvelope(env); err != nil {
		t.Fatal(err)
	}
	if err := p.ObserveEnvelope(env); err != nil {
		t.Fatal(err)
	}
	key := cfg.SeriesKeyForEntry(nil, env.Labels)
	if got := string(p.window.series[key].Sketch.(*fakeSketch).state); got != "AAA" {
		t.Fatalf("replay changed state: %q", got)
	}
}

func TestObserveEnvelopeRejectsGapBadBaseAndChecksum(t *testing.T) {
	cfg := &PrecomputeConfig{AggID: 1, SketchType: SketchTypeDDSketch, Mode: Tumbling,
		Window: WindowSpec{Size: 10 * time.Second}}
	newP := func() *precompute { return New(cfg, newFakeFactory(), &fakeObserver{}).(*precompute) }
	p := newP()
	if err := p.ObserveEnvelope(framedTestEnvelope(2, "delta", "", "cp-1", "B")); !errors.Is(err, ErrFrameGap) {
		t.Fatalf("gap: %v", err)
	}
	p = newP()
	if err := p.ObserveEnvelope(framedTestEnvelope(1, "full", "cp-1", "", "A")); err != nil {
		t.Fatal(err)
	}
	if err := p.ObserveEnvelope(framedTestEnvelope(2, "delta", "", "wrong", "B")); !errors.Is(err, ErrFrameBase) {
		t.Fatalf("base: %v", err)
	}
	p = newP()
	env := framedTestEnvelope(1, "full", "cp-1", "", "A")
	env.Payload[0] = 'X'
	if err := p.ObserveEnvelope(env); !errors.Is(err, ErrFrameChecksum) {
		t.Fatalf("checksum: %v", err)
	}
}
