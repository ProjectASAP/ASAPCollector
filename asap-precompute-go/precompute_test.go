package precompute

import (
	"testing"
	"time"
)

func TestPrecompute_EndToEndTumbling(t *testing.T) {
	t.Parallel()
	cfg := &PrecomputeConfig{
		AggID:      42,
		SketchType: SketchTypeDDSketch,
		Mode:       Tumbling,
		Window:     WindowSpec{Size: 10 * time.Second},
	}
	p := New(cfg, newFakeFactory(), &fakeObserver{}).(*precompute)

	// Two series, multiple observations each, all in window 0.
	mustObs := func(label string, ts uint64) {
		t.Helper()
		if err := p.Observe(&Observation{
			TimestampMs:    ts,
			Metric:         "http.requests",
			ResourceLabels: []KeyValue{{Key: "service.name", Value: "web"}},
			Labels:         []KeyValue{{Key: "method", Value: label}},
			Value:          FloatValue(1),
		}); err != nil {
			t.Fatalf("observe %s@%d: %v", label, ts, err)
		}
	}
	mustObs("GET", 1_000)
	mustObs("GET", 2_000)
	mustObs("POST", 3_000)

	envelopes := p.Tick(10_000)
	if len(envelopes) != 2 {
		t.Fatalf("envelopes: want 2, got %d", len(envelopes))
	}
	for _, env := range envelopes {
		if env.AggID != 42 {
			t.Errorf("AggID: want 42, got %d", env.AggID)
		}
		if env.SketchType != SketchTypeDDSketch {
			t.Errorf("SketchType: want DDSketch, got %v", env.SketchType)
		}
		if env.WindowStartMs != 0 || env.WindowEndMs != 10_000 {
			t.Errorf("window: want [0,10000), got [%d,%d)", env.WindowStartMs, env.WindowEndMs)
		}
		if len(env.ResourceLabels) != 1 || env.ResourceLabels[0].Value != "web" {
			t.Errorf("resource labels: %+v", env.ResourceLabels)
		}
		if len(env.Payload) == 0 {
			t.Error("empty payload")
		}
	}

	// Second tick before next window boundary should be a no-op.
	if got := p.Tick(15_000); len(got) != 0 {
		t.Errorf("intra-window tick: want 0 envelopes, got %d", len(got))
	}
}

func TestPrecompute_DeltaTransmissionRoundTrip(t *testing.T) {
	t.Parallel()
	cfg := &PrecomputeConfig{
		AggID:             1,
		SketchType:        SketchTypeDDSketch,
		Mode:              Tumbling,
		Window:            WindowSpec{Size: 10 * time.Second},
		DeltaTransmission: true,
		DeltaThreshold:    1024, // small enough that "delta:" prefix stays under
	}
	p := New(cfg, newFakeFactory(), &fakeObserver{}).(*precompute)

	mustObs := func(ts uint64) {
		t.Helper()
		if err := p.Observe(&Observation{
			TimestampMs: ts,
			Metric:      "m",
			Labels:      []KeyValue{{Key: "k", Value: "a"}},
			Value:       FloatValue(1),
		}); err != nil {
			t.Fatalf("observe@%d: %v", ts, err)
		}
	}

	// Window 0 — first emission, must be PROTO_FULL.
	mustObs(1_000)
	envelopes := p.Tick(10_000)
	if len(envelopes) != 1 {
		t.Fatalf("e1: want 1, got %d", len(envelopes))
	}
	if envelopes[0].Encoding != EncodingProtoFull {
		t.Errorf("e1 encoding: want PROTO_FULL, got %s", envelopes[0].Encoding)
	}

	// Window 1 — small delta; expect PROTO_DELTA.
	mustObs(11_000)
	envelopes = p.Tick(20_000)
	if len(envelopes) != 1 {
		t.Fatalf("e2: want 1, got %d", len(envelopes))
	}
	if envelopes[0].Encoding != EncodingProtoDelta {
		t.Errorf("e2 encoding: want PROTO_DELTA, got %s", envelopes[0].Encoding)
	}

	// Window 2 — pump enough observations to push the synthetic
	// delta over threshold. Each Observe appends one byte ('f') to
	// the fakeSketch's state; the fake's ComputeDeltaAgainst returns
	// "delta:" + state, so once state > threshold-6 bytes the next
	// emission rolls back to PROTO_FULL.
	for i := 0; i < 1100; i++ {
		mustObs(21_000 + uint64(i))
	}
	envelopes = p.Tick(30_000)
	if len(envelopes) != 1 {
		t.Fatalf("e3: want 1, got %d", len(envelopes))
	}
	if envelopes[0].Encoding != EncodingProtoFull {
		t.Errorf("e3 encoding: want PROTO_FULL after large change, got %s", envelopes[0].Encoding)
	}
}

func TestPrecompute_UpdateConfigSwapsAtomically(t *testing.T) {
	t.Parallel()
	initial := &PrecomputeConfig{
		AggID:      1,
		SketchType: SketchTypeDDSketch,
		Mode:       Tumbling,
		Window:     WindowSpec{Size: 10 * time.Second},
	}
	p := New(initial, newFakeFactory(), &fakeObserver{}).(*precompute)

	// Swap to a new sketch type / config.
	cs := &PrecomputeConfigSet{
		Version: 2,
		Configs: []PrecomputeConfig{
			{AggID: 1, SketchType: SketchTypeKLLSketch, Mode: Tumbling, Window: WindowSpec{Size: 5 * time.Second}},
		},
	}
	p.UpdateConfig(cs)

	got := p.activeConfig()
	if got == nil {
		t.Fatal("active config: nil")
	}
	if got.SketchType != SketchTypeKLLSketch {
		t.Errorf("sketch type: want KLL, got %v", got.SketchType)
	}
	if got.Window.Size != 5*time.Second {
		t.Errorf("window size: want 5s, got %v", got.Window.Size)
	}

	// Nil set is a no-op.
	p.UpdateConfig(nil)
	if p.activeConfig().SketchType != SketchTypeKLLSketch {
		t.Error("nil UpdateConfig should not clobber active config")
	}
}

func TestPrecompute_ObserveReturnsErrNoConfig(t *testing.T) {
	t.Parallel()
	p := New(nil, newFakeFactory(), &fakeObserver{})
	err := p.Observe(&Observation{Metric: "m", Value: FloatValue(1)})
	if err != ErrNoConfig {
		t.Fatalf("want ErrNoConfig, got %v", err)
	}
}

func TestPrecompute_StatsAccountsObservations(t *testing.T) {
	t.Parallel()
	cfg := &PrecomputeConfig{
		AggID: 1, SketchType: SketchTypeDDSketch, Mode: Tumbling,
		Window: WindowSpec{Size: 10 * time.Second},
	}
	p := New(cfg, newFakeFactory(), &fakeObserver{})
	for i := 0; i < 5; i++ {
		_ = p.Observe(&Observation{TimestampMs: 1_000, Metric: "m", Value: FloatValue(1)})
	}
	snap := p.Stats().Snapshot()
	if snap.InputObservations != 5 {
		t.Errorf("input obs: want 5, got %d", snap.InputObservations)
	}
	if snap.ActiveSeries != 1 {
		t.Errorf("active series: want 1, got %d", snap.ActiveSeries)
	}
}

// TestPrecompute_EnvelopeCarriesMetricNameCountTemporality confirms
// the runtime threads PrecomputeConfig.MetricName /
// PrecomputeConfig.Temporality and the per-window observation count
// onto each emitted SketchEnvelope. This is the contract the OTel
// adapter relies on after step 2.4b to drop the legacy
// "_asap_metric_name" label hack.
func TestPrecompute_EnvelopeCarriesMetricNameCountTemporality(t *testing.T) {
	t.Parallel()
	cfg := &PrecomputeConfig{
		AggID:       42,
		SketchType:  SketchTypeDDSketch,
		Mode:        Tumbling,
		Window:      WindowSpec{Size: 10 * time.Second},
		MetricName:  "http.server.duration",
		Temporality: 1, // delta
	}
	p := New(cfg, newFakeFactory(), &fakeObserver{}).(*precompute)

	// Three observations on the same series → entry.Count must be 3.
	for i := 0; i < 3; i++ {
		if err := p.Observe(&Observation{
			TimestampMs: 1_000 + uint64(i),
			Metric:      "http.server.duration",
			Labels:      []KeyValue{{Key: "method", Value: "GET"}},
			Value:       FloatValue(1),
		}); err != nil {
			t.Fatalf("observe: %v", err)
		}
	}

	envelopes := p.Tick(10_000)
	if len(envelopes) != 1 {
		t.Fatalf("envelopes: want 1, got %d", len(envelopes))
	}
	env := envelopes[0]
	if env.MetricName != "http.server.duration" {
		t.Errorf("MetricName: want http.server.duration, got %q", env.MetricName)
	}
	if env.Count != 3 {
		t.Errorf("Count: want 3, got %d", env.Count)
	}
	if env.AggregationTemporality != 1 {
		t.Errorf("AggregationTemporality: want 1 (delta), got %d", env.AggregationTemporality)
	}
}

// TestPrecompute_EnvelopeFieldsRespectZeroConfig confirms that when
// MetricName / Temporality are not set on PrecomputeConfig, the
// emitted envelope mirrors the zero values rather than synthesizing
// defaults. The adapter layer is the right place to default
// Temporality to delta — keeping the runtime free of OTel-specific
// defaulting upholds ADR-0002's host-neutral invariant.
func TestPrecompute_EnvelopeFieldsRespectZeroConfig(t *testing.T) {
	t.Parallel()
	cfg := &PrecomputeConfig{
		AggID:      1,
		SketchType: SketchTypeDDSketch,
		Mode:       Tumbling,
		Window:     WindowSpec{Size: 10 * time.Second},
		// MetricName and Temporality intentionally left zero.
	}
	p := New(cfg, newFakeFactory(), &fakeObserver{}).(*precompute)
	if err := p.Observe(&Observation{
		TimestampMs: 1_000,
		Metric:      "m",
		Value:       FloatValue(1),
	}); err != nil {
		t.Fatalf("observe: %v", err)
	}
	envelopes := p.Tick(10_000)
	if len(envelopes) != 1 {
		t.Fatalf("envelopes: want 1, got %d", len(envelopes))
	}
	env := envelopes[0]
	if env.MetricName != "" {
		t.Errorf("MetricName: want empty, got %q", env.MetricName)
	}
	if env.AggregationTemporality != 0 {
		t.Errorf("AggregationTemporality: want 0 (unspecified), got %d", env.AggregationTemporality)
	}
	if env.Count != 1 {
		t.Errorf("Count: want 1, got %d", env.Count)
	}
}

// hllStub is a compile-only test double that confirms a Sketch impl
// can satisfy CardinalitySketch. The shim refactor in steps 2.5–2.9
// will wire real sketchlib-go sketches; this stub locks in the
// trait-shape contract today.
type hllStub struct {
	fakeSketch
}

func (h *hllStub) EstimateCardinality() float64 { return 0 }

// ddsketchStub locks in the QuantileSketch shape.
type ddsketchStub struct {
	fakeSketch
}

func (d *ddsketchStub) Quantile(q float64) float64 { return 0 }

// cmsStub locks in the FrequencySketch shape.
type cmsStub struct {
	fakeSketch
}

func (c *cmsStub) EstimateCount(key []byte) float64 { return 0 }
func (c *cmsStub) TopK(k int) []FrequencyEntry      { return nil }

// Compile-time assertions that the stubs satisfy each sub-interface.
// Failing assertion = compile error, no need for a runtime check.
var (
	_ Sketch            = (*hllStub)(nil)
	_ CardinalitySketch = (*hllStub)(nil)
	_ Sketch            = (*ddsketchStub)(nil)
	_ QuantileSketch    = (*ddsketchStub)(nil)
	_ Sketch            = (*cmsStub)(nil)
	_ FrequencySketch   = (*cmsStub)(nil)
)

// TestSketchSubtraitAssertion is a smoke test that exercises the
// sub-trait type-assertion adapters will use at the encode boundary.
func TestSketchSubtraitAssertion(t *testing.T) {
	t.Parallel()
	var s Sketch = &hllStub{}
	cs, ok := s.(CardinalitySketch)
	if !ok {
		t.Fatal("hllStub should satisfy CardinalitySketch")
	}
	_ = cs.EstimateCardinality()
}
