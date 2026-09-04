package precompute

import (
	"encoding/json"
	"testing"
	"time"
)

func TestAggModeString(t *testing.T) {
	t.Parallel()
	cases := []struct {
		m    AggMode
		want string
	}{
		{ModePerSeries, "PerSeries"},
		{ModeWholeStream, "WholeStream"},
		{AggMode(99), "unknown"},
	}
	for _, tc := range cases {
		if got := tc.m.String(); got != tc.want {
			t.Errorf("scope=%d: want %q, got %q", tc.m, tc.want, got)
		}
	}
}

func TestParseAggMode(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in     string
		want   AggMode
		wantOK bool
	}{
		{"", ModePerSeries, true}, // omitted ⇒ default
		{"per_series", ModePerSeries, true},
		{"perseries", ModePerSeries, true},
		{"PerSeries", ModePerSeries, true},
		{"whole_stream", ModeWholeStream, true},
		{"wholestream", ModeWholeStream, true},
		{"WholeStream", ModeWholeStream, true},
		{"global", ModeWholeStream, true},
		{"nonsense", ModePerSeries, false},
	}
	for _, tc := range cases {
		got, ok := ParseAggMode(tc.in)
		if got != tc.want || ok != tc.wantOK {
			t.Errorf("ParseAggMode(%q) = (%s,%v), want (%s,%v)", tc.in, got, ok, tc.want, tc.wantOK)
		}
	}
}

// TestEffectiveScopeFoldsGlobalAggregation confirms the legacy bool is read as
// WholeStream and that Scope agrees in every combination.
func TestEffectiveScopeFoldsGlobalAggregation(t *testing.T) {
	t.Parallel()
	cases := []struct {
		scope  AggMode
		global bool
		want   AggMode
	}{
		{ModePerSeries, false, ModePerSeries},
		{ModeWholeStream, false, ModeWholeStream},
		{ModePerSeries, true, ModeWholeStream},   // legacy alias
		{ModeWholeStream, true, ModeWholeStream}, // both agree
	}
	for _, tc := range cases {
		cfg := &PrecomputeConfig{Scope: tc.scope, GlobalAggregation: tc.global}
		if got := cfg.effectiveScope(); got != tc.want {
			t.Errorf("scope=%s global=%v: effectiveScope=%s want %s", tc.scope, tc.global, got, tc.want)
		}
		if cfg.isWholeStream() != (tc.want == ModeWholeStream) {
			t.Errorf("scope=%s global=%v: isWholeStream mismatch", tc.scope, tc.global)
		}
	}
	var nilCfg *PrecomputeConfig
	if nilCfg.effectiveScope() != ModePerSeries {
		t.Errorf("nil config: want PerSeries")
	}
}

// TestPrecomputeConfigJSONRoundTripScope confirms Scope serializes like the
// other enums in this package (a plain integer, no custom MarshalJSON) and that
// an omitted `Scope` key decodes to ModePerSeries — so existing plans stay
// byte-compatible.
func TestPrecomputeConfigJSONRoundTripScope(t *testing.T) {
	t.Parallel()

	in := PrecomputeConfig{AggID: 5, SketchType: SketchTypeHLLSketch, Scope: ModeWholeStream}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out PrecomputeConfig
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Scope != ModeWholeStream {
		t.Errorf("round-trip Scope: want WholeStream, got %s", out.Scope)
	}

	// Scope must serialize as a plain integer (mirroring SketchType/Encoding),
	// i.e. the enum's underlying uint8 value, NOT the String() spelling.
	var generic map[string]any
	if err := json.Unmarshal(b, &generic); err != nil {
		t.Fatalf("unmarshal generic: %v", err)
	}
	if v, ok := generic["Scope"].(float64); !ok || AggMode(v) != ModeWholeStream {
		t.Errorf("Scope JSON should be the integer %d, got %v", ModeWholeStream, generic["Scope"])
	}

	// A config with NO mode field (legacy plan) decodes to the default.
	var legacy PrecomputeConfig
	if err := json.Unmarshal([]byte(`{"AggID":9,"SketchType":1}`), &legacy); err != nil {
		t.Fatalf("unmarshal legacy: %v", err)
	}
	if legacy.Scope != ModePerSeries {
		t.Errorf("missing mode ⇒ PerSeries, got %s", legacy.Scope)
	}
}

// scopeCfg builds a tumbling DDSketch-typed config for the given scope.
func scopeCfg(scope AggMode) *PrecomputeConfig {
	return &PrecomputeConfig{
		AggID:      1,
		SketchType: SketchTypeDDSketch,
		Mode:       Tumbling,
		Scope:      scope,
		Window:     WindowSpec{Size: time.Second},
	}
}

// observeThreeSeries feeds a three-series stream (host=a/b/c) and drains,
// returning the emitted envelopes. The package fakeObserver appends one byte
// per observation to the fakeSketch's state, so an envelope's Payload length
// equals the number of observations its sketch folded in.
func observeThreeSeries(t *testing.T, p Precompute) []*SketchEnvelope {
	t.Helper()
	for i, h := range []string{"a", "b", "c"} {
		obs := &Observation{
			TimestampMs: 1000,
			Labels:      []KeyValue{{Key: "host", Value: h}},
			Value:       FloatValue(float64(i + 1)),
		}
		if err := p.Observe(obs); err != nil {
			t.Fatalf("observe host=%s: %v", h, err)
		}
	}
	return p.Drain()
}

// TestPerSeriesEmitsNWholeStreamEmitsOne is the core dual-mode contract: the
// SAME input stream yields one envelope per series under PerSeries and exactly
// one envelope under WholeStream.
func TestPerSeriesEmitsNWholeStreamEmitsOne(t *testing.T) {
	t.Parallel()

	// PerSeries: three distinct host series ⇒ three envelopes, each folding
	// exactly one observation (payload length 1).
	ps := New(scopeCfg(ModePerSeries), newFakeFactory(), &fakeObserver{})
	psEnvs := observeThreeSeries(t, ps)
	if len(psEnvs) != 3 {
		t.Fatalf("PerSeries: want 3 envelopes, got %d", len(psEnvs))
	}
	for _, e := range psEnvs {
		if len(e.Payload) != 1 {
			t.Errorf("PerSeries: each series folds 1 obs, got payload len %d", len(e.Payload))
		}
	}

	// WholeStream: same stream collapses to ONE envelope pooling all 3 obs.
	ws := New(scopeCfg(ModeWholeStream), newFakeFactory(), &fakeObserver{})
	envs := observeThreeSeries(t, ws)
	if len(envs) != 1 {
		t.Fatalf("WholeStream: want 1 envelope, got %d", len(envs))
	}
	env := envs[0]
	if env.AggID != 1 {
		t.Errorf("WholeStream AggID: want 1, got %d", env.AggID)
	}
	// All three observations pooled into the single sketch ⇒ payload len 3.
	if len(env.Payload) != 3 {
		t.Errorf("WholeStream pooled obs: want payload len 3, got %d (%q)", len(env.Payload), env.Payload)
	}
	// The collapsed envelope strips per-series labels.
	if len(env.Labels) != 0 {
		t.Errorf("WholeStream labels: want empty (collapsed), got %v", env.Labels)
	}
	if env.Count != 3 {
		t.Errorf("WholeStream Count: want 3, got %d", env.Count)
	}
}

// TestGlobalAggregationAliasesWholeStream confirms the legacy GlobalAggregation
// bool produces byte-identical behavior to Scope=ModeWholeStream through the
// unified path (back-compat: existing CountSketch-heap configs keep working).
func TestGlobalAggregationAliasesWholeStream(t *testing.T) {
	t.Parallel()

	legacyCfg := &PrecomputeConfig{
		AggID:             1,
		SketchType:        SketchTypeDDSketch,
		Mode:              Tumbling,
		GlobalAggregation: true, // legacy alias, Scope left at zero value
		Window:            WindowSpec{Size: time.Second},
	}
	legacyEnvs := observeThreeSeries(t, New(legacyCfg, newFakeFactory(), &fakeObserver{}))
	modernEnvs := observeThreeSeries(t, New(scopeCfg(ModeWholeStream), newFakeFactory(), &fakeObserver{}))

	if len(legacyEnvs) != 1 || len(modernEnvs) != 1 {
		t.Fatalf("both should emit 1 envelope: legacy=%d modern=%d", len(legacyEnvs), len(modernEnvs))
	}
	le, me := legacyEnvs[0], modernEnvs[0]
	if le.AggID != me.AggID || le.Count != me.Count || len(le.Labels) != len(me.Labels) {
		t.Errorf("legacy GlobalAggregation diverges from WholeStream: legacy=%+v modern=%+v", le, me)
	}
	if string(le.Payload) != string(me.Payload) {
		t.Errorf("payload differs: legacy=%q modern=%q", le.Payload, me.Payload)
	}
}

// TestWholeStreamIgnoresMaxSeries confirms the cardinality cap is a no-op in
// WholeStream (the lone global bucket is never rejected).
func TestWholeStreamIgnoresMaxSeries(t *testing.T) {
	t.Parallel()
	cfg := scopeCfg(ModeWholeStream)
	cfg.MaxSeries = 1 // small cap; PerSeries would drop series 2 and 3
	cfg.OnOverflow = OnOverflowDrop
	p := New(cfg, newFakeFactory(), &fakeObserver{})
	envs := observeThreeSeries(t, p)
	if len(envs) != 1 {
		t.Fatalf("want 1 envelope, got %d", len(envs))
	}
	if len(envs[0].Payload) != 3 {
		t.Errorf("all 3 observations should pool despite MaxSeries=1, got payload len %d", len(envs[0].Payload))
	}
	if got := p.Stats().DroppedOverflow.Load(); got != 0 {
		t.Errorf("WholeStream should drop nothing on overflow, got %d", got)
	}
}

// TestUpdateConfigStagesAtWindowBoundary confirms a live plan change drains
// the old generation before the replacement becomes active.
func TestUpdateConfigStagesAtWindowBoundary(t *testing.T) {
	t.Parallel()

	// Start PerSeries, accumulate two series, then flip to WholeStream.
	p := New(scopeCfg(ModePerSeries), newFakeFactory(), &fakeObserver{})
	for _, h := range []string{"a", "b"} {
		_ = p.Observe(&Observation{TimestampMs: 1000, Labels: []KeyValue{{Key: "host", Value: h}}, Value: FloatValue(1)})
	}
	p.UpdateConfig(&PrecomputeConfigSet{Version: 2, Configs: []PrecomputeConfig{*scopeCfg(ModeWholeStream)}})
	if got := p.(*precompute).activeConfig().effectiveScope(); got != ModePerSeries {
		t.Fatalf("replacement activated before boundary: %v", got)
	}
	// Drain emits the old per-series generation, then promotes WholeStream.
	if envs := p.Drain(); len(envs) != 2 {
		t.Errorf("old generation should drain 2 envelopes, got %d", len(envs))
	}
	// New observations now accumulate under WholeStream ⇒ one envelope.
	for _, h := range []string{"x", "y", "z"} {
		if err := p.Observe(&Observation{TimestampMs: 2_500, Labels: []KeyValue{{Key: "host", Value: h}}, Value: FloatValue(1)}); err != nil {
			t.Fatal(err)
		}
	}
	if envs := p.Drain(); len(envs) != 1 {
		t.Fatalf("post-flip WholeStream: want 1 envelope, got %d", len(envs))
	}

	// Same-scope changes are staged too because grouping/encoding and other
	// semantics can still change independently of scope.
	p2 := New(scopeCfg(ModeWholeStream), newFakeFactory(), &fakeObserver{})
	_ = p2.Observe(&Observation{TimestampMs: 1000, Labels: []KeyValue{{Key: "host", Value: "a"}}, Value: FloatValue(1)})
	sameScope := *scopeCfg(ModeWholeStream)
	sameScope.AggregateBy = []string{"zone"} // a same-scope tweak
	p2.UpdateConfig(&PrecomputeConfigSet{Version: 3, Configs: []PrecomputeConfig{sameScope}})
	if envs := p2.Drain(); len(envs) != 1 {
		t.Errorf("same-scope change should preserve the window, want 1 envelope, got %d", len(envs))
	}
	if got := p2.(*precompute).activeConfig().AggregateBy; len(got) != 1 || got[0] != "zone" {
		t.Fatalf("pending config not promoted: %v", got)
	}
}
