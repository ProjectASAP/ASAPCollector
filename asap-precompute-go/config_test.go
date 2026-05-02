package precompute

import "testing"

func TestAggregationModeString(t *testing.T) {
	t.Parallel()
	cases := []struct {
		m    AggregationMode
		want string
	}{
		{Tumbling, "Tumbling"},
		{Sliding, "Sliding"},
		{Batch, "Batch"},
		{AggregationMode(99), "unknown"},
	}
	for _, tc := range cases {
		if got := tc.m.String(); got != tc.want {
			t.Errorf("mode=%d: want %q, got %q", tc.m, tc.want, got)
		}
	}
}

func TestOnOverflowString(t *testing.T) {
	t.Parallel()
	cases := []struct {
		o    OnOverflow
		want string
	}{
		{OnOverflowDrop, "Drop"},
		{OnOverflowBlock, "Block"},
		{OnOverflowEvictOldest, "EvictOldest"},
		{OnOverflow(99), "unknown"},
	}
	for _, tc := range cases {
		if got := tc.o.String(); got != tc.want {
			t.Errorf("overflow=%d: want %q, got %q", tc.o, tc.want, got)
		}
	}
}

func TestEncodingString(t *testing.T) {
	t.Parallel()
	cases := []struct {
		e    Encoding
		want string
	}{
		{EncodingUnspecified, "UNSPECIFIED"},
		{EncodingProtoFull, "PROTO_FULL"},
		{EncodingProtoDelta, "PROTO_DELTA"},
		{EncodingMsgpack, "MSGPACK"},
		{Encoding(99), "UNSPECIFIED"},
	}
	for _, tc := range cases {
		if got := tc.e.String(); got != tc.want {
			t.Errorf("encoding=%d: want %q, got %q", tc.e, tc.want, got)
		}
	}
}

func TestMatchOpString(t *testing.T) {
	t.Parallel()
	cases := []struct {
		op   MatchOp
		want string
	}{
		{MatchEqual, "="},
		{MatchNotEqual, "!="},
		{MatchOp(99), "?"},
	}
	for _, tc := range cases {
		if got := tc.op.String(); got != tc.want {
			t.Errorf("op=%d: want %q, got %q", tc.op, tc.want, got)
		}
	}
}

func TestSketchParamsGet(t *testing.T) {
	t.Parallel()
	p := SketchParams{"alpha": 0.01}
	if got := p.Get("alpha", 0.5); got != 0.01 {
		t.Errorf("present: want 0.01, got %v", got)
	}
	if got := p.Get("missing", 0.5); got != 0.5 {
		t.Errorf("missing: want 0.5 default, got %v", got)
	}
}

func TestPrecomputeConfigSetFindByAggID(t *testing.T) {
	t.Parallel()
	cs := &PrecomputeConfigSet{
		Version: 1,
		Configs: []PrecomputeConfig{
			{AggID: 1, SketchType: SketchTypeDDSketch},
			{AggID: 7, SketchType: SketchTypeKLLSketch},
		},
	}
	if got := cs.FindByAggID(7); got == nil || got.SketchType != SketchTypeKLLSketch {
		t.Errorf("hit: want KLL/agg=7, got %+v", got)
	}
	if got := cs.FindByAggID(42); got != nil {
		t.Errorf("miss: want nil, got %+v", got)
	}
	var nilCS *PrecomputeConfigSet
	if got := nilCS.FindByAggID(1); got != nil {
		t.Errorf("nil receiver: want nil, got %+v", got)
	}
}
