package precompute

import (
	"testing"
)

func TestObservationValueConstructors(t *testing.T) {
	t.Parallel()
	t.Run("FloatValue", func(t *testing.T) {
		v := FloatValue(3.14)
		if v.Kind != KindFloat {
			t.Fatalf("kind: want %v, got %v", KindFloat, v.Kind)
		}
		if v.Float != 3.14 {
			t.Fatalf("float: want 3.14, got %v", v.Float)
		}
	})
	t.Run("HashValue", func(t *testing.T) {
		v := HashValue(42)
		if v.Kind != KindHash {
			t.Fatalf("kind: want %v, got %v", KindHash, v.Kind)
		}
		if v.Hash != 42 {
			t.Fatalf("hash: want 42, got %v", v.Hash)
		}
	})
	t.Run("BytesValue", func(t *testing.T) {
		b := []byte{1, 2, 3}
		v := BytesValue(b)
		if v.Kind != KindBytes {
			t.Fatalf("kind: want %v, got %v", KindBytes, v.Kind)
		}
		if string(v.Bytes) != string(b) {
			t.Fatalf("bytes: want %v, got %v", b, v.Bytes)
		}
	})
	t.Run("EnvelopeValue", func(t *testing.T) {
		env := &SketchEnvelope{AggID: 7}
		v := EnvelopeValue(env)
		if v.Kind != KindEnvelope {
			t.Fatalf("kind: want %v, got %v", KindEnvelope, v.Kind)
		}
		if v.Envelope == nil || v.Envelope.AggID != 7 {
			t.Fatalf("envelope: got %+v", v.Envelope)
		}
	})
}

func TestObservationValueKindString(t *testing.T) {
	t.Parallel()
	cases := []struct {
		k    ObservationValueKind
		want string
	}{
		{KindFloat, "Float"},
		{KindHash, "Hash"},
		{KindBytes, "Bytes"},
		{KindEnvelope, "Envelope"},
		{ObservationValueKind(99), "unknown"},
	}
	for _, tc := range cases {
		if got := tc.k.String(); got != tc.want {
			t.Errorf("kind=%d: want %q, got %q", tc.k, tc.want, got)
		}
	}
}

func TestSketchTypeString(t *testing.T) {
	t.Parallel()
	cases := []struct {
		s    SketchType
		want string
	}{
		{SketchTypeUnspecified, "Unspecified"},
		{SketchTypeDDSketch, "DDSketch"},
		{SketchTypeKLLSketch, "KLLSketch"},
		{SketchTypeHLLSketch, "HLLSketch"},
		{SketchTypeCountSketch, "CountSketch"},
		{SketchTypeCountMinSketch, "CountMinSketch"},
		{SketchType(99), "Unspecified"},
	}
	for _, tc := range cases {
		if got := tc.s.String(); got != tc.want {
			t.Errorf("sketch type=%d: want %q, got %q", tc.s, tc.want, got)
		}
	}
}
