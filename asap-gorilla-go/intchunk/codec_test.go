package intchunk

import (
	"math"
	"math/rand"
	"testing"
)

// mkSamples pairs a regular-cadence timestamp column with the given values.
func mkSamples(t0, step int64, vals []float64) []Sample {
	s := make([]Sample, len(vals))
	for i, v := range vals {
		s[i] = Sample{T: t0 + int64(i)*step, V: v}
	}
	return s
}

// assertLossless round-trips a block through Encode/DecodeChunks and demands a
// bit-exact match (timestamps and values).
func assertLossless(t *testing.T, name string, in []Sample) EncodeResult {
	t.Helper()
	res, err := Encode(in)
	if err != nil {
		t.Fatalf("%s: encode: %v", name, err)
	}
	got, err := DecodeChunks(res.Chunks)
	if err != nil {
		t.Fatalf("%s: decode: %v", name, err)
	}
	if len(got) != len(in) {
		t.Fatalf("%s: length mismatch: got %d want %d", name, len(got), len(in))
	}
	for i := range in {
		if got[i].T != in[i].T {
			t.Fatalf("%s: ts[%d] mismatch: got %d want %d", name, i, got[i].T, in[i].T)
		}
		// Bit-exact value comparison (NaN-aware).
		if math.Float64bits(got[i].V) != math.Float64bits(in[i].V) {
			t.Fatalf("%s: val[%d] not bit-exact: got %v (%016x) want %v (%016x)",
				name, i, got[i].V, math.Float64bits(got[i].V), in[i].V, math.Float64bits(in[i].V))
		}
	}
	return res
}

// ---------------------------------------------------------------------------
// Round-trip lossless across varied input shapes.
// ---------------------------------------------------------------------------

func TestRoundTripGauge(t *testing.T) {
	// Genuinely fixed-decimal gauge: values are exact 2dp (constructed as
	// int/100), smoothly varying — the real Serf "temperature/sensor" shape the
	// design targets. INT FOR+delta should beat Gorilla here.
	vals := make([]float64, 1000)
	cents := int64(2000)
	for i := range vals {
		cents += int64(math.Round(math.Sin(float64(i)/30) * 8)) // +/- a few cents
		vals[i] = float64(cents) / 100.0
	}
	res := assertLossless(t, "gauge", mkSamples(1_700_000_000_000, 1000, vals))
	if res.Tag == CodecGorillaXOR {
		t.Errorf("gauge: expected an INT codec to win on fixed-decimal data, got %s", res.Tag)
	}
}

func TestRoundTripMonotonicCounter(t *testing.T) {
	// Monotonic integer counter — FOR_DOD should shine.
	vals := make([]float64, 1000)
	v := 100000.0
	for i := range vals {
		v += float64(40 + i%20)
		vals[i] = v
	}
	res := assertLossless(t, "counter", mkSamples(1_700_000_000_000, 1000, vals))
	if res.Tag == CodecGorillaXOR {
		t.Errorf("counter: expected an INT codec to win, got %s", res.Tag)
	}
}

func TestRoundTripConstantRun(t *testing.T) {
	vals := make([]float64, 500)
	for i := range vals {
		vals[i] = 42.5
	}
	res := assertLossless(t, "const", mkSamples(0, 15000, vals))
	if res.Tag == CodecGorillaXOR {
		t.Logf("const: gorilla won (%d bytes) — acceptable, both tiny", res.Bytes)
	}
}

func TestRoundTripIntegerValued(t *testing.T) {
	vals := make([]float64, 300)
	for i := range vals {
		vals[i] = float64(1000 + i*7)
	}
	assertLossless(t, "int-valued", mkSamples(50, 1000, vals))
}

func TestRoundTripNegativesAndZero(t *testing.T) {
	vals := []float64{-5.5, -0.0, 0.0, 5.5, -1234.25, 9999.75, -0.25}
	assertLossless(t, "neg-zero", mkSamples(10, 1000, vals))
}

func TestRoundTripSingleSample(t *testing.T) {
	assertLossless(t, "single", mkSamples(123, 0, []float64{3.14}))
}

func TestRoundTripTwoSamples(t *testing.T) {
	assertLossless(t, "two", mkSamples(123, 1000, []float64{3.1, 3.2}))
}

func TestRoundTripIrregularTimestamps(t *testing.T) {
	s := []Sample{
		{T: 100, V: 1.5}, {T: 250, V: 1.6}, {T: 251, V: 1.7},
		{T: 9000, V: 1.8}, {T: 9001, V: 1.9},
	}
	assertLossless(t, "irregular-ts", s)
}

// ---------------------------------------------------------------------------
// Decimal-exactness guard: true high-precision floats must fall back to Gorilla
// and still round-trip exactly.
// ---------------------------------------------------------------------------

func TestHighPrecisionFloatFallsBackToGorilla(t *testing.T) {
	// 15-significant-digit irrational-ish floats: no decimal scale exponent in
	// [0,15] makes these int64-exact, so tryScaleToInt64 must reject and the
	// best-of-N must choose Gorilla.
	vals := make([]float64, 500)
	for i := range vals {
		vals[i] = math.Pi*float64(i+1)/7.0 + math.Sqrt2*float64(i)/3.0
	}
	// Sanity: confirm the guard itself rejects.
	if _, _, ok := tryScaleToInt64(vals); ok {
		t.Fatalf("guard wrongly accepted high-precision floats as int64-exact")
	}
	res := assertLossless(t, "hi-precision", mkSamples(1, 1000, vals))
	if res.Tag != CodecGorillaXOR {
		t.Fatalf("hi-precision: expected GORILLA_XOR, got %s", res.Tag)
	}
}

func TestGuardRejectsTooDeepDecimal(t *testing.T) {
	// 1e-16 step needs 16 decimal digits — past the float64 exact limit.
	vals := []float64{1.0000000000000001, 1.0000000000000002, 1.0000000000000003}
	if _, _, ok := tryScaleToInt64(vals); ok {
		t.Fatalf("guard wrongly accepted 16-digit decimals")
	}
	assertLossless(t, "deep-decimal", mkSamples(0, 1000, vals))
}

func TestGuardAcceptsFixedDecimal(t *testing.T) {
	vals := []float64{1.25, 2.50, 3.75, 100.00, 0.05}
	scaleExp, ints, ok := tryScaleToInt64(vals)
	if !ok {
		t.Fatalf("guard rejected genuine fixed-decimal data")
	}
	if scaleExp != 2 {
		t.Errorf("expected scaleExp=2 for 2dp data, got %d", scaleExp)
	}
	// Verify the scaled ints decode back exactly (decode divides by 10^e).
	factor := pow10Abs(scaleExp)
	for i := range vals {
		if float64(ints[i])/factor != vals[i] {
			t.Fatalf("scaled int %d does not round-trip: %v != %v", i, float64(ints[i])/factor, vals[i])
		}
	}
}

func TestNaNInfFallBack(t *testing.T) {
	vals := []float64{1.5, math.NaN(), math.Inf(1), -math.Inf(1), 2.5}
	if _, _, ok := tryScaleToInt64(vals); ok {
		t.Fatalf("guard accepted NaN/Inf")
	}
	res := assertLossless(t, "nan-inf", mkSamples(0, 1000, vals))
	if res.Tag != CodecGorillaXOR {
		t.Fatalf("nan-inf: expected GORILLA_XOR, got %s", res.Tag)
	}
}

// ---------------------------------------------------------------------------
// best-of-N actually picks the smallest valid encoding.
// ---------------------------------------------------------------------------

func TestBestOfNPicksSmallest(t *testing.T) {
	// Build the three candidates directly for a fixed-decimal counter and check
	// Encode's winner equals the minimum-byte candidate.
	vals := make([]float64, 1000)
	v := 0.0
	for i := range vals {
		v += 1.5 // smooth ramp -> dod near-zero -> FOR_DOD tiny
		vals[i] = v
	}
	samples := mkSamples(1_700_000_000_000, 1000, vals)

	scaleExp, ints, ok := tryScaleToInt64(vals)
	if !ok {
		t.Fatalf("expected fixed-decimal to scale")
	}
	delChunks, _ := encodeIntChunks(samples, scaleExp, ints, false)
	dodChunks, _ := encodeIntChunks(samples, scaleExp, ints, true)
	gb, _ := encodeGorillaChunk(samples)
	sizes := map[CodecTag]int{
		CodecIntForDelta: totalLen(delChunks),
		CodecIntForDoD:   totalLen(dodChunks),
		CodecGorillaXOR:  len(gb),
	}
	minTag := CodecGorillaXOR
	for tag, sz := range sizes {
		if sz < sizes[minTag] {
			minTag = tag
		}
	}

	res, err := Encode(samples)
	if err != nil {
		t.Fatal(err)
	}
	if res.Tag != minTag {
		t.Errorf("best-of-N picked %s (%d B) but smallest candidate is %s; sizes=%v",
			res.Tag, res.Bytes, minTag, sizes)
	}
	if res.Bytes != sizes[minTag] {
		t.Errorf("winner byte size %d != smallest candidate %d", res.Bytes, sizes[minTag])
	}
	t.Logf("candidate sizes: delta=%d dod=%d gorilla=%d -> winner=%s",
		sizes[CodecIntForDelta], sizes[CodecIntForDoD], sizes[CodecGorillaXOR], res.Tag)
}

// ---------------------------------------------------------------------------
// Overflow chunk-cut: a residual that exceeds the integer width must end the
// chunk and start a fresh one; the concatenation must decode to the original.
// ---------------------------------------------------------------------------

func TestOverflowChunkCut(t *testing.T) {
	// Integer values with a giant jump partway through forces a residual that
	// overflows maxResidualWidth, so encodeIntChunks must cut and re-base.
	n := 60
	vals := make([]float64, n)
	for i := 0; i < n; i++ {
		switch {
		case i < 20:
			vals[i] = float64(i)
		case i < 40:
			// Jump by > 2^maxResidualWidth to force an overflow cut.
			vals[i] = float64(i) + math.Ldexp(1, maxResidualWidth+4)
		default:
			vals[i] = float64(i) + math.Ldexp(1, maxResidualWidth+4) + math.Ldexp(1, maxResidualWidth+5)
		}
	}
	samples := mkSamples(0, 1000, vals)

	scaleExp, ints, ok := tryScaleToInt64(vals)
	if !ok {
		t.Fatalf("integer data should scale exactly")
	}
	chunks, ok := encodeIntChunks(samples, scaleExp, ints, false)
	if !ok {
		t.Fatalf("encodeIntChunks failed")
	}
	if len(chunks) < 2 {
		t.Fatalf("expected overflow to cut into >=2 chunks, got %d", len(chunks))
	}
	got, err := DecodeChunks(chunks)
	if err != nil {
		t.Fatalf("decode cut chunks: %v", err)
	}
	if len(got) != n {
		t.Fatalf("cut chunks decoded %d samples, want %d", len(got), n)
	}
	for i := range samples {
		if got[i].T != samples[i].T || math.Float64bits(got[i].V) != math.Float64bits(samples[i].V) {
			t.Fatalf("cut sample %d mismatch: got (%d,%v) want (%d,%v)",
				i, got[i].T, got[i].V, samples[i].T, samples[i].V)
		}
	}
	// Each cut chunk must be independently decodable.
	for ci, c := range chunks {
		if _, err := DecodeChunk(c); err != nil {
			t.Fatalf("cut chunk %d not independently decodable: %v", ci, err)
		}
	}
	t.Logf("overflow produced %d chunks", len(chunks))
}

func TestOverflowChunkCutDoD(t *testing.T) {
	// Same idea but exercise the FOR_DOD cut path.
	n := 50
	vals := make([]float64, n)
	for i := 0; i < n; i++ {
		if i == 25 {
			vals[i] = math.Ldexp(1, maxResidualWidth+6)
		} else {
			vals[i] = float64(i * 3)
		}
	}
	samples := mkSamples(0, 1000, vals)
	scaleExp, ints, _ := tryScaleToInt64(vals)
	chunks, ok := encodeIntChunks(samples, scaleExp, ints, true)
	if !ok {
		t.Fatalf("dod encodeIntChunks failed")
	}
	got, err := DecodeChunks(chunks)
	if err != nil {
		t.Fatalf("decode dod cut chunks: %v", err)
	}
	for i := range samples {
		if math.Float64bits(got[i].V) != math.Float64bits(samples[i].V) {
			t.Fatalf("dod cut sample %d mismatch", i)
		}
	}
}

// ---------------------------------------------------------------------------
// Fuzz-ish randomized round-trip across mixed shapes.
// ---------------------------------------------------------------------------

func TestRandomizedRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(0xC01D))
	for iter := 0; iter < 200; iter++ {
		n := 1 + rng.Intn(300)
		vals := make([]float64, n)
		mode := rng.Intn(4)
		base := rng.Float64() * 1000
		for i := 0; i < n; i++ {
			switch mode {
			case 0: // fixed 2dp gauge
				vals[i] = math.Round((base+rng.NormFloat64()*5)*100) / 100
			case 1: // monotonic integer counter
				base += float64(rng.Intn(100))
				vals[i] = base
			case 2: // high-precision float
				vals[i] = rng.NormFloat64() * 1e6
			case 3: // constant
				vals[i] = base
			}
		}
		ts := make([]int64, n)
		t0 := rng.Int63n(1 << 40)
		step := int64(1000 + rng.Intn(5000))
		for i := 0; i < n; i++ {
			ts[i] = t0 + int64(i)*step + int64(rng.Intn(3))
		}
		s := make([]Sample, n)
		for i := range vals {
			s[i] = Sample{T: ts[i], V: vals[i]}
		}
		assertLossless(t, "random", s)
	}
}

// ---------------------------------------------------------------------------
// timestamp column round-trips on its own (it is a FOR_DOD-style stream).
// ---------------------------------------------------------------------------

func TestTimestampColumnRoundTrip(t *testing.T) {
	cases := [][]int64{
		{},
		{42},
		{10, 20},
		{0, 1000, 2000, 3000, 4000},         // regular
		{0, 1000, 2000, 5000, 6000, 7000},   // one gap
		{5, 4, 3, 2, 1},                     // decreasing (codec is agnostic)
		{-100, 0, 100, 250, 251, 1_000_000}, // mixed
	}
	for _, ts := range cases {
		w := &byteWriter{}
		encodeTimestamps(w, ts)
		r := &byteReader{buf: w.buf}
		got, err := decodeTimestamps(r, len(ts))
		if err != nil {
			t.Fatalf("ts decode %v: %v", ts, err)
		}
		if len(got) != len(ts) {
			t.Fatalf("ts len mismatch for %v: got %v", ts, got)
		}
		for i := range ts {
			if got[i] != ts[i] {
				t.Fatalf("ts[%d] mismatch for %v: got %d", i, ts, got[i])
			}
		}
	}
}
