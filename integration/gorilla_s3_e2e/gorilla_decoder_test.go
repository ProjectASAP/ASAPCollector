// Self-contained sanity tests for the in-test Go decoder. These do
// NOT exercise the live docker stack — they hand-build a minimal
// GORILLA1 block by mirroring the encoder's bit layout and round-
// trip it through the decoder. The point is to catch a regression
// in the decoder itself BEFORE the live e2e path runs (which might
// otherwise mask a decoder bug as a "no chunks written" failure).

package gorillas3e2e

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"math"
	"testing"
)

// TestGorillaDecoder_RoundTripSingleSeriesSinglePoint builds the
// minimal valid block: 1 series, 1 sample. Validates that the
// decoder reads back the sample (firstTS / firstValue paths only;
// no bit stream entries since there are no follow-on samples).
func TestGorillaDecoder_RoundTripSingleSeriesSinglePoint(t *testing.T) {
	const ts = int64(1_700_000_000_000_000_000)
	const v = 42.5

	body := buildSeriesBody(t, "single_point_metric",
		map[string]string{"job": "test"},
		[]int64{ts}, []float64{v})

	block := buildBlock(t, []byte(body))
	got, err := DecodeBlock(block)
	if err != nil {
		t.Fatalf("DecodeBlock: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 series, got %d", len(got))
	}
	s := got[0]
	if s.Meta.MetricName != "single_point_metric" {
		t.Errorf("metric name: got %q", s.Meta.MetricName)
	}
	if len(s.Samples) != 1 {
		t.Fatalf("want 1 sample, got %d", len(s.Samples))
	}
	if s.Samples[0].TS != ts {
		t.Errorf("ts: got %d want %d", s.Samples[0].TS, ts)
	}
	if s.Samples[0].Value != v {
		t.Errorf("value: got %v want %v", s.Samples[0].Value, v)
	}
}

// TestGorillaDecoder_RoundTripFlatSequence builds a block with
// constant-delta timestamps (1s spacing) and identical values
// across all samples — the cheapest path to exercise the
// "bucket-0" timestamp path AND the "value-unchanged" XOR path.
func TestGorillaDecoder_RoundTripFlatSequence(t *testing.T) {
	const base = int64(1_700_000_000_000_000_000)
	tss := make([]int64, 10)
	vals := make([]float64, 10)
	for i := range tss {
		tss[i] = base + int64(i)*int64(1_000_000_000)
		vals[i] = 7.0
	}
	body := buildSeriesBody(t, "flat_metric", nil, tss, vals)
	block := buildBlock(t, []byte(body))

	got, err := DecodeBlock(block)
	if err != nil {
		t.Fatalf("DecodeBlock: %v", err)
	}
	if len(got) != 1 || len(got[0].Samples) != 10 {
		t.Fatalf("want 1 series of 10 samples; got %d series", len(got))
	}
	for i, s := range got[0].Samples {
		if s.TS != tss[i] || s.Value != vals[i] {
			t.Errorf("sample %d: got (%d, %v) want (%d, %v)",
				i, s.TS, s.Value, tss[i], vals[i])
		}
	}
}

// TestGorillaDecoder_RoundTripVaryingValues builds a block with
// constant-delta timestamps but VARYING values (the increasing
// 1..N sequence the e2e fixture uses). Exercises the value
// XOR path's "new window" + "reuse window" branches.
func TestGorillaDecoder_RoundTripVaryingValues(t *testing.T) {
	const base = int64(1_700_000_000_000_000_000)
	const n = 100
	tss := make([]int64, n)
	vals := make([]float64, n)
	for i := range tss {
		tss[i] = base + int64(i)*int64(1_000_000_000)
		vals[i] = float64(i + 1)
	}
	body := buildSeriesBody(t, "ramp_metric", nil, tss, vals)
	block := buildBlock(t, []byte(body))

	got, err := DecodeBlock(block)
	if err != nil {
		t.Fatalf("DecodeBlock: %v", err)
	}
	if len(got) != 1 || len(got[0].Samples) != n {
		t.Fatalf("want 1 series of %d samples; got %d series", n, len(got))
	}
	for i, s := range got[0].Samples {
		if s.TS != tss[i] || s.Value != vals[i] {
			t.Errorf("sample %d: got (%d, %v) want (%d, %v)",
				i, s.TS, s.Value, tss[i], vals[i])
		}
	}
	// And confirm the ground-truth sum matches the 5050 invariant
	// the e2e fixture also pins.
	var sum float64
	for _, s := range got[0].Samples {
		sum += s.Value
	}
	if sum != 5050.0 {
		t.Errorf("decoded sum = %v, want 5050.0", sum)
	}
}

// -----------------------------------------------------------------
// In-test mini encoder. Mirrors `gorillas3processor/encoder.go`
// just closely enough to round-trip through `DecodeBlock`. NOT a
// full encoder — only used to seed the round-trip tests above.
// -----------------------------------------------------------------

// bitWriter mirrors the MSB-first packed-bytes writer in
// `gorillas3processor/bitwriter.go`. Kept private to the test
// package since it has no production consumer.
type testBitWriter struct {
	buf     bytes.Buffer
	curByte byte
	nbits   uint8
}

func (w *testBitWriter) writeBit(b uint8) {
	if b != 0 {
		w.curByte |= 1 << (7 - w.nbits)
	}
	w.nbits++
	if w.nbits == 8 {
		w.buf.WriteByte(w.curByte)
		w.curByte = 0
		w.nbits = 0
	}
}

func (w *testBitWriter) writeBits(v uint64, n uint8) {
	for i := int(n) - 1; i >= 0; i-- {
		w.writeBit(uint8((v >> uint(i)) & 1))
	}
}

func (w *testBitWriter) bytes() []byte {
	if w.nbits != 0 {
		w.buf.WriteByte(w.curByte)
	}
	return w.buf.Bytes()
}

// encodeTimestamps builds the delta-of-delta bit stream + advertised
// bit length used by the encoder. First TS goes into `firstTS`; the
// rest into the bit stream.
func encodeTimestamps(ts []int64) (firstTS int64, bits []byte, bitLen uint32) {
	if len(ts) == 0 {
		return 0, nil, 0
	}
	firstTS = ts[0]
	if len(ts) == 1 {
		return firstTS, nil, 0
	}
	bw := &testBitWriter{}
	prevTS := ts[0]
	prevDelta := int64(0)
	bitsWritten := 0
	for i := 1; i < len(ts); i++ {
		delta := ts[i] - prevTS
		dd := delta - prevDelta
		switch {
		case dd == 0:
			bw.writeBit(0)
			bitsWritten++
		case fitsSigned(dd, 7):
			bw.writeBits(0b10, 2)
			bw.writeBits(uint64(dd)&((1<<7)-1), 7)
			bitsWritten += 9
		case fitsSigned(dd, 9):
			bw.writeBits(0b110, 3)
			bw.writeBits(uint64(dd)&((1<<9)-1), 9)
			bitsWritten += 12
		case fitsSigned(dd, 12):
			bw.writeBits(0b1110, 4)
			bw.writeBits(uint64(dd)&((1<<12)-1), 12)
			bitsWritten += 16
		default:
			bw.writeBits(0b1111, 4)
			bw.writeBits(uint64(dd), 64)
			bitsWritten += 68
		}
		prevTS = ts[i]
		prevDelta = delta
	}
	return firstTS, bw.bytes(), uint32(bitsWritten)
}

// encodeValues builds the XOR-encoded float bit stream + advertised
// bit length. First value goes into `firstValBits` (raw IEEE-754),
// the rest into the bit stream.
func encodeValues(vals []float64) (firstValBits uint64, bits []byte, bitLen uint32) {
	if len(vals) == 0 {
		return 0, nil, 0
	}
	firstValBits = math.Float64bits(vals[0])
	if len(vals) == 1 {
		return firstValBits, nil, 0
	}
	bw := &testBitWriter{}
	prev := firstValBits
	var lzPrev, tzPrev uint8
	havePrev := false
	bitsWritten := 0
	for i := 1; i < len(vals); i++ {
		vb := math.Float64bits(vals[i])
		x := prev ^ vb
		if x == 0 {
			bw.writeBit(0)
			bitsWritten++
			prev = vb
			continue
		}
		bw.writeBit(1)
		bitsWritten++
		var lz, tz uint8
		// math/bits leading zero count, but we only need it via
		// our decoder's mirror; inline using 64 - sig - tz when we
		// have lz from a quick loop.
		lz = leadingZerosTest(x)
		tz = trailingZerosTest(x)
		sig := uint8(64 - int(lz) - int(tz))
		if havePrev && lz >= lzPrev && tz >= tzPrev {
			bw.writeBit(0)
			bitsWritten++
			s := uint8(64 - int(lzPrev) - int(tzPrev))
			bw.writeBits(x>>uint(tzPrev), s)
			bitsWritten += int(s)
		} else {
			bw.writeBit(1)
			bitsWritten++
			lz5 := lz
			if lz5 > 31 {
				lz5 = 31
			}
			bw.writeBits(uint64(lz5), 5)
			bitsWritten += 5
			if sig == 0 {
				sig = 64
			}
			bw.writeBits(uint64(sig-1), 6)
			bitsWritten += 6
			bw.writeBits(x>>uint(tz), sig)
			bitsWritten += int(sig)
			lzPrev = lz
			tzPrev = tz
			havePrev = true
		}
		prev = vb
	}
	return firstValBits, bw.bytes(), uint32(bitsWritten)
}

func leadingZerosTest(x uint64) uint8 {
	if x == 0 {
		return 64
	}
	var n uint8
	for i := 63; i >= 0; i-- {
		if x&(uint64(1)<<uint(i)) != 0 {
			return n
		}
		n++
	}
	return 64
}

func trailingZerosTest(x uint64) uint8 {
	if x == 0 {
		return 64
	}
	var n uint8
	for i := 0; i < 64; i++ {
		if x&(uint64(1)<<uint(i)) != 0 {
			return n
		}
		n++
	}
	return 64
}

func fitsSigned(v int64, n uint8) bool {
	if n == 0 || n >= 64 {
		return true
	}
	min := -(int64(1) << (n - 1))
	max := (int64(1) << (n - 1)) - 1
	return v >= min && v <= max
}

// buildSeriesBody assembles one series body the same way
// `encodeSeriesBody` does in the production encoder.
func buildSeriesBody(t *testing.T, metric string, attrs map[string]string,
	ts []int64, vals []float64,
) string {
	t.Helper()
	if len(ts) != len(vals) {
		t.Fatalf("ts/vals length mismatch %d vs %d", len(ts), len(vals))
	}
	if len(ts) == 0 {
		t.Fatal("zero-length series rejected by the format")
	}
	if attrs == nil {
		attrs = map[string]string{}
	}
	meta := SeriesMeta{
		MetricName: metric,
		Attributes: attrs,
		StartTS:    ts[0],
		EndTS:      ts[len(ts)-1],
		PointCount: len(ts),
	}
	mb, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal meta: %v", err)
	}

	firstTS, tsBits, tsBitLen := encodeTimestamps(ts)
	firstValBits, valBits, valBitLen := encodeValues(vals)

	var sb bytes.Buffer
	_ = binary.Write(&sb, binary.LittleEndian, uint16(len(mb)))
	sb.Write(mb)
	_ = binary.Write(&sb, binary.LittleEndian, uint32(len(ts)))
	_ = binary.Write(&sb, binary.LittleEndian, uint64(firstTS))
	_ = binary.Write(&sb, binary.LittleEndian, firstValBits)
	_ = binary.Write(&sb, binary.LittleEndian, tsBitLen)
	sb.Write(tsBits)
	_ = binary.Write(&sb, binary.LittleEndian, valBitLen)
	sb.Write(valBits)
	return sb.String()
}

// buildBlock wraps one or more series bodies in a GORILLA1 block.
func buildBlock(t *testing.T, bodies ...[]byte) []byte {
	t.Helper()
	var blk bytes.Buffer
	blk.WriteString(gorillaMagic)
	blk.WriteByte(byte(gorillaVersion))
	_ = binary.Write(&blk, binary.LittleEndian, uint32(len(bodies)))
	for _, body := range bodies {
		blk.Write(body)
	}
	return blk.Bytes()
}
