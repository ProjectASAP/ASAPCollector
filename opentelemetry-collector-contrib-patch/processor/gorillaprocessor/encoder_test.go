// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package gorillaprocessor

import (
	"math"
	"math/rand"
	"testing"
	"time"
)

// bitReader is a test-only decoder for verifying encoder output.
type bitReader struct {
	b      []byte
	bitlen uint32
	off    uint32
}

func newBitReader(b []byte, bitlen uint32) *bitReader {
	return &bitReader{b: b, bitlen: bitlen, off: 0}
}

func (r *bitReader) readBit() (uint8, bool) {
	if r.off >= r.bitlen {
		return 0, false
	}
	byteIdx := r.off / 8
	bitIdx := r.off % 8
	bit := (r.b[byteIdx] >> (7 - bitIdx)) & 1
	r.off++
	return bit, true
}

func (r *bitReader) readBits(n uint8) (uint64, bool) {
	if n == 0 {
		return 0, true
	}
	var v uint64
	for i := uint8(0); i < n; i++ {
		bit, ok := r.readBit()
		if !ok {
			return 0, false
		}
		v = (v << 1) | uint64(bit)
	}
	return v, true
}

func signExtend(v uint64, n uint8) int64 {
	if n == 0 || n >= 64 {
		return int64(v)
	}
	shift := 64 - n
	return int64(v<<shift) >> shift
}

func decodeTimestamps(firstTS int64, count int, bits []byte, bitlen uint32) ([]int64, bool) {
	out := make([]int64, count)
	out[0] = firstTS
	prevTS := firstTS
	prevDelta := int64(0)
	r := newBitReader(bits, bitlen)
	for i := 1; i < count; i++ {
		b, ok := r.readBit()
		if !ok {
			return nil, false
		}
		var dd int64
		if b == 0 {
			dd = 0
		} else {
			b2, ok := r.readBit()
			if !ok {
				return nil, false
			}
			if b2 == 0 {
				v, ok := r.readBits(7)
				if !ok {
					return nil, false
				}
				dd = signExtend(v, 7)
			} else {
				b3, ok := r.readBit()
				if !ok {
					return nil, false
				}
				if b3 == 0 {
					v, ok := r.readBits(9)
					if !ok {
						return nil, false
					}
					dd = signExtend(v, 9)
				} else {
					b4, ok := r.readBit()
					if !ok {
						return nil, false
					}
					if b4 == 0 {
						v, ok := r.readBits(12)
						if !ok {
							return nil, false
						}
						dd = signExtend(v, 12)
					} else {
						v, ok := r.readBits(64)
						if !ok {
							return nil, false
						}
						dd = int64(v)
					}
				}
			}
		}
		delta := prevDelta + dd
		ts := prevTS + delta
		out[i] = ts
		prevTS = ts
		prevDelta = delta
	}
	return out, true
}

func decodeValues(firstValBits uint64, count int, bits []byte, bitlen uint32) ([]float64, bool) {
	out := make([]float64, count)
	out[0] = math.Float64frombits(firstValBits)
	prev := firstValBits
	var lz, tz uint8
	haveWindow := false
	r := newBitReader(bits, bitlen)
	for i := 1; i < count; i++ {
		c, ok := r.readBit()
		if !ok {
			return nil, false
		}
		var vb uint64
		if c == 0 {
			vb = prev
		} else {
			c2, ok := r.readBit()
			if !ok {
				return nil, false
			}
			if c2 == 0 {
				if !haveWindow {
					return nil, false
				}
				sigLen := 64 - lz - tz
				sig, ok := r.readBits(uint8(sigLen))
				if !ok {
					return nil, false
				}
				x := sig << tz
				vb = prev ^ x
			} else {
				lz5, ok := r.readBits(5)
				if !ok {
					return nil, false
				}
				lz = uint8(lz5)
				sigm1, ok := r.readBits(6)
				if !ok {
					return nil, false
				}
				sigLen := uint8(sigm1) + 1
				sig, ok := r.readBits(sigLen)
				if !ok {
					return nil, false
				}
				if sigLen == 64 {
					tz = 0
				} else {
					tz = 64 - lz - sigLen
				}
				x := sig << tz
				vb = prev ^ x
				haveWindow = true
			}
		}
		out[i] = math.Float64frombits(vb)
		prev = vb
	}
	return out, true
}

func makePoints(n int, start int64, step int64, f func(i int) float64) []point {
	pts := make([]point, n)
	t := start
	for i := 0; i < n; i++ {
		pts[i] = point{ts: t, v: f(i)}
		t += step
	}
	return pts
}

func TestRoundTripConstant(t *testing.T) {
	n := 200
	pts := makePoints(n, time.Now().UnixNano(), int64(time.Second), func(i int) float64 { return 42 })
	firstTS, firstValBits, tsBits, tsBitsLen, valBits, valBitsLen := sortAndEncode(pts)
	dts, ok := decodeTimestamps(firstTS, n, tsBits, tsBitsLen)
	if !ok {
		t.Fatalf("failed to decode timestamps")
	}
	dvals, ok := decodeValues(firstValBits, n, valBits, valBitsLen)
	if !ok {
		t.Fatalf("failed to decode values")
	}
	for i := 0; i < n; i++ {
		if dts[i] != pts[i].ts {
			t.Fatalf("timestamp mismatch at %d: got %d want %d", i, dts[i], pts[i].ts)
		}
		if math.Float64bits(dvals[i]) != math.Float64bits(pts[i].v) {
			t.Fatalf("value mismatch at %d: got %v want %v", i, dvals[i], pts[i].v)
		}
	}
}

func TestRoundTripLinear(t *testing.T) {
	n := 512
	pts := makePoints(n, 1_700_000_000_000_000_000, int64(250*time.Millisecond), func(i int) float64 { return float64(i) })
	firstTS, firstValBits, tsBits, tsBitsLen, valBits, valBitsLen := sortAndEncode(pts)
	dts, ok := decodeTimestamps(firstTS, n, tsBits, tsBitsLen)
	if !ok {
		t.Fatalf("failed to decode timestamps")
	}
	dvals, ok := decodeValues(firstValBits, n, valBits, valBitsLen)
	if !ok {
		t.Fatalf("failed to decode values")
	}
	for i := 0; i < n; i++ {
		if dts[i] != pts[i].ts || math.Float64bits(dvals[i]) != math.Float64bits(pts[i].v) {
			t.Fatalf("mismatch at %d", i)
		}
	}
}

func TestRoundTripRandom(t *testing.T) {
	n := 1000
	rnd := rand.New(rand.NewSource(1234))
	base := time.Now().UnixNano()
	step := int64(time.Second)
	pts := make([]point, n)
	ts := base
	val := 0.0
	for i := 0; i < n; i++ {
		jitter := rnd.Int63n(int64(5 * time.Millisecond))
		ts += step + jitter
		val += rnd.NormFloat64()*0.1 + 0.5
		pts[i] = point{ts: ts, v: val}
	}
	firstTS, firstValBits, tsBits, tsBitsLen, valBits, valBitsLen := sortAndEncode(pts)
	dts, ok := decodeTimestamps(firstTS, n, tsBits, tsBitsLen)
	if !ok {
		t.Fatalf("failed to decode timestamps")
	}
	dvals, ok := decodeValues(firstValBits, n, valBits, valBitsLen)
	if !ok {
		t.Fatalf("failed to decode values")
	}
	for i := 0; i < n; i++ {
		if dts[i] != pts[i].ts || math.Float64bits(dvals[i]) != math.Float64bits(pts[i].v) {
			t.Fatalf("mismatch at %d", i)
		}
	}
}

func TestEncodeSingleThreadThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping throughput test in short mode")
	}
	const (
		seriesCount     = 1000
		pointsPerSeries = 20000
		minThroughput   = 20000.0
	)

	base := time.Now().UnixNano()
	pts := makePoints(pointsPerSeries, base, int64(200*time.Millisecond), func(i int) float64 {
		return math.Sin(float64(i) * 0.01)
	})

	totalPoints := seriesCount * pointsPerSeries
	start := time.Now()
	for i := 0; i < seriesCount; i++ {
		sortAndEncode(pts)
	}
	elapsed := time.Since(start)
	throughput := float64(totalPoints) / elapsed.Seconds()
	t.Logf("encoded %d points in %s (~%.0f pts/s)", totalPoints, elapsed, throughput)
	if throughput < minThroughput {
		t.Fatalf("encode throughput too low: got %.0f pts/s, want >= %.0f pts/s", throughput, minThroughput)
	}
}
