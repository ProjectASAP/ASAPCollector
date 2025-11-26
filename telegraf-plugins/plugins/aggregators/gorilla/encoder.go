package gorilla

import (
	"math"
	"sort"
)

type point struct {
	ts int64
	v  float64
}

type seriesKey struct {
	measurement string
	field       string
	// tagsKey is a canonicalized, sorted tag string: k1=v1|k2=v2
	tagsKey string
}

type seriesMeta struct {
	Measurement string            `json:"measurement"`
	Field       string            `json:"field"`
	Tags        map[string]string `json:"tags"`
	StartTS     int64             `json:"start_ts"`
	EndTS       int64             `json:"end_ts"`
	PointCount  int               `json:"point_count"`
}

// gorillaTimestampEncoder encodes timestamps using delta-of-delta with Gorilla-style buckets.
type gorillaTimestampEncoder struct {
	bw        *bitWriter
	prevTS    int64
	prevDelta int64
	firstSet  bool
}

func newTsEncoder() *gorillaTimestampEncoder {
	return &gorillaTimestampEncoder{bw: newBitWriter()}
}

func (e *gorillaTimestampEncoder) push(ts int64) {
	if !e.firstSet {
		// First timestamp is stored outside the bitstream (in header)
		e.prevTS = ts
		e.prevDelta = 0
		e.firstSet = true
		return
	}
	delta := ts - e.prevTS
	dd := delta - e.prevDelta
	// Gorilla encoding for delta-of-delta
	switch {
	case dd == 0:
		// 0
		e.bw.writeBit(0)
	case fitsInSignedBits(dd, 7):
		// 10 + 7 bits
		e.bw.writeBits(0b10, 2)
		e.bw.writeBits(uint64(uint64(dd)&((1<<7)-1)), 7)
	case fitsInSignedBits(dd, 9):
		// 110 + 9 bits
		e.bw.writeBits(0b110, 3)
		e.bw.writeBits(uint64(uint64(dd)&((1<<9)-1)), 9)
	case fitsInSignedBits(dd, 12):
		// 1110 + 12 bits
		e.bw.writeBits(0b1110, 4)
		e.bw.writeBits(uint64(uint64(dd)&((1<<12)-1)), 12)
	default:
		// 1111 + 64 bits (fallback)
		e.bw.writeBits(0b1111, 4)
		e.bw.writeBits(uint64(dd), 64)
	}
	e.prevTS = ts
	e.prevDelta = delta
}

func (e *gorillaTimestampEncoder) bytes() ([]byte, uint32) {
	b := e.bw.bytes()
	return b, uint32(len(b) * 8)
}

// gorillaValueEncoder encodes float64 values using XOR scheme.
type gorillaValueEncoder struct {
	bw             *bitWriter
	prev           uint64
	prevSet        bool
	leadingZeros   uint8
	trailingZeros  uint8
	havePrevWindow bool
}

func newValEncoder() *gorillaValueEncoder {
	return &gorillaValueEncoder{bw: newBitWriter()}
}

func (e *gorillaValueEncoder) push(v float64) {
	vb := math.Float64bits(v)
	if !e.prevSet {
		// First value stored outside the bitstream
		e.prev = vb
		e.prevSet = true
		e.leadingZeros = 0
		e.trailingZeros = 0
		e.havePrevWindow = false
		return
	}
	x := e.prev ^ vb
	if x == 0 {
		e.bw.writeBit(0) // control bit 0
		e.prev = vb
		return
	}
	e.bw.writeBit(1) // control bit 1

	lz := leadingZeros64(x)
	tz := trailingZeros64(x)
	sig := 64 - lz - tz

	if e.havePrevWindow && lz >= e.leadingZeros && tz >= e.trailingZeros {
		// Use previous window
		e.bw.writeBit(0)
		e.bw.writeBits(x>>uint(e.trailingZeros), uint8(64-int(e.leadingZeros)-int(e.trailingZeros)))
	} else {
		// Define new window
		e.bw.writeBit(1)
		// 5 bits for leading zeros (0..31), clamp at 31
		lz5 := lz
		if lz5 > 31 {
			lz5 = 31
		}
		e.bw.writeBits(uint64(lz5), 5)
		// 6 bits for significant bits length minus 1 (1..64)
		if sig == 0 {
			sig = 64 // degenerate, but write 64 bits
		}
		sig6 := uint8(sig - 1)
		e.bw.writeBits(uint64(sig6), 6)
		e.bw.writeBits(x>>uint(tz), uint8(sig))
		e.leadingZeros = lz
		e.trailingZeros = tz
		e.havePrevWindow = true
	}
	e.prev = vb
}

func (e *gorillaValueEncoder) bytes() ([]byte, uint32) {
	b := e.bw.bytes()
	return b, uint32(len(b) * 8)
}

// sortAndEncode encodes a series of points into container pieces.
func sortAndEncode(points []point) (firstTS int64, firstValBits uint64, tsBits []byte, tsBitsLen uint32, valBits []byte, valBitsLen uint32) {
	if len(points) == 0 {
		return 0, 0, nil, 0, nil, 0
	}
	sort.Slice(points, func(i, j int) bool { return points[i].ts < points[j].ts })
	// First timestamp and value
	firstTS = points[0].ts
	firstValBits = math.Float64bits(points[0].v)

	tsEnc := newTsEncoder()
	valEnc := newValEncoder()

	for _, p := range points {
		tsEnc.push(p.ts)
		valEnc.push(p.v)
	}
	tsBits, tsBitsLen = tsEnc.bytes()
	valBits, valBitsLen = valEnc.bytes()
	return
}
