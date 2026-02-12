// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package gorillaprocessor

import (
	"math"
	"sort"
)

// point holds a single data point's timestamp and value.
type point struct {
	ts int64   // UnixNano timestamp
	v  float64 // double value
}

// seriesKey uniquely identifies a time series within the processor.
type seriesKey struct {
	metricName    string
	attributesKey string // canonical sorted "k1=v1;k2=v2;" string
}

// seriesMeta holds JSON-serializable metadata for a compressed series chunk.
type seriesMeta struct {
	MetricName string            `json:"metric_name"`
	Attributes map[string]string `json:"attributes"`
	StartTS    int64             `json:"start_ts"`
	EndTS      int64             `json:"end_ts"`
	PointCount int               `json:"point_count"`
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
		e.prevTS = ts
		e.prevDelta = 0
		e.firstSet = true
		return
	}
	delta := ts - e.prevTS
	dd := delta - e.prevDelta
	switch {
	case dd == 0:
		e.bw.writeBit(0)
	case fitsInSignedBits(dd, 7):
		e.bw.writeBits(0b10, 2)
		e.bw.writeBits(uint64(uint64(dd)&((1<<7)-1)), 7)
	case fitsInSignedBits(dd, 9):
		e.bw.writeBits(0b110, 3)
		e.bw.writeBits(uint64(uint64(dd)&((1<<9)-1)), 9)
	case fitsInSignedBits(dd, 12):
		e.bw.writeBits(0b1110, 4)
		e.bw.writeBits(uint64(uint64(dd)&((1<<12)-1)), 12)
	default:
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
		e.prev = vb
		e.prevSet = true
		e.leadingZeros = 0
		e.trailingZeros = 0
		e.havePrevWindow = false
		return
	}
	x := e.prev ^ vb
	if x == 0 {
		e.bw.writeBit(0)
		e.prev = vb
		return
	}
	e.bw.writeBit(1)

	lz := leadingZeros64(x)
	tz := trailingZeros64(x)
	sig := 64 - lz - tz

	if e.havePrevWindow && lz >= e.leadingZeros && tz >= e.trailingZeros {
		e.bw.writeBit(0)
		e.bw.writeBits(x>>uint(e.trailingZeros), uint8(64-int(e.leadingZeros)-int(e.trailingZeros)))
	} else {
		e.bw.writeBit(1)
		lz5 := lz
		if lz5 > 31 {
			lz5 = 31
		}
		e.bw.writeBits(uint64(lz5), 5)
		if sig == 0 {
			sig = 64
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

// sortAndEncode sorts points by timestamp and encodes them using Gorilla compression.
func sortAndEncode(points []point) (firstTS int64, firstValBits uint64, tsBits []byte, tsBitsLen uint32, valBits []byte, valBitsLen uint32) {
	if len(points) == 0 {
		return 0, 0, nil, 0, nil, 0
	}
	sort.Slice(points, func(i, j int) bool { return points[i].ts < points[j].ts })

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
