// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package serfprocessor

import (
	"math"
	"math/bits"
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

// ----------------------------------------------------------------------------
// Timestamp encoder: Gorilla-style delta-of-delta (unchanged from Gorilla).
// ----------------------------------------------------------------------------

type serfTimestampEncoder struct {
	bw        *bitWriter
	prevTS    int64
	prevDelta int64
	firstSet  bool
}

func newTsEncoder() *serfTimestampEncoder {
	return &serfTimestampEncoder{bw: newBitWriter()}
}

func (e *serfTimestampEncoder) push(ts int64) {
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
		e.bw.writeBits(uint64(dd)&((1<<7)-1), 7)
	case fitsInSignedBits(dd, 9):
		e.bw.writeBits(0b110, 3)
		e.bw.writeBits(uint64(dd)&((1<<9)-1), 9)
	case fitsInSignedBits(dd, 12):
		e.bw.writeBits(0b1110, 4)
		e.bw.writeBits(uint64(dd)&((1<<12)-1), 12)
	default:
		e.bw.writeBits(0b1111, 4)
		e.bw.writeBits(uint64(dd), 64)
	}
	e.prevTS = ts
	e.prevDelta = delta
}

func (e *serfTimestampEncoder) bytes() ([]byte, uint32) {
	b := e.bw.bytes()
	return b, uint32(len(b) * 8)
}

// ----------------------------------------------------------------------------
// Serf XOR value encoder.
//
// Key improvement over Gorilla: before XOR-encoding each value, FindAppLong
// searches the error interval [v-maxDiff, v+maxDiff] for a float64 whose
// IEEE 754 bit representation maximises leading zeros in XOR with the
// previously stored value. This reduces the average XOR bit-width and
// improves compression — at the cost of bounded approximation error.
//
// References:
//   Elf, SerfXOR: "Elf: Erasing-based Lossless Floating-Point Compression for
//   Time Series Data" (SIGMOD 2023).  The leading/trailing round and
//   representation tables are taken verbatim from the reference C++ implementation.
// ----------------------------------------------------------------------------

// Serf leading-zeros lookup tables (64 entries each, matching C++ reference).
var serfLeadingRound = [64]uint8{
	0, 0, 0, 0, 0, 0, 0, 0,
	8, 8, 8, 8, 12, 12, 12, 12,
	16, 16, 18, 18, 20, 20, 22, 22,
	24, 24, 24, 24, 24, 24, 24, 24,
	24, 24, 24, 24, 24, 24, 24, 24,
	24, 24, 24, 24, 24, 24, 24, 24,
	24, 24, 24, 24, 24, 24, 24, 24,
	24, 24, 24, 24, 24, 24, 24, 24,
}

var serfLeadingRepresentation = [64]uint8{
	0, 0, 0, 0, 0, 0, 0, 0,
	1, 1, 1, 1, 2, 2, 2, 2,
	3, 3, 4, 4, 5, 5, 6, 6,
	7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7,
}

// Serf trailing-zeros lookup tables.
var serfTrailingRound = [64]uint8{
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 22, 22,
	22, 22, 22, 22, 28, 28, 28, 28,
	32, 32, 32, 32, 36, 36, 36, 36,
	40, 40, 42, 42, 42, 42, 46, 46,
	46, 46, 46, 46, 46, 46, 46, 46,
	46, 46, 46, 46, 46, 46, 46, 46,
}

var serfTrailingRepresentation = [64]uint8{
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 1, 1,
	1, 1, 1, 1, 2, 2, 2, 2,
	3, 3, 3, 3, 4, 4, 4, 4,
	5, 5, 6, 6, 6, 6, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7,
}

const (
	serfLeadBitsPerValue  = 3 // 8 leading-zero codes (0-7)
	serfTrailBitsPerValue = 3 // 8 trailing-zero codes (0-7)
)

// serfValueEncoder encodes float64 values using Serf XOR with FindAppLong.
type serfValueEncoder struct {
	bw                  *bitWriter
	storedVal           uint64
	firstValBits        uint64 // bit pattern of the first encoded value
	storedLeadingZeros  int
	storedTrailingZeros int
	prevSet             bool
	maxDiff             float64
	adjustDigit         float64 // cast of int64 adjust_digit for arithmetic
}

func newSerfValEncoder(maxDiff float64, adjustDigit int64) *serfValueEncoder {
	return &serfValueEncoder{
		bw: newBitWriter(),
		// initialise storedVal = Float64bits(2.0), matching C++ reference
		storedVal:           math.Float64bits(2.0),
		storedLeadingZeros:  65, // forces case-00 on first non-zero XOR
		storedTrailingZeros: 65,
		maxDiff:             maxDiff,
		adjustDigit:         float64(adjustDigit),
	}
}

func (e *serfValueEncoder) push(v float64) {
	var thisVal uint64
	if !e.prevSet {
		// First value: store directly; no bits written (stored raw in object header).
		e.storedVal = e.findAppLong(v)
		e.firstValBits = e.storedVal
		e.prevSet = true
		return
	}

	storedDouble := math.Float64frombits(e.storedVal)
	if math.Abs(storedDouble-e.adjustDigit-v) > e.maxDiff {
		thisVal = e.findAppLong(v)
	} else {
		// Current value is within tolerance of stored — XOR will be 0.
		thisVal = e.storedVal
	}

	e.compressValue(thisVal)
	e.storedVal = thisVal
}

// findAppLong returns the uint64 bit pattern for a value within
// [v+adjustDigit-maxDiff, v+adjustDigit+maxDiff] that maximises leading zeros
// in XOR with e.storedVal. Falls back to Float64bits(v+adjustDigit) if no
// better approximation is found.
func (e *serfValueEncoder) findAppLong(v float64) uint64 {
	if e.maxDiff == 0 {
		return math.Float64bits(v + e.adjustDigit)
	}
	adjustedV := v + e.adjustDigit
	minVal := adjustedV - e.maxDiff
	maxVal := adjustedV + e.maxDiff

	if minVal >= 0 {
		return findAppLongInner(minVal, maxVal, 0, v, e.storedVal, e.maxDiff, e.adjustDigit)
	} else if maxVal <= 0 {
		return findAppLongInner(-maxVal, -minVal, 0x8000000000000000, v, e.storedVal, e.maxDiff, e.adjustDigit)
	} else if e.storedVal>>63 == 0 {
		// Previous value is positive — search positive half.
		return findAppLongInner(0, maxVal, 0, v, e.storedVal, e.maxDiff, e.adjustDigit)
	}
	// Previous value is negative — search negative half.
	return findAppLongInner(0, -minVal, 0x8000000000000000, v, e.storedVal, e.maxDiff, e.adjustDigit)
}

// findAppLongInner searches for a bit pattern in [minDouble, maxDouble] (unsigned
// absolute magnitude) with sign `signBit` that has maximum common prefix with
// lastLong, while satisfying the error bound for the original value.
func findAppLongInner(
	minDouble, maxDouble float64,
	signBit uint64,
	original float64,
	lastLong uint64,
	maxDiff float64,
	adjustDigit float64,
) uint64 {
	// Clear sign bit for negative-zero.
	minBits := math.Float64bits(minDouble) &^ uint64(1<<63)
	maxBits := math.Float64bits(maxDouble)

	xorMinMax := minBits ^ maxBits
	var leadingCommon int
	if xorMinMax == 0 {
		leadingCommon = 64
	} else {
		leadingCommon = bits.LeadingZeros64(xorMinMax)
	}

	// Number of free bits after the common prefix.
	shift := 64 - leadingCommon
	// frontMask selects the `leadingCommon` most-significant bits.
	var frontMask uint64
	if leadingCommon == 0 {
		frontMask = 0
	} else {
		frontMask = ^uint64(0) << shift
	}

	for shift >= 0 {
		front := frontMask & minBits
		rear := ^frontMask & lastLong

		candidate := rear | front
		if candidate >= minBits && candidate <= maxBits {
			resultLong := candidate ^ signBit
			diff := math.Float64frombits(resultLong) - adjustDigit - original
			if diff >= -maxDiff && diff <= maxDiff {
				return resultLong
			}
		}

		// Try incrementing by 2^shift (next bit flip), guard against overflow.
		if shift < 64 {
			candidate = (candidate + (uint64(1) << shift)) &^ uint64(1<<63)
			if candidate <= maxBits {
				resultLong := candidate ^ signBit
				diff := math.Float64frombits(resultLong) - adjustDigit - original
				if diff >= -maxDiff && diff <= maxDiff {
					return resultLong
				}
			}
		}

		if shift == 0 {
			break
		}
		frontMask >>= 1
		shift--
	}

	// Fallback: encode exact adjusted value.
	return math.Float64bits(original + adjustDigit)
}

// compressValue encodes thisVal using Serf's three-case XOR scheme.
func (e *serfValueEncoder) compressValue(thisVal uint64) {
	xor := e.storedVal ^ thisVal

	if xor == 0 {
		// Case 01: identical value (XOR = 0) → 2 bits.
		e.bw.writeBits(0b01, 2)
		return
	}

	actualLeading := int(leadingZeros64(xor))
	actualTrailing := int(trailingZeros64(xor))
	leadingZeros := int(serfLeadingRound[actualLeading])
	trailingZeros := int(serfTrailingRound[actualTrailing])

	centerBitsStored := 64 - e.storedLeadingZeros - e.storedTrailingZeros
	savingsIfReuse := (leadingZeros - e.storedLeadingZeros) + (trailingZeros - e.storedTrailingZeros)
	reuseOverhead := 1 + serfLeadBitsPerValue + serfTrailBitsPerValue

	if leadingZeros >= e.storedLeadingZeros &&
		trailingZeros >= e.storedTrailingZeros &&
		savingsIfReuse < reuseOverhead {
		// Case 1: reuse stored window — write flag bit 1 + center bits.
		e.bw.writeBit(1)
		e.bw.writeBits(xor>>uint(e.storedTrailingZeros), uint8(centerBitsStored))
	} else {
		// Case 00: new window — write 00 + encoded leading + trailing + center bits.
		e.storedLeadingZeros = leadingZeros
		e.storedTrailingZeros = trailingZeros
		centerBits := 64 - leadingZeros - trailingZeros
		e.bw.writeBits(0b00, 2)
		e.bw.writeBits(uint64(serfLeadingRepresentation[actualLeading]), serfLeadBitsPerValue)
		e.bw.writeBits(uint64(serfTrailingRepresentation[actualTrailing]), serfTrailBitsPerValue)
		e.bw.writeBits(xor>>uint(trailingZeros), uint8(centerBits))
	}
}

func (e *serfValueEncoder) bytes() ([]byte, uint32) {
	b := e.bw.bytes()
	return b, uint32(len(b) * 8)
}

// ----------------------------------------------------------------------------
// sortAndEncode: sort by timestamp then encode both streams.
// ----------------------------------------------------------------------------

func sortAndEncode(
	pts []point,
	maxDiff float64,
	adjustDigit int64,
) (firstTS int64, firstValBits uint64, tsBits []byte, tsBitsLen uint32, valBits []byte, valBitsLen uint32) {
	if len(pts) == 0 {
		return 0, 0, nil, 0, nil, 0
	}
	sort.Slice(pts, func(i, j int) bool { return pts[i].ts < pts[j].ts })

	firstTS = pts[0].ts
	tsEnc := newTsEncoder()
	valEnc := newSerfValEncoder(maxDiff, adjustDigit)

	for _, p := range pts {
		tsEnc.push(p.ts)
		valEnc.push(p.v)
	}

	firstValBits = valEnc.firstValBits

	tsBits, tsBitsLen = tsEnc.bytes()
	valBits, valBitsLen = valEnc.bytes()
	return
}
