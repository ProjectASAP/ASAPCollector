// Package intchunk implements the cold-tier "best-of-N, all-lossless" raw value
// chunk codec from the ASAP holistic-compression design (DESIGN.md §1.2-1.4).
//
// A cold raw chunk encodes one series' (timestamp, float64-value) samples for a
// block. Several lossless codecs compete per chunk; the encoder emits whichever
// produces the fewest bytes:
//
//	GORILLA_XOR          (tag 0) lossless float64 via prometheus/tsdb/chunkenc
//	                             XOR (XOR == Gorilla). Always valid; the
//	                             fallback for true high-precision floats.
//	INT_FOR_DELTA        (tag 1) float->int64 via a decimal scale exponent, then
//	                             frame-of-reference (subtract base), delta,
//	                             fixed-width bit-pack. For gauges.
//	INT_FOR_DOD          (tag 2) same scale+FOR but delta-of-delta, fixed width.
//	                             For monotonic counters and the regularly-spaced
//	                             timestamp column.
//	INT_FOR_DELTA_VARINT (tag 3) FOR+delta with each residual stored as a zigzag
//	                             varint instead of a fixed-width field. Wins on
//	                             skewed residual distributions where a single
//	                             fixed width over-pays the common case.
//	INT_FOR_DOD_VARINT   (tag 4) FOR delta-of-delta with zigzag-varint residuals.
//
// The INT_* codecs come in two body formats over the SAME residual transform:
// fixed-width bit-pack (tags 1/2) and zigzag-varint (tags 3/4). Best-of-N tries
// both and keeps the smaller, so a block whose residuals are nearly uniform
// takes the tight fixed width while a skewed block takes variable-length
// varints. All are bit-exact lossless. The INT_* candidates are only ever produced
// when tryScaleToInt64 proves float->int64->float round-trips BIT-EXACTLY at the
// chosen scale (the lib/decimal precision trap the benchmark exposed); otherwise
// only Gorilla is offered for that block.
//
// Timestamps are always encoded with the design's t0 + delta-of-delta varint
// scheme regardless of value codec, so the chunk is self-describing.
//
// This package depends only on the standard library plus
// prometheus/tsdb/chunkenc for the XOR fallback. It is wired into the edge
// processor's cold path through the coldpart package: when cold.format is set
// to "intchunk", asapedgeprocessor re-encodes each block's drained samples into
// a coldpart.Part (coldpart.WritePart -> intchunk.Encode). This is opt-in; the
// default cold format remains the gorilla-XOR fragment batch.
package intchunk

import (
	"encoding/binary"
	"errors"
	"math/bits"
)

// CodecTag identifies the value codec used for a chunk (DESIGN.md §1.3).
type CodecTag uint8

const (
	// CodecGorillaXOR is lossless float64 via the prometheus XOR (Gorilla)
	// chunk. Always valid; the fallback for true high-precision floats.
	CodecGorillaXOR CodecTag = 0
	// CodecIntForDelta scales floats to int64, subtracts a frame base, then
	// delta + bit-packs the residuals at a single fixed width. For gauges.
	CodecIntForDelta CodecTag = 1
	// CodecIntForDoD is like CodecIntForDelta but on delta-of-delta. For
	// monotonic counters and the timestamp column.
	CodecIntForDoD CodecTag = 2
	// CodecIntForDeltaVarint is the FOR+delta residual transform with each
	// residual stored as a zigzag varint instead of a single fixed-width field.
	// Variable-length residuals avoid paying the block's widest residual on
	// every sample, so this wins on skewed residual distributions (most deltas
	// tiny, a few large) where a fixed width over-pays.
	CodecIntForDeltaVarint CodecTag = 3
	// CodecIntForDoDVarint is the FOR delta-of-delta transform with zigzag
	// varint residuals. For near-linear counters whose dods are mostly zero but
	// occasionally spike.
	CodecIntForDoDVarint CodecTag = 4
)

func (t CodecTag) String() string {
	switch t {
	case CodecGorillaXOR:
		return "GORILLA_XOR"
	case CodecIntForDelta:
		return "INT_FOR_DELTA"
	case CodecIntForDoD:
		return "INT_FOR_DOD"
	case CodecIntForDeltaVarint:
		return "INT_FOR_DELTA_VARINT"
	case CodecIntForDoDVarint:
		return "INT_FOR_DOD_VARINT"
	default:
		return "UNKNOWN"
	}
}

// Sample is one timestamp/value point. Timestamps are arbitrary int64 (the
// design uses block-relative milliseconds; the codec is agnostic to the unit).
type Sample struct {
	T int64
	V float64
}

// maxResidualWidth bounds the per-residual bit width before the encoder cuts
// the chunk and re-bases (DESIGN.md §1.4 / §3 "overflow chunk-cut"). 56 bits
// keeps every residual well inside int64 while still allowing the bit-packer
// to widen to the natural value range; a residual that needs more than this is
// treated as drift and forces a re-base.
const maxResidualWidth = 56

// Errors returned by the package.
var (
	ErrEmpty        = errors.New("intchunk: no samples")
	ErrCorruptChunk = errors.New("intchunk: corrupt chunk")
	ErrBadCodecTag  = errors.New("intchunk: unknown codec tag")
)

// ---------------------------------------------------------------------------
// varint / zigzag helpers
// ---------------------------------------------------------------------------

// zigzag maps a signed integer to an unsigned one so small-magnitude negatives
// stay small under uvarint (the standard protobuf zigzag transform).
func zigzag(v int64) uint64 {
	return uint64((v << 1) ^ (v >> 63))
}

func unzigzag(u uint64) int64 {
	return int64(u>>1) ^ -int64(u&1)
}

type byteWriter struct {
	buf []byte
}

func (w *byteWriter) u8(b byte) { w.buf = append(w.buf, b) }

func (w *byteWriter) uvarint(v uint64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	w.buf = append(w.buf, tmp[:n]...)
}

func (w *byteWriter) varint(v int64) { w.uvarint(zigzag(v)) }

type byteReader struct {
	buf []byte
	pos int
}

func (r *byteReader) u8() (byte, error) {
	if r.pos >= len(r.buf) {
		return 0, ErrCorruptChunk
	}
	b := r.buf[r.pos]
	r.pos++
	return b, nil
}

func (r *byteReader) uvarint() (uint64, error) {
	v, n := binary.Uvarint(r.buf[r.pos:])
	if n <= 0 {
		return 0, ErrCorruptChunk
	}
	r.pos += n
	return v, nil
}

func (r *byteReader) varint() (int64, error) {
	u, err := r.uvarint()
	if err != nil {
		return 0, err
	}
	return unzigzag(u), nil
}

// ---------------------------------------------------------------------------
// bit packer (fixed-width residual stream)
// ---------------------------------------------------------------------------

// bitPackWidth returns the minimum number of bits needed to hold every value in
// us as a fixed-width field (the values are zigzag-encoded residuals, so they
// are already unsigned). A width of 0 means all values are zero.
func bitPackWidth(us []uint64) uint8 {
	var max uint64
	for _, u := range us {
		if u > max {
			max = u
		}
	}
	if max == 0 {
		return 0
	}
	return uint8(bits.Len64(max))
}

// packBits writes len(us) fixed-width fields of `width` bits each, MSB-first,
// byte-aligned at the end.
func packBits(dst []byte, us []uint64, width uint8) []byte {
	if width == 0 {
		return dst
	}
	var cur uint64
	var nbits uint
	for _, u := range us {
		cur = (cur << width) | (u & ((1 << width) - 1))
		nbits += uint(width)
		for nbits >= 8 {
			nbits -= 8
			dst = append(dst, byte(cur>>nbits))
		}
	}
	if nbits > 0 {
		dst = append(dst, byte(cur<<(8-nbits)))
	}
	return dst
}

// unpackBits reads n fixed-width fields of `width` bits each from src.
func unpackBits(src []byte, n int, width uint8) ([]uint64, error) {
	out := make([]uint64, 0, n)
	if width == 0 {
		for i := 0; i < n; i++ {
			out = append(out, 0)
		}
		return out, nil
	}
	mask := uint64(1)<<width - 1
	var cur uint64
	var nbits uint
	pos := 0
	for i := 0; i < n; i++ {
		for nbits < uint(width) {
			if pos >= len(src) {
				return nil, ErrCorruptChunk
			}
			cur = (cur << 8) | uint64(src[pos])
			pos++
			nbits += 8
		}
		nbits -= uint(width)
		out = append(out, (cur>>nbits)&mask)
	}
	return out, nil
}
