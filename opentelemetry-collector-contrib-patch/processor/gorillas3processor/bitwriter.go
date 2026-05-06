// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package gorillas3processor

import (
	"bytes"
	"math/bits"
)

// bitWriter packs bits MSB-first into a byte buffer. Mirrors the
// telegraf-side encoder's bit layout so chunks are byte-compatible
// across both writers.
type bitWriter struct {
	buf     bytes.Buffer
	curByte byte
	nbits   uint8 // number of bits filled in curByte (0..7)
}

func newBitWriter() *bitWriter { return &bitWriter{} }

func (w *bitWriter) writeBit(bit uint8) {
	if bit != 0 {
		w.curByte |= 1 << (7 - w.nbits)
	}
	w.nbits++
	if w.nbits == 8 {
		w.buf.WriteByte(w.curByte)
		w.curByte = 0
		w.nbits = 0
	}
}

// writeBits writes the lower n bits of v (n in 1..64), MSB-first.
func (w *bitWriter) writeBits(v uint64, n uint8) {
	for i := int(n) - 1; i >= 0; i-- {
		bit := uint8((v >> uint(i)) & 1)
		w.writeBit(bit)
	}
}

// writeByteAlign pads the current byte with zeros so the next write
// starts on a byte boundary.
func (w *bitWriter) writeByteAlign() {
	if w.nbits == 0 {
		return
	}
	for w.nbits != 0 {
		w.writeBit(0)
	}
}

func (w *bitWriter) bytes() []byte {
	w.writeByteAlign()
	return w.buf.Bytes()
}

func leadingZeros64(x uint64) uint8 {
	if x == 0 {
		return 64
	}
	return uint8(bits.LeadingZeros64(x))
}

func trailingZeros64(x uint64) uint8 {
	if x == 0 {
		return 64
	}
	return uint8(bits.TrailingZeros64(x))
}

func fitsInSignedBits(v int64, n uint8) bool {
	if n == 0 || n >= 64 {
		return true
	}
	min := -(int64(1) << (n - 1))
	max := (int64(1) << (n - 1)) - 1
	return v >= min && v <= max
}
