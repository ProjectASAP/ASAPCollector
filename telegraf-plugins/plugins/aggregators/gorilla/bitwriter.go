package gorilla

import (
	"bytes"
)

// bitWriter packs bits into a byte buffer.
type bitWriter struct {
	buf     bytes.Buffer
	curByte byte
	nbits   uint8 // number of bits filled in curByte (0..7)
}

func newBitWriter() *bitWriter {
	return &bitWriter{}
}

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

// writeByteAlign pads with zeros to the next byte boundary.
func (w *bitWriter) writeByteAlign() {
	if w.nbits == 0 {
		return
	}
	for w.nbits != 0 {
		w.writeBit(0)
	}
}

func (w *bitWriter) bytes() []byte {
	// Important: create a copy if caller may retain
	w.writeByteAlign()
	return w.buf.Bytes()
}

func leadingZeros64(x uint64) uint8 {
	if x == 0 {
		return 64
	}
	var n uint8
	for i := 63; i >= 0; i-- {
		if (x>>uint(i))&1 == 0 {
			n++
		} else {
			break
		}
	}
	return n
}

func trailingZeros64(x uint64) uint8 {
	if x == 0 {
		return 64
	}
	var n uint8
	for i := 0; i < 64; i++ {
		if (x>>uint(i))&1 == 0 {
			n++
		} else {
			break
		}
	}
	return n
}

func fitsInSignedBits(v int64, n uint8) bool {
	if n == 0 || n >= 64 {
		return true
	}
	min := -(int64(1) << (n - 1))
	max := (int64(1) << (n - 1)) - 1
	return v >= min && v <= max
}
