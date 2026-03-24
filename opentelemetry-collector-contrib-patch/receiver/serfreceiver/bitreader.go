// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package serfreceiver

import "io"

// bitReader reads individual bits from a byte slice, MSB first.
type bitReader struct {
	data   []byte
	pos    int // current bit position (0-indexed)
	length int // total number of valid bits
}

func newBitReader(data []byte, bitLen uint32) *bitReader {
	return &bitReader{data: data, length: int(bitLen)}
}

func (r *bitReader) readBit() (uint8, error) {
	if r.pos >= r.length {
		return 0, io.EOF
	}
	byteIdx := r.pos / 8
	bitIdx := 7 - (r.pos % 8)
	bit := (r.data[byteIdx] >> uint(bitIdx)) & 1
	r.pos++
	return bit, nil
}

func (r *bitReader) readBits(n int) (uint64, error) {
	var result uint64
	for i := 0; i < n; i++ {
		bit, err := r.readBit()
		if err != nil {
			return 0, err
		}
		result = (result << 1) | uint64(bit)
	}
	return result, nil
}
