// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sketchcountminprocessor

import (
	"encoding/binary"
	"io"

	"github.com/cespare/xxhash/v2"
)

func deriveSeed(base uint64, parts ...string) uint64 {
	h := xxhash.New()
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], base)
	h.Write(buf[:])
	for _, part := range parts {
		io.WriteString(h, part)
	}
	return h.Sum64()
}
