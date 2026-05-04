// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package hllprocessor

import (
	hll "github.com/ProjectASAP/sketchlib-go/sketches/HLL"
)

// cloneHLL returns a deep copy of h suitable for use as a delta
// snapshot. Test-only helper consumed by delta_transmission_test.go;
// production code constructs sketches via
// asap-precompute-go/sketches.NewHLLWrapper.
func cloneHLL(h *hll.HyperLogLog) *hll.HyperLogLog {
	if h == nil {
		return nil
	}
	data, err := h.SerializeProtoBytes()
	if err != nil {
		return nil
	}
	clone, err := hll.DeserializeHyperLogLogFromProtoBytes(data)
	if err != nil {
		return nil
	}
	return clone
}
