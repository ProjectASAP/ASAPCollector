// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package gorillaprocessor

import (
	gorilla "github.com/ProjectASAP/asap-gorilla-go"
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

// sortAndEncode adapts the processor-local point shape to asap-gorilla-go.
func sortAndEncode(points []point) (firstTS int64, firstValBits uint64, tsBits []byte, tsBitsLen uint32, valBits []byte, valBitsLen uint32) {
	if len(points) == 0 {
		return 0, 0, nil, 0, nil, 0
	}
	adapted := make([]gorilla.Point, len(points))
	for i, p := range points {
		adapted[i] = gorilla.Point{TimestampUnixNano: p.ts, Value: p.v}
	}
	encoded, _ := gorilla.SortAndEncode(adapted)
	for i, p := range adapted {
		points[i] = point{ts: p.TimestampUnixNano, v: p.Value}
	}
	return encoded.FirstTimestampUnixNano,
		encoded.FirstValueBits,
		encoded.TimestampBits,
		encoded.TimestampBitLen,
		encoded.ValueBits,
		encoded.ValueBitLen
}
