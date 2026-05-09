// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package gorillas3processor

import (
	gorilla "github.com/ProjectASAP/asap-gorilla-go"
)

// chunkMagic is the 8-byte magic that prefixes every encoded chunk.
// // "GORILLA1" matches asap-gorilla Rust crate (#281) + sibling gorillaprocessor for byte-compat
// (Go/Telegraf, Rust asap-gorilla) shared spec.
const chunkMagic = "GORILLA1"

// chunkVersion is the layout version. Bumped on any wire-incompatible
// header / body change.
const chunkVersion = 1

// point is a single timestamp / float64 sample.
type point struct {
	ts int64   // UnixNano timestamp
	v  float64 // double value
}

// seriesKey identifies a unique time series within a window.
type seriesKey struct {
	metricName    string
	attributesKey string // canonical sorted "k1=v1;k2=v2;" string
}

// seriesBuffer accumulates points for one series during a window.
type seriesBuffer struct {
	attributes map[string]string
	points     []point
}

// seriesMeta is the JSON-serialized header that travels with each
// encoded series chunk inside an GORILLA1 block.
type seriesMeta struct {
	MetricName string            `json:"metric_name"`
	Attributes map[string]string `json:"attributes"`
	StartTS    int64             `json:"start_ts"`
	EndTS      int64             `json:"end_ts"`
	PointCount int               `json:"point_count"`
}

// sortAndEncode adapts the processor-local point shape to asap-gorilla-go.
// Returns the first ts/value (raw) plus the bit-packed delta-of-delta and
// XOR streams.
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

// chunkInfo captures metadata about one written chunk for indexing.
type chunkInfo struct {
	metricName  string
	startTS     int64 // UnixNano of earliest point
	endTS       int64 // UnixNano of latest point
	seriesCount int
	pointCount  int
	rawBytes    int64
	data        []byte
}

// encodeSeries encodes one series buffer into the series-body bytes
// (everything after the chunk's outer GORILLA1 header). Body layout:
//
//	uint16 LE  metaLen
//	[metaLen]  meta JSON
//	uint32 LE  pointCount
//	uint64 LE  firstTS
//	uint64 LE  firstValBits
//	uint32 LE  tsBitsLen
//	[ceil(tsBitsLen/8)]  tsBits
//	uint32 LE  valBitsLen
//	[ceil(valBitsLen/8)] valBits
func encodeSeriesBody(key seriesKey, buf *seriesBuffer) ([]byte, int64, int64, error) {
	series := gorilla.Series{
		MetricName:    key.metricName,
		Attributes:    buf.attributes,
		AttributesKey: key.attributesKey,
		Points:        make([]gorilla.Point, len(buf.points)),
	}
	for i, p := range buf.points {
		series.Points[i] = gorilla.Point{TimestampUnixNano: p.ts, Value: p.v}
	}
	return gorilla.EncodeSeriesBody(series)
}

// buildChunks groups series-bodies by metric name, packs them into
// GORILLA1 blocks (one per metric, optionally split by maxObjectBytes).
//
// Outer GORILLA1 block layout:
//
//	[8]   magic            "GORILLA1"
//	[1]   version          chunkVersion
//	[4]   uint32 LE        seriesCount
//
// followed by seriesCount series bodies (encodeSeriesBody output).
func buildChunks(series map[seriesKey]*seriesBuffer, maxObjectBytes int64) ([]chunkInfo, error) {
	input := make([]gorilla.Series, 0, len(series))
	for key, buf := range series {
		if buf == nil || len(buf.points) == 0 {
			continue
		}
		points := make([]gorilla.Point, len(buf.points))
		for i, p := range buf.points {
			points[i] = gorilla.Point{TimestampUnixNano: p.ts, Value: p.v}
		}
		input = append(input, gorilla.Series{
			MetricName:    key.metricName,
			Attributes:    buf.attributes,
			AttributesKey: key.attributesKey,
			Points:        points,
		})
	}
	chunks, err := gorilla.BuildMetricChunks(input, maxObjectBytes)
	if err != nil {
		return nil, err
	}
	out := make([]chunkInfo, len(chunks))
	for i, c := range chunks {
		out[i] = chunkInfo{
			metricName:  c.MetricName,
			startTS:     c.StartTSNano,
			endTS:       c.EndTSNano,
			seriesCount: c.SeriesCount,
			pointCount:  c.PointCount,
			rawBytes:    c.RawBytes,
			data:        c.Data,
		}
	}
	return out, nil
}

// PointCountSafe returns the number of points in the buffer, guarding
// against nil buffers.
func (b *seriesBuffer) PointCountSafe() int {
	if b == nil {
		return 0
	}
	return len(b.points)
}
