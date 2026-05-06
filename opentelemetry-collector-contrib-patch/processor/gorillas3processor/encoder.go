// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package gorillas3processor

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"sort"
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

// gorillaTimestampEncoder implements delta-of-delta with Gorilla
// 0 / 10+7 / 110+9 / 1110+12 / 1111+64 buckets.
type gorillaTimestampEncoder struct {
	bw        *bitWriter
	prevTS    int64
	prevDelta int64
	firstSet  bool
}

func newTsEncoder() *gorillaTimestampEncoder { return &gorillaTimestampEncoder{bw: newBitWriter()} }

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

func (e *gorillaTimestampEncoder) bytes() ([]byte, uint32) {
	b := e.bw.bytes()
	return b, uint32(len(b) * 8)
}

// gorillaValueEncoder is the Gorilla XOR float64 encoder.
type gorillaValueEncoder struct {
	bw             *bitWriter
	prev           uint64
	prevSet        bool
	leadingZeros   uint8
	trailingZeros  uint8
	havePrevWindow bool
}

func newValEncoder() *gorillaValueEncoder { return &gorillaValueEncoder{bw: newBitWriter()} }

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

// sortAndEncode sorts the points by timestamp (in place) and encodes them.
// Returns the first ts/value (raw) plus the bit-packed delta-of-delta and
// XOR streams.
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
	firstTS, firstValBits, tsBits, tsBitsLen, valBits, valBitsLen := sortAndEncode(buf.points)
	startTS := buf.points[0].ts
	endTS := buf.points[len(buf.points)-1].ts
	meta := seriesMeta{
		MetricName: key.metricName,
		Attributes: buf.attributes,
		StartTS:    startTS,
		EndTS:      endTS,
		PointCount: len(buf.points),
	}
	mb, err := json.Marshal(meta)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("gorillas3: marshal metadata: %w", err)
	}
	if len(mb) > math.MaxUint16 {
		return nil, 0, 0, fmt.Errorf("gorillas3: metadata too large for series %s", key.metricName)
	}
	var sb bytes.Buffer
	_ = binary.Write(&sb, binary.LittleEndian, uint16(len(mb)))
	sb.Write(mb)
	_ = binary.Write(&sb, binary.LittleEndian, uint32(len(buf.points)))
	_ = binary.Write(&sb, binary.LittleEndian, uint64(firstTS))
	_ = binary.Write(&sb, binary.LittleEndian, firstValBits)
	_ = binary.Write(&sb, binary.LittleEndian, tsBitsLen)
	sb.Write(tsBits)
	_ = binary.Write(&sb, binary.LittleEndian, valBitsLen)
	sb.Write(valBits)
	return sb.Bytes(), startTS, endTS, nil
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
	// Group keys by metric.
	byMetric := map[string][]seriesKey{}
	for k, buf := range series {
		if buf == nil || len(buf.points) == 0 {
			continue
		}
		byMetric[k.metricName] = append(byMetric[k.metricName], k)
	}
	metricNames := make([]string, 0, len(byMetric))
	for n := range byMetric {
		metricNames = append(metricNames, n)
	}
	sort.Strings(metricNames)

	const headerOverhead = 8 + 1 + 4 // magic + version + seriesCount

	var chunks []chunkInfo
	for _, metric := range metricNames {
		keys := byMetric[metric]
		sort.Slice(keys, func(i, j int) bool { return keys[i].attributesKey < keys[j].attributesKey })

		var (
			cur          bytes.Buffer
			curSeries    int
			curPoints    int
			curRaw       int64
			curStart     int64
			curEnd       int64
			curStartInit bool
			curSize      int64
		)
		writeHeader := func() {
			cur.Reset()
			cur.WriteString(chunkMagic)
			cur.WriteByte(chunkVersion)
			_ = binary.Write(&cur, binary.LittleEndian, uint32(0))
			curSeries = 0
			curPoints = 0
			curRaw = 0
			curStart = 0
			curEnd = 0
			curStartInit = false
			curSize = headerOverhead
		}
		flush := func() {
			if curSeries == 0 {
				return
			}
			buf := cur.Bytes()
			binary.LittleEndian.PutUint32(buf[5:9], uint32(curSeries))
			data := make([]byte, len(buf))
			copy(data, buf)
			chunks = append(chunks, chunkInfo{
				metricName:  metric,
				startTS:     curStart,
				endTS:       curEnd,
				seriesCount: curSeries,
				pointCount:  curPoints,
				rawBytes:    curRaw,
				data:        data,
			})
			writeHeader()
		}

		writeHeader()
		for _, key := range keys {
			body, startTS, endTS, err := encodeSeriesBody(key, series[key])
			if err != nil {
				return nil, err
			}
			if maxObjectBytes > 0 && curSeries > 0 && curSize+int64(len(body)) > maxObjectBytes {
				flush()
			}
			cur.Write(body)
			curSeries++
			curPoints += series[key].PointCountSafe()
			curRaw += int64(series[key].PointCountSafe()) * 16
			if !curStartInit || startTS < curStart {
				curStart = startTS
				curStartInit = true
			}
			if endTS > curEnd {
				curEnd = endTS
			}
			curSize += int64(len(body))
			if maxObjectBytes > 0 && curSize >= maxObjectBytes {
				flush()
			}
		}
		flush()
	}
	return chunks, nil
}

// PointCountSafe returns the number of points in the buffer, guarding
// against nil buffers.
func (b *seriesBuffer) PointCountSafe() int {
	if b == nil {
		return 0
	}
	return len(b.points)
}
