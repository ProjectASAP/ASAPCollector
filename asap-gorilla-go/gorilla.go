// Package gorilla implements the canonical ASAP GORILLA1 block encoder.
//
// Runtime processors should adapt their local metric/window state into this
// package instead of carrying private copies of the Gorilla bit packing logic.
package gorilla

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/bits"
	"sort"
)

const (
	// Magic is the 8-byte magic prefix for ASAP Gorilla blocks.
	Magic = "GORILLA1"
	// Version is the current GORILLA1 layout version.
	Version byte = 1
	// HeaderLen is the byte length of magic + version + series_count.
	HeaderLen = 8 + 1 + 4
)

// Point is a single timestamp/value sample.
type Point struct {
	TimestampUnixNano int64
	Value             float64
}

// Series is one logical time series within a GORILLA1 block.
type Series struct {
	MetricName    string
	Attributes    map[string]string
	AttributesKey string
	Points        []Point
}

// SeriesMeta is the JSON metadata emitted before each encoded series body.
type SeriesMeta struct {
	MetricName string            `json:"metric_name"`
	Attributes map[string]string `json:"attributes"`
	StartTS    int64             `json:"start_ts"`
	EndTS      int64             `json:"end_ts"`
	PointCount int               `json:"point_count"`
}

// EncodedSeries contains the split timestamp/value bit streams for a series.
type EncodedSeries struct {
	FirstTimestampUnixNano int64
	FirstValueBits         uint64
	TimestampBits          []byte
	TimestampBitLen        uint32
	ValueBits              []byte
	ValueBitLen            uint32
}

// Object is a packed GORILLA1 object containing one or more series.
type Object struct {
	Data        []byte
	SeriesCount int
	PointCount  int
	RawBytes    int64
}

// Chunk is a metric-grouped GORILLA1 object plus index hints.
type Chunk struct {
	MetricName  string
	StartTSNano int64
	EndTSNano   int64
	SeriesCount int
	PointCount  int
	RawBytes    int64
	Data        []byte
}

// SortAndEncode sorts points in place by timestamp and returns the encoded
// timestamp/value streams. It preserves the byte layout used by asap-gorilla-rust.
func SortAndEncode(points []Point) (EncodedSeries, bool) {
	if len(points) == 0 {
		return EncodedSeries{}, false
	}
	sort.Slice(points, func(i, j int) bool {
		return points[i].TimestampUnixNano < points[j].TimestampUnixNano
	})

	tsEnc := newTimestampEncoder()
	valEnc := newValueEncoder()
	for _, p := range points {
		tsEnc.push(p.TimestampUnixNano)
		valEnc.push(p.Value)
	}
	tsBits, tsBitsLen := tsEnc.bytes()
	valBits, valBitsLen := valEnc.bytes()

	return EncodedSeries{
		FirstTimestampUnixNano: points[0].TimestampUnixNano,
		FirstValueBits:         math.Float64bits(points[0].Value),
		TimestampBits:          tsBits,
		TimestampBitLen:        tsBitsLen,
		ValueBits:              valBits,
		ValueBitLen:            valBitsLen,
	}, true
}

// EncodeSeriesBody writes one series body, excluding the outer GORILLA1 header.
func EncodeSeriesBody(series Series) ([]byte, int64, int64, error) {
	encoded, ok := SortAndEncode(series.Points)
	if !ok {
		return nil, 0, 0, fmt.Errorf("gorilla: cannot encode empty series %s", series.MetricName)
	}
	startTS := series.Points[0].TimestampUnixNano
	endTS := series.Points[len(series.Points)-1].TimestampUnixNano
	meta := SeriesMeta{
		MetricName: series.MetricName,
		Attributes: series.Attributes,
		StartTS:    startTS,
		EndTS:      endTS,
		PointCount: len(series.Points),
	}
	mb, err := json.Marshal(meta)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("gorilla: marshal metadata: %w", err)
	}
	if len(mb) > math.MaxUint16 {
		return nil, 0, 0, fmt.Errorf("gorilla: metadata too large for series %s", series.MetricName)
	}

	var sb bytes.Buffer
	_ = binary.Write(&sb, binary.LittleEndian, uint16(len(mb)))
	sb.Write(mb)
	_ = binary.Write(&sb, binary.LittleEndian, uint32(len(series.Points)))
	_ = binary.Write(&sb, binary.LittleEndian, uint64(encoded.FirstTimestampUnixNano))
	_ = binary.Write(&sb, binary.LittleEndian, encoded.FirstValueBits)
	_ = binary.Write(&sb, binary.LittleEndian, encoded.TimestampBitLen)
	sb.Write(encoded.TimestampBits)
	_ = binary.Write(&sb, binary.LittleEndian, encoded.ValueBitLen)
	sb.Write(encoded.ValueBits)
	return sb.Bytes(), startTS, endTS, nil
}

// BuildObjects encodes series and packs them into GORILLA1 objects, sorted by
// metric name and attribute key.
func BuildObjects(series []Series, maxObjectBytes int64) ([]Object, error) {
	clean := nonEmptySeries(series)
	sort.Slice(clean, func(i, j int) bool {
		if clean[i].MetricName != clean[j].MetricName {
			return clean[i].MetricName < clean[j].MetricName
		}
		return clean[i].AttributesKey < clean[j].AttributesKey
	})

	bodies := make([]seriesBody, 0, len(clean))
	for _, s := range clean {
		body, _, _, err := EncodeSeriesBody(s)
		if err != nil {
			return nil, err
		}
		bodies = append(bodies, seriesBody{
			metricName: s.MetricName,
			buf:        body,
			points:     len(s.Points),
			rawBytes:   int64(len(s.Points)) * 16,
		})
	}
	return assembleObjects(bodies, maxObjectBytes), nil
}

// BuildMetricChunks groups series by metric name, then packs each metric into
// one or more GORILLA1 chunks with index metadata.
func BuildMetricChunks(series []Series, maxObjectBytes int64) ([]Chunk, error) {
	byMetric := map[string][]Series{}
	for _, s := range nonEmptySeries(series) {
		byMetric[s.MetricName] = append(byMetric[s.MetricName], s)
	}
	metricNames := make([]string, 0, len(byMetric))
	for metricName := range byMetric {
		metricNames = append(metricNames, metricName)
	}
	sort.Strings(metricNames)

	var chunks []Chunk
	for _, metricName := range metricNames {
		seriesForMetric := byMetric[metricName]
		sort.Slice(seriesForMetric, func(i, j int) bool {
			return seriesForMetric[i].AttributesKey < seriesForMetric[j].AttributesKey
		})

		var cur bytes.Buffer
		curSeries := 0
		curPoints := 0
		curRaw := int64(0)
		curStart := int64(0)
		curEnd := int64(0)
		curStartInit := false
		curSize := int64(0)

		writeHeader := func() {
			cur.Reset()
			cur.WriteString(Magic)
			cur.WriteByte(Version)
			_ = binary.Write(&cur, binary.LittleEndian, uint32(0))
			curSeries = 0
			curPoints = 0
			curRaw = 0
			curStart = 0
			curEnd = 0
			curStartInit = false
			curSize = HeaderLen
		}
		flush := func() {
			if curSeries == 0 {
				return
			}
			buf := cur.Bytes()
			binary.LittleEndian.PutUint32(buf[9:13], uint32(curSeries))
			data := make([]byte, len(buf))
			copy(data, buf)
			chunks = append(chunks, Chunk{
				MetricName:  metricName,
				StartTSNano: curStart,
				EndTSNano:   curEnd,
				SeriesCount: curSeries,
				PointCount:  curPoints,
				RawBytes:    curRaw,
				Data:        data,
			})
			writeHeader()
		}

		writeHeader()
		for _, s := range seriesForMetric {
			body, startTS, endTS, err := EncodeSeriesBody(s)
			if err != nil {
				return nil, err
			}
			if maxObjectBytes > 0 && curSeries > 0 && curSize+int64(len(body)) > maxObjectBytes {
				flush()
			}
			cur.Write(body)
			curSeries++
			curPoints += len(s.Points)
			curRaw += int64(len(s.Points)) * 16
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

type seriesBody struct {
	metricName string
	buf        []byte
	points     int
	rawBytes   int64
}

func nonEmptySeries(series []Series) []Series {
	clean := make([]Series, 0, len(series))
	for _, s := range series {
		if len(s.Points) == 0 {
			continue
		}
		clean = append(clean, s)
	}
	return clean
}

func assembleObjects(bodies []seriesBody, maxBytes int64) []Object {
	if len(bodies) == 0 {
		return nil
	}

	var objects []Object
	var cur bytes.Buffer
	curSeries := 0
	curPoints := 0
	curRaw := int64(0)
	curSize := int64(0)

	writeHeader := func() {
		cur.Reset()
		cur.WriteString(Magic)
		cur.WriteByte(Version)
		_ = binary.Write(&cur, binary.LittleEndian, uint32(0))
		curSeries = 0
		curPoints = 0
		curRaw = 0
		curSize = HeaderLen
	}
	flush := func() {
		if curSeries == 0 {
			return
		}
		buf := cur.Bytes()
		binary.LittleEndian.PutUint32(buf[9:13], uint32(curSeries))
		data := make([]byte, len(buf))
		copy(data, buf)
		objects = append(objects, Object{
			Data:        data,
			SeriesCount: curSeries,
			PointCount:  curPoints,
			RawBytes:    curRaw,
		})
		writeHeader()
	}

	writeHeader()
	for _, body := range bodies {
		if maxBytes > 0 && curSeries > 0 && curSize+int64(len(body.buf)) > maxBytes {
			flush()
		}
		cur.Write(body.buf)
		curSeries++
		curPoints += body.points
		curRaw += body.rawBytes
		curSize += int64(len(body.buf))
		if maxBytes > 0 && curSeries > 0 && curSize >= maxBytes {
			flush()
		}
	}
	flush()
	return objects
}

type bitWriter struct {
	buf     bytes.Buffer
	curByte byte
	nbits   uint8
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

func (w *bitWriter) writeBits(v uint64, n uint8) {
	for i := int(n) - 1; i >= 0; i-- {
		w.writeBit(uint8((v >> uint(i)) & 1))
	}
}

func (w *bitWriter) writeByteAlign() {
	for w.nbits != 0 {
		w.writeBit(0)
	}
}

func (w *bitWriter) bytes() []byte {
	w.writeByteAlign()
	return w.buf.Bytes()
}

type timestampEncoder struct {
	bw        *bitWriter
	prevTS    int64
	prevDelta int64
	firstSet  bool
}

func newTimestampEncoder() *timestampEncoder {
	return &timestampEncoder{bw: &bitWriter{}}
}

func (e *timestampEncoder) push(ts int64) {
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

func (e *timestampEncoder) bytes() ([]byte, uint32) {
	b := e.bw.bytes()
	return b, uint32(len(b) * 8)
}

type valueEncoder struct {
	bw             *bitWriter
	prev           uint64
	prevSet        bool
	leadingZeros   uint8
	trailingZeros  uint8
	havePrevWindow bool
}

func newValueEncoder() *valueEncoder {
	return &valueEncoder{bw: &bitWriter{}}
}

func (e *valueEncoder) push(v float64) {
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
		e.bw.writeBits(uint64(uint8(sig-1)), 6)
		e.bw.writeBits(x>>uint(tz), uint8(sig))
		e.leadingZeros = lz
		e.trailingZeros = tz
		e.havePrevWindow = true
	}
	e.prev = vb
}

func (e *valueEncoder) bytes() ([]byte, uint32) {
	b := e.bw.bytes()
	return b, uint32(len(b) * 8)
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
