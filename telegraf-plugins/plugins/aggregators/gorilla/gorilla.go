//go:generate ../../../tools/readme_config_includer/generator
package gorilla

import (
	"bytes"
	_ "embed"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/plugins/aggregators"
)

//go:embed sample.conf
var sampleConfig string

type Gorilla struct {
	Measurement    string          `toml:"measurement"`
	BlockInterval  config.Duration `toml:"block_interval"`
	MaxObjectBytes int64           `toml:"max_object_bytes"`

	Log telegraf.Logger `toml:"-"`

	series     map[seriesKey]*seriesBuffer
	blockStart time.Time
	blockEnd   time.Time
}

type seriesBuffer struct {
	tags   map[string]string
	points []point
}

type gorillaChunk struct {
	buf      []byte
	points   int
	rawBytes int64
}

type gorillaObject struct {
	data        []byte
	seriesCount int
	points      int
	rawBytes    int64
}

func (*Gorilla) SampleConfig() string {
	return sampleConfig
}

func (g *Gorilla) Init() error {
	if g.Measurement == "" {
		g.Measurement = "gorilla_block"
	}
	if time.Duration(g.BlockInterval) <= 0 {
		g.BlockInterval = config.Duration(10 * time.Minute)
	}
	if g.series == nil {
		g.series = make(map[seriesKey]*seriesBuffer)
	}
	return nil
}

func (g *Gorilla) Add(m telegraf.Metric) {
	if len(m.FieldList()) == 0 {
		return
	}
	if g.series == nil {
		g.series = make(map[seriesKey]*seriesBuffer)
	}
	ts := m.Time()
	if g.blockStart.IsZero() || ts.Before(g.blockStart) {
		g.blockStart = ts
	}
	if ts.After(g.blockEnd) {
		g.blockEnd = ts
	}

	tags := m.Tags()
	tagKey := canonicalizeTags(tags)
	for _, field := range m.FieldList() {
		val, ok := convertNumeric(field.Value)
		if !ok {
			continue
		}
		sk := seriesKey{
			measurement: m.Name(),
			field:       field.Key,
			tagsKey:     tagKey,
		}
		buf, ok := g.series[sk]
		if !ok {
			buf = &seriesBuffer{
				tags:   tags,
				points: make([]point, 0, 128),
			}
			g.series[sk] = buf
		}
		buf.points = append(buf.points, point{ts: ts.UnixNano(), v: val})
	}
}

func (g *Gorilla) Push(acc telegraf.Accumulator) {
	if len(g.series) == 0 {
		return
	}
	objects, err := g.buildObjects()
	if err != nil {
		acc.AddError(err)
		return
	}
	if len(objects) == 0 {
		return
	}

	blockStart := g.blockStart
	blockEnd := g.blockEnd
	now := time.Now()
	if blockStart.IsZero() {
		blockStart = now
	}
	if blockEnd.IsZero() {
		blockEnd = now
	}
	blockInterval := time.Duration(g.BlockInterval)

	for idx, obj := range objects {
		fields := map[string]interface{}{
			"series_count":        int64(obj.seriesCount),
			"point_count":         int64(obj.points),
			"estimated_raw_bytes": obj.rawBytes,
			"compressed_bytes":    int64(len(obj.data)),
		}
		if obj.rawBytes > 0 {
			fields["compression_ratio"] = float64(len(obj.data)) / float64(obj.rawBytes)
		}
		tags := map[string]string{
			"object_index": strconv.Itoa(idx),
			"block_start":  blockStart.UTC().Format(time.RFC3339Nano),
			"block_end":    blockEnd.UTC().Format(time.RFC3339Nano),
		}
		if blockInterval > 0 {
			tags["block_interval"] = blockInterval.String()
		}

		base := metric.New(g.Measurement, tags, fields, blockEnd)
		acc.AddMetric(newGorillaBlockMetric(base, obj.data))
	}
}

func (g *Gorilla) Reset() {
	g.series = make(map[seriesKey]*seriesBuffer)
	g.blockStart = time.Time{}
	g.blockEnd = time.Time{}
}

func (g *Gorilla) buildObjects() ([]gorillaObject, error) {
	keys := make([]seriesKey, 0, len(g.series))
	for k := range g.series {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].measurement != keys[j].measurement {
			return keys[i].measurement < keys[j].measurement
		}
		if keys[i].field != keys[j].field {
			return keys[i].field < keys[j].field
		}
		return keys[i].tagsKey < keys[j].tagsKey
	})

	chunks := make([]gorillaChunk, 0, len(keys))
	for _, key := range keys {
		buf := g.series[key]
		if buf == nil || len(buf.points) == 0 {
			continue
		}
		firstTS, firstValBits, tsBits, tsBitsLen, valBits, valBitsLen := sortAndEncode(buf.points)
		meta := seriesMeta{
			Measurement: key.measurement,
			Field:       key.field,
			Tags:        buf.tags,
			StartTS:     buf.points[0].ts,
			EndTS:       buf.points[len(buf.points)-1].ts,
			PointCount:  len(buf.points),
		}
		mb, err := json.Marshal(meta)
		if err != nil {
			return nil, fmt.Errorf("gorilla aggregator: marshal metadata: %w", err)
		}
		if len(mb) > math.MaxUint16 {
			return nil, fmt.Errorf("gorilla aggregator: metadata too large for series %s %s", key.measurement, key.field)
		}
		var sb bytes.Buffer
		_ = binary.Write(&sb, binary.LittleEndian, uint16(len(mb)))
		sb.Write(mb)
		_ = binary.Write(&sb, binary.LittleEndian, uint32(len(buf.points)))
		_ = binary.Write(&sb, binary.LittleEndian, uint64(firstTS))
		_ = binary.Write(&sb, binary.LittleEndian, uint64(firstValBits))
		_ = binary.Write(&sb, binary.LittleEndian, tsBitsLen)
		sb.Write(tsBits)
		_ = binary.Write(&sb, binary.LittleEndian, valBitsLen)
		sb.Write(valBits)

		chunks = append(chunks, gorillaChunk{
			buf:      sb.Bytes(),
			points:   len(buf.points),
			rawBytes: int64(len(buf.points)) * 16,
		})
	}

	if len(chunks) == 0 {
		return nil, nil
	}
	return assembleObjects(chunks, g.MaxObjectBytes), nil
}

func assembleObjects(chunks []gorillaChunk, maxBytes int64) []gorillaObject {
	if len(chunks) == 0 {
		return nil
	}
	const headerOverhead = 8 + 1 + 4

	var objects []gorillaObject
	var cur bytes.Buffer
	curSeries := 0
	curPoints := 0
	curRaw := int64(0)
	curSize := int64(0)

	writeHeader := func() {
		cur.Reset()
		cur.WriteString("GORILLA1")
		cur.WriteByte(1)
		_ = binary.Write(&cur, binary.LittleEndian, uint32(0))
		curSeries = 0
		curPoints = 0
		curRaw = 0
		curSize = headerOverhead
	}
	writeHeader()

	flush := func() {
		if curSeries == 0 {
			return
		}
		buf := cur.Bytes()
		binary.LittleEndian.PutUint32(buf[9:13], uint32(curSeries))
		data := make([]byte, len(buf))
		copy(data, buf)
		objects = append(objects, gorillaObject{
			data:        data,
			seriesCount: curSeries,
			points:      curPoints,
			rawBytes:    curRaw,
		})
		writeHeader()
	}

	for _, chunk := range chunks {
		if maxBytes > 0 && curSeries > 0 && curSize+int64(len(chunk.buf)) > maxBytes {
			flush()
		}
		cur.Write(chunk.buf)
		curSeries++
		curPoints += chunk.points
		curRaw += chunk.rawBytes
		curSize += int64(len(chunk.buf))

		if maxBytes > 0 && curSeries > 0 && curSize >= maxBytes {
			flush()
		}
	}
	flush()

	return objects
}

func canonicalizeTags(tags map[string]string) string {
	if len(tags) == 0 {
		return ""
	}
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteString("|")
		}
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(tags[k])
	}
	return b.String()
}

func convertNumeric(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int64:
		return float64(val), true
	case int32:
		return float64(val), true
	case int16:
		return float64(val), true
	case int8:
		return float64(val), true
	case int:
		return float64(val), true
	case uint64:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint8:
		return float64(val), true
	case uint:
		return float64(val), true
	default:
		return 0, false
	}
}

func newGorilla() *Gorilla {
	return &Gorilla{
		Measurement:   "gorilla_block",
		BlockInterval: config.Duration(10 * time.Minute),
		series:        make(map[seriesKey]*seriesBuffer),
	}
}

func init() {
	aggregators.Add("gorilla", func() telegraf.Aggregator {
		return newGorilla()
	})
}
