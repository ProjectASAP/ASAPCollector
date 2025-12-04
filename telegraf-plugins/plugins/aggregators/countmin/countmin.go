//go:generate ../../../tools/readme_config_includer/generator
package countmin

import (
	"bytes"
	_ "embed"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"sort"
	"strings"

	"github.com/cespare/xxhash/v2"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/aggregators"
	sketchutil "github.com/influxdata/telegraf/plugins/aggregators/sketchutil"
)

//go:embed sample.conf
var sampleConfig string

const (
	defaultRows    = 3
	defaultColumns = 4096

	countMinMagic   = 0x434d5331 // "CMS1"
	countMinVersion = 1
)

type CountMinSketchAggregator struct {
	Measurement string   `toml:"measurement"`
	TagKeys     []string `toml:"tag_keys"`
	GroupBy     []string `toml:"group_by"`
	Rows        int      `toml:"rows"`
	Columns     int      `toml:"columns"`
	Seed        uint64   `toml:"seed"`

	Log telegraf.Logger `toml:"-"`

	cache       map[string]*aggregate
	groupByKeys map[string]struct{}
}

type aggregate struct {
	measurement string
	groupTags   map[string]string
	sketches    map[string]*countMinSketch
}

func (*CountMinSketchAggregator) SampleConfig() string {
	return sampleConfig
}

func (c *CountMinSketchAggregator) Init() error {
	if c.Measurement == "" {
		c.Measurement = "countmin"
	}
	if c.Rows <= 0 {
		c.Rows = defaultRows
	}
	if c.Columns <= 0 {
		c.Columns = defaultColumns
	}
	if c.Seed == 0 {
		c.Seed = 0x9e3779b185ebca87
	}
	c.groupByKeys = make(map[string]struct{}, len(c.GroupBy))
	for _, key := range c.GroupBy {
		c.groupByKeys[key] = struct{}{}
	}
	if c.cache == nil {
		c.cache = make(map[string]*aggregate)
	}
	return nil
}

/*
PromQL query: Spatial aggregation
sum(rate(http_requests_total[5m])) without (instance, pod)

Example results:
{job="api", method="GET"}        12
{job="api", method="POST"}        3
{job="frontend", method="GET"}    9
*/

func (c *CountMinSketchAggregator) Add(m telegraf.Metric) {
	if c.cache == nil {
		c.cache = make(map[string]*aggregate)
	}

	tags := m.Tags()
	groupTags := make(map[string]string, len(c.groupByKeys))
	if len(c.groupByKeys) > 0 {
		for key := range c.groupByKeys {
			if value, ok := tags[key]; ok {
				groupTags[key] = value
			}
		}
	}

	cacheKey := aggregateKey(m.Name(), groupTags)
	agg, ok := c.cache[cacheKey]
	if !ok {
		agg = &aggregate{
			measurement: m.Name(),
			groupTags:   copyTags(groupTags),
			sketches:    make(map[string]*countMinSketch),
		}
		c.cache[cacheKey] = agg
	}

	keys := c.effectiveTagKeys(tags)
	if len(keys) == 0 {
		return
	}
	valueKey := joinTagValues(tags, keys)

	// Sketches Per Subpopulation; Can be optimized with Hydra sketch later
	// Count-Min Sketch for topk(k, multidimensional metrics e.g., http_requests_total by (tag1, tag2, ...))
	// Example PromQL: topk(3, node_memory_Active_bytes by (instance))
	// Noted that for topk(k, q), "by" clause is inside the function
	for _, tagKey := range keys {
		sk, ok := agg.sketches[tagKey]
		if !ok {
			derived := deriveSeed(c.Seed, agg.measurement, tagKey)
			var err error
			sk, err = newCountMinSketch(c.Rows, c.Columns, derived)
			if err != nil {
				if c.Log != nil {
					c.Log.Errorf("countmin: create sketch for %s/%s failed: %v", agg.measurement, tagKey, err)
				}
				continue
			}
			agg.sketches[tagKey] = sk
		}

		// Assume FieldList only has one field for now; like Prometheus client protocol
		for _, field := range m.FieldList() {
			value, ok := sketchutil.ToFloat(field.Value)
			if !ok {
				continue
			}
			sk.Insert(valueKey, value)
		}

	}
}

func (c *CountMinSketchAggregator) Push(acc telegraf.Accumulator) {
	for _, agg := range c.cache {
		for tagKey, sketch := range agg.sketches {
			payload, err := sketch.MarshalBinary()
			if err != nil {
				acc.AddError(fmt.Errorf("countmin: serialize sketch for %s/%s: %w", agg.measurement, tagKey, err))
				continue
			}
			fields := map[string]interface{}{
				"rows":     int64(sketch.Rows()),
				"columns":  int64(sketch.Columns()),
				"count":    sketch.Total(),
				"countmin": payload,
			}
			tags := copyTags(agg.groupTags)
			tags["source_measurement"] = agg.measurement
			tags["tag_key"] = tagKey
			acc.AddFields(c.Measurement, fields, tags)
		}
	}
}

func (c *CountMinSketchAggregator) Reset() {
	for k := range c.cache {
		delete(c.cache, k)
	}
}

func (c *CountMinSketchAggregator) effectiveTagKeys(all map[string]string) []string {
	if len(c.TagKeys) > 0 {
		return c.TagKeys
	}
	keys := make([]string, 0, len(all))
	for key := range all {
		if _, skip := c.groupByKeys[key]; skip {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

type countMinSketch struct {
	rows   int
	cols   int
	total  float64
	table  []float32
	salts  []uint64
	hashes []hash.Hash64
}

func newCountMinSketch(rows, cols int, seed uint64) (*countMinSketch, error) {
	if rows <= 0 || cols <= 0 {
		return nil, fmt.Errorf("countmin: invalid dimensions rows=%d cols=%d", rows, cols)
	}
	cms := &countMinSketch{
		rows:   rows,
		cols:   cols,
		table:  make([]float32, rows*cols),
		salts:  make([]uint64, rows),
		hashes: make([]hash.Hash64, rows),
	}
	for i := 0; i < rows; i++ {
		cms.salts[i] = mixSeed(seed, uint64(i))
		cms.hashes[i] = xxhash.New()
	}
	return cms, nil
}

func (c *countMinSketch) Insert(key string, weight float64) {
	if weight == 0 {
		return
	}
	for row := 0; row < c.rows; row++ {
		h := c.hashes[row]
		h.Reset()
		var saltBuf [8]byte
		binary.LittleEndian.PutUint64(saltBuf[:], c.salts[row])
		h.Write(saltBuf[:])
		io.WriteString(h, key)
		idx := int(h.Sum64() % uint64(c.cols))
		c.table[row*c.cols+idx] += float32(weight)
	}
	c.total += weight
}

func (c *countMinSketch) Rows() int {
	return c.rows
}

func (c *countMinSketch) Columns() int {
	return c.cols
}

func (c *countMinSketch) Total() float64 {
	return c.total
}

func (c *countMinSketch) MarshalBinary() ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := binary.Write(buf, binary.BigEndian, uint32(countMinMagic)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, uint16(countMinVersion)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, uint16(c.rows)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(c.cols)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, c.total); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, uint16(len(c.salts))); err != nil {
		return nil, err
	}
	for _, salt := range c.salts {
		if err := binary.Write(buf, binary.BigEndian, salt); err != nil {
			return nil, err
		}
	}
	for _, v := range c.table {
		if err := binary.Write(buf, binary.BigEndian, v); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func aggregateKey(measurement string, tags map[string]string) string {
	if len(tags) == 0 {
		return measurement
	}
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(measurement)
	for _, k := range keys {
		b.WriteString(",")
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(tags[k])
	}
	return b.String()
}

func copyTags(tags map[string]string) map[string]string {
	out := make(map[string]string, len(tags))
	for k, v := range tags {
		out[k] = v
	}
	return out
}

func joinTagValues(tags map[string]string, keys []string) string {
	if len(keys) == 0 {
		return ""
	}
	var b strings.Builder
	for i, key := range keys {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(key)
		b.WriteString("=")
		if v, ok := tags[key]; ok {
			b.WriteString(v)
		}
	}
	return b.String()
}

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

func mixSeed(base uint64, row uint64) uint64 {
	const prime uint64 = 0x100000001b3
	value := base ^ (row * prime)
	value ^= value >> 33
	value *= 0xff51afd7ed558ccd
	value ^= value >> 33
	value *= 0xc4ceb9fe1a85ec53
	value ^= value >> 33
	return value
}

func init() {
	aggregators.Add("countmin", func() telegraf.Aggregator {
		return &CountMinSketchAggregator{}
	})
}
