//go:generate ../../../tools/readme_config_includer/generator
package countmin

import (
	_ "embed"
	"encoding/binary"
	"encoding/json"
	"fmt"
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
)

type CountMinSketchAggregator struct {
	Measurement string   `toml:"measurement"`
	TagKeys     []string `toml:"tag_keys"`
	GroupBy     []string `toml:"group_by"`
	Rows        int      `toml:"rows"`
	Columns     int      `toml:"columns"`
	Seed        uint64   `toml:"seed"`
	TopK        int      `toml:"top_k"`

	Log telegraf.Logger `toml:"-"`

	cache       map[string]*aggregate
	groupByKeys map[string]struct{}
}

type aggregate struct {
	measurement string
	groupTags   map[string]string
	sketches    map[string]*sketchutil.CountMinSketch
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
	if c.TopK < 0 {
		return fmt.Errorf("countmin: top_k must be >= 0")
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
			sketches:    make(map[string]*sketchutil.CountMinSketch),
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
			sk, err = sketchutil.NewCountMinSketch(c.Rows, c.Columns, derived, c.TopK)
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
			if top := sketch.TopKEntries(); len(top) > 0 {
				if encoded, err := json.Marshal(top); err != nil {
					acc.AddError(fmt.Errorf("countmin: serialize topk for %s/%s: %w", agg.measurement, tagKey, err))
				} else {
					fields["topk"] = encoded
				}
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

func init() {
	aggregators.Add("countmin", func() telegraf.Aggregator {
		return &CountMinSketchAggregator{}
	})
}
