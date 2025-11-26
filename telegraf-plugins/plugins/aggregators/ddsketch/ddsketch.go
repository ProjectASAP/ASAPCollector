//go:generate ../../../tools/readme_config_includer/generator
package ddsketch

import (
	_ "embed"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/DataDog/sketches-go/ddsketch"
	sketchpb "github.com/DataDog/sketches-go/ddsketch/pb/sketchpb"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/aggregators"
	"google.golang.org/protobuf/proto"
)

//go:embed sample.conf
var sampleConfig string

type DDSketchAggregator struct {
	Measurement string    `toml:"measurement"`
	Accuracy    float64   `toml:"accuracy"`
	Quantiles   []float64 `toml:"quantiles"`

	Log telegraf.Logger `toml:"-"`

	cache map[uint64]*aggregate
}

type aggregate struct {
	name   string
	tags   map[string]string
	fields map[string]*seriesData
}

type zeroPool struct {
	pool sync.Pool
}

func newZeroPool() *zeroPool {
	zp := &zeroPool{}
	zp.pool.New = func() interface{} {
		return &aggregate{
			fields: make(map[string]*seriesData),
		}
	}
	return zp
}

func (p *zeroPool) Get() *aggregate {
	return p.pool.Get().(*aggregate)
}

func (p *zeroPool) Put(v *aggregate) {
	p.pool.Put(v)
}

var aggregatePool = newZeroPool()

type seriesData struct {
	sketch *ddsketch.DDSketch
	count  int64
	sum    float64
	min    float64
	max    float64
}

func (*DDSketchAggregator) SampleConfig() string {
	return sampleConfig
}

func (d *DDSketchAggregator) Init() error {
	if d.Measurement == "" {
		d.Measurement = "ddsketch"
	}
	if d.Accuracy <= 0 {
		d.Accuracy = 0.01
	}
	if len(d.Quantiles) == 0 {
		d.Quantiles = []float64{0.5, 0.9, 0.99}
	}
	sort.Float64s(d.Quantiles)
	d.cache = make(map[uint64]*aggregate)
	return nil
}

func (d *DDSketchAggregator) Add(m telegraf.Metric) {
	if d.cache == nil {
		d.cache = make(map[uint64]*aggregate)
	}
	id := m.HashID()
	agg, ok := d.cache[id]
	if !ok {
		agg = acquireAggregate(m.Name(), m.Tags())
		d.cache[id] = agg
	}

	for _, field := range m.FieldList() {
		value, ok := toFloat(field.Value)
		if !ok {
			continue
		}
		series, ok := agg.fields[field.Key]
		if !ok {
			sk, err := ddsketch.NewDefaultDDSketch(d.Accuracy)
			if err != nil {
				d.Log.Errorf("ddsketch: cannot create sketch: %v", err)
				continue
			}
			series = &seriesData{
				sketch: sk,
				min:    math.Inf(1),
				max:    math.Inf(-1),
			}
			agg.fields[field.Key] = series
		}
		series.sketch.Add(value)
		series.count++
		series.sum += value
		if value < series.min {
			series.min = value
		}
		if value > series.max {
			series.max = value
		}
	}
}

func (d *DDSketchAggregator) Push(acc telegraf.Accumulator) {
	for _, agg := range d.cache {
		for fieldName, data := range agg.fields {
			if data.count == 0 {
				continue
			}
			fields := make(map[string]interface{})
			fields["count"] = data.count
			fields["sum"] = data.sum
			fields["min"] = data.min
			fields["max"] = data.max
			if data.count > 0 {
				fields["mean"] = data.sum / float64(data.count)
			}

			for _, q := range d.Quantiles {
				if q <= 0 || q >= 1 {
					continue
				}
				value, err := data.sketch.GetValueAtQuantile(q)
				if err != nil {
					d.Log.Errorf("ddsketch: quantile %.4f failed: %v", q, err)
					continue
				}
				key := fmt.Sprintf("p%g", q*100)
				fields[key] = value
			}

			if bytes, err := serializeSketch(data.sketch); err != nil {
				d.Log.Errorf("ddsketch: serialize: %v", err)
			} else {
				fields["ddsketch"] = bytes
			}

			tags := copyTags(agg.tags)
			tags["source_measurement"] = agg.name
			tags["field"] = fieldName
			acc.AddFields(d.Measurement, fields, tags)
		}
	}
}

func (d *DDSketchAggregator) Reset() {
	if d.cache == nil {
		d.cache = make(map[uint64]*aggregate)
		return
	}
	for id, agg := range d.cache {
		releaseAggregate(agg)
		delete(d.cache, id)
	}
}

func serializeSketch(sk *ddsketch.DDSketch) ([]byte, error) {
	var protoSketch *sketchpb.DDSketch = sk.ToProto()
	return proto.Marshal(protoSketch)
}

func acquireAggregate(name string, tags map[string]string) *aggregate {
	agg := aggregatePool.Get()
	agg.name = name
	agg.tags = copyTags(tags)
	return agg
}

func releaseAggregate(agg *aggregate) {
	for k := range agg.fields {
		delete(agg.fields, k)
	}
	agg.name = ""
	agg.tags = nil
	aggregatePool.Put(agg)
}

func copyTags(tags map[string]string) map[string]string {
	out := make(map[string]string, len(tags))
	for k, v := range tags {
		out[k] = v
	}
	return out
}

func toFloat(v interface{}) (float64, bool) {
	switch value := v.(type) {
	case float64:
		return value, true
	case float32:
		return float64(value), true
	case int64:
		return float64(value), true
	case int32:
		return float64(value), true
	case int16:
		return float64(value), true
	case int8:
		return float64(value), true
	case int:
		return float64(value), true
	case uint64:
		return float64(value), true
	case uint32:
		return float64(value), true
	case uint16:
		return float64(value), true
	case uint8:
		return float64(value), true
	case uint:
		return float64(value), true
	default:
		return 0, false
	}
}

func newDDSketchAggregator() telegraf.Aggregator {
	return &DDSketchAggregator{}
}

func init() {
	aggregators.Add("ddsketch", func() telegraf.Aggregator {
		return newDDSketchAggregator()
	})
}
