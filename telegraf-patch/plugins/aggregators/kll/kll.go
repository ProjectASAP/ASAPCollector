//go:generate ../../../tools/readme_config_includer/generator
package kll

import (
	_ "embed"
	"fmt"
	"slices"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/aggregators"
	"github.com/zzylol/go-kll"
)

type quantile struct {
	seen []float64; // for debugging, save the seen values
	sketch *kll.Sketch;
}

type metric struct {
	name string;
	fields map[string]*quantile;
}

type KLL struct {
	K int `toml:"k"`;
	Quantiles []float64 `toml:"quantiles"`;
	WriteSeen bool `toml:"write_seen"`;

	cache map[uint64]*metric; // state for each metric/field, key is metric.HashID()
	suffixes map[float64]string; // suffix to attach to output quantiles, e.g.. _p50, _p99, ...
}

//go:embed sample.conf
var sampleConfig string
func (*KLL) SampleConfig() string { return sampleConfig; }

func (k *KLL) Init() error {
	if k.K < 2 { return fmt.Errorf("Invalid Argument. k must be >= 2 (k=%d)", k.K); }

	k.cache = make(map[uint64]*metric);
	k.suffixes = make(map[float64]string);
	for _, q := range k.Quantiles {
		if q < 0 || q > 1 { return fmt.Errorf("Invalid Argument. Quantiles must be in [0, 1] (q=%f)", q); }
		k.suffixes[q] = fmt.Sprintf("_p%d", int(q * 100));
	}

	return nil;
}

// for each numeric field in each metric, update the backing KLL sketch
func (k *KLL) Add(in telegraf.Metric) {
	var id uint64 = in.HashID();

	// get saved metric
	m, ok := k.cache[id];
	if !ok {
		k.cache[id] = &metric{name: in.Name(), fields: make(map[string]*quantile)};
		m = k.cache[id];
	}

	// for each field, get associated sketch
	var fields []*telegraf.Field = in.FieldList();
	for _, field := range fields {
		var val float64;

		// conversion referenced from minmax aggregator
		switch field.Value.(type) {
		case float64:
			val = field.Value.(float64);
		case int64:
			val = float64(field.Value.(int64));
		case uint64:
			val = float64(field.Value.(uint64));
		default:
			continue;
		}

		// get sketch
		sketch, ok := m.fields[field.Key];
		if !ok {
			m.fields[field.Key] = &quantile{seen: nil, sketch: kll.New(k.K)};
			if k.WriteSeen { m.fields[field.Key].seen = make([]float64, 0); }

			sketch = m.fields[field.Key];
		}

		if k.WriteSeen { sketch.seen = append(sketch.seen, val); }

		sketch.sketch.Update(val);
	}
}

func (k *KLL) Push(acc telegraf.Accumulator) {
	for _, m := range k.cache {
		out := make(map[string]any);

		fields := m.fields;
		for name, sketch := range fields {
			// get the desired quantile
			cdf := sketch.sketch.CDF();
			for q, str := range k.suffixes { out[name + str] = cdf.Query(q); }

			if k.WriteSeen {
				slices.Sort(sketch.seen)
				out[name + "_seen"] = fmt.Sprintf("%v", sketch.seen);
			}
		}

		acc.AddSummary(m.name + "_KLL", out, nil);
	}
}

func (k *KLL) Reset() {
	for _, m := range k.cache {
		for key := range m.fields {
			if k.WriteSeen { clear(m.fields[key].seen); }
			m.fields[key].sketch = kll.New(k.K);
		}
	}
}

func init() {
	aggregators.Add("kll", func() telegraf.Aggregator { return &KLL{}; });
}
