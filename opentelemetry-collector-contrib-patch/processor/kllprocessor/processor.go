package kllprocessor

import (
	"context"
	"slices"
	"time"
	"fmt"

	// "github.com/approx-telemetry/sketchlib-go/KLL"
	KLL "github.com/zzylol/go-kll"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type Sketch struct {
	seen []float64 // for debugging, save the seen values
	sketch *KLL.Sketch
}

type KLLSketch struct {
	cfg *Config
	sketches map[string]*Sketch

	logger *zap.Logger
}

func newProcessor(cfg *Config, logger *zap.Logger) *KLLSketch {
	return &KLLSketch{
		cfg: cfg, logger: logger,
		sketches: make(map[string]*Sketch),
	}
}

func (kll *KLLSketch) processMetrics(_ context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	// see here: https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto#L28
	// or: https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/data-model.md
	// for description of how md is structured

	// intake metrics
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			metrics := sms.At(j).Metrics()
			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)

				// gauge is a point in time
				// https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto#L232
				if metric.Type() == pmetric.MetricTypeGauge {
					// https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/data-model.md#gauge
					// according to above, gauge DataPoints should only report a single (most recently sampled) value
					dps := metric.Gauge().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						if kll.cfg.ReadAsInt {
							kll.addPoint(metric.Name(), float64(dps.At(l).IntValue()))
						} else {
							kll.addPoint(metric.Name(), dps.At(l).DoubleValue())
						}
					}
				}
			}
		}
	}

	// TODO: maybe output seen value as log?

	// remove all prev values
	if kll.cfg.DropOriginal {
		md.ResourceMetrics().RemoveIf(func(pmetric.ResourceMetrics) bool { return true });
	}

	// output sketch
 	scope := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	scope.Scope().SetName("otelcol/kllprocessor")

	now := pcommon.NewTimestampFromTime(time.Now())

	for name, sketch := range kll.sketches {
		if sketch.sketch.GetSize() == 0 { continue }
		cdf := sketch.sketch.CDF();

		// add in each quantile
		for q, str := range kll.cfg.suffixes {
			metric := scope.Metrics().AppendEmpty()
			metric.SetName(name + str);

			dp := metric.SetEmptyGauge().DataPoints().AppendEmpty()
			dp.SetTimestamp(now)
			// dp.SetIntValue(int64(sketch.sketch.GetSize()) + 10)
			dp.SetDoubleValue(cdf.Query(q))
			// dp.SetDoubleValue(q)
		}

		if kll.cfg.WriteSeen {
			slices.Sort(sketch.seen)
			metric := scope.Metrics().AppendEmpty()
			metric.SetName(name + "_seen")

			dp := metric.SetEmptyGauge().DataPoints().AppendEmpty()
			dp.SetTimestamp(now)
			// can also do as a slice, but this is a bit easier
			dp.Attributes().PutStr("seen", fmt.Sprintf("%v", sketch.seen))
		}
	}

	return md, nil
}

func (kll *KLLSketch) addPoint(name string, val float64) {
	// add sketch if this is new metric
	sketch, ok := kll.sketches[name]
	if !ok {
		kll.sketches[name] = &Sketch{ sketch: KLL.New(kll.cfg.K), seen: nil }
		sketch = kll.sketches[name]

		if kll.cfg.WriteSeen { sketch.seen = make([]float64, 0) }
	}

	// update backing sketch
	sketch.sketch.Update(val)
	if kll.cfg.WriteSeen { sketch.seen = append(sketch.seen, val) }
}

