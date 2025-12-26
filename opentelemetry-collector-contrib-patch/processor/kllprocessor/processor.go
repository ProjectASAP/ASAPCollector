package kllprocessor

import (
	"context"
	"slices"
	"time"
	"fmt"
	"sync"

	// "github.com/approx-telemetry/sketchlib-go/KLL"
	KLL "github.com/zzylol/go-kll"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type Sketch struct {
	seen []float64 // for debugging, save the seen values
	sketch *KLL.Sketch

	mu sync.Mutex
}

type KLLSketches struct {
	cfg *Config
	sketches map[string]*Sketch
	mu sync.RWMutex

	logger *zap.Logger
}

func newProcessor(cfg *Config, logger *zap.Logger) *KLLSketches {
	return &KLLSketches{
		cfg: cfg, logger: logger,
		sketches: make(map[string]*Sketch),
	}
}

func (klls *KLLSketches) processMetrics(_ context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
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
						if klls.cfg.ReadAsInt {
							klls.addPoint(metric.Name(), float64(dps.At(l).IntValue()))
						} else {
							klls.addPoint(metric.Name(), dps.At(l).DoubleValue())
						}
					}
				}
			}
		}
	}

	// remove all prev values
	if klls.cfg.DropOriginal {
		md.ResourceMetrics().RemoveIf(func(pmetric.ResourceMetrics) bool { return true });
	}

	// output sketch
 	scope := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	scope.Scope().SetName("otelcol/kllprocessor")
	now := pcommon.NewTimestampFromTime(time.Now())

	type snapshot struct {
		key string
		sketch *Sketch
	}

	// based off countmin sketch impl; make local copy (by reference) to avoid expensive global lock
	items := make([]snapshot, 0, len(klls.sketches))
	klls.mu.RLock()
	for name, sketch := range klls.sketches {
		items = append(items, snapshot{key: name, sketch: sketch})
	}
	klls.mu.RUnlock()

	for _, item := range items {
		name := item.key
		sketch := item.sketch

		sketch.mu.Lock()
		if sketch.sketch.GetSize() == 0 {
			sketch.mu.Unlock()
			continue
		}

		cdf := sketch.sketch.CDF();

		// add in each quantile
		for q, str := range klls.cfg.suffixes {
			metric := scope.Metrics().AppendEmpty()
			metric.SetName(name + str);

			dp := metric.SetEmptyGauge().DataPoints().AppendEmpty()
			dp.SetTimestamp(now)
			dp.SetDoubleValue(cdf.Query(q))
		}

		if klls.cfg.WriteSeen {
			slices.Sort(sketch.seen)
			metric := scope.Metrics().AppendEmpty()
			metric.SetName(name + "_seen")

			dp := metric.SetEmptyGauge().DataPoints().AppendEmpty()
			dp.SetTimestamp(now)
			// can also do as a slice, but this is a bit easier
			dp.Attributes().PutStr("seen", fmt.Sprintf("%v", sketch.seen))
		}

		sketch.mu.Unlock()
	}

	return md, nil
}

func (klls *KLLSketches) addPoint(name string, val float64) {
	klls.mu.RLock()
	sketch, ok := klls.sketches[name]
	klls.mu.RUnlock()

	if !ok { // add sketch if this is new metric
		klls.mu.Lock()

		klls.sketches[name] = &Sketch{ sketch: KLL.New(klls.cfg.K), seen: nil }
		sketch = klls.sketches[name]

		if klls.cfg.WriteSeen { sketch.seen = make([]float64, 0) }

		klls.mu.Unlock()
	}

	sketch.mu.Lock()
	// update backing sketch
	sketch.sketch.Update(val)
	if klls.cfg.WriteSeen { sketch.seen = append(sketch.seen, val) }
	sketch.mu.Unlock()
}

