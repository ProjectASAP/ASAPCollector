// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package aggregate // import "go.opentelemetry.io/otel/sdk/metric/internal/aggregate"

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type noopSeries[N int64 | float64] struct {
	attrs attribute.Set
}

type noopAggregate[N int64 | float64] struct {
	limit  limiter[noopSeries[N]]
	series map[attribute.Distinct]*noopSeries[N]
	mu     sync.Mutex
	start  time.Time
}

func newNoopAggregate[N int64 | float64](limit int) *noopAggregate[N] {
	return &noopAggregate[N]{
		limit:  newLimiter[noopSeries[N]](limit),
		series: make(map[attribute.Distinct]*noopSeries[N]),
		start:  now(),
	}
}

func (n *noopAggregate[N]) measure(
	_ context.Context,
	_ N,
	fltrAttr attribute.Set,
	_ []attribute.KeyValue,
) {
	n.mu.Lock()
	defer n.mu.Unlock()

	fltrAttr = n.limit.Attributes(fltrAttr, n.series)
	if _, ok := n.series[fltrAttr.Equivalent()]; ok {
		return
	}
	n.series[fltrAttr.Equivalent()] = &noopSeries[N]{attrs: fltrAttr}
}

func (n *noopAggregate[N]) delta(dest *metricdata.Aggregation) int {
	return n.collect(dest, metricdata.DeltaTemporality, true)
}

func (n *noopAggregate[N]) cumulative(dest *metricdata.Aggregation) int {
	return n.collect(dest, metricdata.CumulativeTemporality, false)
}

func (n *noopAggregate[N]) collect(
	dest *metricdata.Aggregation,
	temporality metricdata.Temporality,
	resetSeries bool,
) int {
	t := now()

	sum, _ := (*dest).(metricdata.Sum[N])
	sum.Temporality = temporality
	sum.IsMonotonic = false

	n.mu.Lock()
	defer n.mu.Unlock()

	dps := reset(sum.DataPoints, len(n.series), len(n.series))
	var zero N
	i := 0
	for _, s := range n.series {
		dps[i] = metricdata.DataPoint[N]{
			Attributes: s.attrs,
			StartTime:  n.start,
			Time:       t,
			Value:      zero,
		}
		i++
	}
	dps = dps[:i]
	sum.DataPoints = dps
	*dest = sum

	if resetSeries {
		for k := range n.series {
			delete(n.series, k)
		}
		n.start = t
	}

	return len(dps)
}
