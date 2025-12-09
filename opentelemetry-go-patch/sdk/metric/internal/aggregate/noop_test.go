// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package aggregate

import (
	"flag"
	"fmt"
	"math/rand"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

var (
	noopThroughputSeries = flag.Int(
		"noop.test.throughput.series",
		64,
		"Number of unique attribute sets for noop throughput simulation",
	)
	noopThroughputScrapes = flag.Int(
		"noop.test.throughput.scrapes",
		200,
		"Number of scrape loops used in noop throughput simulation",
	)
	noopLatencyMeasurements = flag.Int(
		"noop.test.latency.measurements",
		10000,
		"Number of noop measurements for latency sampling",
	)
)

func TestNoopDelta(t *testing.T) {
	ctx := t.Context()
	meas, comp := Builder[float64]{
		Temporality:      metricdata.DeltaTemporality,
		Filter:           attrFltr,
		AggregationLimit: 3,
	}.Noop()

	got := new(metricdata.Aggregation)
	require.Equal(t, 0, comp(got))

	record(meas, []arg[float64]{
		{ctx, 2, alice},
		{ctx, 10, bob},
		{ctx, 2, alice},
		{ctx, 2, alice},
		{ctx, 10, bob},
	})
	require.Equal(t, 2, comp(got))
	agg := (*got).(metricdata.Sum[float64])
	require.Equal(t, metricdata.DeltaTemporality, agg.Temporality)
	findSumDataPoint(t, agg.DataPoints, fltrAlice)
	findSumDataPoint(t, agg.DataPoints, fltrBob)

	record(meas, []arg[float64]{
		{ctx, 1, alice},
		{ctx, 1, bob},
		{ctx, 1, carol},
		{ctx, 1, dave},
	})
	require.Equal(t, 3, comp(got))
	agg = (*got).(metricdata.Sum[float64])
	findSumDataPoint(t, agg.DataPoints, fltrAlice)
	findSumDataPoint(t, agg.DataPoints, fltrBob)
	findSumDataPoint(t, agg.DataPoints, overflowSet)

	require.Equal(t, 0, comp(got))
}

func TestNoopCumulative(t *testing.T) {
	ctx := t.Context()
	meas, comp := Builder[int64]{
		Temporality:      metricdata.CumulativeTemporality,
		Filter:           attrFltr,
		AggregationLimit: 3,
	}.Noop()

	got := new(metricdata.Aggregation)

	record(meas, []arg[int64]{
		{ctx, 2, alice},
		{ctx, 10, bob},
	})
	require.Equal(t, 2, comp(got))
	agg := (*got).(metricdata.Sum[int64])
	require.Equal(t, metricdata.CumulativeTemporality, agg.Temporality)
	findSumDataPoint(t, agg.DataPoints, fltrAlice)
	findSumDataPoint(t, agg.DataPoints, fltrBob)

	record(meas, []arg[int64]{
		{ctx, 4, alice},
		{ctx, 1, bob},
	})
	require.Equal(t, 2, comp(got))
	agg = (*got).(metricdata.Sum[int64])
	findSumDataPoint(t, agg.DataPoints, fltrAlice)
	findSumDataPoint(t, agg.DataPoints, fltrBob)
}

func TestNoopInsertThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping noop throughput simulation in short mode")
	}

	numSeries := *noopThroughputSeries
	scrapes := *noopThroughputScrapes
	if numSeries <= 0 || scrapes <= 0 {
		t.Fatalf("invalid noop throughput configuration numSeries=%d scrapes=%d", numSeries, scrapes)
	}

	ctx := t.Context()
	meas, comp := Builder[float64]{
		Temporality:      metricdata.DeltaTemporality,
		Filter:           attrFltr,
		AggregationLimit: numSeries + 8,
	}.Noop()

	attrSets := make([]attribute.Set, numSeries)
	rnd := rand.New(rand.NewSource(42))
	for i := range attrSets {
		attrSets[i] = attribute.NewSet(
			attribute.String("service", fmt.Sprintf("svc-%d", i%8)),
			attribute.String("region", fmt.Sprintf("region-%d", i%4)),
			attribute.String("instance", fmt.Sprintf("instance-%d", i)),
			attribute.String("endpoint", fmt.Sprintf("/api/%d", rnd.Intn(32))),
		)
	}

	totalPoints := numSeries * scrapes
	var startMem runtime.MemStats
	runtime.ReadMemStats(&startMem)
	cpuStart := sampleProcessCPUSeconds()
	start := time.Now()

	for i := 0; i < scrapes; i++ {
		for j := 0; j < numSeries; j++ {
			value := float64((i*97 + j) % 4096)
			meas(ctx, value, attrSets[j])
		}
	}
	duration := time.Since(start)

	cpuSeconds := sampleProcessCPUSeconds() - cpuStart
	var endMem runtime.MemStats
	runtime.ReadMemStats(&endMem)

	throughput := float64(totalPoints) / duration.Seconds()
	avgLatency := duration / time.Duration(totalPoints)
	heapDelta := int64(endMem.Alloc) - int64(startMem.Alloc)
	objDelta := int64(endMem.HeapObjects) - int64(startMem.HeapObjects)

	t.Logf(
		"noop series=%d scrapes=%d points=%d duration=%s throughput=%.2f samples/s avg-latency=%s cpu=%.4fs heap-delta=%dB heap-objects=%d",
		numSeries,
		scrapes,
		totalPoints,
		duration,
		throughput,
		avgLatency,
		cpuSeconds,
		heapDelta,
		objDelta,
	)

	got := new(metricdata.Aggregation)
	comp(got)
}

func TestNoopLatencyPerMeasurement(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping noop latency sampling in short mode")
	}

	count := *noopLatencyMeasurements
	if count <= 0 {
		t.Fatalf("invalid noop latency measurement count %d", count)
	}

	ctx := t.Context()
	meas, _ := Builder[float64]{
		Temporality:      metricdata.DeltaTemporality,
		Filter:           attrFltr,
		AggregationLimit: 4,
	}.Noop()

	attrs := attribute.NewSet(
		attribute.String("service", "latency-analysis"),
		attribute.String("region", "test"),
		attribute.String("instance", "latency-node"),
	)

	var startMem runtime.MemStats
	runtime.ReadMemStats(&startMem)
	cpuStart := sampleProcessCPUSeconds()
	start := time.Now()

	for i := 0; i < count; i++ {
		meas(ctx, float64(i%2048), attrs)
	}

	total := time.Since(start)
	cpuSeconds := sampleProcessCPUSeconds() - cpuStart
	var endMem runtime.MemStats
	runtime.ReadMemStats(&endMem)

	avgLatency := total / time.Duration(count)
	throughput := float64(count) / total.Seconds()
	memPerRecord := float64(endMem.TotalAlloc-startMem.TotalAlloc) / float64(count)

	t.Logf(
		"noop records=%d duration=%s avg-latency=%s throughput=%.2f records/s cpu=%.4fs mem/record=%.2fB",
		count,
		total,
		avgLatency,
		throughput,
		cpuSeconds,
		memPerRecord,
	)
}

func findSumDataPoint[N int64 | float64](
	t *testing.T,
	dps []metricdata.DataPoint[N],
	attrs attribute.Set,
) metricdata.DataPoint[N] {
	t.Helper()
	for _, dp := range dps {
		if dp.Attributes.Equals(&attrs) {
			return dp
		}
	}
	t.Fatalf("did not find datapoint for %v", attrs)
	return metricdata.DataPoint[N]{}
}
