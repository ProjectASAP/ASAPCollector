// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package aggregate // import "go.opentelemetry.io/otel/sdk/metric/internal/aggregate"

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"
)

var (
	bounds   = []float64{1, 5}
	noMinMax = false

	histogramThroughputSeries = flag.Int(
		"histogram.test.throughput.series",
		64,
		"Number of unique attribute sets for histogram throughput simulation",
	)
	histogramThroughputScrapes = flag.Int(
		"histogram.test.throughput.scrapes",
		200,
		"Number of scrape loops for histogram throughput simulation",
	)
	histogramLatencyMeasurements = flag.Int(
		"histogram.test.latency.measurements",
		10000,
		"Number of histogram measurements for latency sampling",
	)
)

func TestHistogram(t *testing.T) {
	c := new(clock)
	t.Cleanup(c.Register())

	t.Run("Int64/Delta/Sum", testDeltaHist[int64](conf[int64]{hPt: hPointSummed[int64]}))
	c.Reset()
	t.Run("Int64/Delta/NoSum", testDeltaHist[int64](conf[int64]{noSum: true, hPt: hPoint[int64]}))
	c.Reset()
	t.Run("Float64/Delta/Sum", testDeltaHist[float64](conf[float64]{hPt: hPointSummed[float64]}))
	c.Reset()
	t.Run("Float64/Delta/NoSum", testDeltaHist[float64](conf[float64]{noSum: true, hPt: hPoint[float64]}))
	c.Reset()

	t.Run("Int64/Cumulative/Sum", testCumulativeHist[int64](conf[int64]{hPt: hPointSummed[int64]}))
	c.Reset()
	t.Run("Int64/Cumulative/NoSum", testCumulativeHist[int64](conf[int64]{noSum: true, hPt: hPoint[int64]}))
	c.Reset()
	t.Run("Float64/Cumulative/Sum", testCumulativeHist[float64](conf[float64]{hPt: hPointSummed[float64]}))
	c.Reset()
	t.Run("Float64/Cumulative/NoSum", testCumulativeHist[float64](conf[float64]{noSum: true, hPt: hPoint[float64]}))
}

type conf[N int64 | float64] struct {
	noSum bool
	hPt   func(attribute.Set, N, uint64, time.Time, time.Time) metricdata.HistogramDataPoint[N]
}

func testDeltaHist[N int64 | float64](c conf[N]) func(t *testing.T) {
	in, out := Builder[N]{
		Temporality:      metricdata.DeltaTemporality,
		Filter:           attrFltr,
		AggregationLimit: 3,
	}.ExplicitBucketHistogram(bounds, noMinMax, c.noSum)
	ctx := context.Background()
	return test[N](in, out, []teststep[N]{
		{
			input: []arg[N]{},
			expect: output{
				n: 0,
				agg: metricdata.Histogram[N]{
					Temporality: metricdata.DeltaTemporality,
					DataPoints:  []metricdata.HistogramDataPoint[N]{},
				},
			},
		},
		{
			input: []arg[N]{
				{ctx, 2, alice},
				{ctx, 10, bob},
				{ctx, 2, alice},
				{ctx, 2, alice},
				{ctx, 10, bob},
			},
			expect: output{
				n: 2,
				agg: metricdata.Histogram[N]{
					Temporality: metricdata.DeltaTemporality,
					DataPoints: []metricdata.HistogramDataPoint[N]{
						c.hPt(fltrAlice, 2, 3, y2kPlus(1), y2kPlus(2)),
						c.hPt(fltrBob, 10, 2, y2kPlus(1), y2kPlus(2)),
					},
				},
			},
		},
		{
			input: []arg[N]{
				{ctx, 10, alice},
				{ctx, 3, bob},
			},
			expect: output{
				n: 2,
				agg: metricdata.Histogram[N]{
					Temporality: metricdata.DeltaTemporality,
					DataPoints: []metricdata.HistogramDataPoint[N]{
						c.hPt(fltrAlice, 10, 1, y2kPlus(2), y2kPlus(3)),
						c.hPt(fltrBob, 3, 1, y2kPlus(2), y2kPlus(3)),
					},
				},
			},
		},
		{
			input: []arg[N]{},
			// Delta histograms are expected to reset.
			expect: output{
				n: 0,
				agg: metricdata.Histogram[N]{
					Temporality: metricdata.DeltaTemporality,
					DataPoints:  []metricdata.HistogramDataPoint[N]{},
				},
			},
		},
		{
			input: []arg[N]{
				{ctx, 1, alice},
				{ctx, 1, bob},
				// These will exceed cardinality limit.
				{ctx, 1, carol},
				{ctx, 1, dave},
			},
			expect: output{
				n: 3,
				agg: metricdata.Histogram[N]{
					Temporality: metricdata.DeltaTemporality,
					DataPoints: []metricdata.HistogramDataPoint[N]{
						c.hPt(fltrAlice, 1, 1, y2kPlus(4), y2kPlus(5)),
						c.hPt(fltrBob, 1, 1, y2kPlus(4), y2kPlus(5)),
						c.hPt(overflowSet, 1, 2, y2kPlus(4), y2kPlus(5)),
					},
				},
			},
		},
	})
}

func testCumulativeHist[N int64 | float64](c conf[N]) func(t *testing.T) {
	in, out := Builder[N]{
		Temporality:      metricdata.CumulativeTemporality,
		Filter:           attrFltr,
		AggregationLimit: 3,
	}.ExplicitBucketHistogram(bounds, noMinMax, c.noSum)
	ctx := context.Background()
	return test[N](in, out, []teststep[N]{
		{
			input: []arg[N]{},
			expect: output{
				n: 0,
				agg: metricdata.Histogram[N]{
					Temporality: metricdata.CumulativeTemporality,
					DataPoints:  []metricdata.HistogramDataPoint[N]{},
				},
			},
		},
		{
			input: []arg[N]{
				{ctx, 2, alice},
				{ctx, 10, bob},
				{ctx, 2, alice},
				{ctx, 2, alice},
				{ctx, 10, bob},
			},
			expect: output{
				n: 2,
				agg: metricdata.Histogram[N]{
					Temporality: metricdata.CumulativeTemporality,
					DataPoints: []metricdata.HistogramDataPoint[N]{
						c.hPt(fltrAlice, 2, 3, y2kPlus(0), y2kPlus(2)),
						c.hPt(fltrBob, 10, 2, y2kPlus(0), y2kPlus(2)),
					},
				},
			},
		},
		{
			input: []arg[N]{
				{ctx, 2, alice},
				{ctx, 10, bob},
			},
			expect: output{
				n: 2,
				agg: metricdata.Histogram[N]{
					Temporality: metricdata.CumulativeTemporality,
					DataPoints: []metricdata.HistogramDataPoint[N]{
						c.hPt(fltrAlice, 2, 4, y2kPlus(0), y2kPlus(3)),
						c.hPt(fltrBob, 10, 3, y2kPlus(0), y2kPlus(3)),
					},
				},
			},
		},
		{
			input: []arg[N]{},
			expect: output{
				n: 2,
				agg: metricdata.Histogram[N]{
					Temporality: metricdata.CumulativeTemporality,
					DataPoints: []metricdata.HistogramDataPoint[N]{
						c.hPt(fltrAlice, 2, 4, y2kPlus(0), y2kPlus(4)),
						c.hPt(fltrBob, 10, 3, y2kPlus(0), y2kPlus(4)),
					},
				},
			},
		},
		{
			input: []arg[N]{
				// These will exceed cardinality limit.
				{ctx, 1, carol},
				{ctx, 1, dave},
			},
			expect: output{
				n: 3,
				agg: metricdata.Histogram[N]{
					Temporality: metricdata.CumulativeTemporality,
					DataPoints: []metricdata.HistogramDataPoint[N]{
						c.hPt(fltrAlice, 2, 4, y2kPlus(0), y2kPlus(5)),
						c.hPt(fltrBob, 10, 3, y2kPlus(0), y2kPlus(5)),
						c.hPt(overflowSet, 1, 2, y2kPlus(0), y2kPlus(5)),
					},
				},
			},
		},
	})
}

func TestHistogramConcurrentSafe(t *testing.T) {
	t.Run("Int64/Delta", testDeltaHistConcurrentSafe[int64]())
	t.Run("Float64/Delta", testDeltaHistConcurrentSafe[float64]())
	t.Run("Int64/Cumulative", testCumulativeHistConcurrentSafe[int64]())
	t.Run("Float64/Cumulative", testCumulativeHistConcurrentSafe[float64]())
}

func validateHistogram[N int64 | float64](t *testing.T, got metricdata.Aggregation) {
	s, ok := got.(metricdata.Histogram[N])
	if !ok {
		t.Fatalf("wrong aggregation type: %+v", got)
	}
	for _, dp := range s.DataPoints {
		assert.False(t,
			dp.Time.Before(dp.StartTime),
			"Timestamp %v must not be before start time %v", dp.Time, dp.StartTime,
		)
		switch dp.Attributes {
		case fltrAlice:
			// alice observations are always a multiple of 2
			assert.Equal(t, int64(0), int64(dp.Sum)%2)
		case fltrBob:
			// bob observations are always a multiple of 3
			assert.Equal(t, int64(0), int64(dp.Sum)%3)
		default:
			t.Fatalf("wrong attributes %+v", dp.Attributes)
		}
		avg := float64(dp.Sum) / float64(dp.Count)
		if minVal, ok := dp.Min.Value(); ok {
			assert.GreaterOrEqual(t, avg, float64(minVal))
		}
		if maxVal, ok := dp.Max.Value(); ok {
			assert.LessOrEqual(t, avg, float64(maxVal))
		}
		var totalCount uint64
		for _, bc := range dp.BucketCounts {
			totalCount += bc
		}
		assert.Equal(t, totalCount, dp.Count)
	}
}

func testDeltaHistConcurrentSafe[N int64 | float64]() func(t *testing.T) {
	in, out := Builder[N]{
		Temporality:      metricdata.DeltaTemporality,
		Filter:           attrFltr,
		AggregationLimit: 3,
	}.ExplicitBucketHistogram(bounds, noMinMax, false)
	return testAggergationConcurrentSafe[N](in, out, validateHistogram[N])
}

func testCumulativeHistConcurrentSafe[N int64 | float64]() func(t *testing.T) {
	in, out := Builder[N]{
		Temporality:      metricdata.CumulativeTemporality,
		Filter:           attrFltr,
		AggregationLimit: 3,
	}.ExplicitBucketHistogram(bounds, noMinMax, false)
	return testAggergationConcurrentSafe[N](in, out, validateHistogram[N])
}

// hPointSummed returns an HistogramDataPoint that started and ended now with
// multi number of measurements values v. It includes a min and max (set to v).
func hPointSummed[N int64 | float64](
	a attribute.Set,
	v N,
	multi uint64,
	start, t time.Time,
) metricdata.HistogramDataPoint[N] {
	idx := sort.SearchFloat64s(bounds, float64(v))
	counts := make([]uint64, len(bounds)+1)
	counts[idx] += multi
	return metricdata.HistogramDataPoint[N]{
		Attributes:   a,
		StartTime:    start,
		Time:         t,
		Count:        multi,
		Bounds:       bounds,
		BucketCounts: counts,
		Min:          metricdata.NewExtrema(v),
		Max:          metricdata.NewExtrema(v),
		Sum:          v * N(multi),
	}
}

// hPoint returns an HistogramDataPoint that started and ended now with multi
// number of measurements values v. It includes a min and max (set to v).
func hPoint[N int64 | float64](
	a attribute.Set,
	v N,
	multi uint64,
	start, t time.Time,
) metricdata.HistogramDataPoint[N] {
	idx := sort.SearchFloat64s(bounds, float64(v))
	counts := make([]uint64, len(bounds)+1)
	counts[idx] += multi
	return metricdata.HistogramDataPoint[N]{
		Attributes:   a,
		StartTime:    start,
		Time:         t,
		Count:        multi,
		Bounds:       bounds,
		BucketCounts: counts,
		Min:          metricdata.NewExtrema(v),
		Max:          metricdata.NewExtrema(v),
	}
}

func TestBucketsBin(t *testing.T) {
	t.Run("Int64", testBucketsBin[int64]())
	t.Run("Float64", testBucketsBin[float64]())
}

func testBucketsBin[N int64 | float64]() func(t *testing.T) {
	return func(t *testing.T) {
		b := newBuckets[N](alice, 3)
		assertB := func(counts []uint64, count uint64, mi, ma N) {
			t.Helper()
			assert.Equal(t, counts, b.counts)
			assert.Equal(t, count, b.count)
			assert.Equal(t, mi, b.min)
			assert.Equal(t, ma, b.max)
		}

		assertB([]uint64{0, 0, 0}, 0, 0, 0)
		b.bin(1)
		b.minMax(2)
		assertB([]uint64{0, 1, 0}, 1, 0, 2)
		b.bin(0)
		b.minMax(-1)
		assertB([]uint64{1, 1, 0}, 2, -1, 2)
	}
}

func TestBucketsSum(t *testing.T) {
	t.Run("Int64", testBucketsSum[int64]())
	t.Run("Float64", testBucketsSum[float64]())
}

func testBucketsSum[N int64 | float64]() func(t *testing.T) {
	return func(t *testing.T) {
		b := newBuckets[N](alice, 3)

		var want N
		assert.Equal(t, want, b.total)

		b.sum(2)
		want = 2
		assert.Equal(t, want, b.total)

		b.sum(-1)
		want = 1
		assert.Equal(t, want, b.total)
	}
}

func TestHistogramImmutableBounds(t *testing.T) {
	b := []float64{0, 1, 2}
	cpB := make([]float64, len(b))
	copy(cpB, b)

	h := newHistogram[int64](b, false, false, 0, dropExemplars[int64])
	require.Equal(t, cpB, h.bounds)

	b[0] = 10
	assert.Equal(t, cpB, h.bounds, "modifying the bounds argument should not change the bounds")

	h.measure(t.Context(), 5, alice, nil)

	var data metricdata.Aggregation = metricdata.Histogram[int64]{}
	h.cumulative(&data)
	hdp := data.(metricdata.Histogram[int64]).DataPoints[0]
	hdp.Bounds[1] = 10
	assert.Equal(t, cpB, h.bounds, "modifying the Aggregation bounds should not change the bounds")
}

func TestCumulativeHistogramImmutableCounts(t *testing.T) {
	h := newHistogram[int64](bounds, noMinMax, false, 0, dropExemplars[int64])
	h.measure(t.Context(), 5, alice, nil)

	var data metricdata.Aggregation = metricdata.Histogram[int64]{}
	h.cumulative(&data)
	hdp := data.(metricdata.Histogram[int64]).DataPoints[0]

	require.Equal(t, hdp.BucketCounts, h.values[alice.Equivalent()].counts)

	cpCounts := make([]uint64, len(hdp.BucketCounts))
	copy(cpCounts, hdp.BucketCounts)
	hdp.BucketCounts[0] = 10
	assert.Equal(
		t,
		cpCounts,
		h.values[alice.Equivalent()].counts,
		"modifying the Aggregator bucket counts should not change the Aggregator",
	)
}

func TestDeltaHistogramReset(t *testing.T) {
	orig := now
	now = func() time.Time { return y2k }
	t.Cleanup(func() { now = orig })

	h := newHistogram[int64](bounds, noMinMax, false, 0, dropExemplars[int64])

	var data metricdata.Aggregation = metricdata.Histogram[int64]{}
	require.Equal(t, 0, h.delta(&data))
	require.Empty(t, data.(metricdata.Histogram[int64]).DataPoints)

	h.measure(t.Context(), 1, alice, nil)

	expect := metricdata.Histogram[int64]{Temporality: metricdata.DeltaTemporality}
	expect.DataPoints = []metricdata.HistogramDataPoint[int64]{hPointSummed[int64](alice, 1, 1, now(), now())}
	h.delta(&data)
	metricdatatest.AssertAggregationsEqual(t, expect, data)

	// The attr set should be forgotten once Aggregations is called.
	expect.DataPoints = nil
	assert.Equal(t, 0, h.delta(&data))
	assert.Empty(t, data.(metricdata.Histogram[int64]).DataPoints)

	// Aggregating another set should not affect the original (alice).
	h.measure(t.Context(), 1, bob, nil)
	expect.DataPoints = []metricdata.HistogramDataPoint[int64]{hPointSummed[int64](bob, 1, 1, now(), now())}
	h.delta(&data)
	metricdatatest.AssertAggregationsEqual(t, expect, data)
}

func BenchmarkHistogram(b *testing.B) {
	b.Run("Int64/Cumulative", benchmarkAggregate(func() (Measure[int64], ComputeAggregation) {
		return Builder[int64]{
			Temporality: metricdata.CumulativeTemporality,
		}.ExplicitBucketHistogram(bounds, noMinMax, false)
	}))
	b.Run("Int64/Delta", benchmarkAggregate(func() (Measure[int64], ComputeAggregation) {
		return Builder[int64]{
			Temporality: metricdata.DeltaTemporality,
		}.ExplicitBucketHistogram(bounds, noMinMax, false)
	}))
	b.Run("Float64/Cumulative", benchmarkAggregate(func() (Measure[float64], ComputeAggregation) {
		return Builder[float64]{
			Temporality: metricdata.CumulativeTemporality,
		}.ExplicitBucketHistogram(bounds, noMinMax, false)
	}))
	b.Run("Float64/Delta", benchmarkAggregate(func() (Measure[float64], ComputeAggregation) {
		return Builder[float64]{
			Temporality: metricdata.DeltaTemporality,
		}.ExplicitBucketHistogram(bounds, noMinMax, false)
	}))
}

func TestHistogramInsertThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping histogram throughput simulation in short mode")
	}

	numSeries := *histogramThroughputSeries
	scrapes := *histogramThroughputScrapes
	if numSeries <= 0 || scrapes <= 0 {
		t.Fatalf("invalid histogram throughput configuration numSeries=%d scrapes=%d", numSeries, scrapes)
	}

	ctx := t.Context()
	meas, comp := Builder[float64]{
		Temporality:      metricdata.DeltaTemporality,
		Filter:           attrFltr,
		AggregationLimit: numSeries + 8,
	}.ExplicitBucketHistogram(bounds, noMinMax, false)

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
	if totalPoints == 0 {
		t.Fatalf("no histogram points to measure numSeries=%d scrapes=%d", numSeries, scrapes)
	}

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
		"histogram series=%d scrapes=%d points=%d duration=%s throughput=%.2f samples/s avg-latency=%s cpu=%.4fs heap-delta=%dB heap-objects=%d",
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

func TestHistogramLatencyPerMeasurement(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping histogram latency sampling in short mode")
	}

	count := *histogramLatencyMeasurements
	if count <= 0 {
		t.Fatalf("invalid histogram latency measurement count %d", count)
	}

	ctx := t.Context()
	meas, _ := Builder[float64]{
		Temporality:      metricdata.DeltaTemporality,
		Filter:           attrFltr,
		AggregationLimit: 4,
	}.ExplicitBucketHistogram(bounds, noMinMax, false)

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
		"histogram records=%d duration=%s avg-latency=%s throughput=%.2f records/s cpu=%.4fs mem/record=%.2fB",
		count,
		total,
		avgLatency,
		throughput,
		cpuSeconds,
		memPerRecord,
	)
}
