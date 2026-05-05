// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package aggregate

import (
	"flag"
	"fmt"
	"math/rand"
	"runtime"
	"runtime/metrics"
	"testing"
	"time"

	envpb "github.com/ProjectASAP/sketchlib-go/proto/sketch_envelope"
	ddsketch "github.com/ProjectASAP/sketchlib-go/sketches/DDSketch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"
)

const testDDSketchAccuracy = 0.01

var (
	ddsketchThroughputSeries = flag.Int(
		"ddsketch.test.throughput.series",
		64,
		"Number of unique attribute sets for throughput simulation",
	)
	ddsketchThroughputScrapes = flag.Int(
		"ddsketch.test.throughput.scrapes",
		200,
		"Number of scrape loops used in throughput simulation",
	)
	ddsketchThroughputIntervals = flag.Int(
		"ddsketch.test.throughput.intervals",
		5,
		"Number of consecutive collect intervals for throughput simulation",
	)
	ddsketchLatencyMeasurements = flag.Int(
		"ddsketch.test.latency.measurements",
		10000,
		"Number of individual measurements for latency sampling",
	)
)

func TestDDSketchDelta(t *testing.T) {
	c := new(clock)
	t.Cleanup(c.Register())

	ctx := t.Context()
	meas, comp := Builder[float64]{
		Temporality:      metricdata.DeltaTemporality,
		Filter:           attrFltr,
		AggregationLimit: 3,
	}.DDSketch(testDDSketchAccuracy, false, false, false, 0)

	aliceCheckout := attribute.NewSet(
		userAlice,
		adminTrue,
		attribute.String("service", "checkout"),
		attribute.String("region", "us-east-1"),
		attribute.String("endpoint", "/cart/submit"),
	)
	bobInventory := attribute.NewSet(
		userBob,
		adminFalse,
		attribute.String("service", "inventory"),
		attribute.String("region", "eu-west-1"),
		attribute.String("endpoint", "/stock/update"),
	)

	got := new(metricdata.Aggregation)

	require.Equal(t, 0, comp(got))

	record(meas, []arg[float64]{
		{ctx, 2, aliceCheckout},
		{ctx, 10, bobInventory},
		{ctx, 2, aliceCheckout},
		{ctx, 2, aliceCheckout},
		{ctx, 10, bobInventory},
	})
	require.Equal(t, 2, comp(got))
	agg := (*got).(metricdata.DDSketch[float64])
	require.Equal(t, metricdata.DeltaTemporality, agg.Temporality)
	dpAlice := findDDSketchDP(t, agg.DataPoints, fltrAlice)
	require.Equal(t, uint64(3), dpAlice.Count)
	require.InDelta(t, 6, dpAlice.Sum, 1e-9)
	assertExtremaEqual(t, dpAlice.Min, 2)
	assertExtremaEqual(t, dpAlice.Max, 2)
	require.NotEmpty(t, dpAlice.Sketch)

	dpBob := findDDSketchDP(t, agg.DataPoints, fltrBob)
	require.Equal(t, uint64(2), dpBob.Count)
	require.InDelta(t, 20, dpBob.Sum, 1e-9)
	assertExtremaEqual(t, dpBob.Min, 10)
	assertExtremaEqual(t, dpBob.Max, 10)

	record(meas, []arg[float64]{
		{ctx, 10, alice},
		{ctx, 3, bob},
	})
	require.Equal(t, 2, comp(got))
	agg = (*got).(metricdata.DDSketch[float64])
	dpAlice = findDDSketchDP(t, agg.DataPoints, fltrAlice)
	require.Equal(t, uint64(1), dpAlice.Count)
	require.InDelta(t, 10, dpAlice.Sum, 1e-9)
	assertExtremaEqual(t, dpAlice.Min, 10)
	assertExtremaEqual(t, dpAlice.Max, 10)

	dpBob = findDDSketchDP(t, agg.DataPoints, fltrBob)
	require.Equal(t, uint64(1), dpBob.Count)
	require.InDelta(t, 3, dpBob.Sum, 1e-9)
	assertExtremaEqual(t, dpBob.Min, 3)
	assertExtremaEqual(t, dpBob.Max, 3)

	require.Equal(t, 0, comp(got))

	record(meas, []arg[float64]{
		{ctx, 1, alice},
		{ctx, 1, bob},
		{ctx, 1, carol},
		{ctx, 1, dave},
	})
	require.Equal(t, 3, comp(got))
	agg = (*got).(metricdata.DDSketch[float64])

	findDDSketchDP(t, agg.DataPoints, fltrAlice)
	findDDSketchDP(t, agg.DataPoints, fltrBob)
	dpOverflow := findDDSketchDP(t, agg.DataPoints, overflowSet)
	require.Equal(t, uint64(2), dpOverflow.Count)
	require.InDelta(t, 2, dpOverflow.Sum, 1e-9)
	assertExtremaEqual(t, dpOverflow.Min, 1)
	assertExtremaEqual(t, dpOverflow.Max, 1)
}

func TestDDSketchCumulativeNoMinMax(t *testing.T) {
	c := new(clock)
	t.Cleanup(c.Register())

	ctx := t.Context()
	meas, comp := Builder[int64]{
		Temporality:      metricdata.CumulativeTemporality,
		Filter:           attrFltr,
		AggregationLimit: 3,
	}.DDSketch(testDDSketchAccuracy, true, true, false, 0)

	got := new(metricdata.Aggregation)

	record(meas, []arg[int64]{
		{ctx, 2, alice},
		{ctx, 10, bob},
	})
	require.Equal(t, 2, comp(got))
	agg := (*got).(metricdata.DDSketch[int64])
	require.Equal(t, metricdata.CumulativeTemporality, agg.Temporality)
	dpAlice := findDDSketchDP(t, agg.DataPoints, fltrAlice)
	require.Equal(t, uint64(1), dpAlice.Count)
	require.Zero(t, dpAlice.Sum)
	assertExtremaMissing(t, dpAlice.Min)
	assertExtremaMissing(t, dpAlice.Max)

	dpBob := findDDSketchDP(t, agg.DataPoints, fltrBob)
	require.Equal(t, uint64(1), dpBob.Count)
	require.Zero(t, dpBob.Sum)

	record(meas, []arg[int64]{
		{ctx, 4, alice},
		{ctx, 1, bob},
	})
	require.Equal(t, 2, comp(got))
	agg = (*got).(metricdata.DDSketch[int64])
	dpAlice = findDDSketchDP(t, agg.DataPoints, fltrAlice)
	require.Equal(t, uint64(2), dpAlice.Count)
	require.Zero(t, dpAlice.Sum)

	dpBob = findDDSketchDP(t, agg.DataPoints, fltrBob)
	require.Equal(t, uint64(2), dpBob.Count)
	require.Zero(t, dpBob.Sum)
}

func TestDDSketchAggregationEquality(t *testing.T) {
	dp := metricdata.DDSketchDataPoint[float64]{
		Attributes: attribute.NewSet(attribute.String("key", "value")),
		Count:      1,
		Sum:        1,
		Encoding:   metricdata.DDSketchEncodingProto,
		Sketch:     []byte{0x1, 0x2, 0x3},
	}
	agg := metricdata.DDSketch[float64]{
		Temporality: metricdata.DeltaTemporality,
		DataPoints:  []metricdata.DDSketchDataPoint[float64]{dp},
	}
	metricdatatest.AssertAggregationsEqual(t, agg, agg)
}

func TestDDSketchInsertThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping throughput simulation in short mode")
	}

	numSeries := *ddsketchThroughputSeries
	scrapes := *ddsketchThroughputScrapes
	if numSeries <= 0 || scrapes <= 0 {
		t.Fatalf("invalid configuration numSeries=%d scrapes=%d", numSeries, scrapes)
	}

	ctx := t.Context()
	builder := Builder[float64]{
		Temporality:      metricdata.DeltaTemporality,
		Filter:           attrFltr,
		AggregationLimit: numSeries + 8,
	}
	meas, comp := builder.DDSketch(testDDSketchAccuracy, false, false, false, 0)

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
		t.Fatalf("no points to measure numSeries=%d scrapes=%d", numSeries, scrapes)
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
		"series=%d scrapes=%d points=%d duration=%s throughput=%.2f samples/s avg-latency=%s cpu=%.4fs heap-delta=%dB heap-objects=%d",
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

func TestDDSketchThroughputMultiInterval(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-interval throughput simulation in short mode")
	}

	numSeries := *ddsketchThroughputSeries
	scrapes := *ddsketchThroughputScrapes
	intervals := *ddsketchThroughputIntervals
	if numSeries <= 0 || scrapes <= 0 || intervals <= 0 {
		t.Fatalf("invalid configuration series=%d scrapes=%d intervals=%d", numSeries, scrapes, intervals)
	}

	ctx := t.Context()
	meas, comp := Builder[float64]{
		Temporality:      metricdata.DeltaTemporality,
		Filter:           attrFltr,
		AggregationLimit: numSeries + 8,
	}.DDSketch(testDDSketchAccuracy, false, false, false, 0)

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

	pointsPerInterval := numSeries * scrapes
	totalPoints := pointsPerInterval * intervals

	var startMem runtime.MemStats
	runtime.ReadMemStats(&startMem)
	cpuStart := sampleProcessCPUSeconds()

	totalDuration := time.Duration(0)
	for interval := 0; interval < intervals; interval++ {
		iterStart := time.Now()
		for i := 0; i < scrapes; i++ {
			for j := 0; j < numSeries; j++ {
				value := float64((interval*scrapes*37 + i*97 + j) % 4096)
				meas(ctx, value, attrSets[j])
			}
		}
		iterDuration := time.Since(iterStart)
		totalDuration += iterDuration

		var agg metricdata.Aggregation
		comp(&agg) // drain the interval into a DDSketch aggregation

		t.Logf(
			"interval=%d duration=%s throughput=%.2f samples/s",
			interval,
			iterDuration,
			float64(pointsPerInterval)/iterDuration.Seconds(),
		)
	}

	cpuSeconds := sampleProcessCPUSeconds() - cpuStart
	var endMem runtime.MemStats
	runtime.ReadMemStats(&endMem)

	t.Logf(
		"multi-interval summary series=%d scrapes/interval=%d intervals=%d points=%d total-duration=%s avg-throughput=%.2f samples/s cpu=%.4fs heap-delta=%dB heap-objects=%d",
		numSeries,
		scrapes,
		intervals,
		totalPoints,
		totalDuration,
		float64(totalPoints)/totalDuration.Seconds(),
		cpuSeconds,
		int64(endMem.Alloc)-int64(startMem.Alloc),
		int64(endMem.HeapObjects)-int64(startMem.HeapObjects),
	)
}

func TestDDSketchLatencyPerMeasurement(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping latency sampling in short mode")
	}

	count := *ddsketchLatencyMeasurements
	if count <= 0 {
		t.Fatalf("invalid latency measurement count %d", count)
	}

	ctx := t.Context()
	meas, _ := Builder[float64]{
		Temporality:      metricdata.DeltaTemporality,
		Filter:           attrFltr,
		AggregationLimit: 4,
	}.DDSketch(testDDSketchAccuracy, false, false, false, 0)

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
		"records=%d duration=%s avg-latency=%s throughput=%.2f records/s cpu=%.4fs mem/record=%.2fB",
		count,
		total,
		avgLatency,
		throughput,
		cpuSeconds,
		memPerRecord,
	)
}

func record[N int64 | float64](meas Measure[N], inputs []arg[N]) {
	for _, in := range inputs {
		meas(in.ctx, in.value, in.attr)
	}
}

func findDDSketchDP[N int64 | float64](
	t *testing.T,
	dps []metricdata.DDSketchDataPoint[N],
	attrs attribute.Set,
) metricdata.DDSketchDataPoint[N] {
	t.Helper()
	for _, dp := range dps {
		if dp.Attributes.Equals(&attrs) {
			return dp
		}
	}
	t.Fatalf("did not find datapoint for %v", attrs)
	return metricdata.DDSketchDataPoint[N]{}
}

func assertExtremaEqual[N int64 | float64](t *testing.T, extrema metricdata.Extrema[N], expected N) {
	t.Helper()
	value, ok := extrema.Value()
	require.True(t, ok, "expected extrema value")
	require.Equal(t, expected, value)
}

func assertExtremaMissing[N int64 | float64](t *testing.T, extrema metricdata.Extrema[N]) {
	t.Helper()
	_, ok := extrema.Value()
	require.False(t, ok, "expected extrema to be unset")
}

// duration captures wall-clock time (time.Since(start)), so it includes everything—actual work, time waiting on the scheduler, blocking syscalls, idle time. sampleProcessCPUSeconds reads the runtime’s /cpu/classes/total:cpu-seconds, which accumulates how much CPU time the process actually consumed. That excludes idle time: if the test is mostly waiting or preempted, wall time goes up but CPU seconds stay low. Comparing both tells you whether the workload is CPU-bound (values similar) or waiting/blocked (duration ≫ CPU seconds).
func sampleProcessCPUSeconds() float64 {
	samples := []metrics.Sample{
		{Name: "/cpu/classes/total:cpu-seconds"},
	}
	metrics.Read(samples)
	v := samples[0].Value
	if v.Kind() == metrics.KindFloat64 {
		return v.Float64()
	}
	return 0
}

// TestDDSketchPayloadIsSketchlibPortableEnvelope confirms the post-#262
// migration to sketchlib-go: the SDK aggregator's DDSketchDataPoint.Sketch
// payload is now a SketchEnvelope-wrapped DDSketchState (the wire format the
// agent processor and asap-precompute-{go,rs} consumers expect), not the
// pre-migration DataDog sketchpb.DDSketch shape that collided on field 1's
// wire type. The structural mismatch was diagnosed in PR #269.
func TestDDSketchPayloadIsSketchlibPortableEnvelope(t *testing.T) {
	c := new(clock)
	t.Cleanup(c.Register())

	ctx := t.Context()
	meas, comp := Builder[float64]{
		Temporality:      metricdata.DeltaTemporality,
		Filter:           attrFltr,
		AggregationLimit: 4,
	}.DDSketch(testDDSketchAccuracy, false, false, false, 0)

	attrs := attribute.NewSet(
		attribute.String("service", "checkout"),
		attribute.String("region", "us-east-1"),
	)
	for _, v := range []float64{1.5, 2.5, 3.5, 100.0, 99.0} {
		meas(ctx, v, attrs)
	}

	got := new(metricdata.Aggregation)
	require.Equal(t, 1, comp(got))
	agg := (*got).(metricdata.DDSketch[float64])
	require.Len(t, agg.DataPoints, 1)
	dp := agg.DataPoints[0]
	require.Equal(t, metricdata.DDSketchEncodingProto, dp.Encoding)
	require.NotEmpty(t, dp.Sketch)

	// Round-trip through sketchlib-go's portable envelope schema. This
	// pins the SDK's wire format to the same shape decodeDDSketchEnvelope
	// (in opentelemetry-collector-contrib-patch/processor/ddsketchprocessor)
	// and asap-precompute-rs's DDSketchWrapper::decode_envelope expect.
	var env envpb.SketchEnvelope
	require.NoError(t, proto.Unmarshal(dp.Sketch, &env))
	state := env.GetDdsketch()
	require.NotNil(t, state, "envelope must carry a DDSketchState variant")
	require.InDelta(t, testDDSketchAccuracy, state.Alpha, 1e-12)
	require.Equal(t, uint64(5), state.Count)

	// Reconstruct via the canonical NewFromState entrypoint and confirm
	// quantiles agree within the accuracy bound (the actual value the
	// agent will emit on the consume side).
	recovered, err := ddsketch.NewFromState(state)
	require.NoError(t, err)
	q, ok := recovered.Quantile(0.99)
	require.True(t, ok)
	require.InDelta(t, 100.0, q, 100.0*testDDSketchAccuracy)
}

// TestDDSketchDeltaEncodingViaComputeDelta confirms the cumulative-mode
// deltaTransmission path emits a DDSketchEncodingProtoDelta payload on the
// second tick (after a snapshot exists), and that the bytes round-trip
// through sketchlib-go's ApplyDelta.
func TestDDSketchDeltaEncodingViaComputeDelta(t *testing.T) {
	c := new(clock)
	t.Cleanup(c.Register())

	ctx := t.Context()
	meas, comp := Builder[float64]{
		Temporality:      metricdata.CumulativeTemporality,
		Filter:           attrFltr,
		AggregationLimit: 4,
	}.DDSketch(testDDSketchAccuracy, false, false, true, 1)

	attrs := attribute.NewSet(
		attribute.String("service", "checkout"),
		attribute.String("region", "us-east-1"),
	)
	for _, v := range []float64{1.5, 2.5, 3.5} {
		meas(ctx, v, attrs)
	}

	got := new(metricdata.Aggregation)
	require.Equal(t, 1, comp(got))
	agg := (*got).(metricdata.DDSketch[float64])
	require.Len(t, agg.DataPoints, 1)
	// First export is full state — there's no prior snapshot to delta against.
	require.Equal(t, metricdata.DDSketchEncodingProto, agg.DataPoints[0].Encoding)

	// Second tick: add more values. With deltaTransmission=true and a
	// snapshot now present, this must emit a delta payload.
	for _, v := range []float64{10.0, 20.0, 30.0} {
		meas(ctx, v, attrs)
	}
	require.Equal(t, 1, comp(got))
	agg = (*got).(metricdata.DDSketch[float64])
	require.Len(t, agg.DataPoints, 1)
	dp := agg.DataPoints[0]
	require.Equal(t, metricdata.DDSketchEncodingProtoDelta, dp.Encoding)
	require.NotEmpty(t, dp.Sketch)

	// The delta bytes must apply cleanly onto a sketch holding the prior
	// state — this is exactly what the agent's cumulative-state path does
	// when it receives a delta envelope from a downstream SDK.
	prior := ddsketch.NewDDSketch(testDDSketchAccuracy)
	for _, v := range []float64{1.5, 2.5, 3.5} {
		prior.Update(v)
	}
	require.NoError(t, ddsketch.ApplyDelta(prior, dp.Sketch))
	assert.Equal(t, uint64(6), prior.GetCount())
}
