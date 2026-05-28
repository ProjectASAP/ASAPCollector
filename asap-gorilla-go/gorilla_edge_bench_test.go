package gorilla

// Gorilla EDGE processor measurement (codec core). Mirrors the
// ddsketchprocessor processor_bench_test.go style (deterministic synthetic
// input, b.ReportAllocs, ns/op + B/op + allocs/op).
//
// What this isolates: the agent-side gorilla edge ENCODE path — take counter
// samples, XOR-encode them into Gorilla chunks via StreamingFragmentEncoder
// (the exact type the asapedgeprocessor cold tier uses, see cold_encoder.go),
// then serialize the drained fragments into the ASAPFRG1 wire frame with
// EncodeFragmentBatch. The actual network POST + gzip is excluded (that lives
// in the processor's fragmentShipper; the gzip ratio is reported separately by
// the bytes/sample bench below using the raw ASAPFRG1 frame, which is the
// pre-gzip compression the merger re-chunk later improves on).
//
// Four measurements:
//   (1) BenchmarkGorillaEdgeEncode_*  — CPU/alloc of encode at 100/1k/10k series.
//   (2) TestGorillaEdgeFootprintBytesPerSeries — steady-state heap held for the
//       open-chunk + pending + series-map state per series (runtime.ReadMemStats).
//   (3) TestGorillaEdgeBytesPerSample — emitted ASAPFRG1 bytes/sample at edge
//       flush/chunk sizes 10/30/60/120 vs raw 16 B/sample, showing the ratio
//       degradation at small chunks (the suboptimal edge ratio the merger fixes).
//   (4) TestGorillaXORBytesPerSampleByDataShape — PURE Prometheus chunkenc.XOR
//       bytes/sample at 120/chunk across data shapes. The edge/merger gorilla
//       path IS Prometheus chunkenc.EncXOR, so this shows the codec is at parity
//       with vanilla Prometheus gorilla by construction, and that bytes/sample
//       is data-dependent (integer/smooth counters ~1 B/sample; the synthetic
//       random-walk corpus ~7 B/sample; noise ~raw).

import (
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"testing"
	"time"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

const (
	edgeBenchSeed            int64 = 0x5A9C011EC709072
	edgeSamplesPerSeries           = 120 // one full default chunk's worth per series
	edgeBaseTsUnixSec        int64 = 1700000000
	edgeStepMs               int64 = 1000 // 1s scrape cadence (regular ts => good DoD)
	edgeReorderGraceForBench       = 2 * time.Second
)

// buildEdgeSamples builds a deterministic slice of TSDBSample across `series`
// distinct label sets, `perSeries` samples each, in arrival (time-ascending)
// order. The value walk is a small random-magnitude float walk around a
// per-series baseline — the realistic counter/gauge shape the edge XOR encoder
// sees (NOT pure constants, which would compress unrealistically well, nor pure
// noise, which would compress unrealistically badly).
func buildEdgeSamples(series, perSeries int) []TSDBSample {
	rng := rand.New(rand.NewSource(edgeBenchSeed))
	out := make([]TSDBSample, 0, series*perSeries)
	base := time.Unix(edgeBaseTsUnixSec, 0).UTC()
	// Per-series attribute maps built once (cloneStringMap copies them on
	// getSeries, so reuse here is safe and keeps the builder cheap).
	attrs := make([]map[string]string, series)
	vals := make([]float64, series)
	for s := 0; s < series; s++ {
		attrs[s] = map[string]string{
			"host": fmt.Sprintf("host-%05d", s),
			"job":  "node",
		}
		vals[s] = 1000 + rng.Float64()*1000
	}
	// Emit time-ascending across all series so the encoder's watermark advances
	// and chunks actually flush (matches the agent's per-scrape arrival order).
	for i := 0; i < perSeries; i++ {
		ts := base.Add(time.Duration(int64(i)*edgeStepMs) * time.Millisecond)
		for s := 0; s < series; s++ {
			vals[s] += rng.Float64()*2 - 0.5 // gently drifting counter-ish walk
			out = append(out, TSDBSample{
				MetricName: "node_cpu_seconds_total",
				Attributes: attrs[s],
				Timestamp:  ts,
				Value:      vals[s],
			})
		}
	}
	return out
}

// encodeEdgeBatch runs the full edge encode: feed every sample into a fresh
// StreamingFragmentEncoder (XOR-chunk per series), force-drain to flush all
// open chunks into Fragments, then serialize them to the ASAPFRG1 frame. This
// is exactly what the cold tier does per flush window (minus gzip + POST).
func encodeEdgeBatch(samples []TSDBSample, samplesPerChunk int) ([]byte, int, error) {
	enc := NewStreamingFragmentEncoder(StreamingFragmentOptions{
		ReorderGrace:    edgeReorderGraceForBench,
		SamplesPerChunk: samplesPerChunk,
		Source:          "edge-bench",
	})
	for i := range samples {
		if err := enc.AddSample(samples[i]); err != nil {
			return nil, 0, err
		}
	}
	frags, err := enc.Drain(true)
	if err != nil {
		return nil, 0, err
	}
	frame := EncodeFragmentBatch(frags)
	return frame, len(frags), nil
}

func benchmarkGorillaEdgeEncode(b *testing.B, series int) {
	samples := buildEdgeSamples(series, edgeSamplesPerSeries)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		frame, _, err := encodeEdgeBatch(samples, edgeSamplesPerSeries)
		if err != nil {
			b.Fatalf("encode: %v", err)
		}
		if len(frame) == 0 {
			b.Fatal("empty frame")
		}
	}
}

// BenchmarkGorillaEdgeEncode_* — CPU + allocations of the edge XOR-encode +
// ASAPFRG1 build across cardinalities. ns/op / B/op / allocs/op scale with
// series count is visible across the three.
func BenchmarkGorillaEdgeEncode_100Series(b *testing.B) { benchmarkGorillaEdgeEncode(b, 100) }
func BenchmarkGorillaEdgeEncode_1kSeries(b *testing.B)  { benchmarkGorillaEdgeEncode(b, 1000) }
func BenchmarkGorillaEdgeEncode_10kSeries(b *testing.B) { benchmarkGorillaEdgeEncode(b, 10000) }

// TestGorillaEdgeFootprintBytesPerSeries measures the steady-state heap held by
// the encoder for N series each carrying a half-open XOR chunk (the realistic
// worst case: each series has accumulated < SamplesPerChunk points, so its open
// XOR chunk buffer + series-map entry + cloned attrs are all live and
// un-flushed). A huge ReorderGrace keeps the watermark behind every sample so
// nothing drains/flushes. Samples are generated on the fly (NOT held in a giant
// slice) so the only large retained allocation between the before/after reads
// is the encoder's own per-series state — making the HeapAlloc delta a clean
// estimate of bytes held per open series.
func TestGorillaEdgeFootprintBytesPerSeries(t *testing.T) {
	const (
		series = 10000
		open   = edgeSamplesPerSeries / 2 // half-open chunks: live, un-flushed
	)
	base := time.Unix(edgeBaseTsUnixSec, 0).UTC()

	var before, after runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&before)

	enc := NewStreamingFragmentEncoder(StreamingFragmentOptions{
		ReorderGrace:    time.Hour, // huge grace => nothing drains/flushes, state stays open
		SamplesPerChunk: edgeSamplesPerSeries,
		Source:          "edge-footprint",
	})
	// Feed time-ascending without retaining a slice: each series gets `open`
	// points. With ReorderGrace=1h the watermark never catches up, so every
	// series keeps a live open chunk + pending points (un-shipped state).
	rng := rand.New(rand.NewSource(edgeBenchSeed))
	vals := make([]float64, series)
	for s := range vals {
		vals[s] = 1000 + rng.Float64()*1000
	}
	for i := 0; i < open; i++ {
		ts := base.Add(time.Duration(int64(i)*edgeStepMs) * time.Millisecond)
		for s := 0; s < series; s++ {
			vals[s] += rng.Float64()*2 - 0.5
			err := enc.AddSample(TSDBSample{
				MetricName: "node_cpu_seconds_total",
				Attributes: map[string]string{"host": fmt.Sprintf("host-%05d", s), "job": "node"},
				Timestamp:  ts,
				Value:      vals[s],
			})
			if err != nil {
				t.Fatalf("add: %v", err)
			}
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&after)

	if got := enc.ActiveSeries(); got != series {
		t.Fatalf("active series = %d, want %d", got, series)
	}
	heldBytes := int64(after.HeapInuse) - int64(before.HeapInuse)
	perSeries := float64(heldBytes) / float64(series)
	t.Logf("FOOTPRINT: %d series x %d open (un-flushed) samples each => held HeapInuse %d bytes => %.0f bytes/series",
		series, open, heldBytes, perSeries)
	runtime.KeepAlive(enc)
}

// TestGorillaEdgeBytesPerSample measures the emitted ASAPFRG1 frame size and
// computes bytes/sample at edge flush/chunk sizes 10/30/60/120, versus the raw
// 16 B/sample (8B ts + 8B float64). Smaller chunks => more per-chunk + per-
// fragment header overhead and shorter delta-of-delta runs => worse bytes/
// sample. This is the suboptimal edge ratio the merger re-chunk (to ~120
// samples/chunk) later improves on.
func TestGorillaEdgeBytesPerSample(t *testing.T) {
	const series = 1000
	const perSeries = 120 // 1 flush window per series at chunk=120; 12 chunks at chunk=10
	samples := buildEdgeSamples(series, perSeries)
	totalSamples := len(samples)
	rawBytes := totalSamples * 16 // 8B ts + 8B float64

	t.Logf("BYTES/SAMPLE: %d samples (%d series x %d), raw = %d B (16 B/sample)",
		totalSamples, series, perSeries, rawBytes)
	t.Logf("%-12s %-10s %-12s %-14s %-12s %-10s", "chunkSize", "frags", "frameBytes", "bytes/sample", "vs-raw-16B", "ratioDeg")

	var baseline float64
	for idx, chunkSize := range []int{120, 60, 30, 10} {
		frame, nFrags, err := encodeEdgeBatch(samples, chunkSize)
		if err != nil {
			t.Fatalf("chunk %d: %v", chunkSize, err)
		}
		bps := float64(len(frame)) / float64(totalSamples)
		vsRaw := bps / 16.0
		if idx == 0 {
			baseline = bps // chunk=120 is the best (largest) edge chunk
		}
		deg := bps / baseline
		t.Logf("%-12d %-10d %-12d %-14.3f %-12.3f %-10.2fx",
			chunkSize, nFrags, len(frame), bps, vsRaw, deg)
	}
}

// TestGorillaXORBytesPerSampleByDataShape measures the PURE Prometheus
// chunkenc.XOR chunk size (bytes/sample) at the default 120 samples/chunk
// across data shapes. The edge AND merger gorilla path IS Prometheus
// chunkenc.EncXOR — these are the same XOR chunks the merger re-chunks and
// thanos-store-gateway serves — so this is "Prometheus gorilla" by
// construction; there is no separate/custom codec to be faster or slower than.
// The point: gorilla bytes/sample is strongly DATA-DEPENDENT. Integer/smooth
// counters compress to ~1 B/sample; the synthetic random-walk corpus used in
// BENCHMARKS.md lands near ~7 B/sample; uniform noise approaches raw 16 B.
// I.e. BENCHMARKS.md's ~7 B/sample is a property of that synthetic corpus, not
// a codec deficiency — a vanilla Prometheus tsdb XOR chunk yields the same.
func TestGorillaXORBytesPerSampleByDataShape(t *testing.T) {
	const n = 120 // one full default chunk
	baseMs := time.Unix(edgeBaseTsUnixSec, 0).UnixMilli()
	rwRng := rand.New(rand.NewSource(edgeBenchSeed))
	rwVal := 1000.0
	noiseRng := rand.New(rand.NewSource(edgeBenchSeed + 1))

	shapes := []struct {
		name string
		gen  func(i int) float64
	}{
		{"int counter (+1/step)", func(i int) float64 { return float64(1000 + i) }},
		{"smooth counter (+1.5/step)", func(i int) float64 { return 1000 + 1.5*float64(i) }},
		{"low-noise gauge (sin)", func(i int) float64 { return 1000 + 5*math.Sin(float64(i)/8) }},
		{"random-walk (BENCHMARKS.md corpus)", func(i int) float64 { rwVal += rwRng.Float64()*2 - 0.5; return rwVal }},
		{"high-noise (uniform 0..1e6)", func(i int) float64 { return noiseRng.Float64() * 1e6 }},
	}

	t.Logf("PROMETHEUS chunkenc.XOR bytes/sample, %d samples/chunk @ 1s step (raw ref = 16 B/sample):", n)
	for _, sh := range shapes {
		c := chunkenc.NewXORChunk()
		app, err := c.Appender()
		if err != nil {
			t.Fatalf("appender: %v", err)
		}
		for i := 0; i < n; i++ {
			app.Append(baseMs+int64(i)*1000, sh.gen(i))
		}
		bps := float64(len(c.Bytes())) / float64(n)
		t.Logf("  %-36s %6.2f B/sample  (%5.2fx vs raw 16B)", sh.name, bps, bps/16.0)
	}
}
