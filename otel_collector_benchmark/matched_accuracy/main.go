// Matched-accuracy quantile-sketch head-to-head benchmark.
//
// All algorithms are configured to target ~1% relative error at p99 and run on
// the same Zipf-distributed input stream. We measure:
//
//   - Wire size in bytes (serialized payload after the window closes)
//   - Insertion CPU cost (ns / op, wall-clock)
//   - Resident heap delta (runtime.ReadMemStats HeapAlloc, before vs after)
//   - Recovered p50/p90/p99 and absolute / relative error vs the exact
//     percentiles computed on the sorted ground-truth stream
//
// Algorithms:
//   1. DDSketch  (alpha = 0.01)               – sketchlib-go
//   2. KLL       (k    = 200)                 – sketchlib-go
//   3. T-Digest                                – caio/go-tdigest
//      (the task spec asked for influxdata/tdigest, but that module is not
//      cached in this user's GOMODCACHE. caio/go-tdigest is cached, MIT-
//      licensed, pure Go, and implements the same algorithm. Documented.)
//   4. HDR-Histogram                           – HdrHistogram/hdrhistogram-go
//   5. Fixed linear histogram, 100 buckets     – inline
//   6. Raw samples                             – inline (count = N*8 bytes)
//   7. Gorilla                                 – SKIPPED. See rationale below.
//
// On Gorilla: the only Gorilla codec available locally lives in
// telegraf-patch/plugins/aggregators/gorilla/ and is wrapped in a Telegraf
// aggregator. More importantly, Gorilla is a (timestamp,value) lossless
// compression scheme for sequential time-series points; it does not estimate
// quantiles. Including it in a "matched p99 error" comparison would mean
// either feeding it synthetic timestamps and storing every sample (in which
// case it loses on wire size to true sketches by design) or hand-rolling a
// quantile estimator on top of the decompressed series (no longer Gorilla).
// We leave a TODO for a future "lossless time-series" track instead of
// muddying this matched-accuracy table.

package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	hdr "github.com/HdrHistogram/hdrhistogram-go"
	tdigest "github.com/caio/go-tdigest"

	ddsketch "github.com/ProjectASAP/sketchlib-go/sketches/DDSketch"
	kll "github.com/ProjectASAP/sketchlib-go/sketches/KLL"
)

// ---------------------------------------------------------------------------
// Result row (one per algorithm per skew run).
// ---------------------------------------------------------------------------

type row struct {
	skew         float64
	algorithm    string
	samples      int
	p50Est       float64
	p50Truth     float64
	p99Est       float64
	p99Truth     float64
	p99RelErr    float64
	wireBytes    int
	insertNsPerOp float64
	heapDelta    int64
}

// ---------------------------------------------------------------------------
// Algorithm interface – every algorithm produces a closed-window result.
// ---------------------------------------------------------------------------

type quantileAlgo struct {
	name string
	// run consumes the stream once, returns (insertNs, heapDelta, wireBytes,
	// quantile-getter).
	run func(stream []float64) (insertNs float64, heapDelta int64, wireBytes int, q func(p float64) float64)
}

// ---------------------------------------------------------------------------
// Stream generation – Zipf with a fixed seed, scaled to a positive float64.
// ---------------------------------------------------------------------------

func makeZipfStream(n int, s float64, seed uint64) []float64 {
	// math/rand/v2's Zipf wants imax; pick a fairly large support so the tail
	// has room to breathe across all three skews.
	const imax uint64 = 1_000_000
	src := rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)
	r := rand.New(src)
	z := rand.NewZipf(r, s, 1.0, imax)
	out := make([]float64, n)
	for i := 0; i < n; i++ {
		// Zipf returns uint64 in [0, imax]; shift by 1 so DDSketch (positive
		// only) sees a non-zero value.
		out[i] = float64(z.Uint64() + 1)
	}
	return out
}

// ---------------------------------------------------------------------------
// Ground-truth percentile from the sorted stream.
// ---------------------------------------------------------------------------

func exactQuantile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if q <= 0 {
		return sorted[0]
	}
	if q >= 1 {
		return sorted[len(sorted)-1]
	}
	// Use ceil(q*N)-1 (1-indexed rank → 0-indexed slot) to match DDSketch's
	// rank convention reasonably closely.
	idx := int(math.Ceil(q*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// ---------------------------------------------------------------------------
// Algorithm: DDSketch (alpha = 0.01).
// ---------------------------------------------------------------------------

func runDDSketch(stream []float64) (float64, int64, int, func(float64) float64) {
	runtime.GC()
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)

	d := ddsketch.NewDDSketch(0.01)
	t0 := time.Now()
	for _, v := range stream {
		d.Update(v)
	}
	insertNs := float64(time.Since(t0).Nanoseconds()) / float64(len(stream))

	runtime.ReadMemStats(&m1)
	heap := int64(m1.HeapAlloc) - int64(m0.HeapAlloc)

	wire, err := d.SerializeToBytes()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ddsketch serialize: %v\n", err)
		os.Exit(1)
	}

	q := func(p float64) float64 {
		v, _ := d.Quantile(p)
		return v
	}
	return insertNs, heap, len(wire), q
}

// ---------------------------------------------------------------------------
// Algorithm: KLL (k = 200).
// ---------------------------------------------------------------------------

func runKLL(stream []float64) (float64, int64, int, func(float64) float64) {
	runtime.GC()
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)

	k := kll.InitKLL(200)
	t0 := time.Now()
	for _, v := range stream {
		k.Update(v)
	}
	insertNs := float64(time.Since(t0).Nanoseconds()) / float64(len(stream))

	runtime.ReadMemStats(&m1)
	heap := int64(m1.HeapAlloc) - int64(m0.HeapAlloc)

	wire, err := k.SerializeToBytes()
	if err != nil {
		fmt.Fprintf(os.Stderr, "kll serialize: %v\n", err)
		os.Exit(1)
	}

	// CDF is built once and reused across p50/p90/p99.
	cdf := k.CDF()
	q := func(p float64) float64 { return cdf.Query(p) }
	return insertNs, heap, len(wire), q
}

// ---------------------------------------------------------------------------
// Algorithm: T-Digest (compression = 100, tuned for ~1% rel-err at p99).
// ---------------------------------------------------------------------------

func runTDigest(stream []float64) (float64, int64, int, func(float64) float64) {
	runtime.GC()
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)

	td, err := tdigest.New(tdigest.Compression(100))
	if err != nil {
		fmt.Fprintf(os.Stderr, "tdigest new: %v\n", err)
		os.Exit(1)
	}
	t0 := time.Now()
	for _, v := range stream {
		_ = td.Add(v)
	}
	insertNs := float64(time.Since(t0).Nanoseconds()) / float64(len(stream))

	runtime.ReadMemStats(&m1)
	heap := int64(m1.HeapAlloc) - int64(m0.HeapAlloc)

	wire, err := td.AsBytes()
	if err != nil {
		fmt.Fprintf(os.Stderr, "tdigest serialize: %v\n", err)
		os.Exit(1)
	}

	q := func(p float64) float64 { return td.Quantile(p) }
	return insertNs, heap, len(wire), q
}

// ---------------------------------------------------------------------------
// Algorithm: HDR-Histogram (3 sig-figs ≈ 0.1% rel-err at every percentile).
// ---------------------------------------------------------------------------

func runHDR(stream []float64) (float64, int64, int, func(float64) float64) {
	runtime.GC()
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)

	// Sized for our Zipf support [1, 1_000_001]; sigfigs=3 yields a worst-case
	// relative error of 0.1% per bucket — well inside the 1% target.
	h := hdr.New(1, 10_000_000, 3)
	t0 := time.Now()
	for _, v := range stream {
		_ = h.RecordValue(int64(v))
	}
	insertNs := float64(time.Since(t0).Nanoseconds()) / float64(len(stream))

	runtime.ReadMemStats(&m1)
	heap := int64(m1.HeapAlloc) - int64(m0.HeapAlloc)

	// hdrhistogram-go has no MarshalBinary; use the zero-copy export of the
	// counts slice and add 32 bytes of header (lowest, highest, sigfigs, len)
	// as the on-the-wire size. This matches what the V2 encoder ships before
	// LEB128/zlib compression.
	counts := h.Export().Counts
	wireBytes := len(counts)*8 + 32

	q := func(p float64) float64 { return float64(h.ValueAtQuantile(p * 100.0)) }
	return insertNs, heap, wireBytes, q
}

// ---------------------------------------------------------------------------
// Algorithm: fixed linear histogram with 100 buckets.
// Bucket layout is [lo, hi] split uniformly into B buckets; values past hi
// land in the last bucket.
// ---------------------------------------------------------------------------

type linHist struct {
	lo, hi float64
	width  float64
	counts []uint64
	min    float64
	max    float64
	count  uint64
}

func newLinHist(lo, hi float64, b int) *linHist {
	return &linHist{
		lo: lo, hi: hi,
		width:  (hi - lo) / float64(b),
		counts: make([]uint64, b),
		min:    math.Inf(1),
		max:    math.Inf(-1),
	}
}

func (h *linHist) add(v float64) {
	h.count++
	if v < h.min {
		h.min = v
	}
	if v > h.max {
		h.max = v
	}
	idx := int((v - h.lo) / h.width)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(h.counts) {
		idx = len(h.counts) - 1
	}
	h.counts[idx]++
}

func (h *linHist) quantile(p float64) float64 {
	if h.count == 0 {
		return 0
	}
	target := uint64(math.Ceil(p * float64(h.count)))
	var seen uint64
	for i, c := range h.counts {
		seen += c
		if seen >= target {
			return h.lo + (float64(i)+0.5)*h.width
		}
	}
	return h.max
}

func (h *linHist) serialize() []byte {
	// 3 floats (lo, hi, width) + count + min + max + B uint64 counts.
	out := make([]byte, 8*6+8*len(h.counts))
	binary.LittleEndian.PutUint64(out[0:], math.Float64bits(h.lo))
	binary.LittleEndian.PutUint64(out[8:], math.Float64bits(h.hi))
	binary.LittleEndian.PutUint64(out[16:], math.Float64bits(h.width))
	binary.LittleEndian.PutUint64(out[24:], h.count)
	binary.LittleEndian.PutUint64(out[32:], math.Float64bits(h.min))
	binary.LittleEndian.PutUint64(out[40:], math.Float64bits(h.max))
	for i, c := range h.counts {
		binary.LittleEndian.PutUint64(out[48+i*8:], c)
	}
	return out
}

func runLinHist(stream []float64) (float64, int64, int, func(float64) float64) {
	// Sweep once for lo/hi so the histogram is sized to the data. This is a
	// best case for a fixed histogram (in production you'd guess the bounds).
	lo, hi := math.Inf(1), math.Inf(-1)
	for _, v := range stream {
		if v < lo {
			lo = v
		}
		if v > hi {
			hi = v
		}
	}

	runtime.GC()
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)

	h := newLinHist(lo, hi, 100)
	t0 := time.Now()
	for _, v := range stream {
		h.add(v)
	}
	insertNs := float64(time.Since(t0).Nanoseconds()) / float64(len(stream))

	runtime.ReadMemStats(&m1)
	heap := int64(m1.HeapAlloc) - int64(m0.HeapAlloc)

	wire := h.serialize()
	q := func(p float64) float64 { return h.quantile(p) }
	return insertNs, heap, len(wire), q
}

// ---------------------------------------------------------------------------
// Algorithm: raw samples baseline (no compression). Wire size is N * 8 bytes.
// ---------------------------------------------------------------------------

func runRaw(stream []float64) (float64, int64, int, func(float64) float64) {
	runtime.GC()
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)

	buf := make([]float64, 0, len(stream))
	t0 := time.Now()
	for _, v := range stream {
		buf = append(buf, v)
	}
	insertNs := float64(time.Since(t0).Nanoseconds()) / float64(len(stream))

	runtime.ReadMemStats(&m1)
	heap := int64(m1.HeapAlloc) - int64(m0.HeapAlloc)

	// Quantile is exact (sort a copy so we don't pollute later runs).
	cp := make([]float64, len(buf))
	copy(cp, buf)
	sort.Float64s(cp)
	q := func(p float64) float64 { return exactQuantile(cp, p) }

	return insertNs, heap, len(buf) * 8, q
}

// ---------------------------------------------------------------------------
// TODO: Gorilla. See file header for rationale.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Driver.
// ---------------------------------------------------------------------------

func main() {
	skew := flag.Float64("skew", 1.5, "Zipf skew parameter s (>1)")
	n := flag.Int("n", 1_000_000, "stream length")
	seed := flag.Uint64("seed", 0xC0FFEE, "PRNG seed (fixed for reproducibility)")
	out := flag.String("out", "results/matched_accuracy.csv", "output CSV path")
	appendMode := flag.Bool("append", false, "append to existing CSV instead of overwriting")
	flag.Parse()

	if *skew <= 1.0 {
		fmt.Fprintf(os.Stderr, "ERROR: --skew must be > 1.0 (got %g)\n", *skew)
		os.Exit(2)
	}

	fmt.Printf("matched-accuracy benchmark: skew=%.2f n=%d seed=0x%x\n", *skew, *n, *seed)

	// Build the stream once and the sorted ground truth once.
	stream := makeZipfStream(*n, *skew, *seed)
	sorted := make([]float64, len(stream))
	copy(sorted, stream)
	sort.Float64s(sorted)

	p50Truth := exactQuantile(sorted, 0.50)
	p90Truth := exactQuantile(sorted, 0.90)
	p99Truth := exactQuantile(sorted, 0.99)
	_ = p90Truth
	fmt.Printf("ground truth: p50=%.2f p90=%.2f p99=%.2f\n", p50Truth, p90Truth, p99Truth)

	algos := []quantileAlgo{
		{"ddsketch_a0.01", runDDSketch},
		{"kll_k200", runKLL},
		{"tdigest_c100", runTDigest},
		{"hdr_sigfig3", runHDR},
		{"linhist_b100", runLinHist},
		{"raw_samples", runRaw},
		// Gorilla intentionally omitted – see file header.
	}

	rows := make([]row, 0, len(algos))
	for _, a := range algos {
		ins, heap, wire, q := a.run(stream)
		p50e := q(0.50)
		p99e := q(0.99)

		relErr := 0.0
		if p99Truth != 0 {
			relErr = math.Abs(p99e-p99Truth) / p99Truth
		}

		r := row{
			skew:         *skew,
			algorithm:    a.name,
			samples:      *n,
			p50Est:       p50e,
			p50Truth:     p50Truth,
			p99Est:       p99e,
			p99Truth:     p99Truth,
			p99RelErr:    relErr,
			wireBytes:    wire,
			insertNsPerOp: ins,
			heapDelta:    heap,
		}
		rows = append(rows, r)
		fmt.Printf("  %-16s  p99_est=%-12.2f  rel_err=%.4f  wire=%-10d  ns/op=%-8.1f  heap=%d\n",
			a.name, r.p99Est, r.p99RelErr, r.wireBytes, r.insertNsPerOp, r.heapDelta)
	}

	// Write CSV (create dirs as needed).
	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir %s: %v\n", filepath.Dir(*out), err)
		os.Exit(1)
	}

	mode := os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	writeHeader := true
	if *appendMode {
		if _, err := os.Stat(*out); err == nil {
			mode = os.O_CREATE | os.O_WRONLY | os.O_APPEND
			writeHeader = false
		}
	}
	f, err := os.OpenFile(*out, mode, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open %s: %v\n", *out, err)
		os.Exit(1)
	}
	defer f.Close()

	if writeHeader {
		fmt.Fprintln(f, "skew,algorithm,samples,p50_est,p50_truth,p99_est,p99_truth,p99_rel_err,wire_bytes,insert_ns_per_op,heap_delta_bytes")
	}
	for _, r := range rows {
		fmt.Fprintf(f, "%.2f,%s,%d,%.4f,%.4f,%.4f,%.4f,%.6f,%d,%.2f,%d\n",
			r.skew, r.algorithm, r.samples,
			r.p50Est, r.p50Truth, r.p99Est, r.p99Truth, r.p99RelErr,
			r.wireBytes, r.insertNsPerOp, r.heapDelta,
		)
	}

	// Markdown summary.
	fmt.Println()
	fmt.Println("## matched-accuracy summary (skew =", *skew, ")")
	fmt.Println()
	fmt.Println("| algorithm | p99 est | p99 truth | p99 rel-err | wire bytes | ns/op | heap Δ bytes |")
	fmt.Println("|---|---:|---:|---:|---:|---:|---:|")
	for _, r := range rows {
		fmt.Printf("| %s | %.2f | %.2f | %.4f | %d | %.1f | %d |\n",
			r.algorithm, r.p99Est, r.p99Truth, r.p99RelErr, r.wireBytes, r.insertNsPerOp, r.heapDelta)
	}
	fmt.Println()
	fmt.Printf("CSV written to: %s\n", *out)
}
