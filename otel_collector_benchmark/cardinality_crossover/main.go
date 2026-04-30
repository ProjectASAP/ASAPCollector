// cardinality_crossover: identify the series-cardinality break-even point
// where a sketch (CountSketch / CountMinSketch) becomes smaller than raw
// (series_id, value) tuple transmission.
//
// At small N, the fixed-size sketch matrix dominates and is *bigger* than
// raw. At large N, the sketch is constant-bounded while raw grows linearly,
// so the sketch becomes *smaller*. This program sweeps N and writes a CSV
// suitable for plotting bytes vs. N (and an accompanying top-K accuracy
// metric to show the sketch is still useful in the regime where it wins).
//
// Usage:
//   go run . [-smoke] [-out results/cardinality_crossover.csv]
//
//   -smoke runs only N = 10_000 (used by bench_cardinality_crossover.sh's
//          smoke mode). Without it, the full sweep
//          {100, 1k, 10k, 100k, 1M, 5M} is executed.
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/ProjectASAP/sketchlib-go/common"
	countminsketch "github.com/ProjectASAP/sketchlib-go/sketches/CountMinSketch"
	countsketch "github.com/ProjectASAP/sketchlib-go/sketches/CountSketch"
)

// ---------------------------------------------------------------------------
// Tunables
// ---------------------------------------------------------------------------

// Per-tuple raw cost lower bound: 8 bytes series_id + 8 bytes value.
const rawBytesPerTupleMin = 16

// OTLP envelope per series (resource + scope + metric + datapoint framing
// estimate). Spec gives a 32-byte realistic envelope per series on top of
// the 16-byte payload.
const rawBytesOtlpEnvelope = 32

// Zipf parameters (s=1.5, fixed seed). The Go Zipf API requires s > 1.
const (
	zipfS      = 1.5
	zipfV      = 1.0
	zipfImax   = uint64(1 << 20) // 1M-value support; values above N modulo back.
	zipfSeed   = 42
	topKHeavy  = 10
)

// dimSpec captures one (rows, cols) pair to evaluate at every N. cols must
// be a power of two for CountSketch (sketchlib-go requires it for fast
// bit-mask hashing). The "default" variant rounds the spec's 2000 up to the
// closest power of two, 2048; the "narrowed" variant uses (3, 512).
type dimSpec struct {
	label string
	rows  int
	cols  int
}

// nextPow2 returns the smallest power of two >= n, with a floor of 1.
func nextPow2(n int) int {
	if n < 1 {
		return 1
	}
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}

func main() {
	var (
		smoke   bool
		outPath string
	)
	flag.BoolVar(&smoke, "smoke", false, "smoke mode: only N=10_000")
	flag.StringVar(&outPath, "out", "results/cardinality_crossover.csv",
		"output CSV path (relative to working directory)")
	flag.Parse()

	// Resolve outPath relative to the binary's working directory; ensure
	// the parent exists. We do not chdir.
	if absOut, err := filepath.Abs(outPath); err == nil {
		outPath = absOut
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		log.Fatalf("mkdir results dir: %v", err)
	}

	// Cardinality sweep.
	var ns []int
	if smoke {
		ns = []int{10_000}
	} else {
		ns = []int{100, 1_000, 10_000, 100_000, 1_000_000, 5_000_000}
	}

	// Spec asked for rows=5, cols=2000; sketchlib-go's CountSketch requires
	// cols to be a power of two (math/bits trick on the index hash). 2048 is
	// the closest power of two >= 2000 and is the actual sketchlib default.
	// CountMinSketch's processor default is rows=5, cols=1024 (also a power
	// of two); for an apples-to-apples comparison we use 2048 for both.
	defaultCols := nextPow2(2000)   // 2048
	narrowedCols := nextPow2(500)   // 512

	dims := []dimSpec{
		{label: "default", rows: 5, cols: defaultCols},
		{label: "narrowed", rows: 3, cols: narrowedCols},
	}

	f, err := os.Create(outPath)
	if err != nil {
		log.Fatalf("create csv: %v", err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	header := []string{
		"n_series",
		"sketch_type",
		"dim_label",
		"rows",
		"cols",
		"sketch_bytes",
		"raw_bytes_min",
		"raw_bytes_realistic",
		"ratio_min",
		"ratio_realistic",
		"top_k_relative_error",
		"insert_seconds",
	}
	if err := w.Write(header); err != nil {
		log.Fatalf("csv header: %v", err)
	}

	fmt.Printf("=== cardinality crossover sweep ===\n")
	fmt.Printf("smoke=%v out=%s\n", smoke, outPath)
	fmt.Printf("Ns=%v\n", ns)
	for _, d := range dims {
		fmt.Printf("dim %-8s rows=%d cols=%d\n", d.label, d.rows, d.cols)
	}
	fmt.Println()

	for _, n := range ns {
		// Generate the (series_id, value) tuples once per N, reuse for
		// every (sketch_type x dim_label) combination so we are comparing
		// apples to apples.
		tuples, groundTruth := generateTuples(n)
		topKKeys := pickTopKHeavy(groundTruth, topKHeavy)

		rawMin := int64(n) * rawBytesPerTupleMin
		rawReal := int64(n) * (rawBytesPerTupleMin + rawBytesOtlpEnvelope)

		for _, d := range dims {
			// CountSketch
			recordCS := evalCountSketch(n, tuples, groundTruth, topKKeys, d)
			recordCS.rawMin = rawMin
			recordCS.rawReal = rawReal
			writeRow(w, recordCS)
			logRow(recordCS)

			// CountMinSketch
			recordCMS := evalCountMinSketch(n, tuples, groundTruth, topKKeys, d)
			recordCMS.rawMin = rawMin
			recordCMS.rawReal = rawReal
			writeRow(w, recordCMS)
			logRow(recordCMS)
		}
	}

	w.Flush()
	if err := w.Error(); err != nil {
		log.Fatalf("csv flush: %v", err)
	}
	fmt.Printf("\nWrote %s\n", outPath)
}

// ---------------------------------------------------------------------------
// Tuple generation
// ---------------------------------------------------------------------------

// generateTuples produces N distinct (series_id, value) tuples. series_id is
// just the index 0..N-1 (so they are guaranteed distinct). value is drawn
// from a Zipf(s=1.5) distribution, which gives a small number of very heavy
// keys and a long tail. Returns the tuples and a ground-truth map from
// series_id -> count of how many tuples carry that *value*.
//
// For the cardinality crossover the *number of distinct series* is what
// matters for raw transmission cost, while sketch error is driven by the
// value distribution. We treat the value as the "key" for sketch insertion
// (so heavy values collide-resistantly) and record one tuple per series.
func generateTuples(n int) ([]tuple, map[uint64]int64) {
	rng := rand.New(rand.NewSource(zipfSeed))
	z := rand.NewZipf(rng, zipfS, zipfV, zipfImax)

	out := make([]tuple, n)
	gt := make(map[uint64]int64, n) // value -> occurrence count

	for i := 0; i < n; i++ {
		v := z.Uint64()
		out[i] = tuple{seriesID: uint64(i), value: v}
		gt[v]++
	}
	return out, gt
}

type tuple struct {
	seriesID uint64
	value    uint64
}

// pickTopKHeavy returns the k keys with highest ground-truth count, ordered
// from heaviest to lightest. Ties are broken by key ascending.
func pickTopKHeavy(gt map[uint64]int64, k int) []uint64 {
	type kv struct {
		key   uint64
		count int64
	}
	all := make([]kv, 0, len(gt))
	for kk, c := range gt {
		all = append(all, kv{kk, c})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].count != all[j].count {
			return all[i].count > all[j].count
		}
		return all[i].key < all[j].key
	})
	if k > len(all) {
		k = len(all)
	}
	out := make([]uint64, k)
	for i := 0; i < k; i++ {
		out[i] = all[i].key
	}
	return out
}

// ---------------------------------------------------------------------------
// Per-sketch evaluation
// ---------------------------------------------------------------------------

type record struct {
	n          int
	sketchType string
	dimLabel   string
	rows, cols int
	sketchBytes int64
	rawMin      int64
	rawReal     int64
	topKErr     float64
	insertSec   float64
}

func evalCountSketch(n int, tuples []tuple, gt map[uint64]int64, topKKeys []uint64, d dimSpec) record {
	cs, err := countsketch.NewCountSketch(d.rows, d.cols)
	if err != nil {
		log.Fatalf("NewCountSketch(%d,%d): %v", d.rows, d.cols, err)
	}

	// Pre-build SketchInputs to avoid hashing in the timed loop dominating
	// cost differences across sketches; both sketches receive the same
	// pre-hashed input.
	inputs := buildInputs(tuples)

	t0 := time.Now()
	for _, in := range inputs {
		cs.Insert(in)
	}
	insertSec := time.Since(t0).Seconds()

	b, err := cs.SerializeToBytes()
	if err != nil {
		log.Fatalf("CountSketch.SerializeToBytes: %v", err)
	}

	// Top-K estimate: query each heavy key, compare to ground truth.
	var sumRel float64
	for _, k := range topKKeys {
		est := cs.FastEstimateWithHash(common.Hash64(uint64Bytes(k)))
		actual := float64(gt[k])
		if actual == 0 {
			continue
		}
		sumRel += math.Abs(est-actual) / actual
	}
	topKErr := 0.0
	if len(topKKeys) > 0 {
		topKErr = sumRel / float64(len(topKKeys))
	}

	return record{
		n:           n,
		sketchType:  "CountSketch",
		dimLabel:    d.label,
		rows:        d.rows,
		cols:        d.cols,
		sketchBytes: int64(len(b)),
		topKErr:     topKErr,
		insertSec:   insertSec,
	}
}

func evalCountMinSketch(n int, tuples []tuple, gt map[uint64]int64, topKKeys []uint64, d dimSpec) record {
	cms, err := countminsketch.NewCountMinSketch(d.rows, d.cols)
	if err != nil {
		log.Fatalf("NewCountMinSketch(%d,%d): %v", d.rows, d.cols, err)
	}

	inputs := buildInputs(tuples)

	t0 := time.Now()
	for _, in := range inputs {
		cms.Insert(in)
	}
	insertSec := time.Since(t0).Seconds()

	b, err := cms.SerializeToBytes()
	if err != nil {
		log.Fatalf("CountMinSketch.SerializeToBytes: %v", err)
	}

	var sumRel float64
	for _, k := range topKKeys {
		est := cms.FastEstimateWithHash(common.Hash64(uint64Bytes(k)))
		actual := float64(gt[k])
		if actual == 0 {
			continue
		}
		sumRel += math.Abs(est-actual) / actual
	}
	topKErr := 0.0
	if len(topKKeys) > 0 {
		topKErr = sumRel / float64(len(topKKeys))
	}

	return record{
		n:           n,
		sketchType:  "CountMinSketch",
		dimLabel:    d.label,
		rows:        d.rows,
		cols:        d.cols,
		sketchBytes: int64(len(b)),
		topKErr:     topKErr,
		insertSec:   insertSec,
	}
}

// buildInputs converts the value field of each tuple into a *common.SketchInput
// (8-byte little-endian encoding). We key by value, not series_id, so heavy
// Zipf values get hot keys and the top-K query is meaningful.
func buildInputs(tuples []tuple) []*common.SketchInput {
	out := make([]*common.SketchInput, len(tuples))
	for i, t := range tuples {
		out[i] = common.FromU64(t.value)
	}
	return out
}

// uint64Bytes produces an 8-byte little-endian encoding identical to what
// common.FromU64 hashes, so EstimateStringCount-style queries land on the
// same cells as Insert.
func uint64Bytes(v uint64) []byte {
	var buf [8]byte
	for i := 0; i < 8; i++ {
		buf[i] = byte(v >> (8 * i))
	}
	out := make([]byte, 8)
	copy(out, buf[:])
	return out
}

// ---------------------------------------------------------------------------
// CSV / logging
// ---------------------------------------------------------------------------

func writeRow(w *csv.Writer, r record) {
	ratioMin := float64(r.sketchBytes) / float64(r.rawMin)
	ratioReal := float64(r.sketchBytes) / float64(r.rawReal)
	row := []string{
		strconv.Itoa(r.n),
		r.sketchType,
		r.dimLabel,
		strconv.Itoa(r.rows),
		strconv.Itoa(r.cols),
		strconv.FormatInt(r.sketchBytes, 10),
		strconv.FormatInt(r.rawMin, 10),
		strconv.FormatInt(r.rawReal, 10),
		strconv.FormatFloat(ratioMin, 'g', 6, 64),
		strconv.FormatFloat(ratioReal, 'g', 6, 64),
		strconv.FormatFloat(r.topKErr, 'g', 6, 64),
		strconv.FormatFloat(r.insertSec, 'g', 6, 64),
	}
	if err := w.Write(row); err != nil {
		log.Fatalf("csv write: %v", err)
	}
}

func logRow(r record) {
	ratioMin := float64(r.sketchBytes) / float64(r.rawMin)
	ratioReal := float64(r.sketchBytes) / float64(r.rawReal)
	verdict := "sketch BIGGER"
	if ratioReal < 1.0 {
		verdict = "sketch smaller"
	}
	fmt.Printf("N=%-8d %-14s %-9s rows=%d cols=%-4d sketch=%-8d rawMin=%-10d rawReal=%-10d ratioMin=%.3f ratioReal=%.3f topKerr=%.4f t=%.2fs  [%s]\n",
		r.n, r.sketchType, r.dimLabel, r.rows, r.cols,
		r.sketchBytes, r.rawMin, r.rawReal, ratioMin, ratioReal,
		r.topKErr, r.insertSec, verdict)
}
