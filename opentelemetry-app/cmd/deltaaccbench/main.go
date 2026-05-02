// deltaaccbench – Delta-transmission accuracy and payload-size benchmark.
//
// Runs entirely in-process (no collector required). For each sketch type it
// simulates a multi-window stream, emitting either a full sketch payload or a
// sparse delta on every window boundary, then reconstructs the state at the
// receiver and queries it for accuracy.
//
// Metrics reported per sketch type:
//   - full_bytes    : bytes in a full serialised sketch payload
//   - delta_bytes   : bytes in the sparse delta payload (windows ≥ 2)
//   - compression   : full_bytes / delta_bytes ratio
//   - mean_rel_err  : mean relative query error vs. ground truth
//   - max_rel_err   : worst-case relative query error
//   - correct_recon : reconstructed sketch matches reference (pass/fail)
//
// Sketch types benchmarked:
//   cms   CountMinSketch – frequency queries    (supports delta)
//   cs    CountSketch    – frequency queries    (supports delta)
//   hll   HyperLogLog    – cardinality estimate (supports delta)
//   dd    DDSketch       – quantile queries     (supports delta)
//   kll   KLL            – quantile queries     (no delta support)
//
// Aggregation modes simulated:
//   batch  : each window = one flush of independently-built sketch
//   window : each window accumulates all inserts; delta vs. running state
//
// Usage:
//
//	go run ./cmd/deltaaccbench \
//	  --windows=10 --inserts=2000 --output-dir=/tmp/acc_bench
package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"syscall"
	"time"

	"github.com/cespare/xxhash/v2"

	cms "github.com/ProjectASAP/sketchlib-go/sketches/CountMinSketch"
	cs "github.com/ProjectASAP/sketchlib-go/sketches/CountSketch"
	dd "github.com/ProjectASAP/sketchlib-go/sketches/DDSketch"
	hll "github.com/ProjectASAP/sketchlib-go/sketches/HLL"
	kll "github.com/ProjectASAP/sketchlib-go/sketches/KLL"

	"github.com/ProjectASAP/sketchlib-go/common"
)

// ---------------------------------------------------------------------------
// CLI flags
// ---------------------------------------------------------------------------

var (
	windows       = flag.Int("windows", 10, "Number of streaming windows to simulate")
	insertsPerWin = flag.Int("inserts", 2000, "Insertions per window")
	zipfS         = flag.Float64("zipf-s", 1.1, "Zipf s parameter (> 1)")
	zipfV         = flag.Float64("zipf-v", 1.0, "Zipf v parameter (>= 1)")
	zipfMax       = flag.Uint64("zipf-max", 5000, "Zipf imax (distinct key space)")
	deltaThresh   = flag.Float64("delta-threshold", 1.0, "Delta threshold for CMS/CS (minimum change to include in delta)")
	minFreq       = flag.Int("min-freq", 10, "Minimum true frequency for CMS/CS accuracy measurement (skips rare keys that inflate relative error)")
	outputDir     = flag.String("output-dir", ".", "Directory to write result files")
	sketchArg     = flag.String("sketch", "all", "Sketch type(s): cms|cs|hll|dd|kll|all (comma-separated)")
)

// ---------------------------------------------------------------------------
// Result types
// ---------------------------------------------------------------------------

type windowResult struct {
	Window       int     `json:"window"`
	Encoding     string  `json:"encoding"` // "full" or "delta"
	RawBytes     int     `json:"raw_bytes"`  // proto-packed fixed64, 8 B per sample
	FullBytes    int     `json:"full_bytes"`
	DeltaBytes   int     `json:"delta_bytes"`
	MeanRelErr   float64 `json:"mean_rel_err"`
	MaxRelErr    float64 `json:"max_rel_err"`
	CorrectRecon bool    `json:"correct_recon"`
}

type sketchResult struct {
	SketchType       string         `json:"sketch_type"`
	AggMode          string         `json:"agg_mode"` // "batch" or "window"
	DeltaEnabled     bool           `json:"delta_enabled"`
	Windows          []windowResult `json:"windows"`
	AvgRawBytes      float64        `json:"avg_raw_bytes"`      // raw sample stream bytes/window
	AvgFullBytes     float64        `json:"avg_full_bytes"`
	AvgDeltaBytes    float64        `json:"avg_delta_bytes"`
	CompressionRatio float64        `json:"compression_ratio"`  // full / delta
	RawVsFullRatio   float64        `json:"raw_vs_full_ratio"`  // raw / full (>1 means raw is larger)
	RawVsDeltaRatio  float64        `json:"raw_vs_delta_ratio"` // raw / delta (>1 means raw is larger)
	AvgMeanRelErr    float64        `json:"avg_mean_rel_err"`
	MaxMaxRelErr     float64        `json:"max_max_rel_err"`
	WallMs           float64        `json:"wall_ms"`
	CPUUserMs        float64        `json:"cpu_user_ms"`
	CPUSysMs         float64        `json:"cpu_sys_ms"`
	HeapAllocMB      float64        `json:"heap_alloc_mb"`
}

// ---------------------------------------------------------------------------
// CPU / memory helpers (same pattern as e2esdkbench)
// ---------------------------------------------------------------------------

func getRusage() (userMs, sysMs float64) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, 0
	}
	userMs = float64(ru.Utime.Sec)*1000 + float64(ru.Utime.Usec)/1000
	sysMs = float64(ru.Stime.Sec)*1000 + float64(ru.Stime.Usec)/1000
	return
}

func heapAllocMB() float64 {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return float64(ms.HeapAlloc) / (1024 * 1024)
}

// ---------------------------------------------------------------------------
// Zipf stream helpers
// ---------------------------------------------------------------------------

// buildZipfStream returns insertsPerWin key hashes sampled from Zipf(s, v, max).
func buildZipfStream(rng *rand.Rand, n int) []uint64 {
	zipf := rand.NewZipf(rng, *zipfS, *zipfV, *zipfMax)
	keys := make([]uint64, n)
	for i := range keys {
		k := fmt.Sprintf("key:%d", zipf.Uint64())
		keys[i] = xxhash.Sum64String(k)
	}
	return keys
}

// buildTrueFreqs returns the exact frequency of each distinct hash in keys.
func buildTrueFreqs(keys []uint64) map[uint64]int {
	m := make(map[uint64]int, len(keys)/2)
	for _, h := range keys {
		m[h]++
	}
	return m
}

// relErr returns |est - true| / max(true, 1).
func relErr(est, truth float64) float64 {
	return math.Abs(est-truth) / math.Max(truth, 1.0)
}

// ---------------------------------------------------------------------------
// CMS benchmark
// ---------------------------------------------------------------------------

func benchCMS(mode string, deltaOn bool) sketchResult {
	rng := rand.New(rand.NewSource(42))
	result := sketchResult{
		SketchType:   "cms",
		AggMode:      mode,
		DeltaEnabled: deltaOn,
	}

	wallStart := time.Now()
	cpuU0, cpuS0 := getRusage()

	// Match the countminsketchprocessor config: rows=5, columns≈2000.
	// CMS requires power-of-2 cols for bitmasking; use 2048.
	const rows, cols = 5, 2048

	// Receiver state for delta reconstruction.
	var recvSnap *cms.CountMinSketch // last full state known to receiver

	// Processor state for window accumulation (window mode only).
	var winSketch *cms.CountMinSketch
	if mode == "window" {
		s, err := cms.NewCountMinSketch(rows, cols)
		if err != nil {
			log.Fatalf("CMS window init: %v", err)
		}
		winSketch = s
	}

	// Processor snapshot for delta computation.
	var procSnap *cms.CountMinSketch

	var totalFullBytes, totalDeltaBytes int
	var deltaCount int

	// For window mode, track cumulative true frequencies so accuracy is measured
	// against the full accumulated state, not just the latest window slice.
	cumFreqs := make(map[uint64]int)

	for w := 0; w < *windows; w++ {
		keys := buildZipfStream(rng, *insertsPerWin)
		windowFreqs := buildTrueFreqs(keys)

		// Build current window sketch.
		var cur *cms.CountMinSketch
		if mode == "batch" {
			s, err := cms.NewCountMinSketch(rows, cols)
			if err != nil {
				log.Fatalf("CMS batch init: %v", err)
			}
			for _, h := range keys {
				s.InsertWithHash(h)
			}
			cur = s
		} else { // window: accumulate
			for _, h := range keys {
				winSketch.InsertWithHash(h)
			}
			cur = cloneCMS(winSketch)
		}

		// Ground truth: per-window in batch mode, cumulative in window mode.
		var queryFreqs map[uint64]int
		if mode == "batch" {
			queryFreqs = windowFreqs
		} else {
			for h, f := range windowFreqs {
				cumFreqs[h] += f
			}
			queryFreqs = cumFreqs
		}

		// Serialize full payload.
		fullBytes, err := cur.SerializeProtoBytes()
		if err != nil {
			log.Fatalf("CMS serialize: %v", err)
		}
		totalFullBytes += len(fullBytes)

		// Compute transmission payload.
		var transmitBytes []byte
		encoding := "full"
		var reconSketch *cms.CountMinSketch

		if deltaOn && procSnap != nil {
			// Compute delta against previous processor snapshot.
			deltaMsg, err := cms.ComputeDelta(procSnap, cur, *deltaThresh)
			if err != nil {
				log.Fatalf("CMS ComputeDelta: %v", err)
			}
			deltaBytes, err := cms.SerializeDelta(deltaMsg)
			if err != nil {
				log.Fatalf("CMS SerializeDelta: %v", err)
			}
			transmitBytes = deltaBytes
			totalDeltaBytes += len(deltaBytes)
			deltaCount++
			encoding = "delta"

			// Receiver reconstructs: apply delta onto its last snapshot.
			reconSketch = cloneCMS(recvSnap)
			if reconSketch == nil {
				log.Fatalf("CMS: receiver has no snapshot but delta was sent")
			}
			dm, err := cms.DeserializeDelta(transmitBytes)
			if err != nil {
				log.Fatalf("CMS DeserializeDelta: %v", err)
			}
			cms.ApplyDelta(reconSketch, dm)
		} else {
			transmitBytes = fullBytes
			encoding = "full"
			// Receiver gets full sketch.
			reconSketch, err = cms.DeserializeCountMinSketchFromProtoBytes(transmitBytes)
			if err != nil {
				log.Fatalf("CMS DeserializeFromProtoBytes: %v", err)
			}
		}

		// Update snapshots.
		procSnap = cloneCMS(cur)
		recvSnap = cloneCMS(reconSketch)

		// Measure query accuracy on the reconstructed sketch.
		// Only count keys above minFreq — CMS is an upper-bound estimator designed
		// for heavy hitters; rare keys (count ≈ 1) always show high relative error
		// due to the additive ε·N error bound.
		var sumRel, maxRel float64
		var errCount int
		for h, trueF := range queryFreqs {
			if trueF < *minFreq {
				continue
			}
			est, err := reconSketch.QueryWithHash(common.QueryFrequency, h)
			if err != nil {
				continue
			}
			re := relErr(est, float64(trueF))
			sumRel += re
			errCount++
			if re > maxRel {
				maxRel = re
			}
		}
		meanRel := sumRel / math.Max(1, float64(errCount))

		// Verify reconstruction matches reference (direct full sketch).
		refSketch, _ := cms.DeserializeCountMinSketchFromProtoBytes(fullBytes)
		correctRecon := cmsSketchesEqual(refSketch, reconSketch)

		wr := windowResult{
			Window:       w,
			Encoding:     encoding,
			RawBytes:     *insertsPerWin * 8,
			FullBytes:    len(fullBytes),
			DeltaBytes:   len(transmitBytes),
			MeanRelErr:   meanRel,
			MaxRelErr:    maxRel,
			CorrectRecon: correctRecon,
		}
		result.Windows = append(result.Windows, wr)
	}

	// Summary stats.
	result.AvgFullBytes = float64(totalFullBytes) / float64(*windows)
	if deltaCount > 0 {
		result.AvgDeltaBytes = float64(totalDeltaBytes) / float64(deltaCount)
		result.CompressionRatio = result.AvgFullBytes / result.AvgDeltaBytes
	}
	var sumMeanRel, maxMaxRel float64
	for _, wr := range result.Windows {
		sumMeanRel += wr.MeanRelErr
		if wr.MaxRelErr > maxMaxRel {
			maxMaxRel = wr.MaxRelErr
		}
	}
	result.AvgMeanRelErr = sumMeanRel / float64(len(result.Windows))
	result.MaxMaxRelErr = maxMaxRel
	setRawStats(&result)

	result.WallMs = float64(time.Since(wallStart).Milliseconds())
	cpuU1, cpuS1 := getRusage()
	result.CPUUserMs = cpuU1 - cpuU0
	result.CPUSysMs = cpuS1 - cpuS0
	result.HeapAllocMB = heapAllocMB()

	return result
}

func cloneCMS(s *cms.CountMinSketch) *cms.CountMinSketch {
	if s == nil {
		return nil
	}
	data, err := s.SerializeProtoBytes()
	if err != nil {
		return nil
	}
	c, err := cms.DeserializeCountMinSketchFromProtoBytes(data)
	if err != nil {
		return nil
	}
	return c
}

func cmsSketchesEqual(a, b *cms.CountMinSketch) bool {
	if a.Rows != b.Rows || a.Cols != b.Cols {
		return false
	}
	for r := 0; r < a.Rows; r++ {
		for c := 0; c < a.Cols; c++ {
			if a.Count[r][c] != b.Count[r][c] {
				return false
			}
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// CountSketch benchmark
// ---------------------------------------------------------------------------

func benchCS(mode string, deltaOn bool) sketchResult {
	rng := rand.New(rand.NewSource(42))
	result := sketchResult{
		SketchType:   "cs",
		AggMode:      mode,
		DeltaEnabled: deltaOn,
	}

	wallStart := time.Now()
	cpuU0, cpuS0 := getRusage()

	// epsilon = relative error bound, confidence = 99% → failure prob = 0.01
	const epsilon, confidence = 0.01, 0.99
	const failureProb = 1.0 - confidence // 0.01 → rows = ceil(log(1/0.01)) = 5

	newCS := func() *cs.CountSketch {
		s, err := cs.NewCountSketch(computeCSRows(failureProb), computeCSCols(epsilon))
		if err != nil {
			log.Fatalf("CS init: %v", err)
		}
		return s
	}

	var recvSnap *cs.CountSketch
	var winSketch *cs.CountSketch
	if mode == "window" {
		winSketch = newCS()
	}
	var procSnap *cs.CountSketch

	var totalFullBytes, totalDeltaBytes int
	var deltaCount int

	// For window mode, track cumulative true frequencies so accuracy is measured
	// against the full accumulated state, not just the latest window slice.
	cumFreqs := make(map[uint64]int)

	for w := 0; w < *windows; w++ {
		keys := buildZipfStream(rng, *insertsPerWin)
		windowFreqs := buildTrueFreqs(keys)

		var cur *cs.CountSketch
		if mode == "batch" {
			cur = newCS()
			for _, h := range keys {
				cur.InsertWithHash(h)
			}
		} else {
			for _, h := range keys {
				winSketch.InsertWithHash(h)
			}
			cur = cloneCS(winSketch)
		}

		// Ground truth: per-window in batch mode, cumulative in window mode.
		var queryFreqs map[uint64]int
		if mode == "batch" {
			queryFreqs = windowFreqs
		} else {
			for h, f := range windowFreqs {
				cumFreqs[h] += f
			}
			queryFreqs = cumFreqs
		}

		fullBytes, err := cur.SerializeProtoBytes()
		if err != nil {
			log.Fatalf("CS serialize: %v", err)
		}
		totalFullBytes += len(fullBytes)

		var transmitBytes []byte
		encoding := "full"
		var reconSketch *cs.CountSketch

		if deltaOn && procSnap != nil {
			deltaMsg, err := cs.ComputeDelta(procSnap, cur, *deltaThresh)
			if err != nil {
				log.Fatalf("CS ComputeDelta: %v", err)
			}
			dBytes, err := cs.SerializeDelta(deltaMsg)
			if err != nil {
				log.Fatalf("CS SerializeDelta: %v", err)
			}
			transmitBytes = dBytes
			totalDeltaBytes += len(dBytes)
			deltaCount++
			encoding = "delta"

			reconSketch = cloneCS(recvSnap)
			dm, err := cs.DeserializeDelta(transmitBytes)
			if err != nil {
				log.Fatalf("CS DeserializeDelta: %v", err)
			}
			cs.ApplyDelta(reconSketch, dm)
		} else {
			transmitBytes = fullBytes
			encoding = "full"
			reconSketch, err = cs.DeserializeCountSketchFromProtoBytes(transmitBytes)
			if err != nil {
				log.Fatalf("CS Deserialize: %v", err)
			}
		}

		procSnap = cloneCS(cur)
		recvSnap = cloneCS(reconSketch)

		// Only measure accuracy for keys above minFreq (same rationale as CMS).
		var sumRel, maxRel float64
		var errCount int
		for h, trueF := range queryFreqs {
			if trueF < *minFreq {
				continue
			}
			est, err := reconSketch.QueryWithHash(common.QueryFrequency, h)
			if err != nil {
				continue
			}
			re := relErr(est, float64(trueF))
			sumRel += re
			errCount++
			if re > maxRel {
				maxRel = re
			}
		}
		meanRel := sumRel / math.Max(1, float64(errCount))

		refSketch, _ := cs.DeserializeCountSketchFromProtoBytes(fullBytes)
		correctRecon := csSketchesEqual(refSketch, reconSketch)

		result.Windows = append(result.Windows, windowResult{
			Window:       w,
			Encoding:     encoding,
			RawBytes:     *insertsPerWin * 8,
			FullBytes:    len(fullBytes),
			DeltaBytes:   len(transmitBytes),
			MeanRelErr:   meanRel,
			MaxRelErr:    maxRel,
			CorrectRecon: correctRecon,
		})
	}

	result.AvgFullBytes = float64(totalFullBytes) / float64(*windows)
	if deltaCount > 0 {
		result.AvgDeltaBytes = float64(totalDeltaBytes) / float64(deltaCount)
		result.CompressionRatio = result.AvgFullBytes / result.AvgDeltaBytes
	}
	summarizeResult(&result)
	setRawStats(&result)

	result.WallMs = float64(time.Since(wallStart).Milliseconds())
	cpuU1, cpuS1 := getRusage()
	result.CPUUserMs = cpuU1 - cpuU0
	result.CPUSysMs = cpuS1 - cpuS0
	result.HeapAllocMB = heapAllocMB()

	return result
}

func cloneCS(s *cs.CountSketch) *cs.CountSketch {
	if s == nil {
		return nil
	}
	data, err := s.SerializeProtoBytes()
	if err != nil {
		return nil
	}
	c, err := cs.DeserializeCountSketchFromProtoBytes(data)
	if err != nil {
		return nil
	}
	return c
}

func csSketchesEqual(a, b *cs.CountSketch) bool {
	if a.Rows != b.Rows || a.Cols != b.Cols {
		return false
	}
	for r := 0; r < a.Rows; r++ {
		for c := 0; c < a.Cols; c++ {
			if a.Count[r][c] != b.Count[r][c] {
				return false
			}
		}
	}
	return true
}

func computeCSRows(d float64) int {
	rows := int(math.Ceil(math.Log(1 / d)))
	if rows < 1 {
		rows = 1
	}
	return rows
}

func computeCSCols(eps float64) int {
	cols := int(math.Ceil(1 / (eps * eps)))
	if cols < 2 {
		cols = 2
	}
	// next power of two
	p := 1
	for p < cols {
		p <<= 1
	}
	return p
}

// ---------------------------------------------------------------------------
// HLL benchmark
// ---------------------------------------------------------------------------

func benchHLL(mode string, deltaOn bool) sketchResult {
	// Note: HLL delta transmission only works in window mode; in batch mode
	// each window is independent so delta is not meaningful.
	if mode == "batch" && deltaOn {
		deltaOn = false // silently downgrade — no delta in batch HLL
	}

	rng := rand.New(rand.NewSource(42))
	result := sketchResult{
		SketchType:   "hll",
		AggMode:      mode,
		DeltaEnabled: deltaOn,
	}

	wallStart := time.Now()
	cpuU0, cpuS0 := getRusage()

	var winSketch *hll.HyperLogLog
	if mode == "window" {
		winSketch = hll.NewHyperLogLog()
	}
	var procSnap *hll.HyperLogLog
	var recvSnap *hll.HyperLogLog

	var totalFullBytes, totalDeltaBytes int
	var deltaCount int

	// Track true distinct count across all windows (window mode: cumulative).
	seenValues := make(map[float64]struct{}, *windows**insertsPerWin)

	for w := 0; w < *windows; w++ {
		// Generate distinct float64 values (using integer IDs shifted per window).
		vals := make([]float64, *insertsPerWin)
		for i := range vals {
			v := float64(rng.Intn(int(*zipfMax)) + w*int(*zipfMax))
			vals[i] = v
		}

		var cur *hll.HyperLogLog
		if mode == "batch" {
			cur = hll.NewHyperLogLog()
			for _, v := range vals {
				cur.UpdateValue(v)
				seenValues[v] = struct{}{}
			}
		} else {
			for _, v := range vals {
				winSketch.UpdateValue(v)
				seenValues[v] = struct{}{}
			}
			cur = cloneHLL(winSketch)
		}
		trueCard := len(seenValues)
		if mode == "batch" {
			// Batch mode: each window is independent.
			batchSeen := make(map[float64]struct{}, len(vals))
			for _, v := range vals {
				batchSeen[v] = struct{}{}
			}
			trueCard = len(batchSeen)
		}

		// Serialize full.
		fullBytes, err := cur.SerializeProtoBytes()
		if err != nil {
			log.Fatalf("HLL serialize: %v", err)
		}
		totalFullBytes += len(fullBytes)

		var transmitBytes []byte
		encoding := "full"
		var reconSketch *hll.HyperLogLog

		if deltaOn && procSnap != nil {
			deltaMsg := hll.ComputeRegisterDelta(procSnap, cur)
			dBytes, err := hll.SerializeRegisterDelta(deltaMsg)
			if err != nil {
				log.Fatalf("HLL SerializeRegisterDelta: %v", err)
			}
			transmitBytes = dBytes
			totalDeltaBytes += len(dBytes)
			deltaCount++
			encoding = "delta"

			reconSketch = cloneHLL(recvSnap)
			dm, err := hll.DeserializeRegisterDelta(transmitBytes)
			if err != nil {
				log.Fatalf("HLL DeserializeRegisterDelta: %v", err)
			}
			hll.ApplyRegisterDelta(reconSketch, dm)
		} else {
			transmitBytes = fullBytes
			encoding = "full"
			reconSketch, err = hll.DeserializeHyperLogLogFromProtoBytes(transmitBytes)
			if err != nil {
				log.Fatalf("HLL Deserialize: %v", err)
			}
		}

		procSnap = cloneHLL(cur)
		recvSnap = cloneHLL(reconSketch)

		// Accuracy: cardinality relative error.
		estCard := float64(reconSketch.Estimate())
		re := relErr(estCard, float64(trueCard))

		// Verify reconstruction.
		refSketch, _ := hll.DeserializeHyperLogLogFromProtoBytes(fullBytes)
		correctRecon := hllRegistersEqual(refSketch, reconSketch)

		result.Windows = append(result.Windows, windowResult{
			Window:       w,
			Encoding:     encoding,
			RawBytes:     *insertsPerWin * 8,
			FullBytes:    len(fullBytes),
			DeltaBytes:   len(transmitBytes),
			MeanRelErr:   re,
			MaxRelErr:    re,
			CorrectRecon: correctRecon,
		})
	}

	result.AvgFullBytes = float64(totalFullBytes) / float64(*windows)
	if deltaCount > 0 {
		result.AvgDeltaBytes = float64(totalDeltaBytes) / float64(deltaCount)
		result.CompressionRatio = result.AvgFullBytes / result.AvgDeltaBytes
	}
	summarizeResult(&result)
	setRawStats(&result)

	result.WallMs = float64(time.Since(wallStart).Milliseconds())
	cpuU1, cpuS1 := getRusage()
	result.CPUUserMs = cpuU1 - cpuU0
	result.CPUSysMs = cpuS1 - cpuS0
	result.HeapAllocMB = heapAllocMB()

	return result
}

func cloneHLL(h *hll.HyperLogLog) *hll.HyperLogLog {
	if h == nil {
		return nil
	}
	data, err := h.SerializeProtoBytes()
	if err != nil {
		return nil
	}
	c, err := hll.DeserializeHyperLogLogFromProtoBytes(data)
	if err != nil {
		return nil
	}
	return c
}

func hllRegistersEqual(a, b *hll.HyperLogLog) bool {
	ar := a.RegisterSlice()
	br := b.RegisterSlice()
	if len(ar) != len(br) {
		return false
	}
	for i := range ar {
		if ar[i] != br[i] {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// DDSketch benchmark
// ---------------------------------------------------------------------------

func benchDD(mode string, deltaOn bool) sketchResult {
	// DDSketch SerializeToBytes already omits empty buckets — the dense store
	// only spans [min_occupied_index, max_occupied_index].  In batch mode each
	// window is an independent sketch from scratch, so the "full" is already as
	// compact as any delta could be.  Delta is only meaningful in window
	// (accumulation) mode where the full sketch grows over time.
	if mode == "batch" {
		deltaOn = false
	}

	rng := rand.New(rand.NewSource(42))
	result := sketchResult{
		SketchType:   "dd",
		AggMode:      mode,
		DeltaEnabled: deltaOn,
	}

	wallStart := time.Now()
	cpuU0, cpuS0 := getRusage()

	const alpha = 0.01
	quantiles := []float64{0.5, 0.9, 0.99}

	var winSketch *dd.DDSketch
	if mode == "window" {
		winSketch = dd.New(alpha)
	}
	var procSnap *dd.DDSketch
	var recvSnap *dd.DDSketch // receiver's last reconstructed sketch

	var totalFullBytes, totalDeltaBytes int
	var deltaCount int

	// Track all values ever inserted (window mode: cumulative for accuracy).
	var allValues []float64

	for w := 0; w < *windows; w++ {
		vals := make([]float64, *insertsPerWin)
		for i := range vals {
			vals[i] = float64(rng.Intn(int(*zipfMax)) + 1)
		}

		var cur *dd.DDSketch
		if mode == "batch" {
			cur = dd.New(alpha)
			for _, v := range vals {
				cur.Update(v)
			}
		} else {
			for _, v := range vals {
				winSketch.Update(v)
			}
			cur = winSketch.Clone()
		}

		trueVals := vals
		if mode == "window" {
			allValues = append(allValues, vals...)
			trueVals = allValues
		}

		// Serialize full.
		fullBytes, err := cur.SerializeToBytes()
		if err != nil {
			log.Fatalf("DD serialize: %v", err)
		}
		totalFullBytes += len(fullBytes)

		var transmitBytes []byte
		encoding := "full"
		var reconSketch *dd.DDSketch

		if deltaOn && procSnap != nil {
			dBytes, err := dd.ComputeDelta(procSnap, cur, 1)
			if err != nil {
				log.Fatalf("DD ComputeDelta: %v", err)
			}

			// Size-based fallback: send full when delta >= full.
			// DDSketch full payloads are already compact; when the delta is larger
			// (e.g. many buckets changed) the full is a better choice.
			if len(dBytes) < len(fullBytes) {
				transmitBytes = dBytes
				totalDeltaBytes += len(dBytes)
				deltaCount++
				encoding = "delta"

				// Receiver reconstructs by applying the bidirectional delta onto
				// its last known state. The new ComputeDelta includes both increases
				// and decreases, so correct_recon=true in all modes.
				reconSnap := recvSnap.Clone()
				if err := dd.ApplyDelta(reconSnap, transmitBytes); err != nil {
					log.Fatalf("DD ApplyDelta: %v", err)
				}
				reconSketch = reconSnap
			} else {
				// Delta is larger than full — fall back to full transmission.
				transmitBytes = fullBytes
				encoding = "full"
				reconSketch, err = dd.DeserializeDDSketchFromBytes(transmitBytes)
				if err != nil {
					log.Fatalf("DD Deserialize (fallback): %v", err)
				}
			}
		} else {
			transmitBytes = fullBytes
			encoding = "full"
			var err error
			reconSketch, err = dd.DeserializeDDSketchFromBytes(transmitBytes)
			if err != nil {
				log.Fatalf("DD Deserialize: %v", err)
			}
		}

		procSnap = cur.Clone()
		recvSnap = reconSketch.Clone()

		// Accuracy: quantile relative error.
		sortedVals := make([]float64, len(trueVals))
		copy(sortedVals, trueVals)
		sort.Float64s(sortedVals)

		var sumRel, maxRel float64
		for _, q := range quantiles {
			trueQ := trueQuantile(sortedVals, q)
			estQ, ok := reconSketch.Quantile(q)
			if !ok || trueQ == 0 {
				continue
			}
			re := relErr(estQ, trueQ)
			sumRel += re
			if re > maxRel {
				maxRel = re
			}
		}
		meanRel := sumRel / float64(len(quantiles))

		// Verify: re-deserialize full and compare quantiles.
		refSketch, _ := dd.DeserializeDDSketchFromBytes(fullBytes)
		correctRecon := ddSketchesCompatible(refSketch, reconSketch, quantiles)

		result.Windows = append(result.Windows, windowResult{
			Window:       w,
			Encoding:     encoding,
			RawBytes:     *insertsPerWin * 8,
			FullBytes:    len(fullBytes),
			DeltaBytes:   len(transmitBytes),
			MeanRelErr:   meanRel,
			MaxRelErr:    maxRel,
			CorrectRecon: correctRecon,
		})
	}

	result.AvgFullBytes = float64(totalFullBytes) / float64(*windows)
	if deltaCount > 0 {
		result.AvgDeltaBytes = float64(totalDeltaBytes) / float64(deltaCount)
		result.CompressionRatio = result.AvgFullBytes / result.AvgDeltaBytes
	}
	summarizeResult(&result)
	setRawStats(&result)

	result.WallMs = float64(time.Since(wallStart).Milliseconds())
	cpuU1, cpuS1 := getRusage()
	result.CPUUserMs = cpuU1 - cpuU0
	result.CPUSysMs = cpuS1 - cpuS0
	result.HeapAllocMB = heapAllocMB()

	return result
}

func ddSketchesCompatible(a, b *dd.DDSketch, quantiles []float64) bool {
	if a.GetCount() != b.GetCount() {
		return false
	}
	for _, q := range quantiles {
		av, aok := a.Quantile(q)
		bv, bok := b.Quantile(q)
		if aok != bok {
			return false
		}
		if aok && math.Abs(av-bv) > 1e-9 {
			return false
		}
	}
	return true
}

// trueQuantile returns the exact q-th quantile from sorted slice.
func trueQuantile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(q * float64(len(sorted)-1))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// ---------------------------------------------------------------------------
// KLL benchmark (no delta support)
// ---------------------------------------------------------------------------

func benchKLL(mode string) sketchResult {
	rng := rand.New(rand.NewSource(42))
	result := sketchResult{
		SketchType:   "kll",
		AggMode:      mode,
		DeltaEnabled: false,
	}

	wallStart := time.Now()
	cpuU0, cpuS0 := getRusage()

	const k = 256
	quantiles := []float64{0.5, 0.9, 0.99}

	var winSketch *kll.KLLSketch
	if mode == "window" {
		winSketch = kll.InitKLL(k)
	}

	var allValues []float64

	for w := 0; w < *windows; w++ {
		vals := make([]float64, *insertsPerWin)
		for i := range vals {
			vals[i] = float64(rng.Intn(int(*zipfMax)) + 1)
		}

		var cur *kll.KLLSketch
		if mode == "batch" {
			cur = kll.InitKLL(k)
			for _, v := range vals {
				cur.Update(v)
			}
		} else {
			for _, v := range vals {
				winSketch.Update(v)
			}
			// KLL doesn't have Clone; serialize/deserialize for snapshot.
			curBytes, err := winSketch.SerializeProtoBytes()
			if err != nil {
				log.Fatalf("KLL serialize: %v", err)
			}
			cur, err = kll.DeserializeKLLSketchFromProtoBytes(curBytes)
			if err != nil {
				log.Fatalf("KLL deserialize: %v", err)
			}
		}

		trueVals := vals
		if mode == "window" {
			allValues = append(allValues, vals...)
			trueVals = allValues
		}

		fullBytes, err := cur.SerializeProtoBytes()
		if err != nil {
			log.Fatalf("KLL serialize full: %v", err)
		}

		// No delta — always full.
		reconSketch, err := kll.DeserializeKLLSketchFromProtoBytes(fullBytes)
		if err != nil {
			log.Fatalf("KLL deserialize recon: %v", err)
		}

		sortedVals := make([]float64, len(trueVals))
		copy(sortedVals, trueVals)
		sort.Float64s(sortedVals)

		var sumRel, maxRel float64
		for _, q := range quantiles {
			trueQ := trueQuantile(sortedVals, q)
			estQ := reconSketch.Quantile(q)
			if trueQ == 0 {
				continue
			}
			re := relErr(estQ, trueQ)
			sumRel += re
			if re > maxRel {
				maxRel = re
			}
		}
		meanRel := sumRel / float64(len(quantiles))

		result.Windows = append(result.Windows, windowResult{
			Window:       w,
			Encoding:     "full",
			RawBytes:     *insertsPerWin * 8,
			FullBytes:    len(fullBytes),
			DeltaBytes:   len(fullBytes),
			MeanRelErr:   meanRel,
			MaxRelErr:    maxRel,
			CorrectRecon: true,
		})
	}

	var totalFull int
	for _, wr := range result.Windows {
		totalFull += wr.FullBytes
	}
	result.AvgFullBytes = float64(totalFull) / float64(*windows)
	result.AvgDeltaBytes = result.AvgFullBytes
	summarizeResult(&result)
	setRawStats(&result)

	result.WallMs = float64(time.Since(wallStart).Milliseconds())
	cpuU1, cpuS1 := getRusage()
	result.CPUUserMs = cpuU1 - cpuU0
	result.CPUSysMs = cpuS1 - cpuS0
	result.HeapAllocMB = heapAllocMB()

	return result
}

// ---------------------------------------------------------------------------
// Summarise windows → aggregate stats
// ---------------------------------------------------------------------------

func summarizeResult(r *sketchResult) {
	var sumMean, maxMax float64
	for _, wr := range r.Windows {
		sumMean += wr.MeanRelErr
		if wr.MaxRelErr > maxMax {
			maxMax = wr.MaxRelErr
		}
	}
	r.AvgMeanRelErr = sumMean / math.Max(1, float64(len(r.Windows)))
	r.MaxMaxRelErr = maxMax
}

// setRawStats populates raw-sample bandwidth fields after AvgFullBytes /
// AvgDeltaBytes have been set. Raw bytes = proto-packed fixed64: 8 B per sample.
func setRawStats(r *sketchResult) {
	r.AvgRawBytes = float64(*insertsPerWin * 8)
	if r.AvgFullBytes > 0 {
		r.RawVsFullRatio = r.AvgRawBytes / r.AvgFullBytes
	}
	if r.AvgDeltaBytes > 0 {
		r.RawVsDeltaRatio = r.AvgRawBytes / r.AvgDeltaBytes
	}
}

// ---------------------------------------------------------------------------
// Output helpers
// ---------------------------------------------------------------------------

func writeJSON(path string, v interface{}) {
	f, err := os.Create(path)
	if err != nil {
		log.Printf("warn: cannot create %s: %v", path, err)
		return
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeCSV(path string, results []sketchResult) {
	f, err := os.Create(path)
	if err != nil {
		log.Printf("warn: cannot create %s: %v", path, err)
		return
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	_ = w.Write([]string{
		"sketch_type", "agg_mode", "delta_enabled",
		"avg_raw_bytes", "avg_full_bytes", "avg_delta_bytes",
		"compression_ratio", "raw_vs_full_ratio", "raw_vs_delta_ratio",
		"avg_mean_rel_err", "max_max_rel_err",
		"wall_ms", "cpu_user_ms", "cpu_sys_ms", "heap_alloc_mb",
	})
	for _, r := range results {
		_ = w.Write([]string{
			r.SketchType, r.AggMode, fmt.Sprintf("%t", r.DeltaEnabled),
			fmt.Sprintf("%.1f", r.AvgRawBytes),
			fmt.Sprintf("%.1f", r.AvgFullBytes),
			fmt.Sprintf("%.1f", r.AvgDeltaBytes),
			fmt.Sprintf("%.3f", r.CompressionRatio),
			fmt.Sprintf("%.3f", r.RawVsFullRatio),
			fmt.Sprintf("%.3f", r.RawVsDeltaRatio),
			fmt.Sprintf("%.6f", r.AvgMeanRelErr),
			fmt.Sprintf("%.6f", r.MaxMaxRelErr),
			fmt.Sprintf("%.1f", r.WallMs),
			fmt.Sprintf("%.1f", r.CPUUserMs),
			fmt.Sprintf("%.1f", r.CPUSysMs),
			fmt.Sprintf("%.3f", r.HeapAllocMB),
		})
	}
}

func printTable(results []sketchResult) {
	fmt.Printf("\n%-8s %-7s %-6s  %10s  %10s  %10s  %11s  %9s  %9s  %12s  %12s\n",
		"Sketch", "Mode", "Delta",
		"Raw B avg", "Full B avg", "Delta B avg", "Full/Delta",
		"Raw/Full", "Raw/Delta",
		"MeanRelErr", "MaxRelErr")
	fmt.Println(string(make([]byte, 120)))
	for _, r := range results {
		deltaStr := "off"
		if r.DeltaEnabled {
			deltaStr = "on"
		}
		comp := "-"
		if r.CompressionRatio > 0 {
			comp = fmt.Sprintf("%.2fx", r.CompressionRatio)
		}
		rvf := "-"
		if r.RawVsFullRatio > 0 {
			rvf = fmt.Sprintf("%.2fx", r.RawVsFullRatio)
		}
		rvd := "-"
		if r.RawVsDeltaRatio > 0 {
			rvd = fmt.Sprintf("%.2fx", r.RawVsDeltaRatio)
		}
		fmt.Printf("%-8s %-7s %-6s  %10.0f  %10.0f  %10.0f  %11s  %9s  %9s  %12.4f%%  %12.4f%%\n",
			r.SketchType, r.AggMode, deltaStr,
			r.AvgRawBytes, r.AvgFullBytes, r.AvgDeltaBytes, comp,
			rvf, rvd,
			r.AvgMeanRelErr*100, r.MaxMaxRelErr*100)
	}
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	flag.Parse()

	if err := os.MkdirAll(*outputDir, 0o755); err != nil {
		log.Fatalf("cannot create output-dir: %v", err)
	}

	sketches := parseSketchArg(*sketchArg)
	modes := []string{"batch", "window"}

	fmt.Printf("=== Delta Accuracy Benchmark ===\n")
	fmt.Printf("Windows:         %d\n", *windows)
	fmt.Printf("Inserts/window:  %d\n", *insertsPerWin)
	fmt.Printf("Zipf(s=%.2f, v=%.2f, max=%d)\n", *zipfS, *zipfV, *zipfMax)
	fmt.Printf("Delta threshold: %.1f\n", *deltaThresh)
	fmt.Printf("Min freq (CMS/CS): %d\n", *minFreq)
	fmt.Printf("Sketches:        %v\n", sketches)
	fmt.Printf("Output dir:      %s\n\n", *outputDir)

	var allResults []sketchResult

	for _, sk := range sketches {
		for _, mode := range modes {
			switch sk {
			case "cms":
				fmt.Printf("Running CMS  mode=%-6s delta=off ...\n", mode)
				r := benchCMS(mode, false)
				allResults = append(allResults, r)
				fmt.Printf("  avg_full=%.0fB  mean_rel_err=%.4f%%\n", r.AvgFullBytes, r.AvgMeanRelErr*100)

				fmt.Printf("Running CMS  mode=%-6s delta=on  ...\n", mode)
				r2 := benchCMS(mode, true)
				allResults = append(allResults, r2)
				fmt.Printf("  avg_delta=%.0fB  compression=%.2fx  mean_rel_err=%.4f%%\n",
					r2.AvgDeltaBytes, r2.CompressionRatio, r2.AvgMeanRelErr*100)

			case "cs":
				fmt.Printf("Running CS   mode=%-6s delta=off ...\n", mode)
				r := benchCS(mode, false)
				allResults = append(allResults, r)
				fmt.Printf("  avg_full=%.0fB  mean_rel_err=%.4f%%\n", r.AvgFullBytes, r.AvgMeanRelErr*100)

				fmt.Printf("Running CS   mode=%-6s delta=on  ...\n", mode)
				r2 := benchCS(mode, true)
				allResults = append(allResults, r2)
				fmt.Printf("  avg_delta=%.0fB  compression=%.2fx  mean_rel_err=%.4f%%\n",
					r2.AvgDeltaBytes, r2.CompressionRatio, r2.AvgMeanRelErr*100)

			case "hll":
				fmt.Printf("Running HLL  mode=%-6s delta=off ...\n", mode)
				r := benchHLL(mode, false)
				allResults = append(allResults, r)
				fmt.Printf("  avg_full=%.0fB  mean_rel_err=%.4f%%\n", r.AvgFullBytes, r.AvgMeanRelErr*100)

				if mode == "window" {
					fmt.Printf("Running HLL  mode=%-6s delta=on  ...\n", mode)
					r2 := benchHLL(mode, true)
					allResults = append(allResults, r2)
					fmt.Printf("  avg_delta=%.0fB  compression=%.2fx  mean_rel_err=%.4f%%\n",
						r2.AvgDeltaBytes, r2.CompressionRatio, r2.AvgMeanRelErr*100)
				}

			case "dd":
				fmt.Printf("Running DD   mode=%-6s delta=N/A ...\n", mode)
				r := benchDD(mode, false)
				allResults = append(allResults, r)
				fmt.Printf("  avg_full=%.0fB  mean_rel_err=%.4f%%\n", r.AvgFullBytes, r.AvgMeanRelErr*100)

				if mode == "window" {
					// Batch full is already maximally compact (dense array omits
					// empty buckets beyond the occupied range); delta only helps
					// in window mode where the accumulated sketch grows over time.
					fmt.Printf("Running DD   mode=%-6s delta=on  ...\n", mode)
					r2 := benchDD(mode, true)
					allResults = append(allResults, r2)
					fmt.Printf("  avg_delta=%.0fB  compression=%.2fx  mean_rel_err=%.4f%%\n",
						r2.AvgDeltaBytes, r2.CompressionRatio, r2.AvgMeanRelErr*100)
				}

			case "kll":
				fmt.Printf("Running KLL  mode=%-6s delta=N/A ...\n", mode)
				r := benchKLL(mode)
				allResults = append(allResults, r)
				fmt.Printf("  avg_full=%.0fB  mean_rel_err=%.4f%%\n", r.AvgFullBytes, r.AvgMeanRelErr*100)
			}
		}
	}

	printTable(allResults)

	// Write outputs.
	csvPath := filepath.Join(*outputDir, "delta_acc_summary.csv")
	jsonPath := filepath.Join(*outputDir, "delta_acc_summary.json")
	writeCSV(csvPath, allResults)
	writeJSON(jsonPath, allResults)

	fmt.Printf("\nCSV: %s\n", csvPath)
	fmt.Printf("JSON: %s\n", jsonPath)
}

func parseSketchArg(s string) []string {
	all := []string{"cms", "cs", "hll", "dd", "kll"}
	if s == "all" {
		return all
	}
	var out []string
	for _, tok := range splitComma(s) {
		for _, v := range all {
			if tok == v {
				out = append(out, v)
				break
			}
		}
	}
	return out
}

func splitComma(s string) []string {
	var out []string
	cur := ""
	for _, c := range s {
		if c == ',' {
			if cur != "" {
				out = append(out, cur)
			}
			cur = ""
		} else {
			cur += string(c)
		}
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}
