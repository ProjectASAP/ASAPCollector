package intchunk

import (
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"
)

// compare_table_test.go: one definitive table answering "does the VM-style INT
// FOR+delta cold codec save CPU / memory / bandwidth vs plain Gorilla-XOR?".
// For every series it blocks into benchBlockSize chunks and, per codec, reports
// encode + decode ns/sample, bytes (bandwidth), and bytes allocated/sample
// (runtime memory pressure). Driven by INTCHUNK_BENCH_DIR; self-skips if unset.

// timePerSample runs fn over `reps` full passes of the block list and returns
// nanoseconds per sample. reps is auto-scaled to ~50ms total so short blocks
// still get a stable reading.
func timePerSample(nSamples int, fn func()) float64 {
	// Warm + auto-scale.
	fn()
	reps := 1
	for {
		start := time.Now()
		for i := 0; i < reps; i++ {
			fn()
		}
		el := time.Since(start)
		if el > 50*time.Millisecond || reps > 1<<20 {
			return float64(el.Nanoseconds()) / float64(reps) / float64(nSamples)
		}
		reps *= 4
	}
}

// bytesPerSample measures heap bytes allocated per sample by fn (one pass).
func bytesPerSample(nSamples int, fn func()) float64 {
	const iters = 50
	runtime.GC()
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	for i := 0; i < iters; i++ {
		fn()
	}
	runtime.ReadMemStats(&m1)
	return float64(m1.TotalAlloc-m0.TotalAlloc) / float64(iters) / float64(nSamples)
}

func TestCompareCodecCPUMem(t *testing.T) {
	dir := os.Getenv("INTCHUNK_BENCH_DIR")
	if dir == "" {
		t.Skip("set INTCHUNK_BENCH_DIR to a dir of per-series JSON")
	}
	rows := loadBenchDir(dir)
	if len(rows) == 0 {
		t.Skipf("no series JSON in %s", dir)
	}

	t.Logf("=== gorilla vs INT (best body) vs best-of-N, block=%d, %d series ===", benchBlockSize, len(rows))
	t.Logf("%-20s %-8s | %12s %12s %12s | %12s %12s | %10s %10s | %10s %10s",
		"series", "winner",
		"enc gor ns", "enc int ns", "enc bestN ns",
		"dec gor ns", "dec int ns",
		"gor B/s", "int B/s",
		"encAlloc g", "encAlloc i")

	// Aggregate accumulators over series where INT actually wins on bytes (the
	// regime the cold path uses INT — the fair place to judge its CPU/mem cost).
	var n int
	var sEncG, sEncI, sEncB, sDecG, sDecI, sBitsG, sBitsI, sAllocG, sAllocI float64

	for _, r := range rows {
		s := r.samples()
		if len(s) < 2 {
			continue
		}
		// Use the first full block (or whole series if shorter) for timing.
		blk := s
		if len(blk) > benchBlockSize {
			blk = blk[:benchBlockSize]
		}

		gb, gok := encodeGorillaChunk(blk)
		ic, iok := encIntBest(blk)
		if !gok {
			continue
		}
		intWins := iok && totalLen(ic) < len(gb)
		winner := "GORILLA"
		if intWins {
			winner = "INT"
		}

		encG := timePerSample(len(blk), func() { encodeGorillaChunk(blk) })
		decG := timePerSample(len(blk), func() { DecodeChunk(gb) })
		encB := timePerSample(len(blk), func() { Encode(blk) })
		allocG := bytesPerSample(len(blk), func() { encodeGorillaChunk(blk) })

		bitsG := float64(len(gb)*8) / float64(len(blk))
		encI, decI, bitsI, allocI := 0.0, 0.0, 0.0, 0.0
		if iok {
			encI = timePerSample(len(blk), func() { encIntBest(blk) })
			decI = timePerSample(len(blk), func() { DecodeChunks(ic) })
			bitsI = float64(totalLen(ic)*8) / float64(len(blk))
			allocI = bytesPerSample(len(blk), func() { encIntBest(blk) })
		}

		t.Logf("%-20s %-8s | %12.1f %12.1f %12.1f | %12.1f %12.1f | %10.2f %10.2f | %10.0f %10.0f",
			trunc(r.Metric, 20), winner, encG, encI, encB, decG, decI, bitsG, bitsI, allocG, allocI)

		if intWins {
			n++
			sEncG += encG
			sEncI += encI
			sEncB += encB
			sDecG += decG
			sDecI += decI
			sBitsG += bitsG
			sBitsI += bitsI
			sAllocG += allocG
			sAllocI += allocI
		}
	}

	if n == 0 {
		t.Skip("no INT-wins series to aggregate")
	}
	f := float64(n)
	t.Log("--- AGGREGATE over INT-wins series (where the cold path picks INT) ---")
	t.Logf("  encode CPU:  gorilla=%.1f  int=%.1f  best-of-N=%.1f ns/sample  (int=%.2fx gorilla, bestN=%.2fx)",
		sEncG/f, sEncI/f, sEncB/f, (sEncI/f)/(sEncG/f), (sEncB/f)/(sEncG/f))
	t.Logf("  decode CPU:  gorilla=%.1f  int=%.1f ns/sample  (int=%.2fx gorilla)",
		sDecG/f, sDecI/f, (sDecI/f)/(sDecG/f))
	t.Logf("  bandwidth:   gorilla=%.2f  int=%.2f bits/sample  (int saves %.2fx)",
		sBitsG/f, sBitsI/f, (sBitsG/f)/(sBitsI/f))
	t.Logf("  encode mem:  gorilla=%.0f  int=%.0f bytes/sample  (int=%.2fx gorilla)",
		sAllocG/f, sAllocI/f, (sAllocI/f)/(sAllocG/f))
	t.Logf("  VERDICT: %s", fmt.Sprintf(
		"INT wins bandwidth (%.2fx) & storage; decode CPU %.2fx gorilla; encode CPU+mem COST more (best-of-N tries all candidates)",
		(sBitsG/f)/(sBitsI/f), (sDecI/f)/(sDecG/f)))
}
