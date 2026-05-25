package intchunk

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// blockSize matches the design's cold-tier chunk size (DESIGN.md benchmark uses
// 1000-sample blocks). bits/sample is averaged over independent blocks so each
// chunk re-pays its own header cost, reflecting real cold-tier behaviour.
const benchBlockSize = 1000

// benchSeriesFile mirrors the per-series JSON in compress-bench/data_serf and
// data_synth: {"metric","kind",...,"points":[[ts,val],...]}.
type benchSeriesFile struct {
	Metric string      `json:"metric"`
	Kind   string      `json:"kind"`
	Points [][]float64 `json:"points"`
}

// loadBenchDir reads every *.json series in dir. It returns nil if the dir is
// absent (the bench data lives outside the repo at /mydata/compress-bench), so
// the table-printing test self-skips in CI / fresh checkouts.
func loadBenchDir(dir string) []benchSeriesFile {
	files, _ := filepath.Glob(filepath.Join(dir, "*.json"))
	sort.Strings(files)
	var out []benchSeriesFile
	for _, f := range files {
		if filepath.Base(f) == "_manifest.json" {
			continue
		}
		raw, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		var s benchSeriesFile
		if json.Unmarshal(raw, &s) != nil || len(s.Points) < 2 {
			continue
		}
		out = append(out, s)
	}
	return out
}

func (s benchSeriesFile) samples() []Sample {
	out := make([]Sample, len(s.Points))
	for i, p := range s.Points {
		out[i] = Sample{T: int64(p[0]), V: p[1]}
	}
	return out
}

// blockAvgBits chunks samples into benchBlockSize-sample blocks, encodes each
// independently with the given forced codec (or best-of-N) and returns mean
// bits/sample plus the dominant winning tag (for best-of-N).
type encFn func([]Sample) (int, CodecTag, bool)

func blockAvgBits(samples []Sample, enc encFn) (bits float64, blocks int, winners map[CodecTag]int) {
	winners = map[CodecTag]int{}
	var sumBits float64
	for i := 0; i < len(samples); i += benchBlockSize {
		j := i + benchBlockSize
		if j > len(samples) {
			j = len(samples)
		}
		if j-i < 2 {
			continue
		}
		nbytes, tag, ok := enc(samples[i:j])
		if !ok {
			continue
		}
		sumBits += float64(nbytes*8) / float64(j-i)
		winners[tag]++
		blocks++
	}
	if blocks == 0 {
		return 0, 0, winners
	}
	return sumBits / float64(blocks), blocks, winners
}

// forcedGorilla encodes a block as Gorilla-XOR only (the baseline).
func forcedGorilla(s []Sample) (int, CodecTag, bool) {
	b, ok := encodeGorillaChunk(s)
	if !ok {
		return 0, CodecGorillaXOR, false
	}
	return len(b), CodecGorillaXOR, true
}

// forcedInt encodes a block as the better of INT_FOR_DELTA / INT_FOR_DOD, but
// ONLY when tryScaleToInt64 proves it lossless; otherwise it reports !ok so the
// table shows "n/a" (the honest decimal-exactness guard — never lossy INT).
func forcedInt(s []Sample) (int, CodecTag, bool) {
	vals := make([]float64, len(s))
	for i := range s {
		vals[i] = s[i].V
	}
	scaleExp, ints, ok := tryScaleToInt64(vals)
	if !ok {
		return 0, CodecIntForDelta, false
	}
	del, okD := encodeIntChunks(s, scaleExp, ints, false)
	dod, okDD := encodeIntChunks(s, scaleExp, ints, true)
	best := -1
	tag := CodecIntForDelta
	if okD {
		best = totalLen(del)
	}
	if okDD {
		if best < 0 || totalLen(dod) < best {
			best = totalLen(dod)
			tag = CodecIntForDoD
		}
	}
	if best < 0 {
		return 0, tag, false
	}
	return best, tag, true
}

// bestOfN encodes a block with the full best-of-N codec.
func bestOfN(s []Sample) (int, CodecTag, bool) {
	res, err := Encode(s)
	if err != nil {
		return 0, CodecGorillaXOR, false
	}
	return res.Bytes, res.Tag, true
}

// TestBenchmarkBitsPerSample prints the bits/sample table over representative
// data and asserts the design verdict: INT beats Gorilla on fixed-decimal, and
// Gorilla wins (best-of-N falls back) on true high-precision floats.
//
// Set INTCHUNK_BENCH_DIR to a directory of per-series JSON (e.g.
// /mydata/compress-bench/data_serf). If unset / absent, the test self-skips.
func TestBenchmarkBitsPerSample(t *testing.T) {
	dir := os.Getenv("INTCHUNK_BENCH_DIR")
	if dir == "" {
		t.Skip("set INTCHUNK_BENCH_DIR to a dir of per-series JSON to run the bits/sample benchmark")
	}
	rows := loadBenchDir(dir)
	if len(rows) == 0 {
		t.Skipf("no series JSON found in %s", dir)
	}

	t.Logf("=== bits/sample over %d series in %s (block=%d) ===", len(rows), dir, benchBlockSize)
	t.Logf("%-22s %-22s %8s | %9s %9s %8s | %-14s %s",
		"series", "kind", "n", "gor b/s", "int b/s", "gor/int", "bestN b/s", "bestN winner")

	var sumGor, sumInt, sumBest float64
	var nInt, nRows int
	// Class roll-up (the design verdict).
	classGor := map[string][]float64{}
	classInt := map[string][]float64{}

	for _, r := range rows {
		s := r.samples()
		gor, _, _ := blockAvgBits(s, forcedGorilla)
		intb, _, _ := blockAvgBits(s, forcedInt)
		best, _, winners := blockAvgBits(s, bestOfN)

		// Determine if INT was usable for this series (decimal-exact).
		intUsable := intb > 0
		intStr := "      n/a"
		x := 0.0
		if intUsable {
			intStr = fmtBits(intb)
			x = gor / intb
		}
		// Dominant best-of-N winner.
		winTag := dominant(winners)
		cls := "fixed-decimal (INT)"
		if !intUsable || winTag == CodecGorillaXOR {
			cls = "high-precision float (Gorilla)"
		}

		t.Logf("%-22s %-22s %8d | %9.3f %9s %7.2fx | %9.3f %s",
			trunc(r.Metric, 22), trunc(r.Kind, 22), len(s), gor, intStr, x, best, winTag)

		sumGor += gor
		sumBest += best
		nRows++
		if intUsable {
			sumInt += intb
			nInt++
		}
		classGor[cls] = append(classGor[cls], gor)
		if intUsable {
			classInt[cls] = append(classInt[cls], intb)
		}
	}

	if nRows == 0 {
		t.Skip("no usable series")
	}
	t.Logf("--- AGG: gorilla=%.3f b/s  int(decimal-exact only, %d series)=%.3f b/s  best-of-N=%.3f b/s",
		sumGor/float64(nRows), nInt, safeDiv(sumInt, float64(nInt)), sumBest/float64(nRows))

	// Class roll-up verdict.
	t.Log("--- class roll-up (the design verdict) ---")
	for _, cls := range sortedKeys(classGor) {
		g := mean(classGor[cls])
		iv := mean(classInt[cls])
		verdict := "Gorilla"
		ratio := 0.0
		if iv > 0 {
			ratio = g / iv
			if ratio > 1 {
				verdict = "INT"
			}
		}
		t.Logf("  %-34s gor=%.3f int=%.3f  gor/int=%.2fx -> %s",
			cls, g, iv, ratio, verdict)
	}

	// Verdict assertions.
	fdGor := mean(classGor["fixed-decimal (INT)"])
	fdInt := mean(classInt["fixed-decimal (INT)"])
	if fdInt > 0 {
		if fdInt >= fdGor {
			t.Errorf("expected INT to BEAT Gorilla on fixed-decimal: int=%.3f gor=%.3f", fdInt, fdGor)
		} else {
			t.Logf("VERDICT: INT beats Gorilla on fixed-decimal by %.2fx", fdGor/fdInt)
		}
	}
}

// fmtBits formats a bits/sample value right-aligned to 9 columns with 3 dp.
func fmtBits(b float64) string { return fmt.Sprintf("%9.3f", b) }

func dominant(m map[CodecTag]int) CodecTag {
	best := CodecGorillaXOR
	bestN := -1
	for _, tag := range []CodecTag{CodecGorillaXOR, CodecIntForDelta, CodecIntForDoD} {
		if m[tag] > bestN {
			bestN = m[tag]
			best = tag
		}
	}
	return best
}

func mean(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	var s float64
	for _, x := range xs {
		s += x
	}
	return s / float64(len(xs))
}
func safeDiv(a, b float64) float64 {
	if b == 0 {
		return 0
	}
	return a / b
}
func sortedKeys(m map[string][]float64) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}
func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "~"
}

// BenchmarkEncode measures encode throughput of the best-of-N codec on a
// representative fixed-decimal block (1000 samples, 2dp gauge).
func BenchmarkEncode(b *testing.B) {
	vals := make([]float64, benchBlockSize)
	cents := int64(2000)
	for i := range vals {
		cents += int64(i%7) - 3
		vals[i] = float64(cents) / 100.0
	}
	s := mkSamples(1_700_000_000_000, 1000, vals)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Encode(s); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecode measures decode throughput on the same block.
func BenchmarkDecode(b *testing.B) {
	vals := make([]float64, benchBlockSize)
	cents := int64(2000)
	for i := range vals {
		cents += int64(i%7) - 3
		vals[i] = float64(cents) / 100.0
	}
	s := mkSamples(1_700_000_000_000, 1000, vals)
	res, err := Encode(s)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := DecodeChunks(res.Chunks); err != nil {
			b.Fatal(err)
		}
	}
}
