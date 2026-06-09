package intchunk

import (
	"os"
	"testing"
)

// bench_compare_test.go: head-to-head CPU + allocation comparison of the
// VM-style INT FOR+delta cold codec vs plain Gorilla-XOR, on the SAME real
// block. The existing BenchmarkEncode/Decode only exercise best-of-N; these
// isolate each codec so encode/decode ns/sample and B/op are directly
// comparable (the DESIGN.md "VM decode is faster" + bandwidth claims).
//
// Driven by INTCHUNK_BENCH_DIR (a dir of per-series JSON). Skips if unset.

// pickBlock loads the first series whose first benchBlockSize-sample block is
// in the requested regime, returning that one block so the two codecs race on
// identical data. wantInt=true requires the INT codec to actually WIN on bytes
// (the regime the cold path uses INT) — not merely be representable, since some
// fixed-decimal series (5dp Air-pressure) scale to int yet Gorilla still wins.
func pickBlock(b *testing.B, wantInt bool) []Sample {
	b.Helper()
	dir := os.Getenv("INTCHUNK_BENCH_DIR")
	if dir == "" {
		b.Skip("set INTCHUNK_BENCH_DIR to a dir of per-series JSON")
	}
	for _, r := range loadBenchDir(dir) {
		s := r.samples()
		if len(s) < benchBlockSize {
			continue
		}
		blk := s[:benchBlockSize]
		gb, gok := encodeGorillaChunk(blk)
		ic, iok := encIntBest(blk)
		intWins := gok && iok && totalLen(ic) < len(gb)
		if intWins == wantInt {
			b.Logf("using series %q (kind=%s, int-wins=%v: gor=%dB int=%dB)",
				r.Metric, r.Kind, intWins, len(gb), func() int {
					if iok {
						return totalLen(ic)
					}
					return -1
				}())
			return blk
		}
	}
	b.Skipf("no series with int-wins=%v found", wantInt)
	return nil
}

// encIntBest encodes a block with the best INT body (mirrors what best-of-N
// would keep for a fixed-decimal block), returning the winning chunks.
func encIntBest(s []Sample) ([][]byte, bool) {
	vals := make([]float64, len(s))
	for i := range s {
		vals[i] = s[i].V
	}
	scaleExp, ints, ok := tryScaleToInt64(vals)
	if !ok {
		return nil, false
	}
	var best [][]byte
	bestSz := -1
	for _, v := range []struct {
		dod  bool
		body intBodyFormat
	}{{false, bodyFixed}, {true, bodyFixed}, {false, bodyVarint}, {true, bodyVarint}} {
		if chunks, ok := encodeIntChunks(s, scaleExp, ints, v.dod, v.body); ok {
			if sz := totalLen(chunks); bestSz < 0 || sz < bestSz {
				bestSz, best = sz, chunks
			}
		}
	}
	return best, bestSz >= 0
}

// --- ENCODE: fixed-decimal block, gorilla vs int vs best-of-N ---

func BenchmarkEncode_FixedDec_Gorilla(b *testing.B) { benchEncGorilla(b, pickBlock(b, true)) }
func BenchmarkEncode_FixedDec_Int(b *testing.B)     { benchEncInt(b, pickBlock(b, true)) }
func BenchmarkEncode_FixedDec_BestOfN(b *testing.B) { benchEncBest(b, pickBlock(b, true)) }

// --- DECODE: fixed-decimal block, gorilla vs int (the decode-on-read path) ---

func BenchmarkDecode_FixedDec_Gorilla(b *testing.B) { benchDecGorilla(b, pickBlock(b, true)) }
func BenchmarkDecode_FixedDec_Int(b *testing.B)     { benchDecInt(b, pickBlock(b, true)) }

func benchEncGorilla(b *testing.B, s []Sample) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := encodeGorillaChunk(s); !ok {
			b.Fatal("gorilla encode failed")
		}
	}
	b.StopTimer()
	gb, _ := encodeGorillaChunk(s)
	reportPerSample(b, s, len(gb))
}

func benchEncInt(b *testing.B, s []Sample) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := encIntBest(s); !ok {
			b.Fatal("int encode failed")
		}
	}
	b.StopTimer()
	chunks, _ := encIntBest(s)
	reportPerSample(b, s, totalLen(chunks))
}

func benchEncBest(b *testing.B, s []Sample) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Encode(s); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	res, _ := Encode(s)
	reportPerSample(b, s, res.Bytes)
}

func benchDecGorilla(b *testing.B, s []Sample) {
	gb, ok := encodeGorillaChunk(s)
	if !ok {
		b.Fatal("gorilla encode failed")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := DecodeChunk(gb); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	reportPerSample(b, s, len(gb))
}

func benchDecInt(b *testing.B, s []Sample) {
	chunks, ok := encIntBest(s)
	if !ok {
		b.Fatal("int encode failed")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := DecodeChunks(chunks); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	reportPerSample(b, s, totalLen(chunks))
}

// reportPerSample adds ns/sample and bits/sample custom metrics so the codecs
// are comparable independent of block size.
func reportPerSample(b *testing.B, s []Sample, bytes int) {
	nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N)
	b.ReportMetric(nsPerOp/float64(len(s)), "ns/sample")
	b.ReportMetric(float64(bytes*8)/float64(len(s)), "bits/sample")
}
