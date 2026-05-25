package intchunk

import "math"

// pow10 holds 10^e for the decimal exponents we probe. Index i is 10^i.
// We only need positive powers up to ~15 (float64 carries ~15-17 sig digits).
var pow10 = [...]float64{
	1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7,
	1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15,
}

// maxScaleExp is the largest decimal scale exponent magnitude we probe. The
// chunk header stores scale_exp as an i8, and float64 cannot represent more
// than ~15-17 significant decimal digits exactly, so probing 0..15 is enough.
const maxScaleExp = 15

// tryScaleToInt64 is the load-bearing decimal-exactness guard (DESIGN.md §1.4).
//
// It searches for the smallest decimal scale exponent e (0..maxScaleExp) such
// that every value v in vals satisfies:
//
//	int_v := round(v * 10^e)         // scaled to an integer
//	v == float64(int_v) * 10^-e      // round-trips BIT-EXACTLY
//
// If such an e exists it returns (e, ints, true); the caller may then build an
// INT_* candidate. If NO exponent makes the whole series round-trip exactly
// (true high-precision floats), it returns ok=false and the caller must fall
// back to Gorilla-XOR. This is the trap a naive lib/decimal path hits: it
// scales silently and introduces ~1e-12 error. We NEVER emit INT_* unless the
// decode reproduces the original float64 bit pattern exactly.
//
// scaleExp is stored in the header as i8 and applied on decode as
// v = float64(int_v) / 10^e.  The guard checks exactly this DIVISION (not a
// multiply by a pre-rounded reciprocal): dividing by the decimal-exact power
// 10^e is the accurate inverse and matches what decodeIntChunk computes, so the
// guarantee the guard proves is the guarantee decode delivers. (A multiply by
// 1/10^e uses a rounded reciprocal and would wrongly reject many genuinely
// fixed-decimal series, inflating the chosen exponent.)
func tryScaleToInt64(vals []float64) (scaleExp int8, ints []int64, ok bool) {
	// Reject any non-finite value outright; Gorilla handles those losslessly.
	for _, v := range vals {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return 0, nil, false
		}
	}
	for e := 0; e <= maxScaleExp; e++ {
		factor := pow10[e]
		cand := make([]int64, len(vals))
		exact := true
		for i, v := range vals {
			scaled := v * factor
			// Must land on (or essentially on) an integer, within int64 range.
			r := math.Round(scaled)
			if r > math.MaxInt64 || r < math.MinInt64 {
				exact = false
				break
			}
			iv := int64(r)
			// The guard: decode (divide) and demand a bit-exact round-trip.
			if float64(iv)/factor != v {
				exact = false
				break
			}
			cand[i] = iv
		}
		if exact {
			return int8(e), cand, true
		}
	}
	return 0, nil, false
}
