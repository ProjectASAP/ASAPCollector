package intchunk

import "math/bits"

// ---------------------------------------------------------------------------
// timestamp column (t0 + delta-of-delta varints) — shared by every codec
// ---------------------------------------------------------------------------
//
// Per DESIGN.md §1.2 the chunk header carries the timestamps as t0 followed by
// delta-of-delta varints, independent of the value codec. Regular cadences
// (the common case) collapse to a stream of zero-dods, each a single byte.

func encodeTimestamps(w *byteWriter, ts []int64) {
	if len(ts) == 0 {
		return
	}
	w.varint(ts[0]) // t0
	if len(ts) == 1 {
		return
	}
	prevDelta := ts[1] - ts[0]
	w.varint(prevDelta) // first delta
	for i := 2; i < len(ts); i++ {
		delta := ts[i] - ts[i-1]
		dod := delta - prevDelta
		w.varint(dod)
		prevDelta = delta
	}
}

func decodeTimestamps(r *byteReader, n int) ([]int64, error) {
	out := make([]int64, 0, n)
	if n == 0 {
		return out, nil
	}
	t0, err := r.varint()
	if err != nil {
		return nil, err
	}
	out = append(out, t0)
	if n == 1 {
		return out, nil
	}
	delta, err := r.varint()
	if err != nil {
		return nil, err
	}
	prev := t0 + delta
	out = append(out, prev)
	prevDelta := delta
	for i := 2; i < n; i++ {
		dod, err := r.varint()
		if err != nil {
			return nil, err
		}
		prevDelta += dod
		prev += prevDelta
		out = append(out, prev)
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// INT_* residual transform: scale -> FOR(base) -> delta or delta-of-delta
// ---------------------------------------------------------------------------

// intResiduals turns scaled int64 values into the residual stream for the
// chosen INT codec. base is the frame-of-reference (DESIGN.md §1.4): for
// FOR_DELTA it is ints[0] and residuals are first-order deltas; for FOR_DOD it
// is also ints[0] and residuals are second-order (delta-of-delta) values.
//
// firstResidual is stored explicitly in the header (the design's
// `first_residual`); the bit-packed body holds the remaining residuals. This
// keeps the body a clean fixed-width array and lets the very first residual use
// the full varint width without inflating the packed width.
//
// It returns ok=false when a residual would overflow maxResidualWidth — the
// signal for the chunk-cut/re-base in encodeIntCut.
func intResiduals(ints []int64, dod bool) (base, firstResidual int64, residuals []int64, ok bool) {
	if len(ints) == 0 {
		return 0, 0, nil, false
	}
	base = ints[0]
	if len(ints) == 1 {
		return base, 0, nil, true
	}

	if !dod {
		// FOR_DELTA: residual_i = ints[i] - ints[i-1]
		firstResidual = ints[1] - ints[0]
		if !residualFits(firstResidual) {
			return base, 0, nil, false
		}
		residuals = make([]int64, 0, len(ints)-2)
		for i := 2; i < len(ints); i++ {
			d := ints[i] - ints[i-1]
			if !residualFits(d) {
				return base, 0, nil, false
			}
			residuals = append(residuals, d)
		}
		return base, firstResidual, residuals, true
	}

	// FOR_DOD: residual is delta-of-delta. The first delta is stored as
	// firstResidual; the body holds the dods from the 3rd sample on.
	firstDelta := ints[1] - ints[0]
	if !residualFits(firstDelta) {
		return base, 0, nil, false
	}
	firstResidual = firstDelta
	residuals = make([]int64, 0, len(ints)-2)
	prevDelta := firstDelta
	for i := 2; i < len(ints); i++ {
		delta := ints[i] - ints[i-1]
		dodVal := delta - prevDelta
		if !residualFits(dodVal) {
			return base, 0, nil, false
		}
		residuals = append(residuals, dodVal)
		prevDelta = delta
	}
	return base, firstResidual, residuals, true
}

// reconstructInts inverts intResiduals.
func reconstructInts(base, firstResidual int64, residuals []int64, n int, dod bool) []int64 {
	out := make([]int64, 0, n)
	out = append(out, base)
	if n == 1 {
		return out
	}
	if !dod {
		prev := base + firstResidual
		out = append(out, prev)
		for _, d := range residuals {
			prev += d
			out = append(out, prev)
		}
		return out
	}
	prevDelta := firstResidual
	prev := base + prevDelta
	out = append(out, prev)
	for _, dodVal := range residuals {
		prevDelta += dodVal
		prev += prevDelta
		out = append(out, prev)
	}
	return out
}

// residualFits reports whether the zigzag width of a residual stays inside the
// per-chunk overflow bound; a residual that needs more bits triggers a re-base.
func residualFits(v int64) bool {
	return bits.Len64(zigzag(v)) <= maxResidualWidth
}

// ---------------------------------------------------------------------------
// INT_* chunk body encode / decode
// ---------------------------------------------------------------------------

// intBodyFormat selects how an INT_* chunk stores its residual body. Both
// formats share the identical residual transform (intResiduals) and header;
// only the trailing residual stream differs, so best-of-N can try both over the
// same transform and keep the smaller.
type intBodyFormat uint8

const (
	// bodyFixed bit-packs every residual at a single fixed width (tags 1/2).
	bodyFixed intBodyFormat = iota
	// bodyVarint stores each residual as a zigzag varint (tags 3/4).
	bodyVarint
)

// codecTagFor maps (delta-vs-dod, body format) to the wire codec tag.
func codecTagFor(dod bool, body intBodyFormat) CodecTag {
	switch {
	case !dod && body == bodyFixed:
		return CodecIntForDelta
	case dod && body == bodyFixed:
		return CodecIntForDoD
	case !dod && body == bodyVarint:
		return CodecIntForDeltaVarint
	default:
		return CodecIntForDoDVarint
	}
}

// encodeIntChunk encodes a single INT_* chunk over the whole input using the
// requested body format. It assumes the caller (via intResiduals) has confirmed
// no residual overflows; callers that need the overflow chunk-cut use
// encodeIntChunks. Returns the full chunk bytes (header + body) or ok=false if
// the residual transform overflowed.
func encodeIntChunk(samples []Sample, scaleExp int8, ints []int64, dod bool, body intBodyFormat) ([]byte, bool) {
	tag := codecTagFor(dod, body)
	base, firstResidual, residuals, ok := intResiduals(ints, dod)
	if !ok {
		return nil, false
	}

	w := &byteWriter{}
	w.u8(byte(tag))
	w.uvarint(uint64(len(samples)))
	ts := make([]int64, len(samples))
	for i := range samples {
		ts[i] = samples[i].T
	}
	encodeTimestamps(w, ts)

	// INT value params.
	w.u8(byte(uint8(scaleExp)))
	w.varint(base)
	w.varint(firstResidual)

	// Body: residual stream in the chosen format.
	switch body {
	case bodyVarint:
		// Each residual as a self-delimiting zigzag varint. No width byte is
		// needed; the residual count is fixed by n.
		for _, d := range residuals {
			w.varint(d)
		}
	default: // bodyFixed
		// Single fixed-width bit-packed array of zigzag residuals.
		zz := make([]uint64, len(residuals))
		for i, d := range residuals {
			zz[i] = zigzag(d)
		}
		width := bitPackWidth(zz)
		w.u8(width)
		w.buf = packBits(w.buf, zz, width)
	}
	return w.buf, true
}

// decodeIntChunk decodes an INT_* chunk body (the reader is positioned just
// after the codec tag, which the caller has already consumed and matched).
func decodeIntChunk(r *byteReader, tag CodecTag) ([]Sample, error) {
	dod := tag == CodecIntForDoD || tag == CodecIntForDoDVarint
	varintBody := tag == CodecIntForDeltaVarint || tag == CodecIntForDoDVarint
	nU, err := r.uvarint()
	if err != nil {
		return nil, err
	}
	n := int(nU)
	ts, err := decodeTimestamps(r, n)
	if err != nil {
		return nil, err
	}
	rawExp, err := r.u8()
	if err != nil {
		return nil, err
	}
	scaleExp := int8(rawExp)
	base, err := r.varint()
	if err != nil {
		return nil, err
	}
	firstResidual, err := r.varint()
	if err != nil {
		return nil, err
	}
	nResiduals := 0
	if n >= 3 {
		nResiduals = n - 2
	}
	residuals := make([]int64, nResiduals)
	if varintBody {
		// Body: nResiduals self-delimiting zigzag varints.
		for i := 0; i < nResiduals; i++ {
			d, err := r.varint()
			if err != nil {
				return nil, err
			}
			residuals[i] = d
		}
	} else {
		width, err := r.u8()
		if err != nil {
			return nil, err
		}
		zz, err := unpackBits(r.buf[r.pos:], nResiduals, width)
		if err != nil {
			return nil, err
		}
		for i, u := range zz {
			residuals[i] = unzigzag(u)
		}
	}
	ints := reconstructInts(base, firstResidual, residuals, n, dod)

	// Decode divides by the decimal-exact power 10^e — the same operation the
	// exactness guard (tryScaleToInt64) proved bit-exact at encode time.
	factor := pow10Abs(scaleExp)
	out := make([]Sample, n)
	for i := 0; i < n; i++ {
		out[i] = Sample{T: ts[i], V: float64(ints[i]) / factor}
	}
	return out, nil
}

// pow10Abs returns 10^e for a (non-negative) scale exponent. Encoders only ever
// produce e in [0, maxScaleExp]; this mirrors the table used by tryScaleToInt64.
func pow10Abs(e int8) float64 {
	if e >= 0 && int(e) < len(pow10) {
		return pow10[e]
	}
	// Fallback for out-of-table exponents (should not occur for emitted chunks).
	f := 1.0
	for i := int8(0); i < e; i++ {
		f *= 10
	}
	return f
}

// ---------------------------------------------------------------------------
// overflow chunk-cut (DESIGN.md §1.4 / §3): when a residual would overflow the
// chosen width, end the chunk and start a fresh one with a new base.
// ---------------------------------------------------------------------------

// encodeIntChunks splits samples into one-or-more INT_* chunks so that no chunk
// contains a residual exceeding maxResidualWidth. Within a block this is the
// cold analogue of "offset drift -> re-base": the cut starts a new chunk whose
// first sample becomes the fresh frame base. Each returned []byte is a complete,
// independently-decodable chunk.
func encodeIntChunks(samples []Sample, scaleExp int8, ints []int64, dod bool, body intBodyFormat) ([][]byte, bool) {
	if len(samples) == 0 {
		return nil, false
	}
	var chunks [][]byte
	start := 0
	for start < len(samples) {
		end := findCutEnd(ints[start:], dod) + start
		b, ok := encodeIntChunk(samples[start:end], scaleExp, ints[start:end], dod, body)
		if !ok {
			return nil, false
		}
		chunks = append(chunks, b)
		start = end
	}
	return chunks, true
}

// findCutEnd returns the exclusive end index (relative to the slice start) of
// the longest prefix of ints whose residual transform stays within
// maxResidualWidth. It always returns at least 1 so progress is guaranteed.
func findCutEnd(ints []int64, dod bool) int {
	if len(ints) <= 1 {
		return len(ints)
	}
	base := ints[0]
	if !dod {
		prev := base
		for i := 1; i < len(ints); i++ {
			if !residualFits(ints[i] - prev) {
				return i
			}
			prev = ints[i]
		}
		return len(ints)
	}
	// dod
	prevDelta := ints[1] - ints[0]
	if !residualFits(prevDelta) {
		return 1
	}
	prev := ints[1]
	for i := 2; i < len(ints); i++ {
		delta := ints[i] - prev
		if !residualFits(delta - prevDelta) {
			return i
		}
		prevDelta = delta
		prev = ints[i]
	}
	return len(ints)
}
