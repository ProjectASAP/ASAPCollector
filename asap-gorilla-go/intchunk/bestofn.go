package intchunk

// ---------------------------------------------------------------------------
// Best-of-N encoder/decoder (DESIGN.md §1.4)
// ---------------------------------------------------------------------------
//
// Encode runs the per-block best-of-N: it builds every valid candidate
// (INT_FOR_DELTA, INT_FOR_DOD — only when tryScaleToInt64 proves a bit-exact
// scale exists — and always GORILLA_XOR) and returns the smallest. Because the
// INT_* path may cut the block into several chunks on residual overflow
// (§1.4 re-base), Encode returns a slice of chunks: a single chunk for the
// Gorilla / non-overflowing-INT case, or several for the overflow case.
//
// Decode inverts a single chunk; DecodeChunks decodes a concatenation produced
// by appending Encode's outputs.

// EncodeResult describes the winning encoding of a block (for tests/benchmarks).
type EncodeResult struct {
	Tag    CodecTag // winning codec
	Chunks [][]byte // one or more self-contained chunks (>1 only on INT overflow cut)
	Bytes  int      // total bytes across Chunks
}

// Encode encodes one block of samples with the best-of-N codec and returns the
// winning chunk(s). All candidates are lossless; the smallest total byte size
// wins. The samples are used in the order given (the caller sorts by time).
func Encode(samples []Sample) (EncodeResult, error) {
	if len(samples) == 0 {
		return EncodeResult{}, ErrEmpty
	}

	type cand struct {
		tag    CodecTag
		chunks [][]byte
		bytes  int
	}
	var cands []cand

	// INT candidates — only when float->int64->float round-trips bit-exactly.
	vals := make([]float64, len(samples))
	for i := range samples {
		vals[i] = samples[i].V
	}
	if scaleExp, ints, ok := tryScaleToInt64(vals); ok {
		if chunks, ok := encodeIntChunks(samples, scaleExp, ints, false); ok {
			cands = append(cands, cand{CodecIntForDelta, chunks, totalLen(chunks)})
		}
		if chunks, ok := encodeIntChunks(samples, scaleExp, ints, true); ok {
			cands = append(cands, cand{CodecIntForDoD, chunks, totalLen(chunks)})
		}
	}

	// Gorilla is always valid + lossless.
	if gb, ok := encodeGorillaChunk(samples); ok {
		cands = append(cands, cand{CodecGorillaXOR, [][]byte{gb}, len(gb)})
	}

	if len(cands) == 0 {
		// Should not happen for finite, non-empty input, but guard anyway.
		return EncodeResult{}, ErrEmpty
	}

	best := cands[0]
	for _, c := range cands[1:] {
		if c.bytes < best.bytes {
			best = c
		}
	}
	return EncodeResult{Tag: best.tag, Chunks: best.chunks, Bytes: best.bytes}, nil
}

func totalLen(chunks [][]byte) int {
	n := 0
	for _, c := range chunks {
		n += len(c)
	}
	return n
}

// DecodeChunk decodes a single self-contained chunk back to its samples.
func DecodeChunk(chunk []byte) ([]Sample, error) {
	r := &byteReader{buf: chunk}
	tagByte, err := r.u8()
	if err != nil {
		return nil, err
	}
	tag := CodecTag(tagByte)
	switch tag {
	case CodecGorillaXOR:
		return decodeGorillaChunk(r)
	case CodecIntForDelta, CodecIntForDoD:
		return decodeIntChunk(r, tag)
	default:
		return nil, ErrBadCodecTag
	}
}

// DecodeChunks decodes a sequence of concatenated chunks (e.g. all chunks from
// one EncodeResult, possibly multiple after an overflow cut) back to the full
// ordered sample stream.
func DecodeChunks(chunks [][]byte) ([]Sample, error) {
	var out []Sample
	for _, c := range chunks {
		samples, err := DecodeChunk(c)
		if err != nil {
			return nil, err
		}
		out = append(out, samples...)
	}
	return out, nil
}

// PeekTag returns the codec tag of a chunk without fully decoding it.
func PeekTag(chunk []byte) (CodecTag, error) {
	if len(chunk) == 0 {
		return 0, ErrCorruptChunk
	}
	return CodecTag(chunk[0]), nil
}
