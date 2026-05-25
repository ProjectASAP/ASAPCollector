package intchunk

import (
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// ---------------------------------------------------------------------------
// GORILLA_XOR codec (DESIGN.md §1.3 tag 0): lossless float64 via the
// prometheus/tsdb/chunkenc XOR chunk (XOR == Gorilla). Always valid; the
// fallback for true high-precision floats.
// ---------------------------------------------------------------------------
//
// The standard XOR stream already encodes both timestamps (its own
// delta-of-delta scheme) and values, so for this codec the chunk is:
//
//	u8 codec_tag(0) | uvarint n_samples | <raw XOR chunk bytes>
//
// We carry n_samples explicitly so the decoder need not trust the XOR chunk's
// internal 16-bit sample counter, and so the chunk framing is uniform across
// codecs.

// xorChunkMaxSamples is the addressable sample count of a single prometheus XOR
// chunk (its header stores the count in a uint16). A single chunk therefore
// holds at most 65535 samples; larger inputs are split (the best-of-N driver
// chunks by block before reaching here, so in practice this never triggers).
const xorChunkMaxSamples = 1 << 16

func encodeGorillaChunk(samples []Sample) ([]byte, bool) {
	if len(samples) == 0 || len(samples) >= xorChunkMaxSamples {
		return nil, false
	}
	c := chunkenc.NewXORChunk()
	app, err := c.Appender()
	if err != nil {
		return nil, false
	}
	for _, s := range samples {
		app.Append(s.T, s.V)
	}
	xorBytes := c.Bytes()

	w := &byteWriter{}
	w.u8(byte(CodecGorillaXOR))
	w.uvarint(uint64(len(samples)))
	w.buf = append(w.buf, xorBytes...)
	return w.buf, true
}

func decodeGorillaChunk(r *byteReader) ([]Sample, error) {
	nU, err := r.uvarint()
	if err != nil {
		return nil, err
	}
	n := int(nU)
	c, err := chunkenc.FromData(chunkenc.EncXOR, r.buf[r.pos:])
	if err != nil {
		return nil, ErrCorruptChunk
	}
	it := c.Iterator(nil)
	out := make([]Sample, 0, n)
	for it.Next() == chunkenc.ValFloat {
		t, v := it.At()
		out = append(out, Sample{T: t, V: v})
	}
	if it.Err() != nil {
		return nil, ErrCorruptChunk
	}
	return out, nil
}
