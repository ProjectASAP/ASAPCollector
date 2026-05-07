// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package gorillas3processor

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// bitReader is a test-only decoder that mirrors bitWriter.
type bitReader struct {
	b      []byte
	bitlen uint32
	off    uint32
}

func newBitReader(b []byte, bitlen uint32) *bitReader {
	return &bitReader{b: b, bitlen: bitlen}
}

func (r *bitReader) readBit() (uint8, bool) {
	if r.off >= r.bitlen {
		return 0, false
	}
	by := r.off / 8
	bi := r.off % 8
	bit := (r.b[by] >> (7 - bi)) & 1
	r.off++
	return bit, true
}

func (r *bitReader) readBits(n uint8) (uint64, bool) {
	if n == 0 {
		return 0, true
	}
	var v uint64
	for i := uint8(0); i < n; i++ {
		bit, ok := r.readBit()
		if !ok {
			return 0, false
		}
		v = (v << 1) | uint64(bit)
	}
	return v, true
}

func signExtend(v uint64, n uint8) int64 {
	if n == 0 || n >= 64 {
		return int64(v)
	}
	shift := 64 - n
	return int64(v<<shift) >> shift
}

func decodeTimestamps(firstTS int64, count int, bits []byte, bitlen uint32) ([]int64, bool) {
	out := make([]int64, count)
	out[0] = firstTS
	prevTS := firstTS
	prevDelta := int64(0)
	r := newBitReader(bits, bitlen)
	for i := 1; i < count; i++ {
		b, ok := r.readBit()
		if !ok {
			return nil, false
		}
		var dd int64
		if b == 0 {
			dd = 0
		} else {
			b2, ok := r.readBit()
			if !ok {
				return nil, false
			}
			if b2 == 0 {
				v, ok := r.readBits(7)
				if !ok {
					return nil, false
				}
				dd = signExtend(v, 7)
			} else {
				b3, ok := r.readBit()
				if !ok {
					return nil, false
				}
				if b3 == 0 {
					v, ok := r.readBits(9)
					if !ok {
						return nil, false
					}
					dd = signExtend(v, 9)
				} else {
					b4, ok := r.readBit()
					if !ok {
						return nil, false
					}
					if b4 == 0 {
						v, ok := r.readBits(12)
						if !ok {
							return nil, false
						}
						dd = signExtend(v, 12)
					} else {
						v, ok := r.readBits(64)
						if !ok {
							return nil, false
						}
						dd = int64(v)
					}
				}
			}
		}
		delta := prevDelta + dd
		ts := prevTS + delta
		out[i] = ts
		prevTS = ts
		prevDelta = delta
	}
	return out, true
}

func decodeValues(firstValBits uint64, count int, bits []byte, bitlen uint32) ([]float64, bool) {
	out := make([]float64, count)
	out[0] = math.Float64frombits(firstValBits)
	prev := firstValBits
	var lz, tz uint8
	haveWindow := false
	r := newBitReader(bits, bitlen)
	for i := 1; i < count; i++ {
		c, ok := r.readBit()
		if !ok {
			return nil, false
		}
		var vb uint64
		if c == 0 {
			vb = prev
		} else {
			c2, ok := r.readBit()
			if !ok {
				return nil, false
			}
			if c2 == 0 {
				if !haveWindow {
					return nil, false
				}
				sigLen := 64 - lz - tz
				sig, ok := r.readBits(sigLen)
				if !ok {
					return nil, false
				}
				x := sig << tz
				vb = prev ^ x
			} else {
				lz5, ok := r.readBits(5)
				if !ok {
					return nil, false
				}
				lz = uint8(lz5)
				sigm1, ok := r.readBits(6)
				if !ok {
					return nil, false
				}
				sigLen := uint8(sigm1) + 1
				sig, ok := r.readBits(sigLen)
				if !ok {
					return nil, false
				}
				if sigLen == 64 {
					tz = 0
				} else {
					tz = 64 - lz - sigLen
				}
				x := sig << tz
				vb = prev ^ x
				haveWindow = true
			}
		}
		out[i] = math.Float64frombits(vb)
		prev = vb
	}
	return out, true
}

// decodeChunk parses an GORILLA1 block and returns the decoded series.
type decodedSeries struct {
	meta seriesMeta
	tss  []int64
	vals []float64
}

func decodeChunk(t *testing.T, data []byte) []decodedSeries {
	t.Helper()
	// Outer GORILLA1 block layout (must match asap-gorilla::block::HEADER_LEN = 13):
	//   [8]   magic        "GORILLA1"
	//   [1]   version      chunkVersion
	//   [4]   uint32 LE    seriesCount
	// Pre-v7 this helper read the magic as 4 bytes and the seriesCount
	// at offset [5:9], which mismatched both the writer and the
	// asap-gorilla decoder. Aligned now so a regression in either
	// direction (writer offset OR helper offset) fails this test.
	require.True(t, len(data) >= 13, "chunk too small for header")
	require.Equal(t, chunkMagic, string(data[:8]))
	require.Equal(t, byte(chunkVersion), data[8])
	seriesCount := binary.LittleEndian.Uint32(data[9:13])
	off := 13
	out := make([]decodedSeries, 0, seriesCount)
	for i := uint32(0); i < seriesCount; i++ {
		metaLen := binary.LittleEndian.Uint16(data[off : off+2])
		off += 2
		var meta seriesMeta
		require.NoError(t, json.Unmarshal(data[off:off+int(metaLen)], &meta))
		off += int(metaLen)
		pointCount := binary.LittleEndian.Uint32(data[off : off+4])
		off += 4
		firstTS := int64(binary.LittleEndian.Uint64(data[off : off+8]))
		off += 8
		firstVal := binary.LittleEndian.Uint64(data[off : off+8])
		off += 8
		tsBitsLen := binary.LittleEndian.Uint32(data[off : off+4])
		off += 4
		tsBytes := int((tsBitsLen + 7) / 8)
		tsBits := data[off : off+tsBytes]
		off += tsBytes
		valBitsLen := binary.LittleEndian.Uint32(data[off : off+4])
		off += 4
		valBytes := int((valBitsLen + 7) / 8)
		valBits := data[off : off+valBytes]
		off += valBytes

		tss, ok := decodeTimestamps(firstTS, int(pointCount), tsBits, tsBitsLen)
		require.True(t, ok, "ts decode")
		vals, ok := decodeValues(firstVal, int(pointCount), valBits, valBitsLen)
		require.True(t, ok, "val decode")
		out = append(out, decodedSeries{meta: meta, tss: tss, vals: vals})
	}
	return out
}

func makePoints(n int, start int64, step int64, f func(i int) float64) []point {
	pts := make([]point, n)
	t := start
	for i := 0; i < n; i++ {
		pts[i] = point{ts: t, v: f(i)}
		t += step
	}
	return pts
}

func TestEncodeSinglePoint(t *testing.T) {
	pts := []point{{ts: 1_000_000_000, v: 3.14}}
	series := map[seriesKey]*seriesBuffer{
		{metricName: "m", attributesKey: "host=h;"}: {attributes: map[string]string{"host": "h"}, points: pts},
	}
	chunks, err := buildChunks(series, 0)
	require.NoError(t, err)
	require.Len(t, chunks, 1)
	got := decodeChunk(t, chunks[0].data)
	require.Len(t, got, 1)
	assert.Equal(t, "m", got[0].meta.MetricName)
	require.Len(t, got[0].tss, 1)
	assert.Equal(t, int64(1_000_000_000), got[0].tss[0])
	assert.Equal(t, 3.14, got[0].vals[0])
}

func TestEncode100SamplesRoundTrip(t *testing.T) {
	const n = 100
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	pts := makePoints(n, base, int64(time.Second), func(i int) float64 { return float64(i) * 1.5 })
	series := map[seriesKey]*seriesBuffer{
		{metricName: "cpu", attributesKey: "host=a;"}: {attributes: map[string]string{"host": "a"}, points: pts},
	}
	chunks, err := buildChunks(series, 0)
	require.NoError(t, err)
	require.Len(t, chunks, 1)
	got := decodeChunk(t, chunks[0].data)
	require.Len(t, got, 1)
	require.Len(t, got[0].tss, n)
	for i := 0; i < n; i++ {
		assert.Equal(t, base+int64(i)*int64(time.Second), got[0].tss[i], "ts %d", i)
		assert.Equal(t, float64(i)*1.5, got[0].vals[i], "v %d", i)
	}
}

func TestEncodeRegularIntervalCompactSize(t *testing.T) {
	// 60 samples of constant value at constant 1s interval. Naive raw =
	// 60 * 16 = 960 bytes. Gorilla should pack timestamps into 1 bit each
	// after the first and values into 1 bit each after the first => the
	// payload bits should be << 200 bytes total (well under 1/4 of raw).
	const n = 60
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	pts := makePoints(n, base, int64(time.Second), func(i int) float64 { return 42 })
	series := map[seriesKey]*seriesBuffer{
		{metricName: "const", attributesKey: ""}: {attributes: map[string]string{}, points: pts},
	}
	chunks, err := buildChunks(series, 0)
	require.NoError(t, err)
	require.Len(t, chunks, 1)
	// The chunk includes a JSON metadata blob that depends on map
	// iteration order; just assert the *encoded body* (post-headers)
	// is dramatically smaller than the raw 960 bytes.
	t.Logf("constant 60-sample chunk = %d bytes", len(chunks[0].data))
	assert.Less(t, len(chunks[0].data), 240,
		"expected highly compressed chunk, got %d bytes", len(chunks[0].data))

	got := decodeChunk(t, chunks[0].data)
	require.Len(t, got, 1)
	for i := 0; i < n; i++ {
		assert.Equal(t, 42.0, got[0].vals[i])
	}
}

func TestEncodeMultipleSeriesPerMetricGroupedIntoOneChunk(t *testing.T) {
	base := time.Now().UnixNano()
	pts1 := makePoints(5, base, int64(time.Second), func(i int) float64 { return float64(i) })
	pts2 := makePoints(5, base, int64(time.Second), func(i int) float64 { return float64(i) * 2 })
	series := map[seriesKey]*seriesBuffer{
		{metricName: "cpu", attributesKey: "host=a;"}: {attributes: map[string]string{"host": "a"}, points: pts1},
		{metricName: "cpu", attributesKey: "host=b;"}: {attributes: map[string]string{"host": "b"}, points: pts2},
	}
	chunks, err := buildChunks(series, 0)
	require.NoError(t, err)
	require.Len(t, chunks, 1, "two series for same metric should land in one chunk")
	assert.Equal(t, "cpu", chunks[0].metricName)
	assert.Equal(t, 2, chunks[0].seriesCount)
}

func TestEncodeMultipleMetricsSplitChunks(t *testing.T) {
	base := time.Now().UnixNano()
	pts := makePoints(5, base, int64(time.Second), func(i int) float64 { return float64(i) })
	series := map[seriesKey]*seriesBuffer{
		{metricName: "cpu", attributesKey: ""}: {attributes: map[string]string{}, points: pts},
		{metricName: "mem", attributesKey: ""}: {attributes: map[string]string{}, points: pts},
	}
	chunks, err := buildChunks(series, 0)
	require.NoError(t, err)
	require.Len(t, chunks, 2, "different metrics should split into separate chunks")
	names := []string{chunks[0].metricName, chunks[1].metricName}
	assert.ElementsMatch(t, []string{"cpu", "mem"}, names)
}

// TestChunkHeaderByteLayoutMatchesGorillaDecoder is a regression guard for
// the v7 fix in `buildChunks`: the GORILLA1 block header must place the
// magic at [0:8], the version at [8], and the seriesCount at [9:13] —
// the exact offsets the `asap-gorilla::decoder::GorillaDecoder::from_reader`
// path reads (see `asap-gorilla/src/decoder.rs` and `block.rs::HEADER_LEN`).
// Pre-v7 the encoder wrote seriesCount at [5:9], which clobbered bytes
// 5..8 of the magic and the version byte; the consumer side rejected
// every chunk with `BadMagic`, surfacing as empty `last_over_time`
// freshness deltas. Round-tripping the chunk through the test-only
// decoder additionally proves the timestamp/value streams are intact.
func TestChunkHeaderByteLayoutMatchesGorillaDecoder(t *testing.T) {
	const n = 5
	base := time.Date(2026, 5, 7, 0, 0, 0, 0, time.UTC).UnixNano()
	pts := makePoints(n, base, int64(time.Second), func(i int) float64 { return float64(i) + 0.5 })
	series := map[seriesKey]*seriesBuffer{
		{metricName: "http_freshness_probe_emitted_at", attributesKey: "host=h;"}: {
			attributes: map[string]string{"host": "h"},
			points:     pts,
		},
		{metricName: "http_freshness_probe_emitted_at", attributesKey: "host=i;"}: {
			attributes: map[string]string{"host": "i"},
			points:     pts,
		},
	}
	chunks, err := buildChunks(series, 0)
	require.NoError(t, err)
	require.Len(t, chunks, 1, "two series of the same metric should pack into one chunk")

	data := chunks[0].data
	require.GreaterOrEqual(t, len(data), 13, "chunk must contain a 13-byte header")

	// 1. Magic at [0:8] — full "GORILLA1", not a 4-byte prefix.
	assert.Equal(t, chunkMagic, string(data[:8]),
		"magic must be at [0:8]; matches asap-gorilla::block::MAGIC + HEADER_LEN")
	// 2. Version at byte 8.
	assert.Equal(t, byte(chunkVersion), data[8],
		"version must be at byte 8; matches asap-gorilla decoder header[8]")
	// 3. seriesCount at [9:13] — the v7 fix.
	gotSeriesCount := binary.LittleEndian.Uint32(data[9:13])
	assert.Equal(t, uint32(2), gotSeriesCount,
		"seriesCount must be at [9:13]; matches asap-gorilla decoder header[9..13]")

	// 4. Negative assertion: bytes [5:9] must NOT contain the
	//    seriesCount. Pre-v7 they did, which clobbered the magic
	//    suffix + version. Asserting on the magic suffix is the
	//    cleanest way to express the invariant.
	assert.Equal(t, "LA1", string(data[5:8]),
		"bytes 5..8 must remain magic suffix; pre-v7 these were overwritten with seriesCount")
	assert.Equal(t, byte(chunkVersion), data[8],
		"byte 8 must remain version; pre-v7 the seriesCount low byte landed here")

	// 5. Round-trip the body and confirm timestamps decode at the
	//    sample values we encoded. If the writer ever drifts out of
	//    sync with the test helper, the body offset will land in a
	//    different field and either explode or return garbage.
	got := decodeChunk(t, data)
	require.Len(t, got, 2)
	for _, ds := range got {
		require.Len(t, ds.tss, n)
		for i := 0; i < n; i++ {
			assert.Equal(t, base+int64(i)*int64(time.Second), ds.tss[i], "ts %d", i)
			assert.Equal(t, float64(i)+0.5, ds.vals[i], "v %d", i)
		}
	}
}
