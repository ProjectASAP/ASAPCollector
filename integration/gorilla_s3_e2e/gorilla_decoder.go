// In-test Go decoder for the GORILLA1 block format.
//
// Mirrors the byte layout the Phase-2 `gorillas3processor` writes
// (see opentelemetry-collector-contrib-patch/processor/
// gorillas3processor/encoder.go::buildChunks +
// encodeSeriesBody + bitwriter.go) and the Phase-1 `asap-gorilla`
// Rust crate decodes (asap-gorilla/src/decoder.rs).
//
// Why a SECOND decoder instead of importing the existing two?
//
//   * The Go gorillas3processor lives in
//     opentelemetry-collector-contrib-patch/processor/, which has a
//     long replace-directive cascade (patched pdata, patched
//     processor core, sibling sketch processors). Importing it from
//     this test would either drag the whole cascade in, or risk
//     drifting if the replacements change. Phase 6 wants the
//     byte-format CONTRACT pinned in this PR's source — and a
//     fresh, header-only decoder is exactly that pin.
//   * The Rust `asap-gorilla` crate is the production decoder the
//     Phase-3 GorillaS3ColdStore relies on; cross-checking against
//     it is the *separate* CrossLanguageByteCompat subtest, which
//     shells out via `cargo test`.
//
// What this decoder enforces:
//
//   * Outer header  : 8-byte "GORILLA1" magic, 1-byte version=1,
//                     uint32 LE seriesCount.
//   * Per-series    : uint16 LE metaLen, JSON metadata, uint32 LE
//                     pointCount, uint64 LE firstTS, uint64 LE
//                     firstValBits, uint32 LE tsBitsLen, raw ts
//                     bits, uint32 LE valBitsLen, raw val bits.
//   * Bit decoding  : timestamps via Gorilla delta-of-delta
//                     (0 / 10+7 / 110+9 / 1110+12 / 1111+64),
//                     values via Gorilla XOR (1-bit zero-flag,
//                     1-bit reuse-window, 5-bit lz, 6-bit sig-1).
//
// The decoder returns the full sample stream; the test compares
// it byte-for-byte against the input fixture.

package gorillas3e2e

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
)

const (
	gorillaMagic   = "GORILLA1"
	gorillaVersion = 1
	headerLen      = 8 + 1 + 4 // magic + version + seriesCount
)

// SeriesMeta is the JSON metadata header that precedes every
// per-series body. Field tags must match
// `gorillas3processor/encoder.go::seriesMeta` and
// `asap-gorilla/src/block.rs::SeriesMeta` exactly.
type SeriesMeta struct {
	MetricName string            `json:"metric_name"`
	Attributes map[string]string `json:"attributes"`
	StartTS    int64             `json:"start_ts"`
	EndTS      int64             `json:"end_ts"`
	PointCount int               `json:"point_count"`
}

// DecodedSeries is one fully-decoded series in a GORILLA1 block.
type DecodedSeries struct {
	Meta    SeriesMeta
	Samples []Sample
}

// Sample is a single (UnixNano, value) point.
type Sample struct {
	TS    int64
	Value float64
}

// DecodeBlock parses one GORILLA1 block from `data` and returns
// every series with its decoded samples. Returns an error on header
// mismatch, truncated body, or sample-count divergence between the
// metadata's declared PointCount and the bit-stream's actual yield.
func DecodeBlock(data []byte) ([]DecodedSeries, error) {
	if len(data) < headerLen {
		return nil, fmt.Errorf("gorilla1: block too short (%d < %d)", len(data), headerLen)
	}
	if string(data[:8]) != gorillaMagic {
		return nil, fmt.Errorf("gorilla1: bad magic %q (want %q)", string(data[:8]), gorillaMagic)
	}
	if data[8] != gorillaVersion {
		return nil, fmt.Errorf("gorilla1: unsupported version %d (want %d)", data[8], gorillaVersion)
	}
	seriesCount := binary.LittleEndian.Uint32(data[9:13])
	r := bytes.NewReader(data[headerLen:])

	out := make([]DecodedSeries, 0, seriesCount)
	for i := uint32(0); i < seriesCount; i++ {
		s, err := decodeSeriesBody(r)
		if err != nil {
			return nil, fmt.Errorf("gorilla1: series #%d: %w", i, err)
		}
		out = append(out, s)
	}
	return out, nil
}

func decodeSeriesBody(r *bytes.Reader) (DecodedSeries, error) {
	var metaLen uint16
	if err := binary.Read(r, binary.LittleEndian, &metaLen); err != nil {
		return DecodedSeries{}, fmt.Errorf("read metaLen: %w", err)
	}
	metaBytes := make([]byte, metaLen)
	if _, err := io.ReadFull(r, metaBytes); err != nil {
		return DecodedSeries{}, fmt.Errorf("read meta: %w", err)
	}
	var meta SeriesMeta
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return DecodedSeries{}, fmt.Errorf("parse meta: %w", err)
	}

	var pointCount uint32
	if err := binary.Read(r, binary.LittleEndian, &pointCount); err != nil {
		return DecodedSeries{}, fmt.Errorf("read pointCount: %w", err)
	}
	if int(pointCount) != meta.PointCount {
		return DecodedSeries{}, fmt.Errorf(
			"meta.point_count=%d disagrees with body pointCount=%d",
			meta.PointCount, pointCount)
	}

	var firstTS int64
	if err := binary.Read(r, binary.LittleEndian, &firstTS); err != nil {
		return DecodedSeries{}, fmt.Errorf("read firstTS: %w", err)
	}
	var firstValBits uint64
	if err := binary.Read(r, binary.LittleEndian, &firstValBits); err != nil {
		return DecodedSeries{}, fmt.Errorf("read firstValBits: %w", err)
	}

	var tsBitsLen uint32
	if err := binary.Read(r, binary.LittleEndian, &tsBitsLen); err != nil {
		return DecodedSeries{}, fmt.Errorf("read tsBitsLen: %w", err)
	}
	tsByteLen := (int(tsBitsLen) + 7) / 8
	tsBits := make([]byte, tsByteLen)
	if _, err := io.ReadFull(r, tsBits); err != nil {
		return DecodedSeries{}, fmt.Errorf("read tsBits: %w", err)
	}

	var valBitsLen uint32
	if err := binary.Read(r, binary.LittleEndian, &valBitsLen); err != nil {
		return DecodedSeries{}, fmt.Errorf("read valBitsLen: %w", err)
	}
	valByteLen := (int(valBitsLen) + 7) / 8
	valBits := make([]byte, valByteLen)
	if _, err := io.ReadFull(r, valBits); err != nil {
		return DecodedSeries{}, fmt.Errorf("read valBits: %w", err)
	}

	timestamps, err := decodeTimestamps(tsBits, int(tsBitsLen), int(pointCount), firstTS)
	if err != nil {
		return DecodedSeries{}, fmt.Errorf("decode timestamps: %w", err)
	}
	values, err := decodeValues(valBits, int(valBitsLen), int(pointCount), firstValBits)
	if err != nil {
		return DecodedSeries{}, fmt.Errorf("decode values: %w", err)
	}

	samples := make([]Sample, len(timestamps))
	for i := range timestamps {
		samples[i] = Sample{TS: timestamps[i], Value: values[i]}
	}
	return DecodedSeries{Meta: meta, Samples: samples}, nil
}

// bitReader pulls bits MSB-first out of a byte slice. Mirrors
// bitwriter.go's MSB-first writer.
type bitReader struct {
	buf  []byte
	bit  int // total bits consumed
	nMax int // total bits available
}

func newBitReader(buf []byte, nBitsAvail int) *bitReader {
	return &bitReader{buf: buf, bit: 0, nMax: nBitsAvail}
}

func (br *bitReader) readBit() (uint8, error) {
	if br.bit >= br.nMax {
		return 0, io.EOF
	}
	byteIdx := br.bit / 8
	bitInByte := uint(7 - (br.bit % 8))
	b := (br.buf[byteIdx] >> bitInByte) & 1
	br.bit++
	return b, nil
}

func (br *bitReader) readBits(n int) (uint64, error) {
	if n == 0 {
		return 0, nil
	}
	if n < 0 || n > 64 {
		return 0, fmt.Errorf("bad readBits n=%d", n)
	}
	var out uint64
	for i := 0; i < n; i++ {
		b, err := br.readBit()
		if err != nil {
			return 0, err
		}
		out = (out << 1) | uint64(b)
	}
	return out, nil
}

// signExtend treats v's low n bits as a two's-complement signed
// integer and sign-extends to int64. Mirrors the Go encoder's
// `uint64(dd) & ((1<<n)-1)` write path: the high bit of the n-bit
// window is the sign bit.
func signExtend(v uint64, n int) int64 {
	if n == 0 || n >= 64 {
		return int64(v)
	}
	mask := uint64(1) << uint(n-1)
	if v&mask != 0 {
		// negative — sign-extend by ORing in the high bits.
		v |= ^uint64(0) << uint(n)
	}
	return int64(v)
}

// decodeTimestamps inverts gorillaTimestampEncoder.push.
//
// First sample's TS is `firstTS` (carried in the series header as a
// raw uint64 LE — NOT in the bit stream). Subsequent samples are
// reconstructed by reading the variable-length bucket header
// (0 / 10 / 110 / 1110 / 1111) and then the dd payload.
func decodeTimestamps(buf []byte, nBits int, pointCount int, firstTS int64) ([]int64, error) {
	out := make([]int64, 0, pointCount)
	if pointCount == 0 {
		return out, nil
	}
	out = append(out, firstTS)
	if pointCount == 1 {
		return out, nil
	}
	br := newBitReader(buf, nBits)
	prevTS := firstTS
	prevDelta := int64(0)
	for i := 1; i < pointCount; i++ {
		bucketBits, err := readBucketHeader(br)
		if err != nil {
			return nil, fmt.Errorf("ts bucket header @sample %d: %w", i, err)
		}
		var dd int64
		switch bucketBits {
		case 0:
			dd = 0
		case 7, 9, 12:
			raw, err := br.readBits(bucketBits)
			if err != nil {
				return nil, fmt.Errorf("ts dd bits @sample %d: %w", i, err)
			}
			dd = signExtend(raw, bucketBits)
		case 64:
			raw, err := br.readBits(64)
			if err != nil {
				return nil, fmt.Errorf("ts dd 64 @sample %d: %w", i, err)
			}
			dd = int64(raw)
		default:
			return nil, fmt.Errorf("unexpected bucket width %d @sample %d", bucketBits, i)
		}
		delta := prevDelta + dd
		ts := prevTS + delta
		out = append(out, ts)
		prevTS = ts
		prevDelta = delta
	}
	return out, nil
}

// readBucketHeader reads a Gorilla delta-of-delta bucket header and
// returns the WIDTH (in bits) of the dd payload that follows: 0
// (no payload), 7, 9, 12, or 64.
func readBucketHeader(br *bitReader) (int, error) {
	b0, err := br.readBit()
	if err != nil {
		return 0, err
	}
	if b0 == 0 {
		return 0, nil // bucket "0": dd == 0
	}
	b1, err := br.readBit()
	if err != nil {
		return 0, err
	}
	if b1 == 0 {
		return 7, nil // bucket "10": 7 bits
	}
	b2, err := br.readBit()
	if err != nil {
		return 0, err
	}
	if b2 == 0 {
		return 9, nil // bucket "110": 9 bits
	}
	b3, err := br.readBit()
	if err != nil {
		return 0, err
	}
	if b3 == 0 {
		return 12, nil // bucket "1110": 12 bits
	}
	return 64, nil // bucket "1111": 64 bits
}

// decodeValues inverts gorillaValueEncoder.push.
//
// First value bits come from the series header (`firstValBits`).
// Subsequent values follow the Gorilla XOR rules:
//
//   * bit 0 == 0          → value unchanged from previous.
//   * bit 0 == 1, bit 1 == 0 → reuse the previous (lz, sig) window;
//                              read `sig` significant bits, shift
//                              into position by `tz = 64 - lz - sig`.
//   * bit 0 == 1, bit 1 == 1 → read 5-bit lz, 6-bit (sig-1), then
//                              `sig` significant bits, and replace
//                              the prev window.
func decodeValues(buf []byte, nBits int, pointCount int, firstValBits uint64) ([]float64, error) {
	out := make([]float64, 0, pointCount)
	if pointCount == 0 {
		return out, nil
	}
	out = append(out, math.Float64frombits(firstValBits))
	if pointCount == 1 {
		return out, nil
	}
	br := newBitReader(buf, nBits)
	prev := firstValBits
	var leadingZeros, trailingZeros uint8
	havePrevWindow := false
	for i := 1; i < pointCount; i++ {
		b0, err := br.readBit()
		if err != nil {
			return nil, fmt.Errorf("val flag @sample %d: %w", i, err)
		}
		if b0 == 0 {
			out = append(out, math.Float64frombits(prev))
			continue
		}
		b1, err := br.readBit()
		if err != nil {
			return nil, fmt.Errorf("val window-flag @sample %d: %w", i, err)
		}
		if b1 == 0 {
			if !havePrevWindow {
				return nil, fmt.Errorf(
					"val sample %d wants reuse-window but none has been written",
					i)
			}
			sig := 64 - int(leadingZeros) - int(trailingZeros)
			x, err := br.readBits(sig)
			if err != nil {
				return nil, fmt.Errorf("val sig bits @sample %d: %w", i, err)
			}
			x <<= uint(trailingZeros)
			cur := prev ^ x
			out = append(out, math.Float64frombits(cur))
			prev = cur
			continue
		}
		// New window: 5-bit lz, 6-bit sig-1, sig bits.
		lz, err := br.readBits(5)
		if err != nil {
			return nil, fmt.Errorf("val lz @sample %d: %w", i, err)
		}
		sigMinus1, err := br.readBits(6)
		if err != nil {
			return nil, fmt.Errorf("val sig @sample %d: %w", i, err)
		}
		sig := int(sigMinus1) + 1
		x, err := br.readBits(sig)
		if err != nil {
			return nil, fmt.Errorf("val sig bits @sample %d: %w", i, err)
		}
		tz := 64 - int(lz) - sig
		if tz < 0 {
			return nil, fmt.Errorf(
				"val sample %d: invalid window lz=%d sig=%d (tz=%d)",
				i, lz, sig, tz)
		}
		x <<= uint(tz)
		cur := prev ^ x
		out = append(out, math.Float64frombits(cur))
		prev = cur
		leadingZeros = uint8(lz)
		trailingZeros = uint8(tz)
		havePrevWindow = true
	}
	return out, nil
}
