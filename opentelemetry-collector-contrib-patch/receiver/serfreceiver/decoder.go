// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package serfreceiver implements SERF1 binary format decompression for both
// the SerfXOR and SerfQt algorithms.
//
// SERF1 format (written by serfprocessor / serfexporter):
//   "SERF1" (5 B) + version (1 B) + series_count (4 B LE u32)
//   + per series:
//       meta_len (u16 LE) + meta_json (bytes)
//       point_count (u32 LE)
//       first_ts (u64 LE, UnixNano)
//       first_val_bits (u64 LE, IEEE 754 bits)
//       ts_bits_len (u32 LE, in bits) + ts_bytes
//       val_bits_len (u32 LE, in bits) + val_bytes
//
// Timestamp decoding: inverse Gorilla delta-of-delta.
// Value decoding (xor): inverse Serf XOR (3-case scheme).
// Value decoding (qt):  inverse SerfQt (EliasGamma + ZigZag).

package serfreceiver

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
)

// decodedPoint holds a single decoded (timestamp, value) pair.
type decodedPoint struct {
	ts int64
	v  float64
}

// decodedSeries holds metadata and decoded data for one time series.
type decodedSeries struct {
	metricName string
	attributes map[string]string
	startTS    int64
	endTS      int64
	points     []decodedPoint
}

// seriesMeta mirrors the JSON metadata struct used by the encoder.
type seriesMeta struct {
	MetricName string            `json:"metric_name"`
	Attributes map[string]string `json:"attributes"`
	StartTS    int64             `json:"start_ts"`
	EndTS      int64             `json:"end_ts"`
	PointCount int               `json:"point_count"`
}

// decodeSERF1 parses a SERF1 binary object and returns all decoded series.
func decodeSERF1(data []byte, compression string, maxDiff float64) ([]decodedSeries, error) {
	r := bytes.NewReader(data)

	// Read magic
	magic := make([]byte, 5)
	if _, err := r.Read(magic); err != nil {
		return nil, fmt.Errorf("read magic: %w", err)
	}
	if string(magic) != "SERF1" {
		return nil, fmt.Errorf("invalid magic: %q", string(magic))
	}

	// Read version (1 byte, currently 1)
	if _, err := r.ReadByte(); err != nil {
		return nil, fmt.Errorf("read version: %w", err)
	}

	// Read series count
	var seriesCount uint32
	if err := binary.Read(r, binary.LittleEndian, &seriesCount); err != nil {
		return nil, fmt.Errorf("read series count: %w", err)
	}

	result := make([]decodedSeries, 0, seriesCount)

	for i := uint32(0); i < seriesCount; i++ {
		// Read metadata JSON
		var metaLen uint16
		if err := binary.Read(r, binary.LittleEndian, &metaLen); err != nil {
			return nil, fmt.Errorf("series %d: read meta len: %w", i, err)
		}
		metaBytes := make([]byte, metaLen)
		if _, err := r.Read(metaBytes); err != nil {
			return nil, fmt.Errorf("series %d: read meta: %w", i, err)
		}
		var meta seriesMeta
		if err := json.Unmarshal(metaBytes, &meta); err != nil {
			return nil, fmt.Errorf("series %d: unmarshal meta: %w", i, err)
		}

		// Read point count
		var pointCount uint32
		if err := binary.Read(r, binary.LittleEndian, &pointCount); err != nil {
			return nil, fmt.Errorf("series %d: read point count: %w", i, err)
		}

		// Read first timestamp and first value bits
		var firstTS uint64
		if err := binary.Read(r, binary.LittleEndian, &firstTS); err != nil {
			return nil, fmt.Errorf("series %d: read first ts: %w", i, err)
		}
		var firstValBits uint64
		if err := binary.Read(r, binary.LittleEndian, &firstValBits); err != nil {
			return nil, fmt.Errorf("series %d: read first val bits: %w", i, err)
		}

		// Read timestamp bitstream
		var tsBitsLen uint32
		if err := binary.Read(r, binary.LittleEndian, &tsBitsLen); err != nil {
			return nil, fmt.Errorf("series %d: read ts bits len: %w", i, err)
		}
		tsByteLen := (tsBitsLen + 7) / 8
		tsData := make([]byte, tsByteLen)
		if tsByteLen > 0 {
			if _, err := r.Read(tsData); err != nil {
				return nil, fmt.Errorf("series %d: read ts data: %w", i, err)
			}
		}

		// Read value bitstream
		var valBitsLen uint32
		if err := binary.Read(r, binary.LittleEndian, &valBitsLen); err != nil {
			return nil, fmt.Errorf("series %d: read val bits len: %w", i, err)
		}
		valByteLen := (valBitsLen + 7) / 8
		valData := make([]byte, valByteLen)
		if valByteLen > 0 {
			if _, err := r.Read(valData); err != nil {
				return nil, fmt.Errorf("series %d: read val data: %w", i, err)
			}
		}

		if pointCount == 0 {
			result = append(result, decodedSeries{
				metricName: meta.MetricName,
				attributes: meta.Attributes,
				startTS:    meta.StartTS,
				endTS:      meta.EndTS,
			})
			continue
		}

		// Decode timestamps
		timestamps, err := decodeTimestamps(int64(firstTS), tsData, tsBitsLen, pointCount)
		if err != nil {
			return nil, fmt.Errorf("series %d: decode timestamps: %w", i, err)
		}

		// Decode values
		values, err := decodeValues(firstValBits, valData, valBitsLen, pointCount, compression, maxDiff)
		if err != nil {
			return nil, fmt.Errorf("series %d: decode values: %w", i, err)
		}

		pts := make([]decodedPoint, pointCount)
		for j := range pts {
			pts[j] = decodedPoint{ts: timestamps[j], v: values[j]}
		}

		result = append(result, decodedSeries{
			metricName: meta.MetricName,
			attributes: meta.Attributes,
			startTS:    meta.StartTS,
			endTS:      meta.EndTS,
			points:     pts,
		})
	}
	return result, nil
}

// decodeTimestamps reconstructs timestamps using inverse Gorilla delta-of-delta.
// The first timestamp is stored in the header (not the bitstream).
// Subsequent timestamps are in the bitstream.
func decodeTimestamps(firstTS int64, data []byte, bitsLen uint32, count uint32) ([]int64, error) {
	result := make([]int64, count)
	result[0] = firstTS
	if count == 1 {
		return result, nil
	}

	r := newBitReader(data, bitsLen)
	prevTS := firstTS
	prevDelta := int64(0)

	for j := uint32(1); j < count; j++ {
		bit1, err := r.readBit()
		if err != nil {
			return nil, fmt.Errorf("ts[%d]: %w", j, err)
		}
		var dd int64
		if bit1 == 0 {
			dd = 0
		} else {
			bit2, err := r.readBit()
			if err != nil {
				return nil, fmt.Errorf("ts[%d]: %w", j, err)
			}
			if bit2 == 0 {
				// 7-bit signed
				raw, err := r.readBits(7)
				if err != nil {
					return nil, fmt.Errorf("ts[%d]: %w", j, err)
				}
				dd = signExtend(raw, 7)
			} else {
				bit3, err := r.readBit()
				if err != nil {
					return nil, fmt.Errorf("ts[%d]: %w", j, err)
				}
				if bit3 == 0 {
					// 9-bit signed
					raw, err := r.readBits(9)
					if err != nil {
						return nil, fmt.Errorf("ts[%d]: %w", j, err)
					}
					dd = signExtend(raw, 9)
				} else {
					bit4, err := r.readBit()
					if err != nil {
						return nil, fmt.Errorf("ts[%d]: %w", j, err)
					}
					if bit4 == 0 {
						// 12-bit signed
						raw, err := r.readBits(12)
						if err != nil {
							return nil, fmt.Errorf("ts[%d]: %w", j, err)
						}
						dd = signExtend(raw, 12)
					} else {
						// 64-bit full
						raw, err := r.readBits(64)
						if err != nil {
							return nil, fmt.Errorf("ts[%d]: %w", j, err)
						}
						dd = int64(raw)
					}
				}
			}
		}
		delta := prevDelta + dd
		ts := prevTS + delta
		result[j] = ts
		prevTS = ts
		prevDelta = delta
	}
	return result, nil
}

// decodeValues reconstructs float64 values from the value bitstream.
func decodeValues(firstValBits uint64, data []byte, bitsLen uint32, count uint32, compression string, maxDiff float64) ([]float64, error) {
	result := make([]float64, count)

	if compression == "qt" {
		// Qt: ALL values are in the bitstream; firstValBits holds the initial prevValue (2.0).
		r := newBitReader(data, bitsLen)
		eff := maxDiff * 0.999
		step := 2 * eff
		prevValue := math.Float64frombits(firstValBits) // = 2.0
		for j := uint32(0); j < count; j++ {
			v, err := decodeQtValue(r, prevValue, step)
			if err != nil {
				return nil, fmt.Errorf("qt val[%d]: %w", j, err)
			}
			result[j] = v
			prevValue = v
		}
	} else {
		// XOR: first value from header; subsequent from bitstream.
		result[0] = math.Float64frombits(firstValBits)
		if count == 1 {
			return result, nil
		}
		r := newBitReader(data, bitsLen)
		storedVal := firstValBits
		storedLeading := 0
		storedTrailing := 0
		for j := uint32(1); j < count; j++ {
			vBits, newLead, newTrail, err := decodeXORValue(r, storedVal, storedLeading, storedTrailing)
			if err != nil {
				return nil, fmt.Errorf("xor val[%d]: %w", j, err)
			}
			result[j] = math.Float64frombits(vBits)
			storedVal = vBits
			storedLeading = newLead
			storedTrailing = newTrail
		}
	}
	return result, nil
}

// xorLeadingRepr maps 3-bit code → leading zeros count (matches encoder tables).
var xorLeadingRepr = [8]int{0, 8, 12, 16, 18, 20, 22, 24}

// xorTrailingRepr maps 3-bit code → trailing zeros count.
var xorTrailingRepr = [8]int{0, 22, 28, 32, 36, 40, 42, 46}

// decodeXORValue decodes one value from the XOR bitstream.
// Returns: decoded bits, updated storedLeading, updated storedTrailing.
func decodeXORValue(r *bitReader, storedVal uint64, storedLeading, storedTrailing int) (uint64, int, int, error) {
	bit1, err := r.readBit()
	if err != nil {
		return 0, 0, 0, err
	}

	if bit1 == 1 {
		// Case 1: reuse window
		centerBits := 64 - storedLeading - storedTrailing
		if centerBits <= 0 {
			// XOR is all zeros — same value
			return storedVal, storedLeading, storedTrailing, nil
		}
		center, err := r.readBits(centerBits)
		if err != nil {
			return 0, 0, 0, err
		}
		xor := center << uint(storedTrailing)
		return storedVal ^ xor, storedLeading, storedTrailing, nil
	}

	bit2, err := r.readBit()
	if err != nil {
		return 0, 0, 0, err
	}

	if bit2 == 1 {
		// Case 01: XOR = 0, value unchanged
		return storedVal, storedLeading, storedTrailing, nil
	}

	// Case 00: new window
	leadCode, err := r.readBits(3)
	if err != nil {
		return 0, 0, 0, err
	}
	trailCode, err := r.readBits(3)
	if err != nil {
		return 0, 0, 0, err
	}
	leading := xorLeadingRepr[leadCode]
	trailing := xorTrailingRepr[trailCode]
	centerBits := 64 - leading - trailing
	if centerBits <= 0 {
		return 0, 0, 0, errors.New("invalid center bits in case-00")
	}
	center, err := r.readBits(centerBits)
	if err != nil {
		return 0, 0, 0, err
	}
	xor := center << uint(trailing)
	return storedVal ^ xor, leading, trailing, nil
}

// decodeQtValue decodes one value using SerfQt (EliasGamma + ZigZag).
func decodeQtValue(r *bitReader, prevValue, step float64) (float64, error) {
	n, err := eliasGammaDecode(r)
	if err != nil {
		return 0, err
	}
	// Undo the +1 applied during encoding
	n = n - 1
	// ZigZag decode: (n >> 1) ^ -(n & 1)
	q := int64(n>>1) ^ -(int64(n & 1))
	return prevValue + step*float64(q), nil
}

// eliasGammaDecode reads an Elias Gamma encoded value (n >= 1).
// Format: k zero bits then n in k+1 bits (MSB first, so first bit read is the '1').
func eliasGammaDecode(r *bitReader) (uint64, error) {
	k := 0
	for {
		bit, err := r.readBit()
		if err != nil {
			return 0, err
		}
		if bit == 1 {
			break
		}
		k++
	}
	if k == 0 {
		return 1, nil
	}
	rest, err := r.readBits(k)
	if err != nil {
		return 0, err
	}
	return (uint64(1) << uint(k)) | rest, nil
}

// signExtend sign-extends a value of `bits` width to int64.
func signExtend(v uint64, bits int) int64 {
	signBit := uint64(1) << uint(bits-1)
	if v&signBit != 0 {
		return int64(v | (^uint64(0) << uint(bits)))
	}
	return int64(v)
}
