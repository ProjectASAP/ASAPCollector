// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package gorillaprocessor

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"sort"
)

type gorillaChunk struct {
	buf      []byte
	points   int
	rawBytes int64
}

type gorillaObject struct {
	data        []byte
	seriesCount int
	points      int
	rawBytes    int64
}

// buildObjects encodes each series using Gorilla compression and assembles them
// into binary objects with the GORILLA1 header format.
func buildObjects(
	series map[seriesKey]*seriesBuffer,
	maxObjectBytes int64,
) ([]gorillaObject, error) {
	keys := make([]seriesKey, 0, len(series))
	for k := range series {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].metricName != keys[j].metricName {
			return keys[i].metricName < keys[j].metricName
		}
		return keys[i].attributesKey < keys[j].attributesKey
	})

	chunks := make([]gorillaChunk, 0, len(keys))
	for _, key := range keys {
		buf := series[key]
		if buf == nil || len(buf.points) == 0 {
			continue
		}
		firstTS, firstValBits, tsBits, tsBitsLen, valBits, valBitsLen := sortAndEncode(buf.points)
		meta := seriesMeta{
			MetricName: key.metricName,
			Attributes: buf.attributes,
			StartTS:    buf.points[0].ts,
			EndTS:      buf.points[len(buf.points)-1].ts,
			PointCount: len(buf.points),
		}
		mb, err := json.Marshal(meta)
		if err != nil {
			return nil, fmt.Errorf("gorilla: marshal metadata: %w", err)
		}
		if len(mb) > math.MaxUint16 {
			return nil, fmt.Errorf("gorilla: metadata too large for series %s", key.metricName)
		}
		var sb bytes.Buffer
		_ = binary.Write(&sb, binary.LittleEndian, uint16(len(mb)))
		sb.Write(mb)
		_ = binary.Write(&sb, binary.LittleEndian, uint32(len(buf.points)))
		_ = binary.Write(&sb, binary.LittleEndian, uint64(firstTS))
		_ = binary.Write(&sb, binary.LittleEndian, uint64(firstValBits))
		_ = binary.Write(&sb, binary.LittleEndian, tsBitsLen)
		sb.Write(tsBits)
		_ = binary.Write(&sb, binary.LittleEndian, valBitsLen)
		sb.Write(valBits)

		chunks = append(chunks, gorillaChunk{
			buf:      sb.Bytes(),
			points:   len(buf.points),
			rawBytes: int64(len(buf.points)) * 16, // 8 bytes ts + 8 bytes val
		})
	}

	if len(chunks) == 0 {
		return nil, nil
	}
	return assembleObjects(chunks, maxObjectBytes), nil
}

// assembleObjects packs series chunks into GORILLA1 binary objects, splitting
// when maxBytes is exceeded.
func assembleObjects(chunks []gorillaChunk, maxBytes int64) []gorillaObject {
	if len(chunks) == 0 {
		return nil
	}
	const headerOverhead = 8 + 1 + 4 // "GORILLA1" + version + series count

	var objects []gorillaObject
	var cur bytes.Buffer
	curSeries := 0
	curPoints := 0
	curRaw := int64(0)
	curSize := int64(0)

	writeHeader := func() {
		cur.Reset()
		cur.WriteString("GORILLA1")
		cur.WriteByte(1) // version
		_ = binary.Write(&cur, binary.LittleEndian, uint32(0))
		curSeries = 0
		curPoints = 0
		curRaw = 0
		curSize = headerOverhead
	}
	writeHeader()

	flush := func() {
		if curSeries == 0 {
			return
		}
		buf := cur.Bytes()
		binary.LittleEndian.PutUint32(buf[9:13], uint32(curSeries))
		data := make([]byte, len(buf))
		copy(data, buf)
		objects = append(objects, gorillaObject{
			data:        data,
			seriesCount: curSeries,
			points:      curPoints,
			rawBytes:    curRaw,
		})
		writeHeader()
	}

	for _, chunk := range chunks {
		if maxBytes > 0 && curSeries > 0 && curSize+int64(len(chunk.buf)) > maxBytes {
			flush()
		}
		cur.Write(chunk.buf)
		curSeries++
		curPoints += chunk.points
		curRaw += chunk.rawBytes
		curSize += int64(len(chunk.buf))

		if maxBytes > 0 && curSeries > 0 && curSize >= maxBytes {
			flush()
		}
	}
	flush()

	return objects
}
