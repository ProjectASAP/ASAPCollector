// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package gorillaprocessor

import (
	gorilla "github.com/ProjectASAP/asap-gorilla-go"
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
	input := make([]gorilla.Series, 0, len(series))
	for key, buf := range series {
		if buf == nil || len(buf.points) == 0 {
			continue
		}
		points := make([]gorilla.Point, len(buf.points))
		for i, p := range buf.points {
			points[i] = gorilla.Point{TimestampUnixNano: p.ts, Value: p.v}
		}
		input = append(input, gorilla.Series{
			MetricName:    key.metricName,
			Attributes:    buf.attributes,
			AttributesKey: key.attributesKey,
			Points:        points,
		})
	}
	objects, err := gorilla.BuildObjects(input, maxObjectBytes)
	if err != nil {
		return nil, err
	}
	if len(objects) == 0 {
		return nil, nil
	}
	out := make([]gorillaObject, len(objects))
	for i, obj := range objects {
		out[i] = gorillaObject{
			data:        obj.Data,
			seriesCount: obj.SeriesCount,
			points:      obj.PointCount,
			rawBytes:    obj.RawBytes,
		}
	}
	return out, nil
}
