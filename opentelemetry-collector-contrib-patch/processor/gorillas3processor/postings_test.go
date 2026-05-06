// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package gorillas3processor

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildPostings_FrameAndHeader(t *testing.T) {
	series := map[seriesKey]*seriesBuffer{
		{metricName: "http_requests_total", attributesKey: "service=api"}: {
			attributes: map[string]string{"service": "api", "zone": "a"},
			points:     []point{{ts: 1, v: 1.0}},
		},
		{metricName: "http_requests_total", attributesKey: "service=web"}: {
			attributes: map[string]string{"service": "web", "zone": "b"},
			points:     []point{{ts: 2, v: 2.0}},
		},
	}
	body, err := buildPostings(series, 12345)
	require.NoError(t, err)

	// Magic + version.
	require.GreaterOrEqual(t, len(body), 8+1+4+4)
	assert.Equal(t, []byte(postingsMagic), body[:8])
	assert.Equal(t, postingsVersion, body[8])

	bodyLen := binary.LittleEndian.Uint32(body[9:13])
	require.Equal(t, int(bodyLen)+8+1+4+4, len(body),
		"postings frame must be magic(8) + ver(1) + len(4) + body + crc(4)")

	// CRC of body must match.
	jsonBody := body[13 : 13+bodyLen]
	storedCRC := binary.LittleEndian.Uint32(body[13+bodyLen:])
	assert.Equal(t, crc32.ChecksumIEEE(jsonBody), storedCRC, "postings CRC32 over body must match trailer")

	// JSON body parses + has expected entries.
	var parsed postingsBody
	require.NoError(t, json.Unmarshal(jsonBody, &parsed))
	assert.Equal(t, postingsVersion, parsed.SchemaVersion)
	assert.Equal(t, uint64(12345), parsed.GeneratedAtNS)
	// Synthetic __name__ posting + the 2 user labels per series.
	// Two series share __name__=http_requests_total → that posting
	// list has 2 ids; service=api/web each have 1 id; zone=a/b each
	// have 1 id.
	got := map[string]int{}
	for _, e := range parsed.Entries {
		got[e.LabelName+"="+e.LabelValue] = len(e.SeriesIDs)
	}
	assert.Equal(t, 2, got["__name__=http_requests_total"])
	assert.Equal(t, 1, got["service=api"])
	assert.Equal(t, 1, got["service=web"])
	assert.Equal(t, 1, got["zone=a"])
	assert.Equal(t, 1, got["zone=b"])
}

func TestBuildPostings_Deterministic(t *testing.T) {
	// Two semantically identical series maps in different iteration
	// orders must yield byte-identical postings frames (modulo the
	// generated_at_ns timestamp, which we hold equal here).
	mk := func() map[seriesKey]*seriesBuffer {
		return map[seriesKey]*seriesBuffer{
			{metricName: "m", attributesKey: "a=1"}: {
				attributes: map[string]string{"a": "1"},
				points:     []point{{ts: 1, v: 1}},
			},
			{metricName: "m", attributesKey: "b=2"}: {
				attributes: map[string]string{"b": "2"},
				points:     []point{{ts: 1, v: 1}},
			},
			{metricName: "m", attributesKey: "c=3"}: {
				attributes: map[string]string{"c": "3"},
				points:     []point{{ts: 1, v: 1}},
			},
		}
	}
	b1, err := buildPostings(mk(), 999)
	require.NoError(t, err)
	b2, err := buildPostings(mk(), 999)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(b1, b2), "buildPostings must be deterministic")
}

func TestBuildPostings_SkipsEmptySeries(t *testing.T) {
	series := map[seriesKey]*seriesBuffer{
		{metricName: "m", attributesKey: "good"}: {
			attributes: map[string]string{"a": "1"},
			points:     []point{{ts: 1, v: 1}},
		},
		{metricName: "m", attributesKey: "empty"}: {
			attributes: map[string]string{"b": "2"},
			points:     nil,
		},
		{metricName: "m", attributesKey: "nilbuf"}: nil,
	}
	body, err := buildPostings(series, 0)
	require.NoError(t, err)
	bodyLen := binary.LittleEndian.Uint32(body[9:13])
	jsonBody := body[13 : 13+bodyLen]
	var parsed postingsBody
	require.NoError(t, json.Unmarshal(jsonBody, &parsed))
	got := map[string]bool{}
	for _, e := range parsed.Entries {
		got[e.LabelName+"="+e.LabelValue] = true
	}
	assert.True(t, got["a=1"], "non-empty series must contribute postings")
	assert.False(t, got["b=2"], "empty buffer must not contribute postings")
}
