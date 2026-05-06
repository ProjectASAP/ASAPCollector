// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package gorillas3processor

import (
	"sort"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// windowState is the in-memory tumbling-window aggregator. It keeps
// per-(metric, label_set) buffers and tracks the bounding timestamps
// for the current window.
type windowState struct {
	mu       sync.Mutex
	series   map[seriesKey]*seriesBuffer
	earliest time.Time
	latest   time.Time
}

func newWindowState() *windowState {
	return &windowState{series: make(map[seriesKey]*seriesBuffer)}
}

// add records one sample for the given metric/attributes.
func (w *windowState) add(metricName string, attrs pcommon.Map, ts time.Time, v float64) {
	attrsKey := encodeAttributesAsKey(attrs)
	sk := seriesKey{metricName: metricName, attributesKey: attrsKey}

	w.mu.Lock()
	defer w.mu.Unlock()

	buf, ok := w.series[sk]
	if !ok {
		buf = &seriesBuffer{
			attributes: attributesToMap(attrs),
			points:     make([]point, 0, 64),
		}
		w.series[sk] = buf
	}
	buf.points = append(buf.points, point{ts: ts.UnixNano(), v: v})

	if w.earliest.IsZero() || ts.Before(w.earliest) {
		w.earliest = ts
	}
	if ts.After(w.latest) {
		w.latest = ts
	}
}

// snapshot atomically swaps the current series state for an empty one
// and returns the captured map plus the bounding timestamps.
func (w *windowState) snapshot() (map[seriesKey]*seriesBuffer, time.Time, time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.series) == 0 {
		return nil, time.Time{}, time.Time{}
	}
	snap := w.series
	earliest := w.earliest
	latest := w.latest
	w.series = make(map[seriesKey]*seriesBuffer)
	w.earliest = time.Time{}
	w.latest = time.Time{}
	return snap, earliest, latest
}

// activeSeries returns the number of distinct series currently buffered.
func (w *windowState) activeSeries() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return int64(len(w.series))
}

// encodeAttributesAsKey produces a deterministic canonical string from
// an OTel attribute map.
func encodeAttributesAsKey(attrs pcommon.Map) string {
	keys := make([]string, 0, attrs.Len())
	attrs.Range(func(k string, _ pcommon.Value) bool {
		keys = append(keys, k)
		return true
	})
	sort.Strings(keys)
	var sb strings.Builder
	for _, k := range keys {
		v, _ := attrs.Get(k)
		sb.WriteString(k)
		sb.WriteString("=")
		sb.WriteString(v.AsString())
		sb.WriteString(";")
	}
	return sb.String()
}

func attributesToMap(attrs pcommon.Map) map[string]string {
	m := make(map[string]string, attrs.Len())
	attrs.Range(func(k string, v pcommon.Value) bool {
		m[k] = v.AsString()
		return true
	})
	return m
}
