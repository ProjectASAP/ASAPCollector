// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"sort"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// sumAggregator is the asap-native replacement for contrib metricstransform's
// aggregate_labels/sum. It groups (delta) values by the configured
// AggregateBy label set and sums them — the per-shard half of the
// sum-by-zone edge aggregation. There is NO json.Marshal / reflection / map
// AsRaw per data point (the metricstransform path that cost ~17% of agent
// CPU): the group key is a small sorted "k=v;" byte string over only the
// AggregateBy labels.
//
// Sum is associative, so each shard keeps its own partial sums and the
// processor merges partials across shards by group key at flush, then emits
// one delta Sum data point per group — byte/semantically identical to what
// metricstransform emits today (backend unchanged).
type sumAggregator struct {
	// aggregateBy is the (already config-time) label set to group by, e.g.
	// [zone]. Empty groups everything into a single series.
	aggregateBy []string
	groups      map[string]*sumGroup
}

type sumGroup struct {
	labels []kv // the AggregateBy label values identifying this group
	sum    float64
	count  uint64
}

// kv is a minimal label pair (avoids importing precompute just for KeyValue).
type kv struct {
	k string
	v string
}

func newSumAggregator(aggregateBy []string) *sumAggregator {
	// Copy + sort aggregateBy for a stable group-key layout.
	ab := append([]string(nil), aggregateBy...)
	sort.Strings(ab)
	return &sumAggregator{aggregateBy: ab, groups: make(map[string]*sumGroup)}
}

// observe adds value to the group identified by attrs filtered to
// aggregateBy. attrs is the data point's already-decoded attribute map
// (decoded ONCE by the processor and shared with the cold + sketch paths).
func (s *sumAggregator) observe(attrs map[string]string, value float64) {
	// Build the group key (sorted "k=v;" over aggregateBy) into a small
	// builder — only the aggregateBy labels, not all attributes.
	var b strings.Builder
	grp := make([]kv, 0, len(s.aggregateBy))
	for _, k := range s.aggregateBy { // aggregateBy is pre-sorted
		vs, ok := attrs[k]
		if !ok {
			continue
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(vs)
		b.WriteByte(';')
		grp = append(grp, kv{k: k, v: vs})
	}
	key := b.String()
	g := s.groups[key]
	if g == nil {
		g = &sumGroup{labels: grp}
		s.groups[key] = g
	}
	g.sum += value
	g.count++
}

func (s *sumAggregator) reset() {
	s.groups = make(map[string]*sumGroup)
}

// mergeSumGroups folds per-shard sum partials (same metric) into one map
// keyed by group key. Caller drains every shard's aggregator for a metric.
func mergeSumGroups(dst map[string]*sumGroup, src map[string]*sumGroup) {
	for key, g := range src {
		d := dst[key]
		if d == nil {
			d = &sumGroup{labels: g.labels}
			dst[key] = d
		}
		d.sum += g.sum
		d.count += g.count
	}
}

// emitSumMetric appends a delta Sum metric named metricName to md, with one
// data point per merged group (attributes = the group's AggregateBy labels,
// value = summed delta, count = sample count). Matches the metricstransform
// aggregate_labels/sum output the backend ingests.
func emitSumMetric(md pmetric.Metrics, metricName string, groups map[string]*sumGroup, startMs, endMs uint64) {
	if len(groups) == 0 {
		return
	}
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName(metricName)
	sum := m.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	sum.SetIsMonotonic(true)
	dps := sum.DataPoints()
	for _, g := range groups {
		dp := dps.AppendEmpty()
		dp.SetStartTimestamp(pcommon.Timestamp(startMs * 1e6))
		dp.SetTimestamp(pcommon.Timestamp(endMs * 1e6))
		dp.SetDoubleValue(g.sum)
		attrs := dp.Attributes()
		for _, l := range g.labels {
			attrs.PutStr(l.k, l.v)
		}
	}
}
