package precompute

import (
	"sort"
	"strconv"
	"strings"
	"sync"
)

// MatchOp picks the comparison operator for a LabelMatcher.
type MatchOp uint8

const (
	// MatchEqual requires v == matcher.Value.
	MatchEqual MatchOp = iota
	// MatchNotEqual requires v != matcher.Value.
	MatchNotEqual
)

// String returns the operator's debug name.
func (op MatchOp) String() string {
	switch op {
	case MatchEqual:
		return "="
	case MatchNotEqual:
		return "!="
	}
	return "?"
}

// LabelMatcher selects observations by metric name (Name == "")
// or label key (Name != ""). Op defaults to MatchEqual.
//
// Today's per-processor `LabelMatchers` config uses (Key, Value)
// equality only; the Op field is forward-compat for regex/glob.
type LabelMatcher struct {
	// Name is the label key to match against, or the empty string
	// to match the metric name field.
	Name string
	// Value is the exact target value.
	Value string
	// Op picks Equal vs NotEqual.
	Op MatchOp
}

// Matches returns true iff the observation satisfies all matchers.
//
// Semantics replicate today's per-processor matchesMatchers:
//   - Empty matcher list ⇒ accept all.
//   - For each matcher, look up the value (label or metric-name).
//   - For MatchEqual: missing key fails the match. Mismatched
//     value fails. Match-equal-empty-string against missing
//     key behaves like the existing OTel processor: missing key
//     returns false.
//   - For MatchNotEqual: missing key passes (no value to disagree
//     with); present-and-equal fails.
func (cfg *PrecomputeConfig) Matches(obs *Observation) bool {
	if cfg == nil {
		return true
	}
	if len(cfg.Matchers) == 0 {
		return true
	}
	for _, m := range cfg.Matchers {
		var v string
		var present bool
		if m.Name == "" {
			v = obs.Metric
			present = obs.Metric != ""
		} else {
			v, present = lookupLabel(obs.Labels, m.Name)
		}
		switch m.Op {
		case MatchEqual:
			if !present || v != m.Value {
				return false
			}
		case MatchNotEqual:
			if present && v == m.Value {
				return false
			}
		}
	}
	return true
}

// lookupLabel returns the value for key in labels and whether it was
// present. Linear scan; label sets are small (typically <10) so this
// beats building a map for ephemeral matching.
func lookupLabel(labels []KeyValue, key string) (string, bool) {
	for i := range labels {
		if labels[i].Key == key {
			return labels[i].Value, true
		}
	}
	return "", false
}

// SeriesKey produces a stable string identifying the
// (agg_id, label_key) series for storage and snapshot lookup.
//
// Output format MUST stay byte-identical to today's per-processor
// seriesKey/attributesKey output for the same input — that's how
// the snapshot cache survives the Phase 2 refactor (ADR-0002
// "Behavior preservation").
//
// Today's format (see ddsketchprocessor.go::seriesKey +
// ::attributesKey):
//
//	with aggregateBy:    "k1=v1;k2=v2;"  for the listed keys, missing keys skipped
//	without aggregateBy: "k1=v1;k2=v2;"  for ALL labels, sorted by key
//
// SeriesKey prepends the agg_id as a stable per-Precompute prefix
// so the same global series-key namespace can hold series from
// multiple Precompute instances. Adapters that need the bare
// attribute key for snapshot lookup can call AttributesKey directly.
func SeriesKey(aggID AggId, resourceLabels, labels []KeyValue, aggregateBy []string) string {
	var b strings.Builder
	b.WriteString(strconv.FormatUint(uint64(aggID), 10))
	b.WriteByte('|')
	// Resource attributes always go in the key as a flat sorted
	// segment, mirroring today's processor's
	// `attributesKey(rm.Resource().Attributes())` byte-identical.
	// AggregateBy never filters resource attrs — only data-point
	// attrs.
	writeAttributesKey(&b, resourceLabels, nil)
	b.WriteByte('|')
	writeAttributesKey(&b, labels, aggregateBy)
	return b.String()
}

// AttributesKey returns just the per-label-set portion of the
// SeriesKey, byte-identical to today's per-processor
// attributesKey/seriesKey.
//
// When aggregateBy is empty, all labels are included sorted by
// key; otherwise only the listed keys are included in their
// AggregateBy order (which today's processors call out as
// "already sorted by validate"). Missing keys are skipped (no
// "k=;" placeholder), matching today's behavior in
// ddsketchprocessor.seriesKey.
func AttributesKey(labels []KeyValue, aggregateBy []string) string {
	var b strings.Builder
	writeAttributesKey(&b, labels, aggregateBy)
	return b.String()
}

// seriesKeyScratch is a reusable buffer for building a SeriesKey
// without the per-observation string allocation strings.Builder
// incurs. The agent's hot path (windowState.observe) builds one key
// per scalar observation purely to look up an existing series — at
// 30K series × ~100 Hz that's ~3M throwaway key strings/sec. Building
// into a pooled []byte lets the lookup use the compiler's zero-alloc
// `m[string(b)]` form; the retained key string is materialized only
// when a new series is admitted.
//
// Output is byte-identical to SeriesKey — the two MUST agree so that
// serializeSeries's SeriesKeyForEntry rebuilds the same map key.
type seriesKeyScratch struct {
	buf  []byte
	keys []string
}

var seriesKeyScratchPool = sync.Pool{New: func() any { return &seriesKeyScratch{} }}

func getSeriesKeyScratch() *seriesKeyScratch {
	s := seriesKeyScratchPool.Get().(*seriesKeyScratch)
	s.buf = s.buf[:0]
	return s
}

func putSeriesKeyScratch(s *seriesKeyScratch) { seriesKeyScratchPool.Put(s) }

// appendAttributesKey mirrors writeAttributesKey but appends to the
// scratch's byte buffer, reusing the keys slice for the sort.
func (s *seriesKeyScratch) appendAttributesKey(labels []KeyValue, aggregateBy []string) {
	if len(aggregateBy) == 0 {
		s.keys = s.keys[:0]
		for i := range labels {
			s.keys = append(s.keys, labels[i].Key)
		}
		sort.Strings(s.keys)
		for _, k := range s.keys {
			v, ok := lookupLabel(labels, k)
			if !ok {
				continue
			}
			s.buf = append(s.buf, k...)
			s.buf = append(s.buf, '=')
			s.buf = append(s.buf, v...)
			s.buf = append(s.buf, ';')
		}
		return
	}
	for _, k := range aggregateBy {
		v, ok := lookupLabel(labels, k)
		if !ok {
			continue
		}
		s.buf = append(s.buf, k...)
		s.buf = append(s.buf, '=')
		s.buf = append(s.buf, v...)
		s.buf = append(s.buf, ';')
	}
}

// appendSeriesKey is the byte-buffer twin of SeriesKey.
func (s *seriesKeyScratch) appendSeriesKey(aggID AggId, resourceLabels, labels []KeyValue, aggregateBy []string) {
	s.buf = strconv.AppendUint(s.buf, uint64(aggID), 10)
	s.buf = append(s.buf, '|')
	s.appendAttributesKey(resourceLabels, nil)
	s.buf = append(s.buf, '|')
	s.appendAttributesKey(labels, aggregateBy)
}

func writeAttributesKey(b *strings.Builder, labels []KeyValue, aggregateBy []string) {
	if len(aggregateBy) == 0 {
		// Sort by key for stable output. Today's processor uses
		// sort.Strings on the collected keys; we replicate that.
		keys := make([]string, 0, len(labels))
		for i := range labels {
			keys = append(keys, labels[i].Key)
		}
		sort.Strings(keys)
		for _, k := range keys {
			v, ok := lookupLabel(labels, k)
			if !ok {
				continue
			}
			b.WriteString(k)
			b.WriteByte('=')
			b.WriteString(v)
			b.WriteByte(';')
		}
		return
	}
	// AggregateBy already sorted by validate at config time, per
	// today's per-processor convention. We preserve the caller's
	// order rather than re-sort, so legacy callers that sort
	// upstream get bit-identical keys.
	for _, k := range aggregateBy {
		v, ok := lookupLabel(labels, k)
		if !ok {
			continue
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(v)
		b.WriteByte(';')
	}
}

// SeriesAttrs filters labels to only those in aggregateBy,
// preserving aggregateBy order. Mirrors today's seriesAttrs in
// each processor: when aggregateBy is empty, returns a copy of all
// labels; otherwise returns only the listed-and-present ones.
func SeriesAttrs(labels []KeyValue, aggregateBy []string) []KeyValue {
	if len(aggregateBy) == 0 {
		out := make([]KeyValue, len(labels))
		copy(out, labels)
		return out
	}
	out := make([]KeyValue, 0, len(aggregateBy))
	for _, k := range aggregateBy {
		if v, ok := lookupLabel(labels, k); ok {
			out = append(out, KeyValue{Key: k, Value: v})
		}
	}
	return out
}
