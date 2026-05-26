package precompute

import (
	"sort"
	"strconv"
	"strings"
	"testing"
)

// legacyAttributesKey replicates the ddsketch processor's
// `attributesKey(attrs pcommon.Map) string` byte layout. Reproduced
// inline (not imported) so the test catches drift even without the
// processor on the path.
//
//	keys := sort.Strings(map.keys)
//	for k in keys: builder.write(k + "=" + v + ";")
func legacyAttributesKey(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		v, ok := labels[k]
		if !ok {
			continue
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(v)
		b.WriteByte(';')
	}
	return b.String()
}

// legacySeriesKey replicates the ddsketch processor's
// `seriesKey(attrs pcommon.Map) string` byte layout. When
// aggregateBy is set, only the listed keys are included (in
// aggregateBy order). When empty, the full sorted attribute set is
// included.
func legacySeriesKey(labels map[string]string, aggregateBy []string) string {
	if len(aggregateBy) == 0 {
		return legacyAttributesKey(labels)
	}
	var b strings.Builder
	for _, k := range aggregateBy {
		v, ok := labels[k]
		if !ok {
			continue
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(v)
		b.WriteByte(';')
	}
	return b.String()
}

// legacyFullKey replicates today's per-processor combined key:
// resourceAttrsKey + "|" + perSeriesKey, plus an outer agg-id prefix
// supplied by the runtime. Format: "<aggID>|<resKey>|<seriesKey>".
func legacyFullKey(aggID AggId, resAttrs, dpAttrs map[string]string, aggregateBy []string) string {
	var b strings.Builder
	b.WriteString(strconv.FormatUint(uint64(aggID), 10))
	b.WriteByte('|')
	b.WriteString(legacyAttributesKey(resAttrs))
	b.WriteByte('|')
	b.WriteString(legacySeriesKey(dpAttrs, aggregateBy))
	return b.String()
}

// TestSeriesKey_ByteEquivalence is the critical regression: the
// new SeriesKey output MUST match the legacy per-processor seriesKey
// byte-for-byte, so that the snapshot cache survives the runtime
// extraction. (ADR-0002 "Behavior preservation".)
func TestSeriesKey_ByteEquivalence(t *testing.T) {
	t.Parallel()
	type fixture struct {
		name        string
		aggID       AggId
		resAttrs    map[string]string
		dpAttrs     map[string]string
		aggregateBy []string
	}
	fixtures := []fixture{
		{
			name:     "no_aggregate_by_full_set",
			aggID:    42,
			resAttrs: map[string]string{"service.name": "web", "host.name": "node-1"},
			dpAttrs:  map[string]string{"http.method": "GET", "http.status": "200"},
		},
		{
			name:        "with_aggregate_by_filters_keys",
			aggID:       7,
			resAttrs:    map[string]string{"region": "us-east", "service.name": "api"},
			dpAttrs:     map[string]string{"http.method": "POST", "http.status": "500", "k8s.pod": "p-1"},
			aggregateBy: []string{"http.method", "http.status"},
		},
		{
			name:        "aggregate_by_with_missing_keys_skipped",
			aggID:       1,
			resAttrs:    map[string]string{"service.name": "ingest"},
			dpAttrs:     map[string]string{"a": "1", "c": "3"},
			aggregateBy: []string{"a", "b", "c"}, // b missing — should not appear
		},
		{
			name:     "empty_dp_attrs",
			aggID:    99,
			resAttrs: map[string]string{"deployment.environment": "prod"},
			dpAttrs:  map[string]string{},
		},
		{
			name:     "empty_resource_attrs",
			aggID:    3,
			resAttrs: map[string]string{},
			dpAttrs:  map[string]string{"x": "1", "y": "2"},
		},
	}
	for _, fx := range fixtures {
		t.Run(fx.name, func(t *testing.T) {
			oldKey := legacyFullKey(fx.aggID, fx.resAttrs, fx.dpAttrs, fx.aggregateBy)

			newKey := SeriesKey(
				fx.aggID,
				mapToKVSorted(fx.resAttrs),
				mapToKVSorted(fx.dpAttrs),
				fx.aggregateBy,
			)
			if oldKey != newKey {
				t.Fatalf("byte mismatch:\n  old: %q\n  new: %q", oldKey, newKey)
			}
		})
	}
}

// mapToKVSorted converts a map to a sorted []KeyValue (which is what
// the OTel adapter's AttributesToKeyValues produces).
func mapToKVSorted(m map[string]string) []KeyValue {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]KeyValue, 0, len(keys))
	for _, k := range keys {
		out = append(out, KeyValue{Key: k, Value: m[k]})
	}
	return out
}

func TestMatches(t *testing.T) {
	t.Parallel()
	t.Run("nil_config_accepts_all", func(t *testing.T) {
		var cfg *PrecomputeConfig
		if !cfg.Matches(&Observation{}) {
			t.Fatal("nil config: want true")
		}
	})
	t.Run("empty_matchers_accept_all", func(t *testing.T) {
		cfg := &PrecomputeConfig{}
		if !cfg.Matches(&Observation{Metric: "x"}) {
			t.Fatal("empty matchers: want true")
		}
	})
	t.Run("metric_name_match_equal", func(t *testing.T) {
		cfg := &PrecomputeConfig{
			Matchers: []LabelMatcher{{Name: "", Value: "http_requests", Op: MatchEqual}},
		}
		if !cfg.Matches(&Observation{Metric: "http_requests"}) {
			t.Fatal("hit: want true")
		}
		if cfg.Matches(&Observation{Metric: "other"}) {
			t.Fatal("miss: want false")
		}
		if cfg.Matches(&Observation{Metric: ""}) {
			t.Fatal("missing: want false")
		}
	})
	t.Run("multi_matcher_all_must_hold", func(t *testing.T) {
		cfg := &PrecomputeConfig{
			Matchers: []LabelMatcher{
				{Name: "method", Value: "GET", Op: MatchEqual},
				{Name: "status", Value: "200", Op: MatchEqual},
			},
		}
		obs := &Observation{Labels: []KeyValue{{Key: "method", Value: "GET"}, {Key: "status", Value: "200"}}}
		if !cfg.Matches(obs) {
			t.Fatal("all hit: want true")
		}
		obs.Labels = []KeyValue{{Key: "method", Value: "GET"}, {Key: "status", Value: "404"}}
		if cfg.Matches(obs) {
			t.Fatal("partial: want false")
		}
	})
	t.Run("not_equal_missing_passes", func(t *testing.T) {
		cfg := &PrecomputeConfig{
			Matchers: []LabelMatcher{{Name: "kind", Value: "debug", Op: MatchNotEqual}},
		}
		obs := &Observation{Labels: nil}
		if !cfg.Matches(obs) {
			t.Fatal("missing key: want true")
		}
		obs.Labels = []KeyValue{{Key: "kind", Value: "info"}}
		if !cfg.Matches(obs) {
			t.Fatal("present-different: want true")
		}
		obs.Labels = []KeyValue{{Key: "kind", Value: "debug"}}
		if cfg.Matches(obs) {
			t.Fatal("present-equal: want false")
		}
	})
	t.Run("regex_full_match_anchored", func(t *testing.T) {
		cfg := &PrecomputeConfig{
			Matchers: []LabelMatcher{{Name: "path", Value: "/api/v[0-9]+", Op: MatchRegex}},
		}
		// Full match.
		if !cfg.Matches(&Observation{Labels: []KeyValue{{Key: "path", Value: "/api/v2"}}}) {
			t.Fatal("/api/v2: want true")
		}
		// Anchored: a partial / prefix-only match must FAIL (Prometheus
		// semantics anchor both ends).
		if cfg.Matches(&Observation{Labels: []KeyValue{{Key: "path", Value: "/api/v2/users"}}}) {
			t.Fatal("/api/v2/users: anchored regex must not partial-match")
		}
		if cfg.Matches(&Observation{Labels: []KeyValue{{Key: "path", Value: "x/api/v2"}}}) {
			t.Fatal("x/api/v2: anchored regex must not partial-match prefix")
		}
		// Missing key fails a positive regex.
		if cfg.Matches(&Observation{Labels: nil}) {
			t.Fatal("missing key: positive regex want false")
		}
	})
	t.Run("regex_top_level_alternation_anchors_whole", func(t *testing.T) {
		cfg := &PrecomputeConfig{
			Matchers: []LabelMatcher{{Name: "env", Value: "prod|staging", Op: MatchRegex}},
		}
		if !cfg.Matches(&Observation{Labels: []KeyValue{{Key: "env", Value: "prod"}}}) {
			t.Fatal("prod: want true")
		}
		if !cfg.Matches(&Observation{Labels: []KeyValue{{Key: "env", Value: "staging"}}}) {
			t.Fatal("staging: want true")
		}
		// Non-capturing wrap means "prodX" must not match via ^prod|...$.
		if cfg.Matches(&Observation{Labels: []KeyValue{{Key: "env", Value: "prodX"}}}) {
			t.Fatal("prodX: alternation must anchor whole value")
		}
	})
	t.Run("not_regex", func(t *testing.T) {
		cfg := &PrecomputeConfig{
			Matchers: []LabelMatcher{{Name: "kind", Value: "debug|trace", Op: MatchNotRegex}},
		}
		// Missing key passes.
		if !cfg.Matches(&Observation{Labels: nil}) {
			t.Fatal("missing key: not-regex want true")
		}
		// Present-non-matching passes.
		if !cfg.Matches(&Observation{Labels: []KeyValue{{Key: "kind", Value: "info"}}}) {
			t.Fatal("info: not-regex want true")
		}
		// Present-matching fails.
		if cfg.Matches(&Observation{Labels: []KeyValue{{Key: "kind", Value: "trace"}}}) {
			t.Fatal("trace: not-regex want false")
		}
	})
	t.Run("regex_metric_name", func(t *testing.T) {
		cfg := &PrecomputeConfig{
			Matchers: []LabelMatcher{{Name: "", Value: "http_.*_total", Op: MatchRegex}},
		}
		if !cfg.Matches(&Observation{Metric: "http_requests_total"}) {
			t.Fatal("http_requests_total: want true")
		}
		if cfg.Matches(&Observation{Metric: "grpc_requests_total"}) {
			t.Fatal("grpc_requests_total: want false")
		}
	})
	t.Run("regex_bad_pattern_never_matches", func(t *testing.T) {
		bad := "([" // invalid RE2
		pos := &PrecomputeConfig{Matchers: []LabelMatcher{{Name: "k", Value: bad, Op: MatchRegex}}}
		if pos.Matches(&Observation{Labels: []KeyValue{{Key: "k", Value: "anything"}}}) {
			t.Fatal("bad positive regex must not match")
		}
		neg := &PrecomputeConfig{Matchers: []LabelMatcher{{Name: "k", Value: bad, Op: MatchNotRegex}}}
		if !neg.Matches(&Observation{Labels: []KeyValue{{Key: "k", Value: "anything"}}}) {
			t.Fatal("bad negative regex must pass (cannot disagree)")
		}
	})
}

func TestSeriesAttrs(t *testing.T) {
	t.Parallel()
	labels := []KeyValue{
		{Key: "method", Value: "GET"},
		{Key: "host", Value: "h1"},
		{Key: "status", Value: "200"},
	}
	t.Run("empty_aggregate_by_returns_copy", func(t *testing.T) {
		got := SeriesAttrs(labels, nil)
		if len(got) != len(labels) {
			t.Fatalf("len: want %d, got %d", len(labels), len(got))
		}
		for i := range got {
			if got[i] != labels[i] {
				t.Fatalf("idx %d: want %+v, got %+v", i, labels[i], got[i])
			}
		}
	})
	t.Run("aggregate_by_filters_and_orders", func(t *testing.T) {
		got := SeriesAttrs(labels, []string{"status", "method"})
		if len(got) != 2 {
			t.Fatalf("len: want 2, got %d (%+v)", len(got), got)
		}
		if got[0] != (KeyValue{Key: "status", Value: "200"}) {
			t.Fatalf("[0]: %+v", got[0])
		}
		if got[1] != (KeyValue{Key: "method", Value: "GET"}) {
			t.Fatalf("[1]: %+v", got[1])
		}
	})
	t.Run("aggregate_by_skips_missing", func(t *testing.T) {
		got := SeriesAttrs(labels, []string{"missing", "host"})
		if len(got) != 1 {
			t.Fatalf("len: want 1, got %d (%+v)", len(got), got)
		}
		if got[0] != (KeyValue{Key: "host", Value: "h1"}) {
			t.Fatalf("[0]: %+v", got[0])
		}
	})
}

// TestBuildSeriesKeyMatchesSeriesKeyFor guards the byte-identity
// invariant between the zero-alloc observe path (buildSeriesKey) and
// the canonical SeriesKeyFor / SeriesKeyForEntry used at flush. If
// these ever diverge, admitted series fail to round-trip on flush.
func TestBuildSeriesKeyMatchesSeriesKeyFor(t *testing.T) {
	obsCases := []*Observation{
		{},
		{Labels: []KeyValue{{Key: "method", Value: "GET"}, {Key: "status", Value: "200"}}},
		// Deliberately unsorted to exercise the sort path.
		{Labels: []KeyValue{{Key: "status", Value: "200"}, {Key: "method", Value: "GET"}}},
		{ResourceLabels: []KeyValue{{Key: "host", Value: "h1"}}, Labels: []KeyValue{{Key: "zone", Value: "z0"}}},
		{Labels: []KeyValue{{Key: "k", Value: "v;with=weird|chars"}}},
	}
	cfgCases := []*PrecomputeConfig{
		{AggID: 1},
		{AggID: 42, OmitResourceAttrs: true},
		{AggID: 7, GlobalAggregation: true},
		{AggID: 9, AggregateBy: []string{"method"}},
		{AggID: 9, AggregateBy: []string{"missing", "method"}},
	}
	for ci, cfg := range cfgCases {
		for oi, obs := range obsCases {
			want := cfg.SeriesKeyFor(obs)
			sc := getSeriesKeyScratch()
			cfg.buildSeriesKey(sc, obs)
			got := string(sc.buf)
			putSeriesKeyScratch(sc)
			if got != want {
				t.Fatalf("cfg[%d] obs[%d]: buildSeriesKey=%q SeriesKeyFor=%q", ci, oi, got, want)
			}
		}
	}
}
