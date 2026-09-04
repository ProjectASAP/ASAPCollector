package precompute

import "testing"

func TestRuntimeConfigOwnsCompiledMatchers(t *testing.T) {
	source := &PrecomputeConfig{AggID: 1, Matchers: []LabelMatcher{{Name: "zone", Op: MatchRegex, Value: "a|b"}}}
	p := New(source, newFakeFactory(), &fakeObserver{}).(*precompute)
	if p.activeConfig().Matchers[0].compiled == nil {
		t.Fatal("runtime matcher was not compiled at installation")
	}
	source.Matchers[0].Value = "never"
	if !p.activeConfig().Matches(&Observation{Labels: []KeyValue{{Key: "zone", Value: "a"}}}) {
		t.Fatal("caller mutation changed installed matcher")
	}
	if err := ValidateMatchers([]LabelMatcher{{Op: MatchRegex, Value: "["}}); err == nil {
		t.Fatal("invalid regexp accepted")
	}
}
