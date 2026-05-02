package otel

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

func TestAttributesToKeyValues_RoundTrip(t *testing.T) {
	t.Parallel()
	attrs := pcommon.NewMap()
	attrs.PutStr("zeta", "z")
	attrs.PutStr("alpha", "a")
	attrs.PutStr("middle", "m")

	kvs := AttributesToKeyValues(attrs)

	dst := pcommon.NewMap()
	KeyValuesToAttributes(kvs, dst)

	if !attrs.Equal(dst) {
		t.Fatalf("round-trip differs:\n  src: %v\n  dst: %v", attrs.AsRaw(), dst.AsRaw())
	}
}

func TestAttributesToKeyValues_SortedByKey(t *testing.T) {
	t.Parallel()
	attrs := pcommon.NewMap()
	attrs.PutStr("zeta", "z")
	attrs.PutStr("alpha", "a")
	attrs.PutStr("middle", "m")
	attrs.PutStr("beta", "b")

	kvs := AttributesToKeyValues(attrs)
	want := []string{"alpha", "beta", "middle", "zeta"}
	if len(kvs) != len(want) {
		t.Fatalf("len: want %d, got %d", len(want), len(kvs))
	}
	for i, k := range want {
		if kvs[i].Key != k {
			t.Errorf("idx %d: want %q, got %q", i, k, kvs[i].Key)
		}
	}
}

func TestAttributesToKeyValues_EmptyMap(t *testing.T) {
	t.Parallel()
	attrs := pcommon.NewMap()
	if got := AttributesToKeyValues(attrs); got != nil {
		t.Errorf("empty: want nil, got %+v", got)
	}
}

func TestKeyValuesToAttributes_OverwritesDuplicates(t *testing.T) {
	t.Parallel()
	kvs := []precompute.KeyValue{
		{Key: "k", Value: "first"},
		{Key: "k", Value: "second"},
	}
	dst := pcommon.NewMap()
	KeyValuesToAttributes(kvs, dst)
	v, ok := dst.Get("k")
	if !ok || v.AsString() != "second" {
		t.Errorf("want second, got %v ok=%v", v.AsString(), ok)
	}
}
