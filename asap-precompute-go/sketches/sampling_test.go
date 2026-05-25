// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sketches

import (
	"testing"

	"github.com/ProjectASAP/sketchlib-go/common"
)

// TestCMSWrapper_SamplePReachesSketchBuilder proves a configured
// sample_p<1 is threaded all the way into the underlying sketchlib-go
// CountMinSketch builder (read back off w.sk, the real sketch the wrapper
// owns — not just the wrapper's own field), and that the default 1.0 is
// unchanged (exact, sampling disabled). This is the per-metric sampling
// knob's last hop: control plane → agent processor config → NewCMSWrapper
// → WithSampleP → sketchlib-go cms.WithSampleP.
func TestCMSWrapper_SamplePReachesSketchBuilder(t *testing.T) {
	t.Parallel()

	// Default: no WithSampleP call ⇒ exact (1.0) on both the wrapper and
	// the underlying sketchlib-go sketch. Byte-identical to pre-sampling.
	def := NewCMSWrapper(4, 256, false)
	if got := def.SampleP(); got != 1.0 {
		t.Fatalf("default wrapper SampleP = %v, want 1.0", got)
	}
	if got := def.sk.SampleP(); got != 1.0 {
		t.Fatalf("default underlying sketch SampleP = %v, want 1.0", got)
	}

	// WithSampleP(1.0) is also an exact no-op (the safe rollout state the
	// control plane emits when sample_p is unset / normalised to 1.0).
	exact := NewCMSWrapper(4, 256, false).WithSampleP(1.0)
	if got := exact.sk.SampleP(); got != 1.0 {
		t.Fatalf("WithSampleP(1.0) underlying SampleP = %v, want 1.0", got)
	}

	// Configured p<1 reaches the builder.
	const p = 0.1
	w := NewCMSWrapper(4, 256, false).WithSampleP(p)
	if got := w.SampleP(); got != p {
		t.Fatalf("wrapper SampleP = %v, want %v", got, p)
	}
	if got := w.sk.SampleP(); got != p {
		t.Fatalf("underlying sketch SampleP = %v, want %v — p did not reach the sketchlib-go builder", got, p)
	}

	// Sampling probability survives the re-construction paths so a sampled
	// wrapper stays sampled for its whole lifetime.
	w.Reset()
	if got := w.sk.SampleP(); got != p {
		t.Fatalf("after Reset underlying SampleP = %v, want %v", got, p)
	}
}

// TestHLLWrapper_SamplePReachesSketchBuilder is the HLL twin of the CMS
// test above: it pins that a configured sample_p<1 reaches the underlying
// sketchlib-go HyperLogLog builder and that the default 1.0 is unchanged.
func TestHLLWrapper_SamplePReachesSketchBuilder(t *testing.T) {
	t.Parallel()

	def := NewHLLWrapper()
	if got := def.SampleP(); got != 1.0 {
		t.Fatalf("default wrapper SampleP = %v, want 1.0", got)
	}
	if got := def.sk.SampleP(); got != 1.0 {
		t.Fatalf("default underlying sketch SampleP = %v, want 1.0", got)
	}

	exact := NewHLLWrapper().WithSampleP(1.0)
	if got := exact.sk.SampleP(); got != 1.0 {
		t.Fatalf("WithSampleP(1.0) underlying SampleP = %v, want 1.0", got)
	}

	const p = 0.1
	w := NewHLLWrapper().WithSampleP(p)
	if got := w.SampleP(); got != p {
		t.Fatalf("wrapper SampleP = %v, want %v", got, p)
	}
	if got := w.sk.SampleP(); got != p {
		t.Fatalf("underlying sketch SampleP = %v, want %v — p did not reach the sketchlib-go builder", got, p)
	}

	w.Reset()
	if got := w.sk.SampleP(); got != p {
		t.Fatalf("after Reset underlying SampleP = %v, want %v", got, p)
	}
}

// TestCMSWrapper_SamplePThinsUpdates is a behavioural check: with p<1 the
// geometric sampler admits only a fraction of inserts, so the estimated
// frequency of a key inserted N times is materially below N (the RAW
// sampled count — the backend applies the ×1/p rescale at query time). At
// p=1.0 every insert is admitted, so the estimate matches N.
func TestCMSWrapper_SamplePThinsUpdates(t *testing.T) {
	t.Parallel()

	key := []byte("sampled-key")
	h := common.FromBytes(key).Hash
	const n = 10000

	exact := NewCMSWrapper(5, 4096, false) // p=1.0 default
	for i := 0; i < n; i++ {
		exact.InsertHash(h)
	}
	if got := exact.EstimateCount(key); got < n*0.95 {
		t.Fatalf("p=1.0 estimate = %v, want ~%d", got, n)
	}

	sampled := NewCMSWrapper(5, 4096, false).WithSampleP(0.1)
	for i := 0; i < n; i++ {
		sampled.InsertHash(h)
	}
	// Raw sampled count ≈ 0.1*n = 1000; allow a wide margin for RNG, but
	// it must be well below the exact count, proving sampling is live.
	if got := sampled.EstimateCount(key); got >= n*0.5 {
		t.Fatalf("p=0.1 raw estimate = %v, expected well below %d (sampling not applied?)", got, n)
	}
}
