package hllprocessor

import "testing"

// TestDefaultDeltaTransmissionTrue asserts that the factory's default
// Config has `DeltaTransmission: true`. HLL's per-window wire footprint
// is dominated by the fixed-size register array; emitting only the
// registers that increased since the last flush (max semantics) is the
// difference between losing and winning bandwidth versus raw at the
// MVP demo's 10 Hz × 60 s = 600 samples/window operating point. This
// regression test exists so a future factory edit can't silently flip
// the default back to full-state. See the doc comment on
// `createDefaultConfig` for the operating-point math.
func TestDefaultDeltaTransmissionTrue(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	if !cfg.DeltaTransmission {
		t.Fatalf("expected DeltaTransmission=true by default, got %v", cfg.DeltaTransmission)
	}
}
