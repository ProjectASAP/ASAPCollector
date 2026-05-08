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

// TestDefaultDropOriginalTrue is the bandwidth-FAIL guard. The MVP
// agent pipeline relies on the HLL cardinality summary REPLACING the
// raw on the outbound stream — the raw is preserved upstream by the
// gorillas3processor archive write. A future factory edit that flips
// this default back to false would resurrect the ~11x agent→gateway
// bandwidth blow-up the ① bandwidth FAIL fix addressed (especially
// painful for high-cardinality unique-user inputs the HLL is sized
// for).
func TestDefaultDropOriginalTrue(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	if !cfg.DropOriginal {
		t.Fatalf("expected DropOriginal=true by default, got %v (bandwidth-FAIL regression)", cfg.DropOriginal)
	}
}
