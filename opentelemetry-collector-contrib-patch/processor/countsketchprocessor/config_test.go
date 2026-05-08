package countsketchprocessor

import (
	"testing"
	"time"
)

// TestDefaultDeltaTransmissionTrue asserts that the factory's default
// Config has `DeltaTransmission: true`. CountSketch's per-window wire
// footprint is dominated by the d×w cell matrix; emitting only cells
// that changed since the last flush (instead of the full matrix) is
// the difference between a ~133× bandwidth loss (full-state at
// 1 Hz × 60 s) and ~13× (delta at 10 Hz × 60 s). This regression test
// exists so a future factory edit can't silently flip the default
// back to full-state. See the doc comment on `createDefaultConfig`
// for the operating-point math.
func TestDefaultDeltaTransmissionTrue(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	if !cfg.DeltaTransmission {
		t.Fatalf("expected DeltaTransmission=true by default, got %v", cfg.DeltaTransmission)
	}
}

// TestDefaultDropOriginalTrue is the bandwidth-FAIL guard. The MVP
// agent pipeline relies on the sketch summary REPLACING the raw on
// the outbound stream — the raw is preserved upstream by the
// gorillas3processor archive write. A future factory edit that flips
// this default back to false would resurrect the ~11x agent→gateway
// bandwidth blow-up the ① bandwidth FAIL fix addressed.
func TestDefaultDropOriginalTrue(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	if !cfg.DropOriginal {
		t.Fatalf("expected DropOriginal=true by default, got %v (bandwidth-FAIL regression)", cfg.DropOriginal)
	}
}

func TestConfigValidate(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid default config: %v", err)
	}

	// Test Invalid Epsilon (must be between 0 and 1)
	cfg.Epsilon = 1.5
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected error for invalid epsilon > 1")
	}
	cfg.Epsilon = 0.01 // Reset to valid

	// Test Invalid Delta (must be between 0 and 1)
	cfg.Delta = -0.5
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected error for invalid delta < 0")
	}
	cfg.Delta = 0.99 // Reset to valid

	// Test Invalid WindowDuration (must be >= 1s in window mode)
	cfg.Mode = ModeWindow
	cfg.WindowDuration = 500 * time.Millisecond
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected error for invalid window duration (too small)")
	}
}