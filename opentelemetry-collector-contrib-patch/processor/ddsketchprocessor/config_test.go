package ddsketchprocessor

import "testing"

// TestDefaultDeltaTransmissionTrue asserts that the factory's default
// Config has `DeltaTransmission: true`. Operationally this means the
// per-window wire payload is the sparse bucket diff since the last
// flush, not the full DDSketch state — without which the bandwidth
// verdict at 10 Hz × 60 s collapses (full state at 1 Hz × 60 s is
// what produced the v-final demo's -1201% bandwidth result). This
// regression test exists so a future factory edit can't silently
// flip the default back to full-state. See the doc comment on
// `createDefaultConfig` for the operating-point math.
func TestDefaultDeltaTransmissionTrue(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	if !cfg.DeltaTransmission {
		t.Fatalf("expected DeltaTransmission=true by default, got %v", cfg.DeltaTransmission)
	}
}

// TestDefaultDropOriginalTrue is the bandwidth-FAIL guard. The MVP
// agent pipeline relies on the sketch summary (or DDSketch envelope)
// REPLACING the raw on the outbound stream — the raw is preserved
// upstream by the gorillas3processor archive write. A future factory
// edit that flips this default back to false would resurrect the
// ~11x agent→gateway bandwidth blow-up the ① bandwidth FAIL fix
// addressed.
func TestDefaultDropOriginalTrue(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	if !cfg.DropOriginal {
		t.Fatalf("expected DropOriginal=true by default, got %v (bandwidth-FAIL regression)", cfg.DropOriginal)
	}
}

func TestConfigValidate(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	if err := cfg.validate(); err != nil {
		t.Fatalf("expected valid default config: %v", err)
	}

	cfg.TransmitSketch = true
	cfg.Quantiles = nil
	if err := cfg.validate(); err != nil {
		t.Fatalf("expected quantiles to be optional when transmit_sketch=true: %v", err)
	}

	cfg.RelativeAccuracy = 1.5
	if err := cfg.validate(); err == nil {
		t.Fatalf("expected error for invalid accuracy")
	}

	cfg.RelativeAccuracy = 0.5
	cfg.Quantiles = []float64{0.25, 1.2}
	if err := cfg.validate(); err == nil {
		t.Fatalf("expected error for invalid quantile")
	}

	cfg.TransmitSketch = false
	cfg.Quantiles = nil
	if err := cfg.validate(); err == nil {
		t.Fatalf("expected error when transmit_sketch=false and no quantiles configured")
	}

	// mode-specific validation
	cfg = createDefaultConfig().(*Config)
	cfg.Mode = InputMode("unknown")
	if err := cfg.validate(); err == nil {
		t.Fatalf("expected error for unknown mode")
	}

	cfg = createDefaultConfig().(*Config)
	cfg.Mode = ModeWindow
	cfg.WindowDuration = 0
	if err := cfg.validate(); err == nil {
		t.Fatalf("expected error for zero window_duration in window mode")
	}

	cfg = createDefaultConfig().(*Config)
	cfg.Mode = ""
	if err := cfg.validate(); err != nil {
		t.Fatalf("expected default mode batch to be valid, got: %v", err)
	}
}
