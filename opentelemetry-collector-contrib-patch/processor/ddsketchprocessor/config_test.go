package ddsketchprocessor

import "testing"

func TestConfigValidate(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	if err := cfg.validate(); err != nil {
		t.Fatalf("expected valid default config: %v", err)
	}

	cfg.EmitDDSketch = true
	cfg.Quantiles = nil
	if err := cfg.validate(); err != nil {
		t.Fatalf("expected quantiles to be optional when emit_ddsketch=true: %v", err)
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

	cfg.EmitDDSketch = false
	cfg.Quantiles = nil
	if err := cfg.validate(); err == nil {
		t.Fatalf("expected error when emit_ddsketch=false and no quantiles configured")
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
