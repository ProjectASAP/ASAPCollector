package ddsketchprocessor

import "testing"

func TestConfigValidate(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	if err := cfg.validate(); err != nil {
		t.Fatalf("expected valid default config: %v", err)
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
}
