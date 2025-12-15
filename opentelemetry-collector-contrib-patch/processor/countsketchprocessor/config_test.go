package countsketchprocessor

import (
	"testing"
	"time"
)

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

	// Test Invalid WindowSize (must be >= 1s)
	cfg.WindowSize = 500 * time.Millisecond
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected error for invalid window size (too small)")
	}
}