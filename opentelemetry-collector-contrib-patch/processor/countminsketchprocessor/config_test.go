package countminsketchprocessor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestDefaultDeltaTransmissionTrue asserts that the factory's default
// Config has `DeltaTransmission: true`. Count-Min's per-window wire
// footprint is dominated by the d×w cell matrix; emitting only cells
// that changed since the last flush (instead of the full matrix)
// keeps the bandwidth verdict on the right side of break-even at the
// MVP demo's 10 Hz × 60 s = 600 samples/window operating point. This
// regression test exists so a future factory edit can't silently flip
// the default back to full-state. See the doc comment on
// `createDefaultConfig` for the operating-point math.
func TestDefaultDeltaTransmissionTrue(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	assert.True(t, cfg.DeltaTransmission, "expected DeltaTransmission=true by default")
}

func TestConfigValidate(t *testing.T) {
	// 1. Test Valid Config
	cfg := &Config{
		MetricName: "countmin_sketch",
		Rows:       5,
		Columns:    1000,
	}
	assert.NoError(t, cfg.Validate())

	// 2. Test Invalid MetricName
	cfg = &Config{
		MetricName: "", // Error
		Rows:       5,
		Columns:    1000,
	}
	assert.Error(t, cfg.Validate())

	// 3. Test Invalid Rows
	cfg = &Config{
		MetricName: "valid",
		Rows:       0, // Error
		Columns:    1000,
	}
	assert.Error(t, cfg.Validate())

	// 4. Test Invalid Columns
	cfg = &Config{
		MetricName: "valid",
		Rows:       5,
		Columns:    -1, // Error
	}
	assert.Error(t, cfg.Validate())
}
