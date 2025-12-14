package countminsketchprocessor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigValidate(t *testing.T) {
	// 1. Test Valid Config
	cfg := &Config{
		MetricName: "countmin_sketch",
		Rows:       5,
		Columns:    1000,
		Seed:       1,
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
