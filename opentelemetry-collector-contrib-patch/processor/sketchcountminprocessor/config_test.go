package sketchcountminprocessor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigValidate(t *testing.T) {
	cfg := Config{
		Measurement: "countmin",
		Rows:        3,
		Columns:     4,
		TopK:        10,
	}
	assert.NoError(t, cfg.Validate())

	cfg.TopK = -1
	assert.Error(t, cfg.Validate())
	cfg.TopK = 1
	cfg.Rows = -1
	assert.Error(t, cfg.Validate())
	cfg.Rows = 1
	cfg.Columns = -1
	assert.Error(t, cfg.Validate())
}
