// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metric

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeTemp writes content to a temporary YAML file and returns the path.
func writeTemp(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "pipelinecfg-*.yaml")
	require.NoError(t, err)
	_, err = f.WriteString(content)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return f.Name()
}

// ---------------------------------------------------------------------------
// LoadPipelineConfig
// ---------------------------------------------------------------------------

func TestLoadPipelineConfig_DDSketch(t *testing.T) {
	yaml := `
sketch:
  type: ddsketch
  transmit_sketch: true
  ddsketch:
    relative_accuracy: 0.02
    no_min_max: true
reader:
  interval: 2s
  enable_self_monitoring: true
instruments:
  - name: my.metric
    unit: ms
    instrument_kind: histogram
    group_by: [host.name]
    series_per_sketch: 1
`
	cfg, err := LoadPipelineConfig(writeTemp(t, yaml))
	require.NoError(t, err)
	assert.Equal(t, "ddsketch", cfg.Sketch.Type)
	assert.True(t, cfg.Sketch.TransmitSketch)
	assert.InDelta(t, 0.02, cfg.Sketch.DDSketch.RelativeAccuracy, 1e-9)
	assert.True(t, cfg.Sketch.DDSketch.NoMinMax)
	assert.Equal(t, 2*time.Second, cfg.Reader.Interval)
	assert.True(t, cfg.Reader.EnableSelfMonitoring)
	require.Len(t, cfg.Instruments, 1)
	assert.Equal(t, "my.metric", cfg.Instruments[0].Name)
	assert.Equal(t, []string{"host.name"}, cfg.Instruments[0].GroupBy)
}

func TestLoadPipelineConfig_KLL(t *testing.T) {
	yaml := `
sketch:
  type: kll
  transmit_sketch: true
  kll:
    k: 512
`
	cfg, err := LoadPipelineConfig(writeTemp(t, yaml))
	require.NoError(t, err)
	assert.Equal(t, "kll", cfg.Sketch.Type)
	assert.Equal(t, 512, cfg.Sketch.KLL.K)
}

func TestLoadPipelineConfig_HLL(t *testing.T) {
	yaml := `
sketch:
  type: hll
  transmit_sketch: true
`
	cfg, err := LoadPipelineConfig(writeTemp(t, yaml))
	require.NoError(t, err)
	assert.Equal(t, "hll", cfg.Sketch.Type)
	assert.True(t, cfg.Sketch.TransmitSketch)
}

func TestLoadPipelineConfig_CountSketch(t *testing.T) {
	yaml := `
sketch:
  type: countsketch
  transmit_sketch: true
  countsketch:
    rows: 7
    cols: 1024
    epsilon: 0.05
    delta: 0.95
    dimension: latency
`
	cfg, err := LoadPipelineConfig(writeTemp(t, yaml))
	require.NoError(t, err)
	assert.Equal(t, "countsketch", cfg.Sketch.Type)
	assert.Equal(t, 7, cfg.Sketch.CountSketch.Rows)
	assert.Equal(t, 1024, cfg.Sketch.CountSketch.Cols)
	assert.InDelta(t, 0.05, cfg.Sketch.CountSketch.Epsilon, 1e-9)
	assert.InDelta(t, 0.95, cfg.Sketch.CountSketch.Delta, 1e-9)
	assert.Equal(t, "latency", cfg.Sketch.CountSketch.Dimension)
}

func TestLoadPipelineConfig_CountMinSketch(t *testing.T) {
	yaml := `
sketch:
  type: countminsketch
  transmit_sketch: true
  countminsketch:
    rows: 5
    cols: 2000
`
	cfg, err := LoadPipelineConfig(writeTemp(t, yaml))
	require.NoError(t, err)
	assert.Equal(t, "countminsketch", cfg.Sketch.Type)
	assert.Equal(t, 5, cfg.Sketch.CountMinSketch.Rows)
	assert.Equal(t, 2000, cfg.Sketch.CountMinSketch.Cols)
}

func TestLoadPipelineConfig_Baseline(t *testing.T) {
	yaml := `
sketch:
  type: baseline
  transmit_sketch: false
`
	cfg, err := LoadPipelineConfig(writeTemp(t, yaml))
	require.NoError(t, err)
	assert.Equal(t, "baseline", cfg.Sketch.Type)
	assert.False(t, cfg.Sketch.TransmitSketch)
}

func TestLoadPipelineConfig_ASAPQuery(t *testing.T) {
	yaml := `
sketch:
  type: kll
  transmit_sketch: true
asap_query:
  metrics:
    - metric: req.latency
      labels: [host, region]
  query_groups:
    - id: 1
      queries: ["quantile_over_time(0.99, req_latency[5m])"]
      repetition_delay: 3000
      controller_options:
        accuracy_sla: 0.01
        latency_sla: 50.0
`
	cfg, err := LoadPipelineConfig(writeTemp(t, yaml))
	require.NoError(t, err)
	require.NotNil(t, cfg.ASAPQuery)
	require.Len(t, cfg.ASAPQuery.Metrics, 1)
	assert.Equal(t, "req.latency", cfg.ASAPQuery.Metrics[0].Metric)
	assert.Equal(t, []string{"host", "region"}, cfg.ASAPQuery.Metrics[0].Labels)
	require.Len(t, cfg.ASAPQuery.QueryGroups, 1)
	assert.InDelta(t, 0.01, cfg.ASAPQuery.QueryGroups[0].ControllerOptions.AccuracySLA, 1e-9)
	assert.InDelta(t, 50.0, cfg.ASAPQuery.QueryGroups[0].ControllerOptions.LatencySLA, 1e-9)
}

func TestLoadPipelineConfig_FileNotFound(t *testing.T) {
	_, err := LoadPipelineConfig(filepath.Join(t.TempDir(), "nonexistent.yaml"))
	assert.Error(t, err)
}

func TestLoadPipelineConfig_InvalidYAML(t *testing.T) {
	_, err := LoadPipelineConfig(writeTemp(t, ":: invalid: yaml: ["))
	assert.Error(t, err)
}

// ---------------------------------------------------------------------------
// ToAggregation
// ---------------------------------------------------------------------------

func TestToAggregation(t *testing.T) {
	tests := []struct {
		name     string
		yaml     string
		wantType Aggregation
	}{
		{
			name:     "ddsketch",
			yaml:     `sketch: {type: ddsketch, ddsketch: {relative_accuracy: 0.02}}`,
			wantType: AggregationDDSketch{RelativeAccuracy: 0.02},
		},
		{
			name:     "kll",
			yaml:     `sketch: {type: kll, kll: {k: 128}}`,
			wantType: AggregationKLLSketch{K: 128},
		},
		{
			name:     "hll",
			yaml:     `sketch: {type: hll}`,
			wantType: AggregationHLLSketch{},
		},
		{
			name:     "countsketch",
			yaml:     `sketch: {type: countsketch, countsketch: {rows: 3, cols: 500, epsilon: 0.1, delta: 0.9}}`,
			wantType: AggregationCountSketch{Rows: 3, Cols: 500, Epsilon: 0.1, Delta: 0.9},
		},
		{
			name:     "countminsketch",
			yaml:     `sketch: {type: countminsketch, countminsketch: {rows: 4, cols: 800}}`,
			wantType: AggregationCountMinSketch{Rows: 4, Cols: 800},
		},
		{
			name:     "baseline returns nil",
			yaml:     `sketch: {type: baseline}`,
			wantType: nil,
		},
		{
			name:     "unknown type returns nil",
			yaml:     `sketch: {type: bogus}`,
			wantType: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := LoadPipelineConfig(writeTemp(t, tc.yaml))
			require.NoError(t, err)
			assert.Equal(t, tc.wantType, cfg.ToAggregation())
		})
	}
}

// ---------------------------------------------------------------------------
// ToViews
// ---------------------------------------------------------------------------

func TestToViews_TransmitSketchTrue(t *testing.T) {
	yaml := `
sketch:
  type: kll
  transmit_sketch: true
  kll:
    k: 256
instruments:
  - name: metric.a
  - name: metric.b
`
	cfg, err := LoadPipelineConfig(writeTemp(t, yaml))
	require.NoError(t, err)

	views := cfg.ToViews()
	// One View per instrument.
	assert.Len(t, views, 2)
}

func TestToViews_TransmitSketchFalse(t *testing.T) {
	yaml := `
sketch:
  type: kll
  transmit_sketch: false
instruments:
  - name: metric.a
`
	cfg, err := LoadPipelineConfig(writeTemp(t, yaml))
	require.NoError(t, err)

	assert.Nil(t, cfg.ToViews())
}

func TestToViews_Baseline(t *testing.T) {
	yaml := `
sketch:
  type: baseline
  transmit_sketch: true   # transmit_sketch: true but type: baseline → nil
instruments:
  - name: metric.a
`
	cfg, err := LoadPipelineConfig(writeTemp(t, yaml))
	require.NoError(t, err)

	// baseline has no aggregation regardless of transmit_sketch.
	assert.Nil(t, cfg.ToViews())
}

func TestToViews_NoInstruments(t *testing.T) {
	yaml := `
sketch:
  type: ddsketch
  transmit_sketch: true
`
	cfg, err := LoadPipelineConfig(writeTemp(t, yaml))
	require.NoError(t, err)

	assert.Empty(t, cfg.ToViews())
}

// ---------------------------------------------------------------------------
// ReaderInterval
// ---------------------------------------------------------------------------

func TestReaderInterval(t *testing.T) {
	t.Run("configured", func(t *testing.T) {
		yaml := `reader: {interval: 5s}`
		cfg, err := LoadPipelineConfig(writeTemp(t, yaml))
		require.NoError(t, err)
		assert.Equal(t, 5*time.Second, cfg.ReaderInterval())
	})

	t.Run("default when unset", func(t *testing.T) {
		cfg, err := LoadPipelineConfig(writeTemp(t, `sketch: {type: kll}`))
		require.NoError(t, err)
		assert.Equal(t, time.Second, cfg.ReaderInterval())
	})

	t.Run("default when zero", func(t *testing.T) {
		yaml := `reader: {interval: 0}`
		cfg, err := LoadPipelineConfig(writeTemp(t, yaml))
		require.NoError(t, err)
		assert.Equal(t, time.Second, cfg.ReaderInterval())
	})
}
