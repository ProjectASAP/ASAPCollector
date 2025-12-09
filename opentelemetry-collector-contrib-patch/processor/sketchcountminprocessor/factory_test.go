package sketchcountminprocessor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pipeline"
	"go.opentelemetry.io/collector/processor/processortest"
)

func TestFactory_CreateDefaultConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	require.NoError(t, componenttest.CheckConfigStruct(cfg))
}

func TestFactory_CreateMetrics(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()

	mp, err := factory.CreateMetrics(t.Context(), processortest.NewNopSettings(typeStr), cfg, consumertest.NewNop())
	require.NoError(t, err)
	require.NotNil(t, mp)
}

func TestFactory_CreateTracesNotSupported(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()

	tp, err := factory.CreateTraces(t.Context(), processortest.NewNopSettings(typeStr), cfg, consumertest.NewNop())
	require.Nil(t, tp)
	require.Equal(t, pipeline.ErrSignalNotSupported, err)
}
