package sketchcountminprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/processor/processortest"
)

func TestComponentLifecycle(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()

	processor, err := factory.CreateMetrics(context.Background(), processortest.NewNopSettings(typeStr), cfg, consumertest.NewNop())
	require.NoError(t, err)

	host := componenttest.NewNopHost()
	require.NoError(t, processor.Start(context.Background(), host))
	require.NoError(t, processor.Shutdown(context.Background()))
}
