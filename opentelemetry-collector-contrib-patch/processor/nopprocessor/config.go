package nopprocessor
import "go.opentelemetry.io/collector/component"

type Config struct{}
var _ component.Config = (*Config)(nil)
func (c *Config) Validate() error { return nil }