// Package telegraf is the Layer-4 Telegraf codec for the host-neutral
// asap-precompute-go runtime. It performs pure data-shape translation
// between telegraf.Metric and the host-neutral Observation /
// SketchEnvelope types — no goroutines, no tickers, no plugin
// lifecycle. The accompanying Telegraf StreamingProcessor plugin (the
// `allsketches` plugin) lives in `telegraf-patch/processors/allsketches`
// and is shipped separately as Phase C of the integration plan in
// docs/design-asap-telegraf-integration.md.
//
// This package mirrors the shape of asap-precompute-go/otel: a
// `Decode` for the host event → []Observation direction, an `Encode`
// for the runtime's emitted []*SketchEnvelope → host-event direction,
// and an `Adapter` struct that thinly wraps the two for callers that
// want a single instance to thread through a plugin.
package telegraf

// Default values for AdapterConfig.
const (
	// DefaultValueField is the telegraf.Metric field name from which
	// Decode reads the observation's float value when no
	// pre-aggregated envelope field is present.
	DefaultValueField = "value"

	// DefaultEnvelopeField is the telegraf.Metric field name that, if
	// present, carries a pre-aggregated upstream SketchEnvelope. The
	// envelope is JSON-serialized then base64-encoded so it survives
	// round-trips through Telegraf outputs that don't preserve binary
	// (the suffix `_b64` is part of the field name to make that
	// explicit on the wire).
	DefaultEnvelopeField = "_asap_envelope_b64"

	// DefaultOutputMetricName is the metric name Encode stamps onto
	// emitted telegraf.Metric instances when SketchEnvelope.MetricName
	// is empty.
	DefaultOutputMetricName = "asap_sketch"
)

// AdapterConfig holds Telegraf-specific codec knobs that aren't part
// of the host-neutral PrecomputeConfig.
type AdapterConfig struct {
	// ValueField is the telegraf.Metric field name carrying the
	// observation value. Default: "value".
	ValueField string

	// EnvelopeField is the telegraf.Metric field name that, if
	// present, contains a base64-encoded pre-aggregated SketchEnvelope.
	// When non-empty, decode routes through KindEnvelope. Default:
	// "_asap_envelope_b64".
	EnvelopeField string

	// OutputMetricName is the metric name used on the encode side
	// when SketchEnvelope.MetricName is empty. Default: "asap_sketch".
	OutputMetricName string
}

// DefaultAdapterConfig returns an AdapterConfig populated with the
// package-level default field names.
func DefaultAdapterConfig() *AdapterConfig {
	return &AdapterConfig{
		ValueField:       DefaultValueField,
		EnvelopeField:    DefaultEnvelopeField,
		OutputMetricName: DefaultOutputMetricName,
	}
}

// valueField returns the configured value field name, falling back to
// the package default when the config is nil or the field is empty.
func (c *AdapterConfig) valueField() string {
	if c == nil || c.ValueField == "" {
		return DefaultValueField
	}
	return c.ValueField
}

// envelopeField returns the configured envelope field name, falling
// back to the package default. An explicit empty string disables the
// envelope-shortcut path; callers that want to keep the path enabled
// while supplying a partial config should call DefaultAdapterConfig
// and override only what they care about.
func (c *AdapterConfig) envelopeField() string {
	if c == nil {
		return DefaultEnvelopeField
	}
	return c.EnvelopeField
}

// outputMetricName returns the configured fallback metric name used on
// encode when the envelope itself doesn't carry one.
func (c *AdapterConfig) outputMetricName() string {
	if c == nil || c.OutputMetricName == "" {
		return DefaultOutputMetricName
	}
	return c.OutputMetricName
}
