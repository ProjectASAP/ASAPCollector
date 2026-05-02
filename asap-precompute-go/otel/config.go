// Package otel is the Layer-4 OTel Adapter for the host-neutral
// asap-precompute-go runtime. It implements precompute.Adapter for
// pmetric.Metrics, exposing the OTel-specific tunables that aren't
// part of the host-neutral PrecomputeConfig.
package otel

// AdapterConfig holds OTel-specific config knobs that aren't part
// of the host-neutral PrecomputeConfig. These are the per-platform
// "output naming and pmetric pass-through" concerns from gaps 1, 2,
// 5, 6 of the feasibility audit.
type AdapterConfig struct {
	// MetricSuffix is appended to the original metric name when
	// emitting sketch envelopes back as pmetric.Metric. E.g.
	// input "http.server.duration" + suffix "_ddsketch" → output
	// "http.server.duration_ddsketch". Mirrors today's per-
	// processor MetricSuffix knob.
	MetricSuffix string
	// MetricName, if set, overrides the output metric name
	// entirely (CMS-specific in today's processors). Takes
	// precedence over MetricSuffix.
	MetricName string
	// DropOriginal controls whether the input pmetric.Metrics is
	// forwarded to the next consumer. Today's processors default
	// to false (forward) — see PR #211. Setting true means only
	// the synthesized sketch output reaches the next consumer.
	DropOriginal bool
	// ReadAsInt picks how Gauge data points are read: when true
	// use NumberDataPoint.IntValue(), else DoubleValue(). KLL
	// processor's `is_int` knob.
	ReadAsInt bool
	// WriteSeen, when true on KLL, emits zero-count placeholder
	// metrics for keys observed but with no in-window samples,
	// so downstream consumers see consistent series counts.
	// Default false. Equivalent to today's KLL processor's
	// `write_seen` knob.
	WriteSeen bool
	// ScopeName is the InstrumentationScope name attached to
	// emitted ScopeMetrics on the Encode path. Defaults to
	// "asap_precompute" when empty.
	ScopeName string
}

// scopeNameOrDefault returns the configured scope name or the
// package-default "asap_precompute".
func (c *AdapterConfig) scopeNameOrDefault() string {
	if c == nil || c.ScopeName == "" {
		return "asap_precompute"
	}
	return c.ScopeName
}

// metricNameFor returns the output metric name for `inputName` after
// applying MetricName (override) and MetricSuffix (append).
func (c *AdapterConfig) metricNameFor(inputName string) string {
	if c == nil {
		return inputName
	}
	if c.MetricName != "" {
		return c.MetricName
	}
	return inputName + c.MetricSuffix
}
