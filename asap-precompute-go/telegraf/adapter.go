package telegraf

import (
	"github.com/influxdata/telegraf"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// Adapter is a thin wrapper around the package-level Decode and
// Encode helpers. It exists so callers (notably the future
// allsketches plugin in telegraf-patch/processors/allsketches) can
// thread a single configured object through their lifecycle rather
// than passing AdapterConfig around alongside the helper calls.
//
// Unlike asap-precompute-go/otel.Adapter, this codec does NOT
// implement the runtime's full precompute.Adapter trait — it owns no
// goroutines, no tickers, and no scheduling. Lifecycle (Start / Stop /
// flush ticker) lives in the Telegraf StreamingProcessor plugin
// layer; this struct is pure data-shape translation. Phase C of the
// integration plan introduces the plugin and is out of scope here.
//
// The zero-value Adapter is not usable; construct via NewAdapter.
type Adapter struct {
	cfg *AdapterConfig
}

// NewAdapter constructs an Adapter with the supplied config. A nil
// cfg falls back to DefaultAdapterConfig values lazily on each call
// (the helpers' nil-tolerance handles this).
func NewAdapter(cfg *AdapterConfig) *Adapter {
	return &Adapter{cfg: cfg}
}

// Config returns the AdapterConfig the Adapter was constructed with.
// Returned pointer is the same instance held internally; callers
// should not mutate it concurrently with Decode / Encode calls.
func (a *Adapter) Config() *AdapterConfig {
	if a == nil {
		return nil
	}
	return a.cfg
}

// Decode wraps the package-level Decode using the Adapter's config.
// Returns an error for missing / wrong-type value fields — see
// Decode's docstring for the full contract.
func (a *Adapter) Decode(m telegraf.Metric) (*precompute.Observation, error) {
	return Decode(m, a.cfg)
}

// Encode wraps the package-level Encode using the Adapter's config.
// Returns an error if any envelope is nil — see Encode's docstring
// for the full contract.
func (a *Adapter) Encode(envs []*precompute.SketchEnvelope) ([]telegraf.Metric, error) {
	return Encode(envs, a.cfg)
}
