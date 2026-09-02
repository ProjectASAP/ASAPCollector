package precompute

import "time"

// Adapter is the contract every Layer-4 platform shim implements.
// See ADR-0003. The Event type is the host's native event/data
// model (currently pmetric.Metrics for OTel).
//
// Go does not have generic interface methods that erase the type
// parameter cleanly, so adapters in Go embed Event as `any` and
// type-assert internally; concrete adapter types in the otel/
// subpackage do the assertion in one place.
type Adapter interface {
	// Decode converts a host-native event into one or more
	// host-neutral Observations. MUST recognize sketch-typed
	// inputs and produce ObservationValueKind=KindEnvelope rather
	// than expanding them to scalar (bandwidth invariant §5.2).
	Decode(ev any) ([]Observation, error)

	// Encode converts a slice of host-neutral SketchEnvelopes
	// back into a host-native event suitable for handoff to the
	// host's downstream pipeline.
	Encode(envelopes []*SketchEnvelope) (any, error)

	// ScheduleTick installs a periodic callback at the requested
	// period. The callback runs in a goroutine the Adapter owns;
	// returns a cancel function to stop the goroutine on
	// shutdown. The callback typically calls Precompute.Tick(now)
	// and feeds output back via Encode + the host's downstream
	// dispatch.
	ScheduleTick(period time.Duration, cb func()) (cancel func())

	// EmitTelemetry publishes the Precompute's runtime stats
	// through the host's native metric channel.
	EmitTelemetry(stats *PrecomputeStats)
}
