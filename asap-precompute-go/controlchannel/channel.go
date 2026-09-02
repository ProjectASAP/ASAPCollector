// Package controlchannel delivers controller-compiled physical plans to
// runtime adapters. HTTP polling and typed OpAMP custom-message transports are
// implemented; both expose the same Poll/Ack activation boundary.
package controlchannel

import "github.com/ProjectASAP/asap-precompute-go"

// ControlChannel delivers PrecomputeConfig from the controller to
// adapters at runtime.
type ControlChannel interface {
	// Poll returns a non-nil PrecomputeConfigSet if the plan has
	// changed since the last poll. nil means "no change."
	// Implementations may block briefly on network I/O; callers
	// typically call from a dedicated goroutine.
	Poll() *precompute.PrecomputeConfigSet

	// Ack confirms acceptance of a plan version. OpAMP uses this
	// to report effective config back to the supervisor; HTTP-poll
	// implementations may ignore.
	Ack(planVersion uint64)
}
