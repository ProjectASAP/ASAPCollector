// Package controlchannel defines the contract by which the controller
// delivers PrecomputeConfig plans to runtime adapters. Three
// implementations land separately (OpAmpChannel, HttpPollChannel,
// FileWatchChannel — see ADR-0003); step 2.10 of phase-2-execution-plan.md
// is when those land. Phase 2 (this PR) only ships the trait.
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
