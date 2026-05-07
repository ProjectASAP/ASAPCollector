package kllprocessor

import "testing"

// TestDefaultDeltaTransmissionFalse asserts that the factory's default
// Config has `DeltaTransmission: false`. KLL has no delta variant —
// it uses randomised compaction so two sketches over the same input
// history are not bit-identical and not linearly mergeable. The
// `Config.Validate` step rejects `DeltaTransmission: true` outright,
// so the default MUST stay `false` for the factory's default config to
// validate.
//
// This test pairs with the four other families'
// `TestDefaultDeltaTransmissionTrue` (DDSketch / HLL / CountSketch /
// Count-Min) and codifies the cross-family policy: delta-by-default
// for the four mergeable families, full-state by design for KLL. See
// `Implementation.tex` ("KLL has no delta variant and matches its
// full cost") and the doc comment on `createDefaultConfig` for the
// rationale.
func TestDefaultDeltaTransmissionFalse(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	if cfg.DeltaTransmission {
		t.Fatalf("expected DeltaTransmission=false by default for KLL (no delta variant), got %v", cfg.DeltaTransmission)
	}
	// And confirm the default config validates — a regression here
	// would mean somebody set DeltaTransmission=true and Validate
	// (which rejects it) would block the processor from booting.
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default KLL config must validate: %v", err)
	}
}
