// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sketches

import (
	"testing"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// TestCountSketch_RowSampledObserve_ComposesWithGOS proves the collector-side
// reattachment of ASAPCollector #518's SDK row-admission path onto GOS: an
// ObservationValue with RowSampled=true routes through
// CountSketchWrapper.ApplyAdmittedOccurrence rather than silently bypassing
// insert-time detection, so a sampled occurrence still wakes the flush loop
// and gets captured in the drained delta once its cell crosses threshold.
func TestCountSketch_RowSampledObserve_ComposesWithGOS(t *testing.T) {
	w, err := NewCountSketchWrapper(1, 2)
	if err != nil {
		t.Fatalf("NewCountSketchWrapper: %v", err)
	}
	w.SetGosMode(0.9, 1)
	obs := CountSketchObserver{DefaultKey: "fallback"}

	if w.ConsumeWakeSignal() {
		t.Fatal("wake must not be armed before any insert")
	}

	// admittedRows: bit 0 set (this wrapper has exactly 1 row).
	v := precompute.ObservationValue{
		Kind:         precompute.KindFloat,
		Float:        1.0,
		Bytes:        []byte("k"),
		RowSampled:   true,
		AdmittedRows: 0b1,
		SampleP:      0.5,
	}
	var fired bool
	for i := 0; i < 200; i++ {
		if err := obs.Observe(w, v); err != nil {
			t.Fatalf("Observe: %v", err)
		}
		if w.ConsumeWakeSignal() {
			fired = true
			break
		}
	}
	if !fired {
		t.Fatal("expected the wake signal to fire from a row-sampled admitted occurrence under GOS")
	}

	payload, isFull, err := w.ComputeDeltaAgainst(nil, 1<<30)
	if err != nil {
		t.Fatalf("ComputeDeltaAgainst: %v", err)
	}
	if isFull {
		t.Fatal("GOS drain must never report isFull")
	}
	if len(payload) == 0 {
		t.Fatal("expected a non-empty drained delta from the row-sampled crossing")
	}
}

// TestCountSketch_PerRowSampler_ComposesWithGOS proves the OTHER sampling
// path (a live per-row geometric sampler installed via WithSampleP, driving
// UpdateString directly rather than through ApplyAdmittedOccurrence) also
// still triggers GOS detection after the #518 reattachment replaced the old
// whole-item Admit()+divide branch with UpdateStringSampledPerRow(GOS).
func TestCountSketch_PerRowSampler_ComposesWithGOS(t *testing.T) {
	w, err := NewCountSketchWrapper(5, 1024)
	if err != nil {
		t.Fatalf("NewCountSketchWrapper: %v", err)
	}
	w.SetGosMode(0.9, 1)
	w.WithSampleP(0.9) // high p so the loop below reliably admits within N tries

	var fired bool
	for i := 0; i < 2000 && !fired; i++ {
		w.UpdateString("k", 1.0)
		fired = w.ConsumeWakeSignal()
	}
	if !fired {
		t.Fatal("expected the wake signal to fire under per-row sampling + GOS")
	}
}

// TestCMS_RowSampledObserve_ComposesWithGOS is the CMS counterpart of
// TestCountSketch_RowSampledObserve_ComposesWithGOS.
func TestCMS_RowSampledObserve_ComposesWithGOS(t *testing.T) {
	w := NewCMSWrapper(1, 2, false)
	w.SetGosMode(0.9, 1)
	obs := CMSObserver{}

	v := precompute.ObservationValue{
		Kind:         precompute.KindBytes,
		Bytes:        []byte("k"),
		RowSampled:   true,
		AdmittedRows: 0b1,
		SampleP:      0.5,
	}
	var fired bool
	for i := 0; i < 200; i++ {
		if err := obs.Observe(w, v); err != nil {
			t.Fatalf("Observe: %v", err)
		}
		if w.ConsumeWakeSignal() {
			fired = true
			break
		}
	}
	if !fired {
		t.Fatal("expected the wake signal to fire from a row-sampled admitted occurrence under GOS")
	}

	payload, isFull, err := w.ComputeDeltaAgainst(nil, 1<<30)
	if err != nil {
		t.Fatalf("ComputeDeltaAgainst: %v", err)
	}
	if isFull {
		t.Fatal("GOS drain must never report isFull")
	}
	if len(payload) == 0 {
		t.Fatal("expected a non-empty drained delta from the row-sampled crossing")
	}
}
