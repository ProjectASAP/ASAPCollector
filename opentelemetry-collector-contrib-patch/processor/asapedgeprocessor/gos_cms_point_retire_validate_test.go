// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"strings"
	"testing"
	"time"
)

// TestConfigValidate_CMSPointRetiredUnderGOS covers the retirement of CMS's
// local point-query readout (threshold.functional=cms_point) once GOS mode is
// active: CMS's min-across-rows estimator is poisoned by any single row a
// GOS-active cell reset in place at insert time, so the combination is
// rejected at boot. Plain CountSketch implements the SAME functional via a
// median across signed rows (not min), which tolerates a reset row fine, so
// it is deliberately left unaffected.
func TestConfigValidate_CMSPointRetiredUnderGOS(t *testing.T) {
	mkCMS := func(gosEps float64, thresholdEnabled bool, functional string) *Config {
		return &Config{
			ShardCount:     1,
			WindowDuration: time.Hour,
			Metrics: []MetricFamily{{
				Metric:          "m",
				Family:          FamilyCountMinSketch,
				GosDeltaEpsilon: gosEps,
				GosSites:        1,
				Threshold: &ThresholdConfig{
					Enabled:        thresholdEnabled,
					Functional:     functional,
					Key:            "x",
					CoordinatorURL: "passthrough:///test",
					Tau:            100,
					Epsilon:        0.05,
				},
			}},
		}
	}

	// Rejected: CMS + gos_delta_epsilon>0 + threshold.functional=cms_point.
	if err := mkCMS(0.5, true, "cms_point").Validate(); err == nil {
		t.Fatal("validate(cms + gos + cms_point) expected an error, got nil")
	} else if !strings.Contains(err.Error(), "cms_point") {
		t.Fatalf("expected a cms_point-specific error, got %v", err)
	}

	// Accepted: CMS + gos_delta_epsilon>0 with no cms_point monitor (threshold
	// disabled) — GOS itself is unaffected by this retirement.
	if err := mkCMS(0.5, false, "cms_point").Validate(); err != nil {
		t.Fatalf("validate(cms + gos, threshold disabled) unexpected error: %v", err)
	}

	// Accepted: CMS + gos_delta_epsilon>0 + threshold.functional=sum (a
	// different functional is unaffected by the cms_point-specific gate).
	if err := mkCMS(0.5, true, "sum").Validate(); err != nil {
		t.Fatalf("validate(cms + gos + sum monitor) unexpected error: %v", err)
	}

	// Accepted: CMS + threshold.functional=cms_point with NO GOS mode (fixed
	// mode never resets cells mid-window, so the local min read stays valid).
	if err := mkCMS(0, true, "cms_point").Validate(); err != nil {
		t.Fatalf("validate(cms + cms_point, no gos) unexpected error: %v", err)
	}

	// Accepted: plain (non-heap) CountSketch + gos_delta_epsilon>0 +
	// threshold.functional=cms_point — CountSketch's median-based
	// EstimateCount serves this functional unaffected by GOS mode.
	csCfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		Metrics: []MetricFamily{{
			Metric:          "events",
			Family:          FamilyCountSketch,
			GosDeltaEpsilon: 0.5,
			GosSites:        1,
			Threshold: &ThresholdConfig{
				Enabled:        true,
				Functional:     "cms_point",
				Key:            "x",
				CoordinatorURL: "passthrough:///test",
				Tau:            100,
				Epsilon:        0.05,
			},
		}},
	}
	if err := csCfg.Validate(); err != nil {
		t.Fatalf("validate(countsketch + gos + cms_point) unexpected error: %v", err)
	}
}
