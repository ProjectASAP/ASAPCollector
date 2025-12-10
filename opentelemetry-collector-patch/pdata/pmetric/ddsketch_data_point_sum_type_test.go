// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package pmetric

import "testing"

func TestDDSketchDataPointSumTypeString(t *testing.T) {
	tests := []struct {
		name string
		val  DDSketchDataPointSumType
		want string
	}{
		{"empty", DDSketchDataPointSumTypeEmpty, "Empty"},
		{"double", DDSketchDataPointSumTypeDouble, "Double"},
		{"int", DDSketchDataPointSumTypeInt, "Int"},
		{"unknown", DDSketchDataPointSumTypeInt + 1, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.val.String(); got != tt.want {
				t.Fatalf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}
