// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package pmetric

import "testing"

func TestDDSketchDataPointMinTypeString(t *testing.T) {
	tests := []struct {
		name string
		val  DDSketchDataPointMinType
		want string
	}{
		{"empty", DDSketchDataPointMinTypeEmpty, "Empty"},
		{"double", DDSketchDataPointMinTypeDouble, "Double"},
		{"int", DDSketchDataPointMinTypeInt, "Int"},
		{"unknown", DDSketchDataPointMinTypeInt + 1, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.val.String(); got != tt.want {
				t.Fatalf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}
