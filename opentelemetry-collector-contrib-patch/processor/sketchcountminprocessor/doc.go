// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

// Package sketchmetricsprocessor maintains Count-Min sketches over incoming metrics and
// emits serialized sketches and approximate top-k summaries.
package sketchcountminprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/sketchmetricsprocessor"
