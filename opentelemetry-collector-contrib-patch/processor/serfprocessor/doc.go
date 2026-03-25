// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package serfprocessor compresses incoming Gauge and Sum metrics using
// Serf XOR time-series encoding and uploads compressed blocks to S3 or local disk.
//
// Serf improves on Gorilla's XOR encoding by applying a "best-approximation"
// search (FindAppLong) that adjusts each float64 value within an error bound
// [v-maxDiff, v+maxDiff] to maximise leading-zero bits in the XOR with the
// previous stored value, yielding better compression ratios on sensor data.
package serfprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/serfprocessor"
