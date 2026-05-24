// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package gorillas3processor compresses incoming Gauge and Sum metrics
// using Prometheus Gorilla XOR-delta encoding and uploads blocks to an
// S3-compatible object store (e.g. AWS S3, MinIO) on a tumbling window.
// It is the asap-otel-agent S3-cold-engine tier.
//
// Blocks are written as Prometheus TSDB blocks (StreamingTSDBBlockBuilder)
// or as XOR-chunk fragments (StreamingFragmentEncoder) consumed by the
// backend gorilla-merger and served through Thanos ("Path A2"). The
// superseded custom GORILLA1 container format has been removed.
package gorillas3processor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/gorillas3processor"
