// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package gorillas3processor compresses incoming Gauge and Sum metrics
// using Gorilla XOR-delta encoding and uploads chunks to an S3-compatible
// object store (e.g. AWS S3, MinIO) on a tumbling window. It is the
// asap-otel-agent S3-cold-engine tier (Phase 2 of Gorilla-S3-cold-engine).
//
// Block layout is byte-compatible with the Phase 1 Rust `asap-gorilla`
// decoder (magic "GORILLA1", little-endian header) and with the Telegraf
// `gorilla_s3` output plugin (which consumes pre-encoded payloads with
// the same body shape).
package gorillas3processor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/gorillas3processor"
