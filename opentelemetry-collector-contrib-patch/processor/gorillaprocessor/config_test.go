// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package gorillaprocessor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_Validate_S3(t *testing.T) {
	cfg := &Config{
		WindowInterval: 5 * time.Minute,
		S3: S3Config{
			Bucket: "my-bucket",
			Region: "us-east-1",
		},
	}
	require.NoError(t, cfg.Validate())
	assert.Equal(t, int64(8*1024*1024), cfg.S3.MultipartThreshold)
	assert.Equal(t, 3, cfg.S3.MaxRetries)
}

func TestConfig_Validate_LocalOnly(t *testing.T) {
	cfg := &Config{
		WindowInterval: time.Minute,
		LocalDir:       "/tmp/gorilla",
	}
	require.NoError(t, cfg.Validate())
}

func TestConfig_Validate_NeitherS3NorLocal(t *testing.T) {
	cfg := &Config{
		WindowInterval: time.Minute,
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one of s3.bucket or local_dir")
}

func TestConfig_Validate_S3MissingRegion(t *testing.T) {
	cfg := &Config{
		WindowInterval: time.Minute,
		S3: S3Config{
			Bucket: "my-bucket",
		},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "s3.region is required")
}

func TestConfig_Validate_DefaultWindowInterval(t *testing.T) {
	cfg := &Config{
		LocalDir: "/tmp/gorilla",
	}
	require.NoError(t, cfg.Validate())
	assert.Equal(t, 10*time.Minute, cfg.WindowInterval)
}

func TestConfig_Validate_MultipartPartBytesMinimum(t *testing.T) {
	cfg := &Config{
		LocalDir: "/tmp/gorilla",
		S3: S3Config{
			Bucket:             "my-bucket",
			Region:             "us-east-1",
			MultipartPartBytes: 1024, // too small
		},
	}
	require.NoError(t, cfg.Validate())
	assert.Equal(t, int64(5*1024*1024), cfg.S3.MultipartPartBytes)
}
