// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package gorillas3processor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_Defaults(t *testing.T) {
	cfg := &Config{Bucket: "asap-gorilla"}
	require.NoError(t, cfg.Validate())
	assert.Equal(t, 60*time.Second, cfg.WindowInterval)
	assert.Equal(t, defaultPrefixTemplate, cfg.PrefixTemplate)
	assert.Equal(t, "default", cfg.Tenant)
	assert.Equal(t, "us-east-1", cfg.Region)
	assert.Equal(t, 3, cfg.MaxRetries)
	assert.Equal(t, time.Second, cfg.RetryBackoff)
	assert.Equal(t, 30*time.Second, cfg.UploadTimeout)
}

func TestConfig_BucketRequired(t *testing.T) {
	cfg := &Config{}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bucket")
}

func TestConfig_CredentialPairing(t *testing.T) {
	cfg := &Config{
		Bucket:      "asap-gorilla",
		AccessKeyID: "ak",
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "access_key_id")
}

func TestConfig_OverridesPreserved(t *testing.T) {
	cfg := &Config{
		Bucket:         "b",
		WindowInterval: 30 * time.Second,
		PrefixTemplate: "{tenant}/x/{metric}/",
		Tenant:         "shop",
		Region:         "eu-west-1",
		MaxRetries:     7,
	}
	require.NoError(t, cfg.Validate())
	assert.Equal(t, 30*time.Second, cfg.WindowInterval)
	assert.Equal(t, "{tenant}/x/{metric}/", cfg.PrefixTemplate)
	assert.Equal(t, "shop", cfg.Tenant)
	assert.Equal(t, "eu-west-1", cfg.Region)
	assert.Equal(t, 7, cfg.MaxRetries)
}

func TestRenderPrefix(t *testing.T) {
	t1 := time.Date(2026, 5, 6, 13, 7, 12, 0, time.UTC)
	got := renderPrefix(defaultPrefixTemplate, "tnt", "cpu.user", t1)
	assert.Equal(t, "tnt/cpu.user/2026/05/06/13/", got)
}

func TestRenderPrefix_SanitizeMetric(t *testing.T) {
	t1 := time.Date(2026, 5, 6, 13, 7, 12, 0, time.UTC)
	got := renderPrefix(defaultPrefixTemplate, "tnt", "ns/foo bar", t1)
	assert.Equal(t, "tnt/ns_foo_bar/2026/05/06/13/", got)
}

func TestBuildObjectKey(t *testing.T) {
	t1 := time.Date(2026, 5, 6, 13, 7, 12, 0, time.UTC)
	pfx := renderPrefix(defaultPrefixTemplate, "tnt", "cpu.user", t1)
	key := buildObjectKey(pfx, t1, 3)
	assert.Contains(t, key, "tnt/cpu.user/2026/05/06/13/part-")
	assert.Contains(t, key, "-000003.gor")
}
