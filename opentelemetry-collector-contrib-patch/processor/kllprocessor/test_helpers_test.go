// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kllprocessor

import (
	"fmt"

	kll "github.com/ProjectASAP/sketchlib-go/sketches/KLL"
	"google.golang.org/protobuf/proto"
)

// newKLLSketch builds a fresh sketchlib-go KLL respecting the seed
// nullability contract documented on Config.Seed. Test-only helper —
// production code constructs sketches via
// asap-precompute-go/sketches.NewKLLWrapper.
func newKLLSketch(cfg *Config) *kll.KLLSketch {
	if cfg.Seed != nil {
		sk, err := kll.NewKLLSketchWithSeed(cfg.K, *cfg.Seed)
		if err != nil {
			return nil
		}
		return sk
	}
	sk, err := kll.NewKLLSketch(cfg.K)
	if err != nil {
		return nil
	}
	return sk
}

// serializeKLLSketch matches the legacy emit-side serializer
// (SerializePortable + proto.Marshal) so the wire bytes stay
// byte-identical. Test-only helper.
func serializeKLLSketch(sk *kll.KLLSketch) ([]byte, error) {
	if sk == nil {
		return nil, nil
	}
	env, err := sk.SerializePortable()
	if err != nil {
		return nil, fmt.Errorf("kll.SerializePortable: %w", err)
	}
	return proto.Marshal(env)
}
