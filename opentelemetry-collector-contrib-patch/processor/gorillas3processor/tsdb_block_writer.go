// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package gorillas3processor

import (
	"context"
	"fmt"
	"time"

	gorilla "github.com/ProjectASAP/asap-gorilla-go"
	"github.com/oklog/ulid/v2"
)

// tsdbBlockArtifact preserves the processor-local test surface while the
// implementation lives in github.com/ProjectASAP/asap-gorilla-go.
type tsdbBlockArtifact struct {
	ULID          ulid.ULID
	Files         map[string][]byte
	MinTime       int64
	MaxTime       int64
	NumSeries     uint64
	NumSamples    uint64
	NumOOBDropped uint64
}

// tsdbBlockBuilder is a thin adapter over the shared streaming builder. Runtime
// processors should not carry TSDB/Gorilla block-writing implementations.
type tsdbBlockBuilder struct {
	externalLabels map[string]string
	reorderGrace   time.Duration
}

func newTSDBBlockBuilder(_ time.Duration, ext map[string]string, _ any) *tsdbBlockBuilder {
	return &tsdbBlockBuilder{
		externalLabels: cloneMap(ext),
		reorderGrace:   2 * time.Second,
	}
}

func (b *tsdbBlockBuilder) build(ctx context.Context, window map[seriesKey]*seriesBuffer) (*tsdbBlockArtifact, error) {
	if len(window) == 0 {
		return nil, nil
	}
	builder, err := gorilla.NewStreamingTSDBBlockBuilder(gorilla.StreamingTSDBOptions{
		ReorderGrace:   b.reorderGrace,
		ExternalLabels: b.externalLabels,
	})
	if err != nil {
		return nil, err
	}
	for sk, buf := range window {
		if buf == nil {
			continue
		}
		for _, p := range buf.points {
			if err := builder.AddSample(gorilla.TSDBSample{
				MetricName: sk.metricName,
				Attributes: buf.attributes,
				Timestamp:  time.Unix(0, p.ts),
				Value:      p.v,
			}); err != nil {
				return nil, fmt.Errorf("streaming tsdb add sample: %w", err)
			}
		}
	}
	art, err := builder.Finalize(ctx)
	if err != nil {
		return nil, err
	}
	if art == nil {
		return nil, nil
	}
	return &tsdbBlockArtifact{
		ULID:          art.ULID,
		Files:         art.Files,
		MinTime:       art.MinTime,
		MaxTime:       art.MaxTime,
		NumSeries:     art.NumSeries,
		NumSamples:    art.NumSamples,
		NumOOBDropped: art.NumOOODropped,
	}, nil
}

func cloneMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
