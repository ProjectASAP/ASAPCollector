// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// mvp/step2.1: Prometheus TSDB block-format writer.
//
// Adds a second cold-store on-disk layout that writes each flush
// window into a Prometheus TSDB block:
//
//	<bucket>/<ulid>/chunks/000001       (Prometheus chunks file)
//	<bucket>/<ulid>/index               (Prometheus index file)
//	<bucket>/<ulid>/meta.json           (Prometheus block metadata)
//
// The block format is byte-compatible with what Prometheus' TSDB
// produces, so a Thanos store-gateway pointed at the bucket can
// answer arbitrary PromQL with no custom engine code.
//
// Implementation note: we use Prometheus' own `tsdb.BlockWriter`
// rather than reimplementing the index / chunks file format. The
// writer wraps an in-memory `Head` block, accepts samples via
// `storage.Appender`, and on `Flush(ctx)` invokes the upstream
// `LeveledCompactor` to materialise the on-disk block. This avoids
// any semantic divergence from the canonical Prometheus block
// layout — every byte we emit was emitted by the same code that
// runs inside Prometheus itself.
//
// Structural gap discovered: `tsdb.BlockWriter` requires *strict
// non-decreasing timestamps per series* (it inherits the Head's
// out-of-order rules). Since the agent-side window buffer is
// timestamp-sorted in `sortAndEncode` for the ASAP path but the
// writer is fed series-by-series, we sort each series' points
// before appending. Across series order does not matter — the Head
// indexes by series ref. See `appendWindow`.

package gorillas3processor

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"go.uber.org/zap"
)

// zapToSlog returns a `log/slog` logger that drops every record on
// the floor. The Prometheus tsdb package logs exclusively through
// `*slog.Logger`; the surrounding processor uses `*zap.Logger`. Step
// 2.1 keeps the two log paths separate — TSDB internals are noisy
// at info level and we don't want to spam our agent logs. Step 2.4
// can plumb a real bridge if operators want the diagnostics.
func zapToSlog(_ *zap.Logger) *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// tsdbBlockArtifact is one materialised Prometheus block ready for
// upload. The relative path keys are exactly what Thanos / Prometheus
// expect: `chunks/000001`, `index`, `meta.json`.
type tsdbBlockArtifact struct {
	// ULID is the block id ("<ulid>").
	ULID ulid.ULID
	// Files maps `<ulid>/<relpath>` (no leading slash) to file bytes.
	// Always contains `chunks/000001`, `index`, `meta.json`. The
	// chunks file may be split into multiple parts (`000002`, ...)
	// for very large blocks; we surface them all.
	Files map[string][]byte
	// MinTime / MaxTime in milliseconds since the Unix epoch.
	MinTime int64
	MaxTime int64
	// NumSeries / NumSamples are read back from the meta after
	// flush; useful for self-monitoring.
	NumSeries  uint64
	NumSamples uint64
}

// tsdbBlockBuilder turns a per-window seriesKey → seriesBuffer map
// into a flushed Prometheus block on disk, then reads it back so the
// caller can ship the bytes to S3.
type tsdbBlockBuilder struct {
	logger         *slog.Logger
	blockSize      int64 // milliseconds
	externalLabels map[string]string
}

func newTSDBBlockBuilder(blockDuration time.Duration, ext map[string]string, slogLogger *slog.Logger) *tsdbBlockBuilder {
	if slogLogger == nil {
		slogLogger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if blockDuration <= 0 {
		blockDuration = 60 * time.Second
	}
	return &tsdbBlockBuilder{
		logger:         slogLogger,
		blockSize:      blockDuration.Milliseconds(),
		externalLabels: cloneStringMap(ext),
	}
}

// build flushes the supplied window into one Prometheus TSDB block
// under a fresh temp directory, reads the artifact files, removes
// the temp directory, and returns the artifact in memory.
//
// An empty window returns (nil, nil).
func (b *tsdbBlockBuilder) build(ctx context.Context, window map[seriesKey]*seriesBuffer) (*tsdbBlockArtifact, error) {
	if len(window) == 0 {
		return nil, nil
	}
	tmpRoot, err := os.MkdirTemp("", "gorillas3-tsdb-")
	if err != nil {
		return nil, fmt.Errorf("mkdtemp: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpRoot) }()

	bw, err := tsdb.NewBlockWriter(b.logger, tmpRoot, b.blockSize)
	if err != nil {
		return nil, fmt.Errorf("tsdb.NewBlockWriter: %w", err)
	}
	defer func() { _ = bw.Close() }()

	if err := b.appendWindow(ctx, bw, window); err != nil {
		return nil, err
	}

	id, err := bw.Flush(ctx)
	if err != nil {
		return nil, fmt.Errorf("tsdb BlockWriter.Flush: %w", err)
	}
	if id == (ulid.ULID{}) {
		// No samples ended up appended (e.g. all empty buffers).
		return nil, nil
	}

	blockDir := filepath.Join(tmpRoot, id.String())
	files, err := readBlockFiles(blockDir, id.String())
	if err != nil {
		return nil, err
	}
	mint, maxt := windowBounds(window)
	numSeries, numSamples := countSeriesSamples(window)
	return &tsdbBlockArtifact{
		ULID:       id,
		Files:      files,
		MinTime:    mint,
		MaxTime:    maxt,
		NumSeries:  numSeries,
		NumSamples: numSamples,
	}, nil
}

// appendWindow drives `BlockWriter.Appender` over every (series,
// point) pair. Each series' points are sorted ascending by
// timestamp first; the Head accepts in any across-series order but
// rejects out-of-order timestamps inside a single series.
func (b *tsdbBlockBuilder) appendWindow(ctx context.Context, bw *tsdb.BlockWriter, window map[seriesKey]*seriesBuffer) error {
	app := bw.Appender(ctx)
	for sk, buf := range window {
		if buf == nil || len(buf.points) == 0 {
			continue
		}
		ls := b.labelsFor(sk.metricName, buf.attributes)
		// Defensive: tsdb rejects empty label sets; metric name
		// always provides `__name__` so this never trips, but
		// guard anyway.
		if ls.Len() == 0 {
			continue
		}
		// Sort a local copy so we don't mutate the caller's
		// buffer order (the ASAP path may run on the same
		// snapshot when block_format=both).
		pts := make([]point, len(buf.points))
		copy(pts, buf.points)
		sort.Slice(pts, func(i, j int) bool { return pts[i].ts < pts[j].ts })

		var ref storage.SeriesRef
		for _, p := range pts {
			// tsdb timestamps are milliseconds since Unix epoch.
			tms := p.ts / int64(time.Millisecond)
			r, err := app.Append(ref, ls, tms, p.v)
			if err != nil {
				_ = app.Rollback()
				return fmt.Errorf("tsdb appender.Append: %w", err)
			}
			ref = r
		}
	}
	if err := app.Commit(); err != nil {
		return fmt.Errorf("tsdb appender.Commit: %w", err)
	}
	return nil
}

// labelsFor merges the metric name (`__name__`), the series
// attributes, and the configured external labels into a single
// canonical (sorted-by-name) labels.Labels. External labels win
// over series attributes which win over metric name conflicts.
//
// The merge is deterministic so the same `(metric, attrs)` tuple
// always produces the same series ref inside a block — important
// for tests and for postings stability across windows.
func (b *tsdbBlockBuilder) labelsFor(metricName string, attrs map[string]string) labels.Labels {
	bld := labels.NewBuilder(labels.EmptyLabels())
	bld.Set(labels.MetricName, sanitizeMetric(metricName))
	for k, v := range attrs {
		bld.Set(k, v)
	}
	for k, v := range b.externalLabels {
		bld.Set(k, v)
	}
	return bld.Labels()
}

// readBlockFiles slurps every regular file under `<dir>` into a map
// keyed by `<ulidStr>/<relpath>`. The relpath uses forward slashes
// regardless of the host OS so the keys are S3-ready.
func readBlockFiles(dir, ulidStr string) (map[string][]byte, error) {
	out := make(map[string][]byte)
	err := filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, relErr := filepath.Rel(dir, p)
		if relErr != nil {
			return relErr
		}
		body, readErr := os.ReadFile(p)
		if readErr != nil {
			return readErr
		}
		key := ulidStr + "/" + filepath.ToSlash(rel)
		out[key] = body
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk block dir: %w", err)
	}
	return out, nil
}

// windowBounds returns the (min, max) sample timestamps in
// milliseconds. tsdb's block layer reads these from the Head, but
// we surface them on the artifact so callers can log/index without
// reopening the block.
func windowBounds(window map[seriesKey]*seriesBuffer) (int64, int64) {
	const ms = int64(time.Millisecond)
	var mint, maxt int64
	first := true
	for _, buf := range window {
		if buf == nil {
			continue
		}
		for _, p := range buf.points {
			tms := p.ts / ms
			if first {
				mint, maxt = tms, tms
				first = false
				continue
			}
			if tms < mint {
				mint = tms
			}
			if tms > maxt {
				maxt = tms
			}
		}
	}
	return mint, maxt
}

func countSeriesSamples(window map[seriesKey]*seriesBuffer) (uint64, uint64) {
	var nseries, nsamples uint64
	for _, buf := range window {
		if buf == nil || len(buf.points) == 0 {
			continue
		}
		nseries++
		nsamples += uint64(len(buf.points))
	}
	return nseries, nsamples
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
