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
//
// mvp/issue46: The "across series order does not matter" claim above
// turned out to be WRONG. The Head's per-appender `minValidTime` is
// FROZEN at appender-creation time and seeded by the first appended
// sample via the `initAppender` bootstrap (`initTime(t) → headMaxt = t
// → appender.minValidTime = t - chunkRange/2`). Subsequent samples
// older than that initial floor are rejected with
// `storage.ErrOutOfBounds`. If we visit series in random map-
// iteration order — say series A (earliest sample @t=58s) before
// series B (earliest sample @t=0s) — initAppender anchors at 58s,
// the floor becomes 28s, and B's t=0s sample → OOB. PR#338's high-
// cardinality rotating series (`unique_users_per_min`,
// `top_endpoint_qps`) made this trip on every flush.
//
// PR#354 originally fixed this by flattening every (labels, ts, v)
// tuple into one slice, globally sorting by timestamp, then driving
// Append from the sorted flat list. Correct, but it allocates an
// O(N_samples) array (~80 B per slot — ts+value+labels.Labels header
// + *SeriesRef pointer) IN ADDITION to the existing per-series buffer
// copies, AND it interleaves Head series creation across every series
// in the block — so every series' open `headChunks` stays resident
// through the entire flush. With a 5 K-cardinality / 60 s window
// that's 1.2 M samples × 80 B + 5 K simultaneously-open chunks → on
// the order of 150 MiB of churn per flush per gorillas3 instance,
// multiplied by the 6 gorillas3 instances the 5-sketch routing
// topology spawns (one per per-family pipeline). Result: the flush
// itself OOM-killed the agent at the 60 s tick, before any block
// reached MinIO. See PR #355 / fix/thanos-archive-write-regression.
//
// Fix (PR #355): keep the per-series append path (no flat slice),
// but *order the series visits* by each series' earliest sample
// timestamp. That guarantees the very first call to `app.Append(...)`
// carries the global minimum timestamp, anchoring
// `appender.minValidTime` at `globalMin - chunkRange/2`. Every other
// in-window sample is >= the global min, so it's >= the floor and
// passes. Memory is bounded by the same working set the writer
// already needed (one series' sorted points + Head's per-series
// state) — no per-flush O(N_samples) flat slice.
//
// Late-arriving samples that genuinely fall outside the block window
// (e.g. a stale point dredged up by an upstream replay > chunkRange/2
// behind the new minimum) are still skipped via the OOB tolerance
// `app.Append` returns from PR #354 — the appender is NOT rolled
// back, the count is surfaced via
// `gorillas3_tsdb_oob_samples_dropped_total` and a warn log, and the
// agent stays up.

package gorillas3processor

import (
	"context"
	"errors"
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
	// NumOOBDropped counts samples that the Prometheus Head
	// rejected with `storage.ErrOutOfBounds` during append. mvp/issue46:
	// rather than aborting the whole flush, we skip OOB samples and
	// surface the count so operators see drift but the agent stays up.
	NumOOBDropped uint64
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

	dropped, err := b.appendWindow(ctx, bw, window)
	if err != nil {
		return nil, err
	}

	id, err := bw.Flush(ctx)
	if err != nil {
		return nil, fmt.Errorf("tsdb BlockWriter.Flush: %w", err)
	}
	if id == (ulid.ULID{}) {
		// No samples ended up appended (e.g. all empty buffers
		// or every sample tripped OOB). Surface the drop count via
		// a synthetic empty artifact so callers can still observe
		// the rejection without parsing logs.
		if dropped == 0 {
			return nil, nil
		}
		return &tsdbBlockArtifact{NumOOBDropped: dropped}, nil
	}

	blockDir := filepath.Join(tmpRoot, id.String())
	files, err := readBlockFiles(blockDir, id.String())
	if err != nil {
		return nil, err
	}
	mint, maxt := windowBounds(window)
	numSeries, numSamples := countSeriesSamples(window)
	// Subtract OOB-dropped samples from the count so the artifact's
	// NumSamples matches what's actually inside the block.
	if dropped > numSamples {
		numSamples = 0
	} else {
		numSamples -= dropped
	}
	return &tsdbBlockArtifact{
		ULID:          id,
		Files:         files,
		MinTime:       mint,
		MaxTime:       maxt,
		NumSeries:     numSeries,
		NumSamples:    numSamples,
		NumOOBDropped: dropped,
	}, nil
}

// appendWindow drives `BlockWriter.Appender` over every (series,
// point) pair. mvp/issue46 (rewritten under PR #355):
//
//   - Each series' points are sorted ascending in a local copy
//     (preserves per-series monotonicity which the Head requires).
//   - Series are visited in ascending order of each series' EARLIEST
//     timestamp. The series carrying the global-minimum sample is
//     visited first, so the very first `app.Append(...)` call seeds
//     the Head's appender `minValidTime = globalMin - chunkRange/2`.
//     Every other in-window sample is >= globalMin, so it's >= the
//     floor and passes the head's OOB check.
//   - This avoids both (a) the OOB-during-flush bug PR #354 was
//     written to fix and (b) PR #354's O(N_samples) flat-slice
//     allocation, which OOM-killed the agent at the first 60 s flush
//     under 5-sketch routing (6 gorillas3 instances × 1.2 M samples
//     each). See the `mvp/issue46` doc-block at top-of-file for the
//     full reasoning.
//   - `storage.ErrOutOfBounds` from `app.Append` is still NOT treated
//     as fatal. The sample is skipped, the counter bumps, and the
//     appender keeps going. This guards against genuinely late-
//     arriving points (e.g. a replay > chunkRange/2 behind globalMin)
//     so the agent stays up even if upstream sends a stale tail.
//
// Returns the number of samples dropped due to OOB (zero in the
// common case where every series' points fall within
// [globalMin, globalMin + windowSpan] and chunkRange/2 covers the
// whole window).
func (b *tsdbBlockBuilder) appendWindow(ctx context.Context, bw *tsdb.BlockWriter, window map[seriesKey]*seriesBuffer) (uint64, error) {
	// orderedSeries pairs a series key with its non-empty sorted
	// points and the materialised labels.Labels — sorting by
	// `firstTs` once gives us the visit order that anchors
	// `appender.minValidTime` at the global-minimum sample.
	type orderedSeries struct {
		firstTs int64 // milliseconds since Unix epoch (smallest in series)
		pts     []point
		ls      labels.Labels
	}

	// Pre-sort each series' points and capture each series' first
	// (smallest) timestamp. We avoid an O(N_samples) flat slice —
	// `series` is bounded by the cardinality of the window, not the
	// total sample count.
	series := make([]orderedSeries, 0, len(window))
	for sk, buf := range window {
		if buf == nil || len(buf.points) == 0 {
			continue
		}
		ls := b.labelsFor(sk.metricName, buf.attributes)
		// Defensive: tsdb rejects empty label sets; the metric
		// name always provides `__name__` so this never trips,
		// but guard anyway.
		if ls.Len() == 0 {
			continue
		}
		// Sort a local copy so we don't mutate the caller's
		// buffer order (the ASAP path may run on the same
		// snapshot when block_format=both).
		pts := make([]point, len(buf.points))
		copy(pts, buf.points)
		sort.Slice(pts, func(i, j int) bool { return pts[i].ts < pts[j].ts })
		series = append(series, orderedSeries{
			firstTs: pts[0].ts / int64(time.Millisecond),
			pts:     pts,
			ls:      ls,
		})
	}

	// Visit series in ascending order of first-sample ts. The very
	// first `app.Append` will therefore carry the global-minimum
	// timestamp, anchoring the appender's minValidTime at
	// `globalMin - chunkRange/2` so every other in-window sample
	// passes the OOB check.
	sort.Slice(series, func(i, j int) bool { return series[i].firstTs < series[j].firstTs })

	app := bw.Appender(ctx)
	var dropped uint64
	for _, s := range series {
		var ref storage.SeriesRef
		for _, p := range s.pts {
			// tsdb timestamps are milliseconds since Unix epoch.
			tms := p.ts / int64(time.Millisecond)
			r, err := app.Append(ref, s.ls, tms, p.v)
			if err != nil {
				if errors.Is(err, storage.ErrOutOfBounds) {
					// Sample is older than the
					// Head's appendable floor —
					// almost always a late-arriving
					// point from a previous window.
					// Drop it, count it, keep going.
					// The Head remains usable after
					// this return; the appender's
					// transaction is not rolled back.
					dropped++
					continue
				}
				_ = app.Rollback()
				return dropped, fmt.Errorf("tsdb appender.Append: %w", err)
			}
			ref = r
		}
	}
	if err := app.Commit(); err != nil {
		return dropped, fmt.Errorf("tsdb appender.Commit: %w", err)
	}
	return dropped, nil
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
