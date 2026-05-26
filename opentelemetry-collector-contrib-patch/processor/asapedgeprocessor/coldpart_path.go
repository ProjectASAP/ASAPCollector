// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sort"
	"time"

	gorilla "github.com/ProjectASAP/asap-gorilla-go"
	"github.com/ProjectASAP/asap-gorilla-go/coldpart"
	"github.com/prometheus/prometheus/model/labels"
)

// coldPartShipper is the intchunk cold-part producer: the opt-in parallel to
// the gorilla-XOR fragmentShipper. It takes the SAME per-window drained
// fragments the fragment path would ship, decodes each fragment's XOR chunk
// back to raw samples, groups them by series, re-encodes each series losslessly
// with intchunk inside a coldpart.Part (WritePart), and POSTs the complete
// serialized part to the merger's POST /ingest/coldpart. The merger validates
// (OpenPart) and stores the bytes verbatim under a content-addressed key it
// derives itself, so the shipper sends only the bytes — no client-chosen name.
//
// Behavior parity: series labels are built with gorilla.FragmentLabels, the
// SAME mapping the fragment->TSDB finalizer uses, so a series cold-archived via
// either format is queried under one identity. An empty endpoint makes the
// shipper a no-op (the cold tier still drains fragments; they are just not
// shipped as a part).
type coldPartShipper struct {
	endpoint   string
	external   map[string]string
	client     *http.Client
	maxRetries int
	backoff    time.Duration
}

func newColdPartShipper(endpoint string, external map[string]string) *coldPartShipper {
	return &coldPartShipper{
		endpoint:   endpoint,
		external:   external,
		client:     &http.Client{Timeout: 30 * time.Second},
		maxRetries: 3,
		backoff:    time.Second,
	}
}

// noop reports whether cold-part shipping is disabled (nil shipper or empty
// endpoint => drain-only).
func (s *coldPartShipper) noop() bool { return s == nil || s.endpoint == "" }

// buildPart turns a drained fragment batch into a single serialized coldpart
// Part. It returns (nil, nil) when the batch carries no decodable samples (so
// the caller skips the POST). block_start_ms/block_end_ms bound the batch with
// ABSOLUTE-ms timestamps (the minimum and maximum sample timestamp across the
// batch); the merger's manifest overlap + per-series clipping rely on these,
// and the per-series intchunk timestamps are likewise absolute ms.
func (s *coldPartShipper) buildPart(frags []gorilla.Fragment) ([]byte, error) {
	series, blockStart, blockEnd, err := fragmentsToSeries(frags, s.external)
	if err != nil {
		return nil, err
	}
	if len(series) == 0 {
		return nil, nil
	}
	return encodePart(blockStart, blockEnd, series)
}

// encodePart serializes already-grouped series into one coldpart.Part with the
// given absolute-ms block bounds. Shared by the per-flush buildPart and the
// per-block accumulator so both produce a byte-identical wire format.
func encodePart(blockStart, blockEnd int64, series []coldpart.Series) ([]byte, error) {
	var buf bytes.Buffer
	if err := coldpart.WritePart(&buf, blockStart, blockEnd, series, coldpart.Options{}); err != nil {
		return nil, fmt.Errorf("coldpart: write part: %w", err)
	}
	return buf.Bytes(), nil
}

// ship builds + POSTs a cold part for one drained fragment batch. A no-op
// shipper, an empty batch, or a batch with no samples all return nil without a
// network call.
func (s *coldPartShipper) ship(ctx context.Context, frags []gorilla.Fragment) error {
	if s.noop() || len(frags) == 0 {
		return nil
	}
	body, err := s.buildPart(frags)
	if err != nil {
		return err
	}
	return s.shipEncoded(ctx, body)
}

// shipEncoded POSTs an already-serialized cold part to the merger's
// /ingest/coldpart, retrying with linear backoff. The body is the raw bytes
// coldpart.WritePart produced; the merger validates them (OpenPart) and derives
// the object key itself.
func (s *coldPartShipper) shipEncoded(ctx context.Context, body []byte) error {
	if s.noop() || len(body) == 0 {
		return nil
	}
	var lastErr error
	for attempt := 0; attempt < s.maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(s.backoff * time.Duration(attempt)):
			}
		}
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewReader(body))
		if reqErr != nil {
			return reqErr
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		resp, doErr := s.client.Do(req)
		if doErr != nil {
			lastErr = doErr
			continue
		}
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return nil
		}
		lastErr = fmt.Errorf("ship cold part: status %d", resp.StatusCode)
	}
	return lastErr
}

// coldPartAccumulator buffers the decoded samples of MANY drained fragment
// batches across a block_duration window, grouped by series label set, so the
// intchunk cold-part path can emit ONE part per block instead of one per flush.
//
// Each per-flush drain carries only ~1-2 samples/series (the gorilla encoder's
// bounded-OOO window emits XOR-chunk fragments well inside a window, and the
// cold path used to re-encode + POST each emission immediately). A coldpart.Part
// carries a per-series index + a deduped label symbol table, so at that sample
// density the fixed per-part/per-series overhead dwarfs the value chunk —
// measured ~3x LARGER than gorilla-XOR. Accumulating ~a full block's worth of
// samples/series before sealing a part amortizes that overhead away and flips
// the realized ratio to SMALLER than gorilla.
//
// It is NOT safe for concurrent use; the caller serializes append/seal (the
// processor calls these only from the single flush goroutine).
type coldPartAccumulator struct {
	external map[string]string

	byKey map[string]*coldSeriesAcc // label-set key -> merged samples
	order []string                  // first-seen order (stable)

	blockStart int64 // min sample T across the buffered window (absolute ms)
	blockEnd   int64 // max sample T across the buffered window (absolute ms)
	haveBounds bool
	samples    int // total buffered samples (cheap empty check)
}

type coldSeriesAcc struct {
	lbls    labels.Labels
	samples []coldpart.Sample
}

func newColdPartAccumulator(external map[string]string) *coldPartAccumulator {
	return &coldPartAccumulator{
		external: external,
		byKey:    make(map[string]*coldSeriesAcc),
	}
}

// empty reports whether nothing has been buffered yet.
func (a *coldPartAccumulator) empty() bool { return a.samples == 0 }

// spanMs is the absolute-ms time span currently buffered (max-min sample T), or
// 0 when empty. The processor compares this to BlockDuration to decide when to
// seal a part.
func (a *coldPartAccumulator) spanMs() int64 {
	if !a.haveBounds {
		return 0
	}
	return a.blockEnd - a.blockStart
}

// add decodes a drained fragment batch and merges its samples into the buffer
// (per-series), extending the absolute-ms block bounds. Multiple fragments /
// flushes for the same series accumulate into one run; ordering + dedup happen
// at seal time. Fragments with no decodable samples are skipped.
func (a *coldPartAccumulator) add(frags []gorilla.Fragment) error {
	for i := range frags {
		f := frags[i]
		samples, err := gorilla.DecodeFragmentSamples(f)
		if err != nil {
			return fmt.Errorf("coldpart: decode fragment %d: %w", i, err)
		}
		if len(samples) == 0 {
			continue
		}
		ls := gorilla.FragmentLabels(f.MetricName, f.Attributes, a.external)
		key := ls.String()
		sa := a.byKey[key]
		if sa == nil {
			sa = &coldSeriesAcc{lbls: ls}
			a.byKey[key] = sa
			a.order = append(a.order, key)
		}
		sa.samples = append(sa.samples, samples...)
		a.samples += len(samples)
		for _, sm := range samples {
			if !a.haveBounds {
				a.blockStart, a.blockEnd, a.haveBounds = sm.T, sm.T, true
				continue
			}
			if sm.T < a.blockStart {
				a.blockStart = sm.T
			}
			if sm.T > a.blockEnd {
				a.blockEnd = sm.T
			}
		}
	}
	return nil
}

// seal sorts each buffered series' merged samples and dedupes exact-timestamp
// duplicates (keeping the last value, matching the fragment encoder's
// monotonic-per-series contract), then returns the part-ready series plus the
// absolute-ms [blockStart,blockEnd] bounds and resets the buffer. It returns
// (nil,...) when nothing is buffered.
func (a *coldPartAccumulator) seal() ([]coldpart.Series, int64, int64) {
	if !a.haveBounds {
		a.reset()
		return nil, 0, 0
	}
	out := make([]coldpart.Series, 0, len(a.order))
	for _, key := range a.order {
		sa := a.byKey[key]
		// A series may span many fragments/flushes (separate XOR chunks); ensure
		// the merged run is time-ordered ascending, as coldpart requires, and drop
		// any exact-timestamp duplicate so intchunk's strictly-increasing-T
		// contract holds.
		sort.SliceStable(sa.samples, func(i, j int) bool { return sa.samples[i].T < sa.samples[j].T })
		sa.samples = dedupSamples(sa.samples)
		if len(sa.samples) == 0 {
			continue
		}
		out = append(out, coldpart.Series{Labels: sa.lbls, Samples: sa.samples})
	}
	blockStart, blockEnd := a.blockStart, a.blockEnd
	a.reset()
	return out, blockStart, blockEnd
}

// reset drops all buffered state so the accumulator can begin the next block.
func (a *coldPartAccumulator) reset() {
	a.byKey = make(map[string]*coldSeriesAcc)
	a.order = a.order[:0]
	a.blockStart, a.blockEnd, a.haveBounds = 0, 0, false
	a.samples = 0
}

// dedupSamples removes exact-timestamp duplicates from a timestamp-sorted run,
// keeping the LAST value at each timestamp (the freshest re-emission). The input
// must already be sorted ascending by T.
func dedupSamples(in []coldpart.Sample) []coldpart.Sample {
	if len(in) < 2 {
		return in
	}
	out := in[:1]
	for _, sm := range in[1:] {
		if sm.T == out[len(out)-1].T {
			out[len(out)-1] = sm // same T: keep the latest value
			continue
		}
		out = append(out, sm)
	}
	return out
}

// fragmentsToSeries decodes a drained fragment batch into coldpart.Series
// grouped by label set, plus the absolute-ms [blockStart,blockEnd] bounding all
// samples. Multiple fragments for the same series (distinct XOR chunks) are
// concatenated and the merged samples sorted by timestamp, so coldpart's
// time-ordered, ascending-timestamp contract holds. Fragments with no samples
// are skipped. It is the single-batch (per-flush) helper; the per-block path
// uses coldPartAccumulator, which shares the same decode/merge logic.
func fragmentsToSeries(frags []gorilla.Fragment, external map[string]string) ([]coldpart.Series, int64, int64, error) {
	acc := newColdPartAccumulator(external)
	if err := acc.add(frags); err != nil {
		return nil, 0, 0, err
	}
	series, blockStart, blockEnd := acc.seal()
	return series, blockStart, blockEnd, nil
}
