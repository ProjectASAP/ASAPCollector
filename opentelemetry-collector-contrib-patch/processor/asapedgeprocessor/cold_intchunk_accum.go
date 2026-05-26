// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"fmt"
	"sort"

	gorilla "github.com/ProjectASAP/asap-gorilla-go"
	"github.com/ProjectASAP/asap-gorilla-go/coldpart"
	"github.com/prometheus/prometheus/model/labels"
)

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
