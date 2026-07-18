// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	"github.com/ProjectASAP/asap-precompute-go/sketches"
	"github.com/ProjectASAP/sketchlib-go/common"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

// relErr mirrors asap-precompute-go/sketches' own helper of the same name
// (different module, can't import it directly).
func relErr(got, want float64) float64 {
	return math.Abs(got-want) / math.Max(math.Abs(want), 1e-12)
}

// TestNitroSketchE2ESDKtoCollectorTradeoff is a corrected re-run of the
// asap-precompute-go/sketches "nitrosketch tradeoff" eval (see PR #544). That
// eval benchmarked CountSketchWrapper.WithSampleP — COLLECTOR-side coordinated
// sampling, where the collector still receives every occurrence over the
// wire and only saves sketch-compute, never serialization/transport cost.
// That is a real, different, production mechanism (see warm_sketch.go's
// NewCMSWrapper(...).WithSampleP(sampleP) / NewDDSketchWrapper(...).WithSampleP),
// but it is NOT what "NitroSketch-style sampling" means in this codebase.
//
// The actual NitroSketch-style mechanism (opentelemetry-go-patch/sdk/metric/
// internal/aggregate/rowsampledsketch.go's measure()) decides row admission
// AT THE SDK, BEFORE serialization: an occurrence that admits no row is
// "discarded here — it is never buffered, never serialized, never sent"
// (verbatim from that file's doc comment). This test exercises that real
// pipeline end to end using production code at every stage:
//
//  1. SDK-side admission: a *common.GeometricSampler (the exact type
//     rowSampledSketchValues.measure() uses) decides admittedRows per
//     occurrence.
//  2. Admitted occurrences only: build a pmetric.Metrics Gauge batch with the
//     3 reserved row-sampled attributes (matching RowSampledSketchDataPoints'
//     wire shape exactly — see internal/shared/otlp/otlpmetric/transform/
//     metricdata.go.tmpl), marshaled via the REAL OTLP protobuf marshaler
//     (pmetric.ProtoMarshaler, what the SDK's exporter actually calls).
//  3. Unmarshal via pmetric.ProtoUnmarshaler (what the collector's OTLP
//     receiver actually calls).
//  4. Feed the decoded batch through a REAL *asapEdgeProcessor's
//     ConsumeMetrics (the actual production ingest path — attribute
//     extraction, series routing, ApplyAdmittedOccurrence).
//
// A skipped occurrence costs ONLY the SDK-side admission decision (steps
// 2-4 never run for it) — matching the real deployment exactly, unlike
// PR #544's WithSampleP benchmark where every occurrence paid full collector
// processing regardless of admission.
//
// All admitted occurrences from one "pass" over the dataset are batched into
// ONE OTLP message (matching how a real SDK export cycle amortizes the fixed
// per-message overhead — resource/scope wrapping, protobuf framing — across
// many points; sending one message per occurrence would inflate cost
// unrealistically and is not how OTLP export works).
//
// Requires DEBS_CSV set explicitly (see PR #544's asap-precompute-go eval for
// the same convention — no hardcoded fallback path, so a plain `go test
// ./...` never silently costs real wall-clock time on a checkout that
// happens to have the trace):
//
//	DEBS_CSV=/mydata/ASAPCollector/datasets_eval/debs/data/debs2022-gc-trading-day-08-11-21.csv \
//	  go test ./... -run TestNitroSketchE2ESDKtoCollectorTradeoff -v -timeout 30m
func TestNitroSketchE2ESDKtoCollectorTradeoff(t *testing.T) {
	path := os.Getenv("DEBS_CSV")
	if path == "" {
		t.Skip("nitrosketch e2e tradeoff eval: DEBS_CSV not set; skipping (see file doc comment to run for real)")
	}
	f, err := os.Open(path)
	if err != nil {
		t.Skipf("nitrosketch e2e tradeoff eval: %s not present; skipping", path)
	}
	defer f.Close()

	rowCap := 2_000_000
	if v := os.Getenv("NITROSKETCH_TRADEOFF_ROWS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			rowCap = n
		}
	}

	ids := make([]string, 0, rowCap)
	truth := map[string]int{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	nData := 0
	for sc.Scan() && nData < rowCap {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "ID,") {
			continue
		}
		comma := strings.IndexByte(line, ',')
		if comma <= 0 {
			continue
		}
		id := line[:comma]
		ids = append(ids, id)
		truth[id]++
		nData++
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if nData < 10_000 {
		t.Skipf("nitrosketch e2e tradeoff eval: only %d data rows; too few", nData)
	}
	t.Logf("loaded %d rows, %d distinct IDs", nData, len(truth))

	type idFreq struct {
		id   string
		freq int
	}
	all := make([]idFreq, 0, len(truth))
	for id, c := range truth {
		all = append(all, idFreq{id, c})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].freq > all[j].freq })
	const topK = 20
	if len(all) < topK {
		t.Skipf("nitrosketch e2e tradeoff eval: only %d distinct IDs; need >= %d", len(all), topK)
	}
	targets := all[:topK]

	// rows=4: rows*ceil(log2(cols)) <= 64 up through cols=16384 (see
	// NewCountSketchWrapper's row-hash budget check; rows=5 would overflow
	// it at cols=8192/16384 — same constraint PR #544 hit).
	const rows = 4
	widths := []int{512, 1024, 2048, 4096, 8192, 16384}
	samplePs := []float64{1.0, 0.75, 0.5, 0.25, 0.125, 0.0625}
	const cpuProbeWidth = 2048
	const sdkSampleSeed = 0x5a3e06d // same constant sketchlib-go's countSketchSampleSeed uses

	type admittedOcc struct {
		id           string
		admittedRows uint64
		tsOffset     int
	}

	// sdkAdmit replicates rowSampledSketchValues.measure()'s per-row admission
	// loop with the SAME sampler type — this decision does NOT depend on
	// sketch width, so it (and the full wire pipeline it feeds) only needs to
	// run ONCE per sample_p, not once per (sample_p, width) combination.
	sdkAdmit := func(p float64) (sent []admittedOcc, sdkNs time.Duration) {
		sampler := common.NewGeometricSampler(p, sdkSampleSeed)
		start := time.Now()
		for i, id := range ids {
			sampler.BeginItem()
			var admittedRows uint64
			for r := 0; r < rows; r++ {
				if sampler.Admit() {
					admittedRows |= 1 << uint(r)
				}
			}
			if admittedRows == 0 {
				continue // R(x)=empty: discarded here, never serialized, never sent
			}
			sent = append(sent, admittedOcc{id: id, admittedRows: admittedRows, tsOffset: i})
		}
		return sent, time.Since(start)
	}

	newProc := func(cols int) (*asapEdgeProcessor, *sketchAggregator) {
		cfg := &Config{
			ShardCount:     1,
			WindowDuration: time.Hour,
			Metrics:        []MetricFamily{{Metric: "ticker_events", Family: FamilyCountSketch, Rows: rows, Cols: cols}},
			Cold:           ColdConfig{Enabled: false},
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("cfg.Validate: %v", err)
		}
		set := processor.Settings{TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()}}
		proc, err := newProcessor(cfg, set, &capMetrics{})
		if err != nil {
			t.Fatalf("newProcessor: %v", err)
		}
		t.Cleanup(func() { _ = proc.Shutdown(context.Background()) })
		sa := proc.shards[0].sketchAggs["ticker_events"]
		if sa == nil {
			t.Fatal("no sketch aggregator for ticker_events")
		}
		return proc, sa
	}

	// runFullPipeline drives SDK admit -> batch -> marshal -> unmarshal ->
	// ConsumeMetrics for real, at cols=cpuProbeWidth — this is the only
	// combination whose CPU cost is reported (width doesn't affect the
	// admission/wire/apply cost, only the resulting sketch's accuracy).
	runFullPipeline := func(p float64) (totalNs int64, sentCount int, sa *sketchAggregator) {
		proc, sketchAgg := newProc(cpuProbeWidth)
		sent, sdkNs := sdkAdmit(p)
		base := time.Unix(1700000000, 0)

		md := pmetric.NewMetrics()
		m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		m.SetName("ticker_events")
		g := m.SetEmptyGauge()
		for _, a := range sent {
			dp := g.DataPoints().AppendEmpty()
			dp.Attributes().PutStr("ticker", a.id)
			dp.Attributes().PutInt(rowSampledAdmittedRowsKey, int64(a.admittedRows))
			dp.Attributes().PutInt(rowSampledRowsKey, rows)
			dp.Attributes().PutDouble(rowSampledSamplePKey, p)
			dp.SetDoubleValue(1)
			dp.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(a.tsOffset) * time.Microsecond)))
		}

		// Serialize: the REAL OTLP protobuf marshaler (what the SDK's
		// exporter calls).
		marshalStart := time.Now()
		var marshaler pmetric.ProtoMarshaler
		buf, err := marshaler.MarshalMetrics(md)
		if err != nil {
			t.Fatalf("MarshalMetrics: %v", err)
		}
		marshalNs := time.Since(marshalStart)

		// Deserialize: the REAL OTLP protobuf unmarshaler (what the
		// collector's receiver calls).
		unmarshalStart := time.Now()
		var unmarshaler pmetric.ProtoUnmarshaler
		decoded, err := unmarshaler.UnmarshalMetrics(buf)
		if err != nil {
			t.Fatalf("UnmarshalMetrics: %v", err)
		}
		unmarshalNs := time.Since(unmarshalStart)

		// Feed the decoded batch through the REAL production ingest path.
		consumeStart := time.Now()
		if err := proc.ConsumeMetrics(context.Background(), decoded); err != nil {
			t.Fatalf("ConsumeMetrics: %v", err)
		}
		consumeNs := time.Since(consumeStart)

		if sketchAgg.lastObserveErr != nil {
			t.Fatalf("observe error: %v", sketchAgg.lastObserveErr)
		}

		t.Logf("  [stage timing] p=%.4f sent=%d: sdk=%v marshal=%v unmarshal=%v consume=%v",
			p, len(sent), sdkNs, marshalNs, unmarshalNs, consumeNs)

		total := sdkNs + marshalNs + unmarshalNs + consumeNs
		return total.Nanoseconds(), len(sent), sketchAgg
	}

	// accuracyAt replays an ALREADY-ADMITTED set (from sdkAdmit, same p)
	// directly through a fresh sketchAggregator.observe() at the given width
	// — still 100% real production apply-side code (observe ->
	// ApplyAdmittedOccurrence), just skipping the (already-verified-once-per-p
	// in runFullPipeline) marshal/unmarshal/ConsumeMetrics round-trip, since
	// that round-trip's cost and correctness don't vary with sketch width.
	accuracyAt := func(sent []admittedOcc, p float64, cols int) *sketchAggregator {
		start := time.Now()
		_, sa := newProc(cols)
		newProcNs := time.Since(start)
		base := uint64(time.Unix(1700000000, 0).UnixMilli())
		obsStart := time.Now()
		for _, a := range sent {
			am := map[string]string{"ticker": a.id}
			sa.observe(am, 1, base+uint64(a.tsOffset), true, a.admittedRows, p)
		}
		obsNs := time.Since(obsStart)
		if sa.lastObserveErr != nil {
			t.Fatalf("observe error: %v", sa.lastObserveErr)
		}
		t.Logf("  [accuracyAt] p=%.4f cols=%d: newProc=%v observe_loop=%v", p, cols, newProcNs, obsNs)
		return sa
	}

	meanRelErr := func(sa *sketchAggregator, cols int) float64 {
		envs := sa.pc.Drain()
		rebuilt, err := sketches.NewCountSketchWrapper(rows, cols)
		if err != nil {
			t.Fatalf("NewCountSketchWrapper(%d,%d): %v", rows, cols, err)
		}
		for _, env := range envs {
			if env.SketchType != precompute.SketchTypeCountSketch || len(env.Payload) == 0 {
				continue
			}
			if err := rebuilt.ApplyDelta(env.Payload); err != nil {
				t.Fatalf("ApplyDelta: %v", err)
			}
		}
		var sum float64
		for _, tg := range targets {
			key := []byte(precompute.AttributesKey([]precompute.KeyValue{{Key: "ticker", Value: tg.id}}, nil))
			got := rebuilt.EstimateCount(key)
			sum += relErr(got, float64(tg.freq))
		}
		return sum / float64(len(targets))
	}

	type cell struct {
		relErr float64
	}
	grid := make(map[float64]map[int]cell, len(samplePs))
	nsAtProbeWidth := make(map[float64]int64, len(samplePs))
	sentAtProbeWidth := make(map[float64]int, len(samplePs))
	for _, p := range samplePs {
		grid[p] = make(map[int]cell, len(widths))

		// The one real full-pipeline run for this p (SDK admit -> marshal ->
		// unmarshal -> ConsumeMetrics) also gives the admitted set, reused
		// below for the other widths' accuracy without re-running the wire
		// round-trip.
		totalNs, sentCount, saAtProbe := runFullPipeline(p)
		nsAtProbeWidth[p] = totalNs
		sentAtProbeWidth[p] = sentCount
		grid[p][cpuProbeWidth] = cell{relErr: meanRelErr(saAtProbe, cpuProbeWidth)}

		sent, _ := sdkAdmit(p) // same seed => same admitted set as runFullPipeline used
		for _, w := range widths {
			if w == cpuProbeWidth {
				continue
			}
			sa := accuracyAt(sent, p, w)
			grid[p][w] = cell{relErr: meanRelErr(sa, w)}
		}
	}

	baselineErr := grid[1.0][cpuProbeWidth].relErr
	baseNs := nsAtProbeWidth[1.0]

	var report strings.Builder
	fmt.Fprintf(&report, "# NitroSketch SDK-to-collector end-to-end tradeoff (corrected)\n\n")
	fmt.Fprintf(&report, "Real trace: DEBS 2022 Grand Challenge trading data, first %d rows, %d distinct tickers, "+
		"CountSketch rows=%d, point-query target = top-%d heaviest tickers by true frequency.\n\n", nData, len(truth), rows, topK)
	fmt.Fprintf(&report, "**Corrects PR #544**, which benchmarked `CountSketchWrapper.WithSampleP` — collector-side "+
		"coordinated sampling (the collector receives 100%% of the wire traffic; only sketch-compute is saved). "+
		"This eval instead drives the REAL NitroSketch-style path end to end: SDK-side admission "+
		"(`common.GeometricSampler`, the same type `rowSampledSketchValues.measure()` uses) decides row admission "+
		"BEFORE serialization; a fully-skipped occurrence is never serialized, sent, or processed by the collector "+
		"at all. Every stage below is real production code: the SDK-side sampler, `pmetric.ProtoMarshaler`/"+
		"`ProtoUnmarshaler` (the actual OTLP wire codec), and a real `*asapEdgeProcessor.ConsumeMetrics`.\n\n")
	fmt.Fprintf(&report, "Baseline (sample_p=1.0, width=%d): mean relative error = %.4f\n\n", cpuProbeWidth, baselineErr)

	fmt.Fprintf(&report, "## End-to-end CPU cost (rows=%d, width=%d — SDK admit + OTLP marshal/unmarshal + real ConsumeMetrics)\n\n", rows, cpuProbeWidth)
	fmt.Fprintf(&report, "| sample_p | occurrences sent (of %d) | send fraction | total ns | ns/occurrence (over ALL, incl. skipped) | vs p=1.0 |\n|---|---|---|---|---|---|\n", nData)
	for _, p := range samplePs {
		ns := nsAtProbeWidth[p]
		sentN := sentAtProbeWidth[p]
		nsPerOcc := float64(ns) / float64(nData)
		ratio := float64(ns) / float64(baseNs)
		fmt.Fprintf(&report, "| %.4f | %d | %.4f | %d | %.1f | %.3fx |\n",
			p, sentN, float64(sentN)/float64(nData), ns, nsPerOcc, ratio)
		t.Logf("sample_p=%.4f: sent %d/%d (%.2f%%), total %.2fms, %.1f ns/occurrence, %.3fx baseline",
			p, sentN, nData, 100*float64(sentN)/float64(nData), float64(ns)/1e6, nsPerOcc, ratio)
	}

	fmt.Fprintf(&report, "\n## Memory needed to hold accuracy constant\n\n")
	fmt.Fprintf(&report, "Minimum width (from {%v}) whose mean relative error <= the p=1.0/w=%d baseline (%.4f):\n\n",
		widths, cpuProbeWidth, baselineErr)
	fmt.Fprintf(&report, "| sample_p | min width for baseline accuracy | memory ratio vs w=%d | achieved rel_err |\n|---|---|---|---|\n", cpuProbeWidth)
	for _, p := range samplePs {
		neededW := -1
		neededErr := 0.0
		for _, w := range widths {
			c := grid[p][w]
			if c.relErr <= baselineErr {
				neededW = w
				neededErr = c.relErr
				break
			}
		}
		if neededW == -1 {
			fmt.Fprintf(&report, "| %.4f | not reached within swept range (up to %d) | - | %.4f at w=%d |\n",
				p, widths[len(widths)-1], grid[p][widths[len(widths)-1]].relErr, widths[len(widths)-1])
		} else {
			ratio := float64(neededW) / float64(cpuProbeWidth)
			fmt.Fprintf(&report, "| %.4f | %d | %.2fx | %.4f |\n", p, neededW, ratio, neededErr)
		}
	}

	fmt.Fprintf(&report, "\n## Full accuracy grid (mean relative error, top-%d heavy hitters)\n\n", topK)
	fmt.Fprintf(&report, "| sample_p \\ width |")
	for _, w := range widths {
		fmt.Fprintf(&report, " %d |", w)
	}
	fmt.Fprintf(&report, "\n|---|")
	for range widths {
		fmt.Fprintf(&report, "---|")
	}
	fmt.Fprintf(&report, "\n")
	for _, p := range samplePs {
		fmt.Fprintf(&report, "| %.4f |", p)
		for _, w := range widths {
			fmt.Fprintf(&report, " %.4f |", grid[p][w].relErr)
		}
		fmt.Fprintf(&report, "\n")
	}

	fmt.Fprintf(&report, "\n## Methodology and honest caveats\n\n")
	fmt.Fprintf(&report, "- **This isolates the SAMPLING mechanism's own effect, not \"row-sampled export vs "+
		"traditional periodic sketch-state export.\"** The RowSampledSketch wire model sends ONE message per "+
		"admitted occurrence (see the transform doc: \"a row-sampled point carries no sketch STATE at all\"), "+
		"which is architecturally a different (and at sample_p=1.0, COSTLIER per-window) wire model than a plain "+
		"CountSketch aggregation that accumulates in memory and exports one periodic full/delta sketch-state "+
		"message regardless of occurrence count. Comparing sample_p=1.0 vs sample_p<1.0 WITHIN the row-sampled "+
		"path (as this eval does) is the correct comparison for measuring what sampling itself buys you; comparing "+
		"the row-sampled path against the traditional per-window sketch export is a separate, larger question this "+
		"eval does not address.\n")
	fmt.Fprintf(&report, "- All admitted occurrences from one dataset pass are batched into ONE OTLP message, "+
		"matching how a real SDK export cycle amortizes fixed per-message overhead (resource/scope wrapping, "+
		"protobuf framing) across many points. A real deployment would flush periodically in smaller batches, not "+
		"one batch of the whole dataset — this changes how OFTEN the fixed overhead is paid, not the marginal "+
		"per-point cost this eval reports.\n")
	fmt.Fprintf(&report, "- The SDK-side sampler uses the SAME fixed constant seed sketchlib-go's own "+
		"`countSketchSampleSeed`/`rowSampledSketchValues.targetFor` convention uses in production (\"seeds its own "+
		"RNG from the supplied seed so producers are reproducible\") — deterministic across runs by design, not an "+
		"eval artifact. Whether sharing one seed across every series of a given policy/AggID could correlate "+
		"admission timing across series with similar arrival patterns has not been analyzed here; tracked "+
		"separately.\n")
	fmt.Fprintf(&report, "- The accuracy grid is not perfectly monotonic in width even at sample_p=1.0 (every "+
		"width sees the IDENTICAL 300k observations at p=1.0, since Admit() always returns true and draws no RNG) "+
		"— e.g. width=16384 shows slightly WORSE mean relative error than width=8192. This is genuine CountSketch "+
		"row-hash noise (the sketch's own internal hash function differs by width, so the SAME 20 heavy-hitter keys "+
		"can land in a marginally less favorable collision pattern at a larger width by chance) rather than a "+
		"sampling effect — read the coarse trend, not individual grid cells, same caveat as PR #544's own "+
		"single-seed note.\n")

	outPath := os.Getenv("NITROSKETCH_E2E_TRADEOFF_OUT")
	if outPath == "" {
		outPath = "/mydata/ASAPCollector/datasets_eval/multisketch/nitrosketch-e2e-sdk-tradeoff-RESULTS.md"
	}
	if err := os.WriteFile(outPath, []byte(report.String()), 0o644); err != nil {
		t.Logf("could not write report to %s: %v (report also in test log above)", outPath, err)
	} else {
		t.Logf("wrote report to %s", outPath)
	}
}
