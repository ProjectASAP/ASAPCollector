// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package sketches

import (
	"bufio"
	"encoding/json"
	"math"
	"os"
	"sort"
	"testing"
)

// TestRealDataSketchAccuracy is the "agent-accuracy" check (Phase 4): it
// replays the REAL google_cluster mapped rows through the SAME sketch
// wrappers the fused asap_edge agent uses (sketchlib-go) and asserts each
// family's estimate sits within its accuracy envelope vs exact ground truth.
//
// It reads the JSONL produced by datasets_eval/google_cluster (run.py map);
// point GCT_JSONL at it. Skips (not fails) when the dataset isn't present,
// so it is safe in CI without the ~MB sample:
//
//	GCT_JSONL=/tmp/gct-otlp.jsonl go test ./sketches/ -run TestRealDataSketchAccuracy -v
type gctRow struct {
	Metric     string            `json:"metric"`
	Value      float64           `json:"value"`
	Attributes map[string]string `json:"attributes"`
}

func quantileLinear(sorted []float64, q float64) float64 {
	n := len(sorted)
	if n == 0 {
		return math.NaN()
	}
	if n == 1 {
		return sorted[0]
	}
	pos := q * float64(n-1)
	lo := int(math.Floor(pos))
	hi := int(math.Ceil(pos))
	if lo == hi {
		return sorted[lo]
	}
	frac := pos - float64(lo)
	return sorted[lo]*(1-frac) + sorted[hi]*frac
}

func relErr(got, want float64) float64 {
	return math.Abs(got-want) / math.Max(math.Abs(want), 1e-12)
}

func TestRealDataSketchAccuracy(t *testing.T) {
	path := os.Getenv("GCT_JSONL")
	if path == "" {
		path = "/tmp/gct-otlp.jsonl"
	}
	f, err := os.Open(path)
	if err != nil {
		t.Skipf("real-data accuracy: %s not present (run `run.py map`); skipping", path)
	}
	defer f.Close()

	const cpuMetric = "google_cluster_2019_cpu_rate"
	var cpuPos []float64       // cpu_rate values > 0 (DDSketch domain)
	var cpuAll []float64       // all cpu_rate values (Sum domain)
	distinctSvc := map[string]struct{}{}

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var r gctRow
		if err := json.Unmarshal(line, &r); err != nil {
			t.Fatalf("bad JSONL line: %v", err)
		}
		if r.Metric != cpuMetric {
			continue
		}
		cpuAll = append(cpuAll, r.Value)
		if r.Value > 0 {
			cpuPos = append(cpuPos, r.Value)
		}
		if svc := r.Attributes["service"]; svc != "" {
			distinctSvc[svc] = struct{}{}
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(cpuPos) < 100 {
		t.Skipf("real-data accuracy: only %d positive cpu_rate samples; too few", len(cpuPos))
	}

	// --- DDSketch quantile accuracy (alpha = 0.01) ---
	dd := NewDDSketchWrapper(0.01)
	for _, v := range cpuPos {
		dd.Update(v)
	}
	sorted := append([]float64(nil), cpuPos...)
	sort.Float64s(sorted)
	for _, q := range []float64{0.50, 0.99} {
		want := quantileLinear(sorted, q)
		got := dd.Quantile(q)
		if e := relErr(got, want); e > 0.05 {
			t.Errorf("DDSketch p%.0f: got=%.6g want=%.6g rel_err=%.4f > 0.05", q*100, got, want, e)
		} else {
			t.Logf("DDSketch p%.0f rel_err=%.4f (got=%.6g want=%.6g)", q*100, e, got, want)
		}
	}

	// --- HLL distinct-service cardinality ---
	hll := NewHLLWrapper()
	for _, v := range cpuAll { // re-iterate rows is fine; cardinality is over services
		_ = v
	}
	// Feed each distinct service once is exact; to exercise the estimator we
	// feed every row's service (duplicates collapse in HLL).
	f2, _ := os.Open(path)
	defer f2.Close()
	sc2 := bufio.NewScanner(f2)
	sc2.Buffer(make([]byte, 1<<20), 1<<20)
	for sc2.Scan() {
		var r gctRow
		if json.Unmarshal(sc2.Bytes(), &r) != nil || r.Metric != cpuMetric {
			continue
		}
		if svc := r.Attributes["service"]; svc != "" {
			hll.UpdateBytes([]byte(svc))
		}
	}
	wantCard := float64(len(distinctSvc))
	gotCard := hll.EstimateCardinality()
	if e := relErr(gotCard, wantCard); e > 0.10 {
		t.Errorf("HLL distinct(service): got=%.1f want=%.0f rel_err=%.4f > 0.10", gotCard, wantCard, e)
	} else {
		t.Logf("HLL distinct(service) rel_err=%.4f (got=%.1f want=%.0f)", e, gotCard, wantCard)
	}

	// --- Sum (first-class aggregate) lossless ---
	sw := NewSumWrapper()
	var exactSum float64
	for _, v := range cpuAll {
		sw.Update(v)
		exactSum += v
	}
	if e := relErr(sw.Sum(), exactSum); e > 1e-9 {
		t.Errorf("Sum: got=%.6f want=%.6f rel_err=%.3g (must be lossless)", sw.Sum(), exactSum, e)
	} else {
		t.Logf("Sum lossless: %.6f over %d samples", sw.Sum(), sw.Count())
	}
}
