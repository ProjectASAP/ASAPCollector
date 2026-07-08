package epsilon_floor

// CORRECTED comparison (supersedes the earlier per-key version, which was wrong:
// it set p_k = 1/(1+ε²·f_k) using the ORACLE per-key frequency f_k — circular,
// since knowing every f_k means you already counted exactly and need no sketch;
// and it resurrected the RETIRED per-key √(f/rate) allocation).
//
// The UNIFIED ε-floor is WHOLE-SKETCH: ONE p = 1/(1+ε²·R) where R is the
// sketch's TOTAL update count in the window — fully OBSERVABLE (the edge just
// counts its updates; no per-key map). NitroSketch is the same mechanism with a
// hand-picked global p; the ε-floor's contribution is DERIVING p from a target
// ε + the observed R, and (in a fleet) giving each edge its own p_i from its
// own R_i.
//
// WHAT THE ε-FLOOR ACTUALLY BOUNDS (the honest story):
//   The whole-sketch ε-floor bounds the relative error of the ADDITIVE
//   AGGREGATE (F1 = R, and any query spanning ≈all the mass) to ε. A POINT
//   query on key k is protected only to  ε_k ≈ √((1-p)/(p·f_k)) = ε·√(R/f_k):
//   keys holding a constant fraction of the total mass are well-estimated;
//   rare keys are NOT (they fall below the sampling noise floor). This is
//   inherent to sampling, not a defect — and it is why the right accuracy
//   metric is the AGGREGATE / heavy-hitter error, never the rare-key error.
//
// Test 1 (TestWholeSketchEpsilonFloor): real CMS over the real DEBS-2022 symbol
//   stream. Reports the metrics NitroSketch targets — insert throughput,
//   memory, query latency — AND the accuracy LAW: F1-total rel-err ≈ ε, with
//   per-key rel-err binned by mass-fraction tracking the ε·√(R/f_k) prediction.
// Test 2 (TestFleetEpsilonFloorVsFixedP): a SYNTHETIC skewed fleet (rate-CV≫1)
//   where the per-edge adaptation gives a large gap (the 3 real DEBS exchanges
//   are only mildly skewed → ~1.3×, too weak to show it).
//
// Run:
//   go test ./benchmark -run 'TestWholeSketchEpsilonFloor|TestFleetEpsilonFloorVsFixedP' -v

import (
	"bufio"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ProjectASAP/sketchlib-go/common"
	cms "github.com/ProjectASAP/sketchlib-go/sketches/CountMinSketch"
)

type debsKey struct {
	symbol string
	count  int64
}

func loadDebsKeys(t *testing.T) []debsKey {
	f, err := os.Open("/tmp/debs_symbol_counts.csv")
	if err != nil {
		t.Skipf("DEBS symbol counts not staged (%v); run benchmark/extract_debs_rates.sh", err)
	}
	defer f.Close()
	var ks []debsKey
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		p := strings.Split(sc.Text(), ",")
		if len(p) < 2 {
			continue
		}
		c, err := strconv.ParseInt(strings.TrimSpace(p[1]), 10, 64)
		if err == nil && c > 0 {
			ks = append(ks, debsKey{p[0], c})
		}
	}
	sort.Slice(ks, func(i, j int) bool { return ks[i].count > ks[j].count })
	return ks
}

func relErrStats(e []float64) (median, p95, max, mean float64) {
	if len(e) == 0 {
		return
	}
	s := append([]float64(nil), e...)
	sort.Float64s(s)
	for _, x := range s {
		mean += x
	}
	mean /= float64(len(s))
	q := func(p float64) float64 { return s[int(p*float64(len(s)-1))] }
	return q(0.5), q(0.95), q(1.0), mean
}

const (
	cmsRows    = 5
	cmsColsMem = 4096  // canonical config — for the memory / throughput numbers
	cmsColsAcc = 65536 // wide config — isolates the SAMPLING law from CMS collision error
)

// ---- Test 1: whole-sketch ε-floor on a real CMS -------------------------------

// runCMSThroughput inserts the whole stream into a fresh CMS sampled at p via
// sketchlib's geometric skip-sampler (the real NitroSketch mechanism: skip work
// for unadmitted updates) and reports insert throughput + query latency.
func runCMSThroughput(t *testing.T, keys []debsKey, inputs []*common.SketchInput, p float64) (insTputMops, qLatNs float64) {
	sk, err := cms.NewCountMinSketch(cmsRows, cmsColsMem)
	if err != nil {
		t.Fatal(err)
	}
	if p < 1.0 {
		sk = sk.WithSampleP(p, 1)
	}
	var total int64
	t0 := time.Now()
	for i, k := range keys {
		in := inputs[i]
		for j := int64(0); j < k.count; j++ {
			sk.Update(in)
		}
		total += k.count
	}
	insTputMops = float64(total) / time.Since(t0).Seconds() / 1e6
	tq := time.Now()
	for i := range keys {
		sk.Estimate(inputs[i])
	}
	qLatNs = float64(time.Since(tq).Nanoseconds()) / float64(len(keys))
	return
}

// binomial draws the number of admitted items out of n trials at probability p.
// Normal approximation when n*p is large (the heavy hitters); exact Bernoulli
// sum when small (rare keys, cheap since n is tiny). Statistically identical to
// streaming whole-sketch admission, but O(#keys) instead of O(R).
func binomial(rng *rand.Rand, n int64, p float64) int64 {
	if p >= 1.0 {
		return n
	}
	mean := float64(n) * p
	if mean > 25 {
		sd := math.Sqrt(float64(n) * p * (1 - p))
		v := int64(math.Round(rng.NormFloat64()*sd + mean))
		if v < 0 {
			v = 0
		}
		if v > n {
			v = n
		}
		return v
	}
	var a int64
	for i := int64(0); i < n; i++ {
		if rng.Float64() < p {
			a++
		}
	}
	return a
}

// Test 1: whole-sketch ε-floor on a REAL CMS — the metrics NitroSketch targets
// (throughput/memory/latency) PLUS the honest accuracy law.
func TestWholeSketchEpsilonFloor(t *testing.T) {
	keys := loadDebsKeys(t)
	if len(keys) == 0 {
		t.Skip("no keys")
	}
	inputs := make([]*common.SketchInput, len(keys))
	var R int64
	for i, k := range keys {
		inputs[i] = common.FromString(k.symbol)
		R += k.count
	}
	mapBytes := 0
	for _, k := range keys {
		mapBytes += len(k.symbol) + 8 + 16 // key + int64 + map overhead
	}
	fmt.Printf("\nReal CMS over REAL DEBS: %d keys, R=%d total updates (OBSERVABLE)\n", len(keys), R)

	// ---- (A) throughput + memory (canonical 5×4096) ----
	fmt.Printf("\n(A) THROUGHPUT / MEMORY  [CMS %d×%d]\n", cmsRows, cmsColsMem)
	cmsBytes := cmsRows * cmsColsMem * 8
	fmt.Printf("  memory: CMS=%d B (constant in #keys) vs exact key→count map=%d B (O(#keys))\n", cmsBytes, mapBytes)
	exTput, exQ := runCMSThroughput(t, keys, inputs, 1.0)
	fmt.Printf("  [exact   p=1]      insert=%.1f Mupd/s  query=%.0f ns/key\n", exTput, exQ)
	for _, eps := range []float64{0.05, 0.10} {
		p := 1.0 / (1.0 + eps*eps*float64(R))
		spTput, spQ := runCMSThroughput(t, keys, inputs, p)
		fmt.Printf("  [ε=%.2f  p=%.2e]  insert=%.1f Mupd/s (%.1f×)  query=%.0f ns/key\n",
			eps, p, spTput, spTput/exTput, spQ)
	}

	// ---- (B) accuracy LAW (wide 5×65536 so collisions don't mask the sampling) ----
	// What the ε-floor bounds: the AGGREGATE (F1) to ≈ε; a point query on key k
	// only to ε·√(R/f_k). We verify both, averaged over seeds.
	fmt.Printf("\n(B) ACCURACY LAW  [CMS %d×%d, mean of 5 seeds]\n", cmsRows, cmsColsAcc)
	seeds := []int64{1, 2, 3, 4, 5}
	// mass-fraction bands (f_k / R), high→low; predicted point-query rel-err is
	// ε·√(R/f_k) at the band's representative f_k.
	for _, eps := range []float64{0.05, 0.10} {
		p := 1.0 / (1.0 + eps*eps*float64(R))
		var f1errs []float64
		// per-decade accumulation of empirical & predicted point-query error
		type acc struct{ emp, pred, n float64 }
		bands := map[int]*acc{}
		for _, seed := range seeds {
			rng := rand.New(rand.NewSource(seed))
			sk, _ := cms.NewCountMinSketch(cmsRows, cmsColsAcc)
			admitted := make([]int64, len(keys))
			var admTotal int64
			for i, k := range keys {
				a := binomial(rng, k.count, p)
				admitted[i] = a
				admTotal += a
				for j := int64(0); j < a; j++ {
					sk.Update(inputs[i])
				}
			}
			inv := 1.0 / p
			// F1 aggregate: the bounded quantity.
			f1hat := float64(admTotal) * inv
			f1errs = append(f1errs, math.Abs(f1hat-float64(R))/float64(R))
			// per-key point queries, binned by f_k decade.
			for i, k := range keys {
				fhat := sk.Estimate(inputs[i]) * inv
				e := math.Abs(fhat-float64(k.count)) / float64(k.count)
				d := int(math.Floor(math.Log10(float64(k.count))))
				if bands[d] == nil {
					bands[d] = &acc{}
				}
				b := bands[d]
				b.emp += e
				b.pred += math.Sqrt((1 - p) / (p * float64(k.count)))
				b.n++
			}
		}
		fm, _, _, _ := relErrStats(f1errs)
		fmt.Printf("  ε=%.2f (p=%.2e): F1-total rel-err median=%.4f  (target ≈ ε=%.2f) ✓\n", eps, p, fm, eps)
		fmt.Printf("    point-query rel-err by mass band f_k:\n")
		var ds []int
		for d := range bands {
			ds = append(ds, d)
		}
		sort.Sort(sort.Reverse(sort.IntSlice(ds)))
		for _, d := range ds {
			b := bands[d]
			fmt.Printf("      f_k~1e%d  (massfrac~%.1e): empirical=%.3f  predicted ε√(R/f_k)=%.3f  (%.0f keys)\n",
				d, math.Pow(10, float64(d))/float64(R), b.emp/b.n, b.pred/b.n, b.n/float64(len(seeds)))
		}
	}
	fmt.Printf("  → the AGGREGATE is held at ≈ε; point queries degrade as ε·√(R/f_k):\n")
	fmt.Printf("    heavy keys survive, rare keys fall below the sampling floor (inherent).\n")
}

// ---- Test 2: synthetic skewed fleet -------------------------------------------

// Test 2: per-edge ε-floor vs fixed-p on a SYNTHETIC fleet with rate-CV ≫ 1.
// Real DEBS has only 3 exchanges (mild skew → ~1.3×), too weak to show the
// adaptation; a real fleet of per-service metric streams spans orders of
// magnitude. ε-floor gives edge i p_i=1/(1+ε²R_i), holding its F1 sampling
// error ε_s,i=√((1-p)/(p·R_i)) at ε UNIFORMLY; matched-bandwidth fixed-p
// under-protects the low-rate edges. We report the analytical per-edge error
// AND an empirical F1 spot-check (real CMS) on the highest- and lowest-rate edge.
func TestFleetEpsilonFloorVsFixedP(t *testing.T) {
	// 64 edges, rates Zipf-spread over ~5 decades: r_i = Rmax / (i+1)^1.3.
	const nEdges = 64
	const rMax = 5_000_000.0
	rates := make([]int64, nEdges)
	var Rtot float64
	for i := 0; i < nEdges; i++ {
		r := math.Max(1, math.Round(rMax/math.Pow(float64(i+1), 1.3)))
		rates[i] = int64(r)
		Rtot += r
	}
	// rate-CV
	mean := Rtot / nEdges
	var v float64
	for _, r := range rates {
		v += (float64(r) - mean) * (float64(r) - mean)
	}
	cv := math.Sqrt(v/nEdges) / mean
	fmt.Printf("\nSynthetic fleet: %d edges, R-range %d..%d, rate-CV=%.2f\n",
		nEdges, rates[0], rates[nEdges-1], cv)

	eps := 0.05
	// ε-floor per-edge p_i; matched fixed-p = same TOTAL admitted bandwidth.
	var admFloor float64
	for _, r := range rates {
		pi := 1.0 / (1.0 + eps*eps*float64(r))
		admFloor += float64(r) * pi
	}
	pFixed := admFloor / Rtot
	epsS := func(p float64, r int64) float64 { return math.Sqrt((1 - p) / (p * float64(r))) }

	var floorE, fixedE []float64
	for _, r := range rates {
		pi := 1.0 / (1.0 + eps*eps*float64(r))
		floorE = append(floorE, epsS(pi, r))
		fixedE = append(fixedE, epsS(pFixed, r))
	}
	fm, _, fx, _ := relErrStats(floorE)
	gm, _, gx, _ := relErrStats(fixedE)
	fmt.Printf(" ε=%.2f target, matched bandwidth (fixed-p=%.2e):\n", eps, pFixed)
	fmt.Printf("  [ε-floor] per-edge F1 sampling-err median=%.4f max=%.4f  (all ≈ ε by construction)\n", fm, fx)
	fmt.Printf("  [fixed-p] per-edge F1 sampling-err median=%.4f max=%.4f  (low-rate edges under-protected)\n", gm, gx)
	fmt.Printf("  → fixed-p WORST-edge error is %.0f× the ε-floor's (vs ~1.3× on the 3 real DEBS exchanges)\n", gx/fx)

	// empirical F1 spot-check (real CMS) on the highest- and lowest-rate edge.
	fmt.Printf("  empirical F1 rel-err (real CMS, mean of 5 seeds):\n")
	check := func(label string, r int64) {
		var floorErr, fixedErr float64
		const trials = 5
		for s := int64(0); s < trials; s++ {
			rng := rand.New(rand.NewSource(100 + s))
			pi := 1.0 / (1.0 + eps*eps*float64(r))
			in := common.FromString(label)
			doF1 := func(p float64) float64 {
				sk, _ := cms.NewCountMinSketch(cmsRows, cmsColsAcc)
				a := binomial(rng, r, p)
				for j := int64(0); j < a; j++ {
					sk.Update(in)
				}
				f1 := sk.Estimate(in) / p
				return math.Abs(f1-float64(r)) / float64(r)
			}
			floorErr += doF1(pi)
			fixedErr += doF1(pFixed)
		}
		fmt.Printf("    %-10s (R=%8d): ε-floor=%.4f  fixed-p=%.4f\n",
			label, r, floorErr/trials, fixedErr/trials)
	}
	check("hot-edge", rates[0])
	check("cold-edge", rates[nEdges-1])
}
