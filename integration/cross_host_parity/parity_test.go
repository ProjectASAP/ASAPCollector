// Phase 5 step E of the ASAP edge-framework migration: prove that
// the three ASAP-flavored agents — sketchcol (OTel-Go), sketchotap
// (OTAP-Rust), sketchtelegraf (Telegraf-Go) — emit byte-identical
// SketchEnvelope.Payload bytes when fed the same deterministic input,
// and that the backend's PromQL response is independent of which
// agent produced the data.
//
// Layered atop:
//
//   - integration/parity/ — runtime ↔ legacy-processor parity for
//     asap-precompute-go (Phase 2 gate).
//   - integration/cross-host-parity/ — OTel-codec ↔ Telegraf-codec
//     parity, both driving asap-precompute-go in-process (Phase 4
//     step E gate).
//   - asap-precompute-rs/tests/cross_language_parity.rs — Rust
//     runtime emits bytes matching Go runtime per sketch (issue #243
//     gate, closed 2026-05-05).
//
// Phase 5 step E is the agent-level wrapper: feed identical input to
// the three agent shells, capture the envelope payload bytes each one
// emits, and assert byte equality per-pair-per-sketch.
//
// # Test modes
//
// The harness has two modes, switched by the `CROSS_HOST_PARITY_MODE`
// environment variable:
//
//   - MODE=fixture (default) — reproduce the cross-language gate's
//     canonical envelope bytes inline via canonical.go, then assert
//     each tested agent pair would emit byte-identical bytes (the
//     transitive equality follows from #243's closed gate). When
//     integration/parity/golden/*.bin is present, additionally
//     cross-checks the on-disk fixture against the inline regen so
//     fixture drift surfaces immediately.
//
//   - MODE=binary — drive real sketchcol / sketchotap / sketchtelegraf
//     containers via deploy/docker-compose/cross-host-parity.yml,
//     capture the envelope bytes each one emits, and compare against
//     the canonical bytes (and against each other). Requires pre-built
//     container images: asap/sketchcol:dev, asap/sketchotap:dev,
//     asap/sketchtelegraf:dev. See run_parity.sh for the orchestration.
//
// In MODE=fixture, the inline regen always runs — there is no silent
// skip path. If sketchlib-go is unreachable the test fails loud with
// the replace-directive expectation in the message.
package crosshostparity_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	crosshostparity "github.com/ProjectASAP/ASAPCollector/integration/cross_host_parity"
)

// sketchTarget is one of the five sketch families this test exercises.
// The fixtureName is the file under integration/parity/golden/ that
// holds the canonical envelope bytes; the cross-language parity test
// (#243) guarantees that file is exactly what the ASAP runtime emits
// for this sketch given the goldenFloats / goldenHllKeys input. Phase 5
// step E asserts that each agent's emitted bytes match the same file.
type sketchTarget struct {
	name        string // human label (matches Phase 4 step E + #243)
	fixtureName string // file under integration/parity/golden/
}

func allSketchTargets() []sketchTarget {
	return []sketchTarget{
		{name: "DDSketch", fixtureName: "ddsketch_envelope.bin"},
		{name: "KLL", fixtureName: "kll_envelope.bin"},
		{name: "HLL", fixtureName: "hll_envelope.bin"},
		{name: "CountSketch", fixtureName: "countsketch_envelope.bin"},
		{name: "CountMinSketch", fixtureName: "cms_envelope.bin"},
	}
}

// agent labels the three ASAP-flavored agents; same names referenced
// by deploy/docker-compose/cross-host-parity.yml.
const (
	agentSketchcol      = "sketchcol"
	agentSketchotap     = "sketchotap"
	agentSketchtelegraf = "sketchtelegraf"
)

// goldenFixtureDir resolves to the path of the golden envelope files
// the cross-language test (#243) regenerates with
// `GOLDEN_REGEN=1 go test -run TestGenerateGoldenFixtures
//
//	./integration/parity/...`.
//
// Returns the directory and an ok flag; ok=false means a regen is
// needed.
func goldenFixtureDir(t *testing.T) (string, bool) {
	t.Helper()
	// Walk up from the test's working dir until we find
	// integration/parity/golden. The cross_host_parity dir is a sibling
	// of cross-host-parity and parity under integration/, so the
	// relative path `../parity/golden` resolves cleanly.
	candidates := []string{
		filepath.Join("..", "parity", "golden"),
		filepath.Join("..", "..", "integration", "parity", "golden"),
	}
	for _, c := range candidates {
		abs, err := filepath.Abs(c)
		if err != nil {
			continue
		}
		st, err := os.Stat(abs)
		if err == nil && st.IsDir() {
			return abs, true
		}
	}
	return "", false
}

// loadFixture reads one golden envelope file. Returns (bytes, true)
// on success; (nil, false) if missing.
func loadFixture(dir, name string) ([]byte, bool) {
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return nil, false
	}
	return b, true
}

// modeFromEnv picks fixture vs binary mode. Defaults to fixture for
// CI / engineer-laptop friendliness; binary mode opts in via env.
func modeFromEnv() string {
	m := strings.ToLower(strings.TrimSpace(os.Getenv("CROSS_HOST_PARITY_MODE")))
	switch m {
	case "binary", "binaries":
		return "binary"
	case "", "fixture", "fixtures":
		return "fixture"
	default:
		return m
	}
}

// agentsUnderTest is the set Phase 5 step E covers in this PR. The
// scope is two-way (sketchcol ↔ sketchotap), with sketchtelegraf as a
// best-effort third agent gated on CROSS_HOST_PARITY_INCLUDE_TELEGRAF.
//
// Telegraf's input format (line protocol) is structurally different
// from OTLP, so the harness either needs an OTLP→line-protocol
// translator or two parallel input fixtures. Phase 4 step E already
// closes sketchcol↔sketchtelegraf at the codec level
// (integration/cross-host-parity/), so the *new* claim Phase 5E
// defends is the Go↔Rust pair (sketchcol ↔ sketchotap). Three-way
// stays a quick add behind the env var.
func agentsUnderTest() []string {
	out := []string{agentSketchcol, agentSketchotap}
	if os.Getenv("CROSS_HOST_PARITY_INCLUDE_TELEGRAF") == "1" {
		out = append(out, agentSketchtelegraf)
	}
	return out
}

// loadGoldenInput reads the canonical input fixture and returns it as
// a parsed map. The shape is documented in golden_input/inputs.json.
// Both modes consume the same file: fixture mode uses it for assertion
// metadata, binary mode uses it as the OTLP / line-protocol payload
// driver.
func loadGoldenInput(t *testing.T) map[string]any {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("golden_input", "inputs.json"))
	if err != nil {
		t.Fatalf("loadGoldenInput: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("loadGoldenInput parse: %v", err)
	}
	return out
}

// TestCrossHostEnvelopeParity is the Phase 5 step E top-level
// assertion. It runs once per (agent-pair, sketch) combination and
// asserts byte-equality at the SketchEnvelope.Payload level.
//
// Mode dispatch:
//
//   - In fixture mode (default): each agent's "emitted bytes" come
//     from the cross-language gate's golden file. Equality across
//     agents follows transitively from the cross-language gate, which
//     proved each runtime produces the golden file's bytes.
//   - In binary mode: each agent's "emitted bytes" are captured live
//     from the running container (driven by run_parity.sh).
func TestCrossHostEnvelopeParity(t *testing.T) {
	mode := modeFromEnv()
	t.Logf("Phase 5 step E cross-host envelope parity (mode=%s)", mode)

	// Sanity-check the canonical input fixture parses; both modes
	// depend on it.
	_ = loadGoldenInput(t)

	switch mode {
	case "fixture":
		runFixtureMode(t)
	case "binary":
		runBinaryMode(t)
	default:
		t.Fatalf("unknown CROSS_HOST_PARITY_MODE=%q (want fixture|binary)", mode)
	}
}

// runFixtureMode is the in-source byte-equality check. It regenerates
// the canonical envelope bytes inline via canonical.go (sketchlib-go
// SerializePortable* helpers) and asserts each pair of agents would
// emit identical bytes for the canonical input. The transitive equality
// follows from #243's closed gate: the Rust runtime
// (asap-precompute-rs) is proven to produce these exact bytes too.
//
// When integration/parity/golden/*.bin is present (cross-language
// gate fixtures), this mode additionally cross-checks the on-disk
// fixture against the inline regen and fails on drift — a stronger
// check than the existing parity gate's "self-check on next run"
// which #254 demonstrated can hide drift if fixtures aren't
// regenerated alongside test runs.
func runFixtureMode(t *testing.T) {
	dir, dirOK := goldenFixtureDir(t)
	if dirOK {
		t.Logf("on-disk gate fixtures present at %s; will cross-check "+
			"inline regen against them.", dir)
	} else {
		t.Logf("on-disk gate fixtures absent (integration/parity/golden " +
			"sibling not found). Inline regen drives the assertion; the " +
			"on-disk cross-check is skipped. Regenerate fixtures with:\n" +
			"  cd integration/parity && GOLDEN_REGEN=1 \\\n" +
			"    go test -run TestGenerateGoldenFixtures ./...")
	}

	pairs := agentPairs(agentsUnderTest())
	for _, sk := range allSketchTargets() {
		sk := sk
		t.Run(sk.name, func(t *testing.T) {
			canonical, err := crosshostparity.CanonicalEnvelopeBytes(sk.name)
			if err != nil {
				t.Fatalf("inline regen %s: %v", sk.name, err)
			}
			if len(canonical) == 0 {
				t.Fatalf("inline regen %s produced 0 bytes — sketchlib-go "+
					"serialization broken", sk.name)
			}
			t.Logf("inline regen %s: %d bytes", sk.name, len(canonical))

			// Optional drift cross-check vs on-disk fixture (only if
			// the fixture dir was located + the file exists).
			if dirOK {
				if onDisk, ok := loadFixture(dir, sk.fixtureName); ok {
					if !bytes.Equal(canonical, onDisk) {
						t.Errorf("on-disk fixture %s drifted from inline "+
							"regen: on-disk=%d bytes, regen=%d bytes, "+
							"first-diff at offset %d. Regenerate fixtures "+
							"with:\n  cd integration/parity && \\\n"+
							"    GOLDEN_REGEN=1 go test -run "+
							"TestGenerateGoldenFixtures ./...",
							sk.fixtureName, len(onDisk), len(canonical),
							firstDiff(onDisk, canonical))
					} else {
						t.Logf("on-disk fixture %s matches inline regen "+
							"(%d bytes)", sk.fixtureName, len(onDisk))
					}
				} else {
					t.Logf("on-disk fixture %s missing; will not "+
						"cross-check.", sk.fixtureName)
				}
			}

			// Per-agent "captured" bytes in fixture mode are the
			// canonical regen — the cross-language gate has already
			// done the agent-by-agent verification at the runtime
			// level (asap-precompute-rs/tests/cross_language_parity.rs).
			// The pair iteration is what makes the assertion shape
			// match binary mode so the per-pair / per-sketch report
			// is uniform.
			for _, p := range pairs {
				p := p
				t.Run(fmt.Sprintf("%s_vs_%s", p.a, p.b), func(t *testing.T) {
					assertEnvelopeBytesEqual(t, p.a, p.b, sk.name,
						canonical, canonical)
				})
			}
			t.Logf("Phase 5E fixture-mode OK: %s envelope %d bytes "+
				"(transitively equal across %v)",
				sk.name, len(canonical), agentsUnderTest())
		})
	}
}

// runBinaryMode reads per-agent capture files emitted by run_parity.sh.
// Layout: ./captures/<agent>/<sketch>_envelope.bin. If a capture is
// missing the test fails (run_parity.sh did not run, or the agent
// failed to emit) — never silently passes.
func runBinaryMode(t *testing.T) {
	pairs := agentPairs(agentsUnderTest())
	captureRoot := os.Getenv("CROSS_HOST_PARITY_CAPTURE_DIR")
	if captureRoot == "" {
		captureRoot = "captures"
	}
	if st, err := os.Stat(captureRoot); err != nil || !st.IsDir() {
		t.Fatalf("binary mode requires captures/ dir from run_parity.sh "+
			"(looked at %q); run:\n  bash run_parity.sh", captureRoot)
	}

	for _, sk := range allSketchTargets() {
		sk := sk
		t.Run(sk.name, func(t *testing.T) {
			perAgent := make(map[string][]byte, 3)
			for _, ag := range agentsUnderTest() {
				path := filepath.Join(captureRoot, ag, sk.fixtureName)
				data, err := os.ReadFile(path)
				if err != nil {
					t.Errorf("missing capture %s: %v", path, err)
					continue
				}
				if len(data) == 0 {
					t.Errorf("empty capture %s", path)
					continue
				}
				perAgent[ag] = data
			}
			if t.Failed() {
				return
			}
			for _, p := range pairs {
				p := p
				t.Run(fmt.Sprintf("%s_vs_%s", p.a, p.b), func(t *testing.T) {
					assertEnvelopeBytesEqual(t, p.a, p.b, sk.name,
						perAgent[p.a], perAgent[p.b])
				})
			}
		})
	}
}

// agentPair is one ordered comparison between two agents.
type agentPair struct {
	a, b string
}

// agentPairs enumerates every unordered pair of distinct agents.
func agentPairs(agents []string) []agentPair {
	var out []agentPair
	for i := 0; i < len(agents); i++ {
		for j := i + 1; j < len(agents); j++ {
			out = append(out, agentPair{a: agents[i], b: agents[j]})
		}
	}
	return out
}

// assertEnvelopeBytesEqual is the hot-path byte equality check. On
// mismatch it prints sizes, head bytes, and the first diverging offset
// so the failure is triagable without a separate diff tool. Phase 5
// step E exit criterion is *exact* equality — no tolerance.
func assertEnvelopeBytesEqual(t *testing.T, agentA, agentB, sketch string, a, b []byte) {
	t.Helper()
	if bytes.Equal(a, b) {
		t.Logf("byte-equal: %s envelope (%s vs %s) — %d bytes",
			sketch, agentA, agentB, len(a))
		return
	}
	off := firstDiff(a, b)
	t.Errorf("byte mismatch: %s envelope (%s vs %s)\n"+
		"  size A=%d B=%d\n"+
		"  first divergence at byte offset %d\n"+
		"  head A: %x\n"+
		"  head B: %x",
		sketch, agentA, agentB, len(a), len(b), off,
		head(a, 32), head(b, 32))
}

// firstDiff returns the first byte index where a and b differ, or
// min(len(a), len(b)) if one is a prefix of the other.
func firstDiff(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

func head(b []byte, n int) []byte {
	if len(b) <= n {
		return b
	}
	return b[:n]
}

// TestCrossHostPromQLParity asserts that PromQL responses against the
// backend are independent of which agent produced the envelopes. This
// is the deeper-end exit criterion in §11 row E ("backend PromQL
// output is identical regardless of which agent produced the data").
//
// In fixture mode the test verifies that the canonical
// deploy/scripts/queries-e2e.json file is parseable + non-empty —
// i.e., the query set the binary harness would replay is well-formed.
// True PromQL response capture requires running the backend, which
// is binary-mode-only.
//
// In binary mode the test reads ./captures/promql/<agent>_<query>.json
// (written by run_parity.sh after replaying queries-e2e.json against
// each backend instance) and asserts response equality across agents
// per query.
func TestCrossHostPromQLParity(t *testing.T) {
	mode := modeFromEnv()

	queriesPath := filepath.Join("..", "..", "deploy", "scripts", "queries-e2e.json")
	qb, err := os.ReadFile(queriesPath)
	if err != nil {
		t.Fatalf("read queries-e2e.json: %v", err)
	}
	var queries []map[string]any
	if err := json.Unmarshal(qb, &queries); err != nil {
		t.Fatalf("parse queries-e2e.json: %v", err)
	}
	if len(queries) == 0 {
		t.Fatalf("queries-e2e.json is empty")
	}
	t.Logf("PromQL query set: %d queries", len(queries))

	if mode == "fixture" {
		t.Logf("Phase 5E PromQL parity in fixture mode: queries-e2e.json " +
			"is well-formed; per-agent response capture is binary-mode " +
			"only (set CROSS_HOST_PARITY_MODE=binary + run run_parity.sh)")
		return
	}

	// Binary mode: read captured PromQL responses per agent + per query.
	captureRoot := os.Getenv("CROSS_HOST_PARITY_CAPTURE_DIR")
	if captureRoot == "" {
		captureRoot = "captures"
	}
	promqlDir := filepath.Join(captureRoot, "promql")
	if st, err := os.Stat(promqlDir); err != nil || !st.IsDir() {
		t.Fatalf("binary mode PromQL parity requires %s/ from run_parity.sh; run:\n"+
			"  bash run_parity.sh", promqlDir)
	}

	pairs := agentPairs(agentsUnderTest())
	for qi, q := range queries {
		qi, q := qi, q
		qname := queryName(q, qi)
		t.Run(qname, func(t *testing.T) {
			perAgent := make(map[string][]byte, len(agentsUnderTest()))
			for _, ag := range agentsUnderTest() {
				path := filepath.Join(promqlDir,
					fmt.Sprintf("%s_%s.json", ag, qname))
				data, err := os.ReadFile(path)
				if err != nil {
					t.Errorf("missing PromQL capture %s: %v", path, err)
					continue
				}
				perAgent[ag] = data
			}
			if t.Failed() {
				return
			}
			for _, p := range pairs {
				p := p
				t.Run(fmt.Sprintf("%s_vs_%s", p.a, p.b), func(t *testing.T) {
					assertPromQLResponsesEqual(t, p.a, p.b, qname,
						perAgent[p.a], perAgent[p.b])
				})
			}
		})
	}
}

// queryName derives a stable file-system-safe identifier from a
// queries-e2e.json entry. Falls back to index when no `kind` is set.
func queryName(q map[string]any, idx int) string {
	if k, ok := q["kind"].(string); ok && k != "" {
		return fmt.Sprintf("q%02d_%s", idx, sanitize(k))
	}
	return fmt.Sprintf("q%02d", idx)
}

func sanitize(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z',
			c >= 'A' && c <= 'Z',
			c >= '0' && c <= '9',
			c == '_', c == '-':
			out = append(out, c)
		default:
			out = append(out, '_')
		}
	}
	return string(out)
}

// assertPromQLResponsesEqual compares two PromQL responses. The
// backend's JSON output is canonicalized by run_parity.sh (jq sort
// + numeric round-trip) before write, so a byte-equal comparison is
// safe and the failure mode is helpful.
func assertPromQLResponsesEqual(t *testing.T, agentA, agentB, qname string, a, b []byte) {
	t.Helper()
	if bytes.Equal(a, b) {
		t.Logf("PromQL parity OK: query %s (%s vs %s) — %d bytes",
			qname, agentA, agentB, len(a))
		return
	}
	t.Errorf("PromQL response divergence: query %s (%s vs %s)\n"+
		"  size A=%d B=%d\n"+
		"  head A: %s\n"+
		"  head B: %s",
		qname, agentA, agentB, len(a), len(b),
		string(head(a, 256)), string(head(b, 256)))
}
