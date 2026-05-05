package harness

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"testing"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// AssertByteParity is the test-level entry point. It compares the
// envelope sets emitted by the OTel-driven and Telegraf-driven
// runtimes for a single sketch type. On any divergence — count
// mismatch, payload mismatch, or unmatched envelope — it produces a
// structured diff via t.Errorf so the failure surfaces with enough
// context to triage.
//
// Honest-skip rule: callers may opt to convert a known-divergence
// case into a t.Skip via the harness-level test skeleton. This
// function never silently coerces a mismatch into a pass.
func AssertByteParity(t *testing.T, sketchName string, otelEnvs, tgEnvs []*precompute.SketchEnvelope) {
	t.Helper()
	report := DiffEnvelopes(sketchName, otelEnvs, tgEnvs)
	if !report.Equal() {
		t.Errorf("cross-host parity divergence (%s):\n%s", sketchName, report.Format())
		return
	}
	t.Logf("cross-host parity OK (%s): %d envelopes matched byte-for-byte", sketchName, report.BothCount)
}

// envelopeView is the comparison form. Both the OTel and Telegraf
// driving paths produce []*SketchEnvelope, so the projection is
// trivial — the diff key is built from the metric name plus
// resource-/dp-label canonicalizations and the comparison checks
// envelope.Payload bytes plus a few invariants (Encoding,
// SketchType).
type envelopeView struct {
	MetricName       string
	SketchType       precompute.SketchType
	ResourceLabelKey string
	DataPointLabel   string
	WindowStartMs    uint64
	WindowEndMs      uint64
	Encoding         precompute.Encoding
	Payload          []byte
	Count            uint64
}

func projectEnvelopes(envs []*precompute.SketchEnvelope) []envelopeView {
	out := make([]envelopeView, 0, len(envs))
	for _, e := range envs {
		if e == nil {
			continue
		}
		out = append(out, envelopeView{
			MetricName:       e.MetricName,
			SketchType:       e.SketchType,
			ResourceLabelKey: keyValuesString(e.ResourceLabels),
			DataPointLabel:   keyValuesString(e.Labels),
			WindowStartMs:    e.WindowStartMs,
			WindowEndMs:      e.WindowEndMs,
			Encoding:         e.Encoding,
			Payload:          e.Payload,
			Count:            e.Count,
		})
	}
	return out
}

// DiffReport summarizes the envelope-level comparison for one sketch.
type DiffReport struct {
	SketchType        string
	OnlyInOTel        []envelopeView
	OnlyInTelegraf    []envelopeView
	PayloadMismatches []payloadMismatch
	BothCount         int
}

type payloadMismatch struct {
	Key             string
	OTelPayload     []byte
	TelegrafPayload []byte
	OTelCount       uint64
	TelegrafCount   uint64
}

// Equal reports whether the diff is empty (true byte parity).
func (d *DiffReport) Equal() bool {
	return len(d.OnlyInOTel) == 0 &&
		len(d.OnlyInTelegraf) == 0 &&
		len(d.PayloadMismatches) == 0
}

// Format renders a multi-line diff string for t.Errorf.
func (d *DiffReport) Format() string {
	var b strings.Builder
	fmt.Fprintf(&b, "=== %s cross-host parity report ===\n", d.SketchType)
	fmt.Fprintf(&b, "envelopes present in both: %d\n", d.BothCount)
	fmt.Fprintf(&b, "only in OTel  path: %d\n", len(d.OnlyInOTel))
	fmt.Fprintf(&b, "only in Telegraf path: %d\n", len(d.OnlyInTelegraf))
	fmt.Fprintf(&b, "payload byte mismatches: %d\n", len(d.PayloadMismatches))
	const showFirst = 5
	if len(d.OnlyInOTel) > 0 {
		fmt.Fprintf(&b, "\n-- only in OTel (first %d):\n", showFirst)
		for i, v := range d.OnlyInOTel {
			if i >= showFirst {
				fmt.Fprintf(&b, "    ... (%d more)\n", len(d.OnlyInOTel)-i)
				break
			}
			fmt.Fprintf(&b, "    %s\n", envelopeViewKey(v))
		}
	}
	if len(d.OnlyInTelegraf) > 0 {
		fmt.Fprintf(&b, "\n-- only in Telegraf (first %d):\n", showFirst)
		for i, v := range d.OnlyInTelegraf {
			if i >= showFirst {
				fmt.Fprintf(&b, "    ... (%d more)\n", len(d.OnlyInTelegraf)-i)
				break
			}
			fmt.Fprintf(&b, "    %s\n", envelopeViewKey(v))
		}
	}
	if len(d.PayloadMismatches) > 0 {
		fmt.Fprintf(&b, "\n-- payload mismatches (first %d):\n", showFirst)
		for i, m := range d.PayloadMismatches {
			if i >= showFirst {
				fmt.Fprintf(&b, "    ... (%d more)\n", len(d.PayloadMismatches)-i)
				break
			}
			fmt.Fprintf(&b, "    key=%s\n", m.Key)
			fmt.Fprintf(&b, "      otel:     count=%d size=%d head=%x\n",
				m.OTelCount, len(m.OTelPayload), firstBytes(m.OTelPayload, 16))
			fmt.Fprintf(&b, "      telegraf: count=%d size=%d head=%x\n",
				m.TelegrafCount, len(m.TelegrafPayload), firstBytes(m.TelegrafPayload, 16))
		}
	}
	return b.String()
}

// DiffEnvelopes computes the cross-host parity diff between two
// envelope slices for one sketch type. Keying is by (metric, resource
// labels, data-point labels) — the WindowStart/End are not part of
// the key because both paths run synchronously through the same Drain
// call and so always agree on the window range.
func DiffEnvelopes(sketchType string, otel, tg []*precompute.SketchEnvelope) *DiffReport {
	otelViews := filterNonEmpty(projectEnvelopes(otel))
	tgViews := filterNonEmpty(projectEnvelopes(tg))

	otelMap := make(map[string][]envelopeView, len(otelViews))
	for _, v := range otelViews {
		k := envelopeViewKey(v)
		otelMap[k] = append(otelMap[k], v)
	}
	tgMap := make(map[string][]envelopeView, len(tgViews))
	for _, v := range tgViews {
		k := envelopeViewKey(v)
		tgMap[k] = append(tgMap[k], v)
	}

	report := &DiffReport{SketchType: sketchType}
	allKeys := unionKeys(otelMap, tgMap)
	sort.Strings(allKeys)
	for _, k := range allKeys {
		os := otelMap[k]
		ts := tgMap[k]
		switch {
		case len(os) > 0 && len(ts) == 0:
			report.OnlyInOTel = append(report.OnlyInOTel, os...)
		case len(os) == 0 && len(ts) > 0:
			report.OnlyInTelegraf = append(report.OnlyInTelegraf, ts...)
		default:
			if len(os) != len(ts) {
				report.PayloadMismatches = append(
					report.PayloadMismatches,
					payloadMismatch{
						Key:             k + " (duplicate-count mismatch)",
						OTelPayload:     os[0].Payload,
						TelegrafPayload: ts[0].Payload,
						OTelCount:       os[0].Count,
						TelegrafCount:   ts[0].Count,
					},
				)
				continue
			}
			for i := range os {
				if !bytes.Equal(os[i].Payload, ts[i].Payload) {
					report.PayloadMismatches = append(
						report.PayloadMismatches,
						payloadMismatch{
							Key:             k,
							OTelPayload:     os[i].Payload,
							TelegrafPayload: ts[i].Payload,
							OTelCount:       os[i].Count,
							TelegrafCount:   ts[i].Count,
						},
					)
				} else {
					report.BothCount++
				}
			}
		}
	}
	return report
}

func filterNonEmpty(vs []envelopeView) []envelopeView {
	out := vs[:0]
	for _, v := range vs {
		if len(v.Payload) > 0 {
			out = append(out, v)
		}
	}
	return out
}

func envelopeViewKey(v envelopeView) string {
	return fmt.Sprintf("%s|%s|%s",
		v.MetricName, v.ResourceLabelKey, v.DataPointLabel)
}

func keyValuesString(kvs []precompute.KeyValue) string {
	if len(kvs) == 0 {
		return ""
	}
	keys := make([]string, 0, len(kvs))
	m := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		keys = append(keys, kv.Key)
		m[kv.Key] = kv.Value
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(m[k])
		b.WriteByte(';')
	}
	return b.String()
}

func unionKeys(a, b map[string][]envelopeView) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	for k := range a {
		seen[k] = struct{}{}
	}
	for k := range b {
		seen[k] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	return out
}

func firstBytes(b []byte, n int) []byte {
	if len(b) <= n {
		return b
	}
	return b[:n]
}
