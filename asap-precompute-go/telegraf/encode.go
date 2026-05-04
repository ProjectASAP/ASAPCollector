package telegraf

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/influxdata/telegraf"
	telegrafmetric "github.com/influxdata/telegraf/metric"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// Encode converts a slice of host-neutral SketchEnvelopes into a slice
// of telegraf.Metric ready for hand-off to a Telegraf accumulator.
//
// One emitted metric per envelope. Each emitted metric carries:
//   - Name: env.MetricName, falling back to cfg.OutputMetricName when
//     the envelope didn't stamp one.
//   - Tags: env.Labels merged with env.ResourceLabels. Telegraf is
//     flat, so resource attrs land in the same map as data-point
//     attrs. On key collision the data-point Label wins (last-write
//     semantics with Labels written second); the encode path does not
//     log here because the codec is logger-free, but the collision is
//     a config-problem signal the plugin layer can detect by
//     comparing pre/post Tags counts.
//   - Fields: a single field whose key is cfg.EnvelopeField (default
//     `_asap_envelope_b64`) and whose value is the base64-encoded
//     JSON representation of the SketchEnvelope. JSON survives any
//     downstream Telegraf serializer including line-protocol — base64
//     is the +33% size cost we accept in exchange for binary safety
//     across InfluxDB / Prometheus-RW outputs (see design doc §4
//     "Binary-friendly carrier").
//   - Time: env.WindowEndMs converted to time.Time when set, else
//     time.Now(). Choosing WindowEnd matches the OTel codec's encode
//     path which stamps dp.Timestamp() with the window's exclusive
//     upper bound.
//
// Encode returns an error if any envelope is nil or the JSON
// marshaling fails (which should be impossible for the well-typed
// SketchEnvelope struct).
func Encode(envelopes []*precompute.SketchEnvelope, cfg *AdapterConfig) ([]telegraf.Metric, error) {
	if len(envelopes) == 0 {
		return nil, nil
	}
	out := make([]telegraf.Metric, 0, len(envelopes))
	for i, env := range envelopes {
		if env == nil {
			return nil, fmt.Errorf("telegraf.Encode: envelope[%d] is nil", i)
		}
		m, err := encodeEnvelope(env, cfg)
		if err != nil {
			return nil, fmt.Errorf("telegraf.Encode: envelope[%d]: %w", i, err)
		}
		out = append(out, m)
	}
	return out, nil
}

// encodeEnvelope materializes one SketchEnvelope as a telegraf.Metric.
func encodeEnvelope(env *precompute.SketchEnvelope, cfg *AdapterConfig) (telegraf.Metric, error) {
	jsonBytes, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("json marshal: %w", err)
	}
	b64 := base64.StdEncoding.EncodeToString(jsonBytes)

	tags := buildEncodeTags(env)
	envelopeFieldKey := cfg.envelopeField()
	if envelopeFieldKey == "" {
		// An explicitly-empty EnvelopeField on encode is a config
		// error: there's no anonymous field name to fall back to,
		// and emitting a metric with no envelope field would be a
		// silent data loss.
		return nil, fmt.Errorf("encode requires non-empty EnvelopeField (set DefaultEnvelopeField or supply one)")
	}
	fields := map[string]interface{}{
		envelopeFieldKey: b64,
	}

	ts := time.Now()
	if env.WindowEndMs != 0 {
		ts = time.UnixMilli(int64(env.WindowEndMs)).UTC()
	} else if env.WindowStartMs != 0 {
		ts = time.UnixMilli(int64(env.WindowStartMs)).UTC()
	}

	name := env.MetricName
	if name == "" {
		name = cfg.outputMetricName()
	}
	return telegrafmetric.New(name, tags, fields, ts), nil
}

// buildEncodeTags merges ResourceLabels and Labels into a single tag
// map. Telegraf's flat data model has no resource scope, so resource
// attrs are flattened into ordinary tags. Labels are written last so
// data-point-level attrs win on key collision — matching the
// "last-write wins" semantics the codec spec calls for.
func buildEncodeTags(env *precompute.SketchEnvelope) map[string]string {
	if len(env.ResourceLabels) == 0 && len(env.Labels) == 0 {
		return nil
	}
	tags := make(map[string]string, len(env.ResourceLabels)+len(env.Labels))
	for _, kv := range env.ResourceLabels {
		tags[kv.Key] = kv.Value
	}
	for _, kv := range env.Labels {
		tags[kv.Key] = kv.Value
	}
	return tags
}
