package telegraf

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/influxdata/telegraf"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// Decode converts a single telegraf.Metric into a host-neutral
// Observation. The mapping follows §4 of the Telegraf-integration
// design doc:
//
//   - metric.Name() → Observation.Metric
//   - metric.Tags() → Observation.Labels (sorted by key for stable
//     series keys, matching the OTel codec's contract)
//   - Observation.ResourceLabels is always nil because Telegraf is
//     flat — host / region / service hints normally arrive as ordinary
//     tags and so flow through Labels.
//   - metric.Time().UnixMilli() → Observation.TimestampMs
//   - metric.Fields()[cfg.ValueField] → Observation.Value.Float for
//     the scalar (KindFloat) path
//
// If the metric carries a pre-aggregated envelope on
// cfg.EnvelopeField (default `_asap_envelope_b64`), Decode takes the
// shortcut: base64-decode + JSON-unmarshal into a SketchEnvelope and
// emit a KindEnvelope observation. The scalar value field is ignored
// when this path is taken — upstream-aggregated envelopes carry their
// own typed metadata that supersedes any companion scalar.
//
// Decode returns an error if the metric lacks both the envelope field
// and the configured value field, or if the value field is present
// but not float64 / int64.
func Decode(m telegraf.Metric, cfg *AdapterConfig) (*precompute.Observation, error) {
	if m == nil {
		return nil, fmt.Errorf("telegraf.Decode: nil metric")
	}

	labels := tagsToKeyValues(m.Tags())
	tsMs := uint64(0)
	if t := m.Time(); !t.IsZero() {
		// UnixMilli returns a signed int64; clamp to zero on
		// pre-epoch timestamps so the unsigned conversion is well
		// defined (the runtime's window assignment treats t=0 as
		// "no usable wall-clock", which is the safe default).
		ms := t.UnixMilli()
		if ms > 0 {
			tsMs = uint64(ms)
		}
	}

	// Envelope shortcut — when the configured envelope field is
	// present and non-empty, decode it into a typed SketchEnvelope
	// and emit a KindEnvelope observation regardless of whether the
	// scalar value field is also set. This matches the design doc's
	// "pre-aggregated upstream sketches bypass the scalar path"
	// rule.
	if envField := cfg.envelopeField(); envField != "" {
		if raw, ok := m.GetField(envField); ok {
			env, err := decodeEnvelopeField(raw)
			if err != nil {
				return nil, fmt.Errorf("telegraf.Decode: envelope field %q: %w", envField, err)
			}
			return &precompute.Observation{
				TimestampMs:    tsMs,
				Metric:         m.Name(),
				ResourceLabels: nil,
				Labels:         labels,
				Value:          precompute.EnvelopeValue(env),
			}, nil
		}
	}

	// Scalar path — read the configured value field as float64.
	valField := cfg.valueField()
	rawVal, ok := m.GetField(valField)
	if !ok {
		return nil, fmt.Errorf("telegraf.Decode: metric %q missing value field %q", m.Name(), valField)
	}
	floatVal, err := fieldAsFloat(rawVal)
	if err != nil {
		return nil, fmt.Errorf("telegraf.Decode: metric %q value field %q: %w", m.Name(), valField, err)
	}
	return &precompute.Observation{
		TimestampMs:    tsMs,
		Metric:         m.Name(),
		ResourceLabels: nil,
		Labels:         labels,
		Value:          precompute.FloatValue(floatVal),
	}, nil
}

// decodeEnvelopeField turns the raw field value (as returned by
// telegraf.Metric.GetField) into a SketchEnvelope. The on-the-wire
// shape is base64(JSON(SketchEnvelope)). Telegraf's metric.New
// silently coerces []byte to string (see metric.convertField), so the
// field will round-trip as a string regardless of how it was first
// added — Decode accepts both.
func decodeEnvelopeField(raw interface{}) (*precompute.SketchEnvelope, error) {
	var b64 string
	switch v := raw.(type) {
	case string:
		b64 = v
	case []byte:
		b64 = string(v)
	default:
		return nil, fmt.Errorf("expected string or []byte, got %T", raw)
	}
	if b64 == "" {
		return nil, fmt.Errorf("empty envelope field")
	}
	jsonBytes, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, fmt.Errorf("base64 decode: %w", err)
	}
	env := &precompute.SketchEnvelope{}
	if err := json.Unmarshal(jsonBytes, env); err != nil {
		return nil, fmt.Errorf("json unmarshal: %w", err)
	}
	return env, nil
}

// fieldAsFloat converts a telegraf field value to float64. Telegraf's
// metric.convertField pins numeric fields to float64 / int64 / uint64
// (see metric/metric.go); we accept the common cases plus float32 /
// int / uint widths for robustness. Boolean / string / nil values are
// rejected with a typed error so the plugin can surface a clear
// config-mismatch message.
func fieldAsFloat(raw interface{}) (float64, error) {
	switch v := raw.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint:
		return float64(v), nil
	}
	return 0, fmt.Errorf("expected numeric, got %T", raw)
}

// tagsToKeyValues converts Telegraf's tag map into the host-neutral
// []KeyValue slice. Keys are sorted alphabetically so the SeriesKey
// helper produces a stable byte layout — matching the OTel codec's
// AttributesToKeyValues contract.
func tagsToKeyValues(tags map[string]string) []precompute.KeyValue {
	if len(tags) == 0 {
		return nil
	}
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]precompute.KeyValue, 0, len(keys))
	for _, k := range keys {
		out = append(out, precompute.KeyValue{Key: k, Value: tags[k]})
	}
	return out
}
