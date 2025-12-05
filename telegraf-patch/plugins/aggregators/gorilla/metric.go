package gorilla

import (
	"github.com/influxdata/telegraf"
)

// gorillaBlockMetric wraps an existing metric and exposes an additional binary field named "payload".
type gorillaBlockMetric struct {
	telegraf.Metric
	payload []byte
}

func newGorillaBlockMetric(base telegraf.Metric, payload []byte) telegraf.Metric {
	return &gorillaBlockMetric{
		Metric:  base,
		payload: payload,
	}
}

func (m *gorillaBlockMetric) Fields() map[string]interface{} {
	fields := m.Metric.Fields()
	if m.payload != nil {
		fields["payload"] = m.payload
	}
	return fields
}

func (m *gorillaBlockMetric) FieldList() []*telegraf.Field {
	base := m.Metric.FieldList()
	if m.payload == nil {
		return base
	}
	out := make([]*telegraf.Field, len(base)+1)
	copy(out, base)
	out[len(base)] = &telegraf.Field{Key: "payload", Value: m.payload}
	return out
}

func (m *gorillaBlockMetric) HasField(key string) bool {
	if key == "payload" {
		return m.payload != nil
	}
	return m.Metric.HasField(key)
}

func (m *gorillaBlockMetric) GetField(key string) (interface{}, bool) {
	if key == "payload" {
		if m.payload == nil {
			return nil, false
		}
		return m.payload, true
	}
	return m.Metric.GetField(key)
}

func (m *gorillaBlockMetric) AddField(key string, value interface{}) {
	if key == "payload" {
		if value == nil {
			m.payload = nil
			return
		}
		switch v := value.(type) {
		case []byte:
			m.payload = append([]byte(nil), v...)
			return
		case string:
			m.payload = []byte(v)
			return
		}
	}
	m.Metric.AddField(key, value)
}

func (m *gorillaBlockMetric) RemoveField(key string) {
	if key == "payload" {
		m.payload = nil
		return
	}
	m.Metric.RemoveField(key)
}

func (m *gorillaBlockMetric) Copy() telegraf.Metric {
	var payload []byte
	if m.payload != nil {
		payload = append([]byte(nil), m.payload...)
	}
	return &gorillaBlockMetric{
		Metric:  m.Metric.Copy(),
		payload: payload,
	}
}
