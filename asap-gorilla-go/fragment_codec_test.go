package gorilla

import (
	"bytes"
	"testing"
)

func TestFragmentBatchRoundTrip(t *testing.T) {
	in := []Fragment{
		{
			MetricName: "http_requests_total",
			Attributes: map[string]string{"zone": "us", "method": "GET"},
			MinTime:    1000,
			MaxTime:    2000,
			Count:      120,
			Encoding:   "xor",
			Source:     "agent-1",
			Data:       []byte{0x00, 0x78, 0xff, 0x12, 0x00},
		},
		{
			MetricName: "latency",
			Attributes: nil,
			MinTime:    -5,
			MaxTime:    5,
			Count:      1,
			Encoding:   "", // defaults to xor
			Source:     "",
			Data:       []byte{0xab},
		},
	}
	enc := EncodeFragmentBatch(in)
	out, err := DecodeFragmentBatch(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out) != len(in) {
		t.Fatalf("count: got %d want %d", len(out), len(in))
	}
	for i := range in {
		if out[i].MetricName != in[i].MetricName {
			t.Errorf("frag %d metric: got %q want %q", i, out[i].MetricName, in[i].MetricName)
		}
		if out[i].MinTime != in[i].MinTime || out[i].MaxTime != in[i].MaxTime {
			t.Errorf("frag %d time: got [%d,%d] want [%d,%d]", i, out[i].MinTime, out[i].MaxTime, in[i].MinTime, in[i].MaxTime)
		}
		if out[i].Count != in[i].Count {
			t.Errorf("frag %d count: got %d want %d", i, out[i].Count, in[i].Count)
		}
		if out[i].Encoding != "xor" {
			t.Errorf("frag %d encoding: got %q want xor", i, out[i].Encoding)
		}
		if out[i].Source != in[i].Source {
			t.Errorf("frag %d source: got %q want %q", i, out[i].Source, in[i].Source)
		}
		if !bytes.Equal(out[i].Data, in[i].Data) {
			t.Errorf("frag %d data: got %v want %v", i, out[i].Data, in[i].Data)
		}
		if len(out[i].Attributes) != len(in[i].Attributes) {
			t.Errorf("frag %d attr count: got %d want %d", i, len(out[i].Attributes), len(in[i].Attributes))
		}
		for k, v := range in[i].Attributes {
			if out[i].Attributes[k] != v {
				t.Errorf("frag %d attr %q: got %q want %q", i, k, out[i].Attributes[k], v)
			}
		}
	}
}

func TestFragmentBatchEmpty(t *testing.T) {
	out, err := DecodeFragmentBatch(EncodeFragmentBatch(nil))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("want 0 frags, got %d", len(out))
	}
}

func TestFragmentBatchDataDoesNotAliasInput(t *testing.T) {
	enc := EncodeFragmentBatch([]Fragment{{MetricName: "m", Count: 1, Data: []byte{1, 2, 3}}})
	out, err := DecodeFragmentBatch(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	// mutate the source buffer; the decoded Data must be unaffected.
	for i := range enc {
		enc[i] = 0xee
	}
	if !bytes.Equal(out[0].Data, []byte{1, 2, 3}) {
		t.Fatalf("decoded data aliased the input buffer: %v", out[0].Data)
	}
}

func TestFragmentBatchBadMagic(t *testing.T) {
	if _, err := DecodeFragmentBatch([]byte("XXXXXXXX\x01\x00")); err == nil {
		t.Fatal("expected bad-magic error")
	}
}

func TestFragmentBatchTruncated(t *testing.T) {
	enc := EncodeFragmentBatch([]Fragment{{
		MetricName: "m",
		Attributes: map[string]string{"a": "b"},
		Count:      1,
		Data:       []byte{1, 2, 3},
	}})
	for n := 0; n < len(enc); n++ {
		if _, err := DecodeFragmentBatch(enc[:n]); err == nil {
			t.Errorf("expected error decoding truncated buffer of len %d/%d", n, len(enc))
		}
	}
}
