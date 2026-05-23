package gorilla

import (
	"encoding/binary"
	"fmt"
	"sort"
)

// Wire contract between the edge (asap_edge cold tier, producer) and the
// backend gorilla-merger (consumer). A compact binary frame — no JSON, no
// base64 — carrying a batch of XOR-chunk fragments. Labels are emitted in
// sorted name order so the frame is deterministic. Only the fields the merger
// needs to append + attribute a series are carried; transient edge bookkeeping
// (OOODropCount/FragmentULID/WatermarkTime) is intentionally dropped.
//
// Frame layout:
//
//	magic "ASAPFRG1" (8B) | version u8 | fragment_count uvarint
//	repeat fragment_count:
//	  metric_name   : uvarint len + bytes
//	  label_count   : uvarint
//	  repeat: name (uvarint len + bytes), value (uvarint len + bytes)  # sorted by name
//	  min_time_ms   : zigzag varint
//	  max_time_ms   : zigzag varint
//	  sample_count  : uvarint
//	  encoding      : u8 (0 = XOR / chunkenc.EncXOR)
//	  source        : uvarint len + bytes
//	  chunk_len     : uvarint
//	  chunk_bytes   : [chunk_len]   # the raw chunkenc XOR chunk
const (
	fragmentBatchMagic   = "ASAPFRG1"
	fragmentBatchVersion = byte(1)
	fragmentEncodingXOR  = byte(0)
)

// EncodeFragmentBatch serializes fragments into the ASAPFRG1 wire frame.
func EncodeFragmentBatch(frags []Fragment) []byte {
	size := len(fragmentBatchMagic) + 1 + binary.MaxVarintLen64
	for i := range frags {
		size += 64 + len(frags[i].MetricName) + len(frags[i].Source) + len(frags[i].Data)
		for k, v := range frags[i].Attributes {
			size += len(k) + len(v) + 2*binary.MaxVarintLen64
		}
	}
	buf := make([]byte, 0, size)
	buf = append(buf, fragmentBatchMagic...)
	buf = append(buf, fragmentBatchVersion)
	buf = binary.AppendUvarint(buf, uint64(len(frags)))
	for i := range frags {
		f := &frags[i]
		buf = appendCodecStr(buf, f.MetricName)
		keys := make([]string, 0, len(f.Attributes))
		for k := range f.Attributes {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		buf = binary.AppendUvarint(buf, uint64(len(keys)))
		for _, k := range keys {
			buf = appendCodecStr(buf, k)
			buf = appendCodecStr(buf, f.Attributes[k])
		}
		buf = binary.AppendVarint(buf, f.MinTime)
		buf = binary.AppendVarint(buf, f.MaxTime)
		buf = binary.AppendUvarint(buf, uint64(f.Count))
		buf = append(buf, encodingByte(f.Encoding))
		buf = appendCodecStr(buf, f.Source)
		buf = binary.AppendUvarint(buf, uint64(len(f.Data)))
		buf = append(buf, f.Data...)
	}
	return buf
}

// DecodeFragmentBatch parses an ASAPFRG1 frame produced by EncodeFragmentBatch.
// Returned Fragment.Data is copied, so it does not alias the input buffer.
func DecodeFragmentBatch(b []byte) ([]Fragment, error) {
	d := &codecReader{buf: b}
	magic, err := d.take(len(fragmentBatchMagic))
	if err != nil {
		return nil, fmt.Errorf("fragment batch: magic: %w", err)
	}
	if string(magic) != fragmentBatchMagic {
		return nil, fmt.Errorf("fragment batch: bad magic %q", magic)
	}
	ver, err := d.readByte()
	if err != nil {
		return nil, fmt.Errorf("fragment batch: version: %w", err)
	}
	if ver != fragmentBatchVersion {
		return nil, fmt.Errorf("fragment batch: unsupported version %d", ver)
	}
	n, err := d.uvarint()
	if err != nil {
		return nil, fmt.Errorf("fragment batch: count: %w", err)
	}
	frags := make([]Fragment, 0, n)
	for i := uint64(0); i < n; i++ {
		var f Fragment
		if f.MetricName, err = d.str(); err != nil {
			return nil, fmt.Errorf("fragment %d: metric: %w", i, err)
		}
		nl, err := d.uvarint()
		if err != nil {
			return nil, fmt.Errorf("fragment %d: label count: %w", i, err)
		}
		if nl > 0 {
			f.Attributes = make(map[string]string, nl)
			for j := uint64(0); j < nl; j++ {
				k, kerr := d.str()
				if kerr != nil {
					return nil, fmt.Errorf("fragment %d: label %d name: %w", i, j, kerr)
				}
				v, verr := d.str()
				if verr != nil {
					return nil, fmt.Errorf("fragment %d: label %d value: %w", i, j, verr)
				}
				f.Attributes[k] = v
			}
		}
		if f.MinTime, err = d.varint(); err != nil {
			return nil, fmt.Errorf("fragment %d: min_time: %w", i, err)
		}
		if f.MaxTime, err = d.varint(); err != nil {
			return nil, fmt.Errorf("fragment %d: max_time: %w", i, err)
		}
		cnt, err := d.uvarint()
		if err != nil {
			return nil, fmt.Errorf("fragment %d: count: %w", i, err)
		}
		f.Count = int(cnt)
		encB, err := d.readByte()
		if err != nil {
			return nil, fmt.Errorf("fragment %d: encoding: %w", i, err)
		}
		if f.Encoding, err = encodingString(encB); err != nil {
			return nil, fmt.Errorf("fragment %d: %w", i, err)
		}
		if f.Source, err = d.str(); err != nil {
			return nil, fmt.Errorf("fragment %d: source: %w", i, err)
		}
		dl, err := d.uvarint()
		if err != nil {
			return nil, fmt.Errorf("fragment %d: data len: %w", i, err)
		}
		data, err := d.take(int(dl))
		if err != nil {
			return nil, fmt.Errorf("fragment %d: data: %w", i, err)
		}
		f.Data = append([]byte(nil), data...)
		frags = append(frags, f)
	}
	return frags, nil
}

func appendCodecStr(dst []byte, s string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}

func encodingByte(enc string) byte {
	// Only XOR is defined today; the empty string is the Fragment default
	// (MarshalFragment also defaults "" -> "xor"), so it maps to XOR too.
	return fragmentEncodingXOR
}

func encodingString(b byte) (string, error) {
	if b == fragmentEncodingXOR {
		return "xor", nil
	}
	return "", fmt.Errorf("unknown encoding byte %d", b)
}

// codecReader is a bounds-checked cursor over the frame bytes.
type codecReader struct {
	buf []byte
	pos int
}

func (d *codecReader) take(n int) ([]byte, error) {
	if n < 0 || d.pos+n > len(d.buf) {
		return nil, fmt.Errorf("unexpected end of buffer (need %d, have %d)", n, len(d.buf)-d.pos)
	}
	out := d.buf[d.pos : d.pos+n]
	d.pos += n
	return out, nil
}

func (d *codecReader) readByte() (byte, error) {
	if d.pos >= len(d.buf) {
		return 0, fmt.Errorf("unexpected end of buffer")
	}
	b := d.buf[d.pos]
	d.pos++
	return b, nil
}

func (d *codecReader) uvarint() (uint64, error) {
	v, n := binary.Uvarint(d.buf[d.pos:])
	if n <= 0 {
		return 0, fmt.Errorf("invalid uvarint")
	}
	d.pos += n
	return v, nil
}

func (d *codecReader) varint() (int64, error) {
	v, n := binary.Varint(d.buf[d.pos:])
	if n <= 0 {
		return 0, fmt.Errorf("invalid varint")
	}
	d.pos += n
	return v, nil
}

func (d *codecReader) str() (string, error) {
	l, err := d.uvarint()
	if err != nil {
		return "", err
	}
	b, err := d.take(int(l))
	if err != nil {
		return "", err
	}
	return string(b), nil
}
