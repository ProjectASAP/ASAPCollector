package sketchutil

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"math"

	"github.com/cespare/xxhash/v2"
)

type CountMinSketch struct {
	rows   int
	cols   int
	total  float64
	table  []float32
	salts  []uint64
	hashes []hash.Hash64
	topk   *TopKHeap
}

func NewCountMinSketch(rows, cols int, seed uint64, topk int) (*CountMinSketch, error) {
	if rows <= 0 || cols <= 0 {
		return nil, fmt.Errorf("countmin: invalid dimensions rows=%d cols=%d", rows, cols)
	}
	cms := &CountMinSketch{
		rows:   rows,
		cols:   cols,
		table:  make([]float32, rows*cols),
		salts:  make([]uint64, rows),
		hashes: make([]hash.Hash64, rows),
		topk:   NewTopKHeap(topk),
	}
	for i := 0; i < rows; i++ {
		cms.salts[i] = mixSeed(seed, uint64(i))
		cms.hashes[i] = xxhash.New()
	}
	return cms, nil
}

func (c *CountMinSketch) Insert(key string, weight float64) {
	if weight == 0 {
		return
	}
	estimate := math.MaxFloat64
	for row := 0; row < c.rows; row++ {
		h := c.hashes[row]
		h.Reset()
		var saltBuf [8]byte
		binary.LittleEndian.PutUint64(saltBuf[:], c.salts[row])
		h.Write(saltBuf[:])
		io.WriteString(h, key)
		idx := int(h.Sum64() % uint64(c.cols))
		c.table[row*c.cols+idx] += float32(weight)
		value := float64(c.table[row*c.cols+idx])
		if value < estimate {
			estimate = value
		}
	}
	c.total += weight
	if estimate == math.MaxFloat64 {
		estimate = 0
	}
	if c.topk != nil {
		c.topk.Update(key, estimate)
	}
}

func (c *CountMinSketch) Rows() int {
	return c.rows
}

func (c *CountMinSketch) Columns() int {
	return c.cols
}

func (c *CountMinSketch) Total() float64 {
	return c.total
}

func (c *CountMinSketch) TopKEntries() []TopKEntry {
	if c.topk == nil {
		return nil
	}
	return c.topk.Entries()
}

func (c *CountMinSketch) MarshalBinary() ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := binary.Write(buf, binary.BigEndian, uint32(countMinMagic)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, uint16(countMinVersion)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, uint16(c.rows)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(c.cols)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, c.total); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, uint16(len(c.salts))); err != nil {
		return nil, err
	}
	for _, salt := range c.salts {
		if err := binary.Write(buf, binary.BigEndian, salt); err != nil {
			return nil, err
		}
	}
	for _, v := range c.table {
		if err := binary.Write(buf, binary.BigEndian, v); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

const (
	countMinMagic   = 0x434d5331 // "CMS1"
	countMinVersion = 1
)

func mixSeed(base uint64, row uint64) uint64 {
	const prime uint64 = 0x100000001b3
	value := base ^ (row * prime)
	value ^= value >> 33
	value *= 0xff51afd7ed558ccd
	value ^= value >> 33
	value *= 0xc4ceb9fe1a85ec53
	value ^= value >> 33
	return value
}
