// implementation ported from PrecomputeEngine https://github.com/approx-telemetry/PrecomputeEngine/blob/main/aggregators/KLL/kll.go
package kll

import (
	"math/rand"
	"math"
	"unsafe"
	"sort"
)

// 64-bit xorshift multiply rng from http://vigna.di.unimi.it/ftp/papers/xorshift.pdf
func xorshiftMult64(x uint64) uint64 {
	x ^= x >> 12 // a
	x ^= x << 25 // b
	x ^= x >> 27 // c
	return x * 2685821657736338717
}

// coin is a simple struct to let us get random bools and make minimum calls
// to the random number generator.
type coin struct {
	st   uint64
	mask uint64
}

// v is either 0 or 1
func (c *coin) toss() (v int) {
	if c.mask == 0 {
		if c.st == 0 {
			c.st = uint64(rand.Int63())
		}
		c.st = xorshiftMult64(c.st)
		c.mask = 1
	}
	if c.st&c.mask > 0 {
		v = 1
	}
	c.mask <<= 1
	return v
}


// Sketch is a streaming quantiles sketch
type Sketch struct {
	Compactors []Compactor
	k          int
	H          int
	size       int
	maxSize    int

	co coin
}

// New returns a new Sketch.  k controls the maximum memory used by the stream, which is 3*k + lg(n).
func New(k int) *Sketch {
	s := Sketch{
		k: k,
	}
	s.grow()
	return &s
}

func (s *Sketch) GetSize() int {
	return s.size
}

func (s *Sketch) grow() {
	s.Compactors = append(s.Compactors, Compactor{})
	s.H = len(s.Compactors)

	s.maxSize = 0
	for h := 0; h < s.H; h++ {
		s.maxSize += s.capacity(h)
	}
}

func (s *Sketch) capacity(h int) int {
	return int(math.Ceil(float64(s.k)*computeHeight(s.H-h-1))) + 1
}

// Update adds x to the stream.
func (s *Sketch) Update(x float64) {
	s.Compactors[0] = append(s.Compactors[0], x)
	s.size++
	s.compact()
}

func (s *Sketch) compact() {
	for s.size >= s.maxSize {
		for h := 0; h < len(s.Compactors); h++ {
			if len(s.Compactors[h]) >= s.capacity(h) {
				if h+1 >= s.H {
					s.grow()
				}

				prev_h := len(s.Compactors[h])
				prev_h1 := len(s.Compactors[h+1])

				s.Compactors[h+1] = s.Compactors[h].compact(
					&s.co, s.Compactors[h+1])

				s.size += len(s.Compactors[h]) - prev_h
				s.size += len(s.Compactors[h+1]) - prev_h1

				if s.size < s.maxSize {
					break
				}
			}
		}
	}
}

func (s *Sketch) updateSize() {
	s.size = 0
	for _, c := range s.Compactors {
		s.size += len(c)
	}
}

// Merge merges a second sketch into this one
func (s *Sketch) Merge(t *Sketch) {
	for s.H < t.H {
		s.grow()
	}

	for h, c := range t.Compactors {
		s.Compactors[h] = append(s.Compactors[h], c...)
	}

	s.updateSize()
	s.compact()
}

// Rank estimates the rank of the value x in the stream.
func (s *Sketch) Rank(x float64) int {
	var r int
	for h, c := range s.Compactors {
		for _, v := range c {
			if v <= x {
				r += 1 << uint(h)
			}
		}
	}
	return r
}

func (s *Sketch) Count() int {
	var n int
	for h, c := range s.Compactors {
		n += len(c) * (1 << uint(h))
	}
	return n
}

// Quantile estimates the quantile of the value x in the stream.
func (s *Sketch) Quantile(x float64) float64 {
	var r, n int
	for h, c := range s.Compactors {
		for _, v := range c {
			w := 1 << uint(h)
			if v <= x {
				r += w
			}
			n += w
		}
	}
	return float64(r) / float64(n)
}

type CDF []Quantile

func (q CDF) Len() int { return len(q) }

func (q CDF) Less(i int, j int) bool { return q[i].V < q[j].V }

func (q CDF) Swap(i int, j int) { q[i], q[j] = q[j], q[i] }

type Quantile struct {
	Q float64
	V float64
}

func (s *Sketch) GetMemoryBytes() float64 {
	var total_mem float64 = 0
	total_mem += float64(unsafe.Sizeof(*s))
	for i := range s.Compactors {
		total_mem += float64(len(s.Compactors[i])) * 8
	}
	return total_mem // Bytes
}

func (s *Sketch) CDF() CDF {
	q := make(CDF, 0, s.size)

	var totalW float64
	for h, c := range s.Compactors {
		weight := float64(int(1 << uint(h)))
		for _, v := range c {
			q = append(q, Quantile{Q: weight, V: v})
		}
		totalW += float64(len(c)) * weight
	}

	sort.Sort(q)

	var curW float64
	for i := range q {
		curW += q[i].Q
		q[i].Q = curW / totalW
	}

	return q
}

// Quantile estimates the quantile of the value x in the stream.
func (q CDF) Quantile(x float64) float64 {
	idx := sort.Search(len(q), func(i int) bool { return q[i].V >= x })
	if idx == 0 {
		return 0
	}
	return q[idx-1].Q
}

// Query estimates the value given quantile p.
func (q CDF) Query(p float64) float64 {
	idx := sort.Search(len(q), func(i int) bool { return q[i].Q >= p })
	if idx == len(q) {
		return q[len(q)-1].V
	}
	return q[idx].V
}

// QuantileLI estimates the quantile of the value x in the stream using linear interpolation.
func (q CDF) QuantileLI(x float64) float64 {
	idx := sort.Search(len(q), func(i int) bool { return q[i].V >= x })
	if idx == len(q) {
		return 1
	}
	if idx == 0 {
		return 0
	}
	// a < x <= b
	a, aq := q[idx-1].V, q[idx-1].Q
	b, bq := q[idx].V, q[idx].Q
	return ((a-x)*bq + (x-b)*aq) / (a - b)
}

// QueryLI estimates the value given quantile p using linear interpolation.
func (q CDF) QueryLI(p float64) float64 {
	idx := sort.Search(len(q), func(i int) bool { return q[i].Q >= p })
	if idx == len(q) {
		return q[len(q)-1].V
	}
	if idx == 0 {
		return q[0].V
	}
	// aq < p <= b
	a, aq := q[idx-1].V, q[idx-1].Q
	b, bq := q[idx].V, q[idx].Q
	return ((aq-p)*b + (p-bq)*a) / (aq - bq)
}

type Compactor []float64

func (c *Compactor) compact(co *coin, dst []float64) []float64 {
	l := len(*c)

	if l == 0 || l == 1 {
	} else if l == 2 {
		c := *c
		if c[0] > c[1] {
			c[0], c[1] = c[1], c[0]
		}
	} else if l > 100 {
		sort.Float64s([]float64(*c))
	} else {
		c.insertionSort()
	}

	free := cap(dst) - len(dst)
	if free < len(*c)/2 {
		extra := len(*c)/2 - free
		newdst := make([]float64, len(dst), cap(dst)+extra)
		copy(newdst, dst)
		dst = newdst
	}

	// choose either the evens or the odds
	offs := co.toss()
	for len(*c) >= 2 {
		l := len(*c) - 2
		dst = append(dst, (*c)[l+offs])
		*c = (*c)[:l]
	}

	return dst
}

func (c Compactor) insertionSort() {
	l := len(c)
	for i := 1; i < l; i++ {
		v := c[i]
		j := i
		for ; j > 0 && c[j-1] > v; j-- {
		}
		if j == i {
			continue
		}
		copy(c[j+1:], c[j:i])
		c[j] = v
	}
}
