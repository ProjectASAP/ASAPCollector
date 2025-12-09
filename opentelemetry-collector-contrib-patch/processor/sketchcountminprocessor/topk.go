// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sketchmetricsprocessor

import "sort"

// TopKEntry represents a heavy hitter entry.
type TopKEntry struct {
	Key   string  `json:"key"`
	Count float64 `json:"count"`
}

type heapItem struct {
	key   string
	count float64
}

// TopKHeap maintains approximate heavy hitters using a bounded min-heap.
type TopKHeap struct {
	heap  []heapItem
	limit int
}

// NewTopKHeap creates a heap limited to k entries. Returns nil if k <= 0.
func NewTopKHeap(k int) *TopKHeap {
	if k <= 0 {
		return nil
	}
	return &TopKHeap{
		heap:  make([]heapItem, 0, k),
		limit: k,
	}
}

func (h *TopKHeap) leftChild(i int) int {
	return 2*i + 1
}

func (h *TopKHeap) rightChild(i int) int {
	return 2*i + 2
}

func (h *TopKHeap) parent(i int) int {
	return (i - 1) / 2
}

func (h *TopKHeap) swap(i, j int) {
	h.heap[i], h.heap[j] = h.heap[j], h.heap[i]
}

func (h *TopKHeap) updateOrderDown(i int) bool {
	n := len(h.heap)
	orig := i
	for i < n {
		l := h.leftChild(i)
		r := h.rightChild(i)
		smallest := i
		if l < n && h.heap[l].count < h.heap[smallest].count {
			smallest = l
		}
		if r < n && h.heap[r].count < h.heap[smallest].count {
			smallest = r
		}
		if smallest == i {
			break
		}
		h.swap(i, smallest)
		i = smallest
	}
	return i != orig
}

func (h *TopKHeap) updateOrderUp(i int) {
	for i > 0 {
		p := h.parent(i)
		if h.heap[p].count > h.heap[i].count {
			h.swap(p, i)
			i = p
		} else {
			break
		}
	}
}

// Update inserts or updates the given key with its latest approximate count.
func (h *TopKHeap) Update(key string, count float64) {
	if h == nil || count <= 0 {
		return
	}
	if idx, ok := h.find(key); ok {
		h.heap[idx].count = count
		if !h.updateOrderDown(idx) {
			h.updateOrderUp(idx)
		}
		return
	}
	if len(h.heap) < h.limit {
		h.heap = append(h.heap, heapItem{
			key:   key,
			count: count,
		})
		h.updateOrderUp(len(h.heap) - 1)
		return
	}
	if len(h.heap) == 0 || h.heap[0].count >= count {
		return
	}
	h.heap[0].key = key
	h.heap[0].count = count
	h.updateOrderDown(0)
}

func (h *TopKHeap) find(key string) (int, bool) {
	for i, item := range h.heap {
		if item.key == key {
			return i, true
		}
	}
	return -1, false
}

// Entries returns the heap contents sorted descending by count.
func (h *TopKHeap) Entries() []TopKEntry {
	if h == nil || len(h.heap) == 0 {
		return nil
	}
	entries := make([]TopKEntry, len(h.heap))
	for i, item := range h.heap {
		entries[i] = TopKEntry{
			Key:   item.key,
			Count: item.count,
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Count > entries[j].Count
	})
	return entries
}
