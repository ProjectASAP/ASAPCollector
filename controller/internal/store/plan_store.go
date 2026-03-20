// Package store persists the current CollectionPlan per metric and supports
// diff and rollback.
package store

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ProjectASAP/controller/internal/types"
)

// ErrNotFound is returned when no plan exists for the requested metric.
var ErrNotFound = errors.New("plan not found")

// Entry holds a plan and its history for a single metric.
type Entry struct {
	Current  types.CollectionPlan
	Previous *types.CollectionPlan // nil if no prior plan
	UpdatedAt time.Time
}

// PlanStore is a thread-safe, in-memory store of CollectionPlans keyed by
// metric name. It retains the previous plan for each metric to support rollback.
type PlanStore struct {
	mu      sync.RWMutex
	entries map[string]*Entry
}

// New returns an empty PlanStore.
func New() *PlanStore {
	return &PlanStore{entries: make(map[string]*Entry)}
}

// Set stores a new plan for the given metric, archiving the previous one.
func (s *PlanStore) Set(metric string, plan types.CollectionPlan) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.entries[metric]
	if !ok {
		s.entries[metric] = &Entry{Current: plan, UpdatedAt: time.Now()}
		return
	}
	prev := e.Current
	e.Previous = &prev
	e.Current = plan
	e.UpdatedAt = time.Now()
}

// Get returns the current plan for a metric, or ErrNotFound.
func (s *PlanStore) Get(metric string) (types.CollectionPlan, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.entries[metric]
	if !ok {
		return types.CollectionPlan{}, ErrNotFound
	}
	return e.Current, nil
}

// Rollback replaces the current plan with the previous one and returns it.
// Returns ErrNotFound if no plan exists, or an error if there is no previous plan.
func (s *PlanStore) Rollback(metric string) (types.CollectionPlan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.entries[metric]
	if !ok {
		return types.CollectionPlan{}, ErrNotFound
	}
	if e.Previous == nil {
		return types.CollectionPlan{}, fmt.Errorf("no previous plan for metric %q", metric)
	}
	e.Current = *e.Previous
	e.Previous = nil
	e.UpdatedAt = time.Now()
	return e.Current, nil
}

// Metrics returns the list of metrics that have stored plans.
func (s *PlanStore) Metrics() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]string, 0, len(s.entries))
	for k := range s.entries {
		out = append(out, k)
	}
	return out
}

// Expired returns all metrics whose current plan has passed its ValidUntil time.
func (s *PlanStore) Expired(now time.Time) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []string
	for metric, e := range s.entries {
		if !e.Current.ValidUntil.IsZero() && now.After(e.Current.ValidUntil) {
			out = append(out, metric)
		}
	}
	return out
}
