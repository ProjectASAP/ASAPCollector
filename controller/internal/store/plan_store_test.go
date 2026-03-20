package store_test

import (
	"errors"
	"testing"
	"time"

	"github.com/ProjectASAP/controller/internal/store"
	"github.com/ProjectASAP/controller/internal/types"
)

func makePlan(validFor time.Duration) types.CollectionPlan {
	return types.CollectionPlan{ValidUntil: time.Now().Add(validFor)}
}

func TestPlanStore_SetAndGet(t *testing.T) {
	ps := store.New()
	plan := makePlan(10 * time.Minute)
	ps.Set("latency", plan)

	got, err := ps.Get("latency")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !got.ValidUntil.Equal(plan.ValidUntil) {
		t.Errorf("ValidUntil = %v, want %v", got.ValidUntil, plan.ValidUntil)
	}
}

func TestPlanStore_GetNotFound(t *testing.T) {
	ps := store.New()
	_, err := ps.Get("nonexistent")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestPlanStore_Rollback(t *testing.T) {
	ps := store.New()
	plan1 := makePlan(10 * time.Minute)
	plan2 := makePlan(20 * time.Minute)

	ps.Set("latency", plan1)
	ps.Set("latency", plan2)

	rolled, err := ps.Rollback("latency")
	if err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if !rolled.ValidUntil.Equal(plan1.ValidUntil) {
		t.Errorf("rolled back plan ValidUntil = %v, want %v", rolled.ValidUntil, plan1.ValidUntil)
	}

	// After rollback, Get should return the original plan.
	got, _ := ps.Get("latency")
	if !got.ValidUntil.Equal(plan1.ValidUntil) {
		t.Errorf("after rollback Get ValidUntil = %v, want %v", got.ValidUntil, plan1.ValidUntil)
	}
}

func TestPlanStore_RollbackNoPrevious(t *testing.T) {
	ps := store.New()
	ps.Set("latency", makePlan(10*time.Minute))

	_, err := ps.Rollback("latency")
	if err == nil {
		t.Error("expected error when rolling back with no previous plan, got nil")
	}
}

func TestPlanStore_RollbackNotFound(t *testing.T) {
	ps := store.New()
	_, err := ps.Rollback("nonexistent")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestPlanStore_Metrics(t *testing.T) {
	ps := store.New()
	ps.Set("latency", makePlan(5*time.Minute))
	ps.Set("error_rate", makePlan(5*time.Minute))

	metrics := ps.Metrics()
	if len(metrics) != 2 {
		t.Errorf("Metrics() len = %d, want 2", len(metrics))
	}
}

func TestPlanStore_Expired(t *testing.T) {
	ps := store.New()
	ps.Set("expired_metric", makePlan(-1*time.Second)) // already expired
	ps.Set("active_metric", makePlan(10*time.Minute))

	expired := ps.Expired(time.Now())
	if len(expired) != 1 || expired[0] != "expired_metric" {
		t.Errorf("Expired() = %v, want [expired_metric]", expired)
	}
}

func TestPlanStore_Concurrent(t *testing.T) {
	ps := store.New()
	done := make(chan struct{})

	go func() {
		for i := 0; i < 100; i++ {
			ps.Set("m", makePlan(time.Minute))
		}
		close(done)
	}()

	for i := 0; i < 100; i++ {
		ps.Get("m") //nolint:errcheck
	}
	<-done
}
