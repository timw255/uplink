package adaptive

import (
	"context"
	"testing"
	"time"
)

func TestGateAcquireBlocksAtLimit(t *testing.T) {
	g := NewGate(1, 4)
	ctx := context.Background()

	if err := g.Acquire(ctx); err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	acquired := make(chan struct{})
	go func() {
		_ = g.Acquire(ctx)
		close(acquired)
	}()

	select {
	case <-acquired:
		t.Fatal("second acquire returned while at limit")
	case <-time.After(50 * time.Millisecond):
	}

	g.Release()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("second acquire did not unblock after release")
	}
}

func TestGateSetLimitUnblocks(t *testing.T) {
	g := NewGate(1, 4)
	ctx := context.Background()
	if err := g.Acquire(ctx); err != nil {
		t.Fatalf("acquire: %v", err)
	}

	acquired := make(chan struct{})
	go func() {
		_ = g.Acquire(ctx)
		close(acquired)
	}()

	g.SetLimit(2)
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("raising the limit did not unblock a waiter")
	}
}

func TestGateAcquireRespectsContext(t *testing.T) {
	g := NewGate(1, 4)
	ctx, cancel := context.WithCancel(context.Background())
	g.Watch(ctx)
	if err := g.Acquire(ctx); err != nil {
		t.Fatalf("acquire: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- g.Acquire(ctx) }()

	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected acquire to return ctx error after cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("acquire did not return after ctx cancel")
	}
}

func TestMetricsSampleResets(t *testing.T) {
	var m Metrics
	m.ObserveRequest()
	m.ObserveRequest()
	m.Observe429()
	m.ObserveWait(5 * time.Millisecond)

	reqs, wait, thr := m.Sample()
	if reqs != 2 || thr != 1 || wait != int64(5*time.Millisecond) {
		t.Fatalf("sample = (%d, %d, %d)", reqs, wait, thr)
	}
	// Second sample must be zeroed.
	reqs, wait, thr = m.Sample()
	if reqs != 0 || wait != 0 || thr != 0 {
		t.Fatalf("counters not reset: (%d, %d, %d)", reqs, wait, thr)
	}
}

// newController builds a controller with perTick set directly so Step
// can be exercised without the ticker.
func newController(target float64, maxLimit, baseline int) *Controller {
	c := &Controller{TargetRPS: target, MaxLimit: maxLimit, Baseline: baseline}
	c.SetPerTick(target) // 1s-tick equivalence
	return c
}

func TestControllerRampsUpUnderTarget(t *testing.T) {
	c := newController(100, 64, 0)
	limit := 4
	achieved := 10.0
	for i := 0; i < 6; i++ {
		next := c.Step(Sample{Achieved: achieved, HasBacklog: true}, limit)
		if next <= limit {
			t.Fatalf("tick %d: expected growth from %d, got %d", i, limit, next)
		}
		limit = next
		achieved *= 1.5
	}
	if limit > 64 {
		t.Fatalf("limit exceeded max: %d", limit)
	}
}

func TestControllerHoldsAtSaturation(t *testing.T) {
	c := newController(100, 64, 0)
	limit := 32
	for i := 0; i < 3; i++ {
		next := c.Step(Sample{Achieved: 98, HasBacklog: true}, limit)
		if next != limit {
			t.Fatalf("tick %d: expected hold at %d, got %d", i, limit, next)
		}
		limit = next
	}
}

func TestControllerBacksOffOnThrottle(t *testing.T) {
	c := newController(100, 64, 0)
	limit := 40
	next := c.Step(Sample{Achieved: 50, Throttles: 5, HasBacklog: true}, limit)
	if next >= limit {
		t.Fatalf("expected back-off below %d, got %d", limit, next)
	}
	held := next
	for i := 0; i < 3; i++ {
		next = c.Step(Sample{Achieved: 10, HasBacklog: true}, next)
		if next != held {
			t.Fatalf("cooldown tick %d: expected hold at %d, got %d", i, held, next)
		}
	}
}

func TestControllerShrinksWhenIdle(t *testing.T) {
	c := newController(100, 64, 1)
	limit := 32
	// No backlog => pool should shrink toward the baseline.
	for i := 0; i < 10; i++ {
		next := c.Step(Sample{Achieved: 0, HasBacklog: false}, limit)
		if next > limit {
			t.Fatalf("idle tick %d: pool grew from %d to %d", i, limit, next)
		}
		limit = next
	}
	if limit != 1 {
		t.Fatalf("idle pool did not shrink to baseline 1, got %d", limit)
	}
}

func TestControllerRespectsBaselineFloor(t *testing.T) {
	c := newController(100, 64, 4)
	limit := 8
	for i := 0; i < 10; i++ {
		limit = c.Step(Sample{Achieved: 0, HasBacklog: false}, limit)
	}
	if limit != 4 {
		t.Fatalf("expected shrink to floor baseline 4, got %d", limit)
	}
}

func TestControllerRespectsMaxLimit(t *testing.T) {
	c := newController(1e9, 10, 0)
	limit := 8
	for i := 0; i < 20; i++ {
		limit = c.Step(Sample{Achieved: 1, HasBacklog: true}, limit)
	}
	if limit > 10 {
		t.Fatalf("limit %d exceeded max 10", limit)
	}
}
