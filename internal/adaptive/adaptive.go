// Package adaptive provides the building blocks for sizing a worker pool
// to a rate-limited backend in real time. Both the bulk importer and the
// daemon engine drive Aprimo through the same per-tenant token bucket, so
// both face the same question: how many concurrent workers keep that
// bucket drained — running at the licensed ceiling — without piling up
// idle goroutines once it's saturated?
//
// The answer is a feedback loop, not a fixed number. These three pieces
// compose it:
//
//   - Metrics: a cheap, lock-free RateObserver the Aprimo client feeds
//     (request completions, time blocked on the limiter, 429s).
//   - Gate: a resizable concurrency limiter the controller resizes live.
//   - Controller: the AIMD control law that turns each tick's Metrics +
//     backlog signal into the next worker limit.
//
// Worker count is NOT a CPU concern here — the work is network I/O end to
// end, so the governor is the token bucket, and the only CPU-shaped cap
// is the memory/socket safety bound the caller passes as MaxLimit.
package adaptive

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics accumulates rate-limiter telemetry. It structurally satisfies
// aprimo.RateObserver (without importing it, keeping this package
// dependency-free). All methods are on the request hot path — plain
// atomic adds, no allocation.
type Metrics struct {
	requests  atomic.Int64 // completed request attempts since last Sample
	completed atomic.Int64 // cumulative request attempts, never reset
	waitNanos atomic.Int64 // cumulative time blocked acquiring a token
	throttles atomic.Int64 // 429 responses observed
}

func (m *Metrics) ObserveRequest() {
	m.requests.Add(1)
	m.completed.Add(1)
}
func (m *Metrics) ObserveWait(d time.Duration) { m.waitNanos.Add(int64(d)) }
func (m *Metrics) Observe429()                 { m.throttles.Add(1) }

// Completed returns the cumulative request count, never reset by Sample.
// A live status display reads it on its own cadence to compute requests/s
// without disturbing the controller's per-tick sampling. The daemon never
// calls this; it exists for the import command's status line.
func (m *Metrics) Completed() int64 { return m.completed.Load() }

// Sample atomically reads and resets the counters, returning the deltas
// since the previous sample.
func (m *Metrics) Sample() (requests, waitNanos, throttles int64) {
	return m.requests.Swap(0), m.waitNanos.Swap(0), m.throttles.Swap(0)
}

// Gate is a resizable concurrency limiter: at most Limit holders run at
// once, and Limit can change live while workers are blocked waiting. A
// sync.Cond fits naturally — SetLimit broadcasts so parked acquirers
// re-check, and a single ctx watcher broadcasts on cancellation so
// Acquire can unblock and return.
type Gate struct {
	mu    sync.Mutex
	cond  *sync.Cond
	limit int
	max   int
	inUse int
}

// NewGate returns a gate starting at `initial` concurrent slots, never
// exceeding `max`.
func NewGate(initial, max int) *Gate {
	if max < 1 {
		max = 1
	}
	if initial < 1 {
		initial = 1
	}
	if initial > max {
		initial = max
	}
	g := &Gate{limit: initial, max: max}
	g.cond = sync.NewCond(&g.mu)
	return g
}

// Watch wakes parked acquirers when ctx is cancelled so they can return
// promptly instead of waiting for a slot that may never free.
func (g *Gate) Watch(ctx context.Context) {
	go func() {
		<-ctx.Done()
		g.cond.Broadcast()
	}()
}

// Acquire blocks until a slot is free or ctx is cancelled.
func (g *Gate) Acquire(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	for g.inUse >= g.limit {
		if err := ctx.Err(); err != nil {
			return err
		}
		g.cond.Wait()
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	g.inUse++
	return nil
}

// Release returns a slot.
func (g *Gate) Release() {
	g.mu.Lock()
	g.inUse--
	g.mu.Unlock()
	g.cond.Signal()
}

// SetLimit changes the number of concurrent slots, clamped to [1, max].
func (g *Gate) SetLimit(n int) {
	if n < 1 {
		n = 1
	}
	if n > g.max {
		n = g.max
	}
	g.mu.Lock()
	changed := g.limit != n
	g.limit = n
	g.mu.Unlock()
	if changed {
		g.cond.Broadcast()
	}
}

// CurrentLimit reports the active slot count.
func (g *Gate) CurrentLimit() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.limit
}

// Sample is one tick's measurements handed to the controller.
type Sample struct {
	// Achieved is requests completed during the tick.
	Achieved float64
	// Throttles is 429s seen during the tick.
	Throttles int64
	// HasBacklog reports whether there is pending work to do. A bulk
	// import always has a backlog until the manifest is exhausted; the
	// daemon's queue is usually empty, so it passes false on idle ticks
	// to let the pool shrink instead of pegging at max.
	HasBacklog bool
}

// Controller is the AIMD/gradient control law. Construct with TargetRPS,
// MaxLimit, and (for bursty workloads) Baseline, then drive it with Run
// or call Step directly in tests.
type Controller struct {
	// TargetRPS is the licensed request ceiling. 0 disables the
	// saturation check (the loop then ramps on backlog alone).
	TargetRPS float64
	// MaxLimit is the hard worker cap — a memory/socket safety bound,
	// not a CPU bound.
	MaxLimit int
	// Baseline is the floor the pool shrinks to when idle. 0 means 1.
	Baseline int

	perTick  float64
	prev     float64
	cooldown int
}

// Run resizes g once per tick from sample() until ctx is cancelled.
func (c *Controller) Run(ctx context.Context, g *Gate, tick time.Duration, sample func() Sample) {
	if tick <= 0 {
		tick = time.Second
	}
	c.perTick = c.TargetRPS * tick.Seconds()
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		g.SetLimit(c.Step(sample(), g.CurrentLimit()))
	}
}

// Step computes the next worker limit from one tick's sample. Pure (no
// I/O, no clock) so the control law is unit-tested directly. When Run is
// not used, set perTick via SetPerTick (or TargetRPS with a 1s tick).
//
//   - 429s          → multiplicative back-off, then a short cooldown
//   - idle          → shrink toward Baseline (free idle workers)
//   - saturated     → hold (running at the licensed ceiling)
//   - climbing      → additive increase (more in-flight work helps)
//   - plateaued     → hold (latency/semaphore bound, not worker bound)
//
// Big-file workloads settle at a high limit (each record holds a worker
// through many slow segment posts, so it takes more workers to keep the
// limiter fed); small-file or idle workloads settle low. That falls out
// of the loop — no per-item heuristic.
func (c *Controller) Step(s Sample, limit int) int {
	switch {
	case s.Throttles > 0:
		limit = limit * 7 / 10
		c.cooldown = 3
	case c.cooldown > 0:
		c.cooldown--
	case !s.HasBacklog:
		// No pending work: halve toward the baseline so idle ticks don't
		// hold a wide pool of sleeping workers.
		limit /= 2
	case c.perTick > 0 && s.Achieved >= 0.9*c.perTick:
		// Saturated: running at (near) the licensed ceiling. Hold.
	default:
		// Under the ceiling with work waiting. Add workers while doing so
		// still lifts throughput, or while small enough that ramping is
		// obviously safe.
		if s.Achieved > c.prev*1.05 || limit < 8 {
			step := limit / 4
			if step < 2 {
				step = 2
			}
			limit += step
		}
	}

	floor := c.Baseline
	if floor < 1 {
		floor = 1
	}
	if limit < floor {
		limit = floor
	}
	if limit > c.MaxLimit {
		limit = c.MaxLimit
	}
	c.prev = s.Achieved
	return limit
}

// SetPerTick sets the expected requests-per-tick ceiling directly. Tests
// use it to exercise Step without running the ticker.
func (c *Controller) SetPerTick(v float64) { c.perTick = v }
