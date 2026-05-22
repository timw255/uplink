package engine

import (
	"testing"
	"time"
)

// TestBackoff_AppliesJitter proves successive calls for the same
// attempt count don't return identical durations. Without jitter, a
// burst of simultaneously-failing jobs would all retry at exactly the
// same instant.
func TestBackoff_AppliesJitter(t *testing.T) {
	const attempts = 3
	const base = 100 * time.Millisecond

	seen := make(map[time.Duration]int)
	for range 200 {
		seen[backoff(base, attempts)]++
	}
	if len(seen) < 50 {
		// Should be a wide spread across the ±25% window; under 50
		// distinct values across 200 calls means jitter isn't working.
		t.Fatalf("backoff appears non-jittered: only %d distinct values in 200 calls", len(seen))
	}
}

// TestBackoff_JitterWindow asserts the jitter stays inside ±25% of
// the doubled base, never above it (so we don't accidentally exceed
// the cap) and never below 75% of base (so we don't degenerate to 0).
func TestBackoff_JitterWindow(t *testing.T) {
	const attempts = 4 // base * 2 * 2 * 2 = 8x base
	const base = 100 * time.Millisecond
	const expected = 8 * base

	lo := time.Duration(float64(expected) * 0.75)
	hi := time.Duration(float64(expected) * 1.25)

	for i := range 500 {
		d := backoff(base, attempts)
		if d < lo || d > hi {
			t.Fatalf("iteration %d: backoff=%v out of [%v, %v]", i, d, lo, hi)
		}
	}
}

// TestBackoff_FirstAttemptReturnsNearBase asserts attempt=1 jitters
// around the base value (no doubling for the first attempt).
func TestBackoff_FirstAttemptReturnsNearBase(t *testing.T) {
	const base = 200 * time.Millisecond
	lo := time.Duration(float64(base) * 0.75)
	hi := time.Duration(float64(base) * 1.25)
	for i := range 100 {
		d := backoff(base, 1)
		if d < lo || d > hi {
			t.Fatalf("iteration %d: backoff=%v out of [%v, %v]", i, d, lo, hi)
		}
	}
}
