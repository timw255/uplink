package localfs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/timw255/uplink/internal/connector"
	"github.com/timw255/uplink/internal/store"
)

// togglingStateStore wraps a real StateStore and lets the test flip
// LoadState into a fail-mode for a window. Used to drive a transient
// scan error inside Subscribe without touching the filesystem.
type togglingStateStore struct {
	real connector.StateStore
	fail atomic.Bool
}

var errToggledFailure = errors.New("toggling state store: injected failure")

func (s *togglingStateStore) LoadState(ctx context.Context, name string) (map[string]connector.StateEntry, error) {
	if s.fail.Load() {
		return nil, errToggledFailure
	}
	return s.real.LoadState(ctx, name)
}

func (s *togglingStateStore) SaveState(ctx context.Context, name string, state map[string]connector.StateEntry) error {
	if s.fail.Load() {
		return errToggledFailure
	}
	return s.real.SaveState(ctx, name, state)
}

func (s *togglingStateStore) NextGeneration(ctx context.Context, scope string) (int64, error) {
	if s.fail.Load() {
		return 0, errToggledFailure
	}
	return s.real.NextGeneration(ctx, scope)
}

func (s *togglingStateStore) LoadStateFor(ctx context.Context, scope string, paths []string) (map[string]connector.StateEntry, error) {
	if s.fail.Load() {
		return nil, errToggledFailure
	}
	return s.real.LoadStateFor(ctx, scope, paths)
}

func (s *togglingStateStore) ApplyStateDelta(ctx context.Context, scope string, upserts []connector.StateEntry, gen int64) error {
	if s.fail.Load() {
		return errToggledFailure
	}
	return s.real.ApplyStateDelta(ctx, scope, upserts, gen)
}

func (s *togglingStateStore) SweepStateBelowGeneration(ctx context.Context, scope string, gen int64) ([]string, error) {
	if s.fail.Load() {
		return nil, errToggledFailure
	}
	return s.real.SweepStateBelowGeneration(ctx, scope, gen)
}

// TestEventSource_SurvivesScanError proves the Subscribe loop no longer
// returns on a transient scan failure. Previously, a single scan error
// (e.g. backend hiccup) killed the subscription, leaving polling dead
// until the daemon restarted. With the fix, errors log and the next
// tick retries — so a transient outage followed by recovery still sees
// new files.
func TestEventSource_SurvivesScanError(t *testing.T) {
	c := newConnector(t, "fs-in")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dataDir := t.TempDir()
	realStore, err := store.Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = realStore.Close() })

	ts := &togglingStateStore{real: realStore}
	src := NewEventSource(c, ts)

	var (
		mu     sync.Mutex
		events []connector.Event
	)
	handler := connector.HandlerFunc(func(_ context.Context, e connector.Event) error {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, e)
		return nil
	})

	// Subscribe runs until ctx cancel; record the return to confirm it
	// did NOT exit during the failure window.
	subscribeDone := make(chan error, 1)
	go func() { subscribeDone <- src.Subscribe(ctx, handler) }()

	// Warm-up tick uses the real store (fail=false). Wait long enough
	// for the warm-up to definitely complete before flipping fail mode.
	time.Sleep(150 * time.Millisecond)

	// Flip into fail mode. Several poll ticks will hit scan() failures.
	ts.fail.Store(true)
	time.Sleep(300 * time.Millisecond) // ~6 ticks at 50ms PollInterval

	// Subscribe must still be running. With the old behavior, scan
	// errors returned from Subscribe so the channel would have a
	// value on it by now.
	select {
	case err := <-subscribeDone:
		t.Fatalf("Subscribe exited during transient failure window: %v", err)
	default:
	}

	// Flip back to healthy and drop a file. The next tick's scan should
	// pick it up.
	ts.fail.Store(false)
	if err := os.WriteFile(filepath.Join(c.Root(), "after-recovery.bin"),
		[]byte("ok"), 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Wait up to 2s for the post-recovery event.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		var sawRecovery bool
		for _, e := range events {
			if e.Entry.Path == "after-recovery.bin" {
				sawRecovery = true
				break
			}
		}
		mu.Unlock()
		if sawRecovery {
			return // success
		}
		time.Sleep(20 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	t.Fatalf("event for after-recovery.bin never observed; events=%+v", events)
}
