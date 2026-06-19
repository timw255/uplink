package localfs

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/timw255/uplink/internal/connector"
	"github.com/timw255/uplink/internal/store"
)

// TestNestedWatchers_PartitionTreeByLongestPrefix proves the watcher
// semantics: paths belong to the most-specific watcher only, and
// each watcher's scan operates on its own state-table scope.
//
// Layout:
//
//	root/
//	  hot/a.bin       ← owned by "hot/" watcher
//	  hot/sub/b.bin   ← owned by "hot/" watcher
//	  cold/c.bin      ← owned by root catch-all
//
// Both watchers run at fast cadences; the test asserts events surface
// once per file (not duplicated across watchers).
func TestNestedWatchers_PartitionTreeByLongestPrefix(t *testing.T) {
	root := t.TempDir()
	for _, p := range []string{
		filepath.Join(root, "hot", "a.bin"),
		filepath.Join(root, "hot", "sub", "b.bin"),
		filepath.Join(root, "cold", "c.bin"),
	} {
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatalf("write %s: %v", p, err)
		}
	}

	c, err := New("fs-in", Config{
		Root:         root,
		PollInterval: 100 * time.Millisecond,
		Watchers: []connector.WatcherSpec{
			{Prefix: "hot/", PollInterval: 50 * time.Millisecond},
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := c.Init(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}

	dataDir := t.TempDir()
	s, err := store.Open(context.Background(), dataDir)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	src := NewEventSource(c, s)
	ctx := t.Context()

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
	go func() { _ = src.Subscribe(ctx, handler) }()

	// Wait for the three create events.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(events)
		mu.Unlock()
		if n >= 3 {
			break
		}
		time.Sleep(40 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) < 3 {
		t.Fatalf("expected 3 events, got %d: %+v", len(events), events)
	}

	// Each path appears exactly once — no double-emission across
	// watchers despite "hot/sub/b.bin" being technically reachable
	// from BOTH the root and the "hot/" watchers' prefix coverage.
	// The longest-prefix rule means only "hot/" owns it.
	seen := map[string]int{}
	for _, e := range events {
		seen[e.Entry.Path]++
	}
	for _, want := range []string{"hot/a.bin", "hot/sub/b.bin", "cold/c.bin"} {
		if seen[want] != 1 {
			t.Errorf("path %q seen %d times, want 1", want, seen[want])
		}
	}

	// State table is partitioned by scope: hot/ rows live under
	// "fs-in#hot", root rows under "fs-in".
	rootState, _ := s.LoadState(context.Background(), "fs-in")
	hotState, _ := s.LoadState(context.Background(), "fs-in#hot")
	if _, ok := rootState["cold/c.bin"]; !ok {
		t.Errorf("cold/c.bin should be in the root scope's state")
	}
	if _, ok := rootState["hot/a.bin"]; ok {
		t.Errorf("hot/a.bin should NOT be in the root scope (owned by hot/ watcher)")
	}
	if _, ok := hotState["hot/a.bin"]; !ok {
		t.Errorf("hot/a.bin should be in the hot/ scope's state")
	}
	if _, ok := hotState["cold/c.bin"]; ok {
		t.Errorf("cold/c.bin should NOT be in the hot/ scope")
	}
}
