package store

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/timw255/uplink/internal/connector"
)

// TestStateDelta_LifecyclePerScope locks down the streaming-scan
// lifecycle: bump generation, upsert observed entries with the new
// generation, sweep below to surface deletions. Operators using
// scoped state via watchers (P3.3) get one row per scope partition,
// each with its own generation counter.
func TestStateDelta_LifecyclePerScope(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)

	scope := "fs-in"

	// Generation 1: three files observed.
	gen, err := s.NextGeneration(ctx, scope)
	if err != nil {
		t.Fatalf("NextGeneration: %v", err)
	}
	if gen != 1 {
		t.Fatalf("first gen = %d, want 1", gen)
	}

	now := time.Now().UTC()
	mustApply(t, s, scope, []connector.StateEntry{
		{Path: "a.txt", Size: 10, ModTime: now, Hash: "h1"},
		{Path: "b.txt", Size: 20, ModTime: now, Hash: "h2"},
		{Path: "c.txt", Size: 30, ModTime: now, Hash: "h3"},
	}, gen)
	deleted, err := s.SweepStateBelowGeneration(ctx, scope, gen)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(deleted) != 0 {
		t.Errorf("first scan: unexpected deletions %v", deleted)
	}

	// Generation 2: only a and c observed, b is gone, d is new.
	gen2, _ := s.NextGeneration(ctx, scope)
	if gen2 != 2 {
		t.Fatalf("second gen = %d, want 2", gen2)
	}
	mustApply(t, s, scope, []connector.StateEntry{
		{Path: "a.txt", Size: 11, ModTime: now, Hash: "h1-updated"},
		{Path: "c.txt", Size: 30, ModTime: now, Hash: "h3"},
		{Path: "d.txt", Size: 40, ModTime: now, Hash: "h4"},
	}, gen2)
	deleted, err = s.SweepStateBelowGeneration(ctx, scope, gen2)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	sort.Strings(deleted)
	if !reflect.DeepEqual(deleted, []string{"b.txt"}) {
		t.Fatalf("deletions = %v, want [b.txt]", deleted)
	}

	// LoadState reflects the new picture.
	st, err := s.LoadState(ctx, scope)
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	wantPaths := []string{"a.txt", "c.txt", "d.txt"}
	got := make([]string, 0, len(st))
	for k := range st {
		got = append(got, k)
	}
	sort.Strings(got)
	if !reflect.DeepEqual(got, wantPaths) {
		t.Errorf("LoadState paths = %v, want %v", got, wantPaths)
	}
	if st["a.txt"].Hash != "h1-updated" {
		t.Errorf("a.txt hash = %q, want h1-updated", st["a.txt"].Hash)
	}
}

// TestStateDelta_ScopesAreIndependent confirms that two scopes (the
// watcher partitioning unit) don't share state — a delete-sweep on
// one does not touch the other.
func TestStateDelta_ScopesAreIndependent(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)

	now := time.Now().UTC()

	gFast, _ := s.NextGeneration(ctx, "fs-in#images/hot")
	gSlow, _ := s.NextGeneration(ctx, "fs-in")
	mustApply(t, s, "fs-in#images/hot", []connector.StateEntry{{Path: "hero.jpg", Size: 1, ModTime: now}}, gFast)
	mustApply(t, s, "fs-in", []connector.StateEntry{{Path: "archive/old.bin", Size: 2, ModTime: now}}, gSlow)

	gFast2, _ := s.NextGeneration(ctx, "fs-in#images/hot")
	// Hot watcher's second scan sees nothing. Sweep should report
	// hero.jpg as deleted, NOT archive/old.bin (different scope).
	deleted, err := s.SweepStateBelowGeneration(ctx, "fs-in#images/hot", gFast2)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if !reflect.DeepEqual(deleted, []string{"hero.jpg"}) {
		t.Errorf("sweep deletions = %v, want [hero.jpg]", deleted)
	}

	// archive scope untouched.
	st, _ := s.LoadState(ctx, "fs-in")
	if _, ok := st["archive/old.bin"]; !ok {
		t.Errorf("archive scope was touched by hot sweep")
	}
}

func TestStateDelta_LoadStateForChunksBigBatches(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)

	now := time.Now().UTC()
	// Seed 1200 paths so chunking (500) has to kick in twice.
	entries := make([]connector.StateEntry, 0, 1200)
	for i := 0; i < 1200; i++ {
		entries = append(entries, connector.StateEntry{
			Path: testPath(i), Size: int64(i), ModTime: now, Hash: testHash(i),
		})
	}
	gen, _ := s.NextGeneration(ctx, "bulk")
	mustApply(t, s, "bulk", entries, gen)

	paths := make([]string, 0, 1200)
	for _, e := range entries {
		paths = append(paths, e.Path)
	}
	got, err := s.LoadStateFor(ctx, "bulk", paths)
	if err != nil {
		t.Fatalf("LoadStateFor: %v", err)
	}
	if len(got) != 1200 {
		t.Fatalf("len = %d, want 1200", len(got))
	}
	if got[testPath(999)].Size != 999 {
		t.Errorf("entry 999 = %+v", got[testPath(999)])
	}
}

func mustApply(t *testing.T, s *Store, scope string, entries []connector.StateEntry, gen int64) {
	t.Helper()
	if err := s.ApplyStateDelta(context.Background(), scope, entries, gen); err != nil {
		t.Fatalf("ApplyStateDelta(%q, gen=%d): %v", scope, gen, err)
	}
}

func testPath(i int) string { return "path/" + itoa(i) + ".bin" }
func testHash(i int) string { return "h" + itoa(i) }

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}
