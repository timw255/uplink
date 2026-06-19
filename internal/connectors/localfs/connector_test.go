package localfs

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/timw255/uplink/internal/connector"
	"github.com/timw255/uplink/internal/store"
)

func newConnector(t *testing.T, name string) *Connector {
	t.Helper()
	root := t.TempDir()
	c, err := New(name, Config{Root: root, PollInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := c.Init(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	return c
}

func TestWriteReadDelete(t *testing.T) {
	c := newConnector(t, "fs")
	ctx := context.Background()

	body := &connector.ReaderSource{Data: []byte("hello uplink")}
	out, err := c.Write(ctx, "sub/a.txt", body, nil)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if out.Size != int64(len("hello uplink")) {
		t.Fatalf("size mismatch: %d", out.Size)
	}

	rc, err := c.Read(ctx, "sub/a.txt")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(got) != "hello uplink" {
		t.Fatalf("Read returned %q", got)
	}

	if err := c.Delete(ctx, "sub/a.txt"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := c.Stat(ctx, "sub/a.txt"); err != connector.ErrNotFound {
		t.Fatalf("Stat after delete: %v", err)
	}
}

func TestPathTraversalRejected(t *testing.T) {
	c := newConnector(t, "fs")
	if _, err := c.resolve("../etc/passwd"); err == nil {
		t.Fatal("expected traversal rejection")
	}
}

func TestReconcileClassifies(t *testing.T) {
	c := newConnector(t, "fs-rec")
	ctx := context.Background()

	dataDir := t.TempDir()
	s, err := store.Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// First pass: one file on disk, nothing known -> New=1.
	if err := os.WriteFile(filepath.Join(c.Root(), "a.txt"), []byte("hi"), 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}
	res, err := c.Reconcile(ctx, s, nil)
	if err != nil {
		t.Fatalf("Reconcile #1: %v", err)
	}
	if res.New != 1 || res.Total != 1 {
		t.Fatalf("first pass: %+v", res)
	}

	// Second pass: no changes -> Unchanged=1.
	res, err = c.Reconcile(ctx, s, nil)
	if err != nil {
		t.Fatalf("Reconcile #2: %v", err)
	}
	if res.Unchanged != 1 || res.New != 0 {
		t.Fatalf("unchanged pass: %+v", res)
	}

	// Third pass: rewrite with new size -> Modified=1.
	time.Sleep(20 * time.Millisecond) // ensure mtime differs on coarse FS clocks
	if err := os.WriteFile(filepath.Join(c.Root(), "a.txt"), []byte("hello"), 0o600); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	res, err = c.Reconcile(ctx, s, nil)
	if err != nil {
		t.Fatalf("Reconcile #3: %v", err)
	}
	if res.Modified != 1 {
		t.Fatalf("modified pass: %+v", res)
	}

	// Fourth pass: file removed -> Deleted=1.
	if err := os.Remove(filepath.Join(c.Root(), "a.txt")); err != nil {
		t.Fatalf("remove: %v", err)
	}
	res, err = c.Reconcile(ctx, s, nil)
	if err != nil {
		t.Fatalf("Reconcile #4: %v", err)
	}
	if res.Deleted != 1 {
		t.Fatalf("deleted pass: %+v", res)
	}
}

func TestReconcileProgressCallback(t *testing.T) {
	c := newConnector(t, "fs-prog")
	ctx := context.Background()

	dataDir := t.TempDir()
	s, err := store.Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	for _, name := range []string{"a", "b", "c"} {
		if err := os.WriteFile(filepath.Join(c.Root(), name+".txt"), []byte(name), 0o600); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	var ticks int
	_, err = c.Reconcile(ctx, s, func(_ connector.ReconcileProgress) { ticks++ })
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if ticks == 0 {
		t.Fatal("expected at least one progress tick")
	}
}

func TestEventSourceDiff(t *testing.T) {
	c := newConnector(t, "fs-in")
	ctx := t.Context()

	dataDir := t.TempDir()
	s, err := store.Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	src := NewEventSource(c, s)

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

	if err := os.WriteFile(filepath.Join(c.Root(), "x.bin"), []byte("aa"), 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(events)
		mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatal("no events observed")
	}
	if events[0].Kind != connector.EventCreate || events[0].Entry.Path != "x.bin" {
		t.Fatalf("unexpected first event: %+v", events[0])
	}
}

// TestListHidesIgnoredEntries confirms List drops paths matched by
// .uplinkignore and the .uplinkignore file itself. Ignored paths are
// invisible to Uplink — companion scripts reach the files declared
// via the channel's `companions:` block, never paths the matcher
// hid.
func TestListHidesIgnoredEntries(t *testing.T) {
	root := t.TempDir()
	c, err := New("fs-ignore", Config{Root: root, PollInterval: time.Second})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	mustWrite(t, root, ".uplinkignore", "*.tmp\nbuild/\n")
	mustWrite(t, root, "report.pdf", "content")
	mustWrite(t, root, "scratch.tmp", "ignore me")
	if err := os.MkdirAll(filepath.Join(root, "build"), 0o755); err != nil {
		t.Fatalf("mkdir build: %v", err)
	}
	mustWrite(t, root, "build/artifact.bin", "ignore me too")

	if err := c.Init(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}

	entries, err := c.List(context.Background(), "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	paths := make(map[string]bool, len(entries))
	for _, e := range entries {
		paths[e.Path] = true
	}
	if !paths["report.pdf"] {
		t.Errorf("List missing report.pdf: got %v", paths)
	}
	for _, hidden := range []string{".uplinkignore", "scratch.tmp", "build/artifact.bin"} {
		if paths[hidden] {
			t.Errorf("List returned ignored path %q: got %v", hidden, paths)
		}
	}
}

// TestWalkHidesIgnoredEntries confirms Walk silently skips paths
// matched by .uplinkignore and the .uplinkignore file itself.
func TestWalkHidesIgnoredEntries(t *testing.T) {
	root := t.TempDir()
	c, err := New("fs-ignore-walk", Config{Root: root, PollInterval: time.Second})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	mustWrite(t, root, ".uplinkignore", "*.tmp\nbuild/\n")
	mustWrite(t, root, "report.pdf", "content")
	mustWrite(t, root, "scratch.tmp", "ignore me")
	if err := os.MkdirAll(filepath.Join(root, "build"), 0o755); err != nil {
		t.Fatalf("mkdir build: %v", err)
	}
	mustWrite(t, root, "build/artifact.bin", "ignore me too")

	if err := c.Init(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}

	var got []string
	err = c.Walk(context.Background(), "", func(e connector.Entry) error {
		got = append(got, e.Path)
		return nil
	})
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if len(got) != 1 || got[0] != "report.pdf" {
		t.Fatalf("Walk yielded %v, want [report.pdf]", got)
	}
}

// TestReadHidesIgnoredEntries confirms Read returns ErrNotFound for
// .uplinkignore-matched paths, while the .uplinkignore file itself
// stays readable so LoadIgnoreMatcher can bootstrap during Init.
func TestReadHidesIgnoredEntries(t *testing.T) {
	root := t.TempDir()
	c, err := New("fs-ignore-read", Config{Root: root, PollInterval: time.Second})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	mustWrite(t, root, ".uplinkignore", "*.tmp\n")
	mustWrite(t, root, "report.pdf", "content")
	mustWrite(t, root, "scratch.tmp", "ignore me")

	ctx := context.Background()
	if err := c.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}

	// Ignored path is invisible.
	if _, err := c.Read(ctx, "scratch.tmp"); err != connector.ErrNotFound {
		t.Fatalf("Read(scratch.tmp) err = %v, want ErrNotFound", err)
	}

	// .uplinkignore itself stays readable post-Init for re-bootstrap.
	rc, err := c.Read(ctx, ".uplinkignore")
	if err != nil {
		t.Fatalf("Read(.uplinkignore): %v", err)
	}
	body, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(body) != "*.tmp\n" {
		t.Fatalf("Read(.uplinkignore) body = %q", body)
	}

	// Non-ignored path works normally.
	rc, err = c.Read(ctx, "report.pdf")
	if err != nil {
		t.Fatalf("Read(report.pdf): %v", err)
	}
	body, _ = io.ReadAll(rc)
	_ = rc.Close()
	if string(body) != "content" {
		t.Fatalf("Read(report.pdf) body = %q", body)
	}
}

// TestScanFiltersIgnoredEntries confirms the event-emission path
// (scan / Reconcile) DOES apply the ignore matcher. Combined with the
// raw List test above, this is the .uplinkignore semantic:
// "ignored files don't trigger sync events but stay visible to Uplink."
func TestScanFiltersIgnoredEntries(t *testing.T) {
	root := t.TempDir()
	c, err := New("fs-ignore-scan", Config{Root: root, PollInterval: time.Second})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	mustWrite(t, root, ".uplinkignore", "*.tmp\nbuild/\n")
	mustWrite(t, root, "report.pdf", "content")
	mustWrite(t, root, "scratch.tmp", "ignore me")
	if err := os.MkdirAll(filepath.Join(root, "build"), 0o755); err != nil {
		t.Fatalf("mkdir build: %v", err)
	}
	mustWrite(t, root, "build/artifact.bin", "ignore me too")

	ctx := context.Background()
	if err := c.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}

	dataDir := t.TempDir()
	s, err := store.Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	res, err := c.Reconcile(ctx, s, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.New != 1 {
		t.Fatalf("res.New = %d, want 1 (only report.pdf should produce an event)", res.New)
	}
}

func mustWrite(t *testing.T, root, rel, body string) {
	t.Helper()
	full := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.WriteFile(full, []byte(body), 0o600); err != nil {
		t.Fatalf("write %s: %v", rel, err)
	}
}
