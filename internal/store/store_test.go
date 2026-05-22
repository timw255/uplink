package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/timw255/uplink/internal/connector"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(context.Background(), dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestOpenIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	s1, err := Open(ctx, dir)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	_ = s1.Close()

	s2, err := Open(ctx, dir)
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	defer s2.Close()

	// sync_log table exists and is queryable.
	var n int
	if err := s2.DB().QueryRow(`SELECT COUNT(*) FROM sync_log`).Scan(&n); err != nil {
		t.Fatalf("query sync_log: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected empty sync_log, got %d rows", n)
	}
}

func TestJobLifecycleFilesystem(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	id, err := s.EnqueueJob(ctx, Job{
		ChannelName: "chan",
		Kind:        "OnCreate",
		SourcePath:  "in/foo.txt",
	})
	if err != nil {
		t.Fatalf("EnqueueJob: %v", err)
	}

	got, err := s.ClaimNextJob(ctx)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if got.ID != id || got.Attempts != 1 {
		t.Fatalf("unexpected claimed job: %+v", got)
	}

	if err := s.CompleteJob(ctx, id); err != nil {
		t.Fatalf("CompleteJob: %v", err)
	}

	if _, err := s.ClaimNextJob(ctx); !errors.Is(err, ErrNoJob) {
		t.Fatalf("expected ErrNoJob after completion, got %v", err)
	}
}

func TestRetryDeferredViaNextRunAt(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	id, err := s.EnqueueJob(ctx, Job{
		ChannelName: "chan",
		Kind:        "OnCreate",
		SourcePath:  "in/foo",
	})
	if err != nil {
		t.Fatalf("EnqueueJob: %v", err)
	}
	if _, err := s.ClaimNextJob(ctx); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := s.RetryJob(ctx, id, 1, "transient", time.Hour); err != nil {
		t.Fatalf("Retry: %v", err)
	}
	if _, err := s.ClaimNextJob(ctx); !errors.Is(err, ErrNoJob) {
		t.Fatalf("expected ErrNoJob while retry is in the future, got %v", err)
	}
}

func TestStartupRecoveryMovesRunningBackToPending(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	s, err := Open(ctx, dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	id, err := s.EnqueueJob(ctx, Job{ChannelName: "c", Kind: "OnCreate", SourcePath: "foo"})
	if err != nil {
		t.Fatalf("EnqueueJob: %v", err)
	}
	if _, err := s.ClaimNextJob(ctx); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	// Simulate a crash: close the store without completing the running job.
	_ = s.Close()

	s2, err := Open(ctx, dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	// The reopen should have moved the running job back to pending.
	got, err := s2.ClaimNextJob(ctx)
	if err != nil {
		t.Fatalf("Claim after recovery: %v", err)
	}
	if got.ID != id {
		t.Fatalf("got %s, want %s", got.ID, id)
	}
	if got.Attempts != 2 {
		// Each ClaimNextJob increments attempts atomically (SQLite's
		// UPDATE…RETURNING is atomic). After a crash mid-run, recovery
		// flips the row back to pending without rolling back the
		// increment — so a job claimed once, crashed, and reclaimed
		// shows attempts=2. The engine's max-attempts check sees the
		// real run count.
		t.Fatalf("attempts = %d, want 2", got.Attempts)
	}
}

func TestStateLoadSave(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	st := map[string]connector.StateEntry{
		"a/b.txt": {
			Path:    "a/b.txt",
			Size:    42,
			ModTime: time.Now().UTC().Truncate(time.Millisecond),
			Hash:    "sha256:deadbeef",
		},
	}
	if err := s.SaveState(ctx, "fs-in", st); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	got, err := s.LoadState(ctx, "fs-in")
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(got))
	}
	if got["a/b.txt"].Size != 42 || got["a/b.txt"].Hash != "sha256:deadbeef" {
		t.Fatalf("unexpected entry: %+v", got["a/b.txt"])
	}
}

func TestLoadStateMissingFileIsEmpty(t *testing.T) {
	s := openTestStore(t)
	got, err := s.LoadState(context.Background(), "never-saved")
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty map for missing file, got %+v", got)
	}
}

func TestSyncLogInsertAndLookup(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if err := s.InsertSyncLog(ctx, SyncLogEntry{
		ChannelName:     "fs-to-aprimo",
		SourceConnector: "fs-in",
		SourcePath:      "hello.txt",
		SourceVersion:   "v1",
		DestID:  "rec-1",
		Kind:            SyncCreate,
	}); err != nil {
		t.Fatalf("InsertSyncLog: %v", err)
	}
	if err := s.InsertSyncLog(ctx, SyncLogEntry{
		ChannelName:     "fs-to-aprimo",
		SourceConnector: "fs-in",
		SourcePath:      "hello.txt",
		SourceVersion:   "v2",
		DestID:  "rec-1",
		Kind:            SyncUpdate,
	}); err != nil {
		t.Fatalf("InsertSyncLog #2: %v", err)
	}

	latest, err := s.LookupLatest(ctx, "fs-to-aprimo", "hello.txt")
	if err != nil {
		t.Fatalf("LookupLatest: %v", err)
	}
	if latest == nil {
		t.Fatal("expected entry")
	}
	if latest.SourceVersion != "v2" || latest.Kind != SyncUpdate {
		t.Fatalf("expected latest v2/update, got %+v", latest)
	}
}

func TestSyncLogBatchLookup(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	for i, p := range []string{"a", "b", "c"} {
		if err := s.InsertSyncLog(ctx, SyncLogEntry{
			ChannelName:     "ch",
			SourceConnector: "src",
			SourcePath:      p,
			SourceVersion:   "v1",
			DestID:  string(rune('R' + i)),
			Kind:            SyncCreate,
		}); err != nil {
			t.Fatalf("Insert %s: %v", p, err)
		}
	}

	got, err := s.LookupLatestBatch(ctx, "ch", []string{"a", "b", "c", "missing"})
	if err != nil {
		t.Fatalf("LookupLatestBatch: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 matched, got %d (%+v)", len(got), got)
	}
	if got["a"].DestID != "R" || got["c"].DestID != "T" {
		t.Fatalf("unexpected results: %+v", got)
	}
	if _, ok := got["missing"]; ok {
		t.Fatal("missing path should not be in results")
	}
}
