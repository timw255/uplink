package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/timw255/uplink/internal/channel"
	"github.com/timw255/uplink/internal/connector"
	"github.com/timw255/uplink/internal/store"
)

// newRunnableEngine returns an engine wired for actually running its
// worker loop in a test: short poll, tiny backoff, low max attempts.
func newRunnableEngine(
	t *testing.T,
	channels []channel.ChannelSpec,
	conns ...connector.Connector,
) (*Engine, *store.Store) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(context.Background(), dir)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	reg, err := channel.NewRegistry(channels, nil)
	if err != nil {
		t.Fatalf("channel.NewRegistry: %v", err)
	}
	e := New(st, reg, NewStubConnectors(conns...), Options{
		Workers:     2,
		PollIdle:    10 * time.Millisecond,
		MaxAttempts: 3,
		BaseBackoff: 10 * time.Millisecond,
	})
	return e, st
}

// runUntil starts the engine in a goroutine and stops it once
// predicate returns true or the timeout elapses. Returns true if
// predicate fired, false on timeout.
func runUntil(t *testing.T, e *Engine, timeout time.Duration, predicate func() bool) bool {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = e.Run(ctx)
		close(done)
	}()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if predicate() {
			cancel()
			<-done
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
	return false
}

func TestWorkerLoop_HappyPath(t *testing.T) {
	src := &StubSource{NameStr: "fs-in", Files: map[string][]byte{
		"hello.txt": []byte("hello"),
	}}
	dst := &StubDestination{NameStr: "aprimo-prod", ResponseRecordID: "rec-1"}
	e, st := newRunnableEngine(t,
		[]channel.ChannelSpec{simpleChannel("ch1", "fs-in", "aprimo-prod")},
		src, dst,
	)

	ctx := context.Background()
	if err := e.DispatchBatch(ctx, "fs-in", []connector.Event{{
		Connector: "fs-in", Kind: connector.EventCreate,
		Entry: connector.Entry{Path: "hello.txt", Size: 5, Hash: "v1"},
	}}); err != nil {
		t.Fatalf("DispatchBatch: %v", err)
	}

	if !runUntil(t, e, 2*time.Second, func() bool {
		latest, _ := st.LookupLatest(ctx, "ch1", "hello.txt")
		return latest != nil
	}) {
		t.Fatal("timed out waiting for sync_log row")
	}

	// Confirm: one sync_log row with the right shape, no leftover job,
	// no leftover marker (no marker was ever created because the stub
	// doesn't go through the Aprimo connector's marker code path).
	latest, _ := st.LookupLatest(ctx, "ch1", "hello.txt")
	if latest == nil || latest.DestID != "rec-1" || latest.Kind != store.SyncCreate {
		t.Fatalf("unexpected sync_log row: %+v", latest)
	}
	pending, _ := st.ListJobs(ctx, store.StatusPending)
	running, _ := st.ListJobs(ctx, store.StatusRunning)
	failed, _ := st.ListJobs(ctx, store.StatusFailed)
	if len(pending) != 0 || len(running) != 0 || len(failed) != 0 {
		t.Fatalf("expected zero jobs left, got pending=%d running=%d failed=%d", len(pending), len(running), len(failed))
	}
	// The destination should have seen exactly one Write call.
	if got := dst.Writes(); len(got) != 1 {
		t.Fatalf("expected 1 Write call, got %d", len(got))
	}
}

func TestWorkerLoop_RetryThenSucceed(t *testing.T) {
	src := &StubSource{NameStr: "fs-in", Files: map[string][]byte{
		"flaky.txt": []byte("hi"),
	}}
	dst := &StubDestination{
		NameStr:          "aprimo-prod",
		FailNTimes:       2, // first 2 attempts fail, 3rd succeeds
		ResponseRecordID: "rec-2",
	}
	e, st := newRunnableEngine(t,
		[]channel.ChannelSpec{simpleChannel("ch1", "fs-in", "aprimo-prod")},
		src, dst,
	)
	ctx := context.Background()

	if err := e.DispatchBatch(ctx, "fs-in", []connector.Event{{
		Connector: "fs-in", Kind: connector.EventCreate,
		Entry: connector.Entry{Path: "flaky.txt", Size: 2, Hash: "v1"},
	}}); err != nil {
		t.Fatalf("DispatchBatch: %v", err)
	}

	if !runUntil(t, e, 3*time.Second, func() bool {
		latest, _ := st.LookupLatest(ctx, "ch1", "flaky.txt")
		return latest != nil
	}) {
		t.Fatal("timed out waiting for retry success")
	}

	latest, _ := st.LookupLatest(ctx, "ch1", "flaky.txt")
	if latest == nil || latest.DestID != "rec-2" {
		t.Fatalf("unexpected sync_log row: %+v", latest)
	}
	if calls := dst.Writes(); len(calls) != 1 {
		t.Fatalf("expected 1 successful Write call recorded, got %d", len(calls))
	}
	// Failure count: failed.Add(1) ran on every attempt, so after 3 attempts it's 3.
	if got := dst.failed.Load(); got != 3 {
		t.Fatalf("expected 3 total Write attempts, got %d", got)
	}
	pending, _ := st.ListJobs(ctx, store.StatusPending)
	running, _ := st.ListJobs(ctx, store.StatusRunning)
	failed, _ := st.ListJobs(ctx, store.StatusFailed)
	if len(pending) != 0 || len(running) != 0 || len(failed) != 0 {
		t.Fatalf("expected zero jobs left, got pending=%d running=%d failed=%d", len(pending), len(running), len(failed))
	}
}

func TestWorkerLoop_MaxAttemptsExceededLandsInFailed(t *testing.T) {
	src := &StubSource{NameStr: "fs-in", Files: map[string][]byte{
		"doomed.txt": []byte("x"),
	}}
	// FailNTimes is huge — every attempt fails.
	dst := &StubDestination{NameStr: "aprimo-prod", FailNTimes: 100}
	e, st := newRunnableEngine(t,
		[]channel.ChannelSpec{simpleChannel("ch1", "fs-in", "aprimo-prod")},
		src, dst,
	)
	ctx := context.Background()

	if err := e.DispatchBatch(ctx, "fs-in", []connector.Event{{
		Connector: "fs-in", Kind: connector.EventCreate,
		Entry: connector.Entry{Path: "doomed.txt", Size: 1, Hash: "v1"},
	}}); err != nil {
		t.Fatalf("DispatchBatch: %v", err)
	}

	if !runUntil(t, e, 3*time.Second, func() bool {
		failed, _ := st.ListJobs(ctx, store.StatusFailed)
		return len(failed) == 1
	}) {
		t.Fatal("timed out waiting for job to land in failed/")
	}

	// No sync_log row, one failed file with a non-empty LastError.
	latest, _ := st.LookupLatest(ctx, "ch1", "doomed.txt")
	if latest != nil {
		t.Fatalf("expected no sync_log row, got %+v", latest)
	}
	failed, _ := st.ListJobs(ctx, store.StatusFailed)
	if len(failed) != 1 {
		t.Fatalf("expected 1 failed job, got %d", len(failed))
	}
	if failed[0].LastError == "" {
		t.Fatal("expected LastError to be populated")
	}
}

func TestWorkerLoop_IdempotentSyncLogInsert(t *testing.T) {
	src := &StubSource{NameStr: "fs-in", Files: map[string][]byte{
		"resumed.txt": []byte("z"),
	}}
	// The destination returns the same record id every time; we
	// pre-seed sync_log with a row that already matches what the
	// worker would write. The engine's LookupLatest-before-Insert
	// guard should detect the match and skip the duplicate.
	dst := &StubDestination{NameStr: "aprimo-prod", ResponseRecordID: "rec-X"}
	e, st := newRunnableEngine(t,
		[]channel.ChannelSpec{simpleChannel("ch1", "fs-in", "aprimo-prod")},
		src, dst,
	)
	ctx := context.Background()

	if err := st.InsertSyncLog(ctx, store.SyncLogEntry{
		ChannelName:     "ch1",
		SourceConnector: "fs-in",
		SourcePath:      "resumed.txt",
		SourceVersion:   "v1",
		DestID:  "rec-X",
		Kind:            store.SyncCreate,
	}); err != nil {
		t.Fatalf("seed sync_log: %v", err)
	}

	// Enqueue a job directly — we want to bypass the DispatchBatch
	// dedup so we exercise the post-Write idempotency check.
	if _, err := st.EnqueueJob(ctx, store.Job{
		ChannelName:     "ch1",
		Kind:            string(connector.EventCreate),
		SourceConnector: "fs-in",
		SourcePath:      "resumed.txt",
		SourceVersion:   "v1",
	}); err != nil {
		t.Fatalf("EnqueueJob: %v", err)
	}

	if !runUntil(t, e, 2*time.Second, func() bool {
		pending, _ := st.ListJobs(ctx, store.StatusPending)
		running, _ := st.ListJobs(ctx, store.StatusRunning)
		return len(pending) == 0 && len(running) == 0
	}) {
		t.Fatal("timed out waiting for job to drain")
	}

	// Exactly one sync_log row — the seeded one — even though the
	// worker would have inserted a duplicate without the idempotency
	// guard.
	var count int
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM sync_log WHERE channel_name=? AND source_path=?`,
		"ch1", "resumed.txt").Scan(&count); err != nil {
		t.Fatalf("count sync_log: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 sync_log row (idempotent insert), got %d", count)
	}
}

func TestWorkerLoop_CancellationStopsAllWorkers(t *testing.T) {
	src := &StubSource{NameStr: "fs-in", Files: map[string][]byte{}}
	dst := &StubDestination{NameStr: "aprimo-prod"}
	e, _ := newRunnableEngine(t,
		[]channel.ChannelSpec{simpleChannel("ch1", "fs-in", "aprimo-prod")},
		src, dst,
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()

	// Let workers spin up briefly, then cancel.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("workers failed to exit within 2s of cancel")
	}
}
