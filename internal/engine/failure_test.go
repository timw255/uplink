package engine

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/timw255/uplink/internal/channel"
	"github.com/timw255/uplink/internal/connector"
	"github.com/timw255/uplink/internal/store"
)

// failingStore is a StoreAPI decorator that lets a test inject
// targeted failures. Methods not overridden delegate to the embedded
// real store. Each Fail field is the number of calls that should
// return an error before subsequent calls succeed normally.
type failingStore struct {
	*store.Store

	failInsertSyncLog     atomic.Int32 // first N InsertSyncLog calls fail
	failLookupLatestBatch atomic.Int32 // first N LookupLatestBatch calls fail
}

var errInjected = errors.New("failingStore: injected error")

func (f *failingStore) InsertSyncLog(ctx context.Context, e store.SyncLogEntry) error {
	if f.failInsertSyncLog.Add(-1) >= 0 {
		return errInjected
	}
	return f.Store.InsertSyncLog(ctx, e)
}

func (f *failingStore) LookupLatestBatch(ctx context.Context, ch string, paths []string) (map[string]*store.SyncLogEntry, error) {
	if f.failLookupLatestBatch.Add(-1) >= 0 {
		return nil, errInjected
	}
	return f.Store.LookupLatestBatch(ctx, ch, paths)
}

// newEngineWithStore builds an Engine wired to a StoreAPI of the
// caller's choosing. Mirrors newRunnableEngine but lets us swap the
// store with a decorator.
func newEngineWithStore(
	t *testing.T,
	st StoreAPI,
	channels []channel.ChannelSpec,
	conns ...connector.Connector,
) *Engine {
	t.Helper()
	reg, err := channel.NewRegistry(channels, nil)
	if err != nil {
		t.Fatalf("channel.NewRegistry: %v", err)
	}
	return &Engine{
		store:       st,
		channels:    reg,
		connectors:  NewStubConnectors(conns...),
		logger:      slog.Default(),
		workers:     2,
		pollIdle:    10 * time.Millisecond,
		maxAttempts: 3,
		baseBackoff: 10 * time.Millisecond,
	}
}

// TestRunJob_RetriesWhenInsertSyncLogFails proves that a finalize-time
// failure on InsertSyncLog does NOT silently mark the job complete.
// Without the engine.finalize fix, the worker logged the InsertSyncLog
// error and then CompleteJob'd anyway — the sync_log row was lost.
// With the fix, the failure is routed through the retry path; once
// the injected failure is exhausted, the next attempt succeeds and
// exactly one sync_log row exists.
func TestRunJob_RetriesWhenInsertSyncLogFails(t *testing.T) {
	dir := t.TempDir()
	realStore, err := store.Open(context.Background(), dir)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = realStore.Close() })

	fs := &failingStore{Store: realStore}
	fs.failInsertSyncLog.Store(1) // fail the first attempt only

	src := &StubSource{NameStr: "fs-in", Files: map[string][]byte{
		"x.txt": []byte("hello"),
	}}
	dst := &StubDestination{NameStr: "aprimo-prod", ResponseRecordID: "rec-fin"}
	e := newEngineWithStore(t, fs,
		[]channel.ChannelSpec{simpleChannel("ch1", "fs-in", "aprimo-prod")},
		src, dst,
	)
	ctx := context.Background()

	if err := e.DispatchBatch(ctx, "fs-in", []connector.Event{{
		Connector: "fs-in", Kind: connector.EventCreate,
		Entry: connector.Entry{Path: "x.txt", Size: 5, Hash: "v1"},
	}}); err != nil {
		t.Fatalf("DispatchBatch: %v", err)
	}

	if !runUntil(t, e, 3*time.Second, func() bool {
		latest, _ := realStore.LookupLatest(ctx, "ch1", "x.txt")
		return latest != nil
	}) {
		t.Fatal("timed out waiting for retried sync_log insert to succeed")
	}

	// Exactly one sync_log row — the retry should NOT have written two.
	var count int
	if err := realStore.DB().QueryRow(
		`SELECT COUNT(*) FROM sync_log WHERE channel_name=? AND source_path=?`,
		"ch1", "x.txt").Scan(&count); err != nil {
		t.Fatalf("count sync_log: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 sync_log row after retry, got %d", count)
	}
	// Job drained — not stuck in failed/.
	failed, _ := realStore.ListJobs(ctx, store.StatusFailed)
	if len(failed) != 0 {
		t.Fatalf("expected no failed jobs, got %d", len(failed))
	}
}

// TestDispatchBatch_ContinuesOnPerChannelFailure asserts that when one
// channel's sync_log lookup fails, the remaining channels still get
// their events enqueued. Without the per-channel error collection,
// channel A's transient failure would silently drop channel B's
// events for the same batch.
func TestDispatchBatch_ContinuesOnPerChannelFailure(t *testing.T) {
	dir := t.TempDir()
	realStore, err := store.Open(context.Background(), dir)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = realStore.Close() })

	fs := &failingStore{Store: realStore}
	fs.failLookupLatestBatch.Store(1) // first channel's lookup fails

	e := newEngineWithStore(t, fs, []channel.ChannelSpec{
		simpleChannel("ch1", "fs-in", "aprimo-prod"),
		simpleChannel("ch2", "fs-in", "aprimo-prod"),
	})

	err = e.DispatchBatch(context.Background(), "fs-in", []connector.Event{{
		Connector: "fs-in", Kind: connector.EventCreate,
		Entry: connector.Entry{Path: "x.txt", Size: 1, Hash: "v1"},
	}})
	if err == nil {
		t.Fatal("expected combined error from channel 1, got nil")
	}
	if !errors.Is(err, errInjected) {
		t.Fatalf("expected injected error in chain, got: %v", err)
	}

	// Channel 2 must still have enqueued its job — that's the whole
	// point of the per-channel error collection.
	pending, _ := realStore.ListJobs(context.Background(), store.StatusPending)
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending job from ch2, got %d", len(pending))
	}
	if pending[0].ChannelName != "ch2" {
		t.Fatalf("expected pending job for ch2, got %s", pending[0].ChannelName)
	}
}

// TestDispatchBatch_ReturnsErrorOnLookupFailure asserts that a
// LookupLatestBatch failure prevents jobs from being enqueued and
// surfaces the error to the caller (the EventSource poll loop, which
// then decides what to do — typically retry the whole batch on the
// next poll).
func TestDispatchBatch_ReturnsErrorOnLookupFailure(t *testing.T) {
	dir := t.TempDir()
	realStore, err := store.Open(context.Background(), dir)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = realStore.Close() })

	fs := &failingStore{Store: realStore}
	fs.failLookupLatestBatch.Store(1)

	e := newEngineWithStore(t, fs,
		[]channel.ChannelSpec{simpleChannel("ch1", "fs-in", "aprimo-prod")},
	)

	err = e.DispatchBatch(context.Background(), "fs-in", []connector.Event{{
		Connector: "fs-in", Kind: connector.EventCreate,
		Entry: connector.Entry{Path: "a.txt", Size: 1, Hash: "v1"},
	}})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, errInjected) {
		t.Fatalf("expected injected error, got: %v", err)
	}

	// No jobs got enqueued.
	pending, _ := realStore.ListJobs(context.Background(), store.StatusPending)
	if len(pending) != 0 {
		t.Fatalf("expected 0 pending jobs, got %d", len(pending))
	}
}
