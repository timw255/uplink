package engine

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/timw255/uplink/internal/channel"
	"github.com/timw255/uplink/internal/connector"
	"github.com/timw255/uplink/internal/store"
)

// newTestEngine wires a real store + channel registry + stub
// connectors together for a single test. workers=0 means no
// background worker loop — tests drive Dispatch and runJob directly.
func newTestEngine(
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
	e := New(st, reg, NewStubConnectors(conns...), Options{Workers: 0})
	return e, st
}

// simpleChannel returns a ChannelSpec with the given source/dest and
// an OnCreate+OnUpdate trigger. CEL filter is empty (always-match).
func simpleChannel(name, src, dst string) channel.ChannelSpec {
	return channel.ChannelSpec{
		Name:        name,
		Source:      src,
		Destination: dst,
		Trigger:     channel.TriggerSpec{Event: string(connector.EventCreate)},
	}
}

func TestDispatchBatch_EnqueuesNewEvents(t *testing.T) {
	src := &StubSource{NameStr: "fs-in", Files: map[string][]byte{
		"foo.txt": []byte("hello"),
		"bar.txt": []byte("world"),
	}}
	dst := &StubDestination{NameStr: "aprimo-prod"}
	e, st := newTestEngine(t,
		[]channel.ChannelSpec{simpleChannel("ch1", "fs-in", "aprimo-prod")},
		src, dst,
	)

	events := []connector.Event{
		{Connector: "fs-in", Kind: connector.EventCreate,
			Entry: connector.Entry{Path: "foo.txt", Size: 5, Hash: "h-5-deadbeef"}},
		{Connector: "fs-in", Kind: connector.EventCreate,
			Entry: connector.Entry{Path: "bar.txt", Size: 5, Hash: "h-5-cafebabe"}},
	}
	if err := e.DispatchBatch(context.Background(), "fs-in", events); err != nil {
		t.Fatalf("DispatchBatch: %v", err)
	}

	pending, err := st.ListJobs(context.Background(), store.StatusPending)
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("expected 2 pending jobs, got %d", len(pending))
	}
	for _, j := range pending {
		if j.ChannelName != "ch1" {
			t.Fatalf("unexpected channel on job: %+v", j)
		}
		if j.DestID != "" {
			t.Fatalf("expected empty dest_id (no prior sync) on %s, got %q", j.SourcePath, j.DestID)
		}
	}
}

func TestDispatchBatch_SyncLogDedupSkipsUnchanged(t *testing.T) {
	src := &StubSource{NameStr: "fs-in"}
	dst := &StubDestination{NameStr: "aprimo-prod"}
	e, st := newTestEngine(t,
		[]channel.ChannelSpec{simpleChannel("ch1", "fs-in", "aprimo-prod")},
		src, dst,
	)

	// Pre-seed sync_log with the exact version we're about to send.
	if err := st.InsertSyncLog(context.Background(), store.SyncLogEntry{
		ChannelName:     "ch1",
		SourceConnector: "fs-in",
		SourcePath:      "foo.txt",
		SourceVersion:   "v1",
		DestID:          "rec-1",
		Kind:            store.SyncCreate,
	}); err != nil {
		t.Fatalf("seed sync_log: %v", err)
	}

	events := []connector.Event{
		{Connector: "fs-in", Kind: connector.EventCreate,
			Entry: connector.Entry{Path: "foo.txt", Size: 5, Hash: "v1"}},
	}
	if err := e.DispatchBatch(context.Background(), "fs-in", events); err != nil {
		t.Fatalf("DispatchBatch: %v", err)
	}

	pending, _ := st.ListJobs(context.Background(), store.StatusPending)
	if len(pending) != 0 {
		t.Fatalf("expected 0 jobs (already synced at same version), got %d", len(pending))
	}
}

func TestDispatchBatch_DifferentVersionRoutesAsUpdate(t *testing.T) {
	src := &StubSource{NameStr: "fs-in"}
	dst := &StubDestination{NameStr: "aprimo-prod"}
	e, st := newTestEngine(t,
		[]channel.ChannelSpec{simpleChannel("ch1", "fs-in", "aprimo-prod")},
		src, dst,
	)

	if err := st.InsertSyncLog(context.Background(), store.SyncLogEntry{
		ChannelName:     "ch1",
		SourceConnector: "fs-in",
		SourcePath:      "foo.txt",
		SourceVersion:   "v1",
		DestID:          "rec-X",
		Kind:            store.SyncCreate,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	events := []connector.Event{
		{Connector: "fs-in", Kind: connector.EventCreate,
			Entry: connector.Entry{Path: "foo.txt", Size: 5, Hash: "v2"}},
	}
	if err := e.DispatchBatch(context.Background(), "fs-in", events); err != nil {
		t.Fatalf("DispatchBatch: %v", err)
	}

	pending, _ := st.ListJobs(context.Background(), store.StatusPending)
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending job, got %d", len(pending))
	}
	if pending[0].DestID != "rec-X" {
		t.Fatalf("expected job to carry prior record id 'rec-X', got %q", pending[0].DestID)
	}
	if pending[0].SourceVersion != "v2" {
		t.Fatalf("expected new version 'v2', got %q", pending[0].SourceVersion)
	}
}

func TestDispatchBatch_DropsOnDeleteEvents(t *testing.T) {
	src := &StubSource{NameStr: "fs-in"}
	dst := &StubDestination{NameStr: "aprimo-prod"}
	e, st := newTestEngine(t,
		[]channel.ChannelSpec{simpleChannel("ch1", "fs-in", "aprimo-prod")},
		src, dst,
	)

	events := []connector.Event{
		{Connector: "fs-in", Kind: connector.EventDelete,
			Entry: connector.Entry{Path: "gone.txt"}},
	}
	if err := e.DispatchBatch(context.Background(), "fs-in", events); err != nil {
		t.Fatalf("DispatchBatch: %v", err)
	}

	pending, _ := st.ListJobs(context.Background(), store.StatusPending)
	if len(pending) != 0 {
		t.Fatalf("expected 0 jobs (deletes are dropped), got %d", len(pending))
	}
}

func TestDispatchBatch_NoMatchingChannelIsNoop(t *testing.T) {
	src := &StubSource{NameStr: "fs-in"}
	dst := &StubDestination{NameStr: "aprimo-prod"}
	e, st := newTestEngine(t,
		[]channel.ChannelSpec{simpleChannel("ch1", "fs-in", "aprimo-prod")},
		src, dst,
	)

	// Source name doesn't match any channel — should be a quiet no-op.
	events := []connector.Event{
		{Connector: "no-such-connector", Kind: connector.EventCreate,
			Entry: connector.Entry{Path: "x", Hash: "h"}},
	}
	if err := e.DispatchBatch(context.Background(), "no-such-connector", events); err != nil {
		t.Fatalf("DispatchBatch: %v", err)
	}
	pending, _ := st.ListJobs(context.Background(), store.StatusPending)
	if len(pending) != 0 {
		t.Fatalf("expected 0 jobs, got %d", len(pending))
	}
}

func TestDispatchBatch_FilteredByCEL(t *testing.T) {
	src := &StubSource{NameStr: "fs-in"}
	dst := &StubDestination{NameStr: "aprimo-prod"}
	spec := simpleChannel("ch1", "fs-in", "aprimo-prod")
	spec.Trigger.Filter = `size > 100`
	e, st := newTestEngine(t,
		[]channel.ChannelSpec{spec},
		src, dst,
	)

	events := []connector.Event{
		{Connector: "fs-in", Kind: connector.EventCreate,
			Entry: connector.Entry{Path: "tiny.txt", Size: 5, Hash: "h-tiny"}},
		{Connector: "fs-in", Kind: connector.EventCreate,
			Entry: connector.Entry{Path: "big.bin", Size: 1024, Hash: "h-big"}},
	}
	if err := e.DispatchBatch(context.Background(), "fs-in", events); err != nil {
		t.Fatalf("DispatchBatch: %v", err)
	}

	pending, _ := st.ListJobs(context.Background(), store.StatusPending)
	if len(pending) != 1 {
		t.Fatalf("expected 1 job (filter size>100), got %d", len(pending))
	}
	if pending[0].SourcePath != "big.bin" {
		t.Fatalf("expected big.bin to pass the filter, got %s", pending[0].SourcePath)
	}
}

// TestDispatchBatch_LargerThanChunkSize confirms the batched lookup
// returns correct results for inputs larger than the 500-row chunk
// limit. We seed sync_log with 600 already-synced paths and send 601
// events; 600 should be deduped, only 1 should enqueue.
func TestDispatchBatch_LargerThanChunkSize(t *testing.T) {
	src := &StubSource{NameStr: "fs-in"}
	dst := &StubDestination{NameStr: "aprimo-prod"}
	e, st := newTestEngine(t,
		[]channel.ChannelSpec{simpleChannel("ch1", "fs-in", "aprimo-prod")},
		src, dst,
	)

	ctx := context.Background()
	const seeded = 600
	for i := range seeded {
		path := pathID(i)
		if err := st.InsertSyncLog(ctx, store.SyncLogEntry{
			ChannelName:     "ch1",
			SourceConnector: "fs-in",
			SourcePath:      path,
			SourceVersion:   "v1",
			DestID:          "rec-" + path,
			Kind:            store.SyncCreate,
		}); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}

	events := make([]connector.Event, 0, seeded+1)
	for i := range seeded {
		events = append(events, connector.Event{
			Connector: "fs-in",
			Kind:      connector.EventCreate,
			Entry:     connector.Entry{Path: pathID(i), Size: 1, Hash: "v1"}, // same version
		})
	}
	events = append(events, connector.Event{
		Connector: "fs-in",
		Kind:      connector.EventCreate,
		Entry:     connector.Entry{Path: "fresh.txt", Size: 1, Hash: "v1"}, // new path
	})

	if err := e.DispatchBatch(ctx, "fs-in", events); err != nil {
		t.Fatalf("DispatchBatch: %v", err)
	}

	pending, _ := st.ListJobs(ctx, store.StatusPending)
	if len(pending) != 1 {
		t.Fatalf("expected exactly 1 fresh job, got %d", len(pending))
	}
	if pending[0].SourcePath != "fresh.txt" {
		t.Fatalf("expected the fresh path to enqueue, got %s", pending[0].SourcePath)
	}
}

func pathID(i int) string {
	// Use a path that exists across SQLite IN(...) chunking boundaries.
	return "f/" + filepath.Join(strings.Repeat("a", 0), itoa(i)) + ".bin"
}

// itoa avoids importing strconv just for this helper. Used in tests.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
