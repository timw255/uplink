package engine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/timw255/uplink/internal/adaptive"
	"github.com/timw255/uplink/internal/channel"
	"github.com/timw255/uplink/internal/connector"
	"github.com/timw255/uplink/internal/store"
)

// newAdaptiveEngine wires an engine whose worker pool scales via the
// adaptive Gate + Controller, with a fast controller tick so the resize
// path is actually exercised inside a short test.
func newAdaptiveEngine(
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
		Workers:        2, // ignored on the adaptive path
		PollIdle:       5 * time.Millisecond,
		MaxAttempts:    3,
		BaseBackoff:    5 * time.Millisecond,
		MaxWorkers:     8,
		TargetRPS:      100,
		Metrics:        &adaptive.Metrics{},
		ControllerTick: 15 * time.Millisecond,
	})
	return e, st
}

// TestAdaptivePoolProcessesAllJobs verifies the adaptive worker pool
// drains a backlog correctly: every job runs exactly once, nothing is
// dropped by the gate, and the queue empties. The controller is resizing
// the pool live throughout (fast tick), so this also shakes out the
// gate-resize-under-load path.
func TestAdaptivePoolProcessesAllJobs(t *testing.T) {
	const n = 25
	files := make(map[string][]byte, n)
	events := make([]connector.Event, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("file-%02d.txt", i)
		files[name] = []byte(fmt.Sprintf("content-%d", i))
		events[i] = connector.Event{
			Connector: "fs-in", Kind: connector.EventCreate,
			Entry: connector.Entry{Path: name, Size: int64(len(files[name])), Hash: fmt.Sprintf("v%d", i)},
		}
	}

	src := &StubSource{NameStr: "fs-in", Files: files}
	dst := &StubDestination{NameStr: "aprimo-prod"}
	e, st := newAdaptiveEngine(t,
		[]channel.ChannelSpec{simpleChannel("ch1", "fs-in", "aprimo-prod")},
		src, dst,
	)

	if e.gate != nil {
		t.Fatal("gate should be nil until Run starts")
	}
	if !e.adaptiveEnabled() {
		t.Fatal("expected adaptive mode to be enabled")
	}

	ctx := context.Background()
	if err := e.DispatchBatch(ctx, "fs-in", events); err != nil {
		t.Fatalf("DispatchBatch: %v", err)
	}

	ok := runUntil(t, e, 5*time.Second, func() bool {
		return len(dst.Writes()) >= n
	})
	if !ok {
		t.Fatalf("timed out: only %d of %d jobs processed", len(dst.Writes()), n)
	}

	// Exactly n writes (no double-processing), and the queue fully drained.
	if got := len(dst.Writes()); got != n {
		t.Fatalf("expected exactly %d writes, got %d", n, got)
	}
	pending, _ := st.ListJobs(ctx, store.StatusPending)
	running, _ := st.ListJobs(ctx, store.StatusRunning)
	failed, _ := st.ListJobs(ctx, store.StatusFailed)
	if len(pending)+len(running)+len(failed) != 0 {
		t.Fatalf("jobs left over: pending=%d running=%d failed=%d", len(pending), len(running), len(failed))
	}
}
