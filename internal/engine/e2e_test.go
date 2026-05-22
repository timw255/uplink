package engine

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	aprimosdk "github.com/timw255/uplink/internal/aprimo"
	"github.com/timw255/uplink/internal/channel"
	aprimoconn "github.com/timw255/uplink/internal/connectors/aprimo"
	"github.com/timw255/uplink/internal/connectors/localfs"
	"github.com/timw255/uplink/internal/store"
)

// fakeAprimoServer is a tiny httptest backend that responds to the
// uploader + records endpoints the connector hits. It tracks calls so
// e2e tests can assert on the recorded behavior.
type fakeAprimoServer struct {
	srv *httptest.Server

	mu      sync.Mutex
	creates int
	updates []string // record ids that received a PUT

	// nextRecordID seeds new record ids returned from Create. Each
	// Create returns a unique id so update routing can distinguish.
	nextRecordID int
}

func newFakeAprimoServer(t *testing.T) *fakeAprimoServer {
	t.Helper()
	f := &fakeAprimoServer{}
	mux := http.NewServeMux()
	mux.HandleFunc("/uploads/segments", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"uri":"` + f.srv.URL + `/upload/abc"}`))
	})
	mux.HandleFunc("/upload/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/commit") {
			_, _ = w.Write([]byte(`{"token":"tok-final"}`))
			return
		}
		// Segment POST — drain the body to be polite.
		_ = r.ParseMultipartForm(32 << 20)
		if r.MultipartForm != nil {
			for _, fhs := range r.MultipartForm.File {
				for _, fh := range fhs {
					if rc, err := fh.Open(); err == nil {
						_, _ = io.Copy(io.Discard, rc)
						_ = rc.Close()
					}
				}
			}
		}
	})
	mux.HandleFunc("/uploads", func(w http.ResponseWriter, _ *http.Request) {
		// Single-shot path. Same shape as a committed segmented upload.
		_, _ = w.Write([]byte(`{"token":"tok-final"}`))
	})
	mux.HandleFunc("/api/core/records", func(w http.ResponseWriter, _ *http.Request) {
		f.mu.Lock()
		f.creates++
		f.nextRecordID++
		id := "rec-e2e-" + itoa(f.nextRecordID)
		f.mu.Unlock()
		_, _ = w.Write([]byte(`{"id":"` + id + `"}`))
	})
	mux.HandleFunc("/api/core/record/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/api/core/record/")
		f.mu.Lock()
		if r.Method == http.MethodPut {
			f.updates = append(f.updates, id)
		}
		f.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeAprimoServer) createCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.creates
}

func (f *fakeAprimoServer) updateCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.updates)
}

// TestEngine_EndToEnd_LocalfsToAprimo wires real localfs +
// aprimo-connector against a fake Aprimo server, drops files into the
// source dir, drives one engine cycle, and asserts the system moved
// the bytes and recorded the right state. Then modifies a file and
// confirms the update flow produces a second sync_log row with
// kind=update on the same aprimo record id.
func TestEngine_EndToEnd_LocalfsToAprimo(t *testing.T) {
	dataDir := t.TempDir()
	srcDir := t.TempDir()
	ctx := context.Background()

	// File 1: small, will go through the single-shot upload.
	if err := os.WriteFile(filepath.Join(srcDir, "a.txt"), []byte("alpha"), 0o644); err != nil {
		t.Fatalf("seed a.txt: %v", err)
	}
	// File 2: a second one to confirm we handle batches.
	if err := os.WriteFile(filepath.Join(srcDir, "b.txt"), []byte("beta"), 0o644); err != nil {
		t.Fatalf("seed b.txt: %v", err)
	}

	st, err := store.Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	fakeAprimo := newFakeAprimoServer(t)
	fs := buildLocalfsConnector(t, "fs-in", srcDir)
	ap := buildAprimoConnector(t, "aprimo-prod", fakeAprimo, st)

	// Configure the channel to fire on both OnCreate and OnUpdate — the
	// test drops a file (Create) and then modifies it (Update), and
	// expects both to flow through to Aprimo via the same channel.
	reg, err := channel.NewRegistry([]channel.ChannelSpec{
		{
			Name:        "fs-to-aprimo",
			Source:      "fs-in",
			Destination: "aprimo-prod",
			Trigger: channel.TriggerSpec{
				Events: []string{"OnCreate", "OnUpdate"},
			},
		},
	}, nil)
	if err != nil {
		t.Fatalf("channel.NewRegistry: %v", err)
	}

	e := New(st, reg, NewStubConnectors(fs, ap), Options{
		Workers:     2,
		PollIdle:    10 * time.Millisecond,
		MaxAttempts: 3,
		BaseBackoff: 10 * time.Millisecond,
	})

	// Start one long-lived Subscribe goroutine and one long-lived
	// worker pool. Both run for the duration of the test; we drive
	// the system by mutating files on disk and waiting for sync_log
	// to reflect the changes.
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	pollDone := make(chan struct{})
	go func() {
		if err := fs.NewEventSource(st).Subscribe(runCtx, e); err != nil {
			t.Logf("Subscribe returned: %v", err)
		}
		close(pollDone)
	}()
	workerDone := make(chan struct{})
	go func() {
		_ = e.Run(runCtx)
		close(workerDone)
	}()

	// Phase 1: wait for both files to be synced.
	waitFor(t, 5*time.Second, func() bool {
		latestA, _ := st.LookupLatest(ctx, "fs-to-aprimo", "a.txt")
		latestB, _ := st.LookupLatest(ctx, "fs-to-aprimo", "b.txt")
		return latestA != nil && latestB != nil
	}, "both initial sync_log rows")

	if fakeAprimo.createCount() != 2 {
		t.Fatalf("expected 2 Create calls, got %d", fakeAprimo.createCount())
	}
	if fakeAprimo.updateCount() != 0 {
		t.Fatalf("expected 0 Update calls on create pass, got %d", fakeAprimo.updateCount())
	}

	// No leftover jobs or markers.
	waitFor(t, 2*time.Second, func() bool {
		pending, _ := st.ListJobs(ctx, store.StatusPending)
		running, _ := st.ListJobs(ctx, store.StatusRunning)
		markers, _ := st.ListMarkers()
		return len(pending) == 0 && len(running) == 0 && len(markers) == 0
	}, "drained jobs and markers after create pass")

	// Stash the record id we created for a.txt so we can assert the
	// update reuses it.
	first, err := st.LookupLatest(ctx, "fs-to-aprimo", "a.txt")
	if err != nil || first == nil {
		t.Fatalf("LookupLatest a.txt: %+v err=%v", first, err)
	}
	originalRecordID := first.DestID

	// Phase 2: modify a.txt. The poll loop should detect the change,
	// the engine should route as Update on the same record id, and
	// sync_log should gain a second row for a.txt with kind=update.
	time.Sleep(20 * time.Millisecond) // distinguishable mtime on coarse FS clocks
	if err := os.WriteFile(filepath.Join(srcDir, "a.txt"), []byte("alpha-v2"), 0o644); err != nil {
		t.Fatalf("modify a.txt: %v", err)
	}

	waitFor(t, 5*time.Second, func() bool {
		var n int
		_ = st.DB().QueryRow(`SELECT COUNT(*) FROM sync_log WHERE channel_name=? AND source_path=?`,
			"fs-to-aprimo", "a.txt").Scan(&n)
		return n >= 2
	}, "second sync_log row for a.txt")

	if got := fakeAprimo.updateCount(); got != 1 {
		t.Fatalf("expected 1 Update call on update pass, got %d", got)
	}
	fakeAprimo.mu.Lock()
	target := fakeAprimo.updates[0]
	fakeAprimo.mu.Unlock()
	if target != originalRecordID {
		t.Fatalf("Update targeted %q, want original record %q", target, originalRecordID)
	}

	runCancel()
	select {
	case <-pollDone:
	case <-time.After(2 * time.Second):
		t.Fatal("poll loop did not stop within 2s of cancel")
	}
	select {
	case <-workerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("worker pool did not stop within 2s of cancel")
	}
}

// waitFor polls predicate every 25ms until it returns true or timeout
// elapses. Fails the test on timeout with the supplied label.
func waitFor(t *testing.T, timeout time.Duration, predicate func() bool, label string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", label)
}

// buildLocalfsConnector constructs a real localfs connector rooted at
// srcDir for use as an event-emitting source.
func buildLocalfsConnector(t *testing.T, name, srcDir string) *localfs.Connector {
	t.Helper()
	c, err := localfs.New(name, localfs.Config{Root: srcDir, PollInterval: 100 * time.Millisecond})
	if err != nil {
		t.Fatalf("localfs.New: %v", err)
	}
	if err := c.Init(context.Background()); err != nil {
		t.Fatalf("localfs.Init: %v", err)
	}
	return c
}

// buildAprimoConnector constructs a real Aprimo connector pointed at
// the test server. Bypasses the Factory so we can override URLs and
// inject the test store.
func buildAprimoConnector(t *testing.T, name string, fake *fakeAprimoServer, st *store.Store) *aprimoconn.Connector {
	t.Helper()
	client, err := aprimosdk.New(aprimosdk.Config{
		Environment: "test",
		TokenProvider: func(context.Context) (string, error) {
			return "tok-e2e", nil
		},
		HTTPClient: fake.srv.Client(),
	})
	if err != nil {
		t.Fatalf("aprimo client: %v", err)
	}
	client.SetTestEndpoints(fake.srv.URL, fake.srv.URL)
	return aprimoconn.NewForTest(name, client, st)
}
