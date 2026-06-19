package aprimo

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	aprimosdk "github.com/timw255/uplink/internal/aprimo"
)

// TestConnector_BackgroundRefreshSwapsResolver confirms that with
// RefreshInterval > 0, the connector spins up a goroutine that
// periodically rebuilds the catalogs and atomically replaces the
// resolver. New fields added in Aprimo become visible without a
// daemon restart — and the swap is atomic so in-flight reads always
// see a consistent snapshot.
func TestConnector_BackgroundRefreshSwapsResolver(t *testing.T) {
	var calls atomic.Int32 // counts /api/core/fielddefinitions hits

	mux := http.NewServeMux()
	// Field definitions: counts each call so we can confirm refresh
	// fires. Returns "Caption" on call 1, "Caption" + "Subtitle" on
	// every subsequent call — simulating a field being added.
	mux.HandleFunc("/api/core/fielddefinitions", func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		w.Header().Set("Content-Type", "application/hal+json")
		if n == 1 {
			fmt.Fprintln(w, `{
				"items": [{"id": "fld-caption", "name": "Caption", "dataType": "singlelinetext"}],
				"_links": {"self": {"href": "/api/core/fielddefinitions"}}
			}`)
			return
		}
		fmt.Fprintln(w, `{
			"items": [
				{"id": "fld-caption",  "name": "Caption",  "dataType": "singlelinetext"},
				{"id": "fld-subtitle", "name": "Subtitle", "dataType": "singlelinetext"}
			],
			"_links": {"self": {"href": "/api/core/fielddefinitions"}}
		}`)
	})
	// Languages: simple static response.
	mux.HandleFunc("/api/core/languages", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/hal+json")
		fmt.Fprintln(w, `{
			"items": [{"id": "lang-en", "culture": "en-US", "name": "English", "isEnabledForFields": true}],
			"_links": {"self": {"href": "/api/core/languages"}}
		}`)
	})
	// Empty list for the remaining catalogs — they're not under test
	// here but Init walks them all.
	emptyList := func(self string) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/hal+json")
			fmt.Fprintf(w, `{"items": [], "_links": {"self": {"href": %q}}}`+"\n", self)
		}
	}
	mux.HandleFunc("/api/core/classifications", emptyList("/api/core/classifications"))
	mux.HandleFunc("/api/core/users", emptyList("/api/core/users"))
	mux.HandleFunc("/api/core/usergroups", emptyList("/api/core/usergroups"))

	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Build a client pointed at the fake server.
	client, err := aprimosdk.New(aprimosdk.Config{
		Environment:   "test",
		TokenProvider: func(_ context.Context) (string, error) { return "tok", nil },
	})
	if err != nil {
		t.Fatalf("aprimo.New: %v", err)
	}
	client.SetTestEndpoints(srv.URL, srv.URL)

	c := &Connector{
		name: "trial",
		cfg: &Config{
			DefaultStatus:   "draft",
			DefaultLanguage: "en-US",
			RefreshInterval: 60 * time.Millisecond,
		},
		api: client,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	// After Init: one fetch happened.
	if got := calls.Load(); got != 1 {
		t.Fatalf("after Init: calls = %d, want 1", got)
	}
	r1 := c.resolver.Load()
	if _, ok := r1.fieldsByName["caption"]; !ok {
		t.Fatalf("initial resolver missing 'caption'")
	}
	if _, ok := r1.fieldsByName["subtitle"]; ok {
		t.Fatalf("initial resolver should NOT have 'subtitle'")
	}

	// Wait for at least one refresh tick. Poll the resolver pointer
	// instead of sleeping a fixed duration — robust to scheduler jitter.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		r2 := c.resolver.Load()
		if r2 != r1 {
			if _, ok := r2.fieldsByName["subtitle"]; ok {
				return // success
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("resolver was not refreshed within deadline; calls=%d", calls.Load())
}

// TestConnector_RefreshIntervalZeroDisablesGoroutine confirms that
// setting refresh_interval to 0 skips the background loop entirely —
// no goroutine, no extra API calls past Init.
func TestConnector_RefreshIntervalZeroDisablesGoroutine(t *testing.T) {
	var calls atomic.Int32

	mux := http.NewServeMux()
	mux.HandleFunc("/api/core/fielddefinitions", func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/hal+json")
		fmt.Fprintln(w, `{"items": [], "_links": {"self": {"href": "/api/core/fielddefinitions"}}}`)
	})
	emptyList := func(self string) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/hal+json")
			fmt.Fprintf(w, `{"items": [], "_links": {"self": {"href": %q}}}`+"\n", self)
		}
	}
	mux.HandleFunc("/api/core/languages", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/hal+json")
		fmt.Fprintln(w, `{"items": [{"id": "lang-en", "culture": "en-US", "name": "English"}], "_links": {"self": {"href": "/api/core/languages"}}}`)
	})
	mux.HandleFunc("/api/core/classifications", emptyList("/api/core/classifications"))
	mux.HandleFunc("/api/core/users", emptyList("/api/core/users"))
	mux.HandleFunc("/api/core/usergroups", emptyList("/api/core/usergroups"))

	srv := httptest.NewServer(mux)
	defer srv.Close()

	client, _ := aprimosdk.New(aprimosdk.Config{
		Environment:   "test",
		TokenProvider: func(_ context.Context) (string, error) { return "tok", nil },
	})
	client.SetTestEndpoints(srv.URL, srv.URL)

	c := &Connector{
		name: "trial",
		cfg: &Config{
			DefaultStatus:   "draft",
			DefaultLanguage: "en-US",
			RefreshInterval: 0, // disabled
		},
		api: client,
	}

	if err := c.Init(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	defer func() { _ = c.Close() }()

	time.Sleep(150 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Errorf("with refresh disabled, expected 1 call; got %d", got)
	}
	if c.refreshDone != nil {
		t.Errorf("refreshDone should be nil when RefreshInterval=0")
	}
}
