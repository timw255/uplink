package aprimo

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// makeJWT builds a synthetic unsigned JWT with the given exp claim.
// We never validate the signature in our client — only the exp claim.
func makeJWT(exp int64) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`))
	payload, _ := json.Marshal(map[string]any{"exp": exp})
	return header + "." + base64.RawURLEncoding.EncodeToString(payload) + ".sig"
}

func TestClientCredentialsTokenProvider(t *testing.T) {
	var called int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&called, 1)
		if got := r.FormValue("grant_type"); got != "client_credentials" {
			t.Errorf("grant_type = %q", got)
		}
		if got := r.FormValue("client_id"); got != "ID" {
			t.Errorf("client_id = %q", got)
		}
		if got := r.FormValue("client_secret"); got != "SECRET" {
			t.Errorf("client_secret = %q", got)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintln(w, `{"access_token":"tok-1","expires_in":3600}`)
	}))
	defer srv.Close()

	tp := ClientCredentialsTokenProvider(srv.Client(), srv.URL, "ID", "SECRET")
	tok, err := tp(context.Background())
	if err != nil {
		t.Fatalf("tp: %v", err)
	}
	if tok != "tok-1" {
		t.Fatalf("token = %q", tok)
	}
	if atomic.LoadInt32(&called) != 1 {
		t.Fatalf("expected 1 call")
	}
}

func TestCachedTokenProviderCachesUntilExpiry(t *testing.T) {
	var called int32
	inner := func(_ context.Context) (string, error) {
		atomic.AddInt32(&called, 1)
		// far-future expiry
		return makeJWT(time.Now().Add(1 * time.Hour).Unix()), nil
	}
	cp := CachedTokenProvider(inner)

	for i := 0; i < 5; i++ {
		if _, err := cp(context.Background()); err != nil {
			t.Fatalf("cp: %v", err)
		}
	}
	if got := atomic.LoadInt32(&called); got != 1 {
		t.Fatalf("inner called %d times, expected 1", got)
	}
}

func TestCachedTokenProviderRefreshesOnExpiry(t *testing.T) {
	var called int32
	inner := func(_ context.Context) (string, error) {
		n := atomic.AddInt32(&called, 1)
		// First call: expiry inside the 30s refresh-skew window, so the
		// next cached check will deem the token stale and refresh.
		// Second call: an hour out, easily cacheable.
		var exp time.Time
		if n == 1 {
			exp = time.Now().Add(5 * time.Second)
		} else {
			exp = time.Now().Add(1 * time.Hour)
		}
		return makeJWT(exp.Unix()), nil
	}
	cp := CachedTokenProvider(inner)

	if _, err := cp(context.Background()); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if _, err := cp(context.Background()); err != nil {
		t.Fatalf("second call: %v", err)
	}
	if got := atomic.LoadInt32(&called); got != 2 {
		t.Fatalf("inner called %d times, expected 2", got)
	}
}

// TestCachedTokenProviderPastExpDoesNotThrash covers the defensive
// path: if Aprimo (or a clock-skewed environment) hands us a token
// whose exp is already in the past, we treat it like a token with no
// expiry information rather than refreshing on every call.
func TestCachedTokenProviderPastExpDoesNotThrash(t *testing.T) {
	var called int32
	inner := func(_ context.Context) (string, error) {
		atomic.AddInt32(&called, 1)
		return makeJWT(time.Now().Add(-1 * time.Hour).Unix()), nil
	}
	cp := CachedTokenProvider(inner)

	for i := 0; i < 5; i++ {
		if _, err := cp(context.Background()); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	if got := atomic.LoadInt32(&called); got != 1 {
		t.Fatalf("inner called %d times for a token with past exp; want 1 (fallback TTL should keep it cached)", got)
	}
}
