package aprimo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// stubAuth always returns the same token; useful for tests that don't
// care about the auth flow.
func stubAuth(token string) TokenProvider {
	return func(_ context.Context) (string, error) { return token, nil }
}

func newTestRequester(t *testing.T, handler http.HandlerFunc, headers map[string]string) (*requester, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	// Default to no retries so tests that exercise terminal-error paths
	// run fast. Retry-path tests set maxRetries explicitly.
	return &requester{
		client:     srv.Client(),
		auth:       stubAuth("tok-123"),
		baseURL:    srv.URL,
		headers:    headers,
		maxRetries: 0,
	}, srv
}

func TestRequesterAttachesBearerAndHeaders(t *testing.T) {
	r, _ := newTestRequester(t, func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer tok-123" {
			t.Errorf("Authorization = %q", got)
		}
		if got := r.Header.Get("API-VERSION"); got != "1" {
			t.Errorf("API-VERSION = %q", got)
		}
		if got := r.Header.Get("Accept"); got != "application/hal+json" {
			t.Errorf("Accept = %q", got)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"abc"}`))
	}, map[string]string{
		"API-VERSION": "1",
		"Accept":      "application/hal+json",
	})

	var out struct {
		ID string `json:"id"`
	}
	if err := r.getJSON(context.Background(), "/x", nil, &out); err != nil {
		t.Fatalf("getJSON: %v", err)
	}
	if out.ID != "abc" {
		t.Fatalf("decoded id = %q", out.ID)
	}
}

func TestRequesterRetriesOn429(t *testing.T) {
	var calls atomic.Int32
	r, _ := newTestRequester(t, func(w http.ResponseWriter, _ *http.Request) {
		n := calls.Add(1)
		if n < 2 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}, nil)
	r.maxRetries = 3

	if err := r.getJSON(context.Background(), "/x", nil, &map[string]any{}); err != nil {
		t.Fatalf("getJSON: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("expected 2 calls (1 retry), got %d", got)
	}
}

func TestRequesterReturnsTypedHTTPErrors(t *testing.T) {
	cases := []struct {
		status int
		want   error
	}{
		{http.StatusNotFound, ErrNotFound},
		{http.StatusUnauthorized, ErrUnauthorized},
		{http.StatusForbidden, ErrForbidden},
		{http.StatusBadRequest, ErrBadRequest},
		{http.StatusConflict, ErrConflict},
		{http.StatusUnprocessableEntity, ErrValidation},
		{http.StatusInternalServerError, ErrServer},
	}
	for _, tc := range cases {
		t.Run(http.StatusText(tc.status), func(t *testing.T) {
			status := tc.status
			r, _ := newTestRequester(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
				_, _ = w.Write([]byte(fmt.Sprintf(`{"exceptionType":"X","exceptionMessage":"m-%d"}`, status)))
			}, nil)

			err := r.getJSON(context.Background(), "/x", nil, &map[string]any{})
			if err == nil {
				t.Fatalf("expected error")
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("expected errors.Is(err, %v), got %v", tc.want, err)
			}
			var aerr *Error
			if !errors.As(err, &aerr) {
				t.Fatalf("expected *Error, got %T", err)
			}
			if !strings.Contains(aerr.Message, fmt.Sprintf("m-%d", status)) {
				t.Fatalf("expected message with exceptionMessage payload, got %q", aerr.Message)
			}
		})
	}
}

// TestRequester_IoReaderBodySurvivesRetry proves an io.Reader body is
// re-sent on retry instead of being consumed once and arriving empty
// on attempt 2. The bug fix materializes the body to bytes before the
// retry loop so each attempt builds a fresh reader from the same data.
func TestRequester_IoReaderBodySurvivesRetry(t *testing.T) {
	var (
		calls atomic.Int32
		got   [][]byte
		mu    sync.Mutex
	)
	r, _ := newTestRequester(t, func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		mu.Lock()
		got = append(got, body)
		mu.Unlock()
		if calls.Add(1) < 2 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}, nil)
	r.maxRetries = 3

	// Use a PUT (retryable, idempotent) with an io.Reader body. A bare
	// *bytes.Buffer is the historically-buggy shape: it implements
	// io.Reader but Read consumes, so the second attempt would have
	// seen an empty body without the materialization fix.
	reader := bytes.NewBuffer([]byte("payload-bytes-xyz"))
	if err := r.putJSON(context.Background(), "/x", reader, nil, nil); err != nil {
		t.Fatalf("putJSON: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("expected 2 attempts, got %d", len(got))
	}
	if string(got[0]) != "payload-bytes-xyz" || string(got[1]) != "payload-bytes-xyz" {
		t.Fatalf("body lost on retry: got %q then %q", got[0], got[1])
	}
}

// TestRequester_RetriesOn5xxForIdempotentMethods proves GET (and other
// idempotent methods) retry on transient 5xx. Without this, every 500
// forced the engine to retry the entire job — wasteful when a quick
// SDK-level retry would succeed.
func TestRequester_RetriesOn5xxForIdempotentMethods(t *testing.T) {
	var calls atomic.Int32
	r, _ := newTestRequester(t, func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) < 2 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}, nil)
	r.maxRetries = 3

	if err := r.getJSON(context.Background(), "/x", nil, &map[string]any{}); err != nil {
		t.Fatalf("getJSON: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("expected 2 calls (1 retry on 502), got %d", got)
	}
}

// TestRequester_DoesNotRetryPostOn5xx proves POST is NOT retried on 5xx
// even when other idempotent methods would be. Repeating a successful
// POST would create duplicate records on the Aprimo side — the engine
// retries at the job layer, where the marker state machine handles
// the duplicate risk.
func TestRequester_DoesNotRetryPostOn5xx(t *testing.T) {
	var calls atomic.Int32
	r, _ := newTestRequester(t, func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}, nil)
	r.maxRetries = 3

	err := r.postJSON(context.Background(), "/x", map[string]string{"k": "v"}, &map[string]any{}, nil)
	if err == nil {
		t.Fatal("expected error from POST 502")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected 1 call (no POST retry on 5xx), got %d", got)
	}
}

// TestRequester_RetriesTransportErrorForIdempotent: a dropped connection
// (no response) on a GET is retried, the same as a 5xx.
func TestRequester_RetriesTransportErrorForIdempotent(t *testing.T) {
	var calls atomic.Int32
	r, _ := newTestRequester(t, func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) < 2 {
			conn, _, _ := w.(http.Hijacker).Hijack()
			_ = conn.Close() // drop the connection — client sees a transport error
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}, nil)
	r.maxRetries = 3

	if err := r.getJSON(context.Background(), "/x", nil, &map[string]any{}); err != nil {
		t.Fatalf("getJSON: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("expected 2 calls (1 retry after the dropped connection), got %d", got)
	}
}

// TestRequester_DoesNotRetryPostOnTransportError: a POST that may already
// have been processed must not be re-sent on a dropped connection.
func TestRequester_DoesNotRetryPostOnTransportError(t *testing.T) {
	var calls atomic.Int32
	r, _ := newTestRequester(t, func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		conn, _, _ := w.(http.Hijacker).Hijack()
		_ = conn.Close()
	}, nil)
	r.maxRetries = 3

	if err := r.postJSON(context.Background(), "/x", map[string]string{"k": "v"}, &map[string]any{}, nil); err == nil {
		t.Fatal("expected a transport error from the dropped POST")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected 1 call (no POST retry on transport error), got %d", got)
	}
}

// TestBackoffDelay_AppliesJitter samples backoffDelay enough times to
// expose lockstep behavior. With no jitter, two callers at the same
// attempt number would get identical delays and retry in unison.
func TestBackoffDelay_AppliesJitter(t *testing.T) {
	seen := make(map[time.Duration]int)
	for range 200 {
		seen[backoffDelay(3)]++
	}
	if len(seen) < 50 {
		t.Fatalf("backoffDelay appears non-jittered: only %d distinct values in 200 calls", len(seen))
	}
	// All samples should sit in [0.75x, 1.25x] of 4s.
	const base = 4 * time.Second
	lo := time.Duration(float64(base) * 0.75)
	hi := time.Duration(float64(base) * 1.25)
	for d := range seen {
		if d < lo || d > hi {
			t.Fatalf("backoffDelay sample %v out of [%v, %v]", d, lo, hi)
		}
	}
}

func TestRequesterJSONEncodesBody(t *testing.T) {
	r, _ := newTestRequester(t, func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q", got)
		}
		var body struct {
			Foo string `json:"foo"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.Foo != "bar" {
			t.Errorf("body.foo = %q", body.Foo)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"r1"}`))
	}, nil)

	var out CreateResponse
	if err := r.postJSON(context.Background(), "/r",
		map[string]string{"foo": "bar"}, &out, nil); err != nil {
		t.Fatalf("postJSON: %v", err)
	}
	if out.ID != "r1" {
		t.Fatalf("out.ID = %q", out.ID)
	}
}
