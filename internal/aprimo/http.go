package aprimo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"

	"golang.org/x/time/rate"
)

// requester is the shared HTTP plumbing every resource sits on. It is
// not exported — resources call its do/getJSON/postJSON/etc. helpers.
type requester struct {
	client     *http.Client
	auth       TokenProvider
	baseURL    string
	headers    map[string]string
	maxRetries int

	// sem caps the number of in-flight requests across all goroutines
	// using this requester. nil = uncapped. Memory + socket-pool
	// safety net independent of API rate limiting.
	sem chan struct{}

	// limiter models Aprimo's server-side token bucket: tokens refill
	// at Config.RPS per second up to AprimoBurst (100). Each HTTP
	// request acquires one token via Wait(ctx) before firing, so the
	// client never tries to drive Aprimo faster than its licensed
	// rate. nil = no rate limiting (the request layer relies solely
	// on 429-retry).
	limiter *rate.Limiter
}

// getJSON, postJSON, putJSON, deleteJSON decode the response body into
// `out` (which may be nil for void endpoints) and return any non-2xx
// response as an *Error. Headers override per-request defaults.
//
// `body` may be:
//   - nil — no request body
//   - io.Reader — sent as-is with the caller-supplied Content-Type
//   - anything else — JSON-encoded
func (r *requester) getJSON(ctx context.Context, path string, headers map[string]string, out any) error {
	return r.do(ctx, http.MethodGet, path, nil, headers, out)
}

func (r *requester) postJSON(ctx context.Context, path string, body, out any, headers map[string]string) error {
	return r.do(ctx, http.MethodPost, path, body, headers, out)
}

func (r *requester) putJSON(ctx context.Context, path string, body, out any, headers map[string]string) error {
	return r.do(ctx, http.MethodPut, path, body, headers, out)
}

func (r *requester) deleteJSON(ctx context.Context, path string, headers map[string]string) error {
	return r.do(ctx, http.MethodDelete, path, nil, headers, nil)
}

func (r *requester) do(
	ctx context.Context,
	method, path string,
	body any,
	headers map[string]string,
	out any,
) error {
	url := r.baseURL + path

	// Two body shapes: pre-materialized (we hold the bytes; each
	// retry sends a fresh bytes.Reader) and factory-built (we call
	// the factory on every attempt to get a fresh streaming body).
	// Factory bodies are how segment uploads avoid buffering a full
	// segment in memory while still being retryable on 429.
	factory, isFactory := body.(RequestBodyFactory)
	var bodyBytes []byte
	var bodyContentType string
	if !isFactory {
		var err error
		bodyBytes, bodyContentType, err = materializeBody(body)
		if err != nil {
			return err
		}
	}

	attempts := r.maxRetries + 1
	for attempt := 1; attempt <= attempts; attempt++ {
		var (
			reqReader  io.Reader
			reqCloser  io.Closer
			reqContent string
		)
		if isFactory {
			rc, ct, ferr := factory()
			if ferr != nil {
				return newTransportError("build streaming body", ferr)
			}
			reqReader = rc
			reqCloser = rc
			reqContent = ct
		} else if bodyBytes != nil {
			reqReader = bytes.NewReader(bodyBytes)
			reqContent = bodyContentType
		}

		req, err := r.buildRequest(ctx, method, url, reqReader, reqContent, headers)
		if err != nil {
			if reqCloser != nil {
				_ = reqCloser.Close()
			}
			return err
		}

		// Rate limit. Acquired per attempt — retries cost tokens too,
		// which is the right call: the server-side bucket also drains
		// on a request that got 429-rejected. Pacing retries through
		// the limiter avoids a retry-storm right after the bucket
		// drains.
		if r.limiter != nil {
			if err := r.limiter.Wait(ctx); err != nil {
				if reqCloser != nil {
					_ = reqCloser.Close()
				}
				return newTransportError("rate limiter wait", err)
			}
		}

		// Concurrency cap. Acquired per attempt so a retry after
		// release doesn't pin a slot during the backoff sleep.
		if r.sem != nil {
			select {
			case r.sem <- struct{}{}:
			case <-ctx.Done():
				if reqCloser != nil {
					_ = reqCloser.Close()
				}
				return newTransportError("request cancelled while waiting for slot", ctx.Err())
			}
		}
		resp, err := r.client.Do(req)
		if r.sem != nil {
			<-r.sem
		}
		// client.Do consumes (or partially consumes) reqReader. We
		// still close our handle so streaming bodies release any
		// underlying source (e.g., the OpenRange ReadCloser the
		// factory opened). Safe to call after a fully-consumed body
		// — Close is idempotent on our streaming wrapper.
		if reqCloser != nil {
			_ = reqCloser.Close()
		}
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return newTransportError("request cancelled", ctxErr)
			}
			return newTransportError("transport error", err)
		}

		respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
		_ = resp.Body.Close()
		if readErr != nil {
			return newTransportError("read response", readErr)
		}

		if shouldRetry(method, resp.StatusCode) && attempt < attempts {
			delay := parseRetryAfter(resp.Header.Get("Retry-After"))
			if delay <= 0 {
				delay = backoffDelay(attempt)
			}
			select {
			case <-ctx.Done():
				return newTransportError("request cancelled during retry wait", ctx.Err())
			case <-time.After(delay):
			}
			continue
		}

		if resp.StatusCode/100 != 2 {
			return decodeErrorResponse(resp.StatusCode, respBody)
		}

		if out == nil || len(respBody) == 0 {
			return nil
		}
		if err := json.Unmarshal(respBody, out); err != nil {
			return newTransportError(fmt.Sprintf("decode %s response", method), err)
		}
		return nil
	}
	return newTransportError("retries exhausted", errors.New("no successful response"))
}

// RequestBodyFactory builds a fresh request body on demand. The retry
// loop in do() calls this once per attempt, so streaming bodies (e.g.,
// io.Pipe wired to a source connector's OpenRange call) can be safely
// retried — the factory re-opens the source rather than buffering it.
//
// The returned ReadCloser is fully consumed (or closed early on
// failure) by the requester; callers don't need to track it.
type RequestBodyFactory func() (body io.ReadCloser, contentType string, err error)

// materializeBody normalizes a body argument into bytes + content-type
// so the retry loop can build a fresh request on each attempt.
//
//   - nil                → no body, no content-type
//   - RequestBodyFactory → not pre-materialized; do() calls the factory per attempt
//   - io.Reader          → drained to bytes, content-type left to caller headers
//   - anything else      → JSON-encoded with application/json content-type
func materializeBody(body any) ([]byte, string, error) {
	switch b := body.(type) {
	case nil:
		return nil, "", nil
	case RequestBodyFactory:
		// Signal to caller that materialization is deferred. The
		// caller checks the body type with a separate type assertion;
		// returning nil here would short-circuit the body-bytes path.
		return nil, "", nil
	case io.Reader:
		data, err := io.ReadAll(b)
		if err != nil {
			return nil, "", newTransportError("read request body", err)
		}
		return data, "", nil
	default:
		data, err := json.Marshal(body)
		if err != nil {
			return nil, "", newTransportError("encode request body", err)
		}
		return data, "application/json", nil
	}
}

// shouldRetry decides whether a non-success response is worth a second
// attempt. 429 is always retried. 5xx is retried only for methods whose
// failure modes are idempotent — repeating a successful POST on the
// server side would create duplicate records, so POST 5xx propagates
// immediately and the engine retries at the job level (where the marker
// state machine handles the duplicate risk).
func shouldRetry(method string, status int) bool {
	if status == http.StatusTooManyRequests {
		return true
	}
	switch status {
	case http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		switch method {
		case http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodHead:
			return true
		}
	}
	return false
}

func (r *requester) buildRequest(
	ctx context.Context,
	method, fullURL string,
	body io.Reader,
	bodyContentType string,
	headers map[string]string,
) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, fullURL, body)
	if err != nil {
		return nil, newTransportError("build request", err)
	}

	token, err := r.auth(ctx)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)

	for k, v := range r.headers {
		req.Header.Set(k, v)
	}
	if bodyContentType != "" {
		req.Header.Set("Content-Type", bodyContentType)
	}
	for k, v := range headers {
		if v == "" {
			req.Header.Del(k)
			continue
		}
		req.Header.Set(k, v)
	}
	return req, nil
}

// decodeErrorResponse turns a non-2xx HTTP response into an *Error
// enriched with the Aprimo `exceptionType` / `exceptionMessage` when
// present in the body.
func decodeErrorResponse(status int, body []byte) error {
	var payload struct {
		ExceptionType    string `json:"exceptionType"`
		ExceptionMessage string `json:"exceptionMessage"`
	}
	_ = json.Unmarshal(body, &payload)
	return newHTTPError(status, body, payload.ExceptionType, payload.ExceptionMessage, nil)
}

func parseRetryAfter(h string) time.Duration {
	if h == "" {
		return 0
	}
	// Numeric form: seconds.
	if secs, err := strconv.Atoi(h); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second
	}
	// HTTP-date form.
	if when, err := http.ParseTime(h); err == nil {
		if d := time.Until(when); d > 0 {
			return d
		}
	}
	return 0
}

// backoffDelay is a doubling backoff for retries without a Retry-After
// header. Attempt 1 -> ~1s, 2 -> ~2s, 3 -> ~4s, capped at 30s, with
// ±25% jitter applied so N parallel callers throttled by the same
// server don't all retry in lockstep.
func backoffDelay(attempt int) time.Duration {
	const cap = 30 * time.Second
	d := time.Second
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= cap {
			d = cap
			break
		}
	}
	factor := 0.75 + rand.Float64()*0.5
	return time.Duration(float64(d) * factor)
}
