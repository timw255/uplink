package aprimo

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// TokenProvider returns a fresh bearer token. The returned token must
// NOT include the "Bearer " prefix.
type TokenProvider func(ctx context.Context) (string, error)

// ClientCredentialsTokenProvider builds a TokenProvider that performs
// the client_credentials OAuth flow against
// https://{env}.aprimo.com/login/connect/token. Returned tokens are
// not cached; combine with CachedTokenProvider for connection reuse.
func ClientCredentialsTokenProvider(
	httpc *http.Client,
	moBaseURL, clientID, clientSecret string,
) TokenProvider {
	if httpc == nil {
		httpc = http.DefaultClient
	}
	return func(ctx context.Context) (string, error) {
		form := url.Values{}
		form.Set("grant_type", "client_credentials")
		form.Set("client_id", clientID)
		form.Set("client_secret", clientSecret)
		form.Set("scope", "api")

		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			moBaseURL+"/login/connect/token",
			strings.NewReader(form.Encode()))
		if err != nil {
			return "", &Error{Message: "build auth request", Cause: err}
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		resp, err := httpc.Do(req)
		if err != nil {
			return "", &Error{Message: "auth: transport", Cause: fmt.Errorf("%w: %w", ErrAuth, err)}
		}
		defer resp.Body.Close()

		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if readErr != nil {
			return "", &Error{Message: "auth: read response", Cause: readErr}
		}
		if resp.StatusCode/100 != 2 {
			return "", &Error{
				Message:      "auth: token request failed",
				Status:       resp.StatusCode,
				ResponseBody: body,
				Cause:        ErrAuth,
			}
		}

		var out struct {
			AccessToken string `json:"access_token"`
			ExpiresIn   int    `json:"expires_in"`
		}
		if err := json.Unmarshal(body, &out); err != nil {
			return "", &Error{Message: "auth: decode token response", Cause: err}
		}
		if out.AccessToken == "" {
			return "", &Error{Message: "auth: empty access_token", Cause: ErrAuth}
		}
		return out.AccessToken, nil
	}
}

// CachedTokenProvider wraps a TokenProvider with a single-flight cache
// keyed on the inner provider's identity. The token is refreshed when
// its JWT exp claim is within refreshSkew of now; tokens without a
// readable exp use a 9-minute fallback TTL.
//
// Safe for concurrent use.
func CachedTokenProvider(inner TokenProvider) TokenProvider {
	const fallbackTTL = 9 * time.Minute
	const refreshSkew = 30 * time.Second

	var (
		mu       sync.Mutex
		token    string
		expiry   time.Time
		inflight chan struct{}
		fetchErr error
	)

	return func(ctx context.Context) (string, error) {
		mu.Lock()
		if token != "" && time.Now().Before(expiry.Add(-refreshSkew)) {
			t := token
			mu.Unlock()
			return t, nil
		}
		if inflight != nil {
			wait := inflight
			mu.Unlock()
			select {
			case <-wait:
			case <-ctx.Done():
				return "", ctx.Err()
			}
			mu.Lock()
			t, e := token, fetchErr
			mu.Unlock()
			if e != nil {
				return "", e
			}
			return t, nil
		}
		// We are the fetcher.
		done := make(chan struct{})
		inflight = done
		mu.Unlock()

		newToken, err := inner(ctx)

		mu.Lock()
		fetchErr = err
		if err == nil {
			token = newToken
			expiry = tokenExpiry(newToken, fallbackTTL)
		}
		inflight = nil
		close(done)
		mu.Unlock()

		if err != nil {
			return "", err
		}
		return newToken, nil
	}
}

// tokenExpiry returns when the JWT in s expires, based on its `exp`
// claim. Falls back to now+fallback if the token is opaque, the claim
// is missing, or the parsed expiry is already in the past — a past
// `exp` would otherwise make the cache miss on every call and trigger
// a refresh per request under any meaningful clock skew.
func tokenExpiry(s string, fallback time.Duration) time.Time {
	parts := strings.Split(s, ".")
	if len(parts) < 2 {
		return time.Now().Add(fallback)
	}
	payload, err := base64.RawURLEncoding.DecodeString(addBase64Padding(parts[1]))
	if err != nil {
		return time.Now().Add(fallback)
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Exp == 0 {
		return time.Now().Add(fallback)
	}
	exp := time.Unix(claims.Exp, 0)
	if exp.Before(time.Now()) {
		return time.Now().Add(fallback)
	}
	return exp
}

// addBase64Padding pads s to a multiple of 4 with '=' so URL-safe
// base64 strings can be decoded with the standard library.
func addBase64Padding(s string) string {
	if pad := len(s) % 4; pad != 0 {
		s += strings.Repeat("=", 4-pad)
	}
	return s
}
