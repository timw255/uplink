package aprimo

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"golang.org/x/time/rate"
)

// Config holds the construction parameters for a Client.
type Config struct {
	// Environment is the Aprimo subdomain — the `<env>` in
	// `https://<env>.aprimo.com`. Required.
	Environment string

	// ClientID and ClientSecret authenticate via the client_credentials
	// OAuth flow. Both required for the default token provider. For
	// custom flows, leave these zero and set TokenProvider directly.
	ClientID     string
	ClientSecret string

	// TokenProvider, if set, takes precedence over ClientID/ClientSecret.
	// Use it for browser flows or for tests. The returned token must be
	// a raw bearer token without the "Bearer " prefix.
	TokenProvider TokenProvider

	// HTTPClient is the http.Client used for every request. nil
	// means the SDK constructs one with HTTPTimeout + a tuned
	// transport (generous idle pool sized for parallel-segment
	// uploads). Set this only when you need full control of TLS or
	// proxy behavior; otherwise prefer HTTPTimeout alone so the
	// tuned transport ships.
	HTTPClient *http.Client

	// HTTPTimeout is the per-request timeout applied to the SDK's
	// default http.Client. 0 = 60s. Ignored when HTTPClient is set
	// (the caller owns timeout).
	HTTPTimeout time.Duration

	// MaxRetries is the number of retries on rate-limit (429). 0 = no
	// retries beyond the first attempt. Default 3.
	MaxRetries int

	// MaxConcurrent caps the number of in-flight HTTP requests across
	// every resource this client constructs. 0 = uncapped. Tune to
	// bound the daemon's memory + socket-pool footprint; the rate
	// limiter below handles request-rate pacing.
	MaxConcurrent int

	// RPS is the sustained per-second request rate the SDK will
	// allow against Aprimo. Set to your tenant's licensed RPS (the
	// default Aprimo allowance is on the order of 15 RPS; higher
	// values are licensable). 0 disables rate limiting and falls
	// back to retry-on-429.
	//
	// The companion burst capacity is fixed at AprimoBurst (100,
	// matching Aprimo's documented burst buffer). Together with RPS
	// these implement the token-bucket Aprimo enforces server-side:
	// you can fire up to 100 requests instantly when the bucket is
	// full, after which subsequent requests pace at RPS/sec. Pacing
	// the client to match the server's bucket means we stop ever
	// triggering 429 under normal load — the retry path is reserved
	// for genuine transient failures.
	RPS float64
}

// AprimoBurst is the documented Aprimo burst-buffer capacity. The
// SDK rate limiter pairs RPS with this constant as the token-bucket's
// burst size. Fixed (not a per-tenant license knob); if Aprimo ever
// changes the burst contract, update this value.
const AprimoBurst = 100

// Client is the entry point. Construct via New, then use the resource
// fields (Records, Uploader, ...).
type Client struct {
	cfg     Config
	auth    TokenProvider
	dam     *requester
	mo      *requester
	damURL  string
	moURL   string

	Uploader         *Uploader
	Records          *Records
	Collections      *Collections
	FieldDefinitions *FieldDefinitions
	Languages        *Languages
	Classifications  *Classifications
	Users            *Users
	UserGroups       *UserGroups
}

// New constructs a Client.
func New(cfg Config) (*Client, error) {
	if cfg.Environment == "" {
		return nil, fmt.Errorf("aprimo: Config.Environment is required")
	}
	if strings.Contains(cfg.Environment, "/") || strings.Contains(cfg.Environment, ".") {
		return nil, fmt.Errorf("aprimo: Environment must be a bare subdomain (got %q)", cfg.Environment)
	}
	if cfg.HTTPClient == nil {
		timeout := cfg.HTTPTimeout
		if timeout == 0 {
			timeout = 60 * time.Second
		}
		cfg.HTTPClient = &http.Client{
			Timeout:   timeout,
			Transport: tunedHTTPTransport(),
		}
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}

	moURL := fmt.Sprintf("https://%s.aprimo.com", cfg.Environment)
	damURL := fmt.Sprintf("https://%s.dam.aprimo.com", cfg.Environment)

	auth := cfg.TokenProvider
	if auth == nil {
		if cfg.ClientID == "" || cfg.ClientSecret == "" {
			return nil, errors.New("aprimo: ClientID and ClientSecret are required when TokenProvider is nil")
		}
		auth = CachedTokenProvider(
			ClientCredentialsTokenProvider(cfg.HTTPClient, moURL, cfg.ClientID, cfg.ClientSecret),
		)
	}

	c := &Client{
		cfg:    cfg,
		auth:   auth,
		damURL: damURL,
		moURL:  moURL,
	}

	// One shared semaphore across both requesters so the cap applies
	// to total in-flight Aprimo traffic, not per base URL.
	var sem chan struct{}
	if cfg.MaxConcurrent > 0 {
		sem = make(chan struct{}, cfg.MaxConcurrent)
	}

	// One shared rate limiter for the same reason: Aprimo's
	// server-side token bucket is one-per-tenant, not one-per-host.
	var limiter *rate.Limiter
	if cfg.RPS > 0 {
		limiter = rate.NewLimiter(rate.Limit(cfg.RPS), AprimoBurst)
	}

	c.dam = &requester{
		client:  cfg.HTTPClient,
		auth:    auth,
		baseURL: damURL,
		headers: map[string]string{
			"API-VERSION":  "1",
			"Accept":       "application/hal+json",
			"Content-Type": "application/json",
		},
		maxRetries: cfg.MaxRetries,
		sem:        sem,
		limiter:    limiter,
	}
	c.mo = &requester{
		client:  cfg.HTTPClient,
		auth:    auth,
		baseURL: moURL,
		headers: map[string]string{
			"Accept": "application/hal+json",
		},
		maxRetries: cfg.MaxRetries,
		sem:        sem,
		limiter:    limiter,
	}

	c.Uploader = &Uploader{r: c.mo}
	c.Records = &Records{r: c.dam}
	c.Collections = &Collections{r: c.dam}
	c.FieldDefinitions = &FieldDefinitions{r: c.dam}
	c.Languages = &Languages{r: c.dam}
	c.Classifications = &Classifications{r: c.dam}
	c.Users = &Users{r: c.dam}
	c.UserGroups = &UserGroups{r: c.dam}
	return c, nil
}

// tunedHTTPTransport returns an http.Transport with idle-pool and
// concurrency-friendly defaults. Go's stdlib defaults are conservative
// (MaxIdleConnsPerHost=2) which causes every parallel segment upload
// past the second to open a fresh TLS connection and discard it after
// — ~150–300 ms of handshake amortized across the wrong number of
// requests. This transport keeps the pool warm for the parallelism
// the uploader actually does (4+ segments × N workers concurrently
// to the same host).
//
// Callers wanting custom transport behavior should construct their
// own http.Client and set Config.HTTPClient explicitly — this
// function only runs when HTTPClient is nil.
func tunedHTTPTransport() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 128
	t.MaxIdleConnsPerHost = 64
	t.MaxConnsPerHost = 0 // unlimited; semaphore caps via MaxConcurrent
	t.IdleConnTimeout = 90 * time.Second
	t.ForceAttemptHTTP2 = true
	// ExpectContinueTimeout default is fine; segment POSTs don't use
	// Expect: 100-continue. Tighten DialContext default Timeout so a
	// dead endpoint fails fast (default is 30s which is reasonable for
	// fresh connections; we leave it). KeepAlive default 30s.
	return t
}

// SetTestEndpoints overrides the MO and DAM base URLs. Test-only —
// production code must construct via New(Config{Environment: ...}).
// Pass the empty string for either argument to leave it unchanged.
// This exists so connector tests in other packages can point a real
// Client at an httptest.NewServer without having to mock the whole
// SDK surface.
func (c *Client) SetTestEndpoints(moURL, damURL string) {
	if moURL != "" {
		c.moURL = moURL
		if c.mo != nil {
			c.mo.baseURL = moURL
		}
	}
	if damURL != "" {
		c.damURL = damURL
		if c.dam != nil {
			c.dam.baseURL = damURL
		}
	}
}
