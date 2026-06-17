package aprimo

import (
	"context"
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

// ---- Test fakes --------------------------------------------------------

// memorySource implements connector.SegmentSource over an in-memory
// byte slice. Each Open returns a fresh io.ReadCloser over the
// requested byte range so the same source can be opened many times,
// which is what retry + prefetch both need.
type memorySource struct {
	data []byte

	// counters; useful for assertions
	openCalls  atomic.Int32
	openRanges []rangeReq // recorded calls (mu-protected)
	mu         sync.Mutex

	// slowOpen, if non-zero, sleeps before returning the reader. Used
	// to verify prefetch hides source-side latency.
	slowOpen time.Duration

	// failFirstOpen, if true, makes the FIRST Open return an error;
	// subsequent calls succeed. Used to test error propagation through
	// the producer-consumer pipeline.
	failFirstOpen atomic.Bool
}

type rangeReq struct{ start, length int64 }

func (s *memorySource) Size() int64 { return int64(len(s.data)) }

func (s *memorySource) Open(_ context.Context, start, length int64) (io.ReadCloser, error) {
	s.openCalls.Add(1)
	s.mu.Lock()
	s.openRanges = append(s.openRanges, rangeReq{start, length})
	s.mu.Unlock()
	if s.failFirstOpen.CompareAndSwap(true, false) {
		return nil, errors.New("memorySource: synthetic open failure")
	}
	if s.slowOpen > 0 {
		time.Sleep(s.slowOpen)
	}
	if start < 0 || start >= int64(len(s.data)) {
		return io.NopCloser(strings.NewReader("")), nil
	}
	end := start + length
	if end > int64(len(s.data)) {
		end = int64(len(s.data))
	}
	return io.NopCloser(strings.NewReader(string(s.data[start:end]))), nil
}

// recordingServer mimics Aprimo's segmented upload endpoints just
// enough to drive the uploader. Tracks segment payloads (per-index
// SHA-equivalent fingerprints — actually the concatenated body bytes
// minus multipart envelope) so tests can assert correctness.
type recordingServer struct {
	srv         *httptest.Server
	mu          sync.Mutex
	segments    map[int][]byte // index -> received bytes (extracted from multipart)
	uploadCount atomic.Int32
	rateLimitN  atomic.Int32 // if > 0, return 429 on next N segment POSTs
}

func newRecordingServer(t *testing.T) *recordingServer {
	rs := &recordingServer{segments: map[int][]byte{}}
	rs.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/uploads/segments":
			fmt.Fprintf(w, `{"uri":"%s/upload/test-session"}`, rs.srv.URL)
		case r.URL.Path == "/upload/test-session" && r.URL.Query().Get("index") != "":
			rs.handleSegmentPOST(w, r)
		case r.URL.Path == "/upload/test-session/commit":
			fmt.Fprintln(w, `{"token":"final-token"}`)
		case r.URL.Path == "/upload/test-session" && r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/uploads":
			// Single-shot upload — drain the body, return a token.
			_, _ = io.Copy(io.Discard, r.Body)
			fmt.Fprintln(w, `{"token":"final-token"}`)
		default:
			http.Error(w, "unexpected request: "+r.URL.String(), http.StatusBadRequest)
		}
	}))
	t.Cleanup(rs.srv.Close)
	return rs
}

func (rs *recordingServer) handleSegmentPOST(w http.ResponseWriter, r *http.Request) {
	rs.uploadCount.Add(1)
	if rs.rateLimitN.Load() > 0 {
		rs.rateLimitN.Add(-1)
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}
	idx := r.URL.Query().Get("index")
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var n int
	if _, err := fmt.Sscanf(idx, "%d", &n); err != nil {
		http.Error(w, "bad index", http.StatusBadRequest)
		return
	}
	rs.mu.Lock()
	rs.segments[n] = body
	rs.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (rs *recordingServer) uploader(t *testing.T) *Uploader {
	t.Helper()
	return &Uploader{r: &requester{
		client:  rs.srv.Client(),
		auth:    stubAuth("tok"),
		baseURL: rs.srv.URL,
		headers: map[string]string{"Accept": "application/hal+json"},
	}}
}

// ---- #1: ParallelSegments default --------------------------------------

// TestUploadOptions_ParallelSegmentsDefaultsTo4 locks in the
// throughput pass's headline change: the SDK no longer ships with
// serial-by-default segment upload.
func TestUploadOptions_ParallelSegmentsDefaultsTo4(t *testing.T) {
	if DefaultParallelSegments != 4 {
		t.Fatalf("DefaultParallelSegments = %d, want 4", DefaultParallelSegments)
	}
	// And verify the default takes effect via UploadFromSource. Use a
	// recording server so we observe ordering: if 4 segments arrive in
	// parallel, they can interleave; with serial=1 they arrive in
	// strict order. We assert at least 2 segments saw concurrent
	// in-flight requests.
	rs := newRecordingServer(t)
	var inFlight atomic.Int32
	var peakInFlight atomic.Int32
	rs.srv.Close()
	rs.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/upload/test-session") && r.URL.Query().Get("index") != "" {
			cur := inFlight.Add(1)
			for {
				peak := peakInFlight.Load()
				if cur <= peak || peakInFlight.CompareAndSwap(peak, cur) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond) // hold to widen the concurrency window
			inFlight.Add(-1)
			rs.handleSegmentPOST(w, r)
			return
		}
		switch r.URL.Path {
		case "/uploads/segments":
			fmt.Fprintf(w, `{"uri":"%s/upload/test-session"}`, rs.srv.URL)
		case "/upload/test-session/commit":
			fmt.Fprintln(w, `{"token":"final-token"}`)
		}
	}))
	t.Cleanup(rs.srv.Close)

	src := &memorySource{data: make([]byte, 8*DefaultSegmentSize)} // 8 segments
	u := rs.uploader(t)
	res, err := u.UploadFromSource(context.Background(), src, "test.bin", nil)
	if err != nil {
		t.Fatalf("UploadFromSource: %v", err)
	}
	if res.Token != "final-token" {
		t.Fatalf("token = %q", res.Token)
	}
	if peakInFlight.Load() < 2 {
		t.Fatalf("peak in-flight = %d; expected ≥2 (default parallel=4)", peakInFlight.Load())
	}
}

// ---- #2: streaming multipart body --------------------------------------

// TestStreamMultipart_BoundedMemory proves the body is consumed in
// chunks rather than buffered whole. We feed a 4 MiB segment through
// streamMultipart and observe that an early consumer-side read
// doesn't require the full source to have been drained yet.
func TestStreamMultipart_BoundedMemory(t *testing.T) {
	const segSize = 4 * 1024 * 1024
	srcConsumed := atomic.Int64{}
	src := &countingReader{
		r:        io.LimitReader(zeros{}, segSize),
		consumed: &srcConsumed,
	}
	body, ct := streamMultipart("seg0", "x.bin", io.NopCloser(src))
	defer body.Close()
	if !strings.Contains(ct, "multipart/form-data") {
		t.Fatalf("content type = %q", ct)
	}

	// Read just the first 8 KiB of the multipart body. Source must
	// not have been drained — confirming streaming, not buffering.
	buf := make([]byte, 8*1024)
	if _, err := io.ReadFull(body, buf); err != nil {
		t.Fatalf("ReadFull: %v", err)
	}
	// io.Pipe's internal buffer is 16 KiB; the goroutine may have
	// pre-written a bit ahead, but it absolutely cannot have read
	// the entire 4 MiB source before we asked for the first 8 KiB.
	if got := srcConsumed.Load(); got >= segSize {
		t.Fatalf("source fully drained too early: %d bytes consumed before we read", got)
	}
	if got := srcConsumed.Load(); got == 0 {
		t.Fatalf("source not consumed at all; streaming pipe broken")
	}
}

// TestStreamMultipart_CloseReleasesSource confirms that closing the
// returned body also closes the underlying source — critical for
// retry hygiene so we don't leak source connector handles when the
// HTTP layer abandons an in-flight body on 429.
func TestStreamMultipart_CloseReleasesSource(t *testing.T) {
	closed := atomic.Bool{}
	src := &closeRecordingReader{r: strings.NewReader("hello world"), closed: &closed}
	body, _ := streamMultipart("seg0", "x.bin", src)
	// Drain a little so the goroutine starts; otherwise it might exit
	// before we close, masking the cleanup.
	buf := make([]byte, 32)
	_, _ = body.Read(buf)
	if err := body.Close(); err != nil {
		t.Fatalf("body.Close: %v", err)
	}
	if !closed.Load() {
		t.Fatal("closing streaming body did not close underlying source")
	}
}

// ---- #2 retry: 429 → re-stream from fresh OpenRange ---------------------

// TestSegmentUpload_429TriggersReopen is the load-bearing retry test:
// on a 429 the requester must re-invoke the body factory, which must
// in turn call src.Open AGAIN. Without that, retries on streaming
// bodies would send empty payloads.
func TestSegmentUpload_429TriggersReopen(t *testing.T) {
	rs := newRecordingServer(t)
	rs.rateLimitN.Store(1) // first segment POST returns 429
	src := &memorySource{data: []byte("a 1.5x segment-sized payload for a fun roundtrip")}

	u := rs.uploader(t)
	u.r.maxRetries = 2 // allow the 429 retry

	// Force segmented even though the payload is tiny — guarantees we
	// hit the per-segment retry path, not the single-shot path.
	res, err := u.UploadFromSource(context.Background(), src, "x.bin", &UploadOptions{
		SegmentSize:      int64(len(src.data)),
		ParallelSegments: 1,
		ForceSegmented:   true,
	})
	if err != nil {
		t.Fatalf("UploadFromSource: %v", err)
	}
	if res.Token != "final-token" {
		t.Fatalf("token = %q", res.Token)
	}
	// First successful OpenRange may be prefetched + one extra on retry.
	if got := src.openCalls.Load(); got < 2 {
		t.Fatalf("openCalls = %d; expected ≥2 (one prefetch, one retry-reopen)", got)
	}
	// The retry must send the SAME payload, not an empty body.
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if got, want := rs.segments[0], src.data; !payloadContains(got, want) {
		t.Fatalf("uploaded payload missing source bytes after retry: got %q, want substring %q", got, want)
	}
}

// payloadContains reports whether the multipart-wrapped body contains
// the expected source bytes (multipart adds envelope around them).
func payloadContains(got, want []byte) bool {
	return bytes2Has(got, want)
}

func bytes2Has(haystack, needle []byte) bool {
	if len(needle) == 0 {
		return true
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if string(haystack[i:i+len(needle)]) == string(needle) {
			return true
		}
	}
	return false
}

// ---- #3: tuned HTTP transport ------------------------------------------

// TestTunedHTTPTransport_HasGenerousIdlePool verifies the SDK's
// default transport keeps connections warm for the concurrency the
// uploader actually does — Go's stdlib default of 2 idle per host is
// far too small for our pattern.
func TestTunedHTTPTransport_HasGenerousIdlePool(t *testing.T) {
	tr := tunedHTTPTransport()
	if tr.MaxIdleConns < 64 {
		t.Errorf("MaxIdleConns = %d, want ≥64", tr.MaxIdleConns)
	}
	if tr.MaxIdleConnsPerHost < 32 {
		t.Errorf("MaxIdleConnsPerHost = %d, want ≥32 (stdlib default of 2 is too small for our workload)", tr.MaxIdleConnsPerHost)
	}
	if !tr.ForceAttemptHTTP2 {
		t.Errorf("ForceAttemptHTTP2 = false, want true")
	}
	if tr.IdleConnTimeout == 0 {
		t.Errorf("IdleConnTimeout = 0; should be set to recycle stale conns")
	}
}

// TestClientNew_InstallsTunedTransport guards against a future
// regression where someone removes the auto-tune in New().
func TestClientNew_InstallsTunedTransport(t *testing.T) {
	c, err := New(Config{
		Environment:   "trial",
		TokenProvider: stubAuth("tok"),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if c.cfg.HTTPClient == nil {
		t.Fatal("HTTPClient is nil")
	}
	tr, ok := c.cfg.HTTPClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("HTTPClient.Transport is %T, want *http.Transport", c.cfg.HTTPClient.Transport)
	}
	if tr.MaxIdleConnsPerHost < 32 {
		t.Fatalf("MaxIdleConnsPerHost = %d, want tuned value", tr.MaxIdleConnsPerHost)
	}
}

// TestClientNew_HonorsExplicitHTTPClient confirms a caller-supplied
// HTTPClient is not overwritten by the tuning.
func TestClientNew_HonorsExplicitHTTPClient(t *testing.T) {
	custom := &http.Client{Timeout: 5 * time.Second}
	c, err := New(Config{
		Environment:   "trial",
		TokenProvider: stubAuth("tok"),
		HTTPClient:    custom,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if c.cfg.HTTPClient != custom {
		t.Fatal("caller-supplied HTTPClient was replaced")
	}
}

// ---- #20: prefetch pipeline --------------------------------------------

// TestPrefetchPipeline_HidesSlowOpenLatency drives the case the
// prefetch knob is built for: a source with non-trivial OpenRange
// latency. With prefetch_segments > 0 the producer pre-issues
// OpenRange while consumers upload, so the second-segment OpenRange
// RTT overlaps with the first-segment upload.
func TestPrefetchPipeline_HidesSlowOpenLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-sensitive; run without -short")
	}
	const segments = 4
	const segSize = 1024
	const openDelay = 80 * time.Millisecond

	// Synthetic server with fast uploads (we want OpenRange to be the
	// bottleneck, then measure how much prefetch overlaps).
	rs := newRecordingServer(t)
	src := &memorySource{data: make([]byte, segments*segSize), slowOpen: openDelay}

	// No prefetch baseline.
	src.openCalls.Store(0)
	u := rs.uploader(t)
	start := time.Now()
	_, err := u.UploadFromSource(context.Background(), src, "x.bin", &UploadOptions{
		SegmentSize:      segSize,
		ParallelSegments: 1,
		PrefetchSegments: -1, // disable prefetch
		ForceSegmented:   true,
	})
	if err != nil {
		t.Fatalf("baseline upload: %v", err)
	}
	baselineWall := time.Since(start)

	// With prefetch.
	rs2 := newRecordingServer(t)
	src2 := &memorySource{data: make([]byte, segments*segSize), slowOpen: openDelay}
	u2 := rs2.uploader(t)
	start = time.Now()
	_, err = u2.UploadFromSource(context.Background(), src2, "x.bin", &UploadOptions{
		SegmentSize:      segSize,
		ParallelSegments: 1,
		PrefetchSegments: 2,
		ForceSegmented:   true,
	})
	if err != nil {
		t.Fatalf("prefetch upload: %v", err)
	}
	prefetchWall := time.Since(start)

	// With 4 segments × 80ms OpenRange each, baseline is dominated by
	// 4 × openDelay = 320ms of sequential OpenRange calls. Prefetch
	// lets the producer issue subsequent OpenRange calls while the
	// uploader is busy — at least one delay should be hidden.
	if prefetchWall >= baselineWall {
		t.Fatalf("prefetch did not help: baseline=%v prefetch=%v", baselineWall, prefetchWall)
	}
	t.Logf("baseline=%v prefetch=%v (saved %v)", baselineWall, prefetchWall, baselineWall-prefetchWall)
}

// TestUploadOptions_PrefetchSegmentsDefaultsTo2 is a small guard
// against accidentally regressing the default to 0 (which would
// silently undo the prefetch benefit for default configurations).
func TestUploadOptions_PrefetchSegmentsDefaultsTo2(t *testing.T) {
	if DefaultPrefetchSegments != 2 {
		t.Fatalf("DefaultPrefetchSegments = %d, want 2", DefaultPrefetchSegments)
	}
}

// TestPrefetchPipeline_NegativeDisables confirms the documented
// escape hatch: PrefetchSegments=-1 disables prefetch (depth 0).
// Operators wanting strict back-pressure can opt out.
func TestPrefetchPipeline_NegativeDisables(t *testing.T) {
	src := &memorySource{data: make([]byte, 4*1024)}
	rs := newRecordingServer(t)
	u := rs.uploader(t)
	_, err := u.UploadFromSource(context.Background(), src, "x.bin", &UploadOptions{
		SegmentSize:      1024,
		ParallelSegments: 1,
		PrefetchSegments: -1,
		ForceSegmented:   true,
	})
	if err != nil {
		t.Fatalf("upload with prefetch disabled: %v", err)
	}
}

// ---- rate limiter (RPS + AprimoBurst) ----------------------------------

// TestRateLimiter_PacesRequests confirms the token bucket actually
// throttles the client when RPS is set. We fire a batch larger than
// the burst and assert that the tail of the batch is paced — total
// wall-clock matches what the bucket math predicts.
func TestRateLimiter_PacesRequests(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-sensitive; run without -short")
	}
	// Tiny RPS so the test is fast and deterministic. The bucket starts
	// empty, so the whole batch is paced — there's no burst to push past.
	const rps = 20.0
	const total = 40

	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		fmt.Fprintln(w, `{"ok":true}`)
	}))
	t.Cleanup(srv.Close)

	c, err := New(Config{
		Environment:   "trial",
		TokenProvider: stubAuth("tok"),
		RPS:           rps,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c.SetTestEndpoints(srv.URL, srv.URL)

	// Fire requests concurrently. With an empty bucket every request is
	// paced at RPS, so the batch takes about (total-1)/rps.
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var out map[string]any
			_ = c.dam.getJSON(context.Background(), "/test", nil, &out)
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	if hits.Load() != total {
		t.Fatalf("server saw %d hits, want %d", hits.Load(), total)
	}
	// From an empty bucket the k-th token frees at k/rps, so the batch
	// takes about (total-1)/rps. The bucket is precise; wall-clock has a
	// little noise (OS clock granularity, goroutine scheduling, the
	// limiter's float64 accumulator), so absorb 20ms. A "no limiter"
	// regression registers as ~0ms and is caught loudly.
	const tolerance = 20 * time.Millisecond
	expected := time.Duration(float64(total-1) / rps * float64(time.Second))
	minExpected := expected - tolerance
	if elapsed < minExpected {
		t.Fatalf("elapsed = %v, want at least %v (expected ~%v, tolerance %v; the token bucket should pace the whole batch from empty)",
			elapsed, minExpected, expected, tolerance)
	}
	t.Logf("%d requests at RPS=%v completed in %v (expected ≈%v)", total, rps, elapsed, expected)
}

// TestRateLimiter_StartsEmpty confirms the client does not front-load the
// tenant's shared server-side burst buffer at startup: a batch smaller
// than AprimoBurst is paced at RPS rather than firing all at once. A
// full-bucket regression would send them instantly and finish near-zero.
func TestRateLimiter_StartsEmpty(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-sensitive; run without -short")
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, `{"ok":true}`)
	}))
	t.Cleanup(srv.Close)

	const rps = 20.0
	const n = 30 // < AprimoBurst, so a full bucket would fire these instantly
	c, err := New(Config{
		Environment:   "trial",
		TokenProvider: stubAuth("tok"),
		RPS:           rps,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c.SetTestEndpoints(srv.URL, srv.URL)

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var out map[string]any
			_ = c.dam.getJSON(context.Background(), "/test", nil, &out)
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	const tolerance = 20 * time.Millisecond
	expected := time.Duration(float64(n-1) / rps * float64(time.Second))
	if elapsed < expected-tolerance {
		t.Fatalf("%d requests took %v; want at least %v — the bucket must start empty and pace, not front-load the burst",
			n, elapsed, expected-tolerance)
	}
	t.Logf("%d requests at RPS=%v paced from empty in %v (expected ≈%v)", n, rps, elapsed, expected)
}

// TestRateLimiter_NoLimiterWhenRPSIsZero confirms the default
// behavior: RPS=0 means no rate limiting (back-compat with anyone
// not setting it).
func TestRateLimiter_NoLimiterWhenRPSIsZero(t *testing.T) {
	c, err := New(Config{
		Environment:   "trial",
		TokenProvider: stubAuth("tok"),
		RPS:           0,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if c.dam.limiter != nil {
		t.Errorf("limiter should be nil when RPS=0; got %v", c.dam.limiter)
	}
	if c.mo.limiter != nil {
		t.Errorf("MO limiter should be nil when RPS=0")
	}
}

// TestRateLimiter_SharedAcrossBaseURLs confirms the limiter is one
// instance across DAM + MO, mirroring Aprimo's server-side bucket
// which is per-tenant (not per-host).
func TestRateLimiter_SharedAcrossBaseURLs(t *testing.T) {
	c, err := New(Config{
		Environment:   "trial",
		TokenProvider: stubAuth("tok"),
		RPS:           10,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if c.dam.limiter == nil || c.mo.limiter == nil {
		t.Fatal("limiters should be set when RPS > 0")
	}
	if c.dam.limiter != c.mo.limiter {
		t.Fatal("DAM and MO should share one limiter (one tenant bucket)")
	}
}

// ---- helpers -----------------------------------------------------------

type zeros struct{}

func (zeros) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

type countingReader struct {
	r        io.Reader
	consumed *atomic.Int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.consumed.Add(int64(n))
	return n, err
}

type closeRecordingReader struct {
	r      io.Reader
	closed *atomic.Bool
}

func (c *closeRecordingReader) Read(p []byte) (int, error) { return c.r.Read(p) }
func (c *closeRecordingReader) Close() error {
	c.closed.Store(true)
	return nil
}
