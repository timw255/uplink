package aprimo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/timw255/uplink/internal/connector"
)

// InvalidFilenameChars are the characters Aprimo rejects in a filename.
// A name derived from an arbitrary source path or object key may
// legitimately contain these (S3/B2/Azure keys are far more permissive
// than a DAM), so it has to be cleaned before it reaches the upload or
// record API.
const InvalidFilenameChars = `<>:"/\|?*`

// SanitizeFilename replaces every character Aprimo disallows in a filename
// with an underscore. Control characters are also replaced — beyond being
// rejected, an unescaped CR/LF in a multipart filename header would be a
// header-injection vector. Ordinary names pass through untouched.
func SanitizeFilename(name string) string {
	return strings.Map(func(r rune) rune {
		if r < 0x20 || strings.ContainsRune(InvalidFilenameChars, r) {
			return '_'
		}
		return r
	}, name)
}

// Uploader implements Aprimo's segmented file-upload protocol.
//
// Small / unconfigured uploads take the single-shot path:
//
//	POST /uploads multipart {"file1": <bytes>} → {"token": "..."}
//
// Larger or option-driven uploads take the segmented path:
//
//	POST /uploads/segments {"filename": ...} → {"uri": "..."}
//	POST <uri>?index=N      multipart {"segmentN": <bytes>} per segment
//	POST <uri>/commit       {"filename": ..., "segmentcount": N} → {"token": "..."}
//	DELETE <uri>            cancellation
//
// On the segmented path, each parallel worker calls
// SegmentSource.Open(start, length) and streams its byte range directly
// into the segment POST. Memory is bounded by
// `segment_size × parallel_segments` regardless of source file size;
// the source connector only needs to support ranged reads.
type Uploader struct {
	r *requester
}

// UploadOptions tunes a segmented upload. A nil/zero value uses
// SDK defaults.
type UploadOptions struct {
	// SegmentSize in bytes. Defaults to 20 MiB.
	SegmentSize int64

	// ParallelSegments is the maximum number of segments uploaded
	// concurrently. Defaults to DefaultParallelSegments (4).
	ParallelSegments int

	// PrefetchSegments adds pipeline depth between source-side reads
	// and destination-side writes. Set to N to allow N segments worth
	// of OpenRange responses to be sitting ready for upload above and
	// beyond the in-flight uploads. Useful when source-read latency is
	// non-trivial (cross-region S3, network mount) so the next chunk
	// is already streaming by the time a writer slot frees up.
	// Defaults to DefaultPrefetchSegments (2).
	PrefetchSegments int

	// OnProgress is invoked after each segment commit with the
	// running and total segment counts. May be nil.
	OnProgress func(uploaded, total int)

	// OnSegmentCommit is invoked after each successful segment POST
	// with the index that just committed. Used by the Aprimo
	// connector to persist progress into a marker file so a crash
	// mid-upload can resume without re-sending bytes. May be nil.
	OnSegmentCommit func(index int)

	// ForceSegmented sends every file via the segmented path, even
	// small ones. Useful in tests.
	ForceSegmented bool

	// OnSetupComplete is invoked once with the upload session path
	// returned by /uploads/segments, BEFORE any segment is uploaded.
	// The Aprimo connector uses this to persist the path into a
	// marker file so a crash mid-upload can resume the same session.
	// May be nil. Not invoked on the single-shot path or when Resume
	// is supplied.
	OnSetupComplete func(uploadPath string)

	// Resume, when non-nil, reuses an existing upload session
	// instead of calling /uploads/segments. Set when retrying after
	// a crash so already-committed segments aren't re-uploaded.
	Resume *ResumeOptions
}

// ResumeOptions is the state needed to resume a segmented upload.
type ResumeOptions struct {
	// UploadPath is the path returned by the original setup call
	// (e.g. "/upload/abc123"). Re-used for both segment POSTs and
	// the final commit.
	UploadPath string

	// CommittedSegments lists indices that have already POSTed
	// successfully. The resume run skips them.
	CommittedSegments []int
}

// DefaultSegmentSize is the segment size used when UploadOptions.SegmentSize is 0.
const DefaultSegmentSize = 20 * 1024 * 1024

// DefaultParallelSegments is the upload concurrency used when
// UploadOptions.ParallelSegments is 0. Four is a good production
// default: parallel enough to keep typical Aprimo upload throughput
// pinned without risking 429-storms against the rate limiter, and
// safe to combine with the SDK's MaxConcurrent semaphore.
const DefaultParallelSegments = 4

// DefaultPrefetchSegments is the additional pipeline depth between
// source reads and destination writes when
// UploadOptions.PrefetchSegments is 0. Two means up to two more
// segments can be opened from the source and ready to upload above
// the in-flight upload count. Small enough that memory pressure is
// bounded; large enough to hide a slow source's per-request latency.
const DefaultPrefetchSegments = 2

// UploadResult is what UploadFromSource returns on success. Token is
// the opaque upload token consumed downstream by Records.Create or
// Records.Update.
type UploadResult struct {
	Token string `json:"token"`
}

// UploadFromSource uploads bytes from src (under filename) and returns
// the upload token. The source must report a positive Size for the
// segmented path; sources with size <= 0 always go single-shot.
func (u *Uploader) UploadFromSource(
	ctx context.Context,
	src connector.SegmentSource,
	filename string,
	opts *UploadOptions,
) (*UploadResult, error) {
	if filename == "" {
		return nil, &Error{Message: "uploader: filename is required", Cause: ErrUpload}
	}
	if src == nil {
		return nil, &Error{Message: "uploader: nil source", Cause: ErrUpload}
	}
	if opts == nil {
		opts = &UploadOptions{}
	}
	if opts.SegmentSize <= 0 {
		opts.SegmentSize = DefaultSegmentSize
	}
	if opts.ParallelSegments <= 0 {
		opts.ParallelSegments = DefaultParallelSegments
	}
	if opts.PrefetchSegments < 0 {
		opts.PrefetchSegments = 0
	} else if opts.PrefetchSegments == 0 {
		opts.PrefetchSegments = DefaultPrefetchSegments
	}

	size := src.Size()
	// Segmented is mandatory only when the file actually spans
	// multiple segments. ParallelSegments + PrefetchSegments now
	// default to non-zero, so they're no longer signals of operator
	// intent — they're just sensible defaults that activate the moment
	// segmenting is needed. ForceSegmented is the explicit opt-in for
	// callers that want to test the segmented path on small files.
	useSegmented := opts.ForceSegmented || (size > 0 && size > opts.SegmentSize)

	if !useSegmented || size <= 0 {
		return u.uploadSmall(ctx, src, filename)
	}
	return u.uploadSegmented(ctx, src, filename, size, opts)
}

// DirectUpload is the response to a direct-to-storage upload request. The
// bytes are uploaded straight to the returned SASURL (a pre-authorized,
// writable Azure Blob URL) instead of streamed through Aprimo's upload
// service, so the transfer never touches the rate-limited Aprimo API.
// Token references the uploaded file in the record APIs — but only AFTER
// the blob upload has finished.
type DirectUpload struct {
	Token  string `json:"token"`
	SASURL string `json:"sasUrl"`
}

// CreateDirectUpload asks Aprimo for a direct-to-storage upload slot: a
// SAS URL to upload the bytes to and a token to reference them with. This
// is a single Aprimo API call (one rate-limit token); the bytes go
// straight to blob storage out-of-band. The caller uploads to SASURL,
// then passes Token to Records.Create/Update — see the connector's direct
// path for the orchestration.
func (u *Uploader) CreateDirectUpload(ctx context.Context, filename string) (*DirectUpload, error) {
	if filename == "" {
		return nil, &Error{Message: "uploader: filename is required", Cause: ErrUpload}
	}
	var out DirectUpload
	if err := u.r.postJSON(ctx, "/uploads", map[string]any{"fileName": filename}, &out, nil); err != nil {
		return nil, wrapUpload("uploader: create direct upload", err)
	}
	if out.Token == "" || out.SASURL == "" {
		return nil, &Error{Message: "uploader: direct upload response missing token or sasUrl", Cause: ErrUpload}
	}
	return &out, nil
}

func (u *Uploader) uploadSmall(ctx context.Context, src connector.SegmentSource, filename string) (*UploadResult, error) {
	// Streaming factory: each attempt re-opens the source (cheap for
	// every source connector we ship; OpenRange is a ranged GET) and
	// builds a fresh multipart pipe. No segment-sized buffer is held
	// across the request lifetime.
	factory := RequestBodyFactory(func() (io.ReadCloser, string, error) {
		rc, err := src.Open(ctx, 0, src.Size())
		if err != nil {
			return nil, "", &Error{Message: "uploader: open source", Cause: err}
		}
		body, contentType := streamMultipart("file1", filename, rc)
		return body, contentType, nil
	})
	var out UploadResult
	if err := u.r.postJSON(ctx, "/uploads", factory, &out, nil); err != nil {
		return nil, wrapUpload("uploader: single-shot upload", err)
	}
	return &out, nil
}

func (u *Uploader) uploadSegmented(
	ctx context.Context,
	src connector.SegmentSource,
	filename string,
	size int64,
	opts *UploadOptions,
) (*UploadResult, error) {
	// Step 1: setup — skipped when resuming an existing session.
	var uploadPath string
	if opts.Resume != nil && opts.Resume.UploadPath != "" {
		uploadPath = opts.Resume.UploadPath
	} else {
		var setup struct {
			URI string `json:"uri"`
		}
		if err := u.r.postJSON(ctx, "/uploads/segments",
			map[string]any{"filename": filename}, &setup, nil); err != nil {
			return nil, wrapUpload("uploader: setup", err)
		}
		if setup.URI == "" {
			return nil, &Error{Message: "uploader: setup returned empty uri", Cause: ErrUpload}
		}
		var err error
		uploadPath, err = pathFromURI(setup.URI)
		if err != nil {
			return nil, &Error{Message: "uploader: parse setup uri", Cause: err}
		}
		if opts.OnSetupComplete != nil {
			opts.OnSetupComplete(uploadPath)
		}
	}

	segmentCount := int((size + opts.SegmentSize - 1) / opts.SegmentSize)
	if segmentCount == 0 {
		segmentCount = 1
	}

	skip := make(map[int]bool)
	if opts.Resume != nil {
		for _, i := range opts.Resume.CommittedSegments {
			skip[i] = true
		}
	}

	// Step 2: upload segments with a prefetch pipeline.
	//
	// Two concurrent stages, joined by a bounded channel:
	//
	//   producer (1 goroutine): walks the segment list in order, calls
	//     src.Open(start, length) for each, pushes the ReadCloser into
	//     `ready`. The channel buffer is sized at
	//     ParallelSegments + PrefetchSegments so the producer can run
	//     ahead of the uploaders by up to PrefetchSegments segments —
	//     hiding source-side OpenRange latency on the next segment
	//     while the current one is still uploading.
	//
	//   consumers (ParallelSegments goroutines): drain `ready`, POST
	//     each segment with the prefetched ReadCloser as the first
	//     attempt body. On 429 retry the factory falls back to a fresh
	//     OpenRange, so retry doesn't require the prefetch to be
	//     replayable.
	uploadCtx, cancelCtx := context.WithCancel(ctx)
	defer cancelCtx()

	var (
		uploadedMu sync.Mutex
		uploaded   int
		firstErr   error
		errOnce    sync.Once
	)
	recordErr := func(err error) {
		errOnce.Do(func() {
			firstErr = err
			cancelCtx()
		})
	}

	// Pre-count skipped segments so OnProgress shows "X of total" with
	// the right baseline on resume.
	uploaded = len(skip)

	// Producer pool. PrefetchSegments controls how many OpenRange
	// calls can be in flight concurrently against the source — when
	// >1, the source-side latency of multiple segments overlaps. A
	// single producer goroutine would otherwise serialize OpenRange
	// (worth it only when OpenRange is essentially instant; for
	// cloud sources it isn't). Channel buffer is sized big enough to
	// hold one segment per upload slot plus the active prefetch
	// workers, so a producer never blocks unless the consumers are
	// truly saturated.
	producerWorkers := opts.PrefetchSegments
	if producerWorkers < 1 {
		producerWorkers = 1
	}
	bufferDepth := opts.ParallelSegments + producerWorkers
	ready := make(chan prefetchedSegment, bufferDepth)

	// Shared work queue: every producer worker pulls the next segment
	// index atomically until segmentCount is exhausted.
	var nextIndex atomic.Int32
	var producerWG sync.WaitGroup
	for w := 0; w < producerWorkers; w++ {
		producerWG.Add(1)
		go func() {
			defer producerWG.Done()
			for {
				if uploadCtx.Err() != nil {
					return
				}
				i := int(nextIndex.Add(1) - 1)
				if i >= segmentCount {
					return
				}
				if skip[i] {
					continue
				}
				start := int64(i) * opts.SegmentSize
				length := opts.SegmentSize
				if start+length > size {
					length = size - start
				}
				rc, openErr := src.Open(uploadCtx, start, length)
				select {
				case ready <- prefetchedSegment{index: i, start: start, length: length, rc: rc, openErr: openErr}:
				case <-uploadCtx.Done():
					if rc != nil {
						_ = rc.Close()
					}
					return
				}
			}
		}()
	}
	go func() {
		producerWG.Wait()
		close(ready)
	}()

	// Consumers.
	sem := make(chan struct{}, opts.ParallelSegments)
	var wg sync.WaitGroup
	for chunk := range ready {
		chunk := chunk
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-uploadCtx.Done():
				if chunk.rc != nil {
					_ = chunk.rc.Close()
				}
				return
			}
			defer func() { <-sem }()

			if chunk.openErr != nil {
				recordErr(fmt.Errorf("open range [%d,%d): %w",
					chunk.start, chunk.start+chunk.length, chunk.openErr))
				return
			}
			if err := u.uploadSegmentWithPrefetch(uploadCtx, src, uploadPath, filename,
				chunk.index, chunk.start, chunk.length, chunk.rc); err != nil {
				recordErr(err)
				return
			}
			if opts.OnSegmentCommit != nil {
				opts.OnSegmentCommit(chunk.index)
			}
			uploadedMu.Lock()
			uploaded++
			done := uploaded
			uploadedMu.Unlock()
			if opts.OnProgress != nil {
				opts.OnProgress(done, segmentCount)
			}
		}()
	}
	wg.Wait()

	if firstErr != nil {
		if err := u.cleanupCancelledUpload(uploadPath); err != nil {
			slog.Warn("uploader: cancelUpload after segment failure",
				"upload_path", uploadPath, "err", err)
		}
		return nil, wrapUpload("uploader: segment", firstErr)
	}
	if err := ctx.Err(); err != nil {
		if cerr := u.cleanupCancelledUpload(uploadPath); cerr != nil {
			slog.Warn("uploader: cancelUpload after ctx cancel",
				"upload_path", uploadPath, "err", cerr)
		}
		return nil, &Error{Message: "uploader: cancelled", Cause: err}
	}

	// Step 3: commit.
	var commit UploadResult
	if err := u.r.postJSON(ctx, uploadPath+"/commit",
		map[string]any{"filename": filename, "segmentcount": segmentCount},
		&commit, nil); err != nil {
		return nil, wrapUpload("uploader: commit", err)
	}
	if commit.Token == "" {
		return nil, &Error{Message: "uploader: commit returned empty token", Cause: ErrUpload}
	}
	return &commit, nil
}

// prefetchedSegment is the unit the producer-consumer pipeline passes
// from the OpenRange producer to the upload consumers. `rc` may be
// nil if `openErr` is set.
type prefetchedSegment struct {
	index   int
	start   int64
	length  int64
	rc      io.ReadCloser
	openErr error
}

// uploadSegmentWithPrefetch posts one segment, using the already-open
// `prefetched` reader for the first attempt and re-opening on retry
// (e.g., on 429). If the request layer never calls the factory (early
// cancellation), the deferred cleanup closes the prefetched reader so
// the source connector doesn't leak an HTTP body.
func (u *Uploader) uploadSegmentWithPrefetch(
	ctx context.Context,
	src connector.SegmentSource,
	uploadPath, filename string,
	index int,
	start, length int64,
	prefetched io.ReadCloser,
) error {
	fieldName := fmt.Sprintf("segment%d", index)
	segmentFilename := fmt.Sprintf("%s.segment%d", filename, index)

	var once sync.Once
	closePrefetched := func() {
		if prefetched != nil {
			_ = prefetched.Close()
		}
	}
	defer func() {
		once.Do(closePrefetched)
	}()

	factory := RequestBodyFactory(func() (io.ReadCloser, string, error) {
		var rc io.ReadCloser
		// First attempt: hand over the prefetched reader.
		// Subsequent attempts: fresh OpenRange.
		once.Do(func() {
			rc = prefetched
		})
		if rc == nil {
			var err error
			rc, err = src.Open(ctx, start, length)
			if err != nil {
				return nil, "", fmt.Errorf("open range [%d,%d): %w", start, start+length, err)
			}
		}
		body, contentType := streamMultipart(fieldName, segmentFilename, rc)
		return body, contentType, nil
	})
	return u.r.postJSON(ctx, fmt.Sprintf("%s?index=%d", uploadPath, index),
		factory, nil, nil)
}

func (u *Uploader) cancelUpload(ctx context.Context, uploadPath string) error {
	return u.r.deleteJSON(ctx, uploadPath, nil)
}

// cleanupCancelledUpload calls cancelUpload with a bounded timeout
// rooted at context.Background(). We deliberately don't inherit the
// caller's ctx: cleanup must run AFTER caller ctx cancellation. The
// bound prevents a wedged Aprimo endpoint from hanging the cleanup
// indefinitely.
func (u *Uploader) cleanupCancelledUpload(uploadPath string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return u.cancelUpload(ctx, uploadPath)
}

// copyBufSize is the buffer the multipart pump copies through. 1 MiB is
// well past the point of diminishing syscall returns for any segment
// size; larger just wastes pooled memory.
const copyBufSize = 1 << 20

// copyBufPool recycles copy buffers across uploads so concurrent segments
// and the many small single-shot uploads of a bulk import reuse memory
// instead of churning the GC. Buffers are returned by pointer to avoid
// the per-Put slice-header allocation.
var copyBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, copyBufSize)
		return &b
	},
}

// streamMultipart builds an io.ReadCloser that lazily produces a
// multipart body wrapping `src`. Memory use is bounded by the
// io.Pipe internal buffer (~64 KiB) instead of the full segment size.
//
// The returned body wraps `src` so closing the body also closes the
// underlying source — this is what lets an HTTP retry path discard
// the in-flight body without leaking the source connector's
// OpenRange reader.
//
// If src is itself an io.ReadCloser, its Close is invoked when the
// returned body is closed. Callers must not close `src` themselves.
func streamMultipart(fieldName, filename string, src io.ReadCloser) (io.ReadCloser, string) {
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	contentType := mw.FormDataContentType()

	go func() {
		// CloseWithError(nil) is equivalent to Close(); either way the
		// pipe reader sees EOF. CloseWithError(err) makes the reader
		// see err so the HTTP client surfaces the read failure.
		var err error
		defer func() { _ = pw.CloseWithError(err) }()

		part, perr := mw.CreateFormFile(fieldName, filename)
		if perr != nil {
			err = perr
			return
		}
		// Copy through a large pooled buffer rather than io.Copy's 32 KiB
		// default: it cuts source reads and io.Pipe handoffs (and their
		// goroutine wakeups) by ~30x on a 20 MiB segment. Pooled so the
		// thousands of small single-shot uploads in a bulk import don't
		// each allocate a megabyte.
		bufp := copyBufPool.Get().(*[]byte)
		_, cerr := io.CopyBuffer(part, src, *bufp)
		copyBufPool.Put(bufp)
		if cerr != nil {
			err = cerr
			return
		}
		if cerr := mw.Close(); cerr != nil {
			err = cerr
			return
		}
	}()

	return &streamingBody{pr: pr, src: src}, contentType
}

// streamingBody adapts an io.Pipe to an io.ReadCloser whose Close()
// also closes the underlying source. Critical for retry hygiene: when
// the HTTP layer abandons an in-flight body on 429 it must release
// the source connector's range-read handle, not leave it dangling.
type streamingBody struct {
	pr   *io.PipeReader
	src  io.ReadCloser
	mu   sync.Mutex
	done bool
}

func (b *streamingBody) Read(p []byte) (int, error) { return b.pr.Read(p) }

func (b *streamingBody) Close() error {
	b.mu.Lock()
	if b.done {
		b.mu.Unlock()
		return nil
	}
	b.done = true
	b.mu.Unlock()
	// Close pipe reader first to unblock the writer goroutine if it's
	// still iterating; the goroutine's pw.CloseWithError will surface
	// io.ErrClosedPipe back through any pending writes, exiting.
	perr := b.pr.Close()
	rerr := b.src.Close()
	if perr != nil {
		return perr
	}
	return rerr
}

// pathFromURI extracts the path component from an absolute or already
// relative URI. The setup response gives us an absolute uri whose host
// we ignore (we keep using the configured baseURL).
func pathFromURI(s string) (string, error) {
	u, err := url.Parse(s)
	if err != nil {
		return "", err
	}
	if u.Path == "" {
		return "", fmt.Errorf("uri has no path: %q", s)
	}
	return u.Path, nil
}

// wrapUpload tags an error as upload-class while preserving the
// underlying *Error fields for callers that inspect them.
func wrapUpload(context string, err error) error {
	var e *Error
	if errors.As(err, &e) {
		if e.Cause == nil {
			e.Cause = ErrUpload
		}
		e.Message = context + ": " + e.Message
		return e
	}
	return &Error{Message: context, Cause: fmt.Errorf("%w: %w", ErrUpload, err)}
}
