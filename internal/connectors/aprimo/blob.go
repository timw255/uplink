package aprimo

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/timw255/uplink/internal/adaptive"
	"github.com/timw255/uplink/internal/connector"
)

// azureBlobHostSuffixes are the Azure Blob storage domains across the
// public and sovereign clouds. A SAS URL must target one of these before
// we stream file bytes to it.
var azureBlobHostSuffixes = []string{
	".blob.core.windows.net",       // public cloud
	".blob.core.chinacloudapi.cn",  // Azure China
	".blob.core.usgovcloudapi.net", // Azure US Government
}

// isAzureBlobURL reports whether raw is an HTTPS URL pointing at an Azure
// Blob storage host. Used to refuse streaming customer bytes anywhere a
// tampered or misconfigured upload response might point — bytes only ever
// go to Azure Blob storage, never an arbitrary host.
func isAzureBlobURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" {
		return false
	}
	host := strings.ToLower(u.Hostname())
	for _, suffix := range azureBlobHostSuffixes {
		if strings.HasSuffix(host, suffix) {
			return true
		}
	}
	return false
}

// blobUploader streams a source's bytes straight to a pre-authorized
// storage URL (the SAS Aprimo hands back from CreateDirectUpload),
// bypassing the rate-limited Aprimo upload API entirely. Behind an
// interface so the connector's direct path is testable without standing
// up real blob storage.
type blobUploader interface {
	Upload(ctx context.Context, sasURL string, body connector.SegmentSource, filename string) error
}

const (
	// directBlockSize is the staging block size. At 16 MiB a single block
	// list (Azure's 50,000-block cap) covers files up to 800 GB without
	// growing the block — bigger files scale the block up automatically.
	// Bigger blocks mean fewer round-trips on a fat pipe; memory stays
	// bounded because only the global budget's worth are ever in flight.
	directBlockSize = 16 << 20 // 16 MiB
	maxBlockCount   = 50000    // Azure's hard cap on blocks per blob

	// defaultBlockCeiling caps the adaptive block-staging budget — the most
	// block PUTs in flight across ALL files at once. By default uplink ramps
	// the live budget up toward this as throughput climbs, so it self-tunes
	// to the pipe; the value is just the upper bound (peak memory ≈ ceiling
	// × directBlockSize). Lower it only if the full ramp is too aggressive
	// for the machine uplink runs on.
	defaultBlockCeiling = 64

	// controllerTick is how often the budget controller re-evaluates MB/s.
	controllerTick = 250 * time.Millisecond

	// blobTryTimeout bounds a single block PUT. A stalled connection (a TCP
	// black hole with no reset) would otherwise hang a worker indefinitely;
	// the SDK cancels and retries instead, so a blip costs seconds.
	blobTryTimeout = 5 * time.Minute
)

// blockUploader uploads files to Azure Blob via the low-level
// StageBlock/CommitBlockList API, with block staging across EVERY file
// drawing from one shared pool of G worker goroutines — a single global
// concurrency budget, the way AzCopy does it, not a per-file pool. That
// keeps total in-flight block PUTs (and memory) flat no matter how many
// files upload at once, and lets each block be read through a concurrent
// ranged Open for genuine parallel staging on a seekable source.
//
// The live budget is adaptive: a controller ramps how many blocks may
// stage at once toward the ceiling while measured MB/s keeps climbing, then
// holds once the pipe plateaus — so it fills a fat link without a knob, and
// backs off on a thin one. The ceiling is the only resource lever.
//
// One instance is shared for the connector's lifetime. Workers start on
// the first real upload (so tests that swap in a fake never spawn them),
// and each pulls block-sized buffers from a shared pool.
type blockUploader struct {
	ceiling    int
	blockSize  int64
	httpClient *http.Client
	jobs       chan blockJob
	bufPool    sync.Pool
	start      sync.Once
	readGate   chan struct{} // serializes disk reads for sequential sources

	gate        *adaptive.Gate
	stagedBytes atomic.Int64
	ctx         context.Context
	cancel      context.CancelFunc
}

// blockJob is one block ready to stage. stage does the PUT (reading the
// source, or pointing Azure at a source URL) and returns the bytes staged;
// cleanup runs afterward no matter what — it returns any pre-read buffer to
// the pool even if the block was skipped because its file already failed.
type blockJob struct {
	ctx     context.Context
	file    *fileUpload
	stage   func(context.Context) (int64, error)
	cleanup func()
}

// presigner is the optional capability a source exposes when it can hand
// out a short-lived authenticated GET URL for an object — letting Azure
// pull the bytes server-side (StageBlockFromURL) instead of routing them
// through this machine. A source with credentials (S3, Azure Blob, B2) can;
// localfs can't. The TTL must outlive the whole file's copy.
type presigner interface {
	PresignGetURL(ctx context.Context, ttl time.Duration) (string, error)
}

// sequencer is the optional capability a source exposes to request
// sequential, one-at-a-time reads — spinning media (a local HDD or a NAS)
// where parallel ranged reads would thrash the heads.
type sequencer interface {
	SequentialReads() bool
}

// presignTTL is how long a source GET URL must stay valid — long enough to
// copy even a multi-terabyte file server-side.
const presignTTL = 12 * time.Hour

// fileUpload tracks one file's blocks: a countdown to "all staged" and the
// first error (which cancels the rest of that file's blocks).
type fileUpload struct {
	remaining atomic.Int64
	done      chan struct{}
	cancel    context.CancelFunc
	mu        sync.Mutex
	err       error
}

func (f *fileUpload) finish(err error) {
	if err != nil {
		f.mu.Lock()
		if f.err == nil {
			f.err = err
		}
		f.mu.Unlock()
		f.cancel() // abort this file's other in-flight blocks
	}
	if f.remaining.Add(-1) == 0 {
		close(f.done)
	}
}

func (f *fileUpload) firstErr() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.err
}

func newBlockUploader(ceiling int, blockSize int64) *blockUploader {
	if ceiling < 1 {
		ceiling = defaultBlockCeiling
	}
	if blockSize <= 0 {
		blockSize = directBlockSize
	}
	ctx, cancel := context.WithCancel(context.Background())
	u := &blockUploader{
		ceiling:    ceiling,
		blockSize:  blockSize,
		httpClient: newBlobHTTPClient(ceiling),
		jobs:       make(chan blockJob, ceiling*2),
		readGate:   make(chan struct{}, 1),
		gate:       adaptive.NewGate(min(ceiling, 4), ceiling),
		ctx:        ctx,
		cancel:     cancel,
	}
	u.bufPool.New = func() any { b := make([]byte, blockSize); return &b }
	return u
}

// Close stops the controller and worker pool. Safe to call once; the
// connector owns the uploader's lifetime.
func (u *blockUploader) Close() {
	u.cancel()
}

// newBlobHTTPClient hands the blob clients a tuned transport instead of the
// SDK default (MaxIdleConnsPerHost=10). Keeping a warm connection per block
// worker avoids the TLS-handshake churn that otherwise throttles cross-file
// concurrency. No client Timeout — per-try bounding is the SDK's job.
func newBlobHTTPClient(budget int) *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = budget * 2
	t.MaxIdleConnsPerHost = budget
	t.MaxConnsPerHost = 0 // unlimited; the block budget is the real cap
	t.IdleConnTimeout = 90 * time.Second
	t.ForceAttemptHTTP2 = true
	return &http.Client{Transport: t}
}

func (u *blockUploader) startWorkers() {
	u.gate.Watch(u.ctx)
	go u.runController()
	for i := 0; i < u.ceiling; i++ {
		go u.worker()
	}
}

func (u *blockUploader) worker() {
	for job := range u.jobs {
		var err error
		// The adaptive gate caps how many blocks stage at once; a worker
		// past the gate holds one buffer, so live memory tracks the budget.
		if e := u.gate.Acquire(job.ctx); e != nil {
			err = e
		} else {
			n, serr := job.stage(job.ctx)
			u.gate.Release()
			if serr == nil {
				u.stagedBytes.Add(n)
			}
			err = serr
		}
		if job.cleanup != nil {
			job.cleanup()
		}
		job.file.finish(err)
	}
}

// runController ramps the block-staging budget toward the ceiling while
// measured upload MB/s keeps climbing, holding once the pipe plateaus —
// the same control law the importer's upload pool uses, but here it owns
// the one global budget every file's blocks draw from.
func (u *blockUploader) runController() {
	ctrl := &adaptive.Controller{TargetRPS: 0, MaxLimit: u.ceiling, Baseline: 1}
	var lastBytes int64
	lastT := time.Now()
	ctrl.Run(u.ctx, u.gate, controllerTick, func() adaptive.Sample {
		now := time.Now()
		b := u.stagedBytes.Load()
		var mbps float64
		if dt := now.Sub(lastT).Seconds(); dt > 0 {
			mbps = float64(b-lastBytes) / dt / (1 << 20)
		}
		// Backlog = bytes still moving or blocks queued. When uploads go
		// idle the controller shrinks the budget back toward baseline so we
		// don't sit on a wide pool of sleeping workers (and their buffers).
		backlog := b > lastBytes || len(u.jobs) > 0
		lastBytes, lastT = b, now
		return adaptive.Sample{Achieved: mbps, HasBacklog: backlog}
	})
}

// Upload streams a file to the dest blob and commits it, picking the
// fastest staging strategy the source supports:
//
//   - server-side from URL — the source can presign an authenticated GET
//     URL, so Azure pulls the bytes itself (StageBlockFromURL) and they
//     never touch this machine. The win for cloud-to-cloud migrations.
//   - sequential read — the source is spinning media (a configured HDD /
//     NAS); blocks are read one file at a time, front-to-back, to avoid a
//     seek storm, while staging still pipelines through the pool.
//   - parallel read — the default; blocks are read concurrently (great on
//     SSD / object storage).
//
// The bytes never touch the Aprimo rate limiter — only the blob endpoint —
// and completion is synchronous: when CommitBlockList returns, the token is
// valid and the record can be created.
func (u *blockUploader) Upload(ctx context.Context, sasURL string, src connector.SegmentSource, filename string) error {
	u.start.Do(u.startWorkers)

	opts := &blockblob.ClientOptions{}
	opts.Transport = u.httpClient
	opts.Retry.TryTimeout = blobTryTimeout
	client, err := blockblob.NewClientWithNoCredential(sasURL, opts)
	if err != nil {
		return fmt.Errorf("blob client: %w", err)
	}

	size := src.Size()
	blockSize, count := planBlocks(size, u.blockSize)
	if count == 0 { // zero-length source: commit an empty blob
		if _, err := client.CommitBlockList(ctx, nil, nil); err != nil {
			return fmt.Errorf("commit %q: %w", filename, err)
		}
		return nil
	}

	if p, ok := src.(presigner); ok {
		if url, perr := p.PresignGetURL(ctx, presignTTL); perr == nil && url != "" {
			return u.uploadFromURL(ctx, client, url, size, blockSize, count, filename)
		}
		// Presign unsupported for this source's auth mode — fall back to
		// streaming the bytes ourselves.
	}
	if s, ok := src.(sequencer); ok && s.SequentialReads() {
		return u.uploadSequential(ctx, client, src, size, blockSize, count, filename)
	}
	return u.uploadParallel(ctx, client, src, size, blockSize, count, filename)
}

// newFile sets up a file's completion tracker and pre-computes its block
// IDs. The returned context is cancelled when any block fails, so siblings
// abort early.
func (u *blockUploader) newFile(parent context.Context, count int) (*fileUpload, context.Context, []string) {
	fileCtx, cancel := context.WithCancel(parent)
	file := &fileUpload{done: make(chan struct{}), cancel: cancel}
	file.remaining.Store(int64(count))
	ids := make([]string, count)
	for i := range ids {
		ids[i] = blockID(i)
	}
	return file, fileCtx, ids
}

// submit hands a block to the pool, reporting whether it made it (a full
// queue blocks; a failed sibling or an aborting run returns false).
func (u *blockUploader) submit(ctx, fileCtx context.Context, job blockJob) bool {
	select {
	case u.jobs <- job:
		return true
	case <-fileCtx.Done():
		return false
	case <-ctx.Done():
		return false
	}
}

// commitFile waits for all of a file's blocks to settle, then commits.
// submitted is how many reached the pool; the rest are charged to the
// countdown here so it can still reach zero.
func (u *blockUploader) commitFile(ctx context.Context, client *blockblob.Client, filename string, file *fileUpload, ids []string, submitted, count int) error {
	defer file.cancel()
	if miss := count - submitted; miss > 0 && file.remaining.Add(-int64(miss)) == 0 {
		close(file.done)
	}
	select {
	case <-file.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	if e := file.firstErr(); e != nil {
		return fmt.Errorf("upload %q: %w", filename, e)
	}
	if _, err := client.CommitBlockList(ctx, ids, nil); err != nil {
		return fmt.Errorf("commit %q: %w", filename, err)
	}
	return nil
}

// uploadFromURL stages every block server-side: Azure fetches each range
// from the presigned source URL itself. No local reads, no buffers — this
// machine just issues the control-plane calls.
func (u *blockUploader) uploadFromURL(ctx context.Context, client *blockblob.Client, sourceURL string, size, blockSize int64, count int, filename string) error {
	file, fileCtx, ids := u.newFile(ctx, count)
	submitted := 0
	for i := 0; i < count; i++ {
		offset := int64(i) * blockSize
		length := blockSize
		if rem := size - offset; rem < length {
			length = rem
		}
		id, off, ln := ids[i], offset, length
		job := blockJob{ctx: fileCtx, file: file, stage: func(sctx context.Context) (int64, error) {
			opts := &blockblob.StageBlockFromURLOptions{Range: blob.HTTPRange{Offset: off, Count: ln}}
			if _, err := client.StageBlockFromURL(sctx, id, sourceURL, opts); err != nil {
				return 0, fmt.Errorf("stage from url: %w", err)
			}
			return ln, nil
		}}
		if !u.submit(ctx, fileCtx, job) {
			break
		}
		submitted++
	}
	return u.commitFile(ctx, client, filename, file, ids, submitted, count)
}

// uploadParallel reads each block's range concurrently through the pool and
// stages it. The default for SSD / object-store sources.
func (u *blockUploader) uploadParallel(ctx context.Context, client *blockblob.Client, src connector.SegmentSource, size, blockSize int64, count int, filename string) error {
	file, fileCtx, ids := u.newFile(ctx, count)
	submitted := 0
	for i := 0; i < count; i++ {
		offset := int64(i) * blockSize
		length := blockSize
		if rem := size - offset; rem < length {
			length = rem
		}
		id, off, ln := ids[i], offset, length
		job := blockJob{ctx: fileCtx, file: file, stage: func(sctx context.Context) (int64, error) {
			return u.readStage(sctx, client, src, off, ln, id)
		}}
		if !u.submit(ctx, fileCtx, job) {
			break
		}
		submitted++
	}
	return u.commitFile(ctx, client, filename, file, ids, submitted, count)
}

// uploadSequential reads the file front-to-back with one reader — holding
// the global read slot so only one file's disk reads run at a time — and
// hands each pre-read block to the pool to stage. Reads stay sequential (no
// seek storm on spinning media); staging still parallelizes.
func (u *blockUploader) uploadSequential(ctx context.Context, client *blockblob.Client, src connector.SegmentSource, size, blockSize int64, count int, filename string) error {
	select {
	case u.readGate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	rc, err := src.Open(ctx, 0, size)
	if err != nil {
		<-u.readGate
		return fmt.Errorf("open %q: %w", filename, err)
	}
	released := false
	releaseRead := func() {
		if !released {
			released = true
			_ = rc.Close()
			<-u.readGate
		}
	}
	defer releaseRead()

	file, fileCtx, ids := u.newFile(ctx, count)
	submitted := 0
	for i := 0; i < count; i++ {
		length := blockSize
		if rem := size - int64(i)*blockSize; rem < length {
			length = rem
		}
		bufp := u.bufPool.Get().(*[]byte)
		pooled := length <= int64(len(*bufp))
		var buf []byte
		if pooled {
			buf = (*bufp)[:length]
		} else {
			u.bufPool.Put(bufp)
			buf = make([]byte, length)
		}
		if _, rerr := io.ReadFull(rc, buf); rerr != nil {
			if pooled {
				u.bufPool.Put(bufp)
			}
			file.finish(fmt.Errorf("read %q: %w", filename, rerr))
			submitted++ // charged the countdown via finish
			break
		}
		id, b := ids[i], buf
		cleanup := func() {}
		if pooled {
			p := bufp
			cleanup = func() { u.bufPool.Put(p) }
		}
		job := blockJob{ctx: fileCtx, file: file, cleanup: cleanup, stage: func(sctx context.Context) (int64, error) {
			if _, err := client.StageBlock(sctx, id, readSeekNopCloser{bytes.NewReader(b)}, nil); err != nil {
				return 0, fmt.Errorf("stage block: %w", err)
			}
			return int64(len(b)), nil
		}}
		if !u.submit(ctx, fileCtx, job) {
			cleanup() // not submitted — return the buffer ourselves
			break
		}
		submitted++
	}
	releaseRead() // reads done; let the next file's reads begin while these stage
	return u.commitFile(ctx, client, filename, file, ids, submitted, count)
}

// readStage reads one block's range from the source and PUTs it. Buffers
// come from the shared pool; an oversized block from a very large file gets
// a one-off allocation rather than poisoning the pool.
func (u *blockUploader) readStage(ctx context.Context, client *blockblob.Client, src connector.SegmentSource, offset, length int64, id string) (int64, error) {
	bufp := u.bufPool.Get().(*[]byte)
	var buf []byte
	if length <= int64(len(*bufp)) {
		buf = (*bufp)[:length]
		defer u.bufPool.Put(bufp)
	} else {
		u.bufPool.Put(bufp)
		buf = make([]byte, length)
	}
	rc, err := src.Open(ctx, offset, length)
	if err != nil {
		return 0, fmt.Errorf("open block: %w", err)
	}
	_, err = io.ReadFull(rc, buf)
	_ = rc.Close()
	if err != nil {
		return 0, fmt.Errorf("read block: %w", err)
	}
	if _, err := client.StageBlock(ctx, id, readSeekNopCloser{bytes.NewReader(buf)}, nil); err != nil {
		return 0, fmt.Errorf("stage block: %w", err)
	}
	return length, nil
}

// planBlocks picks the block size and count for a file. It honors Azure's
// 50,000-block cap by growing the block (rounded to MiB) for very large
// files, so a multi-terabyte file still fits in one block list.
func planBlocks(size, blockSize int64) (int64, int) {
	if size <= 0 {
		return blockSize, 0
	}
	bs := blockSize
	if (size+bs-1)/bs > maxBlockCount {
		const mib = 1 << 20
		bs = ((size + maxBlockCount - 1) / maxBlockCount)
		bs = ((bs + mib - 1) / mib) * mib
	}
	return bs, int((size + bs - 1) / bs)
}

// blockID encodes a block index as a fixed-width base64 string. Azure
// requires every block ID in a blob to decode to the same byte length, so
// the index is zero-padded to a constant width before encoding.
func blockID(i int) string {
	return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%032d", i)))
}

// readSeekNopCloser adapts a *bytes.Reader to io.ReadSeekCloser so
// StageBlock can seek for its own retries without us buffering a copy.
type readSeekNopCloser struct{ *bytes.Reader }

func (readSeekNopCloser) Close() error { return nil }
