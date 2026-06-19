package importer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/timw255/uplink/internal/aprimo"
	"github.com/timw255/uplink/internal/connector"
)

// liveStats are the upload-pipe counters the status reporter samples.
type liveStats struct {
	uploadsInFlight atomic.Int64 // blob uploads currently streaming
	bytesUploaded   atomic.Int64 // cumulative bytes pushed to blob, for MB/s
}

// createJob is one unit for the create stage: a record plus, for file
// records, the token its bytes were uploaded under (empty for
// metadata-only records, which skip the upload stage).
type createJob struct {
	wr    workRecord
	token string
}

// pipeline is the two-stage upload→create engine. An upload pool streams
// bytes to blob storage (off the rate limiter) and hands tokens to a
// bounded queue; a create pool drains that queue and writes records (paced
// by the rate limiter). Decoupling them means a slow upload never starves
// the create rate, and the size-balanced upload order keeps both the pipe
// and the API busy start to finish. It produces a Result per record into
// results; the caller drains and tallies them.
type pipeline struct {
	dest    Destination
	source  connector.Connector
	results chan<- Result
	stats   *liveStats
	logger  *slog.Logger

	uploadCap  int // fixed pool of file feeders (byte concurrency lives in the connector)
	createConc int // concurrent record writes (rate-limiter paced)
}

// run drives both stages to completion. passed is the validated, stat'd
// work list; uploaded maps a record hash to a saved upload token (resume),
// letting those records skip the upload stage.
func (p *pipeline) run(ctx context.Context, passed []workRecord, uploaded map[string]string) {
	// Never run with an empty pool: zero create workers would deadlock the
	// upload pool on a never-drained queue; zero upload workers would
	// silently drop file records. Callers supply sane values, but the
	// engine must not be a footgun.
	if p.uploadCap < 1 {
		p.uploadCap = 1
	}
	if p.createConc < 1 {
		p.createConc = 1
	}

	fileRecs, direct := p.partition(passed, uploaded)

	// A small buffer between the two stages, scaled to the create pool, so a
	// finished upload always has a slot to hand off to instead of stalling.
	// Purely internal — it's a handoff buffer, not a run-ahead.
	queueDepth := p.createConc * 4
	createWork := make(chan createJob, queueDepth)

	createWG := p.startCreatePool(ctx, createWork)
	uploadWG := p.startUploadPool(ctx, newScheduler(fileRecs), createWork, queueDepth)
	metaDone := p.startDirectFeed(ctx, createWork, direct)

	// Close the create queue once both producers (uploads + direct feed)
	// are done, so the create pool drains and exits.
	go func() {
		uploadWG.Wait()
		<-metaDone
		close(createWork)
	}()

	createWG.Wait()
}

// partition splits the work list: file records still needing an upload go
// to the upload pool; everything ready for the create stage — metadata-only
// records and (on resume) records with a saved token — goes straight to the
// create queue. File records are sorted ascending by size for the blend.
func (p *pipeline) partition(passed []workRecord, uploaded map[string]string) (fileRecs []workRecord, direct []createJob) {
	for _, wr := range passed {
		if wr.rec.File == "" {
			direct = append(direct, createJob{wr: wr})
			continue
		}
		if tok, ok := uploaded[wr.hash]; ok {
			direct = append(direct, createJob{wr: wr, token: tok})
			continue
		}
		fileRecs = append(fileRecs, wr)
	}
	sort.Slice(fileRecs, func(i, j int) bool { return fileRecs[i].size < fileRecs[j].size })
	return fileRecs, direct
}

// startCreatePool launches the create workers. They are paced by the
// shared rate limiter inside the connector.
func (p *pipeline) startCreatePool(ctx context.Context, createWork <-chan createJob) *sync.WaitGroup {
	var wg sync.WaitGroup
	for range p.createConc {
		wg.Go(func() {
			for cj := range createWork {
				p.results <- p.safeCreate(ctx, cj)
			}
		})
	}
	return &wg
}

// startUploadPool launches a fixed pool of file feeders. Each mints a slot
// and streams one file at a time; the actual byte-plane concurrency — and
// its adaptive budget — lives in the connector's blob uploader, one global
// pool shared across every file's blocks. So this pool just keeps files
// flowing: it's paced by the rate-limited mint and by backpressure from a
// full create queue, not by a gate here.
func (p *pipeline) startUploadPool(ctx context.Context, sched *scheduler, createWork chan<- createJob, queueDepth int) *sync.WaitGroup {
	var wg sync.WaitGroup
	for range p.uploadCap {
		wg.Go(func() {
			for {
				if ctx.Err() != nil {
					return // run aborting
				}
				wr, ok := sched.next(len(createWork), queueDepth)
				if !ok {
					return
				}
				job, err := p.safeUpload(ctx, wr)
				if err != nil {
					p.results <- fail(wr.result(), err)
					continue
				}
				// Persist the token (ledger-only, uncounted) so a crash
				// before this record is created lets resume skip the
				// re-upload — for big media the expensive part.
				emit(ctx, p.results, uploadedResult(wr, job.token))
				select {
				case createWork <- job:
				case <-ctx.Done():
					return
				}
			}
		})
	}
	return &wg
}

// startDirectFeed pushes the create-ready records (metadata-only +
// pre-uploaded) straight into the create queue. The returned channel
// closes when the feed is done.
func (p *pipeline) startDirectFeed(ctx context.Context, createWork chan<- createJob, direct []createJob) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, cj := range direct {
			select {
			case createWork <- cj:
			case <-ctx.Done():
				return
			}
		}
	}()
	return done
}

// upload streams one file's bytes to blob storage and returns the create
// job carrying its token. The size came from the pre-scan, so no re-stat.
func (p *pipeline) upload(ctx context.Context, wr workRecord) (createJob, error) {
	seg := connector.SegmentSourceFor(p.source, connector.Entry{Path: wr.rec.File, Size: wr.size})
	if p.stats != nil {
		p.stats.uploadsInFlight.Add(1)
		// Deferred so a panic in UploadOnly (recovered upstream) can't leak
		// the in-flight count.
		defer p.stats.uploadsInFlight.Add(-1)
	}
	token, err := p.dest.UploadOnly(ctx, wr.rec.File, seg, wr.rec.meta())
	if err != nil {
		return createJob{}, err
	}
	if p.stats != nil {
		p.stats.bytesUploaded.Add(wr.size)
	}
	return createJob{wr: wr, token: token}, nil
}

// create writes one record: a metadata PATCH for an id-only record, or
// CreateFromToken for a file record whose bytes are already in blob. If a
// saved token (from a prior run) has been swept by Aprimo's cleanup, the
// create returns ErrUploadTokenMissing and we transparently re-upload and
// retry — no timestamp guard, just handle the failure.
func (p *pipeline) create(ctx context.Context, cj createJob) Result {
	res := cj.wr.result()
	meta := cj.wr.rec.meta()
	if cj.wr.rec.File == "" {
		if err := p.dest.WriteMetadata(ctx, cj.wr.rec.ID, meta); err != nil {
			return fail(res, err)
		}
		res.Action, res.DestID = string(ActionMetadata), cj.wr.rec.ID
		return res
	}
	entry, err := p.dest.CreateFromToken(ctx, cj.wr.rec.File, cj.token, meta)
	if err != nil && errors.Is(err, aprimo.ErrUploadTokenMissing) && p.source != nil {
		// The saved token's blob was swept. Re-stat first (the pre-scan may
		// have skipped or zeroed the size for a pre-uploaded record) so a
		// since-deleted source fails cleanly instead of uploading an empty
		// blob, and the re-upload streams the real bytes.
		statEntry, serr := p.source.Stat(ctx, cj.wr.rec.File)
		if serr != nil {
			return fail(res, fmt.Errorf("re-upload stat %q: %w", cj.wr.rec.File, serr))
		}
		cj.wr.size = statEntry.Size
		job, uerr := p.upload(ctx, cj.wr)
		if uerr != nil {
			return fail(res, uerr)
		}
		entry, err = p.dest.CreateFromToken(ctx, cj.wr.rec.File, job.token, meta)
	}
	if err != nil {
		return fail(res, err)
	}
	res.Action, res.DestID = string(cj.wr.rec.action()), entry.Path
	return res
}

// safeUpload / safeCreate wrap the per-record work with panic recovery, so
// a record-triggered panic deep in the connector/SDK becomes a failed
// record instead of crashing an unattended run.
func (p *pipeline) safeUpload(ctx context.Context, wr workRecord) (job createJob, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = recoverPanic(p.logger, wr.line, r)
		}
	}()
	return p.upload(ctx, wr)
}

func (p *pipeline) safeCreate(ctx context.Context, cj createJob) (res Result) {
	defer func() {
		if r := recover(); r != nil {
			res = fail(cj.wr.result(), recoverPanic(p.logger, cj.wr.line, r))
		}
	}()
	return p.create(ctx, cj)
}

// uploadedResult is the intermediate ledger row recording that a file's
// bytes are in blob storage under token, before its record exists.
func uploadedResult(wr workRecord, token string) Result {
	r := wr.result()
	r.Action = string(ActionUploaded)
	r.Token = token
	return r
}
