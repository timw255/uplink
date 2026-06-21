package importer

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime/debug"
	"slices"
	"sync"
	"time"

	"github.com/timw255/uplink/internal/adaptive"
	"github.com/timw255/uplink/internal/aprimo"
	"github.com/timw255/uplink/internal/connector"
)

// workRecord carries one manifest line through the pipeline. It holds the raw
// line bytes (re-parsed on demand for the full record) plus the routing fields
// set during the pre-scan, rather than keeping every parsed record resident.
type workRecord struct {
	line int
	hash string
	size int64  // stat'd file size; 0 for metadata-only
	id   string // rec.ID, set during the pre-scan
	file string // rec.File, set during the pre-scan
	raw  []byte
}

func (w workRecord) result() Result {
	return Result{Line: w.line, Hash: w.hash, File: w.file}
}

func (w workRecord) action() Action {
	switch {
	case w.id != "" && w.file != "":
		return ActionUpdated
	case w.id != "":
		return ActionMetadata
	default:
		return ActionCreated
	}
}

func (w workRecord) parse() (Record, error) {
	return parseLine(w.raw)
}

// recoverPanic logs a recovered worker panic with its stack and returns
// the error to attribute to the record, so one record-triggered panic
// can't crash an unattended run.
func recoverPanic(logger *slog.Logger, line int, r any) error {
	logger.Error("import worker panic recovered",
		"line", line, "panic", r, "stack", string(debug.Stack()))
	return fmt.Errorf("panic: %v", r)
}

// Destination is the slice of the Aprimo connector the importer drives.
// The pipeline uploads and creates as two separate operations so a slow
// upload never throttles the rate-limited record creation. The Aprimo
// *Connector satisfies it.
type Destination interface {
	// UploadOnly streams a file's bytes to storage and returns its token,
	// without creating a record (off the rate limiter for direct-to-blob).
	UploadOnly(ctx context.Context, srcPath string, body connector.SegmentSource, meta map[string]any) (string, error)
	// CreateFromToken creates/updates a record from an already-uploaded token.
	CreateFromToken(ctx context.Context, srcPath, token string, meta map[string]any) (connector.Entry, error)
	// WriteMetadata patches metadata onto an existing record (no upload).
	WriteMetadata(ctx context.Context, recordID string, meta map[string]any) error
	// ValidateFields resolves field metadata without any API call (dry-run + pre-scan).
	ValidateFields(meta map[string]any) error
}

// rateControlled is the optional interface a Destination implements to
// expose its rate limits and accept a telemetry sink. The real Aprimo
// connector implements it.
type rateControlled interface {
	RateLimit() (rps float64, maxConcurrent int)
	SetRateObserver(obs aprimo.RateObserver)
}

// masterFileResolver is the optional capability a Destination implements to
// batch-resolve record → master-file ids (the Aprimo connector does). It
// lets the update path skip a per-record MasterFile GET — hundreds of search
// calls instead of hundreds of thousands of GETs on an update-heavy run.
type masterFileResolver interface {
	ResolveMasterFiles(ctx context.Context, recordIDs []string) (map[string]string, error)
}

// collectionFiler is the optional capability a Destination implements to
// file records into a default collection in batches (the Aprimo connector
// does). When present, the importer suppresses the connector's per-record
// filing and instead files all created records in chunks after the run,
// tracking progress in the ledger — instead of one rate-limited call per
// record. DefaultCollection is empty when no collection is configured.
type collectionFiler interface {
	DefaultCollection() string
	AddRecordsToCollection(ctx context.Context, recordIDs []string) error
}

// Concurrency defaults. Uploads run off the rate limiter (bandwidth-bound)
// so the pool is wide; creates are limiter-paced so a modest pool keeps it
// saturated; the queue bounds how far uploads run ahead of creates (caps
// crash re-work).
const (
	defaultUploadConcurrency = 32
	defaultCreateConcurrency = 16
)

// Options configures a single import run.
type Options struct {
	// ManifestPath is the JSONL file to read.
	ManifestPath string

	// Dest is the Aprimo destination connector (required).
	Dest Destination

	// Source supplies asset bytes for records that carry a "file". May be
	// nil when every record is metadata-only.
	Source connector.Connector

	// SourceName / DestName are used only for human-readable messages.
	SourceName string
	DestName   string

	// DryRun validates every record (metadata resolves, file exists) and
	// writes nothing.
	DryRun bool

	// StopOnError aborts the run on the first failing record.
	StopOnError bool

	// ResultsPath, when set, receives a JSONL ledger of per-record outcomes.
	ResultsPath string

	// Resume skips records already recorded as done in ResultsPath.
	Resume bool

	// UploadConcurrency caps concurrent blob uploads (the bandwidth knob;
	// uploads don't touch the rate limiter). 0 → MaxWorkers, then a default.
	UploadConcurrency int
	// CreateConcurrency caps concurrent record writes (the rate limiter
	// paces these). 0 → a default.
	CreateConcurrency int
	// MaxWorkers is a convenience alias: when set (and the specific
	// concurrencies aren't), it caps the upload pool.
	MaxWorkers int

	// StatusWriter, when non-nil and a TTY, receives a single live status
	// line refreshed in place. nil → periodic log lines.
	StatusWriter io.Writer
	// StatusInterval overrides the status refresh cadence (tests).
	StatusInterval time.Duration

	Logger *slog.Logger
}

// Result is one record's outcome, written to the ledger and tallied.
type Result struct {
	Line   int    `json:"line"`
	Hash   string `json:"hash"`
	Action string `json:"action"` // created|updated|metadata|valid|invalid|skipped|error
	DestID string `json:"dest_id,omitempty"`
	File   string `json:"file,omitempty"`
	Err    string `json:"error,omitempty"`
	// Warn is a non-fatal note (the record is still valid). Today the only
	// case is a filename Aprimo will rewrite on upload.
	Warn string `json:"warn,omitempty"`
	// Token is set only on an "uploaded" ledger row — the upload token the
	// bytes landed under, so resume can create from it without re-uploading.
	Token string `json:"token,omitempty"`
	// Filed is set only on a "filed" ledger marker row — the record ids
	// filed into the default collection in one batch, so resume skips them.
	Filed []string `json:"filed,omitempty"`
}

// Summary tallies a run.
type Summary struct {
	Total     int
	Created   int
	Updated   int
	Metadata  int
	Skipped   int
	Valid     int // dry-run
	Invalid   int // dry-run
	Rewritten int // dry-run: files whose name Aprimo will rewrite
	Failed    int
	// Aborted is the count of records left unprocessed because the run was
	// stopped early (--stop-on-error tripped, or an interrupt). These are
	// not failures — they were never attempted, and a resume retries them.
	Aborted int
	// Filed is how many records were filed into the default collection this
	// run (0 when no default collection is configured).
	Filed int
	// Unfiled is how many created records still need filing into the default
	// collection because a filing batch failed (the records exist; a re-run
	// finishes them). 0 on a clean run.
	Unfiled int
	// Problems holds the first maxProblems failed/invalid records (line +
	// reason) so the caller can show what needs fixing without re-reading the
	// ledger. Invalid/Failed carry the true totals.
	Problems []Problem
	Elapsed  time.Duration
}

// Problem is one record that failed validation or import, for display.
type Problem struct {
	Line   int
	Reason string
}

// maxProblems caps how many problem details a Summary carries — enough to
// act on, without holding tens of thousands of strings for a wholly broken
// manifest. The Invalid/Failed counts still reflect the true totals.
const maxProblems = 100

func (s *Summary) add(r Result) {
	// "uploaded" and "filed" are intermediate/marker ledger rows, not
	// finished records — the same record also produces a final
	// created/updated row, so counting them would double-count.
	switch Action(r.Action) {
	case ActionUploaded, ActionFiled:
		return
	}
	s.Total++
	switch Action(r.Action) {
	case ActionCreated:
		s.Created++
	case ActionUpdated:
		s.Updated++
	case ActionMetadata:
		s.Metadata++
	case "valid":
		s.Valid++
	case "invalid":
		s.Invalid++
	case "skipped":
		s.Skipped++
	case "error":
		s.Failed++
	}
	if r.Warn != "" {
		s.Rewritten++
	}
}

// Importer executes one run. Construct via New.
type Importer struct {
	opts   Options
	logger *slog.Logger
}

func New(opts Options) (*Importer, error) {
	if opts.ManifestPath == "" {
		return nil, errors.New("importer: manifest path is required")
	}
	if opts.Dest == nil {
		return nil, errors.New("importer: destination is required")
	}
	if opts.Resume && opts.ResultsPath == "" {
		return nil, errors.New("importer: Resume requires ResultsPath")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Importer{opts: opts, logger: opts.Logger}, nil
}

// concurrency resolves the pool sizes from options + destination limits.
func (im *Importer) concurrency() (upload, create int) {
	maxConcurrent := 0
	if rc, ok := im.opts.Dest.(rateControlled); ok {
		_, maxConcurrent = rc.RateLimit()
	}
	upload = im.opts.UploadConcurrency
	if upload <= 0 {
		upload = im.opts.MaxWorkers
	}
	if upload <= 0 {
		upload = maxConcurrent
	}
	if upload <= 0 {
		upload = defaultUploadConcurrency
	}
	create = im.opts.CreateConcurrency
	if create <= 0 {
		create = defaultCreateConcurrency
	}
	return upload, create
}

// resolveMasterFiles batch-resolves the master file id for every
// update-with-file record up front, so each such record's create skips the
// per-record MasterFile GET. Best-effort: a destination without the
// capability, or a failed resolve, just leaves the map empty and the
// connector falls back to the per-record lookup.
func (im *Importer) resolveMasterFiles(ctx context.Context, passed []workRecord) map[string]string {
	r, ok := im.opts.Dest.(masterFileResolver)
	if !ok {
		return nil
	}
	var ids []string
	for _, wr := range passed {
		if wr.action() == ActionUpdated { // id + file → needs the master file
			ids = append(ids, wr.id)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	m, err := r.ResolveMasterFiles(ctx, ids)
	if err != nil {
		// Use the partial result: resolved records skip the per-record GET,
		// the rest fall back to it.
		im.logger.Warn("some master-file batches failed; those records use per-record lookup",
			"err", err, "resolved", len(m), "update_records", len(ids))
		return m
	}
	im.logger.Info("resolved master files up front", "update_records", len(ids), "resolved", len(m))
	return m
}

// fileCreatedRecords files every created record into the default collection
// in chunks, writing a ledger marker after each successful chunk so resume
// skips it. Chunked at the SDK's batch size so each chunk is exactly one
// API call and one marker. Filing failures are logged, not fatal: the
// records exist, and a resume re-files whatever has no marker (idempotent).
func (im *Importer) fileCreatedRecords(ctx context.Context, filer collectionFiler, l *ledger, ids []string) int {
	if len(ids) == 0 {
		return 0
	}
	im.logger.Info("filing records into collection", "records", len(ids), "collection", filer.DefaultCollection())
	// A brief, visible note for the post-upload phase on an interactive run —
	// otherwise a big batch looks like a hang between the last upload and the
	// summary. Cleared before the caller prints the summary.
	if w := im.opts.StatusWriter; w != nil {
		fmt.Fprintf(w, "\rfinishing up — filing %d records into the collection…", len(ids))
		defer fmt.Fprint(w, "\r\033[K")
	}
	filed, failed := 0, 0
	for chunk := range slices.Chunk(ids, aprimo.CollectionBatchSize) {
		if err := filer.AddRecordsToCollection(ctx, chunk); err != nil {
			if ctx.Err() != nil {
				break // run winding down — resume re-files the rest
			}
			// Failed batch: skip it (no marker → resume re-files it) and keep
			// filing the others.
			im.logger.Warn("collection filing batch failed; resume will re-file it", "err", err)
			failed += len(chunk)
			continue
		}
		if werr := l.write(Result{Action: string(ActionFiled), Filed: chunk}); werr != nil {
			im.logger.Warn("ledger write (filed marker) failed", "err", werr)
		}
		filed += len(chunk)
	}
	if failed > 0 || ctx.Err() != nil {
		im.logger.Warn("collection filing incomplete; resume will finish it", "filed", filed, "unfiled", len(ids)-filed)
	} else {
		im.logger.Info("collection filing complete", "filed", filed)
	}
	return filed
}

// Run loads the manifest, validates + stats every record up front, then
// drives the two-stage upload→create pipeline (or, for a dry run, just
// reports validity). Returns a tally. The error is non-nil only for setup
// failures; per-record failures land in the Summary and the ledger.
func (im *Importer) Run(ctx context.Context) (Summary, error) {
	start := time.Now()

	resume := resumeState{done: map[string]bool{}, uploaded: map[string]string{}}
	if im.opts.Resume {
		var err error
		resume, err = loadLedgerState(im.opts.ResultsPath)
		if err != nil {
			return Summary{}, fmt.Errorf("load resume ledger: %w", err)
		}
		if len(resume.done) > 0 || len(resume.uploaded) > 0 || len(resume.unfiled) > 0 {
			im.logger.Info("resuming", "already_done", len(resume.done),
				"pre_uploaded", len(resume.uploaded), "unfiled", len(resume.unfiled))
		}
	}
	done, uploaded := resume.done, resume.uploaded

	// Collection filing: when the destination files into a default
	// collection, batch it (suppress the connector's per-record filing) and
	// track it in the ledger instead of one rate-limited call per record. A
	// dry run never files — it must not write to Aprimo.
	filer, filing := im.opts.Dest.(collectionFiler)
	filing = filing && filer.DefaultCollection() != "" && !im.opts.DryRun
	var createdIDs []string // record ids created this run, to file in batches

	ledger, err := newLedger(im.opts.ResultsPath, im.opts.Resume)
	if err != nil {
		return Summary{}, fmt.Errorf("open results ledger: %w", err)
	}
	defer func() { _ = ledger.close() }()

	// Telemetry: the rate observer (req/s) is attached for real runs; the
	// liveStats counters track the upload pipe.
	m := &adaptive.Metrics{}
	var stats *liveStats
	if !im.opts.DryRun {
		stats = &liveStats{}
		if rc, ok := im.opts.Dest.(rateControlled); ok {
			rc.SetRateObserver(m)
			defer rc.SetRateObserver(nil)
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		summary Summary
		sumMu   sync.Mutex
	)
	results := make(chan Result, 256)

	// Single drainer keeps the ledger ordered and the summary race-free.
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		for r := range results {
			// Once the run is winding down (--stop-on-error tripped, or an
			// interrupt), the in-flight records fail with "context canceled".
			// That's an abort, not a failure — don't write, count, or log it.
			// Left un-done in the ledger, a resume picks them back up.
			if r.Action == "error" && runCtx.Err() != nil {
				continue
			}
			if werr := ledger.write(r); werr != nil {
				im.logger.Warn("ledger write failed", "line", r.Line, "err", werr)
			}
			if filing && Action(r.Action) == ActionCreated && r.DestID != "" {
				createdIDs = append(createdIDs, r.DestID) // file after the run
			}
			sumMu.Lock()
			summary.add(r)
			if (r.Action == "invalid" || r.Action == "error") && len(summary.Problems) < maxProblems {
				summary.Problems = append(summary.Problems, Problem{Line: r.Line, Reason: r.Err})
			}
			sumMu.Unlock()
			im.logResult(r)
			if r.Action == "error" && im.opts.StopOnError {
				cancel()
			}
		}
	}()

	recs, total, lerr := im.scanManifest(runCtx, done, results)
	if lerr != nil {
		cancel()
		close(results)
		<-drainDone
		return Summary{}, lerr
	}

	// Status reporter (now that total is known).
	reportStop := make(chan struct{})
	reportDone := make(chan struct{})
	go im.runReporter(reportStop, reportDone, &summary, &sumMu, m, stats, total, start)

	upload, create := im.concurrency()
	v := &validator{dest: im.opts.Dest, source: im.opts.Source, logger: im.logger}
	if im.opts.DryRun {
		v.dryRun(runCtx, recs, results, upload)
	} else {
		passed := v.prescan(runCtx, recs, results, upload, uploaded)
		p := &pipeline{
			dest:            im.opts.Dest,
			source:          im.opts.Source,
			results:         results,
			stats:           stats,
			logger:          im.logger,
			uploadCap:       upload,
			createConc:      create,
			masterFiles:     im.resolveMasterFiles(runCtx, passed),
			deferCollection: filing,
		}
		p.run(runCtx, passed, uploaded)
	}

	close(results)
	<-drainDone
	close(reportStop)
	<-reportDone

	// File all created records into the default collection in batches, now
	// that the drainer has the full set. Prepend any records left unfiled by
	// a prior run (resume). Skip on an aborted run (the cancelled context
	// would just fail every batch) — resume re-files them; idempotent.
	if filing && runCtx.Err() == nil {
		toFile := append(resume.unfiled, createdIDs...)
		summary.Filed = im.fileCreatedRecords(runCtx, filer, ledger, toFile)
		summary.Unfiled = len(toFile) - summary.Filed
	}

	// Records that parsed but never produced a result were abandoned when
	// the run stopped early — report them as aborted, not failed.
	if n := total - summary.Total; n > 0 {
		summary.Aborted = n
	}
	summary.Elapsed = time.Since(start)
	return summary, nil
}

// scanManifest reads the manifest once, holding each non-blank, not-already-
// done line's raw bytes (parsing happens later, in the pre-scan). Resume-skips
// are emitted here. Returns the index and the total non-blank line count.
func (im *Importer) scanManifest(ctx context.Context, done map[string]bool, results chan<- Result) ([]workRecord, int, error) {
	f, err := os.Open(im.opts.ManifestPath)
	if err != nil {
		return nil, 0, fmt.Errorf("open manifest: %w", err)
	}
	defer func() { _ = f.Close() }()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)

	var recs []workRecord
	total, line := 0, 0
	for sc.Scan() {
		raw := sc.Bytes()
		line++
		if len(trimSpace(raw)) == 0 {
			continue
		}
		total++
		hash := hashLine(raw)
		if done[hash] {
			emit(ctx, results, Result{Line: line, Hash: hash, Action: "skipped"})
			continue
		}
		// Copy: the scanner reuses its buffer on the next Scan.
		recs = append(recs, workRecord{line: line, hash: hash, raw: append([]byte(nil), raw...)})
	}
	if err := sc.Err(); err != nil {
		return nil, total, fmt.Errorf("read manifest: %w", err)
	}
	return recs, total, nil
}

func (im *Importer) logResult(r Result) {
	if r.Warn != "" {
		im.logger.Warn("import record warning", "line", r.Line, "warn", r.Warn)
	}
	switch r.Action {
	case "error", "invalid":
		// Per-record detail is carried in Summary.Problems and shown by the
		// caller; keep the log at debug so it doesn't double up on a TTY.
		im.logger.Debug("import record failed", "line", r.Line, "err", r.Err)
	default:
		im.logger.Debug("import record ok", "line", r.Line, "action", r.Action, "dest_id", r.DestID)
	}
}

func fail(res Result, err error) Result {
	res.Action = "error"
	res.Err = err.Error()
	return res
}

// invalid marks a dry-run record as failing validation (distinct from a
// real-run "error" so the summary can report them separately).
func invalid(res Result, err error) Result {
	res.Action = "invalid"
	res.Err = err.Error()
	return res
}

// emit sends a result unless ctx is already cancelled.
func emit(ctx context.Context, results chan<- Result, r Result) {
	select {
	case results <- r:
	case <-ctx.Done():
	}
}

// trimSpace reports the input with leading/trailing ASCII whitespace
// removed — used to detect blank manifest lines without allocating.
func trimSpace(b []byte) []byte {
	i, j := 0, len(b)
	for i < j && isSpace(b[i]) {
		i++
	}
	for j > i && isSpace(b[j-1]) {
		j--
	}
	return b[i:j]
}

func isSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\r' || c == '\n' }
